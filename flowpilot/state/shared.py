from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol


class SharedStateConflict(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SharedClaim:
    resource_type: str
    scope_key: str
    owner: str
    schema_version: str
    generation: int
    state_version: int
    fencing_token: int
    expires_at: datetime
    last_sequence: int
    payload_digest: str | None


class SharedStateBackend(Protocol):
    async def acquire(
        self,
        resource_type: str,
        scope_key: str,
        *,
        owner: str,
        schema_version: str,
        generation: int,
        ttl_seconds: float,
    ) -> SharedClaim: ...
    async def renew(self, claim: SharedClaim, ttl_seconds: float) -> SharedClaim: ...
    async def commit(
        self,
        claim: SharedClaim,
        *,
        expected_state_version: int,
        sequence: int,
        payload_digest: str,
    ) -> SharedClaim: ...
    async def release(self, claim: SharedClaim) -> None: ...


class SQLiteSharedStateBackend:
    """SQLite test/local backend for single-writer frontier and leases.

    It provides cross-process CAS and fencing for frontier, in-flight binding,
    DCS writer, KV action and trace-writer records.  Payloads are deliberately
    excluded: only caller-provided digests and monotonic metadata are stored.
    SQLite is a local validation backend, not a production HA claim.
    """

    SCHEMA_VERSION = "flowpilot-shared-state-v1"

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS shared_state (
                resource_type TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                generation INTEGER NOT NULL,
                state_version INTEGER NOT NULL,
                owner TEXT NOT NULL,
                fencing_token INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                last_sequence INTEGER NOT NULL,
                payload_digest TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(resource_type, scope_key)
            )
            """
        )

    async def close(self) -> None:
        async with self._lock:
            self._connection.close()

    async def acquire(
        self,
        resource_type: str,
        scope_key: str,
        *,
        owner: str,
        schema_version: str,
        generation: int,
        ttl_seconds: float,
    ) -> SharedClaim:
        if ttl_seconds <= 0 or generation < 0:
            raise ValueError("invalid shared-state lease")
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=ttl_seconds)
        async with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(resource_type, scope_key)
                if row is None:
                    fencing, state_version, last_sequence, digest = 1, 0, 0, None
                else:
                    prior = self._claim(row)
                    if (
                        prior.schema_version != schema_version
                        or prior.generation != generation
                    ):
                        if prior.expires_at > now:
                            raise SharedStateConflict(
                                "active incompatible generation/schema"
                            )
                        state_version, last_sequence, digest = (
                            prior.state_version + 1,
                            0,
                            None,
                        )
                    elif prior.expires_at > now and prior.owner != owner:
                        raise SharedStateConflict(
                            "resource already has an active writer"
                        )
                    else:
                        state_version, last_sequence, digest = (
                            prior.state_version,
                            prior.last_sequence,
                            prior.payload_digest,
                        )
                    fencing = prior.fencing_token + 1
                self._connection.execute(
                    """INSERT INTO shared_state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(resource_type, scope_key) DO UPDATE SET
                    schema_version=excluded.schema_version,
                    generation=excluded.generation,
                    state_version=excluded.state_version, owner=excluded.owner,
                    fencing_token=excluded.fencing_token,
                    expires_at=excluded.expires_at,
                    last_sequence=excluded.last_sequence,
                    payload_digest=excluded.payload_digest,
                    updated_at=excluded.updated_at""",
                    (
                        resource_type,
                        scope_key,
                        schema_version,
                        generation,
                        state_version,
                        owner,
                        fencing,
                        expires_at.isoformat(),
                        last_sequence,
                        digest,
                        now.isoformat(),
                    ),
                )
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return SharedClaim(
            resource_type,
            scope_key,
            owner,
            schema_version,
            generation,
            state_version,
            fencing,
            expires_at,
            last_sequence,
            digest,
        )

    async def renew(self, claim: SharedClaim, ttl_seconds: float) -> SharedClaim:
        if ttl_seconds <= 0:
            raise ValueError("lease TTL must be positive")
        now = datetime.now(UTC)
        expires = now + timedelta(seconds=ttl_seconds)
        async with self._lock:
            current = self._require_current(claim, now)
            self._connection.execute(
                "UPDATE shared_state SET expires_at=?, updated_at=? "
                "WHERE resource_type=? AND scope_key=?",
                (
                    expires.isoformat(),
                    now.isoformat(),
                    claim.resource_type,
                    claim.scope_key,
                ),
            )
        return SharedClaim(
            current.resource_type,
            current.scope_key,
            current.owner,
            current.schema_version,
            current.generation,
            current.state_version,
            current.fencing_token,
            expires,
            current.last_sequence,
            current.payload_digest,
        )

    async def commit(
        self,
        claim: SharedClaim,
        *,
        expected_state_version: int,
        sequence: int,
        payload_digest: str,
    ) -> SharedClaim:
        now = datetime.now(UTC)
        async with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                current = self._require_current(claim, now)
                if expected_state_version != current.state_version:
                    raise SharedStateConflict("shared state version CAS failed")
                if sequence == current.last_sequence:
                    if payload_digest != current.payload_digest:
                        raise SharedStateConflict("duplicate sequence conflicts")
                    self._connection.execute("COMMIT")
                    return current
                if sequence != current.last_sequence + 1:
                    raise SharedStateConflict(
                        "shared state event is lost or out of order"
                    )
                version = current.state_version + 1
                self._connection.execute(
                    "UPDATE shared_state SET state_version=?, last_sequence=?, "
                    "payload_digest=?, updated_at=? "
                    "WHERE resource_type=? AND scope_key=?",
                    (
                        version,
                        sequence,
                        payload_digest,
                        now.isoformat(),
                        claim.resource_type,
                        claim.scope_key,
                    ),
                )
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return SharedClaim(
            current.resource_type,
            current.scope_key,
            current.owner,
            current.schema_version,
            current.generation,
            version,
            current.fencing_token,
            current.expires_at,
            sequence,
            payload_digest,
        )

    async def release(self, claim: SharedClaim) -> None:
        now = datetime.now(UTC)
        async with self._lock:
            current = self._require_current(claim, now)
            self._connection.execute(
                "UPDATE shared_state SET expires_at=?, updated_at=? "
                "WHERE resource_type=? AND scope_key=?",
                (
                    now.isoformat(),
                    now.isoformat(),
                    current.resource_type,
                    current.scope_key,
                ),
            )

    async def read(self, resource_type: str, scope_key: str) -> SharedClaim | None:
        async with self._lock:
            row = self._row(resource_type, scope_key)
            return self._claim(row) if row is not None else None

    def _require_current(self, claim: SharedClaim, now: datetime) -> SharedClaim:
        row = self._row(claim.resource_type, claim.scope_key)
        if row is None:
            raise SharedStateConflict("shared record is missing")
        current = self._claim(row)
        if current.expires_at <= now:
            raise SharedStateConflict("shared writer lease expired")
        if (
            current.owner,
            current.fencing_token,
            current.generation,
            current.schema_version,
        ) != (claim.owner, claim.fencing_token, claim.generation, claim.schema_version):
            raise SharedStateConflict("writer was fenced or is incompatible")
        return current

    def _row(self, resource_type: str, scope_key: str) -> sqlite3.Row | None:
        return self._connection.execute(
            "SELECT * FROM shared_state WHERE resource_type=? AND scope_key=?",
            (resource_type, scope_key),
        ).fetchone()

    @staticmethod
    def _claim(row: sqlite3.Row) -> SharedClaim:
        return SharedClaim(
            row["resource_type"],
            row["scope_key"],
            row["owner"],
            row["schema_version"],
            int(row["generation"]),
            int(row["state_version"]),
            int(row["fencing_token"]),
            datetime.fromisoformat(row["expires_at"]),
            int(row["last_sequence"]),
            row["payload_digest"],
        )

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM shared_state ORDER BY resource_type, scope_key"
            ).fetchall()
            return [dict(row) for row in rows]
