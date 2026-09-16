from __future__ import annotations

import asyncio
import sqlite3
import struct
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .contracts import ReuseConflict, canonical_json, digest

SCHEMA_ID = "flowpilot-trusted-reuse-v4"


class ReuseCache:
    """One payload per origin; publication receipts are the commit authority."""

    def __init__(self, path: Path, *, retry_window_seconds: int = 86400) -> None:
        self.path = path
        self.retry_window_seconds = retry_window_seconds
        self._lock = asyncio.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size:
            # Inspect before WAL or any writable connection can touch an old DB.
            with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as db:
                version = db.execute("PRAGMA user_version").fetchone()[0]
                tables = {
                    r[0]
                    for r in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if version != 4 or "reuse_schema" not in tables:
                    raise ReuseConflict(
                        "reuse schema mismatch; configure a new reuse-v4.sqlite path"
                    )
                if (
                    db.execute("SELECT schema_id FROM reuse_schema").fetchone()[0]
                    != SCHEMA_ID
                ):
                    raise ReuseConflict(
                        "reuse schema mismatch; configure a new cache path"
                    )
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS reuse_schema(schema_id TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS result_payloads(
                    origin_id TEXT PRIMARY KEY, result_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reuse_publications(
                    binding_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                    origin_id TEXT NOT NULL UNIQUE, identity_json TEXT NOT NULL,
                    context_json TEXT NOT NULL, descriptor_json TEXT NOT NULL,
                    registry_json TEXT NOT NULL, input_digest TEXT NOT NULL,
                    result_digest TEXT NOT NULL, result_size INTEGER NOT NULL,
                    cacheable INTEGER NOT NULL, observed_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL, committed_at TEXT NOT NULL,
                    retry_until TEXT NOT NULL, status TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reuse_entries(
                    origin_id TEXT PRIMARY KEY REFERENCES reuse_publications(origin_id),
                    exact_key TEXT NOT NULL, hard_scope_digest TEXT NOT NULL,
                    semantic_text TEXT, expires_at TEXT NOT NULL,
                    last_used_at TEXT NOT NULL, hit_count INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS reuse_exact
                    ON reuse_entries(exact_key, expires_at);
                CREATE INDEX IF NOT EXISTS reuse_scope
                    ON reuse_entries(hard_scope_digest, expires_at);
                CREATE TABLE IF NOT EXISTS origin_execution_refs(
                    origin_id TEXT PRIMARY KEY REFERENCES reuse_publications(origin_id),
                    execution_key TEXT NOT NULL UNIQUE, receipt_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS semantic_vectors(
                    origin_id TEXT PRIMARY KEY REFERENCES reuse_entries(origin_id)
                        ON DELETE CASCADE,
                    semantic_text_digest TEXT NOT NULL, index_id TEXT NOT NULL,
                    dimension INTEGER NOT NULL, pipeline_version TEXT NOT NULL,
                    normalization TEXT NOT NULL, embedding BLOB NOT NULL);
                CREATE TABLE IF NOT EXISTS reuse_match_audit(
                    audit_id TEXT PRIMARY KEY, correlation_digest TEXT NOT NULL,
                    source_id TEXT, tool_name TEXT NOT NULL, match_kind TEXT NOT NULL,
                    score REAL, threshold REAL, decision TEXT NOT NULL,
                    reason TEXT, index_id TEXT, observed_at TEXT NOT NULL,
                    feedback_reason TEXT, evidence_digest TEXT);
                PRAGMA user_version=4;
            """)
            db.execute("INSERT OR IGNORE INTO reuse_schema VALUES (?)", (SCHEMA_ID,))
        # Descriptors and optional semantic text are controlled data, not traces.
        self.path.chmod(0o600)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=5000")
        try:
            with db:
                yield db
        finally:
            db.close()

    async def receipt(self, binding_id: str) -> dict[str, Any] | None:
        async with self._lock:
            with self._connect() as db:
                row = db.execute(
                    "SELECT * FROM reuse_publications WHERE binding_id=?", (binding_id,)
                ).fetchone()
                return dict(row) if row else None

    async def commit(
        self,
        publication: dict[str, Any],
        result_json: str,
        execution: dict[str, Any],
        *,
        exact_key: str,
        hard_scope_digest: str,
        semantic_text: str | None,
    ) -> dict[str, Any]:
        async with self._lock:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                prior = db.execute(
                    "SELECT * FROM reuse_publications WHERE binding_id=?",
                    (publication["binding_id"],),
                ).fetchone()
                if prior:
                    if prior["fingerprint"] != publication["fingerprint"]:
                        raise ReuseConflict("publication fingerprint conflict")
                    return dict(prior)
                fields = tuple(publication)
                db.execute(
                    f"INSERT INTO reuse_publications ({','.join(fields)}) "
                    f"VALUES ({','.join('?' for _ in fields)})",
                    tuple(publication.values()),
                )
                origin = publication["origin_id"]
                db.execute(
                    "INSERT INTO result_payloads VALUES (?,?)", (origin, result_json)
                )
                db.execute(
                    "INSERT INTO origin_execution_refs VALUES (?,?,?)",
                    (origin, digest(execution), canonical_json(execution)),
                )
                if publication["cacheable"]:
                    db.execute(
                        "INSERT INTO reuse_entries VALUES (?,?,?,?,?,?,0)",
                        (
                            origin,
                            exact_key,
                            hard_scope_digest,
                            semantic_text,
                            publication["expires_at"],
                            publication["committed_at"],
                        ),
                    )
            return publication

    async def candidates(
        self, *, exact_key: str | None = None, hard_scope_digest: str | None = None
    ) -> list[dict[str, Any]]:
        column, value = (
            ("exact_key", exact_key)
            if exact_key is not None
            else ("hard_scope_digest", hard_scope_digest)
        )
        async with self._lock:
            with self._connect() as db:
                rows = db.execute(
                    f"""
                    SELECT p.*, e.exact_key, e.hard_scope_digest, e.semantic_text,
                        b.result_json, x.receipt_json,
                        v.index_id, v.dimension, v.embedding,
                        v.normalization, v.pipeline_version, v.semantic_text_digest
                    FROM reuse_entries e JOIN reuse_publications p USING(origin_id)
                    JOIN result_payloads b USING(origin_id)
                    JOIN origin_execution_refs x USING(origin_id)
                    LEFT JOIN semantic_vectors v USING(origin_id)
                    WHERE e.{column}=? AND p.status='committed' AND p.expires_at>?
                    ORDER BY p.observed_at DESC, p.origin_id
                """,
                    (value, datetime.now(UTC).isoformat()),
                ).fetchall()
                return [dict(r) for r in rows]

    async def result(self, origin_id: str) -> dict[str, Any] | None:
        async with self._lock:
            with self._connect() as db:
                row = db.execute(
                    """SELECT p.*, b.result_json, x.receipt_json
                    FROM reuse_publications p
                    JOIN result_payloads b USING(origin_id)
                    JOIN origin_execution_refs x USING(origin_id)
                    WHERE origin_id=? AND status='committed' AND expires_at>?""",
                    (origin_id, datetime.now(UTC).isoformat()),
                ).fetchone()
                return dict(row) if row else None

    async def touch(self, origin_id: str) -> None:
        async with self._lock:
            with self._connect() as db:
                db.execute(
                    "UPDATE reuse_entries SET hit_count=hit_count+1,last_used_at=? "
                    "WHERE origin_id=?",
                    (datetime.now(UTC).isoformat(), origin_id),
                )

    async def vector(
        self, origin_id: str, text: str, index_id: str, values: tuple[float, ...]
    ) -> None:
        blob = struct.pack(f"<{len(values)}f", *values)
        async with self._lock:
            with self._connect() as db:
                db.execute(
                    """INSERT OR REPLACE INTO semantic_vectors
                    SELECT origin_id,?,?,?,?,?,? FROM reuse_entries
                    WHERE origin_id=?""",
                    (
                        digest(text),
                        index_id,
                        len(values),
                        "web-query-equivalence-v1",
                        "L2",
                        blob,
                        origin_id,
                    ),
                )

    async def missing_vectors(self, index_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            with self._connect() as db:
                return [
                    dict(r)
                    for r in db.execute(
                        """SELECT e.origin_id,e.semantic_text
                    FROM reuse_entries e LEFT JOIN semantic_vectors v USING(origin_id)
                    WHERE e.semantic_text IS NOT NULL AND e.expires_at>?
                    AND (v.index_id IS NULL OR v.index_id<>?)""",
                        (datetime.now(UTC).isoformat(), index_id),
                    )
                ]

    async def audit(
        self,
        correlation: Any,
        *,
        source_id: str | None,
        tool_name: str,
        match_kind: str,
        score: float | None,
        threshold: float | None,
        decision: str,
        reason: str | None,
        index_id: str | None,
    ) -> str:
        identifier = "match-" + digest([correlation, source_id, match_kind, decision])
        async with self._lock:
            with self._connect() as db:
                db.execute(
                    "INSERT OR IGNORE INTO reuse_match_audit "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)",
                    (
                        identifier,
                        digest(correlation),
                        source_id,
                        tool_name,
                        match_kind,
                        score,
                        threshold,
                        decision,
                        reason,
                        index_id,
                        datetime.now(UTC).isoformat(),
                    ),
                )
        return identifier

    async def feedback(self, match_id: str, reason: str, evidence: str | None) -> bool:
        async with self._lock:
            with self._connect() as db:
                row = db.execute(
                    "SELECT * FROM reuse_match_audit WHERE audit_id=?", (match_id,)
                ).fetchone()
                if row is None:
                    raise ReuseConflict("unknown semantic match")
                if row["feedback_reason"] is not None:
                    if (row["feedback_reason"], row["evidence_digest"]) != (
                        reason,
                        evidence,
                    ):
                        raise ReuseConflict("conflicting false reuse feedback")
                    return True
                db.execute(
                    "UPDATE reuse_match_audit SET feedback_reason=?,evidence_digest=? "
                    "WHERE audit_id=?",
                    (reason, evidence, match_id),
                )
                return False

    @staticmethod
    def _delete(db: sqlite3.Connection, origin_id: str, status: str) -> None:
        db.execute("DELETE FROM reuse_entries WHERE origin_id=?", (origin_id,))
        db.execute("DELETE FROM result_payloads WHERE origin_id=?", (origin_id,))
        db.execute("DELETE FROM origin_execution_refs WHERE origin_id=?", (origin_id,))
        db.execute("DELETE FROM reuse_match_audit WHERE source_id=?", (origin_id,))
        db.execute(
            "UPDATE reuse_publications SET status=? WHERE origin_id=?",
            (status, origin_id),
        )

    async def revoke(self, origin_id: str, *, status: str = "revoked") -> None:
        async with self._lock:
            with self._connect() as db:
                self._delete(db, origin_id, status)

    async def maintenance(
        self, *, max_payload_bytes: int | None = None, index_id: str | None = None
    ) -> dict[str, int]:
        now = datetime.now(UTC).isoformat()
        async with self._lock:
            with self._connect() as db:
                expired = list(
                    db.execute(
                        "SELECT origin_id FROM reuse_publications "
                        "WHERE status='committed' AND expires_at<=?",
                        (now,),
                    )
                )
                for row in expired:
                    self._delete(db, row[0], "expired")
                vectors = 0
                if index_id:
                    vectors = db.execute(
                        "DELETE FROM semantic_vectors WHERE index_id<>?", (index_id,)
                    ).rowcount
                evicted = 0
                size = db.execute(
                    "SELECT COALESCE(SUM(result_size),0) FROM reuse_publications "
                    "WHERE status='committed'"
                ).fetchone()[0]
                if max_payload_bytes is not None and size > max_payload_bytes:
                    for row in db.execute(
                        """SELECT p.origin_id,p.result_size FROM reuse_entries e
                        JOIN reuse_publications p USING(origin_id)
                        ORDER BY e.last_used_at,e.origin_id"""
                    ).fetchall():
                        if size <= max_payload_bytes:
                            break
                        self._delete(db, row[0], "evicted")
                        size -= row[1]
                        evicted += 1
                # Tombstones prevent a retry from recreating deleted payloads.
                # Receipts persist at least through the configured retry window.
                db.execute(
                    "DELETE FROM reuse_publications WHERE status<>'committed' "
                    "AND retry_until<?",
                    (now,),
                )
                db.execute(
                    "DELETE FROM reuse_match_audit WHERE observed_at<?",
                    (
                        (
                            datetime.now(UTC)
                            - timedelta(seconds=self.retry_window_seconds)
                        ).isoformat(),
                    ),
                )
            with self._connect() as db:
                db.execute("PRAGMA wal_checkpoint(PASSIVE)")
            return {
                "expired_deleted": len(expired),
                "vectors_deleted": vectors,
                "capacity_evicted": evicted,
                "payload_bytes": size,
            }

    async def count(self) -> int:
        async with self._lock:
            with self._connect() as db:
                return int(
                    db.execute("SELECT COUNT(*) FROM reuse_entries").fetchone()[0]
                )

    async def audit_stats(self) -> dict[str, int]:
        async with self._lock:
            with self._connect() as db:
                row = db.execute(
                    "SELECT COUNT(*),COUNT(feedback_reason) FROM reuse_match_audit"
                ).fetchone()
                return {"semantic_matches": row[0], "false_reuse_reports": row[1]}
