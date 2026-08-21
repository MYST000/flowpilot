from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from flowpilot.protocol import (
    ContextDeltaAppend,
    ContextReconcileRequest,
    ContextSyncAck,
    ContextSyncBegin,
    DCSBarrierReason,
    DCSReference,
    DCSReuseKind,
    DCSState,
    DelegationPolicy,
    InternalContinuationRequest,
    RequestIdentity,
    ReuseDecisionKind,
    ReuseMatchKind,
    ReuseType,
    ToolReuseDecision,
    ToolReuseResolveRequest,
)


class DCSConflict(ValueError):
    """The caller attempted an invalid deferred-context transition."""


class DeferredContextManager:
    """Durable single-writer deferred-context WAL for Phase 2 exact reuse."""

    def __init__(self, path: Path, encryption_key: str | bytes) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        try:
            key = (
                encryption_key.encode()
                if isinstance(encryption_key, str)
                else encryption_key
            )
            self._fernet = Fernet(key)
            self._receipt_key = base64.urlsafe_b64decode(key)
        except (TypeError, ValueError) as exc:
            raise DCSConflict("invalid DCS encryption key") from exc
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA secure_delete = ON")
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 1, 2, 3}:
                raise DCSConflict(f"unsupported DCS WAL schema version {version}")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS dcs_lines (
                    tenant_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    line_id TEXT NOT NULL,
                    context_epoch INTEGER NOT NULL,
                    policy_version INTEGER NOT NULL,
                    policy_json TEXT NOT NULL,
                    lease_id TEXT,
                    lease_expires_at TEXT,
                    base_context_cursor TEXT NOT NULL,
                    base_context_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    last_seq INTEGER NOT NULL,
                    last_digest TEXT NOT NULL,
                    pending_count INTEGER NOT NULL,
                    pending_bytes INTEGER NOT NULL,
                    internal_continuations INTEGER NOT NULL,
                    barrier_reason TEXT,
                    barrier_json TEXT,
                    last_ack_first_seq INTEGER,
                    last_ack_last_seq INTEGER,
                    last_ack_digest TEXT,
                    last_ack_cursor TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, job_id, line_id)
                );
                CREATE TABLE IF NOT EXISTS dcs_messages (
                    tenant_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    line_id TEXT NOT NULL,
                    context_epoch INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    previous_digest TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    message_json TEXT NOT NULL,
                    message_bytes INTEGER NOT NULL,
                    parent_llm_call_id TEXT NOT NULL,
                    reuse_kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, job_id, line_id, context_epoch, seq)
                );
                CREATE INDEX IF NOT EXISTS dcs_messages_line_seq
                    ON dcs_messages (tenant_id, job_id, line_id, context_epoch, seq);
                CREATE TABLE IF NOT EXISTS dcs_resolutions (
                    receipt_hash TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    line_id TEXT NOT NULL,
                    context_epoch INTEGER NOT NULL,
                    lease_id TEXT NOT NULL,
                    base_context_cursor TEXT NOT NULL,
                    issued_delta_digest TEXT NOT NULL,
                    tail_request_id TEXT NOT NULL,
                    llm_call_id TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    tool_call_id TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    arguments_digest TEXT NOT NULL,
                    descriptor_digest TEXT NOT NULL,
                    reuse_kind TEXT NOT NULL,
                    result_digest TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    consumed_at TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dcs_acks (
                    tenant_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    line_id TEXT NOT NULL,
                    context_epoch INTEGER NOT NULL,
                    lease_id TEXT NOT NULL,
                    first_seq INTEGER NOT NULL,
                    last_seq INTEGER NOT NULL,
                    payload_digest TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (
                        tenant_id, job_id, line_id, context_epoch, lease_id,
                        first_seq, last_seq
                    )
                );
                """
            )
            if version == 1:
                columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(dcs_lines)")
                }
                if "barrier_json" not in columns:
                    connection.execute(
                        "ALTER TABLE dcs_lines ADD COLUMN barrier_json TEXT"
                    )
                for row in connection.execute(
                    "SELECT tenant_id, job_id, line_id, policy_json FROM dcs_lines"
                ).fetchall():
                    connection.execute(
                        "UPDATE dcs_lines SET policy_json=? "
                        "WHERE tenant_id=? AND job_id=? AND line_id=?",
                        (
                            self._encrypt_text(str(row["policy_json"])),
                            row["tenant_id"],
                            row["job_id"],
                            row["line_id"],
                        ),
                    )
                for row in connection.execute(
                    "SELECT tenant_id, job_id, line_id, context_epoch, seq, "
                    "message_json FROM dcs_messages"
                ).fetchall():
                    connection.execute(
                        "UPDATE dcs_messages SET message_json=? WHERE tenant_id=? "
                        "AND job_id=? AND line_id=? AND context_epoch=? AND seq=?",
                        (
                            self._encrypt_text(str(row["message_json"])),
                            row["tenant_id"],
                            row["job_id"],
                            row["line_id"],
                            row["context_epoch"],
                            row["seq"],
                        ),
                    )
                connection.execute("PRAGMA user_version = 2")
                connection.commit()
                connection.execute("VACUUM")
            if version in {0, 1, 2}:
                connection.execute("PRAGMA user_version = 3")

    async def grant(self, policy: DelegationPolicy) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._grant, policy)

    def _grant(self, policy: DelegationPolicy) -> dict[str, Any]:
        now = datetime.now(UTC)
        if policy.issued_at > now or policy.expires_at <= now:
            raise DCSConflict("delegation lease is not currently valid")
        _validate_snapshot(policy.api_kind, policy.request_snapshot)
        key = (policy.tenant_id, policy.job_id, policy.line_id)
        policy_json = _canonical_json(policy.model_dump(mode="json"))
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM dcs_lines WHERE tenant_id=? AND job_id=? AND line_id=?",
                key,
            ).fetchone()
            if row is not None and int(row["policy_version"]) == policy.policy_version:
                if self._decrypt_text(str(row["policy_json"])) != policy_json:
                    raise DCSConflict("policy version already has different content")
                return _line_snapshot(row)
            current_version = int(row["policy_version"]) if row is not None else 0
            if current_version != policy.expected_policy_version:
                raise DCSConflict(
                    f"expected policy version {current_version}, got "
                    f"{policy.expected_policy_version}"
                )
            if row is not None:
                if row["state"] in {DCSState.OPEN, DCSState.SYNCING} and int(
                    row["pending_count"]
                ):
                    raise DCSConflict("cannot replace delegation with pending context")
                connection.execute(
                    "DELETE FROM dcs_messages "
                    "WHERE tenant_id=? AND job_id=? AND line_id=?",
                    key,
                )
            connection.execute(
                """
                INSERT INTO dcs_lines (
                    tenant_id, job_id, line_id, context_epoch, policy_version,
                    policy_json, lease_id, lease_expires_at, base_context_cursor,
                    base_context_digest, state, last_seq, last_digest,
                    pending_count, pending_bytes, internal_continuations,
                    barrier_reason, barrier_json, last_ack_first_seq,
                    last_ack_last_seq, last_ack_digest, last_ack_cursor, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, 0, 0,
                    NULL, NULL, NULL, NULL, NULL, NULL, ?
                )
                ON CONFLICT(tenant_id, job_id, line_id) DO UPDATE SET
                    context_epoch=excluded.context_epoch,
                    policy_version=excluded.policy_version,
                    policy_json=excluded.policy_json,
                    lease_id=excluded.lease_id,
                    lease_expires_at=excluded.lease_expires_at,
                    base_context_cursor=excluded.base_context_cursor,
                    base_context_digest=excluded.base_context_digest,
                    state=excluded.state,
                    last_seq=0,
                    last_digest=excluded.last_digest,
                    pending_count=0,
                    pending_bytes=0,
                    internal_continuations=0,
                    barrier_reason=NULL,
                    barrier_json=NULL,
                    last_ack_first_seq=NULL,
                    last_ack_last_seq=NULL,
                    last_ack_digest=NULL,
                    last_ack_cursor=NULL,
                    updated_at=excluded.updated_at
                """,
                (
                    *key,
                    policy.context_epoch,
                    policy.policy_version,
                    self._encrypt_text(policy_json),
                    policy.lease_id,
                    policy.expires_at.isoformat(),
                    policy.base_context_cursor,
                    policy.base_context_digest,
                    DCSState.OPEN,
                    policy.base_context_digest,
                    now.isoformat(),
                ),
            )
            created = connection.execute(
                "SELECT * FROM dcs_lines WHERE tenant_id=? AND job_id=? AND line_id=?",
                key,
            ).fetchone()
            assert created is not None
            return _line_snapshot(created)

    async def authorize_reuse(
        self, reference: DCSReference, tool_name: str
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._authorize_reuse, reference, tool_name)

    async def validate_reference(self, reference: DCSReference) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._validate_reference, reference)

    async def release(self, reference: DCSReference) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._release, reference)

    def _validate_reference(self, reference: DCSReference) -> dict[str, Any]:
        with self._connect() as connection:
            row = self._require_reference(connection, reference, require_open=True)
            return _line_snapshot(row)

    def _release(self, reference: DCSReference) -> dict[str, Any]:
        with self._connect() as connection:
            row = self._require_reference(connection, reference, require_open=True)
            if int(row["pending_count"]):
                raise DCSConflict("delegation with pending context must synchronize")
            connection.execute(
                """
                UPDATE dcs_lines SET state=?, lease_id=NULL, lease_expires_at=NULL,
                    policy_json=?, barrier_reason=?, barrier_json=NULL, updated_at=?
                WHERE tenant_id=? AND job_id=? AND line_id=?
                """,
                (
                    DCSState.ABORTED,
                    self._encrypted_redacted_policy(row),
                    DCSBarrierReason.FAILURE,
                    datetime.now(UTC).isoformat(),
                    reference.tenant_id,
                    reference.job_id,
                    reference.line_id,
                ),
            )
            updated = self._get_line(connection, reference)
            return _line_snapshot(updated)

    async def authorize_llm_request(self, identity: RequestIdentity) -> None:
        async with self._lock:
            await asyncio.to_thread(self._authorize_llm_request, identity)

    def _authorize_llm_request(self, identity: RequestIdentity) -> None:
        key = (identity.tenant_id, identity.job_id, identity.line_id)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM dcs_lines WHERE tenant_id=? AND job_id=? AND line_id=?",
                key,
            ).fetchone()
            if row is None:
                if identity.origin == "scheduler_delegated":
                    raise DCSConflict("delegated LLM request has no active policy")
                return
            pending = int(row["pending_count"])
            if identity.origin == "agent":
                if row["lease_id"] is not None and row["state"] in {
                    DCSState.OPEN,
                    DCSState.SYNCING,
                }:
                    raise DCSConflict(
                        "Agent LLM request conflicts with the active DCS writer"
                    )
                return
            if (
                row["state"] != DCSState.OPEN
                or not pending
                or row["lease_id"] != identity.delegation_lease_id
                or int(row["context_epoch"]) != identity.context_epoch
                or row["base_context_cursor"] != identity.base_context_cursor
                or row["last_digest"] != identity.context_digest
            ):
                raise DCSConflict(
                    "delegated LLM request does not match the active DCS writer"
                )
            deadline = row["lease_expires_at"]
            if deadline is None or datetime.fromisoformat(deadline) <= datetime.now(
                UTC
            ):
                raise DCSConflict("delegated LLM request lease expired")

    def _authorize_reuse(
        self, reference: DCSReference, tool_name: str
    ) -> dict[str, Any]:
        with self._connect() as connection:
            row = self._require_reference(connection, reference, require_open=True)
            policy = self._policy(row)
            if tool_name not in policy["allowed_tool_names"]:
                raise DCSConflict("tool is outside the delegation policy")
            return _line_snapshot(row)

    async def issue_resolution(
        self,
        reference: DCSReference,
        reuse: ToolReuseResolveRequest,
        decision: ToolReuseDecision,
    ) -> dict[str, str]:
        async with self._lock:
            return await asyncio.to_thread(
                self._issue_resolution, reference, reuse, decision
            )

    def _issue_resolution(
        self,
        reference: DCSReference,
        reuse: ToolReuseResolveRequest,
        decision: ToolReuseDecision,
    ) -> dict[str, str]:
        if decision.decision != ReuseDecisionKind.DEFER_WITH_CACHED_RESULT:
            raise DCSConflict("only a completed deferred reuse result gets a receipt")
        if (
            decision.result is None
            or decision.provenance is None
            or decision.descriptor_digest is None
        ):
            raise DCSConflict("deferred result is missing validated provenance")
        identity = reuse.identity
        if (identity.tenant_id, identity.job_id, identity.line_id) != (
            reference.tenant_id,
            reference.job_id,
            reference.line_id,
        ):
            raise DCSConflict("reuse identity does not match delegation")
        if decision.provenance.match_kind == ReuseMatchKind.SEMANTIC:
            reuse_kind = (
                DCSReuseKind.SEMANTIC_HISTORICAL
                if decision.provenance.reuse_type == ReuseType.HISTORICAL
                else DCSReuseKind.SEMANTIC_INFLIGHT
            )
        else:
            reuse_kind = (
                DCSReuseKind.EXACT_HISTORICAL
                if decision.provenance.reuse_type == ReuseType.HISTORICAL
                else DCSReuseKind.EXACT_INFLIGHT
            )
        provider_content = _provider_reuse_content(decision)
        result_digest = _result_payload_digest(provider_content)
        arguments_digest = hashlib.sha256(
            _canonical_json(reuse.arguments).encode()
        ).hexdigest()
        with self._connect() as connection:
            row = self._require_reference(connection, reference, require_open=True)
            policy = self._policy(row)
            if reuse.tool_name not in policy["allowed_tool_names"]:
                raise DCSConflict("tool is outside the delegation policy")
            claims = {
                "tenant_id": reference.tenant_id,
                "job_id": reference.job_id,
                "line_id": reference.line_id,
                "context_epoch": reference.context_epoch,
                "lease_id": reference.lease_id,
                "base_context_cursor": reference.base_context_cursor,
                "issued_delta_digest": reference.delta_digest,
                "tail_request_id": identity.tail_request_id,
                "llm_call_id": identity.llm_call_id,
                "action_id": identity.action_id,
                "tool_call_id": identity.tool_call_id,
                "tool_name": reuse.tool_name,
                "arguments_digest": arguments_digest,
                "descriptor_digest": decision.descriptor_digest,
                "reuse_kind": reuse_kind.value,
                "result_digest": result_digest,
                "expires_at": str(row["lease_expires_at"]),
            }
            token = self._receipt_token(claims)
            receipt_hash = hashlib.sha256(token.encode()).hexdigest()
            connection.execute(
                """
                INSERT OR IGNORE INTO dcs_resolutions VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    receipt_hash,
                    reference.tenant_id,
                    reference.job_id,
                    reference.line_id,
                    reference.context_epoch,
                    reference.lease_id,
                    reference.base_context_cursor,
                    reference.delta_digest,
                    identity.tail_request_id,
                    identity.llm_call_id,
                    identity.action_id,
                    identity.tool_call_id,
                    reuse.tool_name,
                    arguments_digest,
                    decision.descriptor_digest,
                    reuse_kind.value,
                    result_digest,
                    row["lease_expires_at"],
                    None,
                    datetime.now(UTC).isoformat(),
                ),
            )
        return {
            "resolution_receipt": token,
            "result_digest": result_digest,
            "reuse_kind": reuse_kind.value,
            "provider_content": provider_content,
        }

    async def append(self, append: ContextDeltaAppend) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._append, append)

    def _append(self, append: ContextDeltaAppend) -> dict[str, Any]:
        facts = _provider_batch_facts(append.messages, append.tool_call_ids)
        now = datetime.now(UTC)
        with self._connect() as connection:
            row = self._require_reference(
                connection, append.reference, require_open=True
            )
            if int(row["last_seq"]) != append.expected_last_seq:
                raise DCSConflict(
                    f"expected delta seq {row['last_seq']}, got "
                    f"{append.expected_last_seq}"
                )
            policy = self._policy(row)
            resolutions = []
            for token in append.resolution_receipts:
                receipt_hash = hashlib.sha256(token.encode()).hexdigest()
                resolution = connection.execute(
                    "SELECT * FROM dcs_resolutions WHERE receipt_hash=?",
                    (receipt_hash,),
                ).fetchone()
                if resolution is None:
                    raise DCSConflict("unknown resolution receipt")
                resolutions.append(resolution)
            reuse_kinds: list[str] = []
            for index, (resolution, fact) in enumerate(
                zip(resolutions, facts, strict=True)
            ):
                expected = (
                    append.reference.tenant_id,
                    append.reference.job_id,
                    append.reference.line_id,
                    append.reference.context_epoch,
                    append.reference.lease_id,
                    append.reference.base_context_cursor,
                    append.reference.delta_digest,
                    append.parent_llm_call_id,
                    append.tool_call_ids[index],
                    append.result_digests[index],
                )
                observed = (
                    resolution["tenant_id"],
                    resolution["job_id"],
                    resolution["line_id"],
                    resolution["context_epoch"],
                    resolution["lease_id"],
                    resolution["base_context_cursor"],
                    resolution["issued_delta_digest"],
                    resolution["llm_call_id"],
                    resolution["tool_call_id"],
                    resolution["result_digest"],
                )
                if observed != expected:
                    raise DCSConflict("resolution receipt does not bind this append")
                if resolution["consumed_at"] is not None:
                    raise DCSConflict("resolution receipt was already consumed")
                if datetime.fromisoformat(resolution["expires_at"]) <= now:
                    raise DCSConflict("resolution receipt expired")
                if (
                    fact["tool_name"] != resolution["tool_name"]
                    or fact["arguments_digest"] != resolution["arguments_digest"]
                    or fact["result_digest"] != resolution["result_digest"]
                ):
                    raise DCSConflict(
                        "provider messages conflict with exact resolution receipt"
                    )
                reuse_kinds.append(str(resolution["reuse_kind"]))
            start_seq = int(row["last_seq"]) + 1
            previous = str(row["last_digest"])
            encoded: list[tuple[int, str, str, str, int]] = []
            for offset, message in enumerate(append.messages):
                canonical = _canonical_json(message)
                digest = _chained_digest(previous, canonical)
                encoded.append(
                    (
                        start_seq + offset,
                        previous,
                        digest,
                        canonical,
                        len(canonical.encode()),
                    )
                )
                previous = digest
            batch_bytes = sum(item[4] for item in encoded)
            if batch_bytes > int(policy["max_bytes"]):
                raise DCSConflict("one provider message batch exceeds delta byte limit")
            for seq, prior, digest, canonical, size in encoded:
                connection.execute(
                    """
                    INSERT INTO dcs_messages VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        append.reference.tenant_id,
                        append.reference.job_id,
                        append.reference.line_id,
                        append.reference.context_epoch,
                        seq,
                        prior,
                        digest,
                        self._encrypt_text(canonical),
                        size,
                        append.parent_llm_call_id,
                        reuse_kinds[0]
                        if len(set(reuse_kinds)) == 1
                        else "mixed_reuse",
                        now.isoformat(),
                    ),
                )
            pending_count = int(row["pending_count"]) + len(encoded)
            pending_bytes = int(row["pending_bytes"]) + batch_bytes
            state = DCSState.OPEN
            barrier: str | None = None
            if pending_count >= int(policy["max_messages"]) or pending_bytes >= int(
                policy["max_bytes"]
            ):
                state = DCSState.SYNCING
                barrier = DCSBarrierReason.CAPACITY
            stored_policy = (
                str(row["policy_json"])
                if state == DCSState.OPEN
                else self._encrypted_redacted_policy(row)
            )
            connection.execute(
                """
                UPDATE dcs_lines SET last_seq=?, last_digest=?, pending_count=?,
                    pending_bytes=?, state=?, barrier_reason=?, policy_json=?,
                    updated_at=?
                WHERE tenant_id=? AND job_id=? AND line_id=?
                """,
                (
                    encoded[-1][0],
                    encoded[-1][2],
                    pending_count,
                    pending_bytes,
                    state,
                    barrier,
                    stored_policy,
                    now.isoformat(),
                    append.reference.tenant_id,
                    append.reference.job_id,
                    append.reference.line_id,
                ),
            )
            connection.executemany(
                "UPDATE dcs_resolutions SET consumed_at=? WHERE receipt_hash=?",
                (
                    (
                        now.isoformat(),
                        hashlib.sha256(token.encode()).hexdigest(),
                    )
                    for token in append.resolution_receipts
                ),
            )
            updated = self._get_line(connection, append.reference)
            return {**_line_snapshot(updated), "reuse_kinds": reuse_kinds}

    async def begin_sync(self, request: ContextSyncBegin) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._begin_sync, request)

    def _begin_sync(self, request: ContextSyncBegin) -> dict[str, Any]:
        _validate_barrier(request)
        with self._connect() as connection:
            row = self._get_line(connection, request.reference)
            if int(row["context_epoch"]) != request.reference.context_epoch:
                raise DCSConflict("context epoch does not match delegation")
            if row["lease_id"] != request.reference.lease_id:
                raise DCSConflict("delegation lease is not the active writer")
            if row["base_context_cursor"] != request.reference.base_context_cursor:
                raise DCSConflict("base context cursor conflicts with WAL")
            if row["state"] == DCSState.DIVERGED:
                raise DCSConflict("context is diverged")
            envelope = {
                "begin_delta_digest": request.reference.delta_digest,
                "parent_llm_call_id": request.parent_llm_call_id,
                "pending_local_tool_call_ids": list(
                    request.pending_local_tool_call_ids
                ),
                "barrier_message_count": len(request.barrier_messages),
                "barrier_messages_digest": hashlib.sha256(
                    _canonical_json(list(request.barrier_messages)).encode()
                ).hexdigest(),
            }
            if row["state"] == DCSState.SYNCING:
                stored = self._barrier(row)
                if request.reference.delta_digest not in {
                    row["last_digest"],
                    stored["begin_delta_digest"],
                }:
                    raise DCSConflict("delta digest conflicts with WAL")
                if (
                    row["barrier_reason"] != request.barrier_reason
                    or stored != envelope
                ):
                    raise DCSConflict(
                        "sync barrier conflicts with active synchronization"
                    )
                return self._sync_chunk(
                    connection, request.reference, request.max_messages
                )
            if row["last_digest"] != request.reference.delta_digest:
                raise DCSConflict("delta digest conflicts with WAL")
            if row["state"] != DCSState.OPEN:
                raise DCSConflict(f"deferred context is {row['state']}")
            if int(row["pending_count"]) == 0 and not request.barrier_messages:
                raise DCSConflict("no pending context to synchronize")
            last_seq = int(row["last_seq"])
            last_digest = str(row["last_digest"])
            added_bytes = 0
            now = datetime.now(UTC)
            for message in request.barrier_messages:
                canonical = _canonical_json(message)
                digest = _chained_digest(last_digest, canonical)
                last_seq += 1
                connection.execute(
                    """
                    INSERT INTO dcs_messages VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        request.reference.tenant_id,
                        request.reference.job_id,
                        request.reference.line_id,
                        request.reference.context_epoch,
                        last_seq,
                        last_digest,
                        digest,
                        self._encrypt_text(canonical),
                        len(canonical.encode()),
                        request.parent_llm_call_id or "barrier",
                        "barrier",
                        now.isoformat(),
                    ),
                )
                last_digest = digest
                added_bytes += len(canonical.encode())
            connection.execute(
                """
                UPDATE dcs_lines SET state=?, barrier_reason=?, barrier_json=?,
                    policy_json=?, last_seq=?, last_digest=?,
                    pending_count=pending_count+?, pending_bytes=pending_bytes+?,
                    updated_at=?
                WHERE tenant_id=? AND job_id=? AND line_id=?
                """,
                (
                    DCSState.SYNCING,
                    request.barrier_reason,
                    self._encrypt_text(_canonical_json(envelope)),
                    self._encrypted_redacted_policy(row),
                    last_seq,
                    last_digest,
                    len(request.barrier_messages),
                    added_bytes,
                    now.isoformat(),
                    request.reference.tenant_id,
                    request.reference.job_id,
                    request.reference.line_id,
                ),
            )
            return self._sync_chunk(connection, request.reference, request.max_messages)

    async def next_sync_chunk(
        self, reference: DCSReference, max_messages: int | None = None
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(
                self._next_sync_chunk, reference, max_messages
            )

    def _next_sync_chunk(
        self, reference: DCSReference, max_messages: int | None
    ) -> dict[str, Any]:
        with self._connect() as connection:
            row = self._require_reference(connection, reference)
            if row["state"] != DCSState.SYNCING:
                raise DCSConflict("context is not synchronizing")
            return self._sync_chunk(connection, reference, max_messages)

    def _sync_chunk(
        self,
        connection: sqlite3.Connection,
        reference: DCSReference,
        max_messages: int | None,
    ) -> dict[str, Any]:
        line = self._get_line(connection, reference)
        policy = self._policy(line)
        barrier = self._barrier(line)
        limit = max_messages or int(policy["max_messages"])
        messages = connection.execute(
            """
            SELECT * FROM dcs_messages
            WHERE tenant_id=? AND job_id=? AND line_id=? AND context_epoch=?
            ORDER BY seq LIMIT ?
            """,
            (*_reference_key(reference), limit),
        ).fetchall()
        if not messages:
            raise DCSConflict("no pending context to synchronize")
        last_parent = messages[-1]["parent_llm_call_id"]
        batch_tail = connection.execute(
            """
            SELECT * FROM dcs_messages
            WHERE tenant_id=? AND job_id=? AND line_id=? AND context_epoch=?
                AND seq>? AND parent_llm_call_id=?
            ORDER BY seq
            """,
            (
                *_reference_key(reference),
                messages[-1]["seq"],
                last_parent,
            ),
        ).fetchall()
        messages.extend(batch_tail)
        return {
            "protocol_version": "flowpilot-phase2-dcs-v1",
            "tenant_id": reference.tenant_id,
            "job_id": reference.job_id,
            "line_id": reference.line_id,
            "context_epoch": reference.context_epoch,
            "base_context_cursor": line["base_context_cursor"],
            "base_context_digest": line["base_context_digest"],
            "first_seq": messages[0]["seq"],
            "last_seq": messages[-1]["seq"],
            "delta_digest": messages[-1]["digest"],
            "wal_delta_digest": line["last_digest"],
            "messages": [
                json.loads(self._decrypt_text(str(item["message_json"])))
                for item in messages
            ],
            "barrier_reason": line["barrier_reason"],
            "parent_llm_call_id": barrier.get("parent_llm_call_id"),
            "pending_local_tool_call_ids": barrier.get(
                "pending_local_tool_call_ids", []
            ),
            "more": len(messages) < int(line["pending_count"]),
        }

    async def acknowledge(self, ack: ContextSyncAck) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._acknowledge, ack)

    def _acknowledge(self, ack: ContextSyncAck) -> dict[str, Any]:
        key = _reference_key(ack.reference)[:3]
        ack_key = (
            *_reference_key(ack.reference),
            ack.reference.lease_id,
            ack.first_seq,
            ack.last_seq,
        )
        payload_digest = hashlib.sha256(
            _canonical_json(ack.model_dump(mode="json")).encode()
        ).hexdigest()
        conflict: str | None = None
        result: dict[str, Any] | None = None
        with self._connect() as connection:
            row = self._get_line(connection, ack.reference)
            recorded = connection.execute(
                """
                SELECT payload_digest FROM dcs_acks
                WHERE tenant_id=? AND job_id=? AND line_id=? AND context_epoch=?
                    AND lease_id=? AND first_seq=? AND last_seq=?
                """,
                ack_key,
            ).fetchone()
            if recorded is not None:
                if recorded["payload_digest"] != payload_digest:
                    conflict = "ACK range was already committed with different content"
                else:
                    result = {**_line_snapshot(row), "duplicate": True}
            elif (
                int(row["context_epoch"]) != ack.reference.context_epoch
                or row["lease_id"] != ack.reference.lease_id
                or row["base_context_cursor"] != ack.reference.base_context_cursor
            ):
                conflict = "ACK reference conflicts with active synchronization"
            elif row["state"] != DCSState.SYNCING:
                conflict = "context is not synchronizing"
            elif ack.reference.delta_digest not in {
                row["last_digest"],
                self._barrier(row)["begin_delta_digest"],
            }:
                conflict = "ACK reference conflicts with active synchronization"
            else:
                messages = connection.execute(
                    """
                    SELECT * FROM dcs_messages
                    WHERE tenant_id=? AND job_id=? AND line_id=? AND context_epoch=?
                    ORDER BY seq
                    """,
                    _reference_key(ack.reference),
                ).fetchall()
                chunk = [
                    item
                    for item in messages
                    if ack.first_seq <= int(item["seq"]) <= ack.last_seq
                ]
                if (
                    not messages
                    or ack.first_seq != int(messages[0]["seq"])
                    or len(chunk) != ack.last_seq - ack.first_seq + 1
                    or chunk[-1]["digest"] != ack.delta_digest
                ):
                    conflict = "ACK range or digest conflicts with pending WAL"
                else:
                    removed_bytes = sum(int(item["message_bytes"]) for item in chunk)
                    connection.execute(
                        """
                        DELETE FROM dcs_messages WHERE tenant_id=? AND job_id=?
                            AND line_id=? AND context_epoch=? AND seq BETWEEN ? AND ?
                        """,
                        (*_reference_key(ack.reference), ack.first_seq, ack.last_seq),
                    )
                    remaining = int(row["pending_count"]) - len(chunk)
                    state = DCSState.SYNCING if remaining else DCSState.ACKED
                    connection.execute(
                        """
                        UPDATE dcs_lines SET base_context_cursor=?,
                            base_context_digest=?, pending_count=?, pending_bytes=?,
                            state=?, lease_id=CASE WHEN ?=0 THEN NULL ELSE lease_id END,
                            lease_expires_at=CASE
                                WHEN ?=0 THEN NULL ELSE lease_expires_at END,
                            barrier_json=CASE
                                WHEN ?=0 THEN NULL ELSE barrier_json END,
                            last_ack_first_seq=?, last_ack_last_seq=?,
                            last_ack_digest=?, last_ack_cursor=?, updated_at=?
                        WHERE tenant_id=? AND job_id=? AND line_id=?
                        """,
                        (
                            ack.new_context_cursor,
                            ack.new_context_digest,
                            remaining,
                            int(row["pending_bytes"]) - removed_bytes,
                            state,
                            remaining,
                            remaining,
                            remaining,
                            ack.first_seq,
                            ack.last_seq,
                            ack.delta_digest,
                            ack.new_context_cursor,
                            datetime.now(UTC).isoformat(),
                            *key,
                        ),
                    )
                    if remaining == 0:
                        connection.execute(
                            "DELETE FROM dcs_resolutions WHERE tenant_id=? "
                            "AND job_id=? "
                            "AND line_id=? AND context_epoch=?",
                            _reference_key(ack.reference),
                        )
                    connection.execute(
                        "INSERT INTO dcs_acks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (*ack_key, payload_digest, datetime.now(UTC).isoformat()),
                    )
                    connection.execute(
                        """
                        DELETE FROM dcs_acks
                        WHERE rowid IN (
                            SELECT rowid FROM dcs_acks
                            WHERE tenant_id=? AND job_id=? AND line_id=?
                            ORDER BY created_at DESC, rowid DESC LIMIT -1 OFFSET 128
                        )
                        """,
                        key,
                    )
                    updated = self._get_line(connection, ack.reference)
                    result = {**_line_snapshot(updated), "duplicate": False}
        if conflict is not None:
            self._mark_diverged(key)
            raise DCSConflict(conflict)
        assert result is not None
        return result

    async def prepare_continuation(
        self, request: InternalContinuationRequest
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._prepare_continuation, request)

    def _prepare_continuation(
        self, request: InternalContinuationRequest
    ) -> dict[str, Any]:
        with self._connect() as connection:
            row = self._require_reference(
                connection, request.reference, require_open=True
            )
            if int(row["pending_count"]) == 0:
                raise DCSConflict("continuation requires pending context")
            policy = self._policy(row)
            count = int(row["internal_continuations"])
            if count >= int(policy["max_internal_continuations"]):
                connection.execute(
                    "UPDATE dcs_lines SET state=?, barrier_reason=?, policy_json=? "
                    "WHERE tenant_id=? AND job_id=? AND line_id=?",
                    (
                        DCSState.SYNCING,
                        DCSBarrierReason.CAPACITY,
                        self._encrypted_redacted_policy(row),
                        request.reference.tenant_id,
                        request.reference.job_id,
                        request.reference.line_id,
                    ),
                )
                connection.commit()
                raise DCSConflict("internal continuation limit reached; sync required")
            messages = connection.execute(
                """
                SELECT message_json FROM dcs_messages
                WHERE tenant_id=? AND job_id=? AND line_id=? AND context_epoch=?
                ORDER BY seq
                """,
                _reference_key(request.reference),
            ).fetchall()
            delta = [
                json.loads(self._decrypt_text(str(item["message_json"])))
                for item in messages
            ]
            body = json.loads(_canonical_json(policy["request_snapshot"]))
            if policy["api_kind"] == "chat":
                body["messages"] = [*body["messages"], *delta]
            else:
                body["input"] = [*body["input"], *delta]
            connection.execute(
                """
                UPDATE dcs_lines SET internal_continuations=?, updated_at=?
                WHERE tenant_id=? AND job_id=? AND line_id=?
                """,
                (
                    count + 1,
                    datetime.now(UTC).isoformat(),
                    request.reference.tenant_id,
                    request.reference.job_id,
                    request.reference.line_id,
                ),
            )
            return {
                "protocol_version": "flowpilot-phase2-dcs-v1",
                "origin": "scheduler_delegated",
                "parent_llm_call_id": request.parent_llm_call_id,
                "context_epoch": row["context_epoch"],
                "base_context_cursor": row["base_context_cursor"],
                "delta_seq": row["last_seq"],
                "delta_digest": row["last_digest"],
                "api_kind": policy["api_kind"],
                "body": body,
            }

    async def reconcile(self, request: ContextReconcileRequest) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._reconcile, request)

    def _reconcile(self, request: ContextReconcileRequest) -> dict[str, Any]:
        key = (request.tenant_id, request.job_id, request.line_id)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM dcs_lines WHERE tenant_id=? AND job_id=? AND line_id=?",
                key,
            ).fetchone()
            if row is None:
                return {"status": "unknown_line", "sync_required": False}
            if request.context_epoch > int(row["context_epoch"]):
                if int(row["pending_count"]):
                    self._mark_diverged(key)
                    return {"status": "context_diverged", "sync_required": False}
                return {
                    "status": "agent_ahead_requires_new_delegation",
                    "sync_required": False,
                }
            if request.context_epoch < int(row["context_epoch"]):
                return {"status": "agent_stale", "sync_required": True}
            if (
                request.context_cursor == row["base_context_cursor"]
                and request.context_digest == row["base_context_digest"]
            ):
                return {
                    "status": "sync_required" if row["pending_count"] else "in_sync",
                    "sync_required": bool(row["pending_count"]),
                    **_line_snapshot(row),
                }
            if not int(row["pending_count"]) and row["state"] in {
                DCSState.ACKED,
                DCSState.ABORTED,
            }:
                return {
                    "status": "agent_ahead_requires_new_delegation",
                    "sync_required": False,
                }
        self._mark_diverged(key)
        return {"status": "context_diverged", "sync_required": False}

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._snapshot)

    def _snapshot(self) -> dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM dcs_lines ORDER BY tenant_id, job_id, line_id"
            ).fetchall()
            return {
                "wal_schema_version": 3,
                "lines": [_line_snapshot(row) for row in rows],
            }

    def _require_reference(
        self,
        connection: sqlite3.Connection,
        reference: DCSReference,
        *,
        require_open: bool = False,
    ) -> sqlite3.Row:
        row = self._get_line(connection, reference)
        if int(row["context_epoch"]) != reference.context_epoch:
            raise DCSConflict("context epoch does not match delegation")
        if row["lease_id"] != reference.lease_id:
            raise DCSConflict("delegation lease is not the active writer")
        if row["base_context_cursor"] != reference.base_context_cursor:
            raise DCSConflict("base context cursor conflicts with WAL")
        if row["last_digest"] != reference.delta_digest:
            raise DCSConflict("delta digest conflicts with WAL")
        if require_open and row["state"] != DCSState.OPEN:
            raise DCSConflict(f"deferred context is {row['state']}")
        if row["state"] == DCSState.OPEN:
            deadline = row["lease_expires_at"]
            policy = self._policy(row)
            oldest = None
            if int(row["pending_count"]):
                oldest = connection.execute(
                    """
                    SELECT created_at FROM dcs_messages
                    WHERE tenant_id=? AND job_id=? AND line_id=? AND context_epoch=?
                    ORDER BY seq LIMIT 1
                    """,
                    _reference_key(reference),
                ).fetchone()
            if oldest is not None and (
                datetime.fromisoformat(oldest["created_at"])
                + timedelta(seconds=float(policy["delta_ttl_seconds"]))
                <= datetime.now(UTC)
            ):
                connection.execute(
                    "UPDATE dcs_lines SET state=?, barrier_reason=?, policy_json=? "
                    "WHERE tenant_id=? AND job_id=? AND line_id=?",
                    (
                        DCSState.SYNCING,
                        DCSBarrierReason.TTL,
                        self._encrypted_redacted_policy(row),
                        reference.tenant_id,
                        reference.job_id,
                        reference.line_id,
                    ),
                )
                connection.commit()
                raise DCSConflict("pending context TTL expired; sync required")
            if deadline and datetime.fromisoformat(deadline) <= datetime.now(UTC):
                state = (
                    DCSState.SYNCING if int(row["pending_count"]) else DCSState.ABORTED
                )
                connection.execute(
                    "UPDATE dcs_lines SET state=?, barrier_reason=?, policy_json=? "
                    "WHERE tenant_id=? AND job_id=? AND line_id=?",
                    (
                        state,
                        DCSBarrierReason.LEASE_EXPIRED,
                        self._encrypted_redacted_policy(row),
                        reference.tenant_id,
                        reference.job_id,
                        reference.line_id,
                    ),
                )
                connection.commit()
                raise DCSConflict("delegation lease expired; sync required")
        return row

    def _encrypt_text(self, value: str) -> str:
        return self._fernet.encrypt(value.encode()).decode()

    def _decrypt_text(self, value: str) -> str:
        try:
            return self._fernet.decrypt(value.encode()).decode()
        except (InvalidToken, UnicodeDecodeError) as exc:
            raise DCSConflict(
                "DCS WAL encryption key does not match stored data"
            ) from exc

    def _policy(self, row: sqlite3.Row) -> dict[str, Any]:
        value = json.loads(self._decrypt_text(str(row["policy_json"])))
        if not isinstance(value, dict):
            raise DCSConflict("stored delegation policy is invalid")
        return value

    def _encrypted_redacted_policy(self, row: sqlite3.Row) -> str:
        policy_json = self._decrypt_text(str(row["policy_json"]))
        return self._encrypt_text(_redact_request_snapshot(policy_json))

    def _barrier(self, row: sqlite3.Row) -> dict[str, Any]:
        value = row["barrier_json"]
        if value is None:
            return {
                "begin_delta_digest": row["last_digest"],
                "parent_llm_call_id": None,
                "pending_local_tool_call_ids": [],
                "barrier_message_count": 0,
                "barrier_messages_digest": hashlib.sha256(b"[]").hexdigest(),
            }
        decoded = json.loads(self._decrypt_text(str(value)))
        if not isinstance(decoded, dict):
            raise DCSConflict("stored barrier envelope is invalid")
        return decoded

    def _receipt_token(self, claims: dict[str, Any]) -> str:
        signature = hmac.new(
            self._receipt_key,
            _canonical_json(claims).encode(),
            hashlib.sha256,
        ).digest()
        return base64.urlsafe_b64encode(signature).decode().rstrip("=")

    @staticmethod
    def _get_line(
        connection: sqlite3.Connection, reference: DCSReference
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM dcs_lines WHERE tenant_id=? AND job_id=? AND line_id=?",
            _reference_key(reference)[:3],
        ).fetchone()
        if row is None:
            raise DCSConflict("unknown deferred-context line")
        return row

    def _mark_diverged(self, key: tuple[str, str, str]) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE dcs_lines SET state=?, lease_id=NULL, lease_expires_at=NULL, "
                "updated_at=? WHERE tenant_id=? AND job_id=? AND line_id=?",
                (DCSState.DIVERGED, datetime.now(UTC).isoformat(), *key),
            )


def _validate_snapshot(api_kind: str, snapshot: dict[str, Any]) -> None:
    field = "messages" if api_kind == "chat" else "input"
    if not isinstance(snapshot.get(field), list):
        raise DCSConflict(f"delegated {api_kind} snapshot requires a {field} array")
    if snapshot.get("stream") is True:
        raise DCSConflict(
            "Phase 2 internal continuations require non-streaming snapshots"
        )


def _validate_provider_batch(
    messages: tuple[dict[str, Any], ...], tool_call_ids: tuple[str, ...]
) -> None:
    _provider_batch_facts(messages, tool_call_ids)


def _provider_batch_facts(
    messages: tuple[dict[str, Any], ...], tool_call_ids: tuple[str, ...]
) -> list[dict[str, str]]:
    first = messages[0]
    if first.get("role") == "assistant":
        calls = first.get("tool_calls")
        if not isinstance(calls, list):
            raise DCSConflict("assistant delta must contain complete tool_calls")
        observed = [item.get("id") for item in calls if isinstance(item, dict)]
        results = [item.get("tool_call_id") for item in messages[1:]]
        if any(item.get("role") != "tool" for item in messages[1:]):
            raise DCSConflict("assistant delta must be followed only by tool messages")
        call_facts = []
        for item in calls:
            if not isinstance(item, dict) or not isinstance(item.get("function"), dict):
                raise DCSConflict("assistant Tool Call is incomplete")
            function = item["function"]
            name = function.get("name")
            arguments = function.get("arguments")
            if not isinstance(name, str) or not isinstance(arguments, str):
                raise DCSConflict("assistant Tool Call name/arguments are incomplete")
            call_facts.append((name, _json_payload_digest(arguments)))
        result_digests = [
            _result_payload_digest(item.get("content")) for item in messages[1:]
        ]
    else:
        call_items = [item for item in messages if item.get("type") == "function_call"]
        result_items = [
            item for item in messages if item.get("type") == "function_call_output"
        ]
        if len(call_items) + len(result_items) != len(messages):
            raise DCSConflict("Responses delta contains unsupported item types")
        observed = [item.get("call_id") for item in call_items]
        results = [item.get("call_id") for item in result_items]
        call_facts = []
        for item in call_items:
            name = item.get("name")
            arguments = item.get("arguments")
            if not isinstance(name, str) or not isinstance(arguments, str):
                raise DCSConflict("Responses function_call is incomplete")
            call_facts.append((name, _json_payload_digest(arguments)))
        result_digests = [
            _result_payload_digest(item.get("output")) for item in result_items
        ]
    expected = list(tool_call_ids)
    if observed != expected or results != expected:
        raise DCSConflict("provider batch does not preserve Tool Call identity/order")
    return [
        {
            "tool_name": name,
            "arguments_digest": arguments_digest,
            "result_digest": result_digest,
        }
        for (name, arguments_digest), result_digest in zip(
            call_facts, result_digests, strict=True
        )
    ]


def _validate_barrier(request: ContextSyncBegin) -> None:
    messages = request.barrier_messages
    pending = list(request.pending_local_tool_call_ids)
    if request.barrier_reason == DCSBarrierReason.LOCAL_TOOL:
        if not messages or not pending:
            raise DCSConflict(
                "local Tool barrier requires messages and pending Tool Call IDs"
            )
        if messages[0].get("role") == "assistant":
            calls = messages[0].get("tool_calls")
            if not isinstance(calls, list):
                raise DCSConflict(
                    "local Tool barrier requires complete assistant calls"
                )
            call_ids = []
            for item in calls:
                if not isinstance(item, dict) or not isinstance(
                    item.get("function"), dict
                ):
                    raise DCSConflict("local Tool barrier has incomplete Chat call")
                function = item["function"]
                name = function.get("name")
                arguments = function.get("arguments")
                if not isinstance(item.get("id"), str) or not isinstance(name, str):
                    raise DCSConflict("local Tool barrier has incomplete Chat call")
                if not isinstance(arguments, str):
                    raise DCSConflict("local Tool barrier has incomplete Chat call")
                _json_payload_digest(arguments)
                call_ids.append(item["id"])
            result_ids = [item.get("tool_call_id") for item in messages[1:]]
            if any(item.get("role") != "tool" for item in messages[1:]):
                raise DCSConflict("local Tool barrier has invalid Chat messages")
        else:
            call_ids = []
            for item in messages:
                if item.get("type") != "function_call":
                    continue
                call_id = item.get("call_id")
                name = item.get("name")
                arguments = item.get("arguments")
                if not isinstance(call_id, str) or not isinstance(name, str):
                    raise DCSConflict(
                        "local Tool barrier has incomplete Responses call"
                    )
                if not isinstance(arguments, str):
                    raise DCSConflict(
                        "local Tool barrier has incomplete Responses call"
                    )
                _json_payload_digest(arguments)
                call_ids.append(call_id)
            result_ids = [
                item.get("call_id")
                for item in messages
                if item.get("type") == "function_call_output"
            ]
            if len(call_ids) + len(result_ids) != len(messages):
                raise DCSConflict("local Tool barrier has invalid Responses items")
        if any(item not in call_ids for item in pending):
            raise DCSConflict(
                "pending local Tool Call is absent from barrier assistant"
            )
        expected_results = [item for item in call_ids if item not in pending]
        if result_ids != expected_results:
            raise DCSConflict(
                "barrier must include results for every non-local Tool Call in order"
            )
        return
    if pending:
        raise DCSConflict("pending local Tool Calls require a local Tool barrier")
    if request.barrier_reason == DCSBarrierReason.TERMINAL_RESPONSE:
        if not messages:
            raise DCSConflict(
                "terminal barrier requires the terminal assistant response"
            )
        if messages[0].get("role") == "assistant":
            if len(messages) != 1 or messages[0].get("tool_calls"):
                raise DCSConflict(
                    "terminal Chat barrier must be one final assistant message"
                )
        elif any(item.get("type") == "function_call" for item in messages):
            raise DCSConflict(
                "terminal Responses barrier cannot contain function calls"
            )


def _json_payload_digest(value: str) -> str:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise DCSConflict("provider Tool payload is not valid JSON") from exc
    return hashlib.sha256(_canonical_json(decoded).encode()).hexdigest()


def _result_payload_digest(value: Any) -> str:
    if (
        isinstance(value, list)
        and len(value) == 1
        and isinstance(value[0], dict)
        and value[0].get("type") == "text"
        and isinstance(value[0].get("text"), str)
    ):
        value = value[0]["text"]
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _provider_reuse_content(decision: ToolReuseDecision) -> str:
    assert decision.result is not None and decision.provenance is not None
    provenance = {
        "match_kind": decision.provenance.match_kind.value,
        "observed_at": decision.provenance.observed_at.isoformat(),
        "result_schema_version": decision.provenance.result_schema_version,
        "reuse_type": decision.provenance.reuse_type.value,
    }
    return (
        f"{_canonical_json(decision.result)}\n"
        "[FlowPilot reuse provenance: "
        f"{_canonical_json(provenance)}]"
    )


def _reference_key(reference: DCSReference) -> tuple[str, str, str, int]:
    return (
        reference.tenant_id,
        reference.job_id,
        reference.line_id,
        reference.context_epoch,
    )


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DCSConflict("value is not canonical JSON") from exc


def _chained_digest(previous: str, canonical_message: str) -> str:
    return hashlib.sha256(f"{previous}\n{canonical_message}".encode()).hexdigest()


def _redact_request_snapshot(policy_json: str) -> str:
    policy = json.loads(policy_json)
    policy["request_snapshot"] = {}
    return _canonical_json(policy)


def _line_snapshot(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "protocol_version": "flowpilot-phase2-dcs-v1",
        "tenant_id": row["tenant_id"],
        "job_id": row["job_id"],
        "line_id": row["line_id"],
        "context_epoch": row["context_epoch"],
        "policy_version": row["policy_version"],
        "lease_id": row["lease_id"],
        "base_context_cursor": row["base_context_cursor"],
        "base_context_digest": row["base_context_digest"],
        "state": row["state"],
        "last_seq": row["last_seq"],
        "delta_digest": row["last_digest"],
        "pending_message_count": row["pending_count"],
        "pending_bytes": row["pending_bytes"],
        "internal_continuation_count": row["internal_continuations"],
        "barrier_reason": row["barrier_reason"],
    }
