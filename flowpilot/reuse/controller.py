from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from flowpilot.protocol import (
    REUSE_PROTOCOL_VERSION,
    SEMANTIC_REUSE_PROTOCOL_VERSION,
    BindingFailureReport,
    FalseReuseReport,
    FollowerCancellation,
    LeaderProgressReport,
    LeaderResultPublish,
    ResultProvenance,
    ReuseDecisionKind,
    ReuseMatchKind,
    ReuseProtocolVersion,
    ReuseType,
    SemanticReusePolicyUpdate,
    ToolRegistryEntry,
    ToolReuseDecision,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
)
from flowpilot.reuse.semantic import (
    HashingEmbedder,
    SemanticEmbedder,
    cosine_similarity,
)


class ReuseConflict(ValueError):
    """The caller attempted an invalid reuse state transition."""


@dataclass(frozen=True, slots=True)
class _Descriptor:
    protocol_version: ReuseProtocolVersion
    digest: str
    query_digest: str
    registry: ToolRegistryEntry
    canonical_json: str
    hard_scope_digest: str
    semantic_text: str | None
    embedding: tuple[float, ...] | None
    embedding_index_id: str | None


@dataclass(frozen=True, slots=True)
class _HistoricalMatch:
    result: dict[str, Any]
    created_at: datetime
    original_size: int
    source_query_digest: str
    source_descriptor_digest: str
    match_kind: ReuseMatchKind
    similarity_score: float | None = None


@dataclass(frozen=True, slots=True)
class _SemanticLookup:
    match: _HistoricalMatch | None
    scope_rejections: int = 0
    stale_rejections: int = 0
    threshold_rejections: int = 0
    corrupt_rejections: int = 0


@dataclass(slots=True)
class _Follower:
    identity: ToolReuseIdentity
    output_budget_bytes: int | None
    descriptor: _Descriptor
    defer_allowed: bool = False
    match_kind: ReuseMatchKind = ReuseMatchKind.EXACT
    similarity_score: float | None = None
    semantic_match_id: str | None = None


@dataclass(slots=True)
class _Binding:
    binding_id: str
    descriptor: _Descriptor
    leader: ToolReuseIdentity
    lease_deadline: datetime
    started_at: datetime
    followers: dict[str, _Follower] = field(default_factory=dict)
    status: str = "running"
    result: dict[str, Any] | None = None
    completed_at: datetime | None = None
    error_class: str | None = None
    progress_sequence: int = 0
    last_progress_at: datetime | None = None
    estimated_remaining_ms: float | None = None
    predicted_finish_time: datetime | None = None
    prediction_error_ms: float | None = None


class ReuseCache:
    """SQLite-backed exact/semantic cache and metadata-only match audit."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 3}:
                raise ReuseConflict(f"unsupported reuse cache schema version {version}")
            if version == 0:
                # A pre-migration cache may have tenant/auth partition columns.
                # Never open it as canonical state: CREATE TABLE IF NOT EXISTS
                # would otherwise leave those columns reachable indefinitely.
                existing = connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
                if existing:
                    raise ReuseConflict(
                        "legacy reuse cache schema requires explicit migration"
                    )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS exact_results (
                    descriptor_digest TEXT PRIMARY KEY,
                    canonical_descriptor TEXT NOT NULL,
                    source_query_digest TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    result_schema_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    freshness_deadline TEXT NOT NULL,
                    original_size INTEGER NOT NULL,
                    hard_scope_digest TEXT,
                    semantic_text TEXT,
                    embedding_json TEXT,
                    embedding_index_id TEXT
                )
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(exact_results)")
            }
            additions = {
                "hard_scope_digest": "TEXT",
                "semantic_text": "TEXT",
                "embedding_json": "TEXT",
                "embedding_index_id": "TEXT",
            }
            for name, kind in additions.items():
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE exact_results ADD COLUMN {name} {kind}"
                    )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_semantic_results_lookup
                ON exact_results(
                    hard_scope_digest, embedding_index_id, freshness_deadline
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS semantic_match_audit (
                    semantic_match_id TEXT PRIMARY KEY,
                    tool_name TEXT NOT NULL,
                    canonical_tool_family TEXT NOT NULL,
                    reuse_type TEXT NOT NULL,
                    request_query_digest TEXT NOT NULL,
                    source_query_digest TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    hard_scope_digest TEXT NOT NULL,
                    similarity_score REAL NOT NULL,
                    threshold REAL NOT NULL,
                    observed_at TEXT NOT NULL,
                    false_reuse_reason TEXT,
                    false_reuse_evidence_digest TEXT,
                    false_reuse_observed_at TEXT
                )
                """
            )
            connection.execute("PRAGMA user_version = 3")

    async def lookup_exact(self, descriptor: _Descriptor) -> _HistoricalMatch | None:
        async with self._lock:
            return await asyncio.to_thread(self._lookup_exact, descriptor)

    def _lookup_exact(self, descriptor: _Descriptor) -> _HistoricalMatch | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM exact_results WHERE descriptor_digest = ?",
                (descriptor.digest,),
            ).fetchone()
            if row is None:
                return None
            try:
                deadline = datetime.fromisoformat(row["freshness_deadline"])
                created_at = datetime.fromisoformat(row["created_at"])
            except (TypeError, ValueError):
                connection.execute(
                    "DELETE FROM exact_results WHERE descriptor_digest = ?",
                    (descriptor.digest,),
                )
                return None
            now = datetime.now(UTC)
            if not _valid_cache_window(
                created_at,
                deadline,
                now,
                descriptor.registry.default_ttl_seconds,
            ):
                connection.execute(
                    "DELETE FROM exact_results WHERE descriptor_digest = ?",
                    (descriptor.digest,),
                )
                return None
            if row["canonical_descriptor"] != descriptor.canonical_json:
                raise ReuseConflict("descriptor digest collision")
            # Persisted rows are an untrusted boundary.  The key lookup alone
            # is not enough: a corrupt row could otherwise attach a valid
            # result to a forged source-query or hard-scope digest and leak
            # misleading provenance.  Invalid metadata is discarded just as
            # malformed result payloads are.
            if (
                row["source_query_digest"] != descriptor.query_digest
                or row["hard_scope_digest"] != descriptor.hard_scope_digest
            ):
                connection.execute(
                    "DELETE FROM exact_results WHERE descriptor_digest = ?",
                    (descriptor.digest,),
                )
                return None
            # Cache entries are an untrusted persistence boundary. A partial
            # write or schema mismatch must never become an Observation.
            if (
                row["result_schema_version"]
                != descriptor.registry.result_schema_version
            ):
                connection.execute(
                    "DELETE FROM exact_results WHERE descriptor_digest = ?",
                    (descriptor.digest,),
                )
                return None
            try:
                result = json.loads(row["result_json"])
                encoded_size = len(_canonical_json(result).encode())
                _reject_sensitive_fields(result)
            except (TypeError, ValueError, json.JSONDecodeError, ReuseConflict):
                connection.execute(
                    "DELETE FROM exact_results WHERE descriptor_digest = ?",
                    (descriptor.digest,),
                )
                return None
            try:
                original_size = int(row["original_size"])
            except (TypeError, ValueError):
                original_size = -1
            if (
                not isinstance(result, dict)
                or encoded_size != original_size
                or encoded_size > descriptor.registry.max_result_bytes
                or re.fullmatch(r"[0-9a-f]{64}", str(row["source_query_digest"]))
                is None
            ):
                connection.execute(
                    "DELETE FROM exact_results WHERE descriptor_digest = ?",
                    (descriptor.digest,),
                )
                return None
            return _HistoricalMatch(
                result=result,
                created_at=created_at,
                original_size=original_size,
                source_query_digest=str(row["source_query_digest"]),
                source_descriptor_digest=str(row["descriptor_digest"]),
                match_kind=ReuseMatchKind.EXACT,
            )

    async def lookup_semantic(
        self, descriptor: _Descriptor, embedder: SemanticEmbedder
    ) -> _SemanticLookup:
        if descriptor.embedding is None or descriptor.embedding_index_id is None:
            return _SemanticLookup(match=None)
        async with self._lock:
            return await asyncio.to_thread(self._lookup_semantic, descriptor, embedder)

    def _lookup_semantic(
        self, descriptor: _Descriptor, embedder: SemanticEmbedder
    ) -> _SemanticLookup:
        assert descriptor.embedding is not None
        assert descriptor.embedding_index_id is not None
        now = datetime.now(UTC)
        with self._connect() as connection:
            scope_rejections = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM exact_results
                    WHERE embedding_index_id=? AND freshness_deadline>?
                        AND hard_scope_digest<>?
                    """,
                    (
                        descriptor.embedding_index_id,
                        now.isoformat(),
                        descriptor.hard_scope_digest,
                    ),
                ).fetchone()[0]
            )
            stale_rejections = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM exact_results
                    WHERE embedding_index_id=? AND hard_scope_digest=?
                        AND freshness_deadline<=?
                    """,
                    (
                        descriptor.embedding_index_id,
                        descriptor.hard_scope_digest,
                        now.isoformat(),
                    ),
                ).fetchone()[0]
            )
            connection.execute(
                "DELETE FROM exact_results WHERE freshness_deadline <= ?",
                (now.isoformat(),),
            )
            rows = connection.execute(
                """
                SELECT * FROM exact_results
                WHERE hard_scope_digest=? AND embedding_index_id=?
                    AND freshness_deadline>? AND descriptor_digest<>?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (
                    descriptor.hard_scope_digest,
                    descriptor.embedding_index_id,
                    now.isoformat(),
                    descriptor.digest,
                    descriptor.registry.semantic_candidate_limit,
                ),
            ).fetchall()
        best: tuple[float, _HistoricalMatch] | None = None
        threshold_rejections = 0
        corrupt_rejections = 0
        for row in rows:
            candidate_match = self._validated_semantic_candidate(
                row, descriptor, embedder, now
            )
            if candidate_match is None:
                corrupt_rejections += 1
                with self._connect() as cleanup:
                    cleanup.execute(
                        "DELETE FROM exact_results WHERE descriptor_digest=?",
                        (row["descriptor_digest"],),
                    )
                continue
            score, match = candidate_match
            if score < descriptor.registry.semantic_similarity_threshold:
                threshold_rejections += 1
                continue
            if best is None or score > best[0]:
                best = (score, match)
        if best is None:
            return _SemanticLookup(
                match=None,
                scope_rejections=scope_rejections,
                stale_rejections=stale_rejections,
                threshold_rejections=threshold_rejections,
                corrupt_rejections=corrupt_rejections,
            )
        score, match = best
        return _SemanticLookup(
            match=match,
            scope_rejections=scope_rejections,
            stale_rejections=stale_rejections,
            threshold_rejections=threshold_rejections,
            corrupt_rejections=corrupt_rejections,
        )

    @staticmethod
    def _validated_semantic_candidate(
        row: sqlite3.Row,
        descriptor: _Descriptor,
        embedder: SemanticEmbedder,
        now: datetime,
    ) -> tuple[float, _HistoricalMatch] | None:
        request_embedding = descriptor.embedding
        if request_embedding is None:
            return None
        try:
            canonical = row["canonical_descriptor"]
            if not isinstance(canonical, str):
                return None
            source = json.loads(canonical)
            if not isinstance(source, dict) or _canonical_json(source) != canonical:
                return None
            source_digest = hashlib.sha256(canonical.encode()).hexdigest()
            if source_digest != row["descriptor_digest"]:
                return None
            arguments = source.get("arguments")
            scope = source.get("scope")
            registry = descriptor.registry
            if not isinstance(arguments, dict) or not isinstance(scope, dict):
                return None
            if (
                source.get("canonical_tool_family") != registry.canonical_tool_family
                or source.get("tool_version") != registry.tool_version
                or source.get("result_schema_version") != registry.result_schema_version
                or row["result_schema_version"] != registry.result_schema_version
            ):
                return None
            hard_scope = {
                "canonical_tool_family": registry.canonical_tool_family,
                "tool_version": registry.tool_version,
                "result_schema_version": registry.result_schema_version,
                "scope": scope,
                "hard_arguments": {
                    key: value
                    for key, value in arguments.items()
                    if key not in registry.semantic_query_fields
                },
            }
            hard_scope_digest = hashlib.sha256(
                _canonical_json(hard_scope).encode()
            ).hexdigest()
            if (
                hard_scope_digest != row["hard_scope_digest"]
                or hard_scope_digest != descriptor.hard_scope_digest
            ):
                return None
            query_digest = hashlib.sha256(
                _canonical_json(arguments).encode()
            ).hexdigest()
            if query_digest != row["source_query_digest"]:
                return None
            values = [arguments.get(name) for name in registry.semantic_query_fields]
            if any(value is None for value in values):
                return None
            semantic_text = " ".join(_canonical_json(value) for value in values)
            if semantic_text != row["semantic_text"]:
                return None
            # Re-apply semantic admission rules at the persistence boundary.
            # Rows may have been written by an older implementation or altered
            # out of band; a valid digest alone must not revive a sensitive or
            # time-sensitive query that is forbidden for semantic reuse.
            if _contains_sensitive_fields(arguments) or _TIME_SENSITIVE_QUERY.search(
                semantic_text
            ):
                return None
            raw_embedding = json.loads(row["embedding_json"])
            if not isinstance(raw_embedding, list) or any(
                isinstance(value, bool) or not isinstance(value, (int, float))
                for value in raw_embedding
            ):
                return None
            candidate = tuple(float(value) for value in raw_embedding)
            if (
                row["embedding_index_id"] != embedder.index_id
                or len(candidate) != len(request_embedding)
                or not all(math.isfinite(value) for value in candidate)
            ):
                return None
            created_at = datetime.fromisoformat(row["created_at"])
            deadline = datetime.fromisoformat(row["freshness_deadline"])
            if not _valid_cache_window(
                created_at,
                deadline,
                now,
                descriptor.registry.default_ttl_seconds,
            ):
                return None
            result = json.loads(row["result_json"])
            if not isinstance(result, dict):
                return None
            _reject_sensitive_fields(result)
            original_size = int(row["original_size"])
            if (
                original_size < 0
                or len(_canonical_json(result).encode()) != original_size
                or original_size > registry.max_result_bytes
            ):
                return None
        except (
            TypeError,
            ValueError,
            KeyError,
            OverflowError,
            json.JSONDecodeError,
            ReuseConflict,
        ):
            return None
        score = cosine_similarity(request_embedding, candidate)
        return score, _HistoricalMatch(
            result=result,
            created_at=created_at,
            original_size=original_size,
            source_query_digest=query_digest,
            source_descriptor_digest=source_digest,
            match_kind=ReuseMatchKind.SEMANTIC,
            similarity_score=score,
        )

    async def publish(
        self,
        descriptor: _Descriptor,
        result: dict[str, Any],
        *,
        created_at: datetime,
        ttl_seconds: int,
        original_size: int,
    ) -> None:
        result_json = _canonical_json(result)
        async with self._lock:
            await asyncio.to_thread(
                self._publish,
                descriptor,
                result_json,
                created_at,
                ttl_seconds,
                original_size,
            )

    def _publish(
        self,
        descriptor: _Descriptor,
        result_json: str,
        created_at: datetime,
        ttl_seconds: int,
        original_size: int,
    ) -> None:
        deadline = created_at + timedelta(seconds=ttl_seconds)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO exact_results (
                    descriptor_digest, canonical_descriptor, source_query_digest,
                    result_json, result_schema_version, created_at,
                    freshness_deadline, original_size, hard_scope_digest,
                    semantic_text, embedding_json, embedding_index_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(descriptor_digest) DO UPDATE SET
                    canonical_descriptor=excluded.canonical_descriptor,
                    source_query_digest=excluded.source_query_digest,
                    result_json=excluded.result_json,
                    result_schema_version=excluded.result_schema_version,
                    created_at=excluded.created_at,
                    freshness_deadline=excluded.freshness_deadline,
                    original_size=excluded.original_size,
                    hard_scope_digest=excluded.hard_scope_digest,
                    semantic_text=excluded.semantic_text,
                    embedding_json=excluded.embedding_json,
                    embedding_index_id=excluded.embedding_index_id
                """,
                (
                    descriptor.digest,
                    descriptor.canonical_json,
                    descriptor.query_digest,
                    result_json,
                    descriptor.registry.result_schema_version,
                    created_at.isoformat(),
                    deadline.isoformat(),
                    original_size,
                    descriptor.hard_scope_digest,
                    descriptor.semantic_text,
                    (
                        _canonical_json(descriptor.embedding)
                        if descriptor.embedding is not None
                        else None
                    ),
                    descriptor.embedding_index_id,
                ),
            )

    async def record_semantic_match(
        self,
        *,
        descriptor: _Descriptor,
        identity: ToolReuseIdentity,
        reuse_type: ReuseType,
        source_query_digest: str,
        source_id: str,
        similarity_score: float,
    ) -> str:
        # The match ID is derived from the complete request/source tuple so a
        # retried resolution reuses the same audit row and feedback key.
        match_id = (
            "sem-"
            + hashlib.sha256(
                _canonical_json(
                    {
                        "identity": identity.model_dump(mode="json"),
                        "reuse_type": reuse_type.value,
                        "request_query_digest": descriptor.query_digest,
                        "source_query_digest": source_query_digest,
                        "source_id": source_id,
                        "hard_scope_digest": descriptor.hard_scope_digest,
                    }
                ).encode()
            ).hexdigest()
        )
        async with self._lock:
            await asyncio.to_thread(
                self._record_semantic_match,
                match_id,
                descriptor,
                identity,
                reuse_type,
                source_query_digest,
                source_id,
                similarity_score,
            )
        return match_id

    def _record_semantic_match(
        self,
        match_id: str,
        descriptor: _Descriptor,
        identity: ToolReuseIdentity,
        reuse_type: ReuseType,
        source_query_digest: str,
        source_id: str,
        similarity_score: float,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO semantic_match_audit VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL
                )
                """,
                (
                    match_id,
                    descriptor.registry.tool_name,
                    descriptor.registry.canonical_tool_family,
                    reuse_type.value,
                    descriptor.query_digest,
                    source_query_digest,
                    source_id,
                    descriptor.hard_scope_digest,
                    similarity_score,
                    descriptor.registry.semantic_similarity_threshold,
                    datetime.now(UTC).isoformat(),
                ),
            )

    async def report_false_reuse(self, report: FalseReuseReport) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._report_false_reuse, report)

    def _report_false_reuse(self, report: FalseReuseReport) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT false_reuse_reason,
                    false_reuse_evidence_digest
                FROM semantic_match_audit
                WHERE semantic_match_id=?
                """,
                (report.semantic_match_id,),
            ).fetchone()
            if row is None:
                raise ReuseConflict("unknown semantic match")
            if row["false_reuse_reason"] is not None:
                if (
                    row["false_reuse_reason"] != report.reason
                    or row["false_reuse_evidence_digest"] != report.evidence_digest
                ):
                    raise ReuseConflict(
                        "semantic match already has conflicting feedback"
                    )
                return True
            connection.execute(
                """
                UPDATE semantic_match_audit
                SET false_reuse_reason=?, false_reuse_evidence_digest=?,
                    false_reuse_observed_at=?
                WHERE semantic_match_id=?
                """,
                (
                    report.reason,
                    report.evidence_digest,
                    report.observed_at.isoformat(),
                    report.semantic_match_id,
                ),
            )
            return False

    async def audit_stats(self) -> dict[str, int]:
        async with self._lock:
            return await asyncio.to_thread(self._audit_stats)

    def _audit_stats(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT reuse_type, COUNT(*) AS count,
                    SUM(CASE WHEN false_reuse_reason IS NOT NULL THEN 1 ELSE 0 END)
                        AS false_count
                FROM semantic_match_audit GROUP BY reuse_type
                """
            ).fetchall()
        result = {"semantic_matches": 0, "false_reuse_reports": 0}
        for row in rows:
            count = int(row["count"])
            result["semantic_matches"] += count
            result[f"semantic_{row['reuse_type']}_matches"] = count
            result["false_reuse_reports"] += int(row["false_count"] or 0)
        return result

    async def count(self) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._count)

    def _count(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) FROM exact_results").fetchone()
            return int(row[0])


class WebReuseController:
    def __init__(
        self,
        registry: tuple[ToolRegistryEntry, ...],
        cache_path: Path,
        *,
        lease_seconds: float = 30.0,
        embedder: SemanticEmbedder | None = None,
    ) -> None:
        if len({entry.tool_name for entry in registry}) != len(registry):
            raise ReuseConflict("Tool registry names must be unique")
        self._registry = {entry.tool_name: entry for entry in registry}
        self._cache = ReuseCache(cache_path)
        self._embedder = embedder or HashingEmbedder()
        self._semantic_disabled_tools: set[str] = set()
        self._semantic_policy_version = 0
        self._lease_seconds = lease_seconds
        self._terminal_retention_seconds = lease_seconds
        self._lock = asyncio.Lock()
        self._bindings: dict[str, _Binding] = {}
        self._descriptor_bindings: dict[str, str] = {}
        self._counters: Counter[str] = Counter()

    async def resolve(
        self,
        request: ToolReuseResolveRequest,
        *,
        defer_allowed: bool = False,
        exact_only: bool = False,
    ) -> ToolReuseDecision:
        if exact_only and request.protocol_version != REUSE_PROTOCOL_VERSION:
            raise ReuseConflict("Phase 2 DCS accepts exact reuse requests only")
        descriptor = self._descriptor(request)
        if descriptor is None:
            return ToolReuseDecision(decision=ReuseDecisionKind.EXECUTE_LOCALLY)
        async with self._lock:
            self._reject_active_identity_conflict_locked(request.identity, descriptor)
        cached = await self._lookup_history(descriptor)
        if cached is not None:
            return await self._historical_decision(
                request, descriptor, cached, defer_allowed=defer_allowed
            )
        now = datetime.now(UTC)
        async with self._lock:
            cached = await self._lookup_history(descriptor)
            if cached is not None:
                return await self._historical_decision(
                    request, descriptor, cached, defer_allowed=defer_allowed
                )
            self._expire_locked(now)
            self._reject_active_identity_conflict_locked(request.identity, descriptor)
            binding_id = self._descriptor_bindings.get(descriptor.digest)
            match_kind = ReuseMatchKind.EXACT
            similarity_score: float | None = None
            if binding_id is not None:
                binding = self._bindings[binding_id]
                if binding.leader == request.identity:
                    return ToolReuseDecision(
                        decision=ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER,
                        binding_id=binding.binding_id,
                        descriptor_digest=descriptor.digest,
                        match_kind=ReuseMatchKind.EXACT,
                    )
            else:
                semantic = self._semantic_binding_match_locked(descriptor)
                if semantic is not None:
                    binding, similarity_score = semantic
                    binding_id = binding.binding_id
                    match_kind = ReuseMatchKind.SEMANTIC
            if binding_id is not None:
                binding = self._bindings[binding_id]
                follower_key = _identity_key(request.identity)
                follower = binding.followers.get(follower_key)
                if follower is None:
                    match_id = None
                    if match_kind == ReuseMatchKind.SEMANTIC:
                        assert similarity_score is not None
                        match_id = await self._cache.record_semantic_match(
                            descriptor=descriptor,
                            identity=request.identity,
                            reuse_type=ReuseType.INFLIGHT,
                            source_query_digest=binding.descriptor.query_digest,
                            source_id=binding.binding_id,
                            similarity_score=similarity_score,
                        )
                        self._counters["semantic_inflight_matches"] += 1
                    follower = _Follower(
                        identity=request.identity,
                        output_budget_bytes=request.output_budget_bytes,
                        descriptor=descriptor,
                        defer_allowed=defer_allowed,
                        match_kind=match_kind,
                        similarity_score=similarity_score,
                        semantic_match_id=match_id,
                    )
                    binding.followers[follower_key] = follower
                return ToolReuseDecision(
                    protocol_version=(
                        SEMANTIC_REUSE_PROTOCOL_VERSION
                        if follower.match_kind == ReuseMatchKind.SEMANTIC
                        else request.protocol_version
                    ),
                    decision=(
                        ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
                        if follower.defer_allowed
                        else ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT
                    ),
                    binding_id=binding.binding_id,
                    descriptor_digest=descriptor.digest,
                    match_kind=follower.match_kind,
                    similarity_score=follower.similarity_score,
                    semantic_match_id=follower.semantic_match_id,
                    leader_estimated_remaining_ms=binding.estimated_remaining_ms,
                    retry_after_ms=max(
                        0, int((binding.lease_deadline - now).total_seconds() * 1000)
                    ),
                )
            binding = _Binding(
                binding_id=str(uuid4()),
                descriptor=descriptor,
                leader=request.identity,
                lease_deadline=now + timedelta(seconds=self._lease_seconds),
                started_at=now,
            )
            self._bindings[binding.binding_id] = binding
            self._descriptor_bindings[descriptor.digest] = binding.binding_id
            return ToolReuseDecision(
                decision=ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER,
                binding_id=binding.binding_id,
                descriptor_digest=descriptor.digest,
                match_kind=ReuseMatchKind.EXACT,
            )

    def _reject_active_identity_conflict_locked(
        self, identity: ToolReuseIdentity, descriptor: _Descriptor
    ) -> None:
        identity_key = _identity_key(identity)
        for active in self._bindings.values():
            if active.status != "running":
                continue
            follower = active.followers.get(identity_key)
            if (
                active.leader == identity
                and active.descriptor.digest != descriptor.digest
            ) or (
                follower is not None and follower.descriptor.digest != descriptor.digest
            ):
                raise ReuseConflict(
                    "identity already owns a different active reuse descriptor"
                )

    async def _lookup_history(self, descriptor: _Descriptor) -> _HistoricalMatch | None:
        exact = await self._cache.lookup_exact(descriptor)
        if exact is not None:
            self._counters["exact_historical_matches"] += 1
            return exact
        if descriptor.embedding is None:
            return None
        lookup = await self._cache.lookup_semantic(descriptor, self._embedder)
        self._counters["semantic_historical_scope_rejections"] += (
            lookup.scope_rejections
        )
        self._counters["semantic_historical_stale_rejections"] += (
            lookup.stale_rejections
        )
        self._counters["semantic_historical_threshold_rejections"] += (
            lookup.threshold_rejections
        )
        self._counters["semantic_historical_corrupt_rejections"] += (
            lookup.corrupt_rejections
        )
        if lookup.match is None:
            self._counters["semantic_historical_misses"] += 1
        return lookup.match

    async def _historical_decision(
        self,
        request: ToolReuseResolveRequest,
        descriptor: _Descriptor,
        match: _HistoricalMatch,
        *,
        defer_allowed: bool,
    ) -> ToolReuseDecision:
        adapted, returned_size, policy = _adapt_result(
            match.result,
            request.output_budget_bytes,
            descriptor.registry.max_result_bytes,
        )
        match_id = None
        if match.match_kind == ReuseMatchKind.SEMANTIC:
            assert match.similarity_score is not None
            match_id = await self._cache.record_semantic_match(
                descriptor=descriptor,
                identity=request.identity,
                reuse_type=ReuseType.HISTORICAL,
                source_query_digest=match.source_query_digest,
                source_id=match.source_descriptor_digest,
                similarity_score=match.similarity_score,
            )
            self._counters["semantic_historical_matches"] += 1
        return ToolReuseDecision(
            protocol_version=(
                SEMANTIC_REUSE_PROTOCOL_VERSION
                if match.match_kind == ReuseMatchKind.SEMANTIC
                else request.protocol_version
            ),
            decision=(
                ReuseDecisionKind.DEFER_WITH_CACHED_RESULT
                if defer_allowed
                else ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
            ),
            descriptor_digest=descriptor.digest,
            result=adapted,
            provenance=ResultProvenance(
                reuse_type=ReuseType.HISTORICAL,
                match_kind=match.match_kind,
                observed_at=match.created_at,
                result_schema_version=descriptor.registry.result_schema_version,
                source_query_digest=match.source_query_digest,
                original_size=match.original_size,
                returned_size=returned_size,
                truncation_policy=policy,
                similarity_score=match.similarity_score,
                semantic_match_id=match_id,
            ),
            match_kind=match.match_kind,
            similarity_score=match.similarity_score,
            semantic_match_id=match_id,
        )

    async def publish(self, report: LeaderResultPublish) -> ToolReuseDecision:
        now = datetime.now(UTC)
        async with self._lock:
            self._expire_locked(now)
            binding = self._require_binding(report.binding_id)
            self._require_leader(binding, report.identity)
            if binding.status == "complete":
                if binding.result != report.result:
                    raise ReuseConflict("binding already completed with another result")
            elif binding.status != "running":
                raise ReuseConflict(f"binding is {binding.status}")
            else:
                _reject_sensitive_fields(report.result)
                result_json = _canonical_json(report.result)
                if not isinstance(report.result, dict):
                    raise ReuseConflict("leader result must be a JSON object")
                size = len(result_json.encode())
                if size > binding.descriptor.registry.max_result_bytes:
                    raise ReuseConflict(
                        "leader result exceeds registry max_result_bytes"
                    )
                binding.status = "complete"
                binding.result = report.result
                binding.completed_at = now
                if binding.predicted_finish_time is not None:
                    binding.prediction_error_ms = (
                        now - binding.predicted_finish_time
                    ).total_seconds() * 1000
                self._descriptor_bindings.pop(binding.descriptor.digest, None)
                if report.cacheable:
                    requested_ttl = (
                        report.ttl_seconds
                        or binding.descriptor.registry.default_ttl_seconds
                    )
                    await self._cache.publish(
                        binding.descriptor,
                        report.result,
                        created_at=now,
                        ttl_seconds=min(
                            requested_ttl,
                            binding.descriptor.registry.default_ttl_seconds,
                        ),
                        original_size=size,
                    )
            assert binding.result is not None
            return self._result_decision(binding, report.identity, ReuseType.INFLIGHT)

    async def poll(
        self,
        binding_id: str,
        identity: ToolReuseIdentity,
        *,
        defer_allowed: bool | None = None,
    ) -> ToolReuseDecision:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            binding = self._require_binding(binding_id)
            follower = binding.followers.get(_identity_key(identity))
            if follower is None:
                raise ReuseConflict("identity is not a follower of this binding")
            if binding.status == "running":
                should_defer = (
                    follower.defer_allowed
                    if defer_allowed is None
                    else defer_allowed and follower.defer_allowed
                )
                return ToolReuseDecision(
                    protocol_version=(
                        SEMANTIC_REUSE_PROTOCOL_VERSION
                        if follower.match_kind == ReuseMatchKind.SEMANTIC
                        else follower.descriptor.protocol_version
                    ),
                    decision=(
                        ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
                        if should_defer
                        else ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT
                    ),
                    binding_id=binding_id,
                    descriptor_digest=follower.descriptor.digest,
                    match_kind=follower.match_kind,
                    similarity_score=follower.similarity_score,
                    semantic_match_id=follower.semantic_match_id,
                    leader_estimated_remaining_ms=binding.estimated_remaining_ms,
                    retry_after_ms=max(
                        0,
                        int(
                            (binding.lease_deadline - datetime.now(UTC)).total_seconds()
                            * 1000
                        ),
                    ),
                )
            if binding.status == "complete":
                return self._result_decision(
                    binding,
                    identity,
                    ReuseType.INFLIGHT,
                    defer_allowed=defer_allowed,
                )
            return ToolReuseDecision(decision=ReuseDecisionKind.EXECUTE_LOCALLY)

    async def poll_deferred(
        self,
        binding_id: str,
        request: ToolReuseResolveRequest,
        *,
        exact_only: bool = False,
    ) -> ToolReuseDecision:
        if exact_only and request.protocol_version != REUSE_PROTOCOL_VERSION:
            raise ReuseConflict("Phase 2 DCS accepts exact reuse requests only")
        descriptor = self._descriptor(request)
        if descriptor is None:
            raise ReuseConflict("deferred poll references a non-reusable Tool")
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            binding = self._require_binding(binding_id)
            follower = binding.followers.get(_identity_key(request.identity))
            if follower is None:
                raise ReuseConflict("identity is not a follower of this binding")
            if exact_only and follower.match_kind != ReuseMatchKind.EXACT:
                raise ReuseConflict("Phase 2 DCS accepts exact reuse bindings only")
            if follower.descriptor.digest != descriptor.digest:
                raise ReuseConflict("deferred poll descriptor conflicts with binding")
            if follower.descriptor.protocol_version != descriptor.protocol_version:
                raise ReuseConflict("deferred poll protocol version conflicts")
            if binding.status == "running":
                return ToolReuseDecision(
                    protocol_version=(
                        SEMANTIC_REUSE_PROTOCOL_VERSION
                        if follower.match_kind == ReuseMatchKind.SEMANTIC
                        else request.protocol_version
                    ),
                    decision=(
                        ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
                        if follower.defer_allowed
                        else ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT
                    ),
                    binding_id=binding_id,
                    descriptor_digest=follower.descriptor.digest,
                    match_kind=follower.match_kind,
                    similarity_score=follower.similarity_score,
                    semantic_match_id=follower.semantic_match_id,
                    leader_estimated_remaining_ms=binding.estimated_remaining_ms,
                    retry_after_ms=max(
                        0,
                        int(
                            (binding.lease_deadline - datetime.now(UTC)).total_seconds()
                            * 1000
                        ),
                    ),
                )
            if binding.status == "complete":
                return self._result_decision(
                    binding,
                    request.identity,
                    ReuseType.INFLIGHT,
                    defer_allowed=True,
                )
            return ToolReuseDecision(
                protocol_version=descriptor.protocol_version,
                decision=ReuseDecisionKind.EXECUTE_LOCALLY,
            )

    async def progress(self, report: LeaderProgressReport) -> bool:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            binding = self._require_binding(report.binding_id)
            self._require_leader(binding, report.identity)
            if binding.status != "running":
                raise ReuseConflict(f"binding is already {binding.status}")
            if report.sequence < binding.progress_sequence:
                raise ReuseConflict("leader progress sequence regressed")
            if report.sequence == binding.progress_sequence:
                if (
                    binding.last_progress_at == report.observed_at
                    and binding.estimated_remaining_ms == report.estimated_remaining_ms
                ):
                    return True
                raise ReuseConflict("leader progress sequence conflicts")
            binding.progress_sequence = report.sequence
            binding.last_progress_at = report.observed_at
            binding.estimated_remaining_ms = report.estimated_remaining_ms
            binding.predicted_finish_time = (
                report.observed_at
                + timedelta(milliseconds=report.estimated_remaining_ms)
                if report.estimated_remaining_ms is not None
                else None
            )
            self._counters["leader_progress_updates"] += 1
            return False

    async def report_false_reuse(self, report: FalseReuseReport) -> bool:
        duplicate = await self._cache.report_false_reuse(report)
        if not duplicate:
            self._counters["false_reuse_reports"] += 1
        return duplicate

    async def update_semantic_policy(
        self, update: SemanticReusePolicyUpdate
    ) -> dict[str, Any]:
        async with self._lock:
            if update.expected_version != self._semantic_policy_version:
                raise ReuseConflict(
                    "expected semantic policy version "
                    f"{self._semantic_policy_version}, "
                    f"got {update.expected_version}"
                )
            assert update.tool_name is not None
            registry = self._registry.get(update.tool_name)
            if registry is None:
                raise ReuseConflict("semantic policy references an unknown Tool")
            if update.enabled and not registry.semantic_reuse_enabled:
                raise ReuseConflict("registry does not allow semantic reuse")
            target = self._semantic_disabled_tools
            value = update.tool_name
            if update.enabled:
                target.discard(value)
            else:
                target.add(value)
            self._semantic_policy_version = update.version
            self._counters["semantic_policy_updates"] += 1
            return self._semantic_policy_snapshot()

    async def fail(self, report: BindingFailureReport) -> None:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            binding = self._require_binding(report.binding_id)
            self._require_leader(binding, report.identity)
            if binding.status == "failed" and binding.error_class == report.error_class:
                return
            if binding.status != "running":
                raise ReuseConflict(f"binding is already {binding.status}")
            binding.status = "failed"
            binding.error_class = report.error_class
            binding.completed_at = datetime.now(UTC)
            self._descriptor_bindings.pop(binding.descriptor.digest, None)

    async def cancel_follower(self, cancellation: FollowerCancellation) -> None:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            binding = self._require_binding(cancellation.binding_id)
            if binding.leader == cancellation.identity:
                raise ReuseConflict("leader cannot cancel itself as a follower")
            # Cancellation is idempotent so a retry after a successful removal
            # does not turn a client-side timeout into a control-plane error.
            binding.followers.pop(_identity_key(cancellation.identity), None)

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            audit = await self._cache.audit_stats()
            return {
                "registry_tools": sorted(self._registry),
                "cache_entries": await self._cache.count(),
                "semantic": {
                    "embedding_index_id": self._embedder.index_id,
                    **self._semantic_policy_snapshot(),
                    "counters": {**dict(self._counters), **audit},
                },
                "bindings": [
                    {
                        "binding_id": item.binding_id,
                        "descriptor_digest": item.descriptor.digest,
                        "status": item.status,
                        "follower_count": len(item.followers),
                        "lease_deadline": item.lease_deadline.isoformat(),
                        "progress_sequence": item.progress_sequence,
                        "estimated_remaining_ms": item.estimated_remaining_ms,
                        "prediction_error_ms": item.prediction_error_ms,
                    }
                    for item in self._bindings.values()
                ],
            }

    def _descriptor(self, request: ToolReuseResolveRequest) -> _Descriptor | None:
        registry = self._registry.get(request.tool_name)
        if (
            registry is None
            or not registry.read_only
            or not registry.exact_reuse_enabled
        ):
            return None
        # Exact reuse must never persist credentials or session-bound values
        # supplied as Tool arguments. Let the Agent execute such a call locally
        # instead of turning it into a reusable cache key.
        if _contains_sensitive_fields(request.arguments):
            self._counters["sensitive_argument_rejections"] += 1
            return None
        scope = request.scope.model_dump(mode="json")
        # Constraint collections are sets for matching purposes; normalize
        # their order so equivalent requests share one exact descriptor.
        scope["data_source_constraints"] = sorted(set(scope["data_source_constraints"]))
        value = {
            "canonical_tool_family": registry.canonical_tool_family,
            "tool_version": registry.tool_version,
            "result_schema_version": registry.result_schema_version,
            "scope": scope,
            "arguments": request.arguments,
        }
        canonical = _canonical_json(value)
        hard_arguments = {
            key: item
            for key, item in request.arguments.items()
            if key not in registry.semantic_query_fields
        }
        hard_scope = {
            "canonical_tool_family": registry.canonical_tool_family,
            "tool_version": registry.tool_version,
            "result_schema_version": registry.result_schema_version,
            "scope": scope,
            "hard_arguments": hard_arguments,
        }
        semantic_text: str | None = None
        embedding: tuple[float, ...] | None = None
        embedding_index_id: str | None = None
        semantic_requested = (
            request.protocol_version == SEMANTIC_REUSE_PROTOCOL_VERSION
            and registry.semantic_reuse_enabled
        )
        if semantic_requested and request.tool_name in self._semantic_disabled_tools:
            self._counters["semantic_tool_kill_switch_rejections"] += 1
            semantic_requested = False
        if semantic_requested and request.scope.time_sensitivity_class not in (
            registry.semantic_time_sensitivity_classes
        ):
            self._counters["semantic_time_scope_rejections"] += 1
            semantic_requested = False
        if semantic_requested and _contains_sensitive_fields(request.arguments):
            self._counters["semantic_sensitive_query_rejections"] += 1
            semantic_requested = False
        if semantic_requested:
            values = [
                request.arguments.get(name) for name in registry.semantic_query_fields
            ]
            if any(value is None for value in values):
                self._counters["semantic_missing_query_rejections"] += 1
            else:
                semantic_text = " ".join(_canonical_json(value) for value in values)
                if _TIME_SENSITIVE_QUERY.search(semantic_text):
                    self._counters["semantic_temporal_query_rejections"] += 1
                    semantic_text = None
                else:
                    candidate = self._embedder.embed(semantic_text)
                    if any(candidate):
                        embedding = candidate
                        embedding_index_id = self._embedder.index_id
        return _Descriptor(
            protocol_version=request.protocol_version,
            digest=hashlib.sha256(canonical.encode()).hexdigest(),
            query_digest=hashlib.sha256(
                _canonical_json(request.arguments).encode()
            ).hexdigest(),
            registry=registry,
            canonical_json=canonical,
            hard_scope_digest=hashlib.sha256(
                _canonical_json(hard_scope).encode()
            ).hexdigest(),
            semantic_text=semantic_text,
            embedding=embedding,
            embedding_index_id=embedding_index_id,
        )

    def _semantic_binding_match_locked(
        self, descriptor: _Descriptor
    ) -> tuple[_Binding, float] | None:
        if descriptor.embedding is None or descriptor.embedding_index_id is None:
            return None
        best: tuple[_Binding, float] | None = None
        candidates = 0
        for binding in self._bindings.values():
            candidate = binding.descriptor
            if (
                binding.status == "running"
                and candidate.embedding_index_id == descriptor.embedding_index_id
                and candidate.hard_scope_digest != descriptor.hard_scope_digest
            ):
                self._counters["semantic_inflight_scope_rejections"] += 1
            if (
                binding.status != "running"
                or candidate.hard_scope_digest != descriptor.hard_scope_digest
                or candidate.embedding_index_id != descriptor.embedding_index_id
                or candidate.embedding is None
            ):
                continue
            candidates += 1
            if candidates > descriptor.registry.semantic_candidate_limit:
                break
            score = cosine_similarity(descriptor.embedding, candidate.embedding)
            if score < descriptor.registry.semantic_similarity_threshold:
                self._counters["semantic_inflight_threshold_rejections"] += 1
                continue
            if best is None or score > best[1]:
                best = (binding, score)
        return best

    def _semantic_policy_snapshot(self) -> dict[str, Any]:
        return {
            "policy_version": self._semantic_policy_version,
            "disabled_tools": sorted(self._semantic_disabled_tools),
        }

    def _expire_locked(self, now: datetime) -> None:
        for binding_id, binding in list(self._bindings.items()):
            if binding.status == "running" and binding.lease_deadline <= now:
                binding.status = "expired"
                binding.completed_at = now
                self._descriptor_bindings.pop(binding.descriptor.digest, None)
            if (
                binding.status != "running"
                and binding.completed_at is not None
                and binding.completed_at
                + timedelta(seconds=self._terminal_retention_seconds)
                <= now
            ):
                self._bindings.pop(binding_id, None)

    def _require_binding(self, binding_id: str) -> _Binding:
        binding = self._bindings.get(binding_id)
        if binding is None:
            raise ReuseConflict("unknown binding")
        return binding

    @staticmethod
    def _require_leader(binding: _Binding, identity: ToolReuseIdentity) -> None:
        if binding.leader != identity:
            raise ReuseConflict("identity does not own this leader binding")

    @staticmethod
    def _result_decision(
        binding: _Binding,
        identity: ToolReuseIdentity,
        reuse_type: ReuseType,
        defer_allowed: bool | None = None,
    ) -> ToolReuseDecision:
        assert binding.result is not None and binding.completed_at is not None
        follower = binding.followers.get(_identity_key(identity))
        budget = follower.output_budget_bytes if follower else None
        descriptor = follower.descriptor if follower else binding.descriptor
        match_kind = follower.match_kind if follower else ReuseMatchKind.EXACT
        should_defer = bool(
            follower
            and follower.defer_allowed
            and (defer_allowed is None or defer_allowed)
        )
        result, returned_size, policy = _adapt_result(
            binding.result, budget, binding.descriptor.registry.max_result_bytes
        )
        original_size = len(_canonical_json(binding.result).encode())
        return ToolReuseDecision(
            protocol_version=(
                SEMANTIC_REUSE_PROTOCOL_VERSION
                if match_kind == ReuseMatchKind.SEMANTIC
                else descriptor.protocol_version
            ),
            decision=(
                ReuseDecisionKind.DEFER_WITH_CACHED_RESULT
                if should_defer
                else ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
            ),
            binding_id=binding.binding_id,
            descriptor_digest=descriptor.digest,
            result=result,
            provenance=ResultProvenance(
                reuse_type=reuse_type,
                match_kind=match_kind,
                observed_at=binding.completed_at,
                result_schema_version=binding.descriptor.registry.result_schema_version,
                source_query_digest=binding.descriptor.query_digest,
                original_size=original_size,
                returned_size=returned_size,
                truncation_policy=policy,
                similarity_score=(
                    follower.similarity_score if follower is not None else None
                ),
                semantic_match_id=(
                    follower.semantic_match_id if follower is not None else None
                ),
            ),
            match_kind=match_kind,
            similarity_score=(
                follower.similarity_score if follower is not None else None
            ),
            semantic_match_id=(
                follower.semantic_match_id if follower is not None else None
            ),
        )


def _adapt_result(
    result: dict[str, Any], budget: int | None, registry_limit: int
) -> tuple[dict[str, Any], int, str]:
    limit = min(value for value in (budget, registry_limit) if value is not None)
    encoded = _canonical_json(result).encode()
    if len(encoded) <= limit:
        return result, len(encoded), "none"
    items = result.get("items")
    if not isinstance(items, list):
        raise ReuseConflict("result exceeds budget and has no structured items array")
    kept: list[Any] = []
    for item in items:
        candidate = {**result, "items": [*kept, item]}
        if len(_canonical_json(candidate).encode()) > limit:
            break
        kept.append(item)
    adapted = {**result, "items": kept}
    size = len(_canonical_json(adapted).encode())
    if size > limit:
        raise ReuseConflict("result metadata alone exceeds output budget")
    return adapted, size, "items_prefix_v1"


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
        raise ReuseConflict("value is not canonical JSON") from exc


def _valid_cache_window(
    created_at: datetime,
    deadline: datetime,
    now: datetime,
    max_ttl_seconds: int,
) -> bool:
    if (
        created_at.tzinfo is None
        or created_at.utcoffset() is None
        or deadline.tzinfo is None
        or deadline.utcoffset() is None
        or deadline <= now
        or deadline <= created_at
    ):
        return False
    try:
        maximum_deadline = created_at + timedelta(seconds=max_ttl_seconds)
    except OverflowError:
        return False
    return deadline <= maximum_deadline


def _identity_key(identity: ToolReuseIdentity) -> str:
    return _canonical_json(identity.model_dump(mode="json"))


_SENSITIVE_KEY_PARTS = {
    "access_key",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "cookie",
    "credential",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "session",
    "token",
}
_SENSITIVE_KEY_NAMES = {
    "accesskey",
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "password",
    "privatekey",
    "refreshtoken",
    "secret",
    "session",
    "token",
}
_SENSITIVE_VALUE_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
        r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}",
        r"\bAKIA[0-9A-Z]{16}\b",
        r"\b(?:sk|rk)-[A-Za-z0-9_-]{16,}\b",
        (
            r"\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)"
            r"\s*[:=]\s*[^\s,;]{6,}"
        ),
    )
)
_TIME_SENSITIVE_QUERY = re.compile(
    r"(?:\b(?:today|current|currently|latest|now|price|prices|weather)\b|"
    r"今天|当前|现在|最新|价格|天气)",
    re.IGNORECASE,
)


def _contains_sensitive_fields(value: Any) -> bool:
    try:
        _reject_sensitive_fields(value)
    except ReuseConflict:
        return True
    return False


def _reject_sensitive_fields(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            parts = set(normalized.split("_"))
            if normalized.replace("_", "") in _SENSITIVE_KEY_NAMES or any(
                part in _SENSITIVE_KEY_PARTS for part in parts
            ):
                raise ReuseConflict(
                    "leader result contains a forbidden sensitive field"
                )
            _reject_sensitive_fields(item)
    elif isinstance(value, list):
        for item in value:
            _reject_sensitive_fields(item)
    elif isinstance(value, str) and any(
        pattern.search(value) for pattern in _SENSITIVE_VALUE_PATTERNS
    ):
        raise ReuseConflict("leader result contains sensitive text")
