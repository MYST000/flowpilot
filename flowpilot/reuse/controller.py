from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import struct
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from flowpilot.frontier.store import LineTailFrontier
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
    ReuseType,
    SemanticReusePolicyUpdate,
    ToolRegistryEntry,
    ToolReuseDecision,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
    ToolTelemetryEvent,
)

from .adapters.browsecomp import BrowseCompSearchAdapter
from .adapters.registry import get_adapter
from .adapters.tavily import TAVILY_SCHEMA_DIGESTS, TavilySiteAdapter
from .adapters.terminal_url import TerminalUrlFetchAdapter, terminal_input
from .contracts import (
    ReuseConflict,
    TrustedContext,
    canonical_json,
    digest,
    reject_sensitive,
    secret_dependent,
    tool_call_key,
)
from .semantic import SemanticEmbedder, cosine_similarity
from .store import ReuseCache

logger = logging.getLogger(__name__)
_TEMPORAL = re.compile(
    r"\b(today|current|currently|latest|now|price|prices|weather)\b|今天|当前|现在|最新|价格|天气",
    re.I,
)


@dataclass(frozen=True)
class _Descriptor:
    request: ToolReuseResolveRequest
    registry: ToolRegistryEntry
    context: TrustedContext
    canonical_json: str
    digest: str
    query_digest: str
    hard_scope_digest: str
    semantic_text: str | None
    embedding: tuple[float, ...] | None = None
    embedding_index_id: str | None = None


@dataclass
class _Follower:
    identity: ToolReuseIdentity
    descriptor: _Descriptor
    defer_allowed: bool
    match_kind: ReuseMatchKind = ReuseMatchKind.EXACT
    similarity_score: float | None = None
    semantic_match_id: str | None = None


@dataclass
class _Binding:
    binding_id: str
    descriptor: _Descriptor
    leader: ToolReuseIdentity
    lease_deadline: datetime
    started_at: datetime
    followers: dict[str, _Follower] = field(default_factory=dict)
    status: str = "running"
    execution: ToolTelemetryEvent | None = None
    origin_id: str | None = None
    completed_at: datetime | None = None
    error_class: str | None = None
    progress_sequence: int = 0
    last_progress_at: datetime | None = None
    estimated_remaining_ms: float | None = None


class WebReuseController:
    def __init__(
        self,
        registry: tuple[ToolRegistryEntry, ...],
        cache_path: Path,
        *,
        lease_seconds: float = 30,
        embedder: SemanticEmbedder | None = None,
        frontier: LineTailFrontier | None = None,
        max_payload_bytes: int | None = None,
    ) -> None:
        if len({r.tool_name for r in registry}) != len(registry):
            raise ReuseConflict("Tool registry names must be unique")
        self._registry = {r.tool_name: r for r in registry}
        self._cache = ReuseCache(cache_path)
        self._embedder = embedder
        self._embedding_status = "configured" if embedder else "disabled"
        self._frontier = frontier
        self._max_payload_bytes = max_payload_bytes
        self._maintenance_stats: dict[str, int] = {}
        self._lease_seconds = lease_seconds
        self._terminal_retention_seconds = lease_seconds
        self._lock = asyncio.Lock()
        self._bindings: dict[str, _Binding] = {}
        self._binding_generation = 0
        self._descriptor_bindings: dict[str, str] = {}
        self._semantic_disabled_tools: set[str] = set()
        self._semantic_policy_version = 0
        self._counters: Counter[str] = Counter()
        self._index_tasks: set[asyncio.Task[None]] = set()

    def _descriptor(
        self, request: ToolReuseResolveRequest, trusted_context: TrustedContext
    ) -> _Descriptor | None:
        registry = self._registry.get(request.tool_name)
        if (
            registry is None
            or not registry.read_only
            or not registry.exact_reuse_enabled
        ):
            return None
        if secret_dependent(request.arguments):
            self._counters["secret_dependent_input"] += 1
            return None
        try:
            reject_sensitive(request.arguments)
            canonical_json(request.arguments)
        except ReuseConflict:
            self._counters["sensitive_argument_rejections"] += 1
            return None
        adapter = get_adapter(request.tool_name, registry.adapter_id)
        arguments = request.arguments
        if adapter is not None:
            if (registry.adapter_id, registry.adapter_version) != (
                adapter.adapter_id,
                adapter.adapter_version,
            ):
                self._counters["adapter_version_mismatch"] += 1
                return None
            if request.tool_name == "curl":
                self._counters["url_executor_unverified"] += 1
                return None
            if (
                isinstance(adapter, TerminalUrlFetchAdapter)
                and registry.command_line_reuse == "disabled"
            ):
                return None
            if (
                request.tool_name == "url_fetch"
                and registry.url_execution_policy_id != "public-pinned-get-v1"
            ):
                return None
            if request.tool_name.startswith("tavily-") and (
                registry.tool_version != "0.2.1"
                or registry.input_schema_digest
                != TAVILY_SCHEMA_DIGESTS[request.tool_name]
                or request.input_schema_digest != registry.input_schema_digest
            ):
                self._counters["mcp_schema_unverified"] += 1
                return None
            if isinstance(adapter, BrowseCompSearchAdapter) and (
                registry.input_schema_digest is None
                or request.input_schema_digest != registry.input_schema_digest
                or registry.policy_digest is None
                or f"browsecomp-search:{registry.policy_digest}"
                not in request.scope.data_source_constraints
            ):
                self._counters["browsecomp_profile_unverified"] += 1
                return None
            parsed = adapter.parse_tool_call(request.tool_name, arguments)
            if parsed is None:
                self._counters["adapter_rejections"] += 1
                return None
            arguments = adapter.canonicalize_arguments(parsed)
            if (
                isinstance(adapter, TerminalUrlFetchAdapter)
                and registry.command_line_reuse == "curl_url_exact"
                and arguments["executable_family"] != "curl"
            ):
                return None
        elif registry.adapter_id != "generic_v1":
            return None
        scope = request.scope.model_dump(mode="json")
        scope["data_source_constraints"] = sorted(set(scope["data_source_constraints"]))
        base = {
            "deployment_id": trusted_context.deployment_id,
            "namespace_id": trusted_context.namespace_id,
            "canonical_tool_family": registry.canonical_tool_family,
            "tool_version": registry.tool_version,
            "adapter_id": registry.adapter_id,
            "adapter_version": registry.adapter_version,
            "tool_schema_version": registry.tool_schema_version,
            "input_schema_digest": registry.input_schema_digest,
            "result_schema_version": registry.result_schema_version,
            "security_policy_id": registry.security_policy_id,
            "freshness_policy_id": registry.freshness_policy_id,
            "policy_digest": registry.policy_digest,
            "url_execution_policy_id": registry.url_execution_policy_id,
            **(
                {"command_line_reuse": registry.command_line_reuse}
                if isinstance(adapter, TerminalUrlFetchAdapter)
                else {}
            ),
            "scope": scope,
        }
        value = {**base, "arguments": arguments}
        hard = {
            **base,
            "hard_arguments": {
                k: v
                for k, v in arguments.items()
                if k not in registry.semantic_query_fields
            },
        }
        text = adapter.build_semantic_text(arguments) if adapter else None
        if request.scope.time_sensitivity_class != "standard" or (
            text and _TEMPORAL.search(text)
        ):
            text = None
        execution_arguments = arguments
        if isinstance(adapter, TerminalUrlFetchAdapter):
            execution_arguments = terminal_input(request.arguments)
        elif isinstance(adapter, TavilySiteAdapter):
            execution_arguments = request.arguments
        return _Descriptor(
            request,
            registry,
            trusted_context,
            canonical_json(value),
            digest(value),
            # Runtime binds the actual TerminalAction, while only Scheduler
            # interprets command families and constructs the matching key.
            digest(execution_arguments),
            digest(hard),
            text,
        )

    def _semantic_enabled(self, descriptor: _Descriptor) -> bool:
        return (
            descriptor.request.protocol_version == SEMANTIC_REUSE_PROTOCOL_VERSION
            and descriptor.registry.semantic_reuse_enabled
            and descriptor.registry.tool_name not in self._semantic_disabled_tools
            and descriptor.semantic_text is not None
            and self._embedder is not None
        )

    async def _embed(self, descriptor: _Descriptor) -> _Descriptor:
        if not self._semantic_enabled(descriptor):
            return descriptor
        assert self._embedder is not None and descriptor.semantic_text is not None
        try:
            values = await asyncio.wait_for(
                self._embedder.embed([descriptor.semantic_text]), timeout=5.0
            )
            vector = values[0]
            self._embedding_status = "ready"
            self._validate_vector(vector)
            return replace(
                descriptor, embedding=vector, embedding_index_id=self._embedder.index_id
            )
        except Exception as exc:
            self._embedding_status = "unavailable"
            self._counters["semantic_embedding_failed"] += 1
            logger.warning("semantic embedding failed: %s", type(exc).__name__)
            return descriptor

    def _validate_vector(self, vector: tuple[float, ...]) -> None:
        if (
            self._embedder is None
            or len(vector) != self._embedder.dimension
            or not all(math.isfinite(v) for v in vector)
        ):
            raise ReuseConflict("invalid embedding dimension or values")
        if abs(sum(v * v for v in vector) - 1) > 0.001:
            raise ReuseConflict("embedding is not L2 normalized")

    async def resolve(
        self,
        request: ToolReuseResolveRequest,
        *,
        trusted_context: TrustedContext,
        defer_allowed: bool = False,
        exact_only: bool = False,
    ) -> ToolReuseDecision:
        if exact_only and request.protocol_version != REUSE_PROTOCOL_VERSION:
            raise ReuseConflict("Phase 2 DCS accepts exact reuse requests only")
        descriptor = self._descriptor(request, trusted_context)
        if descriptor is None:
            return ToolReuseDecision(
                protocol_version=request.protocol_version,
                decision=ReuseDecisionKind.EXECUTE_LOCALLY,
                reason="input_or_adapter_ineligible",
            )
        result = await self._history(descriptor, defer_allowed=defer_allowed)
        if result:
            return result
        candidate_hints: list[dict[str, Any]] = []
        if not exact_only:
            descriptor = await self._embed(descriptor)
            result = await self._semantic_history(
                descriptor, defer_allowed=defer_allowed, candidates=candidate_hints
            )
            if result:
                return result
        while True:
            generation = -1
            inflight_scores: list[tuple[float, str]] = []
            if descriptor.embedding is not None and self._semantic_enabled(descriptor):
                async with self._lock:
                    generation = self._binding_generation
                    snapshot = [
                        (
                            item.binding_id,
                            item.started_at.timestamp(),
                            item.descriptor.embedding,
                        )
                        for item in self._bindings.values()
                        if item.status == "running"
                        and item.descriptor.hard_scope_digest
                        == descriptor.hard_scope_digest
                        and item.descriptor.embedding_index_id
                        == descriptor.embedding_index_id
                        and item.descriptor.embedding is not None
                    ]

                def score_snapshot(snapshot=snapshot) -> list[tuple[float, str]]:
                    assert descriptor.embedding is not None
                    values = [
                        (
                            cosine_similarity(descriptor.embedding, vector),
                            started,
                            identifier,
                        )
                        for identifier, started, vector in snapshot
                        if vector is not None
                    ]
                    values.sort(key=lambda value: (-value[0], -value[1], value[2]))
                    return [
                        (similarity, identifier) for similarity, _, identifier in values
                    ]

                inflight_scores = await asyncio.to_thread(score_snapshot)
            async with self._lock:
                if generation >= 0 and generation != self._binding_generation:
                    continue
                await self._expire_locked()
                # Publication may have committed while embedding ran without the lock.
                result = await self._history(descriptor, defer_allowed=defer_allowed)
                if result:
                    return result
                key = tool_call_key(request.identity)
                for active in self._bindings.values():
                    if (
                        active.status != "running"
                        or active.descriptor.context != trusted_context
                    ):
                        continue
                    prior = (
                        active.descriptor
                        if tool_call_key(active.leader) == key
                        else (
                            active.followers[key].descriptor
                            if key in active.followers
                            else None
                        )
                    )
                    if prior and prior.digest != descriptor.digest:
                        raise ReuseConflict(
                            "ToolCallRef already bound to different input"
                        )
                binding_id = self._descriptor_bindings.get(descriptor.digest)
                binding = self._bindings.get(binding_id or "")
                if binding and tool_call_key(binding.leader) == key:
                    if (
                        binding.leader.action_id
                        and request.identity.action_id != binding.leader.action_id
                    ):
                        raise ReuseConflict("Action conflicts with registered leader")
                    return self._decision(
                        descriptor,
                        ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER,
                        binding_id=binding.binding_id,
                    )
                match_kind, score, match_id = ReuseMatchKind.EXACT, None, None
                if (
                    binding is None
                    and descriptor.embedding is not None
                    and self._semantic_enabled(descriptor)
                ):
                    # Heavy scoring used an immutable snapshot outside the lock.
                    # Recheck the candidate and policy after acquiring it again.
                    scored = [
                        (similarity, self._bindings[identifier])
                        for similarity, identifier in inflight_scores
                        if identifier in self._bindings
                        and self._bindings[identifier].status == "running"
                        and self._bindings[identifier].lease_deadline
                        > datetime.now(UTC)
                    ]
                    if (
                        scored
                        and scored[0][0]
                        >= descriptor.registry.semantic_similarity_threshold
                    ):
                        score, candidate_binding = scored[0]
                        match_id = await self._audit(
                            descriptor, candidate_binding.binding_id, score, "inflight"
                        )
                        if descriptor.registry.semantic_mode == "active":
                            binding, match_kind = (
                                candidate_binding,
                                ReuseMatchKind.SEMANTIC,
                            )
                        elif descriptor.registry.semantic_mode == "candidate":
                            candidate_hints.append(
                                {
                                    "source_kind": "inflight",
                                    "source_id": candidate_binding.binding_id,
                                    "score": score,
                                    "semantic_match_id": match_id,
                                }
                            )
                if binding is not None:
                    follower = binding.followers.get(key)
                    if (
                        follower is not None
                        and follower.identity.action_id
                        and request.identity.action_id != follower.identity.action_id
                    ):
                        raise ReuseConflict("Action conflicts with registered follower")
                    binding.followers[key] = _Follower(
                        request.identity,
                        descriptor,
                        defer_allowed,
                        match_kind,
                        score,
                        match_id,
                    )
                    return self._waiting(binding, binding.followers[key])
                now = datetime.now(UTC)
                binding = _Binding(
                    str(uuid4()),
                    descriptor,
                    request.identity,
                    now + timedelta(seconds=self._lease_seconds),
                    now,
                )
                self._binding_generation += 1
                self._bindings[binding.binding_id] = binding
                self._descriptor_bindings[descriptor.digest] = binding.binding_id
                return self._decision(
                    descriptor,
                    ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER,
                    binding_id=binding.binding_id,
                    semantic_candidates=tuple(candidate_hints),
                )

    async def _history(
        self, descriptor: _Descriptor, *, defer_allowed: bool
    ) -> ToolReuseDecision | None:
        for row in await self._cache.candidates(exact_key=descriptor.digest):
            decision = await self._delivery(
                descriptor, row, ReuseType.HISTORICAL, defer_allowed=defer_allowed
            )
            if decision:
                self._counters["exact_historical_matches"] += 1
                return decision
        return None

    async def _semantic_history(
        self,
        descriptor: _Descriptor,
        *,
        defer_allowed: bool,
        candidates: list[dict[str, Any]],
    ) -> ToolReuseDecision | None:
        if descriptor.embedding is None or not self._semantic_enabled(descriptor):
            return None
        rows = await self._cache.candidates(
            hard_scope_digest=descriptor.hard_scope_digest
        )
        scored, corrupt = await asyncio.to_thread(self._score_history, descriptor, rows)
        self._counters["semantic_corrupt_vector"] += corrupt
        for score, row in scored[: descriptor.registry.semantic_candidate_limit]:
            match_id = await self._audit(
                descriptor, row["origin_id"], score, "historical"
            )
            if descriptor.registry.semantic_mode == "candidate":
                candidates.append(
                    {
                        "source_kind": "historical",
                        "source_id": row["origin_id"],
                        "score": score,
                        "semantic_match_id": match_id,
                    }
                )
            if (
                descriptor.registry.semantic_mode != "active"
                or not self._semantic_enabled(descriptor)
            ):
                continue
            decision = await self._delivery(
                descriptor,
                row,
                ReuseType.HISTORICAL,
                defer_allowed=defer_allowed,
                score=score,
                match_id=match_id,
            )
            if decision:
                self._counters["semantic_historical_matches"] += 1
                return decision
        return None

    def _score_history(
        self, descriptor: _Descriptor, rows: list[dict[str, Any]]
    ) -> tuple[list[tuple[float, dict[str, Any]]], int]:
        assert descriptor.embedding is not None
        corrupt = 0
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            try:
                if (
                    row["index_id"] != descriptor.embedding_index_id
                    or row["normalization"] != "L2"
                ):
                    continue
                if row["semantic_text_digest"] != digest(row["semantic_text"]):
                    raise ReuseConflict("semantic text digest mismatch")
                vector = struct.unpack(f"<{row['dimension']}f", row["embedding"])
                self._validate_vector(vector)
                score = cosine_similarity(descriptor.embedding, vector)
                if score >= descriptor.registry.semantic_similarity_threshold:
                    scored.append((score, row))
            except (ValueError, TypeError, struct.error, ReuseConflict):
                corrupt += 1
        scored.sort(
            key=lambda pair: (
                -pair[0],
                -datetime.fromisoformat(pair[1]["observed_at"]).timestamp(),
                pair[1]["origin_id"],
            )
        )
        return scored, corrupt

    async def _audit(
        self, descriptor: _Descriptor, source: str, score: float, reason: str
    ) -> str:
        return await self._cache.audit(
            descriptor.request.identity.model_dump(mode="json"),
            source_id=source,
            tool_name=descriptor.registry.tool_name,
            match_kind="semantic",
            score=score,
            threshold=descriptor.registry.semantic_similarity_threshold,
            decision=descriptor.registry.semantic_mode,
            reason=reason,
            index_id=descriptor.embedding_index_id,
        )

    async def _delivery(
        self,
        descriptor: _Descriptor,
        row: dict[str, Any],
        reuse_type: ReuseType,
        *,
        defer_allowed: bool,
        score: float | None = None,
        match_id: str | None = None,
    ) -> ToolReuseDecision | None:
        with self._cache.protect_delivery(row["origin_id"]):
            return await self._deliver_protected(
                descriptor,
                row,
                reuse_type,
                defer_allowed=defer_allowed,
                score=score,
                match_id=match_id,
            )

    async def _deliver_protected(
        self,
        descriptor: _Descriptor,
        row: dict[str, Any],
        reuse_type: ReuseType,
        *,
        defer_allowed: bool,
        score: float | None = None,
        match_id: str | None = None,
    ) -> ToolReuseDecision | None:
        try:
            result = json.loads(row["result_json"])
            if not isinstance(result, dict) or digest(result) != row["result_digest"]:
                raise ReuseConflict("result_digest_mismatch")
            if len(canonical_json(result).encode()) != row["result_size"]:
                raise ReuseConflict("result_size_mismatch")
            reject_sensitive(result)
            execution = json.loads(row["receipt_json"])
            start = ToolTelemetryEvent.model_validate(execution["start"])
            finish = ToolTelemetryEvent.model_validate(execution["finish"])
            if (
                start.event_kind.value != "start"
                or finish.event_kind.value != "finish"
                or start.binding_id != row["binding_id"]
                or finish.binding_id != row["binding_id"]
                or finish.input_digest != row["input_digest"]
                or finish.result_digest != row["result_digest"]
                or finish.result_size_bytes != row["result_size"]
                or finish.tool_name != descriptor.registry.tool_name
                or finish.adapter_id != descriptor.registry.adapter_id
                or finish.adapter_version != descriptor.registry.adapter_version
                or finish.result_schema_version
                != descriptor.registry.result_schema_version
                or start.action_id != finish.action_id
                or start.execution_attempt != finish.execution_attempt
            ):
                raise ReuseConflict("origin_execution_receipt_mismatch")
            observed = datetime.fromisoformat(row["observed_at"])
            expires = datetime.fromisoformat(row["expires_at"])
            initial_expires = datetime.fromisoformat(row["initial_expires_at"])
            last_used = datetime.fromisoformat(row["last_used_at"])
            ttl_limit = min(
                v
                for v in (
                    descriptor.registry.max_ttl_seconds
                    or descriptor.registry.default_ttl_seconds,
                    descriptor.registry.scope_max_ttl_seconds,
                )
                if v is not None
            )
            if (
                not observed.tzinfo
                or not expires.tzinfo
                or not initial_expires.tzinfo
                or not last_used.tzinfo
                or not (
                    observed
                    < initial_expires
                    <= observed + timedelta(seconds=ttl_limit)
                )
                or expires
                not in (
                    initial_expires,
                    last_used + (initial_expires - observed),
                )
            ):
                raise ReuseConflict("origin_freshness_metadata_mismatch")
            source = json.loads(row["descriptor_json"])
            current = json.loads(descriptor.canonical_json)
            if score is None and source != current:
                raise ReuseConflict("descriptor_mismatch")
            if score is not None:
                fields = descriptor.registry.semantic_query_fields
                for value in (source, current):
                    value["arguments"] = {
                        k: v for k, v in value["arguments"].items() if k not in fields
                    }
                if source != current:
                    raise ReuseConflict("hard_scope_mismatch")
            adapter = get_adapter(
                descriptor.registry.tool_name, descriptor.registry.adapter_id
            )
            if adapter and not adapter.validate_result(result, finish):
                raise ReuseConflict("observation_schema_mismatch")
            if isinstance(adapter, TerminalUrlFetchAdapter):
                result = adapter.adapt_result(result)
                result["command"] = descriptor.request.arguments["command"]
            provenance = ResultProvenance(
                reuse_type=reuse_type,
                match_kind=ReuseMatchKind.SEMANTIC
                if score is not None
                else ReuseMatchKind.EXACT,
                observed_at=datetime.fromisoformat(row["observed_at"]),
                result_schema_version=descriptor.registry.result_schema_version,
                source_query_digest=row["input_digest"],
                original_size=row["result_size"],
                returned_size=len(canonical_json(result).encode()),
                truncation_policy="whole_observation",
                similarity_score=score,
                semantic_match_id=match_id,
                expires_at=datetime.fromisoformat(row["expires_at"]),
                result_digest=digest(result),
                origin_id=row["origin_id"],
            )
            budget = descriptor.request.output_budget_bytes
            provider_provenance = {
                k: provenance.model_dump(mode="json")[k]
                for k in ("reuse_type", "observed_at", "result_schema_version")
            }
            suffix = (
                "\n[FlowPilot reuse provenance: "
                + canonical_json(provider_provenance)
                + "]"
            )
            delivered = dict(result)
            delivered["content"] = [
                *result.get("content", []),
                {"type": "text", "cache_prompt": False, "text": suffix},
            ]
            delivery_size = len(canonical_json(delivered).encode())
            if budget is not None and delivery_size > budget:
                self._counters["budget_exceeded"] += 1
                return None
            # Renew only after validation and budget acceptance. The atomic
            # touch rejects results that expired or were revoked during delivery.
            expires = await self._cache.touch(row["origin_id"])
            if expires is None or expires <= datetime.now(UTC):
                return None
            provenance = provenance.model_copy(update={"expires_at": expires})
            return self._decision(
                descriptor,
                ReuseDecisionKind.DEFER_WITH_CACHED_RESULT
                if defer_allowed
                else ReuseDecisionKind.SYNC_WITH_REUSED_RESULT,
                result=result,
                provenance=provenance,
                match_kind=provenance.match_kind,
                similarity_score=score,
                semantic_match_id=match_id,
            )
        except (ValueError, TypeError, KeyError) as exc:
            self._counters["invalid_result"] += 1
            logger.warning("reuse delivery rejected: %s", type(exc).__name__)
            return None

    @staticmethod
    def _decision(
        descriptor: _Descriptor, kind: ReuseDecisionKind, **kwargs: Any
    ) -> ToolReuseDecision:
        adapter = get_adapter(
            descriptor.registry.tool_name, descriptor.registry.adapter_id
        )
        return ToolReuseDecision(
            protocol_version=descriptor.request.protocol_version,
            decision=kind,
            descriptor_digest=descriptor.digest,
            input_digest=descriptor.query_digest,
            input_schema_digest=descriptor.registry.input_schema_digest,
            adapter_id=descriptor.registry.adapter_id,
            adapter_version=descriptor.registry.adapter_version,
            result_schema_version=descriptor.registry.result_schema_version,
            executor_kind=adapter.executor_kind if adapter else "openhands_local",
            **kwargs,
        )

    def _waiting(self, binding: _Binding, follower: _Follower) -> ToolReuseDecision:
        return self._decision(
            follower.descriptor,
            ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
            if follower.defer_allowed
            else ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT,
            binding_id=binding.binding_id,
            match_kind=follower.match_kind,
            similarity_score=follower.similarity_score,
            semantic_match_id=follower.semantic_match_id,
            leader_estimated_remaining_ms=binding.estimated_remaining_ms,
            retry_after_ms=max(
                0,
                int(
                    (binding.lease_deadline - datetime.now(UTC)).total_seconds() * 1000
                ),
            ),
        )

    async def record_execution(
        self, event: ToolTelemetryEvent, *, trusted_context: TrustedContext
    ) -> tuple[Any, bool]:
        if self._frontier is None:
            raise ReuseConflict("frontier execution authority is unavailable")
        async with self._lock:
            await self._expire_locked()
            binding = self._require_binding(event.binding_id or "", trusted_context)
            identity = ToolReuseIdentity(
                job_id=event.job_id,
                line_id=event.line_id,
                tail_request_id=event.tail_request_id,
                llm_call_id=event.llm_call_id,
                tool_call_id=event.tool_call_id,
                action_id=event.action_id,
            )
            self._require_leader(binding, identity)
            descriptor = binding.descriptor
            adapter = get_adapter(event.tool_name, descriptor.registry.adapter_id)
            expected = (
                descriptor.query_digest,
                descriptor.registry.tool_name,
                descriptor.registry.adapter_id,
                descriptor.registry.adapter_version,
                descriptor.registry.result_schema_version,
                adapter.executor_kind if adapter else "openhands_local",
            )
            actual = (
                event.input_digest,
                event.tool_name,
                event.adapter_id,
                event.adapter_version,
                event.result_schema_version,
                event.executor_kind,
            )
            if actual != expected:
                raise ReuseConflict(
                    "execution credentials do not match binding input/adapter"
                )
            if binding.execution and (
                binding.execution.action_id != event.action_id
                or binding.execution.execution_attempt != event.execution_attempt
            ):
                raise ReuseConflict("ExecutionRef conflicts with bound START")
            if event.event_kind.value == "start":
                if binding.execution and binding.execution != event:
                    raise ReuseConflict("START conflicts with immutable ExecutionRef")
            elif binding.execution is None:
                raise ReuseConflict("missing accepted reuse START")
            if binding.status != "running":
                raise ReuseConflict("binding is not running")
            result = await self._frontier.record_tool_event(event)
            if event.event_kind.value == "start":
                binding.execution = event.model_copy(deep=True)
                binding.leader = identity
            elif event.event_kind.value in {"fail", "cancel"}:
                await self._fail_locked(binding, event.event_kind.value)
            return result

    async def publish(
        self, report: LeaderResultPublish, *, trusted_context: TrustedContext
    ) -> ToolReuseDecision:
        async with self._lock:
            receipt = await self._cache.receipt(report.binding_id)
            if receipt is not None:
                fingerprint = self._publication_fingerprint(
                    report,
                    ToolRegistryEntry.model_validate_json(receipt["registry_json"]),
                )
                if json.loads(receipt["context_json"]) != {
                    "deployment_id": trusted_context.deployment_id,
                    "namespace_id": trusted_context.namespace_id,
                } or json.loads(receipt["identity_json"]) != report.identity.model_dump(
                    mode="json"
                ):
                    raise ReuseConflict("publication identity/namespace mismatch")
                if receipt["fingerprint"] != fingerprint:
                    await self._cache.revoke(receipt["origin_id"])
                    raise ReuseConflict(
                        "publication fingerprint conflict; origin revoked"
                    )
                if datetime.fromisoformat(receipt["retry_until"]) <= datetime.now(UTC):
                    raise ReuseConflict("publication retry window expired")
                binding = self._bindings.get(report.binding_id)
                if binding is not None:
                    binding.status, binding.origin_id = "complete", receipt["origin_id"]
                    self._descriptor_bindings.pop(binding.descriptor.digest, None)
                if self._frontier is not None:
                    await self._frontier.release_reuse_receipt(report.binding_id)
                return self._publication_ack(receipt, report.protocol_version)
            await self._expire_locked()
            binding = self._require_binding(report.binding_id, trusted_context)
            self._require_leader(binding, report.identity)
            try:
                if (
                    binding.status != "running"
                    or binding.execution is None
                    or self._frontier is None
                ):
                    raise ReuseConflict(
                        "publication requires running leader and accepted START"
                    )
                line = await self._frontier.line_snapshot(
                    report.identity.job_id, report.identity.line_id
                )
                if (
                    line["tail_request_id"] != report.identity.tail_request_id
                    or line["llm_call_id"] != report.identity.llm_call_id
                ):
                    raise ReuseConflict("first publication requires active tail")
                (
                    start,
                    finish,
                    observed_at,
                ) = await self._frontier.reuse_execution_receipt(
                    binding.binding_id, report.identity, report.execution_attempt
                )
                if start != binding.execution or (start.event_id, finish.event_id) != (
                    report.start_event_id,
                    report.finish_event_id,
                ):
                    raise ReuseConflict("publication ExecutionRef mismatch")
                if (
                    finish.action_id != report.identity.action_id
                    or finish.llm_call_id != report.identity.llm_call_id
                ):
                    raise ReuseConflict("publication Action or LLM identity mismatch")
                result_json = canonical_json(report.result)
                size = len(result_json.encode())
                result_digest = digest(report.result)
                if (
                    report.input_digest != binding.descriptor.query_digest
                    or report.input_digest != finish.input_digest
                    or report.result_digest != result_digest
                    or finish.result_digest != result_digest
                    or report.result_size_bytes != size
                    or finish.result_size_bytes != size
                    or report.result_schema_version
                    != binding.descriptor.registry.result_schema_version
                    or finish.error_class is not None
                ):
                    raise ReuseConflict(
                        "publication input/result digest, size or schema mismatch"
                    )
                reject_sensitive(report.result)
                adapter = get_adapter(
                    binding.descriptor.registry.tool_name,
                    binding.descriptor.registry.adapter_id,
                )
                if adapter and not adapter.validate_result(report.result, finish):
                    raise ReuseConflict("adapter rejected leader result")
                if (
                    isinstance(adapter, TerminalUrlFetchAdapter)
                    and report.result.get("command")
                    != binding.descriptor.request.arguments["command"]
                ):
                    raise ReuseConflict("terminal result command does not match Action")
                if adapter and adapter.executor_kind == "isolated_curl_argv":
                    arguments = json.loads(binding.descriptor.canonical_json)[
                        "arguments"
                    ]
                    if finish.final_url_digest != digest(arguments["url"]):
                        raise ReuseConflict("URL receipt does not match requested URL")
                if size > binding.descriptor.registry.max_result_bytes:
                    raise ReuseConflict("result exceeds max_result_bytes")
                registry = binding.descriptor.registry
                fingerprint = self._publication_fingerprint(report, registry)
                ttl = min(
                    v
                    for v in (
                        report.ttl_seconds or registry.default_ttl_seconds,
                        registry.max_ttl_seconds or registry.default_ttl_seconds,
                        registry.scope_max_ttl_seconds,
                    )
                    if v is not None
                )
                now = datetime.now(UTC)
                publication = {
                    "binding_id": binding.binding_id,
                    "fingerprint": fingerprint,
                    "origin_id": "origin-" + uuid4().hex,
                    "identity_json": canonical_json(
                        report.identity.model_dump(mode="json")
                    ),
                    "context_json": canonical_json(
                        {
                            "deployment_id": trusted_context.deployment_id,
                            "namespace_id": trusted_context.namespace_id,
                        }
                    ),
                    "descriptor_json": binding.descriptor.canonical_json,
                    "registry_json": canonical_json(registry.model_dump(mode="json")),
                    "input_digest": report.input_digest,
                    "result_digest": result_digest,
                    "result_size": size,
                    "cacheable": int(report.cacheable),
                    "observed_at": observed_at.isoformat(),
                    "expires_at": (observed_at + timedelta(seconds=ttl)).isoformat(),
                    "committed_at": now.isoformat(),
                    "retry_until": (
                        now + timedelta(seconds=self._cache.retry_window_seconds)
                    ).isoformat(),
                    "status": "committed",
                }
                publication = await self._cache.commit(
                    publication,
                    result_json,
                    {
                        "start": start.model_dump(mode="json"),
                        "finish": finish.model_dump(mode="json"),
                        "upstream_completeness": "unknown"
                        if adapter and adapter.executor_kind == "mcp"
                        else "observed",
                    },
                    exact_key=binding.descriptor.digest,
                    hard_scope_digest=binding.descriptor.hard_scope_digest,
                    semantic_text=binding.descriptor.semantic_text,
                )
            except Exception:
                # An uncommitted failure releases followers, never a fake success.
                if await self._cache.receipt(report.binding_id) is None:
                    await self._fail_locked(binding, "publication_failed")
                raise
            binding.status = "complete"
            binding.origin_id = publication["origin_id"]
            binding.completed_at = datetime.now(UTC)
            self._descriptor_bindings.pop(binding.descriptor.digest, None)
            await self._frontier.release_reuse_receipt(binding.binding_id)
            if self._max_payload_bytes is not None:
                await self._maintain_locked(self._max_payload_bytes)
            if report.cacheable and binding.descriptor.semantic_text is not None:
                task = asyncio.create_task(
                    self._index_origin(
                        publication["origin_id"], binding.descriptor.semantic_text
                    )
                )
                self._index_tasks.add(task)
                task.add_done_callback(self._index_tasks.discard)
            return self._publication_ack(publication, report.protocol_version)

    @staticmethod
    def _publication_fingerprint(
        report: LeaderResultPublish, registry: ToolRegistryEntry
    ) -> str:
        value = report.model_dump(mode="json")
        value["ttl_seconds"] = report.ttl_seconds or registry.default_ttl_seconds
        return digest(value)

    @staticmethod
    def _publication_ack(receipt: dict[str, Any], version: Any) -> ToolReuseDecision:
        return ToolReuseDecision(
            protocol_version=version,
            decision=ReuseDecisionKind.EXECUTE_LOCALLY,
            binding_id=receipt["binding_id"],
            reason="publication_receipt",
            publication={
                k: receipt[k]
                for k in (
                    "origin_id",
                    "observed_at",
                    "expires_at",
                    "result_digest",
                    "result_size",
                    "status",
                )
            },
        )

    async def _index_origin(self, origin_id: str, text: str) -> None:
        if self._embedder is None:
            return
        try:
            vector = (await asyncio.wait_for(self._embedder.embed([text]), 5.0))[0]
            self._validate_vector(vector)
            await self._cache.vector(origin_id, text, self._embedder.index_id, vector)
        except Exception as exc:
            self._counters["semantic_index_failed"] += 1
            logger.warning("semantic index failed: %s", type(exc).__name__)

    async def rebuild_vectors(self) -> int:
        if self._embedder is None:
            return 0
        rows = await self._cache.missing_vectors(self._embedder.index_id)
        for row in rows:
            await self._index_origin(row["origin_id"], row["semantic_text"])
        return len(rows)

    async def poll(
        self,
        binding_id: str,
        identity: ToolReuseIdentity,
        *,
        trusted_context: TrustedContext,
        defer_allowed: bool | None = None,
    ) -> ToolReuseDecision:
        async with self._lock:
            await self._expire_locked()
            binding = self._require_binding(binding_id, trusted_context)
            follower = binding.followers.get(tool_call_key(identity))
            if follower is None:
                raise ReuseConflict("identity is not a follower")
            if (
                follower.identity.action_id
                and follower.identity.action_id != identity.action_id
            ):
                raise ReuseConflict("follower Action mismatch")
            follower.identity = identity
            if binding.status == "running":
                # A durable commit may precede memory recovery.
                receipt = await self._cache.receipt(binding_id)
                if receipt:
                    binding.status, binding.origin_id = "complete", receipt["origin_id"]
                else:
                    return self._waiting(binding, follower)
            if binding.origin_id:
                row = await self._cache.result(binding.origin_id)
                if row and (
                    follower.match_kind == ReuseMatchKind.EXACT
                    or self._semantic_enabled(follower.descriptor)
                ):
                    decision = await self._delivery(
                        follower.descriptor,
                        row,
                        ReuseType.INFLIGHT,
                        defer_allowed=follower.defer_allowed
                        and defer_allowed is not False,
                        score=follower.similarity_score
                        if follower.match_kind == ReuseMatchKind.SEMANTIC
                        else None,
                        match_id=follower.semantic_match_id,
                    )
                    if decision:
                        return decision.model_copy(update={"binding_id": binding_id})
            binding.followers.pop(tool_call_key(identity), None)
            return self._decision(
                follower.descriptor,
                ReuseDecisionKind.EXECUTE_LOCALLY,
                reason="result_expired_revoked_or_unavailable",
            )

    async def poll_deferred(
        self,
        binding_id: str,
        request: ToolReuseResolveRequest,
        *,
        trusted_context: TrustedContext,
        exact_only: bool = False,
    ) -> ToolReuseDecision:
        if exact_only and request.protocol_version != REUSE_PROTOCOL_VERSION:
            raise ReuseConflict("DCS accepts exact reuse only")
        descriptor = self._descriptor(request, trusted_context)
        binding = self._require_binding(binding_id, trusted_context)
        follower = binding.followers.get(tool_call_key(request.identity))
        if (
            descriptor is None
            or follower is None
            or descriptor.digest != follower.descriptor.digest
        ):
            raise ReuseConflict("deferred poll descriptor mismatch")
        if exact_only and follower.match_kind != ReuseMatchKind.EXACT:
            raise ReuseConflict("DCS accepts exact bindings only")
        return await self.poll(
            binding_id,
            request.identity,
            trusted_context=trusted_context,
            defer_allowed=True,
        )

    async def progress(
        self, report: LeaderProgressReport, *, trusted_context: TrustedContext
    ) -> bool:
        async with self._lock:
            await self._expire_locked()
            binding = self._require_binding(report.binding_id, trusted_context)
            self._require_leader(binding, report.identity)
            if binding.status != "running":
                raise ReuseConflict("binding is not running")
            if report.sequence == binding.progress_sequence:
                if (report.observed_at, report.estimated_remaining_ms) == (
                    binding.last_progress_at,
                    binding.estimated_remaining_ms,
                ):
                    return True
                raise ReuseConflict("conflicting progress")
            if report.sequence < binding.progress_sequence:
                raise ReuseConflict("progress sequence regressed")
            binding.progress_sequence = report.sequence
            binding.last_progress_at = report.observed_at
            binding.estimated_remaining_ms = report.estimated_remaining_ms
            return False

    async def fail(
        self, report: BindingFailureReport, *, trusted_context: TrustedContext
    ) -> None:
        async with self._lock:
            binding = self._require_binding(report.binding_id, trusted_context)
            self._require_leader(binding, report.identity)
            if binding.status == "failed" and binding.error_class == report.error_class:
                return
            if binding.status != "running":
                raise ReuseConflict("binding is not running")
            await self._fail_locked(binding, report.error_class)

    async def _fail_locked(self, binding: _Binding, reason: str) -> None:
        binding.status, binding.error_class = "failed", reason
        binding.completed_at = datetime.now(UTC)
        self._descriptor_bindings.pop(binding.descriptor.digest, None)
        if self._frontier:
            await self._frontier.release_reuse_receipt(binding.binding_id)

    async def cancel_follower(
        self, cancellation: FollowerCancellation, *, trusted_context: TrustedContext
    ) -> None:
        async with self._lock:
            binding = self._require_binding(cancellation.binding_id, trusted_context)
            if tool_call_key(binding.leader) == tool_call_key(cancellation.identity):
                raise ReuseConflict("leader cannot cancel as follower")
            binding.followers.pop(tool_call_key(cancellation.identity), None)

    async def _expire_locked(self) -> None:
        now = datetime.now(UTC)
        for identifier, binding in list(self._bindings.items()):
            if binding.status == "running" and binding.lease_deadline <= now:
                receipt = await self._cache.receipt(identifier)
                if receipt is not None:
                    binding.status, binding.origin_id = "complete", receipt["origin_id"]
                    binding.completed_at = now
                    self._descriptor_bindings.pop(binding.descriptor.digest, None)
                    if self._frontier:
                        await self._frontier.release_reuse_receipt(identifier)
                else:
                    await self._fail_locked(binding, "lease_expired")
            if (
                binding.completed_at
                and binding.completed_at
                + timedelta(seconds=self._terminal_retention_seconds)
                <= now
            ):
                self._bindings.pop(identifier)

    def _require_binding(self, binding_id: str, context: TrustedContext) -> _Binding:
        binding = self._bindings.get(binding_id)
        if binding is None or binding.descriptor.context != context:
            raise ReuseConflict("unknown binding in namespace")
        return binding

    @staticmethod
    def _require_leader(binding: _Binding, identity: ToolReuseIdentity) -> None:
        if tool_call_key(binding.leader) != tool_call_key(identity):
            raise ReuseConflict("identity does not own leader binding")
        if binding.leader.action_id and binding.leader.action_id != identity.action_id:
            raise ReuseConflict("Action does not own leader binding")

    async def report_false_reuse(self, report: FalseReuseReport) -> bool:
        return await self._cache.feedback(
            report.semantic_match_id, report.reason, report.evidence_digest
        )

    async def update_semantic_policy(
        self, update: SemanticReusePolicyUpdate
    ) -> dict[str, Any]:
        async with self._lock:
            if update.expected_version != self._semantic_policy_version:
                raise ReuseConflict("semantic policy version mismatch")
            if update.tool_name not in self._registry:
                raise ReuseConflict("unknown Tool")
            if update.enabled:
                if not self._registry[update.tool_name].semantic_reuse_enabled:
                    raise ReuseConflict("registry does not allow semantic reuse")
                self._semantic_disabled_tools.discard(update.tool_name)
            else:
                self._semantic_disabled_tools.add(update.tool_name)
            self._semantic_policy_version = update.version
            return self._semantic_policy_snapshot()

    def _semantic_policy_snapshot(self) -> dict[str, Any]:
        return {
            "policy_version": self._semantic_policy_version,
            "disabled_tools": sorted(self._semantic_disabled_tools),
        }

    async def maintenance(
        self, *, max_payload_bytes: int | None = None
    ) -> dict[str, int]:
        try:
            async with self._lock:
                await self._expire_locked()
                stats = await self._maintain_locked(
                    max_payload_bytes
                    if max_payload_bytes is not None
                    else self._max_payload_bytes
                )
            self._counters["maintenance_expired_deleted"] += stats["expired_deleted"]
            return stats
        except Exception:
            self._counters["maintenance_failed"] += 1
            logger.exception("Tool cache maintenance failed")
            raise

    async def _maintain_locked(self, capacity: int | None) -> dict[str, int]:
        # Follower polls are retryable, without a delivery ACK. Retain the
        # publication for the binding's existing retry lifetime, including
        # non-cacheable results. DCS independently owns a full WAL payload copy.
        protected = frozenset(
            b.origin_id for b in self._bindings.values() if b.origin_id and b.followers
        )
        stats = await self._cache.maintenance(
            max_payload_bytes=capacity,
            index_id=self._embedder.index_id if self._embedder else None,
            protected_origins=protected,
        )
        self._maintenance_stats = stats
        return stats

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            await self._expire_locked()
            return {
                "registry_tools": sorted(self._registry),
                "registry": [
                    entry.model_dump(mode="json") for entry in self._registry.values()
                ],
                "cache_entries": await self._cache.count(),
                "retention": {
                    "policy": "saved-work-per-byte",
                    **self._maintenance_stats,
                },
                "semantic": {
                    "embedding_index_id": self._embedder.index_id
                    if self._embedder
                    else None,
                    "status": self._embedding_status,
                    **self._semantic_policy_snapshot(),
                    "counters": {**self._counters, **await self._cache.audit_stats()},
                },
                "bindings": [
                    {
                        "binding_id": b.binding_id,
                        "descriptor_digest": b.descriptor.digest,
                        "status": b.status,
                        "follower_count": len(b.followers),
                        "lease_deadline": b.lease_deadline.isoformat(),
                        "progress_sequence": b.progress_sequence,
                        "estimated_remaining_ms": b.estimated_remaining_ms,
                    }
                    for b in self._bindings.values()
                ],
            }

    async def close(self) -> None:
        if self._index_tasks:
            await asyncio.gather(*self._index_tasks)


# Internal aliases retained for context digest callers, not a protocol fallback.
_canonical_json = canonical_json
