"""vLLM KV control v1 client and factual response retention policy.

No restore method, restore queue, GPU-ready wait, or token-to-byte conversion.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Literal
from uuid import uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.observability.trace import TraceRecorder
from flowpilot.protocol import RequestIdentity
from flowpilot.scheduling.cost import OfflineCostModel
from flowpilot.scheduling.projection import ProjectionCalculator

logger = logging.getLogger(__name__)
Action = Literal["KEEP", "OFFLOAD", "DROP"]


class RetentionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    enabled: bool = False
    owner_scope: str = Field(default="flowpilot-local", min_length=1)
    timeout_seconds: float = Field(default=1.0, gt=0)
    refresh_seconds: float = Field(default=1.0, gt=0)
    keep_horizon_seconds: float = Field(default=1.0, ge=0)
    gpu_free_reserve_allocations: int = Field(default=128, ge=0)
    gpu_seconds_per_gib_second: float = Field(default=1.0, ge=0)
    cpu_seconds_per_gib_second: float = Field(default=0.01, ge=0)


class EngineIdentity(BaseModel):
    engine_epoch: str
    identity_digest: str | None = None


class _ResolvedDescriptor(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    descriptor_id: str
    engine: EngineIdentity
    binding: dict[str, Any]
    expires_at_monotonic: float = Field(ge=0)


class _ResolveResult(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    binding: dict[str, Any]
    status: Literal["PENDING", "READY", "UNKNOWN_BINDING", "DESCRIPTOR_EXPIRED"]
    descriptors: tuple[_ResolvedDescriptor, ...]
    observed_at_monotonic: float = Field(ge=0)


class Capabilities(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    schema_version: Literal[1]
    engine: EngineIdentity
    metadata_ttl_seconds: float | None = Field(default=None, gt=0)
    descriptor_query: bool
    gpu_retention_preference: bool
    cpu_backed_eviction_preference: bool
    safe_direct_drop: bool
    cpu_store: bool
    engine_cpu_reuse: bool
    restore_cost_estimate: bool
    continuation_proof: bool


class PrefixObservation(BaseModel):
    schema_version: Literal[1]
    descriptor_id: str
    engine_epoch: str
    state_version: int = Field(ge=0)
    event_seq: int = Field(ge=0)
    prefix_token_count: int = Field(ge=0)
    gpu_ready_tokens: int = Field(ge=0)
    recoverable_tokens: int | None = Field(ge=0)
    cpu_standalone_tokens: int | None = Field(default=None, ge=0)
    lookup_state: Literal["COMPLETE", "PENDING", "UNSUPPORTED"]
    reuse_basis: Literal["DESCRIPTOR_ONLY", "ASSUMED_CONTINUATION"]
    effective_policy_version: int | None = None
    effective_policy_action: Action | None = None
    gpu_retention_bytes: int | None = Field(default=None, ge=0)
    offload_target_tokens: int | None = Field(default=None, ge=0)
    offload_object_bytes: int | None = Field(default=None, ge=0)


@dataclass(frozen=True)
class RetentionDecision:
    action: Action | None
    reason: str


def choose_retention(
    *,
    config: RetentionConfig,
    capabilities: Capabilities,
    observation: PrefixObservation,
    phase: str,
    need_in_seconds: float | None,
    free_gpu_allocations: int,
    cost_model: OfflineCostModel | None = None,
    remaining_slo_seconds: float | None = None,
) -> RetentionDecision:
    if phase == "TERMINAL" or (
        observation.lookup_state == "COMPLETE" and observation.recoverable_tokens == 0
    ):
        return RetentionDecision(
            "DROP" if capabilities.safe_direct_drop else None,
            "terminal_or_no_recoverable_prefix",
        )
    near = phase == "READY" or (
        need_in_seconds is not None and need_in_seconds <= config.keep_horizon_seconds
    )
    pressure = free_gpu_allocations <= config.gpu_free_reserve_allocations
    gap = (
        0.0
        if phase == "READY"
        else max(0.0, need_in_seconds)
        if need_in_seconds is not None
        else None
    )
    if cost_model is not None and gap is not None:
        decision = _cost_retention(
            config,
            capabilities,
            observation,
            cost_model,
            gap,
            remaining_slo_seconds,
            pressure,
        )
        if decision is not None:
            return decision
    if near and not pressure and capabilities.gpu_retention_preference:
        return RetentionDecision("KEEP", "fallback_cost_unknown:near_factual_successor")
    if (
        capabilities.cpu_backed_eviction_preference
        and capabilities.cpu_store
        and capabilities.engine_cpu_reuse
    ):
        return RetentionDecision(
            "OFFLOAD",
            "fallback_cost_unknown:"
            + ("gpu_pressure" if pressure else "waiting_for_successor"),
        )
    if capabilities.gpu_retention_preference:
        return RetentionDecision("KEEP", "cpu_retention_unsupported")
    return RetentionDecision(None, "retention_unsupported")


def _cost_retention(
    config: RetentionConfig,
    caps: Capabilities,
    obs: PrefixObservation,
    model: OfflineCostModel,
    gap: float,
    remaining: float | None,
    pressure: bool,
) -> RetentionDecision | None:
    if caps.engine.identity_digest != model.engine_identity_digest:
        return None
    # Response-time costs concern the known prefix plus one continuation token.
    # Tool result length and future rendering remain unknown until arrival.
    p = obs.prefix_token_count + 1
    cold = model.prefill_seconds(p, 0)
    keep = model.prefill_seconds(p, obs.gpu_ready_tokens)
    if cold is None or keep is None or obs.gpu_retention_bytes is None:
        return None
    options: list[tuple[Action, float, float]] = []
    if caps.safe_direct_drop:
        options.append(("DROP", cold, cold))
    if caps.gpu_retention_preference and obs.gpu_ready_tokens:
        carrying = (
            obs.gpu_retention_bytes / 2**30 * gap * config.gpu_seconds_per_gib_second
        )
        options.append(("KEEP", keep, keep + carrying * (2 if pressure else 1)))
    if (
        caps.cpu_store
        and caps.cpu_backed_eviction_preference
        and caps.engine_cpu_reuse
        and model.restore is not None
        and obs.offload_object_bytes is not None
        and obs.offload_target_tokens
    ):
        after = model.prefill_seconds(p, min(p, obs.offload_target_tokens))
        if after is not None:
            # The native CPU lookup proves readiness independently of GPU
            # residency or an accepted (possibly still pending) OFFLOAD policy.
            cpu_ready = (
                obs.cpu_standalone_tokens is not None
                and obs.cpu_standalone_tokens >= obs.offload_target_tokens
            )
            offload = (
                0.0
                if cpu_ready
                else model.offload.seconds(obs.offload_object_bytes)
                if model.offload is not None
                else None
            )
            restore = model.restore.seconds(obs.offload_object_bytes)
            # Do not assume an unfinished D2H can be consumed at successor arrival.
            if offload is not None and offload <= gap:
                carrying = (
                    obs.offload_object_bytes
                    / 2**30
                    * gap
                    * config.cpu_seconds_per_gib_second
                )
                options.append(
                    ("OFFLOAD", after + restore, after + restore + offload + carrying)
                )
    if not options:
        return None
    budget = remaining - gap if remaining is not None else None
    action, _, _ = min(
        options,
        key=lambda item: (
            max(0.0, item[1] - budget) if budget is not None else 0.0,
            item[2],
        ),
    )
    return RetentionDecision(action, "calibrated_slo_and_capacity:assumed_continuation")


@dataclass
class _Source:
    identity: RequestIdentity
    tail_version: int
    descriptor_id: str
    engine_epoch: str
    # Local clock, converted from the engine's remaining TTL at resolve.
    expires_at_monotonic: float
    expiry_handle: asyncio.TimerHandle | None = None
    operation_id: str | None = None
    action_id: str | None = None
    last_action: Action | None = None
    last_status: str | None = None
    pending_command: dict[str, Any] | None = None
    dirty: bool = True
    observation: PrefixObservation | None = None
    decision_reason: str | None = None
    retry_at: float = 0.0


class RetentionController:
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        config: RetentionConfig,
        frontier: LineTailFrontier,
        projections: ProjectionCalculator,
        recorder: TraceRecorder,
        *,
        api_key: str | None = None,
        cost_model: OfflineCostModel | None = None,
    ) -> None:
        self.client = client
        self.base_url = base_url.removesuffix("/v1") + "/v1/kv"
        self.config = config
        self.frontier = frontier
        self.projections = projections
        self.recorder = recorder
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.capabilities: Capabilities | None = None
        self.status = "unnegotiated"
        self._sources: dict[str, _Source] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._event_refresh_task: asyncio.Task[None] | None = None
        self._events_pending = False
        self._lock = asyncio.Lock()
        self._event_seq = 0
        self._engine_epoch: str | None = None
        self._binding_supported = False
        self._metadata_ttl_seconds: float | None = None
        self._last_error: str | None = None
        self._bound: set[tuple[str, str]] = set()
        self._finished_lines: set[tuple[str, str, int]] = set()
        self._source_expirations: Counter[str] = Counter()
        self.cost_model = cost_model

    def _remove_source(self, source: _Source) -> bool:
        if self._sources.get(source.descriptor_id) is not source:
            return False
        del self._sources[source.descriptor_id]
        if source.expiry_handle is not None:
            source.expiry_handle.cancel()
            source.expiry_handle = None
        return True

    def _clear_sources(self) -> None:
        for source in list(self._sources.values()):
            self._remove_source(source)

    def _expire_source(self, source: _Source, reason: str) -> None:
        # Runs on the event loop without awaiting the RPC lock. Engine expiry
        # owns physical cleanup, including any still-running OFFLOAD or DROP.
        if self._remove_source(source):
            self._source_expirations[reason] += 1

    def _source_live(self, source: _Source) -> bool:
        if monotonic() >= source.expires_at_monotonic:
            self._expire_source(source, "deadline")
        return self._sources.get(source.descriptor_id) is source

    def _register_source(self, source: _Source) -> None:
        prior = self._sources.get(source.descriptor_id)
        if prior is not None:
            if (
                prior.identity != source.identity
                or prior.engine_epoch != source.engine_epoch
                or prior.tail_version != source.tail_version
            ):
                raise ValueError("KV descriptor identity changed")
            prior.expires_at_monotonic = min(
                prior.expires_at_monotonic, source.expires_at_monotonic
            )
            if prior.expiry_handle is not None:
                prior.expiry_handle.cancel()
            source = prior
        self._sources[source.descriptor_id] = source
        if self._source_live(source):
            source.expiry_handle = asyncio.get_running_loop().call_later(
                max(0.0, source.expires_at_monotonic - monotonic()),
                self._expire_source,
                source,
                "deadline",
            )

    async def _rpc(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        response = await self.client.request(
            method,
            self.base_url + path,
            json=body,
            headers=self.headers,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or result.get("schema_version") != 1:
            raise ValueError("incompatible KV control schema")
        return result

    async def negotiate(self) -> None:
        try:
            capability = Capabilities.model_validate(
                await self._rpc("GET", "/capabilities")
            )
            if self._engine_epoch is not None and (
                capability.engine.engine_epoch != self._engine_epoch
            ):
                self._clear_sources()
                self._event_seq = 0
            self._engine_epoch = capability.engine.engine_epoch
            self._binding_supported = capability.descriptor_query
            self._metadata_ttl_seconds = capability.metadata_ttl_seconds
            self.capabilities = capability
            self.status = "supported" if capability.descriptor_query else "unsupported"
            self._last_error = None
        except (httpx.HTTPError, ValueError) as exc:
            self.capabilities = None
            self.status = (
                "unsupported"
                if isinstance(exc, httpx.HTTPStatusError)
                and (exc.response.status_code in {404, 501})
                else "unavailable"
            )
            self._last_error = type(exc).__name__
            if self.status == "unsupported":
                self._binding_supported = False
            await self.recorder.increment("kv_capability_failures")

    def bind(self, body: bytes, identity: RequestIdentity) -> bytes:
        # Binding is ingress metadata, not permission to issue a KV action.
        # A transient control outage must not lose a known engine's association.
        if not self._binding_supported:
            return body
        payload = json.loads(body)
        transfer = dict(payload.get("kv_transfer_params") or {})
        if "kv_control_binding" in transfer:
            raise ValueError("kv_control_binding is owned by FlowPilot")
        transfer["kv_control_binding"] = self._binding(identity)
        payload["kv_transfer_params"] = transfer
        # A closed conversation may be reopened under the same line identity.
        # Its old retirement fact must not terminalize the new incarnation.
        self._finished_lines = {
            item
            for item in self._finished_lines
            if item[:2] != (identity.job_id, identity.line_id)
        }
        self._bound.add((identity.job_id, identity.llm_call_id))
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()

    def _binding(self, identity: RequestIdentity) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "owner_scope": self.config.owner_scope,
            "job_id": identity.job_id,
            "line_id": identity.line_id,
            "request_id": identity.request_id,
            "llm_call_id": identity.llm_call_id,
            "attempt": identity.attempt,
            "context_epoch": identity.context_epoch,
        }

    def finished(self, identity: RequestIdentity, version: int | None) -> None:
        key = (identity.job_id, identity.llm_call_id)
        if key not in self._bound:
            return
        self._bound.discard(key)
        if version is not None:
            task = asyncio.create_task(self._resolve(identity, version))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def forget(self, job_id: str, llm_call_id: str) -> None:
        self._bound.discard((job_id, llm_call_id))

    def line_finished(self, job_id: str, line_id: str, version: int) -> None:
        # Frontier prunes finished lines. Keep only the retirement fact until
        # in-flight descriptor resolution and safe DROP receipts have finished.
        self._finished_lines.add((job_id, line_id, version))
        self.line_changed(job_id, line_id)

    def line_changed(self, job_id: str, line_id: str) -> None:
        for source in self._sources.values():
            if (source.identity.job_id, source.identity.line_id) == (job_id, line_id):
                source.dirty = True
        self._events_pending = True
        if self._event_refresh_task is not None and not self._event_refresh_task.done():
            return
        task = asyncio.create_task(self._refresh_events())
        self._event_refresh_task = task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _refresh_events(self) -> None:
        try:
            while self._events_pending:
                self._events_pending = False
                async with self._lock:
                    await self._refresh_locked(events_only=True)
        except Exception as exc:
            self._last_error = type(exc).__name__
            await self.recorder.increment("kv_refresh_failures")
            logger.warning("KV event refresh failed: %s", type(exc).__name__)

    async def _current(self, identity: RequestIdentity, version: int) -> bool:
        try:
            snapshot = await self.frontier.line_snapshot(
                identity.job_id, identity.line_id
            )
        except FrontierConflict:
            return (identity.job_id, identity.line_id, version) in self._finished_lines
        return (
            snapshot["version"] == version
            and snapshot["tail_request_id"] == identity.tail_request_id
            and snapshot["llm_call_id"] == identity.llm_call_id
            and snapshot["phase"] != "ACTIVE"
        )

    async def _resolve(self, identity: RequestIdentity, version: int) -> None:
        epoch = self._engine_epoch
        try:
            # Bound retries by the engine's advertised metadata lifetime, not
            # by one transport timeout. Older engines retain the single window.
            async with asyncio.timeout(
                self._metadata_ttl_seconds or self.config.timeout_seconds
            ):
                while self._engine_epoch == epoch and await self._current(
                    identity, version
                ):
                    resolve_started = monotonic()
                    try:
                        result = _ResolveResult.model_validate(
                            await self._rpc("POST", "/resolve", self._binding(identity))
                        )
                    except httpx.HTTPError as exc:
                        if isinstance(exc, httpx.HTTPStatusError) and (
                            exc.response.status_code < 500
                            or exc.response.status_code == 501
                        ):
                            raise
                        self._last_error = type(exc).__name__
                        await self.recorder.increment("kv_resolution_retries")
                        await asyncio.sleep(self.config.refresh_seconds)
                        continue
                    if result.binding != self._binding(identity):
                        raise ValueError("KV resolve binding mismatch")
                    if result.status != "PENDING":
                        break
                    await asyncio.sleep(self.config.refresh_seconds)
                else:
                    return
            if result.status != "READY":
                await self.recorder.increment("kv_resolve_" + result.status.lower())
                return
            async with self._lock:
                if self._engine_epoch != epoch or not await self._current(
                    identity, version
                ):
                    return
                for descriptor in result.descriptors:
                    if descriptor.binding != self._binding(identity):
                        raise ValueError("KV descriptor binding mismatch")
                    if descriptor.engine.engine_epoch != epoch:
                        raise ValueError("KV resolve engine epoch mismatch")
                    source = _Source(
                        identity,
                        version,
                        descriptor.descriptor_id,
                        descriptor.engine.engine_epoch,
                        # Subtract within the engine clock domain, then anchor
                        # at RPC start so transport latency cannot extend TTL.
                        resolve_started
                        + max(
                            0.0,
                            descriptor.expires_at_monotonic
                            - result.observed_at_monotonic,
                        ),
                    )
                    self._register_source(source)
                await self._refresh_locked(events_only=True)
        except Exception as exc:
            self._last_error = type(exc).__name__
            await self.recorder.increment("kv_resolution_failures")
            logger.warning("KV resolve failed: %s", type(exc).__name__)

    async def refresh(self) -> None:
        async with self._lock:
            await self._refresh_locked(events_only=True)

    async def _refresh_locked(self, *, events_only: bool = False) -> None:
        for source in list(self._sources.values()):
            self._source_live(source)
        capability = self.capabilities
        if capability is None or not capability.descriptor_query or not self._sources:
            return
        telemetry = await self._rpc(
            "POST",
            "/telemetry",
            {
                "schema_version": 1,
                "owner_scope": self.config.owner_scope,
                "expected_engine_epoch": capability.engine.engine_epoch,
                "after_event_seq": self._event_seq,
            },
        )
        if telemetry["engine_epoch"] != capability.engine.engine_epoch:
            raise ValueError("KV telemetry epoch mismatch")
        if self.capabilities is None or (
            self.capabilities.engine.engine_epoch != capability.engine.engine_epoch
        ):
            return
        for event in telemetry.get("events", ()):
            if (
                event["kind"] == "DESCRIPTOR_EXPIRED"
                and event["engine_epoch"] == capability.engine.engine_epoch
                and event["owner_scope"] == self.config.owner_scope
                and self._event_seq < event["event_seq"] <= telemetry["event_seq"]
            ):
                source = self._sources.get(event["descriptor_id"])
                if source is not None and source.engine_epoch == event["engine_epoch"]:
                    self._expire_source(source, "engine_event")
        self._event_seq = telemetry["event_seq"]
        for source in list(self._sources.values()):
            if source.engine_epoch != capability.engine.engine_epoch or not (
                await self._current(source.identity, source.tail_version)
            ):
                self._remove_source(source)
                continue
            try:
                query = not events_only or source.dirty
                # A concurrent Tool event can dirty the source during an RPC.
                source.dirty = False
                await self._refresh_source(
                    source,
                    telemetry["free_gpu_allocations"],
                    query=query,
                )
            except httpx.HTTPStatusError as exc:
                source.dirty = True
                if exc.response.status_code in {409, 410}:
                    self._remove_source(source)
                await self.recorder.increment("kv_policy_http_failures")
                self._last_error = f"HTTP_{exc.response.status_code}"
            except (httpx.HTTPError, ValueError) as exc:
                source.dirty = True
                self._last_error = type(exc).__name__
                await self.recorder.increment("kv_policy_refresh_failures")
                logger.warning("KV source refresh failed: %s", type(exc).__name__)

    async def _refresh_source(
        self, source: _Source, free: int, *, query: bool = True
    ) -> None:
        if not self._source_live(source):
            return
        finished = (
            source.identity.job_id,
            source.identity.line_id,
            source.tail_version,
        ) in self._finished_lines
        common = {
            "schema_version": 1,
            "owner_scope": self.config.owner_scope,
            "expected_engine_epoch": source.engine_epoch,
        }
        if source.pending_command is not None:
            receipt = await self._rpc("POST", "/apply", source.pending_command)
            if not self._source_live(source):
                return
            await self._record_receipt(source, receipt)
            source.pending_command = None
        if source.operation_id is not None:
            receipt = await self._rpc(
                "POST",
                "/status",
                {
                    **common,
                    "operation_id": source.operation_id,
                },
            )
            if not self._source_live(source):
                return
            await self._record_receipt(source, receipt)
            if source.last_status == "ACCEPTED" and not finished:
                return
        if not self._source_live(source):
            return
        if query or source.observation is None:
            source.observation = PrefixObservation.model_validate(
                await self._rpc(
                    "POST", "/query", {**common, "descriptor_id": source.descriptor_id}
                )
            )
            query = True
        if not self._source_live(source):
            return
        observation = source.observation
        if (
            observation.descriptor_id != source.descriptor_id
            or observation.engine_epoch != source.engine_epoch
        ):
            raise ValueError("KV query identity mismatch")
        finished = (
            source.identity.job_id,
            source.identity.line_id,
            source.tail_version,
        ) in self._finished_lines
        projection = (
            None
            if finished
            else await self.projections.for_line(
                source.identity.job_id, source.identity.line_id
            )
        )
        snapshot = (
            {"phase": "TERMINAL"}
            if finished
            else (
                await self.frontier.line_snapshot(
                    source.identity.job_id, source.identity.line_id
                )
            )
        )
        if not self._source_live(source):
            return
        decision = choose_retention(
            config=self.config,
            capabilities=Capabilities.model_validate(self.capabilities),
            observation=observation,
            phase=snapshot["phase"],
            need_in_seconds=(projection.t_need - datetime.now(UTC)).total_seconds()
            if projection is not None and projection.t_need is not None
            else None,
            free_gpu_allocations=free,
            cost_model=self.cost_model,
            remaining_slo_seconds=projection.deadline_slack_ms / 1000
            if projection is not None and projection.deadline_slack_ms is not None
            else None,
        )
        source.decision_reason = decision.reason
        if decision.action is None:
            await self.recorder.increment("kv_retention_unsupported")
            return
        retry_offload = (
            decision.action == "OFFLOAD"
            and source.last_status in {"FAILED", "PARTIAL"}
            and monotonic() >= source.retry_at
        )
        if decision.action == source.last_action and not retry_offload:
            return
        if not query:
            await self._refresh_source(source, free, query=True)
            return
        if not await self._current(source.identity, source.tail_version):
            return
        if not self._source_live(source):
            return
        action_id = "kv-action-" + uuid4().hex
        current_version = observation.effective_policy_version or 0
        source.action_id = action_id
        source.pending_command = {
            **common,
            "action_id": action_id,
            "idempotency_key": action_id,
            "action": decision.action,
            "descriptor_id": source.descriptor_id,
            "expected_policy_version": current_version,
            "policy_version": current_version + 1,
            "source_llm_call_id": source.identity.llm_call_id,
            # vLLM binds this field to CallBinding.request_id (logical request).
            "expected_tail_request_id": source.identity.request_id,
            "expected_tail_version": source.tail_version,
            "decision_ref": decision.reason,
        }
        source.last_action = decision.action
        receipt = await self._rpc("POST", "/apply", source.pending_command)
        if not self._source_live(source):
            return
        await self._record_receipt(source, receipt)
        source.pending_command = None

    async def _record_receipt(self, source: _Source, receipt: dict[str, Any]) -> None:
        if (
            receipt["action_id"] != source.action_id
            or receipt["descriptor_id"] != source.descriptor_id
            or receipt["owner_scope"] != self.config.owner_scope
            or receipt["engine_epoch"] != source.engine_epoch
            or receipt["action"] != source.last_action
        ):
            raise ValueError("KV policy receipt identity mismatch")
        status = receipt["status"]
        if status not in {
            "ACCEPTED",
            "APPLIED",
            "PARTIAL",
            "FAILED",
            "EXPIRED",
            "STALE",
            "UNSUPPORTED",
        }:
            raise ValueError("unknown KV receipt status")
        source.last_status = status
        if source.last_action == "OFFLOAD" and status in {"FAILED", "PARTIAL"}:
            source.retry_at = monotonic() + self.config.refresh_seconds
        source.operation_id = receipt["operation_id"] if status == "ACCEPTED" else None
        await self.recorder.emit(
            "kv_policy_receipt",
            identity={
                "job_id": source.identity.job_id,
                "line_id": source.identity.line_id,
                "llm_call_id": source.identity.llm_call_id,
            },
            fields={
                k: receipt.get(k)
                for k in (
                    "action",
                    "status",
                    "action_id",
                    "descriptor_id",
                    "operation_id",
                    "applied_policy_version",
                    "cpu_committed_bytes",
                    "gpu_reclaimed_bytes",
                    "skipped_reasons",
                )
            },
        )
        if status in {"FAILED", "PARTIAL", "EXPIRED", "STALE", "UNSUPPORTED"}:
            await self.recorder.increment("kv_policy_" + status.lower())
        if source.last_action == "DROP" and status == "APPLIED":
            self._remove_source(source)

    def snapshot(self) -> dict[str, Any]:
        for source in list(self._sources.values()):
            self._source_live(source)
        return {
            "status": self.status,
            "last_error": self._last_error,
            "capabilities": self.capabilities.model_dump()
            if self.capabilities
            else None,
            "sources": [
                {
                    "descriptor_id": s.descriptor_id,
                    "action": s.last_action,
                    "receipt_status": s.last_status,
                    "decision_reason": s.decision_reason,
                    "remaining_ttl_seconds": max(
                        0.0, s.expires_at_monotonic - monotonic()
                    ),
                }
                for s in self._sources.values()
            ],
            "target_prefix_basis": "COLD:no_target_proof",
            "cost_model": self.cost_model.version
            if self.cost_model
            else "unknown:no_calibration",
            "refresh_scope": "response_tool_line_and_pressure_events",
            "source_expirations": dict(self._source_expirations),
        }

    async def run(self) -> None:
        while True:
            await self.negotiate()
            try:
                async with self._lock:
                    await self._refresh_locked(events_only=True)
            except Exception as exc:
                self._last_error = type(exc).__name__
                await self.recorder.increment("kv_refresh_failures")
                logger.warning("KV refresh failed: %s", type(exc).__name__)
            if not self._tasks and not self._bound:
                self._finished_lines.intersection_update(
                    {
                        (s.identity.job_id, s.identity.line_id, s.tail_version)
                        for s in self._sources.values()
                    }
                )
            await asyncio.sleep(self.config.refresh_seconds)

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._clear_sources()
