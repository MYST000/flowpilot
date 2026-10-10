"""vLLM KV control v1 client and factual response retention policy.

No restore method, restore queue, GPU-ready wait, or token-to-byte conversion.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
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
from flowpilot.scheduling.prefill import PrefillLoad
from flowpilot.scheduling.projection import ProjectionCalculator
from flowpilot.scheduling.wait_feedback import QueueWaitEstimate

logger = logging.getLogger(__name__)
Action = Literal["KEEP", "OFFLOAD", "DROP"]


class RetentionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    enabled: bool = False
    owner_scope: str = Field(default="flowpilot-local", min_length=1)
    timeout_seconds: float = Field(default=1.0, gt=0)
    refresh_seconds: float = Field(default=1.0, gt=0)
    window_basis: Literal["tool_and_queue", "tool_only"] = "tool_and_queue"
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
    prefill_cost_context: bool = False


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
    offload_new_object_bytes: int | None = Field(default=None, ge=0)
    prefill_load: PrefillLoad | None = None


@dataclass(frozen=True)
class RetentionCandidate:
    action: Action
    cost_seconds: float | None
    reason: str


@dataclass(frozen=True)
class RetentionDecision:
    action: Action | None
    reason: str
    candidates: tuple[RetentionCandidate, ...] = ()


def choose_retention(
    *,
    config: RetentionConfig,
    capabilities: Capabilities,
    observation: PrefixObservation,
    phase: str,
    retention_window_seconds: float | None,
    free_gpu_allocations: int,
    cost_model: OfflineCostModel | None = None,
) -> RetentionDecision:
    if phase == "TERMINAL" or (
        observation.lookup_state == "COMPLETE" and observation.recoverable_tokens == 0
    ):
        return RetentionDecision(
            "DROP" if capabilities.safe_direct_drop else None,
            "terminal_or_no_recoverable_prefix",
        )
    window = retention_window_seconds
    near = window is not None and window <= config.keep_horizon_seconds
    pressure = free_gpu_allocations <= config.gpu_free_reserve_allocations
    candidates = _cost_retention(
        config, capabilities, observation, cost_model, window, pressure
    )
    evaluable = [c for c in candidates if c.cost_seconds is not None]
    if evaluable:
        selected = min(
            evaluable,
            key=lambda c: (
                c.cost_seconds if c.cost_seconds is not None else float("inf")
            ),
        )
        return RetentionDecision(
            selected.action, "calibrated_cost:assumed_continuation", candidates
        )
    if near and not pressure and capabilities.gpu_retention_preference:
        return RetentionDecision(
            "KEEP", "fallback_cost_unknown:near_factual_successor", candidates
        )
    if (
        capabilities.cpu_backed_eviction_preference
        and capabilities.cpu_store
        and capabilities.engine_cpu_reuse
    ):
        return RetentionDecision(
            "OFFLOAD",
            "fallback_cost_unknown:"
            + ("gpu_pressure" if pressure else "waiting_for_successor"),
            candidates,
        )
    if capabilities.gpu_retention_preference:
        return RetentionDecision("KEEP", "cpu_retention_unsupported", candidates)
    return RetentionDecision(None, "retention_unsupported", candidates)


def _cost_retention(
    config: RetentionConfig,
    caps: Capabilities,
    obs: PrefixObservation,
    model: OfflineCostModel | None,
    window: float | None,
    pressure: bool,
) -> tuple[RetentionCandidate, ...]:
    missing = (
        "retention_window_unknown"
        if window is None
        else "calibration_missing"
        if model is None
        else "calibration_identity_mismatch"
        if caps.engine.identity_digest != model.engine_identity_digest
        else None
    )
    if missing is not None:
        return tuple(
            RetentionCandidate(action, None, missing)
            for action in ("DROP", "KEEP", "OFFLOAD")
        )
    assert model is not None and window is not None
    # Future Tool output is unknown; this is an assumed continuation scenario.
    p = obs.prefix_token_count + 1
    load = obs.prefill_load if caps.prefill_cost_context else None
    cold_estimate = model.prefill_estimate(
        p, 0, load=load, engine_epoch=obs.engine_epoch
    )
    keep_estimate = model.prefill_estimate(
        p, obs.gpu_ready_tokens, load=load, engine_epoch=obs.engine_epoch
    )
    cold, keep = cold_estimate.seconds, keep_estimate.seconds
    drop_reason = (
        "safe_drop_unsupported"
        if not caps.safe_direct_drop
        else (cold_estimate.basis if cold is None else "calibrated")
    )
    candidates = [
        RetentionCandidate(
            "DROP", cold if drop_reason == "calibrated" else None, drop_reason
        )
    ]
    keep_reason = (
        "gpu_retention_unsupported"
        if not caps.gpu_retention_preference
        else "no_gpu_prefix"
        if not obs.gpu_ready_tokens
        else "gpu_bytes_unknown"
        if obs.gpu_retention_bytes is None
        else keep_estimate.basis
        if keep is None
        else "calibrated"
    )
    keep_cost = None
    if keep_reason == "calibrated":
        assert keep is not None and obs.gpu_retention_bytes is not None
        keep_cost = (
            keep
            + obs.gpu_retention_bytes
            / 2** 30
            * window
            * config.gpu_seconds_per_gib_second
            * (2 if pressure else 1)
        )
    candidates.append(RetentionCandidate("KEEP", keep_cost, keep_reason))
    offload_reason = (
        "cpu_retention_unsupported"
        if not (
            caps.cpu_store
            and caps.cpu_backed_eviction_preference
            and caps.engine_cpu_reuse
        )
        else "restore_calibration_missing"
        if model.restore is None
        else "offload_bytes_unknown"
        if obs.offload_object_bytes is None
        else "offload_target_unknown"
        if not obs.offload_target_tokens
        else None
    )
    offload_cost = None
    if offload_reason is None:
        assert (
            model.restore is not None
            and obs.offload_object_bytes is not None
            and obs.offload_target_tokens is not None
        )
        after_estimate = model.prefill_estimate(
            p, min(p, obs.offload_target_tokens), load=load,
            engine_epoch=obs.engine_epoch,
        )
        after = after_estimate.seconds
        cpu_ready = (
            obs.cpu_standalone_tokens is not None
            and obs.cpu_standalone_tokens >= obs.offload_target_tokens
        )
        d2h = (
            0.0
            if cpu_ready
            else (
                model.offload.seconds(obs.offload_new_object_bytes)
                if (
                    model.offload is not None
                    and obs.offload_new_object_bytes is not None
                )
                else None
            )
        )
        if after is None:
            offload_reason = after_estimate.basis
        elif d2h is None:
            offload_reason = (
                "offload_calibration_missing"
                if model.offload is None
                else "offload_new_bytes_unknown"
            )
        elif d2h > window:
            offload_reason = "estimated_d2h_exceeds_window"
        else:
            offload_cost = (
                d2h
                + model.restore.seconds(obs.offload_object_bytes)
                + after
                + obs.offload_object_bytes
                / 2**30
                * window
                * config.cpu_seconds_per_gib_second
            )
            offload_reason = (
                "calibrated:cpu_already_covered" if cpu_ready else "calibrated:new_d2h"
            )
    candidates.append(RetentionCandidate("OFFLOAD", offload_cost, offload_reason))
    return tuple(candidates)


@dataclass
class _ResponseInputs:
    waiting_tools: set[str]
    changed: asyncio.Event
    prediction: asyncio.Future[Any] | None
    ready: bool = False
    prediction_failed: bool = False


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
    inputs: _ResponseInputs | None = None
    decision: RetentionDecision | None = None
    tool_gap_seconds: float | None = None
    queue_wait_estimate: QueueWaitEstimate | None = None
    retention_window_seconds: float | None = None
    decision_inputs: dict[str, Any] | None = None


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
        self._response_inputs: dict[tuple[str, str], _ResponseInputs] = {}
        self._finished_lines: set[tuple[str, str, int]] = set()
        self._source_expirations: Counter[str] = Counter()
        self.cost_model = cost_model
        self._queue_wait_provider: Callable[[], Awaitable[QueueWaitEstimate]] | None = (
            None
        )

    def set_queue_wait_provider(
        self, provider: Callable[[], Awaitable[QueueWaitEstimate]]
    ) -> None:
        self._queue_wait_provider = provider

    async def _queue_wait(self) -> QueueWaitEstimate:
        if self._queue_wait_provider is not None:
            return await self._queue_wait_provider()
        return QueueWaitEstimate(
            source="admission_disabled", observed_at_monotonic=monotonic()
        )

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

    def finished(
        self,
        identity: RequestIdentity,
        version: int | None,
        *,
        reuse_tool_call_ids: tuple[str, ...] = (),
        prediction: Awaitable[Any] | None = None,
    ) -> None:
        key = (identity.job_id, identity.llm_call_id)
        if key not in self._bound:
            return
        self._bound.discard(key)
        if version is not None:
            inputs = _ResponseInputs(
                set(reuse_tool_call_ids),
                asyncio.Event(),
                asyncio.ensure_future(prediction) if prediction is not None else None,
            )
            self._response_inputs[key] = inputs
            task = asyncio.create_task(self._resolve(identity, version, inputs))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def tool_resolved(self, identity: Any) -> None:
        inputs = self._response_inputs.get((identity.job_id, identity.llm_call_id))
        if inputs is not None:
            inputs.waiting_tools.discard(identity.tool_call_id)
            inputs.changed.set()

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

    async def _resolve(
        self, identity: RequestIdentity, version: int, inputs: _ResponseInputs
    ) -> None:
        epoch = self._engine_epoch
        deadline = monotonic() + (
            self._metadata_ttl_seconds or self.config.timeout_seconds
        )
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
                        inputs=inputs,
                    )
                    self._register_source(source)
                await self._refresh_locked(events_only=True)
            # KV observations and the caller's cache/prediction work overlap.
            # No control lock is held while the response inputs are collected.
            async with asyncio.timeout(max(0.0, deadline - monotonic())):
                await self._collect_response_inputs(identity, inputs)
                inputs.ready = True
                async with self._lock:
                    await self._refresh_locked(events_only=True)
        except Exception as exc:
            self._last_error = type(exc).__name__
            await self.recorder.increment("kv_resolution_failures")
            logger.warning("KV resolve failed: %s", type(exc).__name__)
        finally:
            self._response_inputs.pop((identity.job_id, identity.llm_call_id), None)
            if inputs.prediction is not None and not inputs.prediction.done():
                inputs.prediction.cancel()
            if inputs.prediction is not None:
                await asyncio.gather(inputs.prediction, return_exceptions=True)
                inputs.prediction = None

    async def _collect_response_inputs(
        self, identity: RequestIdentity, inputs: _ResponseInputs
    ) -> None:
        while True:
            # Clear before reading facts so a concurrent update cannot be lost.
            inputs.changed.clear()
            if inputs.waiting_tools:
                await inputs.changed.wait()
                continue
            prediction = inputs.prediction
            if prediction is None:
                return
            records = await self.projections.resolutions.get_for_line(
                identity.job_id, identity.line_id, identity.tail_request_id
            )
            needs_prediction = any(
                r.status == "resolving"
                and r.resolution in {"local_only", "local_leader"}
                for r in records
            )
            if not needs_prediction:
                prediction.cancel()
                await asyncio.gather(prediction, return_exceptions=True)
                return
            if prediction.done():
                try:
                    await prediction
                except (Exception, asyncio.CancelledError) as exc:
                    if (
                        isinstance(exc, asyncio.CancelledError)
                        and (task := asyncio.current_task()) is not None
                        and task.cancelling()
                    ):
                        raise
                    inputs.prediction_failed = True
                    await self.recorder.increment("tool_duration_prediction_failures")
                    logger.warning(
                        "Tool duration prediction unavailable: %s", type(exc).__name__
                    )
                return
            changed = asyncio.create_task(inputs.changed.wait())
            try:
                await asyncio.wait(
                    (prediction, changed), return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                changed.cancel()
                await asyncio.gather(changed, return_exceptions=True)

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
        retry_offload = (
            source.last_action == "OFFLOAD"
            and source.last_status in {"FAILED", "PARTIAL"}
            and monotonic() >= source.retry_at
        )
        cleanup = finished and source.last_action != "DROP"
        if source.decision is not None and not cleanup and not retry_offload:
            return
        # Once selected, only execution retries and terminal cleanup need a
        # fresh policy version. Tool/capacity changes never reselect placement.
        query = (query and source.decision is None) or retry_offload or cleanup
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
        if not finished and source.inputs is not None and not source.inputs.ready:
            return
        caps = Capabilities.model_validate(self.capabilities)
        if finished:
            # Releasing a finished line is lifecycle cleanup, not a second
            # optimization of the response's frozen placement decision.
            decision = RetentionDecision(
                "DROP" if caps.safe_direct_drop else None, "line_finished"
            )
        elif source.decision is not None:
            decision = source.decision
        else:
            projection = await self.projections.for_line(
                source.identity.job_id, source.identity.line_id
            )
            snapshot = await self.frontier.line_snapshot(
                source.identity.job_id, source.identity.line_id
            )
            gap = (
                0.0
                if projection.ready
                else max(0.0, (projection.t_need - datetime.now(UTC)).total_seconds())
                if projection.t_need is not None
                else None
            )
            if source.inputs is not None and source.inputs.prediction_failed:
                records = await self.projections.resolutions.get_for_line(
                    source.identity.job_id,
                    source.identity.line_id,
                    source.identity.tail_request_id,
                )
                if any(
                    r.status == "resolving"
                    and r.resolution in {"local_only", "local_leader"}
                    for r in records
                ):
                    gap = None
            queue_wait = await self._queue_wait()
            window = (
                gap
                if self.config.window_basis == "tool_only"
                else (
                    gap + queue_wait.estimate_ms / 1000
                    if gap is not None and queue_wait.estimate_ms is not None
                    else None
                )
            )
            if (
                not self._source_live(source)
                or not await self._current(source.identity, source.tail_version)
                or not await self.projections.validate_current(projection)
            ):
                return
            decision = choose_retention(
                config=self.config,
                capabilities=caps,
                observation=observation,
                phase=snapshot["phase"],
                retention_window_seconds=window,
                free_gpu_allocations=free,
                cost_model=self.cost_model,
            )
            source.queue_wait_estimate = queue_wait
            source.retention_window_seconds = window
            source.decision_inputs = {
                "window_basis": self.config.window_basis,
                "queue_wait_estimate": queue_wait.model_dump(),
                "retention_window_seconds": window,
                "missing_inputs": (["tool_gap"] if gap is None else [])
                + (
                    ["queue_wait"]
                    if self.config.window_basis == "tool_and_queue"
                    and queue_wait.estimate_ms is None
                    else []
                ),
                "free_gpu_allocations": free,
                "gpu_pressure": free <= self.config.gpu_free_reserve_allocations,
                "observation": observation.model_dump(),
                "capabilities": caps.model_dump(),
                "calibration": self.cost_model.model_dump(mode="json")
                if self.cost_model
                else None,
                "gpu_seconds_per_gib_second": self.config.gpu_seconds_per_gib_second,
                "cpu_seconds_per_gib_second": self.config.cpu_seconds_per_gib_second,
                "candidates": [asdict(candidate) for candidate in decision.candidates],
            }
            source.decision = decision
            source.tool_gap_seconds = gap
            await self.recorder.emit(
                "kv_retention_decision",
                identity={
                    "job_id": source.identity.job_id,
                    "line_id": source.identity.line_id,
                    "llm_call_id": source.identity.llm_call_id,
                },
                fields={
                    "descriptor_id": source.descriptor_id,
                    "action": decision.action,
                    "reason": decision.reason,
                    "tool_gap_seconds": gap,
                    **source.decision_inputs,
                    "prediction_failed": bool(
                        source.inputs and source.inputs.prediction_failed
                    ),
                },
            )
        source.decision_reason = decision.reason
        if decision.action is None:
            await self.recorder.increment("kv_retention_unsupported")
            return
        if decision.action == source.last_action and not retry_offload:
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
                    "selected_action": s.decision.action if s.decision else None,
                    "tool_gap_seconds": s.tool_gap_seconds,
                    "decision_inputs": s.decision_inputs,
                    "inputs_ready": s.inputs is None or s.inputs.ready,
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
            "refresh_scope": "receipts_retries_expiry_and_line_cleanup",
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
