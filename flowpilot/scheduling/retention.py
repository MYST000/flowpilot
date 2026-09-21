"""vLLM KV control v1 client and factual response retention policy.

No restore method, restore queue, GPU-ready wait, or token-to-byte conversion.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.observability.trace import TraceRecorder
from flowpilot.protocol import RequestIdentity
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


class EngineIdentity(BaseModel):
    engine_epoch: str


class Capabilities(BaseModel):
    schema_version: Literal[1]
    engine: EngineIdentity
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
    lookup_state: Literal["COMPLETE", "PENDING", "UNSUPPORTED"]
    reuse_basis: Literal["DESCRIPTOR_ONLY", "ASSUMED_CONTINUATION"]
    effective_policy_version: int | None = None
    effective_policy_action: Action | None = None


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
    if near and not pressure and capabilities.gpu_retention_preference:
        return RetentionDecision("KEEP", "near_factual_successor")
    if (
        capabilities.cpu_backed_eviction_preference
        and capabilities.cpu_store
        and capabilities.engine_cpu_reuse
    ):
        return RetentionDecision(
            "OFFLOAD", "gpu_pressure" if pressure else "waiting_for_successor"
        )
    if capabilities.gpu_retention_preference:
        return RetentionDecision("KEEP", "cpu_retention_unsupported")
    return RetentionDecision(None, "retention_unsupported")


@dataclass
class _Source:
    identity: RequestIdentity
    tail_version: int
    descriptor_id: str
    engine_epoch: str
    operation_id: str | None = None
    action_id: str | None = None
    last_action: Action | None = None
    last_status: str | None = None
    pending_command: dict[str, Any] | None = None


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
        self._lock = asyncio.Lock()
        self._event_seq = 0
        self._last_error: str | None = None
        self._bound: set[tuple[str, str]] = set()
        self._finished_lines: set[tuple[str, str, int]] = set()

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
            if self.capabilities and (
                capability.engine.engine_epoch != self.capabilities.engine.engine_epoch
            ):
                self._sources.clear()
                self._event_seq = 0
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
            await self.recorder.increment("kv_capability_failures")

    def bind(self, body: bytes, identity: RequestIdentity) -> bytes:
        if self.status != "supported":
            return body
        payload = json.loads(body)
        transfer = dict(payload.get("kv_transfer_params") or {})
        if "kv_control_binding" in transfer:
            raise ValueError("kv_control_binding is owned by FlowPilot")
        transfer["kv_control_binding"] = self._binding(identity)
        payload["kv_transfer_params"] = transfer
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
        try:
            # Engine finish and HTTP terminal can cross. Resolve PENDING using
            # the bounded control timeout; never hold response delivery for it.
            async with asyncio.timeout(self.config.timeout_seconds):
                while await self._current(identity, version):
                    result = await self._rpc(
                        "POST", "/resolve", self._binding(identity)
                    )
                    if result.get("binding") != self._binding(identity):
                        raise ValueError("KV resolve binding mismatch")
                    if result["status"] != "PENDING":
                        break
                    await asyncio.sleep(0.02)
                else:
                    return
            if result["status"] != "READY":
                await self.recorder.increment("kv_resolve_" + result["status"].lower())
                return
            async with self._lock:
                if not await self._current(identity, version):
                    return
                for descriptor in result["descriptors"]:
                    if descriptor["binding"] != self._binding(identity):
                        raise ValueError("KV descriptor binding mismatch")
                    source = _Source(
                        identity,
                        version,
                        descriptor["descriptor_id"],
                        descriptor["engine"]["engine_epoch"],
                    )
                    self._sources[source.descriptor_id] = source
                await self._refresh_locked()
        except Exception as exc:
            self._last_error = type(exc).__name__
            await self.recorder.increment("kv_resolution_failures")
            logger.warning("KV resolve failed: %s", type(exc).__name__)

    async def refresh(self) -> None:
        async with self._lock:
            await self._refresh_locked()

    async def _refresh_locked(self) -> None:
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
        self._event_seq = telemetry["event_seq"]
        # Query anew on every refresh, including event gaps. No stale observation
        # is reused for a policy, and descriptor hits never enter target ordering.
        for descriptor_id, source in list(self._sources.items()):
            if not await self._current(source.identity, source.tail_version):
                self._sources.pop(descriptor_id, None)
                continue
            try:
                await self._refresh_source(source, telemetry["free_gpu_allocations"])
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {409, 410}:
                    self._sources.pop(descriptor_id, None)
                await self.recorder.increment("kv_policy_http_failures")
                self._last_error = f"HTTP_{exc.response.status_code}"

    async def _refresh_source(self, source: _Source, free: int) -> None:
        common = {
            "schema_version": 1,
            "owner_scope": self.config.owner_scope,
            "expected_engine_epoch": source.engine_epoch,
        }
        if source.pending_command is not None:
            receipt = await self._rpc("POST", "/apply", source.pending_command)
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
            await self._record_receipt(source, receipt)
            if source.last_status == "ACCEPTED":
                return
        observation = PrefixObservation.model_validate(
            await self._rpc(
                "POST", "/query", {**common, "descriptor_id": source.descriptor_id}
            )
        )
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
        decision = choose_retention(
            config=self.config,
            capabilities=Capabilities.model_validate(self.capabilities),
            observation=observation,
            phase=snapshot["phase"],
            need_in_seconds=(projection.t_need - datetime.now(UTC)).total_seconds()
            if projection is not None and projection.t_need is not None
            else None,
            free_gpu_allocations=free,
        )
        if decision.action is None:
            await self.recorder.increment("kv_retention_unsupported")
            return
        if decision.action == source.last_action:
            return
        if not await self._current(source.identity, source.tail_version):
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
            self._sources.pop(source.descriptor_id, None)

    def snapshot(self) -> dict[str, Any]:
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
                }
                for s in self._sources.values()
            ],
            "target_prefix_basis": "COLD:no_target_proof",
        }

    async def run(self) -> None:
        while True:
            await self.negotiate()
            try:
                await self.refresh()
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
