"""One external admission queue ordered by measured wait minus startup cost."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from flowpilot.scheduling.capacity import (
    AdaptiveAdmissionConfig,
    CapacityFeedback,
    EngineLoad,
)
from flowpilot.scheduling.cost import OfflineCostModel, RequestWork
from flowpilot.scheduling.wait_feedback import (
    AdmissionWaitFeedback,
    QueueWaitEstimate,
    WaitFeedbackConfig,
)


class AdmissionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    enabled: bool = False
    # Operator configured gateway concurrency, not vLLM's internal batch size.
    limit: int = Field(default=8, gt=0)
    heartbeat_interval_seconds: float = Field(default=1.0, gt=0)
    heartbeat_ttl_seconds: float = Field(default=5.0, gt=0)
    probe_timeout_seconds: float = Field(default=1.0, gt=0)
    policy: Literal["wait_cost", "fifo"] = "wait_cost"
    wait_feedback: WaitFeedbackConfig = WaitFeedbackConfig()
    cost_model: OfflineCostModel | None = None
    prefix_ttl_seconds: float = Field(default=2.0, gt=0)
    adaptive: AdaptiveAdmissionConfig = AdaptiveAdmissionConfig()

    @model_validator(mode="before")
    @classmethod
    def migrate_policy(cls, value: Any) -> Any:
        if isinstance(value, dict):
            removed = {
                "age_reference_seconds",
                "work_reference_tokens",
                "weights",
                "best_effort",
            } & value.keys()
            if removed or value.get("policy") in {
                "prefill_slack",
                "slo_unexpired_first",
                "weighted",
            }:
                raise ValueError(
                    "Admission configuration migration required: "
                    "select wait_cost or fifo; "
                    "remove weights, best_effort, age_reference_seconds "
                    "and work_reference_tokens. "
                    "SLO/importance and expiry quotas no longer control admission."
                )
        return value

    @model_validator(mode="after")
    def heartbeat_window(self) -> AdmissionConfig:
        if self.heartbeat_ttl_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("heartbeat TTL must exceed its interval")
        if self.adaptive.enabled and self.adaptive.initial_limit > self.limit:
            raise ValueError("adaptive initial limit exceeds admission maximum")
        return self


@dataclass(frozen=True)
class RequestPriority:
    key: tuple[str, str]
    job_id: str
    prompt_tokens: int | None = None
    work: RequestWork | None = None


@dataclass
class _Waiting:
    request: RequestPriority
    sequence: int
    entered: float
    queued_prompt_tokens_at_entry: int
    queued_prompt_tokens_complete: bool
    future: asyncio.Future[dict[str, Any]]


@dataclass(frozen=True)
class _InFlight:
    job_id: str


class AdmissionQueue:
    """Consume credits under one lock; callers send HTTP outside the lock.

    Heartbeats contain real health observations and the explicit gateway limit.
    A stale heartbeat stops new dispatch, including dispatch on credit return.
    """

    def __init__(
        self,
        config: AdmissionConfig,
        refresh_work: Callable[
            [list[RequestPriority]], Awaitable[dict[tuple[str, str], RequestWork]]
        ]
        | None = None,
    ) -> None:
        self.config = config
        self._lock = asyncio.Lock()
        self._waiting: dict[tuple[str, str], _Waiting] = {}
        self._inflight: dict[tuple[str, str], _InFlight] = {}
        self._capacity = CapacityFeedback(config.adaptive, config.limit)
        self._wait_feedback = AdmissionWaitFeedback(config.wait_feedback)
        self._cancelled_wait_count = 0
        self._cancelled_wait_ms = 0.0
        self._sweep = 0
        self._last_sweep: dict[str, Any] | None = None
        self._sequence = 0
        self._healthy = False
        self._heartbeat_at = -math.inf
        self._closed = False
        self._refresh_work = refresh_work
        self._refresh_task: asyncio.Task[None] | None = None

    def _available(self, now: float) -> bool:
        return (
            self._healthy
            and (now - self._heartbeat_at < self.config.heartbeat_ttl_seconds)
            and not self._closed
        )

    async def heartbeat(self, healthy: bool) -> None:
        async with self._lock:
            self._healthy = healthy
            self._heartbeat_at = time.monotonic()
            self._dispatch()

    async def engine_load(self, load: EngineLoad) -> dict[str, Any]:
        async with self._lock:
            self._capacity.observe(
                load,
                time.monotonic(),
                demand=any(not v.future.cancelled() for v in self._waiting.values())
                or len(self._inflight) >= self._capacity.limit,
            )
            self._dispatch()
            return self._capacity.snapshot(time.monotonic())

    async def engine_load_unavailable(self, reason: str) -> None:
        async with self._lock:
            self._capacity.unavailable(reason)

    def set_work_refresher(
        self,
        refresh_work: Callable[
            [list[RequestPriority]], Awaitable[dict[tuple[str, str], RequestWork]]
        ],
    ) -> None:
        self._refresh_work = refresh_work

    async def acquire(self, request: RequestPriority) -> dict[str, Any]:
        key = request.key
        async with self._lock:
            if self._closed:
                raise RuntimeError("admission queue is closed")
            if key in self._waiting or key in self._inflight:
                raise ValueError("duplicate admission key")
            self._sequence += 1
            waiting = _Waiting(
                request,
                self._sequence,
                time.monotonic(),
                sum(
                    e.request.prompt_tokens or 0
                    for e in self._waiting.values()
                    if not e.future.cancelled()
                ),
                all(
                    e.request.prompt_tokens is not None
                    for e in self._waiting.values()
                    if not e.future.cancelled()
                ),
                asyncio.get_running_loop().create_future(),
            )
            self._waiting[key] = waiting
            self._dispatch()
        try:
            return await waiting.future
        except BaseException:
            # Includes cancellation after reservation but before acquire returns.
            await self.release(key)
            raise

    async def release(self, key: tuple[str, str]) -> None:
        async with self._lock:
            waiting = self._waiting.pop(key, None)
            if waiting is not None:
                self._record_cancelled(waiting, time.monotonic())
                if not waiting.future.done():
                    waiting.future.cancel()
            self._inflight.pop(key, None)
            self._dispatch()

    async def notify_state_changed(self) -> None:
        async with self._lock:
            self._dispatch()

    def _record_cancelled(self, item: _Waiting, now: float) -> None:
        self._cancelled_wait_count += 1
        self._cancelled_wait_ms += (now - item.entered) * 1000

    def _prune_cancelled(self, now: float) -> None:
        for key, item in list(self._waiting.items()):
            if item.future.cancelled():
                self._waiting.pop(key)
                self._record_cancelled(item, now)

    def _projection(self, item: _Waiting, now: float) -> dict[str, Any]:
        work = item.request.work
        if (
            work is not None
            and work.observed_at_monotonic is not None
            and now - work.observed_at_monotonic >= self.config.prefix_ttl_seconds
        ):
            work = RequestWork(
                prompt_tokens=work.prompt_tokens,
                prefill_tokens=work.prompt_tokens,
                prefix_basis="COLD:expired_full_sweep_observation",
                cost_basis="unknown:stale_prefix",
            )
        wait_ms = (now - item.entered) * 1000
        cost_ms = (
            work.cost_seconds * 1000
            if work is not None and work.cost_seconds is not None
            else None
        )
        return {
            "job_id": item.request.job_id,
            "llm_call_id": item.request.key[1],
            "queue_entered_monotonic": item.entered,
            "queue_wait_ms": wait_ms,
            "kv_start_cost_ms": cost_ms,
            "score_ms": wait_ms - cost_ms if cost_ms is not None else None,
            "prompt_tokens": item.request.prompt_tokens,
            "work_basis": work.cost_basis
            if work is not None
            else "unknown:no_work_estimate",
            "prefix_basis": work.prefix_basis if work else "COLD:no_target_proof",
            "queued_prompt_tokens_at_entry": item.queued_prompt_tokens_at_entry,
            "queued_prompt_tokens_complete": item.queued_prompt_tokens_complete,
            "sequence": item.sequence,
            "policy": self.config.policy,
            "work": work.model_dump() if work is not None else None,
        }

    def _selection(
        self, entries: list[_Waiting], now: float
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        projections = [
            self._projection(item, now)
            for item in entries
            if not item.future.cancelled()
        ]
        missing = [
            {"sequence": p["sequence"], "reason": p["work_basis"]}
            for p in projections
            if p["kv_start_cost_ms"] is None
        ]
        basis = (
            "fifo:configured"
            if self.config.policy == "fifo"
            else ("fifo:cost_unknown" if missing else "wait_cost")
        )
        projections.sort(
            key=lambda p: (
                (
                    p["queue_entered_monotonic"] + p["kv_start_cost_ms"] / 1000,
                    p["sequence"],
                )
                if basis == "wait_cost"
                else (p["sequence"],)
            )
        )
        details = {
            "ordering_basis": basis,
            "cost_unknown_reasons": missing,
            "candidate_sequences": sorted(p["sequence"] for p in projections),
        }
        for projection in projections:
            projection.update(details)
        return projections, details

    def _dispatch(self) -> None:
        if self._refresh_work is not None:
            if self._waiting and not self._closed and self._refresh_task is None:
                self._refresh_task = asyncio.create_task(self._refresh_and_dispatch())
            return
        self._dispatch_ready()

    async def _refresh_and_dispatch(self) -> None:
        assert self._refresh_work is not None
        async with self._lock:
            snapshot = dict(self._waiting)
        try:
            estimates = await self._refresh_work([v.request for v in snapshot.values()])
            async with self._lock:
                for key, original in snapshot.items():
                    if self._waiting.get(key) is original:
                        work = estimates[key]
                        original.request = replace(
                            original.request,
                            work=work,
                            prompt_tokens=work.prompt_tokens,
                        )
                self._dispatch_ready(snapshot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A failed full sweep is visible to callers; it cannot manufacture
            # a successful measurement or leave their futures pending forever.
            async with self._lock:
                for key, original in snapshot.items():
                    if self._waiting.get(key) is original:
                        self._waiting.pop(key)
                        if not original.future.done():
                            original.future.set_exception(exc)
        finally:
            async with self._lock:
                self._refresh_task = None
                if any(
                    snapshot.get(key) is not item for key, item in self._waiting.items()
                ):
                    self._dispatch()

    def _dispatch_ready(
        self, eligible: dict[tuple[str, str], _Waiting] | None = None
    ) -> None:
        now = time.monotonic()
        self._prune_cancelled(now)
        candidates = [
            item
            for key, item in self._waiting.items()
            if eligible is None or eligible.get(key) is item
        ]
        projections, details = self._selection(candidates, now)
        self._sweep += 1
        self._last_sweep = {
            "sweep_id": self._sweep,
            **details,
            "dispatched_sequences": [],
        }
        # Freeze one ordering for all credits in this selection, including after
        # an unknown-cost request leaves the queue. No awaits under this lock.
        for projection in projections:
            if not self._available(now) or len(self._inflight) >= self._capacity.limit:
                break
            key = (projection["job_id"], projection["llm_call_id"])
            item = self._waiting.pop(key)
            self._inflight[key] = _InFlight(item.request.job_id)
            self._wait_feedback.record(dispatched_at=now, entered_at=item.entered)
            projection["effective_limit"] = self._capacity.limit
            projection["sweep_id"] = self._sweep
            self._last_sweep["dispatched_sequences"].append(item.sequence)
            item.future.set_result(projection)

    def _queue_wait_estimate(self, now: float) -> QueueWaitEstimate:
        return self._wait_feedback.estimate(
            now=now,
            idle_capacity=not any(
                not v.future.cancelled() for v in self._waiting.values()
            )
            and self._available(now)
            and len(self._inflight) < self._capacity.limit,
        )

    async def queue_wait_estimate(self) -> QueueWaitEstimate:
        async with self._lock:
            return self._queue_wait_estimate(time.monotonic())

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            now = time.monotonic()
            projections, details = self._selection(list(self._waiting.values()), now)
            return {
                "healthy": self._available(now),
                "capacity_source": "measured_engine_feedback"
                if self.config.adaptive.enabled
                else "configured_gateway_limit",
                "limit": self.config.limit,
                "effective_limit": self._capacity.limit,
                "policy": self.config.policy,
                "inflight": len(self._inflight),
                "free": max(0, self._capacity.limit - len(self._inflight)),
                "adaptive": self._capacity.snapshot(now),
                "queued": projections,
                **details,
                "last_sweep": self._last_sweep,
                "queue_wait_estimate": self._queue_wait_estimate(now).model_dump(),
                "cancelled_wait_count": self._cancelled_wait_count,
                "cancelled_wait_ms": self._cancelled_wait_ms,
            }

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            for item in self._waiting.values():
                if not item.future.done():
                    item.future.set_exception(RuntimeError("admission queue closed"))
            self._waiting.clear()
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
