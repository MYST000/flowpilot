"""One external queue ordered by remaining prefill slack."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from flowpilot.scheduling.cost import OfflineCostModel, RequestWork


class PriorityWeights(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    slo: float = Field(default=0.55, ge=0)
    age: float = Field(default=0.35, gt=0)
    progress: float = Field(default=0.05, ge=0)
    release: float = Field(default=0.03, ge=0)
    cost: float = Field(default=0.02, ge=0)
    fairness: float = Field(default=0.0, ge=0)


class AdmissionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    enabled: bool = False
    # Operator configured gateway concurrency, not vLLM's internal batch size.
    limit: int = Field(default=8, gt=0)
    heartbeat_interval_seconds: float = Field(default=1.0, gt=0)
    heartbeat_ttl_seconds: float = Field(default=5.0, gt=0)
    probe_timeout_seconds: float = Field(default=1.0, gt=0)
    age_reference_seconds: float = Field(default=5.0, gt=0)
    work_reference_tokens: float = Field(default=4096.0, gt=0)
    weights: PriorityWeights = PriorityWeights()
    policy: Literal["prefill_slack", "weighted"] = "prefill_slack"
    cost_model: OfflineCostModel | None = None
    prefix_ttl_seconds: float = Field(default=2.0, gt=0)

    @model_validator(mode="after")
    def heartbeat_window(self) -> AdmissionConfig:
        if self.heartbeat_ttl_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("heartbeat TTL must exceed its interval")
        return self


@dataclass(frozen=True)
class RequestPriority:
    key: tuple[str, str]
    job_id: str
    arrived_at: datetime
    workflow_started_at: datetime
    deadline: datetime | None
    blocking_lines: int = 0
    prompt_tokens: int | None = None
    work: RequestWork | None = None

    @property
    def cp_seconds(self) -> float:
        return max(0.0, (self.arrived_at - self.workflow_started_at).total_seconds())


def priority_score(
    request: RequestPriority,
    config: AdmissionConfig,
    *,
    now: datetime,
    age_seconds: float,
    job_inflight: int = 0,
) -> dict[str, float]:
    budget = (
        max(0.001, (request.deadline - request.workflow_started_at).total_seconds())
        if request.deadline is not None
        else 60.0
    )
    urgency = 0.0
    if request.deadline is not None:
        remaining = (request.deadline - now).total_seconds()
        urgency = (
            budget / (budget + remaining)
            if remaining >= 0
            else 1.0 + min(1.0, -remaining / budget)
        )
    p = request.prompt_tokens
    features = {
        "slo": urgency,
        "age": max(0.0, age_seconds) / config.age_reference_seconds,
        "progress": min(1.0, request.cp_seconds / budget),
        "release": request.blocking_lines / (1.0 + request.blocking_lines),
        # Unknown work has no cost contribution. It is exposed as unknown,
        # never reported as a zero-token prompt or as a cache hit.
        "cost": -p / (config.work_reference_tokens + p) if p is not None else 0.0,
        "fairness": -job_inflight / (1.0 + job_inflight),
    }
    return {
        name: value * getattr(config.weights, name) for name, value in features.items()
    }


@dataclass
class _Waiting:
    request: RequestPriority
    sequence: int
    entered: float
    queue_work_before_tokens: int
    queue_work_complete: bool
    future: asyncio.Future[dict[str, Any]]


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
        self._inflight: dict[tuple[str, str], str] = {}
        self._sequence = 0
        self._healthy = False
        self._heartbeat_at = -math.inf
        self._closed = False
        self._refresh_work = refresh_work
        self._refresh_task: asyncio.Task[None] | None = None

    def _available(self) -> bool:
        return (
            self._healthy
            and (
                time.monotonic() - self._heartbeat_at
                < self.config.heartbeat_ttl_seconds
            )
            and not self._closed
        )

    async def heartbeat(self, healthy: bool) -> None:
        async with self._lock:
            self._healthy = healthy
            self._heartbeat_at = time.monotonic()
            self._dispatch()

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
                time.monotonic()
                - max(0.0, (datetime.now(UTC) - request.arrived_at).total_seconds()),
                0,
                True,
                asyncio.get_running_loop().create_future(),
            )
            order = self._order(self._projection(waiting))
            preceding = [
                e
                for e in self._waiting.values()
                if self._order(self._projection(e)) <= order
            ]
            waiting.queue_work_before_tokens = sum(
                e.request.prompt_tokens or 0 for e in preceding
            )
            waiting.queue_work_complete = all(
                e.request.prompt_tokens is not None for e in preceding
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
            if waiting is not None and not waiting.future.done():
                waiting.future.cancel()
            self._inflight.pop(key, None)
            self._dispatch()

    async def refresh_dependencies(
        self, job_id: str, lines: list[dict[str, Any]]
    ) -> None:
        async with self._lock:
            for line in lines:
                call_id = line.get("llm_call_id")
                if not isinstance(call_id, str):
                    continue
                key = (job_id, call_id)
                item = self._waiting.get(key)
                if item is not None:
                    item.request = replace(
                        item.request, blocking_lines=int(line["blocking_line_count"])
                    )
            self._dispatch()

    def _projection(self, item: _Waiting) -> dict[str, Any]:
        request = item.request
        work = request.work
        age = time.monotonic() - item.entered
        remaining = (
            (request.deadline - request.workflow_started_at).total_seconds()
            - request.cp_seconds
            - age
            if request.deadline is not None
            else None
        )
        cost = work.cost_seconds if work is not None else None
        contributions = priority_score(
            item.request,
            self.config,
            now=datetime.now(UTC),
            age_seconds=time.monotonic() - item.entered,
            job_inflight=sum(j == item.request.job_id for j in self._inflight.values()),
        )
        return {
            "job_id": item.request.job_id,
            "llm_call_id": item.request.key[1],
            "score": sum(contributions.values()),
            "contributions": contributions,
            "cp_seconds": item.request.cp_seconds,
            "prompt_tokens": item.request.prompt_tokens,
            "work_basis": work.cost_basis
            if work is not None
            else "tokenizer_cold"
            if item.request.prompt_tokens is not None
            else "unknown",
            "prefix_basis": work.prefix_basis if work else "COLD:no_target_proof",
            "queue_work_before_tokens": item.queue_work_before_tokens,
            "queue_work_complete": item.queue_work_complete,
            "sequence": item.sequence,
            "policy": self.config.policy,
            "age_seconds": age,
            "remaining_slo_seconds": remaining,
            "prefill_slack_seconds": remaining - cost
            if remaining is not None and cost is not None
            else None,
            "ordering_basis": "prefill_slack"
            if cost is not None
            else "deadline_only:cost_unknown",
            "work": work.model_dump() if work is not None else None,
            "job_inflight": sum(j == request.job_id for j in self._inflight.values()),
            "blocking_lines": request.blocking_lines,
            "latest_prefill_start": request.deadline.timestamp()
            - (cost if cost is not None else 0.0)
            if request.deadline is not None
            else None,
        }

    def _dispatch(self) -> None:
        if self._refresh_work is not None:
            if self._waiting and not self._closed and self._refresh_task is None:
                self._refresh_task = asyncio.create_task(self._refresh_and_dispatch())
            return
        self._dispatch_ready()

    def _order(self, projection: dict[str, Any]) -> tuple:
        if self.config.policy == "weighted":
            return (-projection["score"], projection["sequence"])
        latest = projection["latest_prefill_start"]
        return (
            latest if latest is not None else math.inf,
            projection["job_inflight"] if self.config.weights.fairness else 0,
            -projection["blocking_lines"],
            -projection["age_seconds"],
            projection["sequence"],
        )

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
                self._dispatch_ready(set(snapshot))
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
                if any(key not in snapshot for key in self._waiting):
                    self._dispatch()

    def _dispatch_ready(self, eligible: set[tuple[str, str]] | None = None) -> None:
        for item in self._waiting.values():
            work = item.request.work
            if (
                work is not None
                and work.observed_at_monotonic is not None
                and (
                    time.monotonic() - work.observed_at_monotonic
                    >= self.config.prefix_ttl_seconds
                )
            ):
                item.request = replace(
                    item.request,
                    work=RequestWork(
                        prompt_tokens=work.prompt_tokens,
                        prefill_tokens=work.prompt_tokens,
                        prefix_basis="COLD:expired_full_sweep_observation",
                        cost_basis="unknown:stale_prefix",
                    ),
                )
        while self._available() and len(self._inflight) < self.config.limit:
            cancelled = [k for k, v in self._waiting.items() if v.future.cancelled()]
            for key in cancelled:
                self._waiting.pop(key)
            if not self._waiting:
                break
            projections = {
                k: self._projection(v)
                for k, v in self._waiting.items()
                if eligible is None or k in eligible
            }
            if not projections:
                break
            key = min(
                projections,
                key=lambda k: self._order(projections[k]),
            )
            item = self._waiting.pop(key)
            self._inflight[key] = item.request.job_id
            item.future.set_result(projections[key])

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "healthy": self._available(),
                "capacity_source": "configured_gateway_limit",
                "limit": self.config.limit,
                "inflight": len(self._inflight),
                "free": max(0, self.config.limit - len(self._inflight)),
                "weights": self.config.weights.model_dump(),
                "queued": sorted(
                    (self._projection(v) for v in self._waiting.values()),
                    key=self._order,
                ),
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


def priority_from_snapshot(
    *,
    key: tuple[str, str],
    snapshot: dict[str, Any],
    arrived_at: datetime,
    prompt_tokens: int | None,
) -> RequestPriority:
    deadline = snapshot.get("deadline") or snapshot.get("job_deadline")
    return RequestPriority(
        key=key,
        job_id=key[0],
        arrived_at=arrived_at,
        workflow_started_at=datetime.fromisoformat(snapshot["workflow_started_at"]),
        deadline=datetime.fromisoformat(deadline) if deadline else None,
        blocking_lines=int(snapshot.get("blocking_line_count", 0)),
        prompt_tokens=prompt_tokens,
    )
