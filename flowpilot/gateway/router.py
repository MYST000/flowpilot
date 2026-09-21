from __future__ import annotations

import asyncio
import heapq
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

import httpx

from flowpilot.config import InferenceInstance


class NoCompatibleInstance(RuntimeError):
    pass


class RoutingPolicy(StrEnum):
    ROUND_ROBIN = "round-robin"
    QUEUE_AWARE = "queue-aware"
    QUEUE_SLO = "queue+slo"
    QUEUE_SLO_BLOCKING = "queue+slo+blocking"


@dataclass(frozen=True, slots=True)
class InstanceLoadProfile:
    instance_id: str
    queue_depth: int = 0
    running_requests: int = 0
    ttft_ms: float = 0.0
    throughput_tokens_per_second: float = 1.0
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if (
            min(self.queue_depth, self.running_requests, self.ttft_ms) < 0
            or self.throughput_tokens_per_second <= 0
        ):
            raise ValueError("invalid inference load profile")


@dataclass(frozen=True, slots=True)
class RoutingRequest:
    job_id: str
    line_id: str
    request_weight: float = 1.0
    deadline: datetime | None = None
    blocking_line_count: int = 0
    wait_age_ms: float = 0.0
    job_weight: float = 1.0
    slack_ms: float | None = None

    def __post_init__(self) -> None:
        if self.slack_ms is not None and self.slack_ms < 0:
            raise ValueError("request slack cannot be negative")
        if self.request_weight <= 0 or self.job_weight <= 0:
            raise ValueError("request and fairness weights must be positive")
        if self.blocking_line_count < 0 or self.wait_age_ms < 0:
            raise ValueError("blocking count and wait age cannot be negative")


@dataclass(order=True, slots=True)
class _FairEntry:
    negative_priority: float
    sequence: int
    request: RoutingRequest = field(compare=False)


class WeightedFairRequestQueue:
    """Job virtual-time queue; line count does not mint extra share."""

    def __init__(self) -> None:
        self._job_queues: dict[str, list[_FairEntry]] = {}
        self._job_finish: dict[str, float] = {}
        self._job_weight: dict[str, float] = {}
        self._sequence = 0

    def push(self, request: RoutingRequest) -> None:
        weight = max(request.request_weight, 1e-6)
        job_key = request.job_id
        queue = self._job_queues.setdefault(job_key, [])
        job_weight = max(request.job_weight, 1e-6)
        prior_job_weight = self._job_weight.get(job_key)
        if queue and prior_job_weight != job_weight:
            raise ValueError("job weight changed while requests are queued")
        self._job_weight[job_key] = job_weight
        self._sequence += 1
        urgency = _urgency(request.deadline, request.slack_ms)
        age_boost = 1.0 + max(request.wait_age_ms, 0.0) / 10_000.0
        priority = (
            weight * (1.0 + urgency) * (1.0 + request.blocking_line_count) * age_boost
        )
        heapq.heappush(queue, _FairEntry(-priority, self._sequence, request))
        self._job_finish.setdefault(job_key, 0.0)

    def pop(self) -> RoutingRequest:
        active_jobs = [job_id for job_id, queue in self._job_queues.items() if queue]
        if not active_jobs:
            raise IndexError("fair request queue is empty")
        job_key = min(
            active_jobs,
            key=lambda key: (self._job_finish[key], self._job_queues[key][0].sequence),
        )
        entry = heapq.heappop(self._job_queues[job_key])
        self._job_finish[job_key] += 1.0 / self._job_weight[job_key]
        return entry.request

    def __len__(self) -> int:
        return sum(len(queue) for queue in self._job_queues.values())


class InferenceRouter:
    """Pluggable placement using load, SLO and blocking."""

    def __init__(
        self,
        instances: Iterable[InferenceInstance],
        *,
        policy: RoutingPolicy | str = RoutingPolicy.ROUND_ROBIN,
    ) -> None:
        self.instances = tuple(instances)
        if not self.instances:
            raise ValueError("at least one inference instance is required")
        self.policy = RoutingPolicy(policy)
        self._cursor = 0
        self._profiles: dict[str, InstanceLoadProfile] = {
            item.instance_id: InstanceLoadProfile(item.instance_id)
            for item in self.instances
        }
        self._lock = asyncio.Lock()

    async def update_load(self, profile: InstanceLoadProfile) -> None:
        if not self.contains(profile.instance_id):
            raise ValueError("unknown inference instance")
        async with self._lock:
            current = self._profiles.get(profile.instance_id)
            if current is not None and profile.updated_at < current.updated_at:
                return
            self._profiles[profile.instance_id] = profile

    async def candidates(
        self, model: str, request: RoutingRequest | None = None
    ) -> tuple[InferenceInstance, ...]:
        compatible = tuple(item for item in self.instances if item.supports(model))
        if not compatible:
            raise NoCompatibleInstance(f"no instance supports model {model!r}")
        async with self._lock:
            if self.policy is RoutingPolicy.ROUND_ROBIN:
                start = self._cursor % len(compatible)
                self._cursor += 1
                return compatible[start:] + compatible[:start]
            return tuple(
                sorted(compatible, key=lambda item: self._score(item, request))
            )

    def _score(
        self, instance: InferenceInstance, request: RoutingRequest | None
    ) -> float:
        profile = self._profiles[instance.instance_id]
        queue_cost = (
            profile.ttft_ms
            + 1000.0
            * (profile.queue_depth + profile.running_requests)
            / profile.throughput_tokens_per_second
        )
        if request is None or self.policy is RoutingPolicy.QUEUE_AWARE:
            multiplier = 1.0
        else:
            # Urgency and blocking are priority boosts.  Dividing the queue
            # estimate makes an urgent request prefer a less loaded instance,
            # instead of multiplying its cost and accidentally deprioritizing it.
            urgency = _urgency(request.deadline, request.slack_ms)
            multiplier = 1.0 / (1.0 + urgency * max(request.request_weight, 0.0))
            if self.policy is RoutingPolicy.QUEUE_SLO_BLOCKING:
                multiplier /= 1.0 + max(request.blocking_line_count, 0)
        return queue_cost * multiplier

    async def health(self, client: httpx.AsyncClient) -> dict[str, bool]:
        async def check(instance: InferenceInstance) -> tuple[str, bool]:
            suffix = "/models" if instance.base_url.endswith("/v1") else "/v1/models"
            try:
                response = await client.get(f"{instance.base_url}{suffix}", timeout=5)
            except httpx.HTTPError:
                return instance.instance_id, False
            return instance.instance_id, response.is_success

        return dict(await asyncio.gather(*(check(item) for item in self.instances)))

    def contains(self, instance_id: str) -> bool:
        return any(item.instance_id == instance_id for item in self.instances)


def _urgency(
    deadline: datetime | None,
    slack_ms: float | None = None,
    now: datetime | None = None,
) -> float:
    if slack_ms is not None:
        if slack_ms <= 0:
            return 100.0
        return min(100.0, 1000.0 / slack_ms)
    if deadline is None:
        return 0.0
    now = now or datetime.now(UTC)
    remaining = (deadline - now).total_seconds()
    return 100.0 if remaining <= 0 else min(100.0, 1.0 / max(remaining, 0.001))
