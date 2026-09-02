from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from flowpilot.protocol import (
    KVAction,
    KVActionKind,
    KVLease,
    KVTier,
    SchedulingProjection,
)
from flowpilot.scheduling.kv import (
    KVActionResult,
    KVFact,
    KVStale,
    KVUnsupported,
    VLLMKVAdapter,
    to_state_fact,
)


@dataclass(frozen=True, slots=True)
class ResourceCapacities:
    """Independent physical budgets; no cross-type byte arithmetic exists."""

    tool_cache_bytes: int
    kv_gpu_bytes: int
    kv_cpu_bytes: int
    kv_nvme_bytes: int

    def __post_init__(self) -> None:
        if (
            min(
                self.tool_cache_bytes,
                self.kv_gpu_bytes,
                self.kv_cpu_bytes,
                self.kv_nvme_bytes,
            )
            < 0
        ):
            raise ValueError("resource capacities cannot be negative")

    def tool_admissible(self, used_bytes: int, candidate_bytes: int) -> bool:
        return used_bytes + candidate_bytes <= self.tool_cache_bytes

    def kv_admissible(
        self, tier: KVTier, used_bytes: int, candidate_bytes: int
    ) -> bool:
        capacity = {
            KVTier.GPU: self.kv_gpu_bytes,
            KVTier.CPU: self.kv_cpu_bytes,
            KVTier.NVME: self.kv_nvme_bytes,
            KVTier.DROPPED: 0,
        }[tier]
        return tier is KVTier.DROPPED or used_bytes + candidate_bytes <= capacity


@dataclass(frozen=True, slots=True)
class Request2Timing:
    t_need: datetime
    t_kv: datetime | None
    t2: datetime
    restore_laxity_ms: float | None
    kv_telemetry: str


def request2_timing(
    *,
    tool_ready_at: datetime,
    continuation_cost_ms: float,
    fact: KVFact | None,
    now: datetime | None = None,
) -> Request2Timing:
    """Compute T_need, measured T_KV and T2 without estimating KV facts."""
    if continuation_cost_ms < 0:
        raise ValueError("continuation cost cannot be negative")
    now = now or datetime.now(UTC)
    t_need = tool_ready_at + timedelta(milliseconds=continuation_cost_ms)
    if fact is None:
        return Request2Timing(t_need, None, t_need, None, "unsupported")
    if fact.tier is KVTier.GPU:
        t_kv = now
        restore_cost = 0.0
    elif fact.tier in {KVTier.CPU, KVTier.NVME} and fact.restore_cost_ms is not None:
        restore_cost = fact.restore_cost_ms
        t_kv = now + timedelta(milliseconds=restore_cost)
    elif fact.tier is KVTier.DROPPED and fact.rematerialization_cost_ms is not None:
        restore_cost = fact.rematerialization_cost_ms
        t_kv = now + timedelta(milliseconds=restore_cost)
    else:
        return Request2Timing(t_need, None, t_need, None, "unsupported")
    t2 = max(t_need, t_kv)
    laxity = (t_need - now).total_seconds() * 1000.0 - restore_cost
    return Request2Timing(t_need, t_kv, t2, laxity, "supported")


@dataclass(frozen=True, slots=True)
class RestoreQueueEntry:
    projection: SchedulingProjection
    fact: KVFact
    enqueued_at: datetime
    cooldown_until: datetime | None = None

    def laxity_ms(self, now: datetime) -> float:
        if self.projection.t_need is None or self.fact.restore_cost_ms is None:
            return float("inf")
        return (
            self.projection.t_need - now
        ).total_seconds() * 1000.0 - self.fact.restore_cost_ms

    def priority(self, now: datetime, epsilon_ms: float = 1.0) -> float:
        laxity = self.laxity_ms(now)
        queue_age_ms = max(0.0, (now - self.enqueued_at).total_seconds() * 1000.0)
        age_boost = 1.0 + queue_age_ms / 10_000.0
        return (
            self.projection.request_weight * age_boost / (max(laxity, 0.0) + epsilon_ms)
        )


class RestoreQueue:
    """Multi-request restore queue with overdue and cooldown semantics."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str, str], RestoreQueueEntry] = {}
        self._lock = asyncio.Lock()

    async def upsert(self, entry: RestoreQueueEntry) -> None:
        key = (
            entry.projection.job_id,
            entry.projection.line_id,
            entry.fact.kv_handle,
        )
        async with self._lock:
            prior = self._entries.get(key)
            if prior is not None:
                entry = replace(entry, enqueued_at=prior.enqueued_at)
            self._entries[key] = entry

    async def remove(self, entry: RestoreQueueEntry) -> None:
        key = (
            entry.projection.job_id,
            entry.projection.line_id,
            entry.fact.kv_handle,
        )
        async with self._lock:
            self._entries.pop(key, None)

    async def ordered(
        self, now: datetime | None = None
    ) -> tuple[RestoreQueueEntry, ...]:
        now = now or datetime.now(UTC)
        async with self._lock:
            eligible = [
                item
                for item in self._entries.values()
                if item.cooldown_until is None or item.cooldown_until <= now
            ]
        return tuple(
            sorted(
                eligible,
                key=lambda item: (item.laxity_ms(now) <= 0, item.priority(now)),
                reverse=True,
            )
        )

    async def overdue(
        self, now: datetime | None = None
    ) -> tuple[RestoreQueueEntry, ...]:
        now = now or datetime.now(UTC)
        return tuple(
            item for item in await self.ordered(now) if item.laxity_ms(now) <= 0
        )


class TemporalKVCoordinator:
    """Lease/fencing coordinator; vLLM remains the action executor."""

    def __init__(
        self,
        adapter: VLLMKVAdapter,
        validate_projection: Callable[[SchedulingProjection], Awaitable[bool]],
        *,
        lease_ttl_seconds: float = 5.0,
        migration_cooldown_seconds: float = 1.0,
    ) -> None:
        self.adapter = adapter
        self.validate_projection = validate_projection
        self.lease_ttl_seconds = lease_ttl_seconds
        self.migration_cooldown_seconds = migration_cooldown_seconds
        self.restore_queue = RestoreQueue()
        self._last_action: dict[tuple[str, str, str], datetime] = {}

    async def acquire(self, fact: KVFact, owner: str) -> KVLease:
        if self.adapter.schema_version != "flowpilot-vllm-kv-v2":
            raise KVUnsupported("kv_telemetry=unsupported")
        return await self.adapter.acquire_lease(
            to_state_fact(fact), owner, self.lease_ttl_seconds
        )

    async def renew(self, lease: KVLease) -> KVLease:
        return await self.adapter.renew_lease(lease, self.lease_ttl_seconds)

    async def release(self, lease: KVLease) -> None:
        await self.adapter.release_lease(lease)

    async def execute(
        self,
        kind: KVActionKind,
        projection: SchedulingProjection,
        fact: KVFact,
        lease: KVLease,
        *,
        target_tier: KVTier | None = None,
        now: datetime | None = None,
    ) -> KVActionResult:
        now = now or datetime.now(UTC)
        if not await self.validate_projection(projection):
            raise KVStale("tail request/version changed before KV action")
        action_key = (fact.instance_id, fact.engine_epoch, fact.kv_handle)
        last = self._last_action.get(action_key)
        if (
            last is not None
            and kind not in {KVActionKind.KEEP, KVActionKind.RESTORE}
            and (now - last).total_seconds() < self.migration_cooldown_seconds
        ):
            raise KVStale("KV action is inside migration cooldown")
        material = ":".join(
            (
                projection.job_id,
                projection.line_id,
                projection.tail_request_id or "none",
                str(projection.tail_version),
                fact.engine_epoch,
                fact.kv_handle,
                str(fact.generation),
                kind.value,
                target_tier.value if target_tier else "none",
            )
        )
        key = hashlib.sha256(material.encode()).hexdigest()
        action = KVAction(
            action_id=f"action-{key[:24]}",
            idempotency_key=f"kv-{key}",
            action=kind,
            target_tier=(
                target_tier or KVTier.CPU if kind == KVActionKind.OFFLOAD else None
            ),
            job_id=projection.job_id,
            line_id=projection.line_id,
            instance_id=fact.instance_id,
            engine_epoch=fact.engine_epoch,
            session_id=fact.session_id,
            kv_handle=fact.kv_handle,
            generation=fact.generation,
            lease_id=lease.lease_id,
            fencing_token=lease.fencing_token,
            expected_tail_request_id=projection.tail_request_id,
            expected_tail_version=projection.tail_version,
        )
        result = await self.adapter.execute(action)
        if result.status == "applied":
            self._last_action[action_key] = now
        return result


def wait_age_tier_action(
    fact: KVFact,
    *,
    wait_age_seconds: float,
    cpu_pressure: bool = False,
    nvme_pressure: bool = False,
    cpu_age_seconds: float = 30.0,
    nvme_age_seconds: float = 120.0,
) -> KVActionKind:
    """Prediction-independent fallback using KV-local age/pressure only."""
    return wait_age_tier_decision(
        fact,
        wait_age_seconds=wait_age_seconds,
        cpu_pressure=cpu_pressure,
        nvme_pressure=nvme_pressure,
        cpu_age_seconds=cpu_age_seconds,
        nvme_age_seconds=nvme_age_seconds,
    ).action


@dataclass(frozen=True, slots=True)
class KVTierDecision:
    action: KVActionKind
    target_tier: KVTier | None = None


def wait_age_tier_decision(
    fact: KVFact,
    *,
    wait_age_seconds: float,
    cpu_pressure: bool = False,
    nvme_pressure: bool = False,
    cpu_age_seconds: float = 30.0,
    nvme_age_seconds: float = 120.0,
) -> KVTierDecision:
    """Return a KV-local tier transition without reading Tool capacity."""
    if fact.tier is KVTier.GPU and cpu_pressure:
        return KVTierDecision(KVActionKind.OFFLOAD, KVTier.CPU)
    if fact.tier is KVTier.CPU and wait_age_seconds >= cpu_age_seconds and cpu_pressure:
        return KVTierDecision(KVActionKind.OFFLOAD, KVTier.NVME)
    if (
        fact.tier is KVTier.NVME
        and wait_age_seconds >= nvme_age_seconds
        and nvme_pressure
    ):
        return KVTierDecision(KVActionKind.DROP)
    return KVTierDecision(KVActionKind.KEEP)


@dataclass(frozen=True, slots=True)
class AlignmentSnapshot:
    reason: str
    projection: SchedulingProjection
    fact: KVFact | None
    computed_at: datetime


class RollingAlignmentController:
    """Recompute affected request-2 views on factual scheduling events."""

    def __init__(
        self,
        projection_calculator: Any,
        kv_directory: Any,
        *,
        history_limit: int = 10_000,
    ) -> None:
        self.projection_calculator = projection_calculator
        self.kv_directory = kv_directory
        self.restore_queue = RestoreQueue()
        self.history_limit = history_limit
        self._latest: dict[tuple[str, str, str | None], AlignmentSnapshot] = {}
        self._lock = asyncio.Lock()

    async def recompute_line(
        self,
        job_id: str,
        line_id: str,
        *,
        reason: str,
        continuation_cost_ms: float = 0.0,
    ) -> tuple[AlignmentSnapshot, ...]:
        facts = await self.kv_directory.facts_for_line(job_id, line_id)
        inputs: tuple[KVFact | None, ...] = facts or (None,)
        snapshots: list[AlignmentSnapshot] = []
        for fact in inputs:
            projection = await self.projection_calculator.for_line(
                job_id,
                line_id,
                continuation_cost_ms=continuation_cost_ms,
                kv_instance_id=fact.instance_id if fact else None,
                kv_session_id=fact.session_id if fact else None,
            )
            snapshot = AlignmentSnapshot(reason, projection, fact, datetime.now(UTC))
            snapshots.append(snapshot)
            if (
                fact is not None
                and projection.t_need is not None
                and fact.tier is not KVTier.GPU
                and fact.restore_cost_ms is not None
            ):
                await self.restore_queue.upsert(
                    RestoreQueueEntry(projection, fact, snapshot.computed_at)
                )
            async with self._lock:
                key = (
                    job_id,
                    line_id,
                    fact.kv_handle if fact else None,
                )
                self._latest[key] = snapshot
                while len(self._latest) > self.history_limit:
                    self._latest.pop(next(iter(self._latest)))
        return tuple(snapshots)

    async def snapshot(self) -> tuple[AlignmentSnapshot, ...]:
        async with self._lock:
            return tuple(self._latest.values())
