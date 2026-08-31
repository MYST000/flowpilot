from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from flowpilot.protocol import KVStateEvent, KVTier, SchedulingProjection


@dataclass(frozen=True, slots=True)
class KVFact:
    session_id: str
    instance_id: str
    tier: KVTier
    bytes: int
    restore_cost_ms: float | None
    observed_at: datetime
    schema_version: str


@dataclass(frozen=True, slots=True)
class KVActionRecommendation:
    """Metadata-only action recommendation guarded by a tail projection."""

    action: str
    session_id: str
    instance_id: str
    tail_request_id: str | None
    tail_version: int
    execute_after: datetime | None
    reason: str


class KVDirectory:
    """Directory of genuine versioned KV telemetry; absent facts stay unsupported."""

    def __init__(self) -> None:
        self._facts: dict[tuple[str, str], KVFact] = {}
        self._lock = asyncio.Lock()

    async def record(self, event: KVStateEvent, *, supported: bool) -> KVFact | None:
        if not supported:
            return None
        fact = KVFact(
            session_id=event.session_id,
            instance_id=event.instance_id,
            tier=event.tier,
            bytes=event.bytes,
            restore_cost_ms=event.restore_cost_ms,
            observed_at=event.observed_at,
            schema_version="flowpilot-vllm-kv-v1",
        )
        async with self._lock:
            self._facts[(event.instance_id, event.session_id)] = fact
        return fact

    async def get(self, instance_id: str, session_id: str) -> KVFact | None:
        async with self._lock:
            return self._facts.get((instance_id, session_id))

    async def recommend_action(
        self,
        projection: SchedulingProjection,
        *,
        instance_id: str,
        session_id: str,
        now: datetime | None = None,
    ) -> KVActionRecommendation:
        now = now or datetime.now(UTC)
        fact = await self.get(instance_id, session_id)
        if fact is None:
            return KVActionRecommendation(
                "unsupported",
                session_id,
                instance_id,
                projection.tail_request_id,
                projection.tail_version,
                None,
                "kv_telemetry=unsupported",
            )
        if fact.tier is KVTier.GPU:
            return KVActionRecommendation(
                "keep",
                session_id,
                instance_id,
                projection.tail_request_id,
                projection.tail_version,
                None,
                "kv already resident on GPU",
            )
        if projection.t_need is None or fact.restore_cost_ms is None:
            return KVActionRecommendation(
                "offload",
                session_id,
                instance_id,
                projection.tail_request_id,
                projection.tail_version,
                None,
                "no factual request-2 readiness time",
            )
        start_at = projection.t_need - timedelta(milliseconds=fact.restore_cost_ms)
        return KVActionRecommendation(
            "restore",
            session_id,
            instance_id,
            projection.tail_request_id,
            projection.tail_version,
            max(start_at, now),
            "restore before factual T_need",
        )

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [
                {
                    "session_id": item.session_id,
                    "instance_id": item.instance_id,
                    "tier": item.tier.value,
                    "bytes": item.bytes,
                    "restore_cost_ms": item.restore_cost_ms,
                    "observed_at": item.observed_at.isoformat(),
                    "schema_version": item.schema_version,
                }
                for item in self._facts.values()
            ]
