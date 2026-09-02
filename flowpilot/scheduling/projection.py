from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

from flowpilot.frontier.store import LineTailFrontier
from flowpilot.protocol import KVTier, SchedulingProjection
from flowpilot.scheduling.kv import KVDirectory
from flowpilot.scheduling.resolution import ToolResolutionStore


def slo_urgency(
    *,
    now: datetime,
    arrival_at: datetime,
    deadline: datetime | None,
    max_urgency: float = 100.0,
    overtime_lambda: float = 2.0,
    epsilon: float = 0.01,
) -> float:
    if deadline is None:
        return 1.0
    original_slo = max((deadline - arrival_at).total_seconds(), epsilon)
    remaining = (deadline - now).total_seconds()
    urgency = original_slo / (max(remaining, 0.0) + epsilon * original_slo)
    if remaining < 0:
        urgency += overtime_lambda * (-remaining) / original_slo
    return min(max_urgency, max(0.0, urgency))


def dag_importance(
    *,
    blocking_line_count: int,
    downstream_depth: int = 0,
    wait_age_ms: float = 0.0,
    alpha: float = 0.5,
    beta: float = 0.5,
    gamma: float = 0.2,
    max_depth: int = 8,
    age_reference_ms: float = 1_000.0,
) -> float:
    return (
        1.0
        + alpha * math.log1p(max(blocking_line_count, 0))
        + beta * (max(downstream_depth, 0) / max(max_depth, 1))
        + gamma * (max(wait_age_ms, 0.0) / max(age_reference_ms, 1.0))
    )


class ProjectionCalculator:
    """Builds ephemeral projections from frontier, resolution, and KV facts."""

    def __init__(
        self,
        frontier: LineTailFrontier,
        resolutions: ToolResolutionStore,
        kv_directory: KVDirectory | None = None,
    ) -> None:
        self.frontier = frontier
        self.resolutions = resolutions
        self.kv_directory = kv_directory or KVDirectory()

    async def for_line(
        self,
        job_id: str,
        line_id: str,
        *,
        now: datetime | None = None,
        estimated_inference_ms: float | None = None,
        arrival_at: datetime | None = None,
        downstream_depth: int = 0,
        continuation_cost_ms: float = 0.0,
        kv_instance_id: str | None = None,
        kv_session_id: str | None = None,
    ) -> SchedulingProjection:
        if continuation_cost_ms < 0:
            raise ValueError("continuation cost cannot be negative")
        now = now or datetime.now(UTC)
        snapshot = await self.frontier.line_snapshot(job_id, line_id)
        deadline = _parse_datetime(snapshot.get("deadline")) or _parse_datetime(
            snapshot.get("job_deadline")
        )
        workflow_started_at = _parse_datetime(snapshot.get("workflow_started_at"))
        if arrival_at is None:
            arrival_at = now
        if downstream_depth <= 0:
            downstream_depth = await self._downstream_depth(job_id, line_id)
        wait_age_ms = 0.0
        records = []
        tail_request_id = snapshot.get("tail_request_id")
        if tail_request_id:
            records = await self.resolutions.get_for_line(
                job_id, line_id, tail_request_id
            )
        unresolved = [
            item
            for item in records
            if item.status not in {"ready", "failed", "cancelled"}
        ]
        ready = snapshot["phase"] in {"READY", "EMPTY"} and not unresolved
        t_need = None
        if records:
            estimates = [
                item.ready_at_estimate
                for item in records
                if item.ready_at_estimate is not None
            ]
            if estimates:
                t_need = max(estimates) + timedelta(milliseconds=continuation_cost_ms)
                wait_age_ms = max(0.0, (now - min(estimates)).total_seconds() * 1000)
        urgency = slo_urgency(
            now=now,
            arrival_at=arrival_at,
            deadline=deadline,
        )
        importance = dag_importance(
            blocking_line_count=int(snapshot.get("blocking_line_count", 0)),
            downstream_depth=downstream_depth,
            wait_age_ms=wait_age_ms,
        )
        weight = float(snapshot.get("weight", 1.0)) * importance * urgency
        laxity = None
        t_kv = None
        kv_cost_ms = None
        kv_supported = False
        if kv_instance_id is not None and kv_session_id is not None:
            fact = await self.kv_directory.get(
                kv_instance_id,
                kv_session_id,
                job_id=job_id,
                line_id=line_id,
            )
            if fact is not None:
                if fact.tier is KVTier.GPU:
                    kv_cost_ms = 0.0
                elif fact.tier in {KVTier.CPU, KVTier.NVME}:
                    kv_cost_ms = fact.restore_cost_ms
                elif fact.tier is KVTier.DROPPED:
                    kv_cost_ms = fact.rematerialization_cost_ms
                if kv_cost_ms is not None:
                    kv_supported = True
                    t_kv = now + timedelta(milliseconds=kv_cost_ms)
        if t_need is not None and kv_cost_ms is not None:
            laxity = (t_need - now).total_seconds() * 1000 - kv_cost_ms
        t2 = max(t_need, t_kv) if t_need is not None and t_kv is not None else t_need
        return SchedulingProjection(
            job_id=job_id,
            line_id=line_id,
            tail_request_id=tail_request_id,
            tail_version=int(snapshot["version"]),
            ready=ready,
            t_need=t_need,
            t_kv=t_kv,
            t2=t2,
            estimated_inference_ms=estimated_inference_ms,
            request_weight=max(0.0, weight),
            kv_restore_laxity_ms=laxity,
            dag_importance=importance,
            slo_urgency=urgency,
            blocking_line_count=int(snapshot.get("blocking_line_count", 0)),
            wait_age_ms=wait_age_ms,
            workflow_age_ms=(
                max(0.0, (now - workflow_started_at).total_seconds() * 1000)
                if workflow_started_at is not None
                else 0.0
            ),
            deadline_slack_ms=(
                (deadline - now).total_seconds() * 1000
                if deadline is not None
                else None
            ),
            kv_telemetry="supported" if kv_supported else "unsupported",
            computed_at=now,
        )

    async def validate_current(self, projection: SchedulingProjection) -> bool:
        """Check the tail identity immediately before an action is executed."""
        snapshot = await self.frontier.line_snapshot(
            projection.job_id, projection.line_id
        )
        return (
            int(snapshot["version"]) == projection.tail_version
            and snapshot.get("tail_request_id") == projection.tail_request_id
        )

    async def _downstream_depth(self, job_id: str, line_id: str) -> int:
        snapshot = await self.frontier.snapshot(job_id)
        reverse: dict[str, list[str]] = {
            str(item["line_id"]): [] for item in snapshot["lines"]
        }
        for item in snapshot["lines"]:
            waiter = str(item["line_id"])
            for prerequisite in item.get("dependencies", []):
                reverse.setdefault(str(prerequisite), []).append(waiter)

        def depth(current: str, visiting: set[str]) -> int:
            if current in visiting:
                return 0
            children = reverse.get(current, [])
            if not children:
                return 0
            next_visiting = {*visiting, current}
            return 1 + max(depth(child, next_visiting) for child in children)

        return depth(line_id, set())


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        return None
    return result if result.tzinfo is not None else None
