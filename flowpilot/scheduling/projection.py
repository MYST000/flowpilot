from __future__ import annotations

from datetime import UTC, datetime, timedelta

from flowpilot.frontier.store import LineTailFrontier
from flowpilot.protocol import SchedulingProjection
from flowpilot.scheduling.resolution import ToolResolutionStore


class ProjectionCalculator:
    """Builds ephemeral factual Tool-readiness projections."""

    def __init__(
        self,
        frontier: LineTailFrontier,
        resolutions: ToolResolutionStore,
    ) -> None:
        self.frontier = frontier
        self.resolutions = resolutions

    async def for_line(
        self,
        job_id: str,
        line_id: str,
        *,
        now: datetime | None = None,
        continuation_cost_ms: float = 0.0,
    ) -> SchedulingProjection:
        if continuation_cost_ms < 0:
            raise ValueError("continuation cost cannot be negative")
        now = now or datetime.now(UTC)
        snapshot = await self.frontier.line_snapshot(job_id, line_id)
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
            if estimates and all(
                item.ready_at_estimate is not None for item in unresolved
            ):
                t_need = max(estimates) + timedelta(milliseconds=continuation_cost_ms)
            # Runtime executes local calls in provider order. Absolute follower
            # estimates can overlap; unstarted local durations cannot.
            if unresolved and all(
                item.duration_estimate_ms is not None
                or item.ready_at_estimate is not None
                for item in unresolved
            ):
                cursor = now
                for item in unresolved:
                    if item.duration_estimate_ms is not None and item.resolution in {
                        "local_only",
                        "local_leader",
                    }:
                        duration = timedelta(milliseconds=item.duration_estimate_ms)
                        cursor = (
                            max(cursor, item.execution_started_at + duration)
                            if item.execution_started_at
                            else cursor + duration
                        )
                    elif item.ready_at_estimate is not None:
                        cursor = max(cursor, item.ready_at_estimate)
                t_need = cursor + timedelta(milliseconds=continuation_cost_ms)
        if snapshot.get("dependencies"):
            ready = False
            t_need = None
        return SchedulingProjection(
            job_id=job_id,
            line_id=line_id,
            tail_request_id=tail_request_id,
            tail_version=int(snapshot["version"]),
            ready=ready,
            t_need=t_need,
            blocking_line_count=int(snapshot.get("blocking_line_count", 0)),
            dependencies=tuple(snapshot.get("dependencies", [])),
            unresolved_tool_count=len(unresolved),
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
