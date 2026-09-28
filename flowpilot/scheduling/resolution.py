from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from flowpilot.protocol import (
    ForecastRequest,
    ForecastResult,
    ToolResolutionKind,
    ToolResolutionRecord,
    ToolResolutionSource,
    ToolResolutionStatus,
)
from flowpilot.scheduling.duration import SyntheticToolDurationPrior

type ForecastKey = tuple[str, str, str]


class ToolResolutionStore:
    """Authoritative Tool facts and bounded, scoped forecast priors."""

    def __init__(
        self,
        *,
        forecast_max_entries: int = 10_000,
        forecast_sweep_interval_seconds: float = 1.0,
        resolution_ttl_seconds: float = 3_600.0,
        resolution_max_entries: int = 50_000,
        duration_prior: SyntheticToolDurationPrior | None = None,
    ) -> None:
        if (
            forecast_max_entries <= 0
            or forecast_sweep_interval_seconds <= 0
            or resolution_ttl_seconds <= 0
            or resolution_max_entries <= 0
        ):
            raise ValueError("forecast/resolution capacity and TTL must be positive")
        self._records: dict[tuple[str, str, str, str], ToolResolutionRecord] = {}
        self._forecasts: dict[ForecastKey, ForecastResult] = {}
        self._forecast_max_entries = forecast_max_entries
        self._forecast_sweep_interval_seconds = forecast_sweep_interval_seconds
        self._resolution_ttl_seconds = resolution_ttl_seconds
        self._resolution_max_entries = resolution_max_entries
        self._duration_prior = duration_prior
        self._lock = asyncio.Lock()
        self._sweeper: asyncio.Task[None] | None = None

    async def close(self) -> None:
        async with self._lock:
            sweeper = self._sweeper
            self._sweeper = None
        if sweeper is not None:
            sweeper.cancel()
            await asyncio.gather(sweeper, return_exceptions=True)

    async def save_forecast(
        self,
        request: ForecastRequest | ForecastResult,
        result: ForecastResult | None = None,
    ) -> None:
        """Persist a prior under its full scope.

        Passing only ``ForecastResult`` is kept for old local callers; it is
        accepted only if the result carries explicit scope.
        """
        if isinstance(request, ForecastResult):
            result = request
            if not (result.job_id and result.line_id):
                raise ValueError(
                    "unscoped forecast cannot be stored; provide ForecastRequest"
                )
            key = (
                result.job_id,
                result.line_id,
                result.based_on_request_id,
            )
        else:
            if result is None:
                raise ValueError("forecast result is required")
            key = (
                request.job_id,
                request.line_id,
                request.tail_request_id or request.request_id,
            )
        async with self._lock:
            self._ensure_sweeper_locked()
            self._forecasts[key] = result
            self._trim_forecasts_locked()

    async def forecast(
        self,
        request_id: str,
        *,
        job_id: str | None = None,
        line_id: str | None = None,
    ) -> ForecastResult | None:
        if any(item is not None for item in (job_id, line_id)):
            if not (job_id and line_id):
                raise ValueError("job_id and line_id must be supplied together")
            key = (job_id, line_id, request_id)
        else:
            raise ValueError("forecast lookup requires job/line scope")
        async with self._lock:
            result = self._forecasts.get(key)
            if result is not None and result.expires_at <= datetime.now(UTC):
                self._forecasts.pop(key, None)
                return None
            return result

    async def sweep_forecasts(self) -> int:
        now = datetime.now(UTC)
        async with self._lock:
            expired = [
                key
                for key, result in self._forecasts.items()
                if result.expires_at <= now
            ]
            for key in expired:
                self._forecasts.pop(key, None)
            self._trim_forecasts_locked()
            return len(expired)

    async def sweep_resolutions(self) -> int:
        """Evict stale readiness records and enforce bounded storage."""
        cutoff = datetime.now(UTC) - timedelta(seconds=self._resolution_ttl_seconds)
        async with self._lock:
            expired = [
                key
                for key, record in self._records.items()
                if record.updated_at <= cutoff
            ]
            for key in expired:
                self._records.pop(key, None)
            removed = len(expired)
            if len(self._records) > self._resolution_max_entries:
                ordered = sorted(
                    self._records.items(), key=lambda item: item[1].updated_at
                )
                for key, _record in ordered[
                    : len(self._records) - self._resolution_max_entries
                ]:
                    self._records.pop(key, None)
                    removed += 1
            return removed

    async def observe_tool_call(
        self,
        *,
        job_id: str,
        line_id: str,
        tail_request_id: str,
        llm_call_id: str,
        tool_call_id: str,
        tool_family: str,
    ) -> ToolResolutionRecord:
        key = (job_id, line_id, tail_request_id, tool_call_id)
        async with self._lock:
            self._ensure_sweeper_locked()
            prior = self._records.get(key)
            if prior is not None:
                return prior
            record = ToolResolutionRecord(
                job_id=job_id,
                line_id=line_id,
                tail_request_id=tail_request_id,
                llm_call_id=llm_call_id,
                tool_call_id=tool_call_id,
                tool_family=tool_family,
                resolution=ToolResolutionKind.LOCAL_ONLY,
                status=ToolResolutionStatus.RESOLVING,
                source=ToolResolutionSource.LOCAL_MODEL,
                confidence=1.0,
                version=1,
                updated_at=datetime.now(UTC),
            )
            self._records[key] = record
            self._trim_resolutions_locked()
            return record

    async def update(
        self, record: ToolResolutionRecord, *, allow_stale: bool = False
    ) -> ToolResolutionRecord:
        key = self._key(record)
        async with self._lock:
            self._ensure_sweeper_locked()
            prior = self._records.get(key)
            if prior is not None:
                if record.version < prior.version and not allow_stale:
                    raise ValueError("Tool resolution version regressed")
                if record.version == prior.version and record != prior:
                    raise ValueError("Tool resolution version conflicts")
                if record.version <= prior.version:
                    return prior
            self._records[key] = record
            self._trim_resolutions_locked()
            return record

    async def resolve_reuse(
        self,
        *,
        identity: Any,
        tool_family: str,
        resolution: ToolResolutionKind,
        status: ToolResolutionStatus,
        source: ToolResolutionSource,
        ready_at_estimate: datetime | None = None,
        actual_latency_ms: float | None = None,
        actual_result_bytes: int | None = None,
        confidence: float = 1.0,
        execution_started_at: datetime | None = None,
    ) -> ToolResolutionRecord:
        key = (
            identity.job_id,
            identity.line_id,
            identity.tail_request_id,
            identity.tool_call_id,
        )
        async with self._lock:
            self._ensure_sweeper_locked()
            prior = self._records.get(key)
            version = prior.version + 1 if prior else 1
            duration_estimate_ms = prior.duration_estimate_ms if prior else None
            duration_estimate_basis = prior.duration_estimate_basis if prior else None
            if (
                status == ToolResolutionStatus.RESOLVING
                and ready_at_estimate is None
                and resolution
                in {ToolResolutionKind.LOCAL_LEADER, ToolResolutionKind.LOCAL_ONLY}
            ):
                if prior is not None and prior.ready_at_estimate is not None:
                    ready_at_estimate = prior.ready_at_estimate
                else:
                    forecast = self._forecasts.get(
                        (identity.job_id, identity.line_id, identity.tail_request_id)
                    )
                    if forecast is not None and forecast.expires_at <= datetime.now(
                        UTC
                    ):
                        forecast = None
                    # A multi-call response may consume the same candidate for
                    # several factual Tool Calls without creating new calls.
                    candidate = (
                        next(
                            (
                                item
                                for item in forecast.candidates
                                if item.tool_family == tool_family
                            ),
                            None,
                        )
                        if forecast is not None
                        else None
                    )
                    if candidate is not None:
                        duration_estimate_ms = candidate.duration_p50
                        duration_estimate_basis = "forecast_p50"
                        confidence = candidate.probability
                    elif self._duration_prior is not None:
                        duration_estimate_ms = self._duration_prior.estimate_ms(
                            tool_family
                        )
                        duration_estimate_basis = "synthetic_factual_family_v1"
                    if duration_estimate_ms is not None:
                        ready_at_estimate = datetime.now(UTC) + timedelta(
                            milliseconds=duration_estimate_ms
                        )
            record = ToolResolutionRecord(
                job_id=identity.job_id,
                line_id=identity.line_id,
                tail_request_id=identity.tail_request_id,
                llm_call_id=identity.llm_call_id,
                tool_call_id=identity.tool_call_id,
                tool_family=tool_family,
                resolution=resolution,
                status=status,
                source=source,
                ready_at_estimate=ready_at_estimate,
                duration_estimate_ms=duration_estimate_ms,
                duration_estimate_basis=duration_estimate_basis,
                actual_latency_ms=actual_latency_ms,
                execution_started_at=execution_started_at
                or (prior.execution_started_at if prior else None),
                actual_result_bytes=actual_result_bytes,
                confidence=confidence,
                version=version,
                updated_at=datetime.now(UTC),
            )
            self._records[key] = record
            self._trim_resolutions_locked()
            return record

    async def get_for_line(
        self, job_id: str, line_id: str, tail_request_id: str
    ) -> list[ToolResolutionRecord]:
        async with self._lock:
            return [
                item
                for key, item in self._records.items()
                if key[:3] == (job_id, line_id, tail_request_id)
            ]

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [item.model_dump(mode="json") for item in self._records.values()]

    def _ensure_sweeper_locked(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._forecast_sweep_interval_seconds)
                await self.sweep_forecasts()
                await self.sweep_resolutions()
        except asyncio.CancelledError:
            return

    def _trim_forecasts_locked(self) -> None:
        if len(self._forecasts) <= self._forecast_max_entries:
            return
        ordered = sorted(self._forecasts.items(), key=lambda item: item[1].expires_at)
        for key, _ in ordered[: len(self._forecasts) - self._forecast_max_entries]:
            self._forecasts.pop(key, None)

    def _trim_resolutions_locked(self) -> None:
        if len(self._records) <= self._resolution_max_entries:
            return
        ordered = sorted(self._records.items(), key=lambda item: item[1].updated_at)
        for key, _record in ordered[
            : len(self._records) - self._resolution_max_entries
        ]:
            self._records.pop(key, None)

    @staticmethod
    def _key(record: ToolResolutionRecord) -> tuple[str, str, str, str]:
        return (
            record.job_id,
            record.line_id,
            record.tail_request_id,
            record.tool_call_id,
        )
