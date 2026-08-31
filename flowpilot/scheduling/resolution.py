from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from flowpilot.protocol import (
    ForecastResult,
    ToolResolutionKind,
    ToolResolutionRecord,
    ToolResolutionSource,
    ToolResolutionStatus,
)


class ToolResolutionStore:
    """In-memory authoritative Tool resolution facts, isolated from LineTail."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str, str, str, str], ToolResolutionRecord] = {}
        self._forecasts: dict[str, ForecastResult] = {}
        self._lock = asyncio.Lock()

    async def save_forecast(self, result: ForecastResult) -> None:
        async with self._lock:
            self._forecasts[result.based_on_request_id] = result

    async def forecast(self, request_id: str) -> ForecastResult | None:
        async with self._lock:
            result = self._forecasts.get(request_id)
            if result is not None and result.expires_at <= datetime.now(UTC):
                self._forecasts.pop(request_id, None)
                return None
            return result

    async def observe_tool_call(
        self,
        *,
        tenant_id: str,
        job_id: str,
        line_id: str,
        tail_request_id: str,
        llm_call_id: str,
        tool_call_id: str,
        tool_family: str,
    ) -> ToolResolutionRecord:
        key = (tenant_id, job_id, line_id, tail_request_id, tool_call_id)
        async with self._lock:
            prior = self._records.get(key)
            if prior is not None:
                return prior
            forecast = self._forecasts.get(tail_request_id)
            if forecast is not None and forecast.expires_at <= datetime.now(UTC):
                self._forecasts.pop(tail_request_id, None)
                forecast = None
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
            ready_at = (
                datetime.now(UTC)
                + timedelta(milliseconds=candidate.duration_p50)
                if candidate is not None
                else None
            )
            record = ToolResolutionRecord(
                tenant_id=tenant_id,
                job_id=job_id,
                line_id=line_id,
                tail_request_id=tail_request_id,
                llm_call_id=llm_call_id,
                tool_call_id=tool_call_id,
                tool_family=tool_family,
                resolution=ToolResolutionKind.LOCAL_ONLY,
                status=ToolResolutionStatus.RESOLVING,
                source=ToolResolutionSource.LOCAL_MODEL,
                confidence=(candidate.probability if candidate is not None else 1.0),
                ready_at_estimate=ready_at,
                version=1,
                updated_at=datetime.now(UTC),
            )
            self._records[key] = record
            return record

    async def update(
        self,
        record: ToolResolutionRecord,
        *,
        allow_stale: bool = False,
    ) -> ToolResolutionRecord:
        key = self._key(record)
        async with self._lock:
            prior = self._records.get(key)
            if prior is not None:
                if record.version < prior.version and not allow_stale:
                    raise ValueError("Tool resolution version regressed")
                if record.version == prior.version and record != prior:
                    raise ValueError("Tool resolution version conflicts")
                if record.version <= prior.version:
                    return prior
            self._records[key] = record
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
    ) -> ToolResolutionRecord:
        key = (
            identity.tenant_id,
            identity.job_id,
            identity.line_id,
            identity.tail_request_id,
            identity.tool_call_id,
        )
        async with self._lock:
            prior = self._records.get(key)
            version = prior.version + 1 if prior else 1
            if (
                prior is not None
                and status == ToolResolutionStatus.RESOLVING
                and ready_at_estimate is None
            ):
                ready_at_estimate = prior.ready_at_estimate
            record = ToolResolutionRecord(
                tenant_id=identity.tenant_id,
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
                actual_latency_ms=actual_latency_ms,
                actual_result_bytes=actual_result_bytes,
                confidence=confidence,
                version=version,
                updated_at=datetime.now(UTC),
            )
            self._records[key] = record
            return record

    async def get_for_line(
        self, tenant_id: str, job_id: str, line_id: str, tail_request_id: str
    ) -> list[ToolResolutionRecord]:
        async with self._lock:
            return [
                item
                for key, item in self._records.items()
                if key[:4] == (tenant_id, job_id, line_id, tail_request_id)
            ]

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [item.model_dump(mode="json") for item in self._records.values()]

    @staticmethod
    def _key(record: ToolResolutionRecord) -> tuple[str, str, str, str, str]:
        return (
            record.tenant_id,
            record.job_id,
            record.line_id,
            record.tail_request_id,
            record.tool_call_id,
        )
