from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Protocol

from flowpilot.protocol import (
    PHASE4_PROTOCOL_VERSION,
    ForecastRequest,
    ForecastResult,
)


class ForecastAdapter(Protocol):
    """External predictor boundary. Implementations must not execute Tools."""

    async def forecast(self, request: ForecastRequest) -> ForecastResult | None: ...

    async def cancel(self, request_id: str) -> None: ...


class NoOpForecastAdapter:
    async def forecast(self, request: ForecastRequest) -> ForecastResult | None:
        del request
        return None

    async def forecast_async(self, request: ForecastRequest) -> ForecastResult | None:
        return await self.forecast(request)

    async def cancel(self, request_id: str) -> None:
        del request_id
        return None

    async def cancel_forecast(self, request_id: str) -> None:
        await self.cancel(request_id)


class TraceReplayForecastAdapter:
    """Deterministic adapter for local replay; input keys are request IDs."""

    def __init__(self, results: Mapping[str, ForecastResult]) -> None:
        self._results = dict(results)
        self.cancelled: set[str] = set()

    async def forecast(self, request: ForecastRequest) -> ForecastResult | None:
        if request.request_id in self.cancelled:
            return None
        return self._results.get(request.request_id)

    async def forecast_async(self, request: ForecastRequest) -> ForecastResult | None:
        return await self.forecast(request)

    async def cancel(self, request_id: str) -> None:
        self.cancelled.add(request_id)

    async def cancel_forecast(self, request_id: str) -> None:
        await self.cancel(request_id)


class ForecastManager:
    """Owns forecast TTL/cancellation/degradation and metadata-only prewarm."""

    def __init__(
        self,
        adapter: ForecastAdapter | None = None,
        *,
        timeout_seconds: float = 0.25,
        ttl_seconds: float = 30.0,
        min_confidence: float = 0.0,
        on_event: Callable[[str, ForecastRequest, ForecastResult | None, str | None],
                           Awaitable[None]] | None = None,
        on_prewarm: Callable[[ForecastRequest, ForecastResult], Awaitable[None]]
        | None = None,
    ) -> None:
        if timeout_seconds <= 0 or ttl_seconds <= 0:
            raise ValueError("forecast timeout and TTL must be positive")
        if not 0 <= min_confidence <= 1:
            raise ValueError("forecast minimum confidence must be between 0 and 1")
        self.adapter = adapter or NoOpForecastAdapter()
        self.timeout_seconds = timeout_seconds
        self.ttl_seconds = ttl_seconds
        self.min_confidence = min_confidence
        self.on_event = on_event
        self.on_prewarm = on_prewarm
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._results: dict[str, ForecastResult] = {}
        self._requests: dict[str, ForecastRequest] = {}
        self._lock = asyncio.Lock()

    async def start(self, request: ForecastRequest) -> None:
        async with self._lock:
            prior = self._tasks.pop(request.request_id, None)
            if prior is not None:
                prior.cancel()
            task = asyncio.create_task(self._run(request))
            self._tasks[request.request_id] = task
            self._requests[request.request_id] = request

    async def cancel(self, request_id: str) -> None:
        async with self._lock:
            task = self._tasks.pop(request_id, None)
            self._requests.pop(request_id, None)
            if task is not None:
                task.cancel()
        self._schedule_adapter_cancel(request_id)
        await asyncio.sleep(0)

    async def supersede(
        self, request_id: str, *, reason: str = "factual_tool_call"
    ) -> None:
        """Discard a forecast once a factual Tool Call resolves its purpose."""
        async with self._lock:
            task = self._tasks.pop(request_id, None)
            result = self._results.pop(request_id, None)
            request = self._requests.pop(request_id, None)
            if task is not None:
                task.cancel()
        self._schedule_adapter_cancel(request_id)
        await asyncio.sleep(0)
        if request is not None:
            await self._emit(
                "forecast_discarded",
                request,
                result,
                reason,
            )

    async def result(self, request_id: str) -> ForecastResult | None:
        async with self._lock:
            result = self._results.get(request_id)
            request = self._requests.get(request_id)
            expired = result is not None and result.expires_at <= datetime.now(UTC)
            if expired:
                self._results.pop(request_id, None)
                self._requests.pop(request_id, None)
        if expired:
            if request is not None:
                await self._emit("forecast_discarded", request, result, "expired")
            return None
        if result is None:
            return None
        return result

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "active_requests": sorted(self._tasks),
                "results": [
                    result.model_dump(mode="json") for result in self._results.values()
                ],
            }

    async def _run(self, request: ForecastRequest) -> None:
        await self._emit("forecast_request", request, None, None)
        reason: str | None = None
        result: ForecastResult | None = None
        try:
            result = await asyncio.wait_for(
                self._forecast_adapter(request), timeout=self.timeout_seconds
            )
        except asyncio.CancelledError:
            reason = "cancelled"
            await self._emit("forecast_discarded", request, None, reason)
            await self._remove_task(request.request_id)
            return
        except TimeoutError:
            reason = "timeout"
        except Exception as exc:
            reason = f"error:{type(exc).__name__}"
        if result is None:
            await self._emit(
                "forecast_discarded", request, None, reason or "unavailable"
            )
            await self._remove_task(request.request_id)
            return
        reason = self._validate_result(request, result)
        if reason is not None:
            await self._emit("forecast_discarded", request, result, reason)
            await self._remove_task(request.request_id)
            return
        async with self._lock:
            if self._requests.get(request.request_id) != request:
                return
            self._results[request.request_id] = result
        if not await self._is_active(request.request_id, request):
            return
        await self._emit("forecast_result", request, result, None)
        if self.on_prewarm is not None:
            try:
                await self.on_prewarm(request, result)
            except Exception:
                await self._emit("forecast_discarded", request, result, "prewarm_error")
        await self._remove_task(request.request_id)

    def _validate_result(
        self, request: ForecastRequest, result: ForecastResult
    ) -> str | None:
        if result.schema_version != PHASE4_PROTOCOL_VERSION:
            return "incompatible_version"
        if result.based_on_request_id != request.request_id:
            return "request_id_mismatch"
        now = datetime.now(UTC)
        if result.expires_at <= now:
            return "expired"
        if result.expires_at > now + timedelta(seconds=self.ttl_seconds):
            return "ttl_exceeded"
        if result.confidence < self.min_confidence:
            return "low_confidence"
        if len(result.candidates) > request.requested_top_n:
            return "top_n_exceeded"
        total = sum(item.probability for item in result.candidates)
        if total > 1.000001:
            return "probability_sum_exceeded"
        return None

    async def _remove_task(self, request_id: str) -> None:
        async with self._lock:
            self._tasks.pop(request_id, None)

    async def _is_active(self, request_id: str, request: ForecastRequest) -> bool:
        async with self._lock:
            return self._requests.get(request_id) == request

    def _forecast_adapter(
        self, request: ForecastRequest
    ) -> Awaitable[ForecastResult | None]:
        method = getattr(self.adapter, "forecast_async", None)
        if method is None:
            method = self.adapter.forecast
        return method(request)

    async def _cancel_adapter(self, request_id: str) -> None:
        method = getattr(self.adapter, "cancel_forecast", None)
        if method is None:
            method = self.adapter.cancel
        await method(request_id)

    def _schedule_adapter_cancel(self, request_id: str) -> None:
        async def cancel() -> None:
            try:
                await self._cancel_adapter(request_id)
            except Exception:
                # Predictor failure is deliberately non-fatal to the LLM path.
                return

        asyncio.create_task(cancel())

    async def _emit(
        self,
        event_type: str,
        request: ForecastRequest,
        result: ForecastResult | None,
        reason: str | None,
    ) -> None:
        if self.on_event is not None:
            await self.on_event(event_type, request, result, reason)
