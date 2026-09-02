from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Protocol

from flowpilot.protocol import PHASE4_PROTOCOL_VERSION, ForecastRequest, ForecastResult

type ForecastKey = tuple[str, str, str]


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


def _forecast_key(request: ForecastRequest) -> ForecastKey:
    return (request.job_id, request.line_id, request.request_id)


def _key_to_dict(key: ForecastKey) -> dict[str, str]:
    job_id, line_id, request_id = key
    return {
        "job_id": job_id,
        "line_id": line_id,
        "request_id": request_id,
    }


class ForecastManager:
    """Scoped forecast store with asynchronous TTL/capacity reclamation."""

    def __init__(
        self,
        adapter: ForecastAdapter | None = None,
        *,
        timeout_seconds: float = 0.25,
        ttl_seconds: float = 30.0,
        min_confidence: float = 0.0,
        on_event: Callable[
            [str, ForecastRequest, ForecastResult | None, str | None], Awaitable[None]
        ]
        | None = None,
        on_prewarm: Callable[[ForecastRequest, ForecastResult], Awaitable[None]]
        | None = None,
        sweep_interval_seconds: float | None = None,
        max_entries: int = 10_000,
    ) -> None:
        if timeout_seconds <= 0 or ttl_seconds <= 0:
            raise ValueError("forecast timeout and TTL must be positive")
        if not 0 <= min_confidence <= 1:
            raise ValueError("forecast minimum confidence must be between 0 and 1")
        if max_entries <= 0:
            raise ValueError("forecast max_entries must be positive")
        self.adapter = adapter or NoOpForecastAdapter()
        self.timeout_seconds = timeout_seconds
        self.ttl_seconds = ttl_seconds
        self.min_confidence = min_confidence
        self.on_event = on_event
        self.on_prewarm = on_prewarm
        self.max_entries = max_entries
        self.sweep_interval_seconds = sweep_interval_seconds or max(
            0.05, min(ttl_seconds / 4.0, 1.0)
        )
        self._tasks: dict[ForecastKey, asyncio.Task[None]] = {}
        self._results: dict[ForecastKey, ForecastResult] = {}
        self._requests: dict[ForecastKey, ForecastRequest] = {}
        self._lock = asyncio.Lock()
        self._sweeper: asyncio.Task[None] | None = None

    async def close(self) -> None:
        async with self._lock:
            sweeper = self._sweeper
            self._sweeper = None
            tasks = tuple(self._tasks.values())
            self._tasks.clear()
        if sweeper is not None:
            sweeper.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(
            *(item for item in (*tasks, sweeper) if item), return_exceptions=True
        )

    async def start(self, request: ForecastRequest) -> None:
        key = _forecast_key(request)
        evicted: tuple[ForecastRequest, ForecastResult | None] | None = None
        async with self._lock:
            self._ensure_sweeper_locked()
            prior = self._tasks.pop(key, None)
            if prior is not None:
                prior.cancel()
            if key not in self._requests and len(self._requests) >= self.max_entries:
                oldest = next(iter(self._requests))
                oldest_task = self._tasks.pop(oldest, None)
                if oldest_task is not None:
                    oldest_task.cancel()
                evicted = (
                    self._requests.pop(oldest),
                    self._results.pop(oldest, None),
                )
            self._tasks[key] = asyncio.create_task(self._run(request))
            self._requests[key] = request
            await self._trim_locked()
        if evicted is not None:
            self._schedule_adapter_cancel(evicted[0].request_id)
            await self._emit("forecast_discarded", evicted[0], evicted[1], "capacity")

    async def cancel(
        self,
        request_id: str,
        *,
        job_id: str | None = None,
        line_id: str | None = None,
    ) -> None:
        keys = await self._resolve_keys(request_id, job_id, line_id)
        async with self._lock:
            tasks = [self._tasks.pop(key, None) for key in keys]
            for key in keys:
                self._requests.pop(key, None)
                self._results.pop(key, None)
            for task in tasks:
                if task is not None:
                    task.cancel()
        for key in keys:
            self._schedule_adapter_cancel(key[2])
        await asyncio.sleep(0)

    async def supersede(
        self,
        request_id: str,
        *,
        reason: str = "factual_tool_call",
        job_id: str | None = None,
        line_id: str | None = None,
    ) -> None:
        keys = await self._resolve_keys(request_id, job_id, line_id)
        discarded: list[tuple[ForecastRequest, ForecastResult | None]] = []
        async with self._lock:
            for key in keys:
                task = self._tasks.pop(key, None)
                result = self._results.pop(key, None)
                request = self._requests.pop(key, None)
                if task is not None:
                    task.cancel()
                if request is not None:
                    discarded.append((request, result))
        for key in keys:
            self._schedule_adapter_cancel(key[2])
        await asyncio.sleep(0)
        for request, result in discarded:
            await self._emit("forecast_discarded", request, result, reason)

    async def result(
        self,
        request_id: str,
        *,
        job_id: str | None = None,
        line_id: str | None = None,
    ) -> ForecastResult | None:
        keys = await self._resolve_keys(request_id, job_id, line_id)
        if not keys:
            return None
        key = keys[0]
        async with self._lock:
            result = self._results.get(key)
            request = self._requests.get(key)
            expired = result is not None and result.expires_at <= datetime.now(UTC)
            if expired:
                self._results.pop(key, None)
                self._requests.pop(key, None)
        if expired:
            if request is not None:
                await self._emit("forecast_discarded", request, result, "expired")
            return None
        return result

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "active_requests": [_key_to_dict(key) for key in sorted(self._tasks)],
                "results": [
                    {**_key_to_dict(key), **result.model_dump(mode="json")}
                    for key, result in self._results.items()
                ],
            }

    async def sweep(self) -> int:
        now = datetime.now(UTC)
        expired: list[tuple[ForecastRequest, ForecastResult | None]] = []
        async with self._lock:
            for key, result in tuple(self._results.items()):
                if result.expires_at <= now:
                    self._results.pop(key, None)
                    request = self._requests.pop(key, None)
                    if request is not None:
                        expired.append((request, result))
            await self._trim_locked()
        for request, result in expired:
            await self._emit("forecast_discarded", request, result, "expired")
        return len(expired)

    async def _run(self, request: ForecastRequest) -> None:
        await self._emit("forecast_request", request, None, None)
        reason: str | None = None
        try:
            result = await asyncio.wait_for(
                self._forecast_adapter(request), timeout=self.timeout_seconds
            )
        except asyncio.CancelledError:
            await self._emit("forecast_discarded", request, None, "cancelled")
            await self._remove_task(request)
            return
        except TimeoutError:
            result, reason = None, "timeout"
        except Exception as exc:
            result, reason = None, f"error:{type(exc).__name__}"
        if result is None:
            await self._emit(
                "forecast_discarded", request, None, reason or "unavailable"
            )
            await self._remove_task(request)
            return
        reason = self._validate_result(request, result)
        if reason is not None:
            await self._emit("forecast_discarded", request, result, reason)
            await self._remove_task(request)
            return
        key = _forecast_key(request)
        async with self._lock:
            if self._requests.get(key) != request:
                return
            self._results[key] = result
            await self._trim_locked()
        if not await self._is_active(request):
            return
        await self._emit("forecast_result", request, result, None)
        if self.on_prewarm is not None:
            try:
                await self.on_prewarm(request, result)
            except Exception:
                await self._emit("forecast_discarded", request, result, "prewarm_error")
        await self._remove_task(request)

    def _validate_result(
        self, request: ForecastRequest, result: ForecastResult
    ) -> str | None:
        if result.schema_version != PHASE4_PROTOCOL_VERSION:
            return "incompatible_version"
        if result.based_on_request_id != request.request_id:
            return "request_id_mismatch"
        if any(
            value is not None and value != expected
            for value, expected in (
                (result.job_id, request.job_id),
                (result.line_id, request.line_id),
            )
        ):
            return "scope_mismatch"
        now = datetime.now(UTC)
        if result.expires_at <= now:
            return "expired"
        if result.expires_at > now + timedelta(seconds=self.ttl_seconds):
            return "ttl_exceeded"
        if result.confidence < self.min_confidence:
            return "low_confidence"
        if len(result.candidates) > request.requested_top_n:
            return "top_n_exceeded"
        if sum(item.probability for item in result.candidates) > 1.000001:
            return "probability_sum_exceeded"
        return None

    async def _remove_task(self, request: ForecastRequest) -> None:
        async with self._lock:
            self._tasks.pop(_forecast_key(request), None)

    async def _is_active(self, request: ForecastRequest) -> bool:
        async with self._lock:
            return self._requests.get(_forecast_key(request)) == request

    def _ensure_sweeper_locked(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = asyncio.create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.sweep_interval_seconds)
                await self.sweep()
        except asyncio.CancelledError:
            return

    async def _trim_locked(self) -> None:
        if len(self._results) <= self.max_entries:
            return
        ordered = sorted(self._results.items(), key=lambda item: item[1].expires_at)
        for key, _ in ordered[: len(self._results) - self.max_entries]:
            self._results.pop(key, None)
            self._requests.pop(key, None)

    async def _resolve_keys(
        self,
        request_id: str,
        job_id: str | None,
        line_id: str | None,
    ) -> tuple[ForecastKey, ...]:
        async with self._lock:
            supplied = (job_id, line_id)
            if any(item is not None for item in supplied):
                if not all(item is not None for item in supplied):
                    raise ValueError(
                        "job_id and line_id must be supplied together"
                    )
                return ((job_id or "", line_id or "", request_id),)
            matches = tuple(
                key
                for key in set((*self._tasks, *self._results, *self._requests))
                if key[2] == request_id
            )
            if len(matches) > 1:
                raise ValueError(
                    "forecast request_id is ambiguous; provide job/line"
                )
            return matches

    def _forecast_adapter(
        self, request: ForecastRequest
    ) -> Awaitable[ForecastResult | None]:
        method = getattr(self.adapter, "forecast_async", None) or self.adapter.forecast
        return method(request)

    async def _cancel_adapter(self, request_id: str) -> None:
        method = getattr(self.adapter, "cancel_forecast", None) or self.adapter.cancel
        await method(request_id)

    def _schedule_adapter_cancel(self, request_id: str) -> None:
        async def cancel() -> None:
            try:
                await self._cancel_adapter(request_id)
            except Exception:
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
