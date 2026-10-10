from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from flowpilot.config import InferenceInstance
from flowpilot.frontier.store import LineTailFrontier
from flowpilot.gateway.router import InferenceRouter
from flowpilot.gateway.service import LLMGateway
from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.protocol import (
    ForecastCandidate,
    ForecastRequest,
    ForecastResult,
    JobRegistration,
    LineRegistration,
    RequestIdentity,
    ToolResolutionKind,
    ToolResolutionSource,
    ToolResolutionStatus,
)
from flowpilot.scheduling import (
    ForecastManager,
    ProjectionCalculator,
    ToolResolutionStore,
)
from flowpilot.scheduling.duration import SyntheticToolDurationPrior


def _digest() -> str:
    return "a" * 64


def _forecast_request(request_id: str = "tail-1") -> ForecastRequest:
    return ForecastRequest(
        request_id=request_id,
        job_id="job-1",
        line_id="line-1",
        model_id="model-a",
        history_features_ref=f"tail:{request_id}",
        tool_catalog_version="catalog-v1",
        requested_top_n=2,
    )


def _forecast_result(
    request_id: str = "tail-1", *, expires_at: datetime | None = None
) -> ForecastResult:
    return ForecastResult(
        based_on_request_id=request_id,
        candidates=(
            ForecastCandidate(
                tool_family="web_search",
                probability=0.8,
                duration_p50=20,
                duration_p90=80,
            ),
        ),
        confidence=0.95,
        predictor_version="predictor-v1",
        expires_at=expires_at or datetime.now(UTC) + timedelta(seconds=5),
    )


class _GateAdapter:
    def __init__(self, result: ForecastResult | None = None) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled: list[str] = []
        self.result = result

    async def forecast(self, request: ForecastRequest) -> ForecastResult | None:
        self.started.set()
        await self.release.wait()
        return self.result

    async def cancel(self, request_id: str) -> None:
        self.cancelled.append(request_id)
        self.release.set()


@pytest.mark.anyio
async def test_forecast_is_non_blocking_and_factually_superseded() -> None:
    adapter = _GateAdapter(_forecast_result())
    events: list[tuple[str, str | None]] = []

    async def on_event(
        event_type: str,
        _request: ForecastRequest,
        _result: ForecastResult | None,
        reason: str | None,
    ) -> None:
        events.append((event_type, reason))

    manager = ForecastManager(adapter, on_event=on_event, timeout_seconds=1)
    await manager.start(_forecast_request())
    await asyncio.wait_for(adapter.started.wait(), timeout=0.2)
    assert await manager.result("tail-1") is None

    adapter.release.set()
    for _ in range(20):
        if await manager.result("tail-1") is not None:
            break
        await asyncio.sleep(0)
    assert await manager.result("tail-1") is not None

    await manager.supersede("tail-1")
    assert await manager.result("tail-1") is None
    assert "tail-1" in adapter.cancelled
    assert ("forecast_result", None) in events
    assert ("forecast_discarded", "factual_tool_call") in events


@pytest.mark.anyio
async def test_forecast_timeout_degrades_without_result() -> None:
    adapter = _GateAdapter(_forecast_result())
    events: list[tuple[str, str | None]] = []

    async def on_event(
        event_type: str,
        _request: ForecastRequest,
        _result: ForecastResult | None,
        reason: str | None,
    ) -> None:
        events.append((event_type, reason))

    manager = ForecastManager(adapter, on_event=on_event, timeout_seconds=0.001)
    await manager.start(_forecast_request())
    await asyncio.sleep(0.02)
    assert await manager.result("tail-1") is None
    assert ("forecast_discarded", "timeout") in events


@pytest.mark.anyio
async def test_resolution_uses_forecast_as_prior_then_actual_finish_overrides() -> None:
    resolutions = ToolResolutionStore()
    forecast = _forecast_result("logical-1")
    await resolutions.save_forecast(
        _forecast_request("logical-1").model_copy(update={"tail_request_id": "tail-1"}),
        forecast,
    )
    identity = RequestIdentity(
        job_id="job-1",
        line_id="line-1",
        request_id="logical-1",
        attempt=1,
        conversation_id=f"conversation-{'line-1'}",
        tail_request_id="tail-1",
        llm_call_id="llm-1",
        expected_tail_version=0,
        context_epoch=1,
        context_sequence=0,
        base_context_cursor="root",
        context_digest=_digest(),
    )
    record = await resolutions.observe_tool_call(
        job_id=identity.job_id,
        line_id=identity.line_id,
        tail_request_id=identity.tail_request_id,
        llm_call_id=identity.llm_call_id,
        tool_call_id="tool-1",
        tool_family="web_search",
    )
    assert record.status is ToolResolutionStatus.RESOLVING
    assert record.ready_at_estimate is None
    pending = await resolutions.resolve_reuse(
        identity=identity.model_copy(update={"tool_call_id": "tool-1"}),
        tool_family="web_search",
        resolution=ToolResolutionKind.LOCAL_ONLY,
        status=ToolResolutionStatus.RESOLVING,
        source=ToolResolutionSource.LOCAL_MODEL,
    )
    assert pending.ready_at_estimate is not None
    assert pending.duration_estimate_ms == 20
    assert pending.duration_estimate_basis == "forecast_p50"
    finished = await resolutions.resolve_reuse(
        identity=identity.model_copy(update={"tool_call_id": "tool-1"}),
        tool_family="web_search",
        resolution=ToolResolutionKind.LOCAL_ONLY,
        status=ToolResolutionStatus.READY,
        source=ToolResolutionSource.LOCAL_MODEL,
        actual_latency_ms=37,
        actual_result_bytes=512,
        ready_at_estimate=datetime.now(UTC),
    )
    assert finished.version == 3
    assert finished.actual_latency_ms == 37
    assert finished.actual_result_bytes == 512
    assert finished.status is ToolResolutionStatus.READY
    assert finished.duration_estimate_ms == 20


@pytest.mark.anyio
async def test_synthetic_duration_is_used_only_for_factual_local_misses() -> None:
    resolutions = ToolResolutionStore(
        duration_prior=SyntheticToolDurationPrior(seed=23)
    )
    identity = RequestIdentity(
        job_id="job-1",
        line_id="line-1",
        request_id="logical-1",
        attempt=1,
        conversation_id="conversation-1",
        tail_request_id="tail-1",
        llm_call_id="llm-1",
        expected_tail_version=0,
        context_epoch=1,
        context_sequence=0,
        base_context_cursor="root",
        context_digest=_digest(),
    )
    for tool_name, tool_id, lower, upper in (
        ("tavily-search", "tool-search-1", 1000, 2000),
        ("web_search", "tool-search-2", 1000, 2000),
        ("terminal", "tool-terminal", 100, 200),
    ):
        await resolutions.observe_tool_call(
            job_id=identity.job_id,
            line_id=identity.line_id,
            tail_request_id=identity.tail_request_id,
            llm_call_id=identity.llm_call_id,
            tool_call_id=tool_id,
            tool_family=tool_name,
        )
        local = await resolutions.resolve_reuse(
            identity=identity.model_copy(update={"tool_call_id": tool_id}),
            tool_family=tool_name,
            resolution=ToolResolutionKind.LOCAL_ONLY,
            status=ToolResolutionStatus.RESOLVING,
            source=ToolResolutionSource.LOCAL_MODEL,
        )
        assert local.duration_estimate_ms is not None
        assert lower <= local.duration_estimate_ms <= upper
        assert local.duration_estimate_basis == "synthetic_factual_family_v1"
        repeated = await resolutions.resolve_reuse(
            identity=identity.model_copy(update={"tool_call_id": tool_id}),
            tool_family=tool_name,
            resolution=ToolResolutionKind.LOCAL_ONLY,
            status=ToolResolutionStatus.RESOLVING,
            source=ToolResolutionSource.LOCAL_MODEL,
        )
        assert repeated.duration_estimate_ms == local.duration_estimate_ms
    hit = await resolutions.resolve_reuse(
        identity=identity.model_copy(update={"tool_call_id": "tool-hit"}),
        tool_family="web_search",
        resolution=ToolResolutionKind.HISTORICAL_HIT,
        status=ToolResolutionStatus.READY,
        source=ToolResolutionSource.WEB_HISTORY,
        ready_at_estimate=datetime.now(UTC),
    )
    assert hit.duration_estimate_ms is None


@pytest.mark.anyio
async def test_readiness_ignores_slo_weight_and_uses_independent_version() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-1", default_slo_ms=1000))
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-1",
            conversation_id=f"conversation-{'line-1'}",
            context_epoch=1,
            base_context_cursor="root",
            context_digest=_digest(),
            deadline=datetime.now(UTC) + timedelta(seconds=1),
            weight=2.0,
        )
    )
    projections = ProjectionCalculator(frontier, ToolResolutionStore())
    projection = await projections.for_line("job-1", "line-1")
    assert projection.ready is True
    assert projection.schema_version == "flowpilot-readiness-v1"
    assert projection.blocking_line_count == 0
    assert "request_weight" not in projection.model_dump()
    snapshot = await frontier.line_snapshot("job-1", "line-1")
    assert "request_weight" not in snapshot


@pytest.mark.anyio
async def test_projection_is_version_guarded() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-1",
            conversation_id=f"conversation-{'line-1'}",
            context_epoch=1,
            base_context_cursor="root",
            context_digest=_digest(),
        )
    )
    resolutions = ToolResolutionStore()
    projections = ProjectionCalculator(frontier, resolutions)
    projection = await projections.for_line("job-1", "line-1")
    assert await projections.validate_current(projection)
    assert projection.tail_version == 0
    await frontier.begin_request(
        RequestIdentity(
            job_id="job-1",
            line_id="line-1",
            request_id="request-1",
            attempt=1,
            conversation_id="conversation-line-1",
            tail_request_id="tail-1",
            llm_call_id="llm-1",
            expected_tail_version=0,
            context_epoch=1,
            context_sequence=0,
            base_context_cursor="root",
            context_digest=_digest(),
        ),
        model="model-a",
    )
    assert not await projections.validate_current(projection)
    current = await projections.for_line("job-1", "line-1")
    assert current.tail_request_id == "tail-1"
    assert await projections.validate_current(current)


@pytest.mark.anyio
async def test_gateway_starts_forecast_before_upstream_response() -> None:
    adapter = _GateAdapter()
    forecast = ForecastManager(adapter, timeout_seconds=1)
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-1",
            conversation_id=f"conversation-{'line-1'}",
            context_epoch=1,
            base_context_cursor="root",
            context_digest=_digest(),
        )
    )

    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "r1", "choices": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    gateway = LLMGateway(
        client,
        InferenceRouter((InferenceInstance("instance-a", "http://upstream"),)),
        frontier,
        TraceRecorder(InMemoryTraceSink()),
        ingress_api_key="key",
        require_ingress_auth=True,
        forecast_manager=forecast,
    )
    identity_headers = {
        "x-flowpilot-api-key": "key",
        "x-flowpilot-protocol-version": "flowpilot-phase0-v2",
        "x-flowpilot-job-id": "job-1",
        "x-flowpilot-line-id": "line-1",
        "x-flowpilot-tail-request-id": "tail-1",
        "x-flowpilot-llm-call-id": "llm-1",
        "x-flowpilot-tail-version": "0",
        "x-flowpilot-context-epoch": "1",
        "x-flowpilot-context-sequence": "0",
        "x-flowpilot-context-cursor": "root",
        "x-flowpilot-context-digest": _digest(),
        "x-flowpilot-request-id": "request-1",
        "x-flowpilot-request-attempt": "1",
        "x-flowpilot-conversation-id": "conversation-line-1",
    }
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[]}',
        headers=identity_headers,
        raw_query=b"",
    )
    assert response.status_code == 200
    await asyncio.wait_for(adapter.started.wait(), timeout=0.2)
    await forecast.cancel("tail-1")
    await client.aclose()
