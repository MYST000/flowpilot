from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.gateway.router import (
    InferenceRouter,
    InstanceLoadProfile,
    RoutingPolicy,
    RoutingRequest,
    WeightedFairRequestQueue,
)
from flowpilot.observability.trace import JsonlTraceSink
from flowpilot.protocol import (
    ForecastCandidate,
    ForecastRequest,
    ForecastResult,
)
from flowpilot.scheduling import (
    DeterministicToolAnalysisAdapter,
    ForecastManager,
    ToolObservation,
    ToolResolutionStore,
)
from flowpilot.state import SharedStateConflict, SQLiteSharedStateBackend


def _forecast_request(job: str, request_id: str = "same-request") -> ForecastRequest:
    return ForecastRequest(
        request_id=request_id,
        job_id=job,
        line_id=f"line-{job}",
        model_id="model",
        history_features_ref="features:1",
        tool_catalog_version="catalog-1",
    )


def _forecast_result(
    request: ForecastRequest, *, lifetime: float = 1.0
) -> ForecastResult:
    return ForecastResult(
        based_on_request_id=request.request_id,
        job_id=request.job_id,
        line_id=request.line_id,
        candidates=(
            ForecastCandidate(
                tool_family="web", probability=1, duration_p50=10, duration_p90=20
            ),
        ),
        confidence=1,
        predictor_version="deterministic-1",
        expires_at=datetime.now(UTC) + timedelta(seconds=lifetime),
    )


class _ScopedAdapter:
    async def forecast(self, request: ForecastRequest) -> ForecastResult:
        return _forecast_result(request, lifetime=0.06)

    async def cancel(self, request_id: str) -> None:
        del request_id


@pytest.mark.anyio
async def test_forecast_identity_collision_and_background_ttl_cleanup() -> None:
    manager = ForecastManager(
        _ScopedAdapter(),
        ttl_seconds=1,
        sweep_interval_seconds=0.01,
    )
    one = _forecast_request("job-a")
    two = _forecast_request("job-b")
    await manager.start(one)
    await manager.start(two)
    await asyncio.sleep(0.01)
    with pytest.raises(ValueError, match="ambiguous"):
        await manager.result(one.request_id)
    assert (
        await manager.result(one.request_id, job_id=one.job_id, line_id=one.line_id)
        is not None
    )
    assert (
        await manager.result(two.request_id, job_id=two.job_id, line_id=two.line_id)
        is not None
    )
    await manager.supersede(one.request_id, job_id=one.job_id, line_id=one.line_id)
    assert await manager.result(one.request_id, job_id="job", line_id="line") is None
    assert (
        await manager.result(two.request_id, job_id=two.job_id, line_id=two.line_id)
        is not None
    )
    await asyncio.sleep(0.08)
    assert (await manager.snapshot())["results"] == []
    await manager.close()


@pytest.mark.anyio
async def test_resolution_forecast_scope_capacity_and_ttl() -> None:
    store = ToolResolutionStore(
        forecast_max_entries=1, forecast_sweep_interval_seconds=0.01
    )
    one, two = (
        _forecast_request("job-a", "one"),
        _forecast_request("job-b", "two"),
    )
    await store.save_forecast(one, _forecast_result(one, lifetime=0.05))
    await store.save_forecast(two, _forecast_result(two, lifetime=0.05))
    assert await store.forecast("one", job_id=one.job_id, line_id=one.line_id) is None
    assert (
        await store.forecast("two", job_id=two.job_id, line_id=two.line_id) is not None
    )
    await asyncio.sleep(0.07)
    assert await store.sweep_forecasts() in {0, 1}
    assert await store.forecast("two", job_id=two.job_id, line_id=two.line_id) is None
    await store.close()


@pytest.mark.anyio
async def test_forecast_manager_capacity_is_bounded() -> None:
    manager = ForecastManager(
        _ScopedAdapter(),
        ttl_seconds=1,
        max_entries=1,
    )
    await manager.start(_forecast_request("job-a", "one"))
    await asyncio.sleep(0.01)
    await manager.start(_forecast_request("job-b", "two"))
    await asyncio.sleep(0.01)
    snapshot = await manager.snapshot()
    results = cast(list[dict[str, object]], snapshot["results"])
    assert len(results) == 1
    assert results[0]["job_id"] == "job-b"
    await manager.close()


@pytest.mark.anyio
async def test_queue_slo_blocking_policy_and_job_fairness() -> None:
    instances = (InferenceInstance("a", "http://a"), InferenceInstance("b", "http://b"))
    router = InferenceRouter(instances, policy=RoutingPolicy.QUEUE_SLO_BLOCKING)
    now = datetime.now(UTC)
    await router.update_load(
        InstanceLoadProfile(
            "a",
            queue_depth=10,
            ttft_ms=100,
            throughput_tokens_per_second=10,
            updated_at=now,
        )
    )
    await router.update_load(
        InstanceLoadProfile(
            "b",
            queue_depth=0,
            ttft_ms=5,
            throughput_tokens_per_second=10,
            updated_at=now,
        )
    )
    request = RoutingRequest("job", "line", 2, now + timedelta(milliseconds=20), 4)
    assert (await router.candidates("model", request))[0].instance_id == "b"
    queue = WeightedFairRequestQueue()
    queue.push(RoutingRequest("job-a", "line-1"))
    queue.push(RoutingRequest("job-a", "line-2"))
    queue.push(RoutingRequest("job-b", "line-1"))
    assert queue.pop().job_id == "job-a"
    assert queue.pop().job_id == "job-b"
    same_job = WeightedFairRequestQueue()
    same_job.push(RoutingRequest("job", "normal"))
    same_job.push(
        RoutingRequest(
            "job",
            "urgent",
            deadline=now - timedelta(milliseconds=1),
            blocking_line_count=2,
        )
    )
    assert same_job.pop().line_id == "urgent"


def test_profile_costs_output_duration_and_hysteresis() -> None:
    adapter = DeterministicToolAnalysisAdapter(minimum_samples=2)
    first = adapter.observe(
        ToolObservation("web", 90, 90, 80, 100, 1000), inference_cost_ms=10
    )
    second = adapter.observe(
        ToolObservation("web", 90, 90, 70, 80, 800), inference_cost_ms=10
    )
    assert not first.heavy and second.heavy
    middle = adapter.observe(
        ToolObservation("web", 50, 50, 40, 60, 600), inference_cost_ms=50
    )
    assert middle.heavy
    light = adapter.observe(
        ToolObservation("web", 10, 10, 5, 20, 100), inference_cost_ms=90
    )
    assert not light.heavy
    assert light.calibration_status == "uncalibrated"
    assert adapter.project("web") == light


@pytest.mark.anyio
async def test_sqlite_shared_writer_crash_expiry_fencing_and_order(
    tmp_path: Path,
) -> None:
    path = tmp_path / "shared.sqlite"
    one = SQLiteSharedStateBackend(path)
    two = SQLiteSharedStateBackend(path)
    claim = await one.acquire(
        "frontier",
        "job/line",
        owner="worker-1",
        schema_version="v1",
        generation=1,
        ttl_seconds=0.05,
    )
    with pytest.raises(SharedStateConflict):
        await two.acquire(
            "frontier",
            "job/line",
            owner="worker-2",
            schema_version="v1",
            generation=1,
            ttl_seconds=1,
        )
    committed = await one.commit(
        claim, expected_state_version=0, sequence=1, payload_digest="a" * 64
    )
    assert (
        await one.commit(
            committed, expected_state_version=1, sequence=1, payload_digest="a" * 64
        )
        == committed
    )
    with pytest.raises(SharedStateConflict):
        await one.commit(
            committed, expected_state_version=1, sequence=3, payload_digest="b" * 64
        )
    await asyncio.sleep(0.06)
    replacement = await two.acquire(
        "frontier",
        "job/line",
        owner="worker-2",
        schema_version="v1",
        generation=1,
        ttl_seconds=1,
    )
    assert replacement.fencing_token > claim.fencing_token
    with pytest.raises(SharedStateConflict):
        await one.commit(
            committed, expected_state_version=1, sequence=2, payload_digest="b" * 64
        )
    with pytest.raises(SharedStateConflict):
        await one.acquire(
            "frontier",
            "job/line",
            owner="worker-3",
            schema_version="v2",
            generation=2,
            ttl_seconds=1,
        )
    await two.release(replacement)
    upgraded = await one.acquire(
        "frontier",
        "job/line",
        owner="worker-3",
        schema_version="v2",
        generation=2,
        ttl_seconds=1,
    )
    assert upgraded.generation == 2 and upgraded.last_sequence == 0
    await one.close()
    await two.close()


@pytest.mark.anyio
async def test_trace_rotation_and_restart_append(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    sink = JsonlTraceSink(path, max_bytes=80, backup_count=2)
    await sink.write({"event": "one", "padding": "x" * 50})
    await sink.write({"event": "two", "padding": "x" * 50})
    assert (tmp_path / "trace.jsonl.1").exists()
    restarted = JsonlTraceSink(path, max_bytes=1_000, backup_count=2)
    await restarted.write({"event": "after-restart"})
    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert [item["event"] for item in records] == ["two", "after-restart"]


def test_multi_worker_uses_explicit_backend_contract_and_fails_closed(
    tmp_path: Path,
) -> None:
    settings = Settings(
        instances=(InferenceInstance("a", "http://a"),),
        trace_path=tmp_path / "trace.jsonl",
        ingress_api_key="key",
        workers=2,
        shared_state_path=tmp_path / "shared.sqlite",
    )
    with pytest.raises(ValueError, match="multi-worker serving is fail-closed"):
        create_app(settings)
