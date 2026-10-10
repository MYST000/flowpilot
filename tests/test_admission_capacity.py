from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from test_admission import priority

from flowpilot.frontier.store import LineTailFrontier
from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.scheduling.admission import (
    AdmissionConfig,
    AdmissionQueue,
)
from flowpilot.scheduling.capacity import (
    AdaptiveAdmissionConfig,
    CapacityFeedback,
    EngineLoad,
)
from flowpilot.scheduling.cost import RequestWork
from flowpilot.scheduling.runtime import SchedulingRuntime


@pytest.fixture
def clock(monkeypatch):
    import flowpilot.scheduling.admission as module

    now = [1000.0]
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: now[0]))
    return now


def config(*, limit=6, adaptive=False):
    return AdmissionConfig(
        enabled=True,
        limit=limit,
        adaptive=AdaptiveAdmissionConfig(
            enabled=adaptive, initial_limit=6, min_limit=4, decrease_step=2
        ),
    )


def test_controls_require_consistent_explicit_configuration():
    assert not AdmissionConfig().adaptive.enabled
    with pytest.raises(ValidationError, match="exceeds admission"):
        AdmissionConfig(adaptive=AdaptiveAdmissionConfig(enabled=True))


@pytest.mark.asyncio
async def test_waiters_without_deadlines_are_productive_demand(clock):
    queue = AdmissionQueue(config(limit=8, adaptive=True))
    tasks = [asyncio.create_task(queue.acquire(priority(str(i)))) for i in range(7)]
    await asyncio.sleep(0)
    await queue.engine_load(EngineLoad(0, 0, 0, 0))
    for i in range(1, 5):
        clock[0] += 30
        await queue.engine_load(EngineLoad(0, 0, 0, i * 30))
    assert (await queue.snapshot())["effective_limit"] == 7
    await queue.heartbeat(True)
    await asyncio.gather(*tasks)
    for i in range(7):
        await queue.release(("job", str(i)))
    await queue.close()


def test_pressure_hysteresis_bounds_and_recovery_require_productive_demand():
    feedback = CapacityFeedback(AdaptiveAdmissionConfig(enabled=True), 32)
    feedback.observe(EngineLoad(24, 10, 0, 0), 0, demand=True)
    for i in range(1, 8):
        feedback.observe(EngineLoad(24, 10, i, i * 30), i * 30, demand=True)
        if i == 2:
            assert feedback.limit == 24
        if i == 3:
            assert feedback.limit == 20
    assert feedback.limit == 16
    for i in range(8, 11):
        feedback.observe(EngineLoad(16, 0, 7, i * 30), i * 30, demand=True)
    assert feedback.limit == 17
    for i in range(11, 15):
        feedback.observe(EngineLoad(0, 0, 7, 300), i * 30, demand=False)
    assert feedback.limit == 17  # Low load with no completions is not recovery.
    for i in range(15, 80):
        feedback.observe(EngineLoad(32, 0, 7, i * 30), i * 30, demand=True)
    assert feedback.limit == 32


def test_increasing_throughput_does_not_trigger_pressure_backoff():
    feedback = CapacityFeedback(AdaptiveAdmissionConfig(enabled=True), 32)
    completed = 0
    for i in range(7):
        completed += 30 * (i + 1)
        feedback.observe(EngineLoad(24, 10, i, completed), i * 30, demand=True)
    assert feedback.limit == 24


def test_missing_reset_or_stale_metrics_never_mean_zero_pressure():
    feedback = CapacityFeedback(AdaptiveAdmissionConfig(enabled=True), 32)
    for i in range(3):
        feedback.observe(EngineLoad(24, 10, i + 10, i * 30 + 100), i * 30, demand=True)
    assert feedback.snapshot(60)["pressure_windows"] == 1
    feedback.unavailable("HTTPError")
    assert feedback.snapshot(61)["status"] == "metrics_unavailable"
    assert feedback.limit == 24
    feedback.observe(EngineLoad(24, 10, 14, 220), 90, demand=True)
    assert feedback.snapshot(90)["pressure_windows"] == 0
    feedback.observe(EngineLoad(1, 0, 0, 0), 120, demand=False)
    assert feedback.snapshot(120)["reason"] == "counter_reset"
    assert feedback.snapshot(151)["status"] == "metrics_stale"
    feedback.observe(EngineLoad(24, 0, 0, 100), 200, demand=True)
    assert feedback.snapshot(200)["reason"] == "sampling_gap"
    assert feedback.limit == 24


@pytest.mark.asyncio
async def test_reduction_below_inflight_waits_for_natural_credit_return(clock):
    queue = AdmissionQueue(config(adaptive=True))
    await queue.heartbeat(True)
    for i in range(6):
        await queue.acquire(priority(f"live{i}"))
    pending = asyncio.create_task(queue.acquire(priority("next")))
    await asyncio.sleep(0)
    await queue.engine_load(EngineLoad(6, 10, 0, 0))
    for i in range(1, 4):
        clock[0] += 30
        await queue.heartbeat(True)
        await queue.engine_load(EngineLoad(6, 10, i, i * 30))
    state = await queue.snapshot()
    assert state["limit"] == 6 and state["effective_limit"] == 4
    assert state["inflight"] == 6 and state["free"] == 0
    for i in range(2):
        await queue.release(("job", f"live{i}"))
        assert not pending.done()
    await queue.release(("job", "live2"))
    assert (await asyncio.wait_for(pending, 1))["effective_limit"] == 4
    await queue.close()


def metrics():
    return "\n".join(
        f'{name}{{engine="0",model_name="model name"}} {value}'
        for name, value in [
            ("vllm:num_requests_running", 2),
            ("vllm:num_requests_waiting", 0),
            ("vllm:num_preemptions_total", 3),
            ("vllm:e2e_request_latency_seconds_count", 10),
        ]
    )


@pytest.mark.asyncio
async def test_capacity_reduction_during_prefix_query_is_checked_at_dispatch(clock):
    started, finish = asyncio.Event(), asyncio.Event()

    async def refresh(requests):
        started.set()
        await finish.wait()
        return {r.key: RequestWork(cost_seconds=1) for r in requests}

    queue = AdmissionQueue(config(adaptive=True), refresh)
    await queue.heartbeat(True)
    pending = [
        asyncio.create_task(queue.acquire(priority(f"live{i}"))) for i in range(6)
    ]
    await asyncio.wait_for(started.wait(), 1)
    await queue.engine_load(EngineLoad(6, 10, 0, 0))
    for i in range(1, 4):
        clock[0] += 30
        await queue.heartbeat(True)
        await queue.engine_load(EngineLoad(6, 10, i, i * 30))
    assert (await queue.snapshot())["effective_limit"] == 4
    finish.set()
    for task in pending[:4]:
        await asyncio.wait_for(asyncio.shield(task), 1)
    state = await queue.snapshot()
    assert state["inflight"] == 4 and len(state["queued"]) == 2
    for task in pending[4:]:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await queue.close()


@pytest.mark.parametrize("invalid", ["missing", "nan", "negative", "duplicate"])
def test_engine_metrics_are_required_finite_and_unambiguous(invalid):
    text = metrics()
    assert EngineLoad.from_prometheus(text) == EngineLoad(2, 0, 3, 10)
    if invalid == "missing":
        text = "\n".join(text.splitlines()[:-1])
    elif invalid == "nan":
        text = text.replace("} 10", "} NaN")
    elif invalid == "negative":
        text = text.replace("} 10", "} -1")
    else:
        text += "\n" + text.splitlines()[0]
    with pytest.raises(ValueError):
        EngineLoad.from_prometheus(text)


@pytest.mark.asyncio
async def test_runtime_reads_actual_metrics_and_reports_unavailability():
    calls = []
    malformed = False

    def upstream(request):
        calls.append(request.url.path)
        assert request.headers["Authorization"] == "Bearer engine-key"
        if request.url.path == "/health":
            return httpx.Response(200)
        assert request.url.path == "/metrics"
        return httpx.Response(200, text="" if malformed else metrics())

    sink = InMemoryTraceSink()
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        runtime = SchedulingRuntime(
            client,
            "http://engine/v1",
            config(adaptive=True),
            LineTailFrontier(),
            TraceRecorder(sink),
            api_key="engine-key",
        )
        await runtime._heartbeat()
        await runtime._engine_load()
        state = (await runtime.snapshot())["admission"]
        assert state["effective_limit"] == 6
        assert state["adaptive"]["load"]["completed"] == 10
        assert sink.records[-1]["event_type"] == "admission_capacity_observation"
        malformed = True
        await runtime._engine_load()
        state = (await runtime.snapshot())["admission"]
        assert state["healthy"] is True
        assert state["adaptive"]["status"] == "metrics_unavailable"
        assert state["effective_limit"] == 6
        assert calls == ["/health", "/metrics", "/metrics"]
        await runtime.close()
