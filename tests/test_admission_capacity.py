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
    BestEffortAdmissionConfig,
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


def config(*, limit=6, adaptive=False, relax=True):
    return AdmissionConfig(
        enabled=True,
        limit=limit,
        policy="slo_unexpired_first",
        best_effort=BestEffortAdmissionConfig(
            enabled=True, limit=2, ramp_step=2, relax_when_quiet=relax
        ),
        adaptive=AdaptiveAdmissionConfig(
            enabled=adaptive, initial_limit=6, min_limit=4, decrease_step=2
        ),
    )


def test_controls_require_consistent_explicit_configuration():
    assert not AdmissionConfig().best_effort.enabled
    assert not AdmissionConfig().adaptive.enabled
    with pytest.raises(ValidationError, match="requires slo_unexpired_first"):
        AdmissionConfig(best_effort=BestEffortAdmissionConfig(enabled=True))
    with pytest.raises(ValidationError, match="exceeds total"):
        AdmissionConfig(
            limit=1,
            policy="slo_unexpired_first",
            best_effort=BestEffortAdmissionConfig(enabled=True),
        )
    with pytest.raises(ValidationError, match="exceeds admission"):
        AdmissionConfig(adaptive=AdaptiveAdmissionConfig(enabled=True))


@pytest.mark.asyncio
async def test_background_quota_preserves_room_and_unknown_deadlines_count(clock):
    queue = AdmissionQueue(config())
    await queue.heartbeat(True)
    await queue.acquire(priority("expired", deadline=-1))
    await queue.acquire(priority("unknown"))
    held = asyncio.create_task(queue.acquire(priority("held", deadline=-1)))
    await asyncio.sleep(0)
    assert not held.done()
    live = await queue.acquire(priority("live", deadline=1000))
    assert live["slo_status"] == "unexpired"
    state = await queue.snapshot()
    assert state["inflight"] == 3 and state["free"] == 3
    assert state["best_effort_inflight"] == 2
    await queue.release(("job", "unknown"))
    await asyncio.wait_for(held, 1)
    await queue.release(("job", "unknown"))  # No duplicate credit.
    assert (await queue.snapshot())["inflight"] == 3
    await queue.close()


@pytest.mark.asyncio
async def test_expiry_of_inflight_calls_counts_without_preemption(clock):
    queue = AdmissionQueue(config(relax=False))
    await queue.heartbeat(True)
    for i in range(3):
        await queue.acquire(priority(f"live{i}", deadline=5))
    await queue.acquire(priority("expired", deadline=-1))
    clock[0] += 10
    await queue.heartbeat(True)
    pending = asyncio.create_task(queue.acquire(priority("next", deadline=-1)))
    await asyncio.sleep(0)
    state = await queue.snapshot()
    assert state["inflight"] == state["best_effort_inflight"] == 4
    assert state["best_effort"]["over_limit"] == 2
    for i in range(2):
        await queue.release(("job", f"live{i}"))
        assert not pending.done()
    await queue.release(("job", "live2"))
    await asyncio.wait_for(pending, 1)
    assert (await queue.snapshot())["best_effort_inflight"] == 2
    await queue.close()


@pytest.mark.asyncio
async def test_quiet_relaxation_uses_time_not_event_count_and_is_not_proof(clock):
    queue = AdmissionQueue(config())
    await queue.heartbeat(True)
    pending = [
        asyncio.create_task(queue.acquire(priority(f"old{i}", deadline=-1)))
        for i in range(8)
    ]
    await asyncio.sleep(0)
    for _ in range(100):
        await queue.heartbeat(True)
    assert (await queue.snapshot())["inflight"] == 2
    clock[0] += 59
    await queue.heartbeat(True)
    assert (await queue.snapshot())["best_effort"]["effective_limit"] == 2
    clock[0] += 1
    await queue.heartbeat(True)
    state = await queue.snapshot()
    assert state["inflight"] == state["best_effort"]["effective_limit"] == 4
    assert state["best_effort"]["mode"] == "heuristic_relaxed"
    assert state["best_effort"]["drain_confirmed"] is False
    clock[0] += 30
    await queue.heartbeat(True)
    assert (await queue.snapshot())["inflight"] == 6
    # A previously invisible Tool-blocked Job (or future arrival) returns now.
    live = asyncio.create_task(queue.acquire(priority("returning", deadline=1000)))
    await asyncio.sleep(0)
    state = await queue.snapshot()
    assert state["best_effort"]["effective_limit"] == 2
    assert state["best_effort"]["over_limit"] == 4
    assert state["inflight"] == 6 and not live.done()
    await queue.release(("job", "old0"))
    assert (await asyncio.wait_for(live, 1))["slo_status"] == "unexpired"
    assert not pending[-1].done()
    for task in pending:
        if not task.done():
            task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await queue.close()


@pytest.mark.asyncio
async def test_live_inflight_prevents_relaxation_and_idle_time_does_not_count(clock):
    queue = AdmissionQueue(config())
    clock[0] += 3600  # Idle startup does not buy background quota.
    await queue.heartbeat(True)
    await queue.acquire(priority("live", deadline=1000))
    await queue.acquire(priority("old", deadline=-1))
    clock[0] += 120
    await queue.heartbeat(True)
    assert (await queue.snapshot())["best_effort"]["quiet_seconds"] == 0
    await queue.release(("job", "live"))
    clock[0] += 60
    await queue.heartbeat(True)
    assert (await queue.snapshot())["best_effort"]["effective_limit"] == 4
    await queue.release(("job", "old"))
    assert (await queue.snapshot())["best_effort"]["effective_limit"] == 2
    await queue.close()


@pytest.mark.asyncio
async def test_cancelled_background_waiter_and_reserved_call_return_credit(clock):
    queue = AdmissionQueue(config())
    await queue.heartbeat(True)
    for i in range(2):
        await queue.acquire(priority(f"old{i}", deadline=-1))
    waiter = asyncio.create_task(queue.acquire(priority("cancel", deadline=-1)))
    await asyncio.sleep(0)
    await queue.release(("job", "old0"))  # Reserved, but acquire has not resumed.
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    state = await queue.snapshot()
    assert state["inflight"] == state["best_effort_inflight"] == 1
    assert state["queued"] == []
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
        await queue.acquire(priority(f"live{i}", deadline=1000))
    pending = asyncio.create_task(queue.acquire(priority("next", deadline=1000)))
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


@pytest.mark.asyncio
async def test_live_arrival_during_prefix_query_revokes_relaxed_quota(clock):
    started, finish = asyncio.Event(), asyncio.Event()

    async def refresh(requests):
        started.set()
        await finish.wait()
        return {r.key: RequestWork(cost_seconds=1) for r in requests}

    queue = AdmissionQueue(config())
    await queue.heartbeat(True)
    for i in range(2):
        await queue.acquire(priority(f"old{i}", deadline=-1))
    clock[0] += 60
    queue.set_work_refresher(refresh)
    old = asyncio.create_task(queue.acquire(priority("old2", deadline=-1)))
    await asyncio.wait_for(started.wait(), 1)
    live = asyncio.create_task(queue.acquire(priority("live", deadline=1000)))
    await asyncio.sleep(0)
    await queue.heartbeat(True)
    finish.set()
    await asyncio.wait_for(live, 1)
    assert not old.done()
    assert (await queue.snapshot())["best_effort"]["effective_limit"] == 2
    old.cancel()
    await asyncio.gather(old, return_exceptions=True)
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
        asyncio.create_task(queue.acquire(priority(f"live{i}", deadline=1000)))
        for i in range(6)
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
