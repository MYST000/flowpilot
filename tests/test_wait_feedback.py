from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from test_admission import body, priority, scheduled_gateway
from test_gateway import _headers

from flowpilot.scheduling.admission import AdmissionConfig, AdmissionQueue
from flowpilot.scheduling.wait_feedback import AdmissionWaitFeedback, WaitFeedbackConfig


def test_sliding_mean_expiry_and_idle_snapshot_do_not_insert_samples():
    feedback = AdmissionWaitFeedback(WaitFeedbackConfig(window_seconds=30))
    assert feedback.estimate(now=0, idle_capacity=False).source == "no_samples"
    idle = feedback.estimate(now=0, idle_capacity=True)
    assert idle.estimate_ms == 0 and idle.sample_count == 0
    feedback.record(entered_at=0, dispatched_at=1)
    feedback.record(entered_at=2, dispatched_at=5)
    measured = feedback.estimate(now=10, idle_capacity=False)
    assert measured.estimate_ms == 2000 and measured.sample_count == 2
    assert measured.last_sample_at_monotonic == 5
    assert feedback.estimate(now=10, idle_capacity=True).estimate_ms == 0
    assert feedback.estimate(now=31, idle_capacity=False).estimate_ms == 3000
    expired = feedback.estimate(now=35, idle_capacity=False)
    assert expired.estimate_ms is None and expired.source == "expired"
    assert expired.last_sample_at_monotonic == 5
    assert expired.window_seconds == 30


@pytest.fixture
def clock(monkeypatch):
    import flowpilot.scheduling.admission as module

    now = [1000.0]
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: now[0]))
    return now


async def test_queue_samples_once_at_dispatch_and_cancelled_waits_separately(clock):
    queue = AdmissionQueue(AdmissionConfig(limit=1))
    assert (await queue.queue_wait_estimate()).estimate_ms is None
    pending = asyncio.create_task(queue.acquire(priority("waiting")))
    await asyncio.sleep(0)
    clock[0] += 2
    assert (await queue.queue_wait_estimate()).sample_count == 0
    await queue.heartbeat(True)
    assert (await pending)["queue_wait_ms"] == 2000
    measured = await queue.queue_wait_estimate()
    assert measured.estimate_ms == 2000 and measured.sample_count == 1
    cancelled = asyncio.create_task(queue.acquire(priority("cancel")))
    await asyncio.sleep(0)
    clock[0] += 1
    cancelled.cancel()
    await asyncio.gather(cancelled, return_exceptions=True)
    for _ in range(3):
        await queue.snapshot()
        await queue.heartbeat(True)
        await queue.release(("job", "absent"))
    assert (await queue.queue_wait_estimate()).sample_count == 1
    state = await queue.snapshot()
    assert state["cancelled_wait_count"] == 1 and state["cancelled_wait_ms"] == 1000
    await queue.release(("job", "waiting"))
    await queue.release(("job", "waiting"))
    idle = await queue.queue_wait_estimate()
    assert idle.source == "idle_capacity" and idle.sample_count == 1
    clock[0] += 31
    expired = await queue.queue_wait_estimate()
    assert expired.source == "expired" and expired.estimate_ms is None
    assert not (await queue.snapshot())["healthy"]
    await queue.close()


async def test_cancel_after_reservation_keeps_exactly_one_wait_sample(clock):
    queue = AdmissionQueue(AdmissionConfig(limit=1))
    task = asyncio.create_task(queue.acquire(priority("cancel")))
    await asyncio.sleep(0)
    clock[0] += 2
    await queue.heartbeat(True)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    state = await queue.snapshot()
    assert state["inflight"] == 0 and state["free"] == 1
    assert state["queue_wait_estimate"]["sample_count"] == 1
    assert state["cancelled_wait_count"] == 0
    await queue.close()


async def test_tokenization_and_gateway_received_time_are_excluded(clock):
    async def upstream(request):
        if request.url.path == "/tokenize":
            clock[0] += 50
            return httpx.Response(200, json={"count": 100})
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]},
        )

    gateway, runtime, _, client = await scheduled_gateway(upstream)
    assert runtime.queue is not None
    try:
        await runtime.queue.heartbeat(False)
        pending = asyncio.create_task(
            gateway.proxy(
                path="/v1/chat/completions",
                api_kind="chat",
                body=body(),
                headers=_headers(),
                raw_query=b"",
            )
        )
        await asyncio.sleep(0)
        state = await runtime.queue.snapshot()
        assert state["queued"][0]["queue_entered_monotonic"] == 1050
        assert state["queued"][0]["queue_wait_ms"] == 0
        clock[0] += 2
        await runtime.queue.heartbeat(True)
        assert (await pending).status_code == 200
        events = runtime.recorder._sink.records
        admitted = next(e for e in events if e["event_type"] == "request_admitted")
        assert admitted["fields"]["queue_wait_ms"] == 2000
        assert (await runtime.queue.snapshot())["queue_wait_estimate"][
            "sample_count"
        ] == 1
    finally:
        await runtime.close()
        await client.aclose()
