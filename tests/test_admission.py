from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse
from test_gateway import _gateway, _headers

from flowpilot.app import _proxy_request, create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.gateway.router import InferenceRouter
from flowpilot.gateway.service import GatewayUpstreamError, LLMGateway
from flowpilot.gateway.stream import ObservedStream
from flowpilot.observability.trace import TraceRecorder
from flowpilot.protocol import JobRegistration, LineRegistration
from flowpilot.scheduling.admission import (
    AdmissionConfig,
    AdmissionQueue,
    PriorityWeights,
    RequestPriority,
    priority_score,
)
from flowpilot.scheduling.cost import OfflineCostModel, RequestWork, estimate_work
from flowpilot.scheduling.runtime import SchedulingRuntime


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "first"),
    [("prefill_slack", "expired"), ("slo_unexpired_first", "unexpired")],
)
async def test_expired_demotion_is_explicit_and_preserves_legacy_order(policy, first):
    queue = AdmissionQueue(AdmissionConfig(limit=1, policy=policy))
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    pending = {
        "expired": asyncio.create_task(
            queue.acquire(priority("expired", deadline=-60))
        ),
        "unexpired": asyncio.create_task(
            queue.acquire(priority("unexpired", deadline=60))
        ),
    }
    await asyncio.sleep(0)
    assert (await queue.snapshot())["queued"][0]["llm_call_id"] == first
    await queue.release(("job", "occupied"))
    assert (await asyncio.wait_for(pending[first], 1))["llm_call_id"] == first
    second = next(name for name in pending if name != first)
    assert not pending[second].done()
    await queue.release(("job", first))
    await asyncio.wait_for(pending[second], 1)
    await queue.release(("job", second))
    assert (await queue.snapshot())["inflight"] == 0
    await queue.close()


@pytest.mark.asyncio
async def test_expired_and_no_deadline_requests_share_best_effort_fifo():
    queue = AdmissionQueue(AdmissionConfig(limit=1, policy="slo_unexpired_first"))
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    pending = [
        asyncio.create_task(queue.acquire(priority("first_expired", deadline=-1))),
        asyncio.create_task(queue.acquire(priority("no_deadline"))),
        asyncio.create_task(
            queue.acquire(priority("much_older_deadline", deadline=-10000))
        ),
    ]
    await asyncio.sleep(0)
    state = await queue.snapshot()
    assert [p["slo_status"] for p in state["queued"]] == [
        "expired",
        "no_deadline",
        "expired",
    ]
    assert [p["llm_call_id"] for p in state["queued"]] == [
        "first_expired",
        "no_deadline",
        "much_older_deadline",
    ]
    await queue.release(("job", "occupied"))
    for task in pending:
        projection = await asyncio.wait_for(task, 1)
        await queue.release(("job", projection["llm_call_id"]))
    await queue.close()


@pytest.mark.asyncio
async def test_negative_prefill_slack_is_not_an_expired_workflow():
    queue = AdmissionQueue(AdmissionConfig(limit=1, policy="slo_unexpired_first"))
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    expired = asyncio.create_task(queue.acquire(priority("expired", deadline=-60)))
    live = asyncio.create_task(
        queue.acquire(
            replace(priority("live", deadline=60), work=RequestWork(cost_seconds=120))
        )
    )
    await asyncio.sleep(0)
    await queue.release(("job", "occupied"))
    projection = await asyncio.wait_for(live, 1)
    assert projection["slo_status"] == "unexpired"
    assert projection["prefill_slack_seconds"] < 0
    assert not expired.done()
    await queue.release(("job", "live"))
    await asyncio.wait_for(expired, 1)
    await queue.release(("job", "expired"))
    await queue.close()


@pytest.mark.asyncio
async def test_waiting_request_is_demoted_after_deadline_without_preempting(
    monkeypatch,
):
    import flowpilot.scheduling.admission as admission_module

    clock = [1000.0]
    monkeypatch.setattr(
        admission_module, "time", SimpleNamespace(monotonic=lambda: clock[0])
    )
    queue = AdmissionQueue(AdmissionConfig(limit=1, policy="slo_unexpired_first"))
    await queue.heartbeat(True)
    await queue.acquire(priority("running", deadline=1))
    expiring = asyncio.create_task(queue.acquire(priority("expiring", deadline=1)))
    live = asyncio.create_task(queue.acquire(priority("live", deadline=60)))
    await asyncio.sleep(0)
    initial = await queue.snapshot()
    assert initial["queued"][0]["llm_call_id"] == "expiring"
    frozen_cp = initial["queued"][0]["cp_seconds"]
    clock[0] += 10
    await queue.heartbeat(True)
    state = await queue.snapshot()
    assert state["inflight"] == 1  # Expiry does not preempt the accepted call.
    assert [p["llm_call_id"] for p in state["queued"]] == ["live", "expiring"]
    assert state["queued"][1]["cp_seconds"] == frozen_cp
    assert state["queued"][1]["age_seconds"] >= 10
    await queue.release(("job", "running"))
    await asyncio.wait_for(live, 1)
    assert not expiring.done()
    await queue.release(("job", "live"))
    assert (await asyncio.wait_for(expiring, 1))["slo_status"] == "expired"
    await queue.release(("job", "expiring"))
    await queue.close()


@pytest.mark.asyncio
async def test_expiry_during_query_yields_to_live_request_from_next_sweep(monkeypatch):
    import flowpilot.scheduling.admission as admission_module

    clock = [1000.0]
    monkeypatch.setattr(
        admission_module, "time", SimpleNamespace(monotonic=lambda: clock[0])
    )
    first_started, first_finish = asyncio.Event(), asyncio.Event()
    second_started, second_finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def refresh(requests):
        calls.append({request.key[1] for request in requests})
        if len(calls) == 1:
            first_started.set()
            await first_finish.wait()
        elif len(calls) == 2:
            second_started.set()
            await second_finish.wait()
        return {
            request.key: RequestWork(cost_seconds=None, cost_basis="unknown:test")
            for request in requests
        }

    queue = AdmissionQueue(
        AdmissionConfig(limit=1, policy="slo_unexpired_first"), refresh
    )
    await queue.heartbeat(True)
    expiring = asyncio.create_task(queue.acquire(priority("expiring", deadline=1)))
    await asyncio.wait_for(first_started.wait(), 1)
    live = asyncio.create_task(queue.acquire(priority("live", deadline=60)))
    await asyncio.sleep(0)
    clock[0] += 10
    await queue.heartbeat(True)
    first_finish.set()
    await asyncio.wait_for(second_started.wait(), 1)
    assert calls == [{"expiring"}, {"expiring", "live"}]
    assert not expiring.done() and not live.done()
    assert (await queue.snapshot())["inflight"] == 0
    second_finish.set()
    projection = await asyncio.wait_for(live, 1)
    assert projection["slo_status"] == "unexpired"
    assert projection["ordering_basis"] == "deadline_only:cost_unknown"
    assert not expiring.done()
    await queue.release(("job", "live"))
    assert (await asyncio.wait_for(expiring, 1))["slo_status"] == "expired"
    await queue.release(("job", "expiring"))
    await queue.close()


def priority(name: str, *, age: float = 0, deadline: float | None = None):
    now = datetime.now(UTC)
    return RequestPriority(
        key=("job", name),
        job_id="job",
        arrived_at=now - timedelta(seconds=age),
        workflow_started_at=now - timedelta(seconds=60),
        deadline=now + timedelta(seconds=deadline) if deadline is not None else None,
        prompt_tokens=100,
    )


def calibrated_model():
    return OfflineCostModel(
        source="test fixture",
        version="test-v1",
        measured_at=datetime.now(UTC),
        model="test",
        engine_identity_digest="test-layout",
        measurement_basis="test calibration, not a production measurement",
        prefill=({"max_context_tokens": 10000, "seconds_per_token": 0.001},),
        offload={"seconds_per_byte": 1e-9},
        restore={"seconds_per_byte": 2e-9},
    )


def test_offline_costs_distinguish_gpu_cpu_and_unknown_restore():
    observation = {
        "query_id": "q",
        "engine_epoch": "e",
        "engine_identity_digest": "test-layout",
        "state_version": 1,
        "reuse_basis": "TARGET_REQUEST",
        "prompt_tokens": 1000,
        "gpu_ready_tokens": 100,
        "recoverable_tokens": 900,
        "cpu_load_object_bytes": 1000000,
    }
    model = calibrated_model()
    work = estimate_work([observation], model, observed_at=1)
    assert work.gpu_cost_seconds == pytest.approx(0.9)
    assert work.cpu_cost_seconds == pytest.approx(0.102)
    assert work.cost_seconds == work.cpu_cost_seconds
    unknown = estimate_work(
        [observation], model.model_copy(update={"restore": None}), observed_at=1
    )
    assert unknown.cpu_cost_seconds is None
    assert unknown.cost_seconds == unknown.gpu_cost_seconds
    incompatible = estimate_work(
        [observation],
        model.model_copy(update={"engine_identity_digest": "other"}),
        observed_at=1,
    )
    assert incompatible.cost_seconds is None
    assert "mismatch" in incompatible.cost_basis


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["prefill_slack", "slo_unexpired_first"])
async def test_full_sweep_reorders_all_waiters_after_cache_loss(policy):
    import time

    calls = []
    costs = {"occupied": 0.1, "a": 0.1, "b": 0.4}
    gate = asyncio.Event()
    gate.set()

    async def refresh(requests):
        calls.append({r.key[1] for r in requests})
        await gate.wait()
        return {
            r.key: RequestWork(
                prompt_tokens=1000,
                cost_seconds=costs[r.key[1]],
                prefix_basis="TARGET_REQUEST",
                observed_at_monotonic=time.monotonic(),
            )
            for r in requests
        }

    queue = AdmissionQueue(AdmissionConfig(limit=1, policy=policy), refresh)
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    deadline = datetime.now(UTC) + timedelta(seconds=10)
    a = asyncio.create_task(queue.acquire(replace(priority("a"), deadline=deadline)))
    b = asyncio.create_task(queue.acquire(replace(priority("b"), deadline=deadline)))
    for _ in range(4):
        await asyncio.sleep(0)
    assert (await queue.snapshot())["queued"][0]["llm_call_id"] == "b"
    costs["a"] = 0.8
    gate.clear()
    await queue.release(("job", "occupied"))
    await asyncio.sleep(0)
    assert not a.done() and not b.done()
    gate.set()
    assert (await asyncio.wait_for(a, 1))["llm_call_id"] == "a"
    assert calls[-1] == {"a", "b"} and all("occupied" not in call for call in calls[1:])
    assert not b.done()
    await queue.release(("job", "a"))
    await asyncio.wait_for(b, 1)
    await queue.release(("job", "b"))
    await queue.close()


@pytest.mark.asyncio
async def test_cancellation_during_full_query_does_not_consume_credit():
    started, finish_query = asyncio.Event(), asyncio.Event()

    async def refresh(requests):
        started.set()
        await finish_query.wait()
        return {r.key: RequestWork() for r in requests}

    queue = AdmissionQueue(AdmissionConfig(limit=1), refresh)
    await queue.heartbeat(True)
    request = asyncio.create_task(queue.acquire(priority("cancel")))
    await started.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    finish_query.set()
    await asyncio.sleep(0)
    assert (await queue.snapshot())["inflight"] == 0
    await queue.close()


def test_continuous_priority_no_risk_classes_and_fairness_disabled():
    config = AdmissionConfig()
    now = datetime.now(UTC)
    base = priority("base", deadline=60)
    closer = replace(base, deadline=now + timedelta(seconds=1))
    assert sum(priority_score(closer, config, now=now, age_seconds=0).values()) > sum(
        priority_score(base, config, now=now, age_seconds=0).values()
    )
    a = priority_score(base, config, now=now, age_seconds=0, job_inflight=0)
    b = priority_score(base, config, now=now, age_seconds=0, job_inflight=100)
    assert a == b
    assert b["fairness"] == 0
    aged = priority_score(base, config, now=now, age_seconds=100)
    overdue = priority_score(
        replace(base, deadline=now - timedelta(seconds=100)),
        config,
        now=now,
        age_seconds=0,
    )
    assert sum(aged.values()) > sum(overdue.values())
    assert aged["progress"] == a["progress"]  # CP frozen, age separate.
    assert (
        priority_score(
            base,
            AdmissionConfig(weights=PriorityWeights(fairness=0.05)),
            now=now,
            age_seconds=0,
            job_inflight=2,
        )["fairness"]
        < 0
    )


@pytest.mark.asyncio
async def test_queue_orders_sum_then_fifo_and_returns_each_credit_once():
    queue = AdmissionQueue(AdmissionConfig(limit=1))
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    low = asyncio.create_task(queue.acquire(priority("low")))
    high = asyncio.create_task(queue.acquire(priority("high", deadline=0.1)))
    await asyncio.sleep(0)
    state = await queue.snapshot()
    assert [q["llm_call_id"] for q in state["queued"]] == ["high", "low"]
    assert state["queued"][0]["queue_work_before_tokens"] == 0
    await queue.release(("job", "occupied"))
    assert (await high)["llm_call_id"] == "high"
    await queue.release(("job", "occupied"))
    assert not low.done()
    await queue.release(("job", "high"))
    await low
    await queue.release(("job", "low"))
    assert (await queue.snapshot())["inflight"] == 0


@pytest.mark.asyncio
async def test_stale_heartbeat_stops_admission_but_not_accepted_calls():
    queue = AdmissionQueue(
        AdmissionConfig(
            limit=1,
            heartbeat_interval_seconds=0.001,
            heartbeat_ttl_seconds=0.01,
        )
    )
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    waiting = asyncio.create_task(queue.acquire(priority("waiting")))
    await asyncio.sleep(0.02)
    await queue.release(("job", "occupied"))
    assert not waiting.done()
    assert (await queue.snapshot())["healthy"] is False
    await queue.heartbeat(True)
    await waiting
    await queue.release(("job", "waiting"))


@pytest.mark.asyncio
async def test_cancellation_after_credit_reservation_does_not_leak():
    queue = AdmissionQueue(AdmissionConfig(limit=1))
    waiting = asyncio.create_task(queue.acquire(priority("cancel")))
    await asyncio.sleep(0)
    await queue.heartbeat(True)  # Future resolved, acquire has not resumed yet.
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert (await queue.snapshot())["inflight"] == 0


class HeldStream(httpx.AsyncByteStream):
    def __init__(self):
        self.release = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        yield b'data: {"choices":[{"delta":{"content":"ok"}}]}\n\n'
        await self.release.wait()
        yield b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
        yield b"data: [DONE]\n\n"

    async def aclose(self):
        self.closed = True


async def scheduled_gateway(handler):
    _, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    runtime = SchedulingRuntime(
        client,
        "http://inference-a",
        AdmissionConfig(enabled=True, limit=1),
        frontier,
        TraceRecorder(sink),
    )
    assert runtime.queue
    await runtime.queue.heartbeat(True)
    gateway = LLMGateway(
        client,
        InferenceRouter((InferenceInstance("inference-a", "http://inference-a"),)),
        frontier,
        TraceRecorder(sink),
        ingress_api_key="test-key",
        require_ingress_auth=True,
        scheduling=runtime,
    )
    return gateway, runtime, frontier, client


def body(*, stream=False):
    return json.dumps(
        {"model": "m", "messages": [{"role": "user", "content": "x"}], "stream": stream}
    ).encode()


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", ["complete", "cancel"])
async def test_gateway_stream_holds_credit_until_close(finish):
    stream = HeldStream()
    sent = []

    async def upstream(request):
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": 10})
        sent.append(request)
        if len(sent) == 1:
            return httpx.Response(200, stream=stream)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]},
        )

    gateway, runtime, frontier, client = await scheduled_gateway(upstream)
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-2",
            conversation_id="conversation-line-2",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest="a" * 64,
        )
    )
    first = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=body(stream=True),
        headers=_headers(),
        raw_query=b"",
    )
    assert isinstance(first, StreamingResponse)
    assert isinstance(first.body_iterator, ObservedStream)
    second_headers = {
        **_headers(call_id="call-2", request_id="req-2"),
        "x-flowpilot-line-id": "line-2",
        "x-flowpilot-conversation-id": "conversation-line-2",
    }
    second = asyncio.create_task(
        gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=body(),
            headers=second_headers,
            raw_query=b"",
        )
    )
    await asyncio.sleep(0)
    assert len(sent) == 1
    if finish == "cancel":
        await first.body_iterator.aclose()
    else:
        stream.release.set()
        chunks = [chunk async for chunk in first.body_iterator]
        assert chunks[-1] == b"data: [DONE]\n\n"
    assert stream.closed
    assert (await second).status_code == 200
    assert (await runtime.snapshot())["admission"]["inflight"] == 0
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["send", "provider", "malformed", "queued_cancel"])
async def test_gateway_failure_returns_credit_and_queue_cancel_rolls_back(failure):
    async def upstream(request):
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": 10})
        if failure == "send":
            raise httpx.ConnectError("offline")
        return httpx.Response(500 if failure == "provider" else 200, content=b"invalid")

    gateway, runtime, frontier, client = await scheduled_gateway(upstream)
    assert runtime.queue
    if failure == "queued_cancel":
        await runtime.queue.acquire(priority("occupied"))
    task = asyncio.create_task(
        gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=body(),
            headers=_headers(),
            raw_query=b"",
        )
    )
    if failure == "queued_cancel":
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await frontier.line_snapshot("job-1", "line-1"))["phase"] == "EMPTY"
        await runtime.queue.release(("job", "occupied"))
    elif failure == "send":
        with pytest.raises(GatewayUpstreamError):
            await task
    else:
        await task
    state = await runtime.queue.snapshot()
    assert state["inflight"] == 0 and state["queued"] == []
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_cancel_during_terminal_credit_return_preserves_committed_outcome():
    async def upstream(request):
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": 10})
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {"content": "ok"},
                        "finish_reason": "stop",
                    }
                ]
            },
        )

    gateway, runtime, _, client = await scheduled_gateway(upstream)
    entered, proceed, released = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def return_credit(call):
        entered.set()
        await proceed.wait()
        await runtime.terminal(call)
        released.set()

    gateway._call_store._on_terminal = return_credit
    task = asyncio.create_task(
        gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=body(),
            headers=_headers(),
            raw_query=b"",
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    proceed.set()
    await released.wait()
    assert (await gateway.gateway_calls())[0]["phase"] == "completed"
    assert (await runtime.snapshot())["admission"]["inflight"] == 0
    await runtime.close()
    await client.aclose()


def test_scheduler_configuration_requires_one_instance(tmp_path):
    with pytest.raises(ValueError, match="one fixed"):
        Settings(
            instances=(
                InferenceInstance("a", "http://a"),
                InferenceInstance("b", "http://b"),
            ),
            trace_path=tmp_path / "trace",
            require_ingress_auth=False,
            admission=AdmissionConfig(enabled=True),
        )


@pytest.mark.asyncio
async def test_http_disconnect_removes_queued_request_without_upstream_send(tmp_path):
    sent = []
    tokenized = asyncio.Event()

    async def upstream(request):
        sent.append(request.url.path)
        if request.url.path == "/health":
            return httpx.Response(200)
        if request.url.path == "/v1/kv/capabilities":
            return httpx.Response(501)
        if request.url.path == "/tokenize":
            tokenized.set()
            return httpx.Response(200, json={"count": 10})
        raise AssertionError("disconnected request must never reach inference")

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        app = create_app(
            Settings(
                instances=(InferenceInstance("a", "http://a"),),
                trace_path=tmp_path / "trace",
                ingress_api_key="test-key",
                admission=AdmissionConfig(enabled=True, limit=1),
            ),
            http_client=client,
        )
        async with app.router.lifespan_context(app):
            await app.state.frontier.register_job(JobRegistration(job_id="job-1"))
            await app.state.frontier.register_line(
                LineRegistration(
                    job_id="job-1",
                    line_id="line-1",
                    conversation_id="conversation-line-1",
                    context_epoch=1,
                    base_context_cursor="cursor-0",
                    context_digest="a" * 64,
                )
            )
            queue = app.state.scheduling.queue
            await queue.acquire(priority("occupied"))
            disconnected = asyncio.Event()
            read_body = False

            async def receive():
                nonlocal read_body
                if not read_body:
                    read_body = True
                    return {"type": "http.request", "body": body(), "more_body": False}
                await disconnected.wait()
                return {"type": "http.disconnect"}

            request = Request(
                {
                    "type": "http",
                    "app": app,
                    "method": "POST",
                    "path": "/v1/chat/completions",
                    "query_string": b"",
                    "headers": [
                        (k.encode(), v.encode()) for k, v in _headers().items()
                    ],
                },
                receive,
            )
            task = asyncio.create_task(
                _proxy_request(request, "/v1/chat/completions", "chat")
            )
            async with asyncio.timeout(2):
                await tokenized.wait()
                assert len((await queue.snapshot())["queued"]) == 1
                disconnected.set()
                assert (await task).status_code == 499
            state = await queue.snapshot()
            assert state["queued"] == [] and state["inflight"] == 1
            assert "/v1/chat/completions" not in sent
            assert (await app.state.frontier.line_snapshot("job-1", "line-1"))[
                "phase"
            ] == "EMPTY"
            await queue.release(("job", "occupied"))
