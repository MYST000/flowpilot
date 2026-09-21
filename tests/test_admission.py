from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

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
from flowpilot.scheduling.runtime import SchedulingRuntime


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
