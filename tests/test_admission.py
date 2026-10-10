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
    RequestPriority,
)
from flowpilot.scheduling.cost import OfflineCostModel, RequestWork, estimate_work
from flowpilot.scheduling.runtime import SchedulingRuntime


@pytest.fixture
def admission_clock(monkeypatch):
    import flowpilot.scheduling.admission as module

    clock = [1000.0]
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    return clock


def priority(name: str, *, cost: float | None = None):
    return RequestPriority(
        key=("job", name),
        job_id="job",
        prompt_tokens=100,
        work=RequestWork(cost_seconds=cost),
    )


@pytest.mark.asyncio
async def test_wait_minus_cost_c_a_b_and_fixed_cost_order(admission_clock):
    queue = AdmissionQueue(AdmissionConfig(limit=3))
    c = asyncio.create_task(queue.acquire(priority("c", cost=0.3)))
    await asyncio.sleep(0)
    admission_clock[0] += 0.4
    a = asyncio.create_task(queue.acquire(priority("a", cost=0.02)))
    b = asyncio.create_task(queue.acquire(priority("b", cost=0.3)))
    await asyncio.sleep(0)
    admission_clock[0] += 0.2
    state = await queue.snapshot()
    assert [p["llm_call_id"] for p in state["queued"]] == ["c", "a", "b"]
    assert [p["score_ms"] for p in state["queued"]] == pytest.approx([300, 180, -100])
    admission_clock[0] += 2
    assert [p["llm_call_id"] for p in (await queue.snapshot())["queued"]] == [
        "c",
        "a",
        "b",
    ]
    await queue.heartbeat(True)
    results = await asyncio.gather(c, a, b)
    assert all(p["ordering_basis"] == "wait_cost" for p in results)
    assert (await queue.snapshot())["last_sweep"]["dispatched_sequences"] == [1, 2, 3]
    for name in ("c", "a", "b"):
        await queue.release(("job", name))
    await queue.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["wait_cost", "fifo"])
@pytest.mark.parametrize("refresh", [False, True])
async def test_whole_sweep_fifo_freezes_all_credits_then_recovers(
    policy, admission_clock, refresh
):
    async def refresh_work(requests):
        return {r.key: r.work for r in requests}

    queue = AdmissionQueue(
        AdmissionConfig(limit=2, policy=policy), refresh_work if refresh else None
    )
    tasks = [
        asyncio.create_task(queue.acquire(priority(name, cost=cost)))
        for name, cost in [
            ("unknown", None),
            ("expensive", 2),
            ("cheap", 0),
            ("cheaper_later", 0.1),
        ]
    ]
    await asyncio.sleep(0)
    await queue.heartbeat(True)
    first, second = await asyncio.gather(*tasks[:2])
    expected = "fifo:cost_unknown" if policy == "wait_cost" else "fifo:configured"
    assert first["ordering_basis"] == second["ordering_basis"] == expected
    assert first["score_ms"] is None and second["kv_start_cost_ms"] == 2000
    assert first["sweep_id"] == second["sweep_id"]
    assert first["candidate_sequences"] == [1, 2, 3, 4]
    assert not tasks[2].done()
    await queue.release(("job", "unknown"))
    third = await tasks[2]
    assert third["ordering_basis"] == (
        "wait_cost" if policy == "wait_cost" else "fifo:configured"
    )
    await queue.release(("job", "expensive"))
    await tasks[3]
    await queue.release(("job", "cheap"))
    await queue.release(("job", "cheaper_later"))
    assert (await queue.snapshot())["inflight"] == 0
    await queue.close()


@pytest.mark.asyncio
async def test_new_unknown_arrival_waits_for_next_sweep(admission_clock):
    started, finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def refresh(requests):
        calls.append([r.key[1] for r in requests])
        if len(calls) == 1:
            started.set()
            await finish.wait()
        return {
            r.key: RequestWork(cost_seconds=None if r.key[1] == "new" else 1)
            for r in requests
        }

    queue = AdmissionQueue(AdmissionConfig(limit=2), refresh)
    await queue.heartbeat(True)
    old = asyncio.create_task(queue.acquire(priority("old")))
    await started.wait()
    new = asyncio.create_task(queue.acquire(priority("new")))
    await asyncio.sleep(0)
    finish.set()
    first, second = await asyncio.gather(old, new)
    assert calls == [["old"], ["new"]]
    assert first["ordering_basis"] == "wait_cost"
    assert second["ordering_basis"] == "fifo:cost_unknown"
    assert first["sweep_id"] != second["sweep_id"]
    await queue.release(("job", "old"))
    await queue.release(("job", "new"))
    await queue.close()


@pytest.mark.asyncio
async def test_cancel_unknown_during_sweep_restores_cost_order(admission_clock):
    started, finish = asyncio.Event(), asyncio.Event()

    async def refresh(requests):
        started.set()
        await finish.wait()
        return {r.key: r.work for r in requests}

    queue = AdmissionQueue(AdmissionConfig(limit=1), refresh)
    await queue.heartbeat(True)
    tasks = [
        asyncio.create_task(queue.acquire(priority(name, cost=cost)))
        for name, cost in [("unknown", None), ("expensive", 2), ("cheap", 0)]
    ]
    await started.wait()
    tasks[0].cancel()
    await asyncio.gather(tasks[0], return_exceptions=True)
    finish.set()
    assert (await tasks[2])["ordering_basis"] == "wait_cost"
    assert not tasks[1].done()
    await queue.release(("job", "cheap"))
    await tasks[1]
    await queue.release(("job", "expensive"))
    assert (await queue.snapshot())["queue_wait_estimate"]["sample_count"] == 2
    await queue.close()


@pytest.mark.asyncio
async def test_expired_observation_is_unknown_for_entire_selection(admission_clock):
    queue = AdmissionQueue(AdmissionConfig(limit=2))
    stale = replace(
        priority("stale", cost=0),
        work=RequestWork(cost_seconds=0, observed_at_monotonic=999.0),
    )
    tasks = [
        asyncio.create_task(queue.acquire(r))
        for r in [priority("expensive", cost=2), stale]
    ]
    await asyncio.sleep(0)
    admission_clock[0] += 1
    state = await queue.snapshot()
    assert state["ordering_basis"] == "fifo:cost_unknown"
    assert state["queued"][1]["kv_start_cost_ms"] is None
    await queue.heartbeat(True)
    results = await asyncio.gather(*tasks)
    assert all(p["ordering_basis"] == "fifo:cost_unknown" for p in results)
    assert "stale_prefix" in results[0]["cost_unknown_reasons"][0]["reason"]
    await queue.close()


@pytest.mark.parametrize(
    "old",
    [
        {"policy": "prefill_slack"},
        {"policy": "slo_unexpired_first"},
        {"policy": "weighted"},
        {"weights": {}},
        {"best_effort": {}},
        {"age_reference_seconds": 5},
        {"work_reference_tokens": 4096},
    ],
)
def test_removed_configuration_has_migration_error(old):
    with pytest.raises(ValueError, match="configuration migration required"):
        AdmissionConfig.model_validate(old)


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
async def test_full_sweep_reorders_all_waiters_after_cache_loss():
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

    queue = AdmissionQueue(AdmissionConfig(limit=1), refresh)
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    a = asyncio.create_task(queue.acquire(priority("a")))
    b = asyncio.create_task(queue.acquire(priority("b")))
    for _ in range(4):
        await asyncio.sleep(0)
    assert (await queue.snapshot())["queued"][0]["llm_call_id"] == "a"
    costs["a"] = 0.8
    gate.clear()
    await queue.release(("job", "occupied"))
    await asyncio.sleep(0)
    assert not a.done() and not b.done()
    gate.set()
    assert (await asyncio.wait_for(b, 1))["llm_call_id"] == "b"
    assert calls[-1] == {"a", "b"} and all("occupied" not in call for call in calls[1:])
    assert not a.done()
    await queue.release(("job", "b"))
    await asyncio.wait_for(a, 1)
    await queue.release(("job", "a"))
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


@pytest.mark.asyncio
async def test_queue_orders_cost_then_fifo_and_returns_each_credit_once():
    queue = AdmissionQueue(AdmissionConfig(limit=1))
    await queue.heartbeat(True)
    await queue.acquire(priority("occupied"))
    low = asyncio.create_task(queue.acquire(priority("low", cost=0.5)))
    high = asyncio.create_task(queue.acquire(priority("high", cost=0.1)))
    await asyncio.sleep(0)
    state = await queue.snapshot()
    assert [q["llm_call_id"] for q in state["queued"]] == ["high", "low"]
    assert state["queued"][0]["queued_prompt_tokens_at_entry"] == 100
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


@pytest.mark.parametrize(
    "deadline_seconds,weight", [(-100, 100), (100, 0.1), (None, 1)]
)
async def test_runtime_order_is_invariant_to_line_metadata(
    admission_clock, deadline_seconds, weight
):
    async def upstream(request):
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": 100})
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]},
        )

    gateway, runtime, frontier, client = await scheduled_gateway(upstream)
    queue = runtime.queue
    assert queue is not None

    async def refresh(requests):
        return {
            r.key: RequestWork(cost_seconds=0.3 if r.key[1] == "call-1" else 0.1)
            for r in requests
        }

    queue.set_work_refresher(refresh)
    try:
        await queue.heartbeat(False)
        await frontier.register_line(
            LineRegistration(
                job_id="job-1",
                line_id="line-2",
                conversation_id="conversation-line-2",
                context_epoch=1,
                base_context_cursor="cursor-0",
                context_digest="a" * 64,
                weight=weight,
                deadline=datetime.now(UTC) + timedelta(seconds=deadline_seconds)
                if deadline_seconds is not None
                else None,
            )
        )
        first = asyncio.create_task(
            gateway.proxy(
                path="/v1/chat/completions",
                api_kind="chat",
                body=body(),
                headers=_headers(),
                raw_query=b"",
            )
        )
        await asyncio.sleep(0)
        second = asyncio.create_task(
            gateway.proxy(
                path="/v1/chat/completions",
                api_kind="chat",
                body=body(),
                headers={
                    **_headers(call_id="call-2", request_id="req-2"),
                    "x-flowpilot-line-id": "line-2",
                    "x-flowpilot-conversation-id": "conversation-line-2",
                },
                raw_query=b"",
            )
        )
        for _ in range(6):
            await asyncio.sleep(0)
        assert len((await queue.snapshot())["queued"]) == 2
        await queue.heartbeat(True)
        assert all(r.status_code == 200 for r in await asyncio.gather(first, second))
        records = runtime.recorder._sink.records
        admitted = [r for r in records if r["event_type"] == "request_admitted"]
        assert [r["identity"]["llm_call_id"] for r in admitted] == ["call-2", "call-1"]
        assert all(r["fields"]["ordering_basis"] == "wait_cost" for r in admitted)
        assert (await queue.snapshot())["inflight"] == 0
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.parametrize(
    "policy,order", [("wait_cost", ["b", "c", "a"]), ("fifo", ["a", "b", "c"])]
)
async def test_equal_cost_fifo_and_configured_fifo_with_known_costs(
    admission_clock, policy, order
):
    queue = AdmissionQueue(AdmissionConfig(limit=1, policy=policy))
    tasks = {
        name: asyncio.create_task(queue.acquire(priority(name, cost=cost)))
        for name, cost in [("a", 1), ("b", 0), ("c", 0)]
    }
    await asyncio.sleep(0)
    assert [p["llm_call_id"] for p in (await queue.snapshot())["queued"]] == order
    await queue.heartbeat(True)
    for name in order:
        assert (await tasks[name])["llm_call_id"] == name
        await queue.release(("job", name))
    assert (await queue.snapshot())["inflight"] == 0
    await queue.close()


async def test_missing_full_sweep_result_is_failure_not_fifo_success():
    async def refresh(requests):
        return {}

    queue = AdmissionQueue(AdmissionConfig(limit=1), refresh)
    await queue.heartbeat(True)
    with pytest.raises(KeyError):
        await queue.acquire(priority("missing"))
    state = await queue.snapshot()
    assert state["inflight"] == 0 and state["queued"] == []
    assert state["queue_wait_estimate"]["sample_count"] == 0
    await queue.close()
