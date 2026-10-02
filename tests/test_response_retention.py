"""Response input ordering through the gateway, real stores and KV controller."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest
from test_admission import body
from test_gateway import _headers
from test_retention import Engine

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import (
    JobRegistration,
    LineRegistration,
    ReuseDecisionKind,
    ToolReuseDecision,
    ToolReuseIdentity,
)
from flowpilot.scheduling.admission import AdmissionConfig
from flowpilot.scheduling.retention import RetentionConfig


class ToolEngine(Engine):
    def __init__(self, calls, api_kind):
        super().__init__()
        self.calls = calls
        self.api_kind = api_kind

    async def __call__(self, request):
        if request.url.path == "/health":
            return httpx.Response(200)
        if request.url.path in {"/v1/chat/completions", "/v1/responses"}:
            if self.api_kind == "responses":
                return httpx.Response(
                    200,
                    json={
                        "id": "r1",
                        "status": "completed",
                        "output": [
                            {
                                "type": "function_call",
                                "call_id": call,
                                "name": "web_search",
                                "arguments": "{}",
                            }
                            for call in self.calls
                        ],
                    },
                )
            message = {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": call,
                        "type": "function",
                        "function": {"name": "web_search", "arguments": "{}"},
                    }
                    for call in self.calls
                ],
            }
            if json.loads(request.content).get("stream"):
                for i, call in enumerate(message["tool_calls"]):
                    call["index"] = i
                chunk = {"choices": [{"delta": message, "finish_reason": "tool_calls"}]}
                return httpx.Response(
                    200,
                    content=("data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n"),
                    headers={"content-type": "text/event-stream"},
                )
            return httpx.Response(
                200,
                json={"choices": [{"message": message, "finish_reason": "tool_calls"}]},
            )
        return await super().__call__(request)


class Predictor:
    def __init__(self, durations, failure):
        self.durations = durations
        self.failure = failure
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()

    def bind(self, app):
        self.store = app.state.tool_resolutions

    def on_response(self, identity, *_args, **_kwargs):
        if self.failure == "submit":
            raise RuntimeError("predictor unavailable")
        return self.predict(identity)

    async def predict(self, identity):
        self.started.set()
        try:
            await self.release.wait()
            records = await self.store.get_for_line(
                identity.job_id, identity.line_id, identity.tail_request_id
            )
            for record in records:
                await self.store.apply_duration_estimate(
                    record,
                    expected_version=record.version,
                    duration_ms=self.durations[record.tool_call_id],
                    quantile="q50",
                    is_current=self.current,
                )
            # A partial prediction must not survive a failed batch as known.
            if self.failure == "timeout":
                raise TimeoutError("predictor timeout")
            if self.failure == "cancel":
                raise asyncio.CancelledError
        finally:
            self.finished.set()

    async def current(self):
        return True

    def on_resolution(self, _record):
        pass

    async def close(self):
        pass


class Cache:
    def __init__(self, hits):
        self.hits = hits
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def resolve(self, request, **_kwargs):
        self.started.set()
        await self.release.wait()
        return self.decision(request.identity.tool_call_id)

    def decision(self, call):
        return ToolReuseDecision(
            decision=ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
            if call in self.hits
            else ReuseDecisionKind.EXECUTE_LOCALLY,
            result={"text": "private cached result"} if call in self.hits else None,
        )


@asynccontextmanager
async def scenario(tmp_path, *, hits=(), durations=None, failure=None, api_kind="chat"):
    durations = durations or {"a": 60_000}
    engine = ToolEngine(tuple(durations), api_kind)
    cache = Cache(hits)
    predictor = Predictor(durations, failure) if failure != "missing" else None
    sink = InMemoryTraceSink()
    async with httpx.AsyncClient(transport=httpx.MockTransport(engine)) as client:
        app = create_app(
            Settings(
                ingress_api_key="test-key",
                instances=(InferenceInstance("inference-a", "http://inference-a"),),
                trace_path=tmp_path / "trace.jsonl",
                admission=AdmissionConfig(enabled=True, limit=4),
                retention=RetentionConfig(enabled=True),
            ),
            http_client=client,
            trace_sink=sink,
            tool_duration_adapter=predictor,
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
            gateway = app.state.llm_gateway
            gateway._reuse = cache
            yield app, engine, cache, predictor, sink


async def response(app, *, api_kind="chat", stream=False, policy=True):
    headers = _headers()
    headers["x-flowpilot-reuse-policy"] = json.dumps(
        {
            "allowed_tool_names": ["web_search"],
            "deferred": False,
            "policy_version": 1,
            "expected_policy_version": 0,
            "lease_id": "lease-1",
            "lease_seconds": 60,
            "max_messages": 20,
            "max_bytes": 10000,
            "max_internal_continuations": 3,
            "delta_ttl_seconds": 60,
        }
    )
    if not policy:
        del headers["x-flowpilot-reuse-policy"]
    return await app.state.llm_gateway.proxy(
        path="/v1/chat/completions" if api_kind == "chat" else "/v1/responses",
        api_kind=api_kind,
        body=body(stream=stream)
        if api_kind == "chat"
        else b'{"model":"m","input":"x"}',
        headers=headers,
        raw_query=b"",
    )


def decisions(sink):
    return [
        r["fields"] for r in sink.records if r["event_type"] == "kv_retention_decision"
    ]


@pytest.mark.parametrize("api_kind", ["chat", "responses"])
@pytest.mark.parametrize("first", ["prediction", "cache", "kv"])
async def test_hit_is_zero_for_every_input_completion_order(tmp_path, first, api_kind):
    async with scenario(tmp_path, hits={"a"}, api_kind=api_kind) as state:
        app, engine, cache, predictor, sink = state
        assert predictor is not None
        engine.query_gate = asyncio.Event()
        pending = asyncio.create_task(response(app, api_kind=api_kind))
        await asyncio.wait_for(
            asyncio.gather(
                predictor.started.wait(),
                cache.started.wait(),
                engine.query_started.wait(),
            ),
            1,
        )
        assert not engine.commands
        if first == "prediction":
            predictor.release.set()
            await predictor.finished.wait()
            cache.release.set()
        elif first == "cache":
            cache.release.set()
        else:
            engine.query_gate.set()
            await app.state.scheduling.retention.refresh()
            assert not engine.commands
            cache.release.set()
        assert (await asyncio.wait_for(pending, 1)).status_code == 200
        # Returning a cache hit never waits for pending KV work or prediction.
        engine.query_gate.set()
        retention = app.state.scheduling.retention
        await asyncio.wait_for(asyncio.gather(*retention._tasks), 1)
        assert decisions(sink)[0]["tool_gap_seconds"] == 0.0
        assert decisions(sink)[0]["action"] == "KEEP"
        assert predictor.finished.is_set()
        store = app.state.tool_resolutions
        record = (await store.get_for_line("job-1", "line-1", "tail-1"))[0]
        assert record.status == "ready"
        if first == "prediction":
            assert record.duration_estimate_ms == 60_000
        assert not await store.apply_duration_estimate(
            record,
            expected_version=record.version,
            duration_ms=90_000,
            quantile="q50",
            is_current=predictor.current,
        )
        engine.free = 0
        retention.line_changed("job-1", "line-1")
        await asyncio.gather(*retention._tasks)
        await retention.refresh()
        assert len(decisions(sink)) == len(engine.commands) == 1
        assert "private cached result" not in json.dumps(sink.records)


async def test_miss_waits_for_prediction_without_holding_response_or_credit(tmp_path):
    async with scenario(tmp_path) as (app, engine, cache, predictor, sink):
        assert predictor is not None
        cache.release.set()
        assert (await asyncio.wait_for(response(app), 1)).status_code == 200
        await engine.query_started.wait()
        assert (await app.state.scheduling.queue.snapshot())["inflight"] == 0
        assert not engine.commands
        predictor.release.set()
        retention = app.state.scheduling.retention
        await asyncio.wait_for(asyncio.gather(*retention._tasks), 1)
        assert decisions(sink)[0]["tool_gap_seconds"] == pytest.approx(60, abs=0.05)
        assert len(engine.commands) == 1


async def test_serial_multi_tool_gap_excludes_hit_and_sums_misses(tmp_path):
    async with scenario(
        tmp_path, hits={"a"}, durations={"a": 60_000, "b": 2000, "c": 3000}
    ) as (app, engine, cache, predictor, sink):
        assert predictor is not None
        predictor.release.set()
        pending = asyncio.create_task(response(app))
        await predictor.finished.wait()
        cache.release.set()
        await pending
        await asyncio.wait_for(
            asyncio.gather(*app.state.scheduling.retention._tasks), 1
        )
        assert decisions(sink)[0]["tool_gap_seconds"] == pytest.approx(5, abs=0.05)
        assert len(engine.commands) == 1


@pytest.mark.parametrize("failure", ["missing", "submit", "timeout", "cancel"])
async def test_unavailable_prediction_decides_once_with_unknown(tmp_path, failure):
    async with scenario(tmp_path, failure=failure) as state:
        app, engine, cache, predictor, sink = state
        cache.release.set()
        if predictor is not None:
            predictor.release.set()
        await response(app)
        await asyncio.wait_for(
            asyncio.gather(*app.state.scheduling.retention._tasks), 1
        )
        assert len(decisions(sink)) == len(engine.commands) == 1
        assert decisions(sink)[0]["tool_gap_seconds"] is None
        assert decisions(sink)[0]["reason"].startswith("fallback_cost_unknown:")
        assert engine.commands[0]["action"] == "OFFLOAD"


@pytest.mark.parametrize("stream,policy", [(True, True), (True, False), (False, False)])
async def test_delivery_finishes_before_sdk_cache_resolution(tmp_path, stream, policy):
    async with scenario(tmp_path, hits={"a"}) as (app, engine, cache, predictor, sink):
        assert predictor is not None
        result = await response(app, stream=stream, policy=policy)

        async def consume():
            return b"".join([chunk async for chunk in result.body_iterator])

        if stream:
            assert b"[DONE]" in await asyncio.wait_for(consume(), 1)
        else:
            assert result.status_code == 200
            predictor.release.set()
            await predictor.finished.wait()
        await engine.query_started.wait()
        assert not engine.commands
        await app.state.llm_gateway._on_reuse_resolution(
            ToolReuseIdentity(
                job_id="job-1",
                line_id="line-1",
                tail_request_id="tail-1",
                llm_call_id="call-1",
                tool_call_id="a",
            ),
            "web_search",
            cache.decision("a"),
        )
        await asyncio.wait_for(
            asyncio.gather(*app.state.scheduling.retention._tasks), 1
        )
        assert len(decisions(sink)) == 1
        assert decisions(sink)[0]["tool_gap_seconds"] == 0.0
