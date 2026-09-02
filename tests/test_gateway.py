from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from starlette.responses import StreamingResponse

from flowpilot.config import InferenceInstance
from flowpilot.frontier.store import LineTailFrontier
from flowpilot.gateway.call_state import GatewayCallPhase, GatewayCallStore
from flowpilot.gateway.router import InferenceRouter
from flowpilot.gateway.service import (
    GatewayAuthenticationError,
    GatewayUpstreamError,
    LLMGateway,
    identity_from_headers,
)
from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.protocol import JobRegistration, LineRegistration, RequestIdentity


def _digest() -> str:
    return "a" * 64


def _headers(expected_version: int = 0) -> dict[str, str]:
    return {
        "x-flowpilot-api-key": "test-key",
        "x-flowpilot-protocol-version": "flowpilot-phase0-v2",
        "x-flowpilot-job-id": "job-1",
        "x-flowpilot-line-id": "line-1",
        "x-flowpilot-tail-request-id": f"tail-{expected_version + 1}",
        "x-flowpilot-llm-call-id": f"call-{expected_version + 1}",
        "x-flowpilot-tail-version": str(expected_version),
        "x-flowpilot-context-epoch": "1",
        "x-flowpilot-context-sequence": str(expected_version),
        "x-flowpilot-context-cursor": f"cursor-{expected_version}",
        "x-flowpilot-context-digest": _digest(),
        "authorization": "Bearer provider-token",
    }


def test_old_protocol_version_is_rejected_at_ingress() -> None:
    headers = _headers()
    headers["x-flowpilot-protocol-version"] = "flowpilot-phase0-v1"
    with pytest.raises(GatewayAuthenticationError, match="invalid FlowPilot"):
        identity_from_headers({key: [value] for key, value in headers.items()})


async def _gateway(
    handler: httpx.AsyncBaseTransport,
) -> tuple[LLMGateway, LineTailFrontier, InMemoryTraceSink, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=handler)
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    sink = InMemoryTraceSink()
    gateway = LLMGateway(
        client,
        InferenceRouter((InferenceInstance("inference-a", "http://inference-a"),)),
        frontier,
        TraceRecorder(sink),
        ingress_api_key="test-key",
        require_ingress_auth=True,
    )
    return gateway, frontier, sink, client


@pytest.mark.anyio
async def test_gateway_call_attempts_require_terminal_retry() -> None:
    store = GatewayCallStore()
    common = {
        "job_id": "job-1",
        "line_id": "line-1",
        "tail_request_id": "tail-1",
        "llm_call_id": "shared-call",
        "expected_tail_version": 0,
        "context_epoch": 1,
        "context_sequence": 0,
        "base_context_cursor": "cursor-0",
        "context_digest": _digest(),
    }
    left = await store.start(
        RequestIdentity(**common),
        stream=False,
        api_kind="chat",
    )
    initial = (await store.snapshot())[0]
    assert initial["upstream_sent_at"] is None
    assert initial["upstream_first_byte_at"] is None
    with pytest.raises(Exception, match="active gateway call"):
        await store.start(RequestIdentity(**common), stream=False, api_kind="chat")
    await store.terminal(
        left,
        GatewayCallPhase.COMPLETED,
        authoritative_tail_version=1,
        status_code=200,
    )
    right = await store.start(RequestIdentity(**common), stream=False, api_kind="chat")
    calls = await store.snapshot()
    assert [(call["attempt"], call["phase"]) for call in calls] == [
        (1, "completed"),
        (2, "active"),
    ]
    await store.terminal(
        right,
        GatewayCallPhase.CANCELLED,
        authoritative_tail_version=0,
        status_code=None,
    )


@pytest.mark.anyio
async def test_gateway_preserves_provider_request_query_headers_and_body() -> None:
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.url.path == "/v1/chat/completions"
        assert request.url.query == b"trace=one&trace=two"
        assert request.headers["authorization"] == "Bearer provider-token"
        assert "x-flowpilot-api-key" not in request.headers
        return httpx.Response(
            200,
            json={
                "id": "resp-1",
                "choices": [{"message": {"content": "provider-secret"}}],
            },
        )

    gateway, _, sink, client = await _gateway(httpx.MockTransport(handler))
    body = b'{"model":"model-a","messages":[{"role":"user","content":"do-not-log"}]}'
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=body,
        headers=_headers(),
        raw_query=b"trace=one&trace=two",
    )
    assert response.status_code == 200
    assert b"provider-secret" in response.body
    assert len(seen) == 1
    response_events = [
        item for item in sink.records if item["event_type"] == "llm_response"
    ]
    assert response_events[0]["fields"]["response_id"] == "resp-1"
    serialized_trace = json.dumps(sink.records)
    assert "do-not-log" not in serialized_trace
    assert "provider-secret" not in serialized_trace
    calls = await gateway.gateway_calls()
    assert calls[0]["attempt"] == 1
    assert calls[0]["phase"] == GatewayCallPhase.COMPLETED.value
    assert calls[0]["authoritative_tail_version"] == 1
    assert calls[0]["status_code"] == 200
    assert calls[0]["terminal_reason"] is None
    assert calls[0]["gateway_received_at"] <= calls[0]["scheduler_queue_entered_at"]
    assert calls[0]["scheduler_queue_entered_at"] <= calls[0]["upstream_sent_at"]
    assert calls[0]["upstream_sent_at"] <= calls[0]["upstream_first_byte_at"]
    assert calls[0]["upstream_first_byte_at"] <= calls[0]["response_completed_at"]
    await client.aclose()


@pytest.mark.anyio
async def test_streaming_parallel_tool_calls_are_correlated_without_byte_changes() -> (
    None
):
    def sse(payload: dict[str, object]) -> bytes:
        body = json.dumps(payload, separators=(",", ":")).encode()
        return b"data: " + body + b"\n\n"

    chunks = [
        sse(
            {
                "id": "resp-2",
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-a",
                                    "function": {
                                        "name": "web_search",
                                        "arguments": '{"q":"a',
                                    },
                                },
                                {
                                    "index": 1,
                                    "id": "call-b",
                                    "function": {
                                        "name": "terminal",
                                        "arguments": "{}",
                                    },
                                },
                            ]
                        },
                        "finish_reason": None,
                    }
                ],
            }
        ),
        sse(
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {"index": 0, "function": {"arguments": '"}'}}
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            }
        ),
        b"data: [DONE]\n\n",
    ]

    class BodyStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for chunk in chunks:
                yield chunk

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=BodyStream()
        )

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","stream":true}',
        headers=_headers(),
        raw_query=b"",
    )
    assert isinstance(response, StreamingResponse)
    received = [chunk async for chunk in response.body_iterator]
    assert response.background is not None
    await response.background()
    assert received == chunks
    snapshot = await frontier.snapshot("job-1")
    calls = snapshot["lines"][0]["tool_calls"]
    assert [item["tool_call_id"] for item in calls] == ["call-a", "call-b"]
    assert [item["tool_name"] for item in calls] == ["web_search", "terminal"]
    event = next(item for item in sink.records if item["event_type"] == "llm_response")
    assert event["fields"]["stream_chunks"] == 3
    await client.aclose()


@pytest.mark.anyio
async def test_responses_api_collects_function_call_usage_without_mutating_body() -> (
    None
):
    payload = {
        "id": "response-api-1",
        "output": [
            {
                "type": "function_call",
                "call_id": "response-call-1",
                "name": "web_search",
                "arguments": '{"query":"phase zero"}',
            }
        ],
        "usage": {"input_tokens": 10, "output_tokens": 4},
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/responses"
        return httpx.Response(200, content=json.dumps(payload, separators=(",", ":")))

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/responses",
        api_kind="responses",
        body=b'{"model":"model-a","input":"hello"}',
        headers=_headers(),
        raw_query=b"",
    )
    assert json.loads(bytes(response.body)) == payload
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["tool_calls"][0]["tool_call_id"] == "response-call-1"
    assert snapshot["lines"][0]["phase"] == "BLOCKED"
    event = next(item for item in sink.records if item["event_type"] == "llm_response")
    assert event["fields"]["usage"] == {"input_tokens": 10, "output_tokens": 4}
    await client.aclose()


@pytest.mark.anyio
async def test_provider_error_and_repeated_headers_are_preserved() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers=[
                ("set-cookie", "a=1"),
                ("set-cookie", "b=2"),
                ("server", "upstream"),
            ],
            content=b'{"error":{"type":"rate_limit"}}',
        )

    gateway, frontier, _, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[]}',
        headers=_headers(),
        raw_query=b"",
    )
    assert response.status_code == 429
    assert response.body == b'{"error":{"type":"rate_limit"}}'
    assert [
        value for key, value in response.raw_headers if key.lower() == b"set-cookie"
    ] == [b"a=1", b"b=2"]
    assert response.headers["content-length"] == str(len(response.body))
    assert "server" not in response.headers
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["phase"] == "EMPTY"
    assert snapshot["lines"][0]["version"] == 0
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.PROVIDER_ERROR.value
    assert calls[-1]["terminal_reason"] == "upstream_http_429"
    retry = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[]}',
        headers=_headers(),
        raw_query=b"",
    )
    assert retry.status_code == 429
    await client.aclose()


@pytest.mark.anyio
async def test_streaming_provider_error_rolls_back_tail() -> None:
    body = b'data: {"error":{"type":"rate_limit"}}\n\ndata: [DONE]\n\n'

    class BodyStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield body

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"content-type": "text/event-stream"},
            stream=BodyStream(),
        )

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[],"stream":true}',
        headers=_headers(),
        raw_query=b"",
    )
    assert isinstance(response, StreamingResponse)
    chunks = [chunk async for chunk in response.body_iterator]
    assert all(isinstance(chunk, bytes) for chunk in chunks)
    received = b"".join(chunk for chunk in chunks if isinstance(chunk, bytes))
    assert received == body
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["version"] == 0
    assert snapshot["lines"][0]["state"] == "EMPTY"
    event = next(
        item for item in sink.records if item["event_type"] == "llm_provider_error"
    )
    assert event["fields"]["authoritative_tail_version"] == 0
    await client.aclose()


@pytest.mark.anyio
async def test_connection_failure_fails_over_to_next_compatible_instance() -> None:
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host or "")
        if request.url.host == "inference-a":
            raise httpx.ConnectError("unavailable", request=request)
        return httpx.Response(200, json={"id": "response-b", "choices": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    sink = InMemoryTraceSink()
    gateway = LLMGateway(
        client,
        InferenceRouter(
            (
                InferenceInstance(
                    "inference-a", "http://inference-a", frozenset({"model-a"})
                ),
                InferenceInstance(
                    "inference-b", "http://inference-b", frozenset({"model-a"})
                ),
            )
        ),
        frontier,
        TraceRecorder(sink),
        ingress_api_key="test-key",
        require_ingress_auth=True,
    )
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[]}',
        headers=_headers(),
        raw_query=b"",
    )
    assert response.status_code == 200
    assert seen == ["inference-a", "inference-b"]
    assert response.headers["x-flowpilot-instance-id"] == "inference-b"
    await client.aclose()


@pytest.mark.anyio
async def test_incompatible_model_returns_typed_gateway_failure() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("incompatible instance must not be called")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    gateway = LLMGateway(
        client,
        InferenceRouter(
            (
                InferenceInstance(
                    "inference-a", "http://inference-a", frozenset({"model-b"})
                ),
            )
        ),
        frontier,
        TraceRecorder(InMemoryTraceSink()),
        ingress_api_key="test-key",
        require_ingress_auth=True,
    )
    with pytest.raises(GatewayUpstreamError, match="all compatible"):
        await gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=b'{"model":"model-a","messages":[]}',
            headers=_headers(),
            raw_query=b"",
        )
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.UPSTREAM_FAILED.value
    assert calls[-1]["authoritative_tail_version"] == 0
    await client.aclose()


@pytest.mark.anyio
async def test_malformed_stream_tool_fragment_rolls_back_tail() -> None:
    class BodyStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield (
                b'data: {"id":"r","choices":[{"delta":{"tool_calls":'
                b'[{"index":0,"id":"tc","function":{"name":"terminal",'
                b'"arguments":"{"}}]},"finish_reason":"tool_calls"}]}\n\n'
            )
            yield b"data: [DONE]\n\n"

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=BodyStream())

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","stream":true}',
        headers=_headers(),
        raw_query=b"",
    )
    assert isinstance(response, StreamingResponse)
    _received = [chunk async for chunk in response.body_iterator]
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["version"] == 0
    event = next(
        item for item in sink.records if item["event_type"] == "llm_stream_failed"
    )
    assert event["fields"]["authoritative_tail_version"] == 0
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.PROTOCOL_ERROR.value
    await client.aclose()


@pytest.mark.anyio
async def test_malformed_nonstream_tool_fragment_rolls_back_tail() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "r",
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "tc",
                                    "function": {
                                        "name": "terminal",
                                        "arguments": "{",
                                    },
                                }
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
            },
        )

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[]}',
        headers=_headers(),
        raw_query=b"",
    )
    snapshot = await frontier.snapshot("job-1")
    assert response.status_code == 200
    assert response.headers["x-flowpilot-tail-version"] == "0"
    assert snapshot["lines"][0]["version"] == 0
    assert snapshot["lines"][0]["state"] == "EMPTY"
    event = next(
        item for item in sink.records if item["event_type"] == "llm_response_failed"
    )
    assert event["fields"]["protocol_error"] == "malformed_tool_arguments"
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.PROTOCOL_ERROR.value
    await client.aclose()


@pytest.mark.anyio
async def test_cancel_while_waiting_for_upstream_headers_rolls_back_tail() -> None:
    request_started = asyncio.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        request_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    task = asyncio.create_task(
        gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=b'{"model":"model-a","messages":[]}',
            headers=_headers(),
            raw_query=b"",
        )
    )
    await request_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["state"] == "EMPTY"
    assert snapshot["lines"][0]["version"] == 0
    cancelled = [item for item in sink.records if item["event_type"] == "llm_cancelled"]
    assert len(cancelled) == 1
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.CANCELLED.value
    await client.aclose()


@pytest.mark.anyio
async def test_cancel_while_reading_nonstream_response_closes_and_rolls_back() -> None:
    read_started = asyncio.Event()
    closed = asyncio.Event()

    class BlockingBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            read_started.set()
            await asyncio.Event().wait()
            yield b"unreachable"

        async def aclose(self) -> None:
            closed.set()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=BlockingBody())

    gateway, frontier, sink, client = await _gateway(httpx.MockTransport(handler))
    task = asyncio.create_task(
        gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=b'{"model":"model-a","messages":[]}',
            headers=_headers(),
            raw_query=b"",
        )
    )
    await read_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert closed.is_set()
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["state"] == "EMPTY"
    assert snapshot["lines"][0]["version"] == 0
    assert sum(item["event_type"] == "llm_cancelled" for item in sink.records) == 1
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.CANCELLED.value
    await client.aclose()


@pytest.mark.anyio
async def test_nonstream_read_and_close_errors_still_terminate_call() -> None:
    class BrokenBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise RuntimeError("read failed")
            yield b"unreachable"

        async def aclose(self) -> None:
            raise OSError("close failed")

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=BrokenBody())

    gateway, frontier, _, client = await _gateway(httpx.MockTransport(handler))
    with pytest.raises(GatewayUpstreamError, match="response read failed"):
        await gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=b'{"model":"model-a","messages":[]}',
            headers=_headers(),
            raw_query=b"",
        )
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["phase"] == "EMPTY"
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.STREAM_ERROR.value
    assert calls[-1]["terminal_reason"] == "stream_RuntimeError"
    await client.aclose()


@pytest.mark.anyio
async def test_failed_stream_visible_version_allows_only_same_identity_retry() -> None:
    attempts = 0

    class MalformedBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(200, stream=MalformedBody())
        return httpx.Response(200, json={"id": "retry-ok", "choices": []})

    gateway, frontier, _, client = await _gateway(httpx.MockTransport(handler))
    first = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[],"stream":true}',
        headers=_headers(),
        raw_query=b"",
    )
    assert first.headers["x-flowpilot-tail-version"] == "1"
    assert isinstance(first, StreamingResponse)
    _received = [chunk async for chunk in first.body_iterator]

    conflicting_headers = _headers(1)
    with pytest.raises(Exception, match="tail version") as conflict:
        await gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=b'{"model":"model-a","messages":[]}',
            headers=conflicting_headers,
            raw_query=b"",
        )
    assert getattr(conflict.value, "status_code", None) == 409

    retry_headers = _headers()
    retry_headers["x-flowpilot-tail-version"] = "1"
    retry = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","messages":[]}',
        headers=retry_headers,
        raw_query=b"",
    )
    assert retry.status_code == 200
    assert retry.headers["x-flowpilot-tail-version"] == "1"
    snapshot = await frontier.snapshot("job-1")
    assert snapshot["lines"][0]["state"] == "READY"
    assert snapshot["lines"][0]["version"] == 1
    calls = await gateway.gateway_calls()
    assert [
        (call["llm_call_id"], call["attempt"], call["phase"]) for call in calls
    ] == [
        ("call-1", 1, GatewayCallPhase.PROTOCOL_ERROR.value),
        ("call-2", 1, GatewayCallPhase.PROTOCOL_ERROR.value),
        ("call-1", 2, GatewayCallPhase.COMPLETED.value),
    ]
    assert all(call["phase"] != GatewayCallPhase.ACTIVE.value for call in calls)
    await client.aclose()


@pytest.mark.anyio
async def test_late_stream_callback_records_current_version() -> None:
    class BodyStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield (
                b'data: {"id":"late","choices":[{"delta":{},'
                b'"finish_reason":"stop"}]}\n\n'
            )
            yield b"data: [DONE]\n\n"

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=BodyStream())

    gateway, frontier, _sink, client = await _gateway(httpx.MockTransport(handler))
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=b'{"model":"model-a","stream":true}',
        headers=_headers(),
        raw_query=b"",
    )
    await frontier.mark_terminal("job-1", "line-1", "external_conflict")
    assert isinstance(response, StreamingResponse)
    _received = [chunk async for chunk in response.body_iterator]
    calls = await gateway.gateway_calls()
    assert calls[-1]["phase"] == GatewayCallPhase.COMPLETED.value
    assert calls[-1]["authoritative_tail_version"] == 1
    await client.aclose()
