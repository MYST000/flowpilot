from __future__ import annotations

import asyncio
import gzip
import json
import zlib

import httpx
import pytest
from cryptography.fernet import Fernet
from starlette.responses import StreamingResponse
from test_gateway import _gateway, _headers

from flowpilot.context.manager import DeferredContextManager
from flowpilot.gateway.stream import ObservedStream


class Wire(httpx.AsyncByteStream):
    def __init__(
        self, body: bytes, *, stay_open: bool = False, chunk_size: int = 7
    ) -> None:
        self.body = body
        self.stay_open = stay_open
        self.chunk_size = chunk_size
        self.closed = False
        self.read_past_terminal = False

    async def __aiter__(self):
        # Split both compressed bytes and SSE frames across transport chunks.
        for start in range(0, len(self.body), self.chunk_size):
            yield self.body[start : start + self.chunk_size]
        if self.stay_open:
            self.read_past_terminal = True
            await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


def response_body(api_kind: str, stream: bool) -> bytes:
    if api_kind == "chat":
        payload = {
            "id": "reply",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
        }
    else:
        payload = {"id": "reply", "status": "completed", "output": []}
    if not stream:
        return json.dumps(payload).encode()
    if api_kind == "chat":
        return b"data: " + json.dumps(payload).encode() + b"\n\ndata: [DONE]\n\n"
    event = {"type": "response.completed", "response": payload}
    return b"event: response.completed\ndata: " + json.dumps(event).encode() + b"\n\n"


def request_path(api_kind: str) -> str:
    return "/v1/chat/completions" if api_kind == "chat" else "/v1/responses"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "api_kind,done_suffix", [("chat", False), ("responses", False), ("responses", True)]
)
async def test_terminal_frame_commits_before_client_closes(api_kind, done_suffix):
    body = response_body(api_kind, True)
    if done_suffix:
        body += b"data: [DONE]\n\n"
    source = Wire(body, stay_open=True, chunk_size=len(body) if done_suffix else 7)
    gateway, frontier, sink, client = await _gateway(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200, headers={"content-type": "text/event-stream"}, stream=source
            )
        )
    )
    async with client:
        response = await gateway.proxy(
            path=request_path(api_kind),
            api_kind=api_kind,
            body=b'{"model":"test","stream":true}',
            headers=_headers(),
            raw_query=b"",
        )
        assert isinstance(response, StreamingResponse)
        assert isinstance(response.body_iterator, ObservedStream)
        received = b""
        while len(received) < len(body):
            received += await response.body_iterator.__anext__()
        assert received == body
        # Clients stop consuming as soon as the terminal event is received.
        await response.body_iterator.aclose()
        assert (await gateway.gateway_calls())[0]["phase"] == "completed"
        assert (await frontier.line_snapshot("job-1", "line-1"))["phase"] == "READY"
        assert source.closed
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(response.body_iterator.__anext__(), timeout=1)
        assert not source.read_past_terminal
        assert not any(
            record["event_type"] == "llm_cancelled" for record in sink.records
        )


@pytest.mark.anyio
@pytest.mark.parametrize("api_kind", ["chat", "responses"])
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
async def test_compressed_response_has_consistent_body_headers_and_tail(
    api_kind, stream, encoding
):
    body = response_body(api_kind, stream)
    compressed = gzip.compress(body) if encoding == "gzip" else zlib.compress(body)
    source = Wire(compressed)
    gateway, frontier, _, client = await _gateway(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers=[
                    (
                        "content-type",
                        "text/event-stream" if stream else "application/json",
                    ),
                    ("content-encoding", encoding),
                    ("content-length", str(len(compressed))),
                    ("set-cookie", "a=1"),
                    ("set-cookie", "b=2"),
                ],
                stream=source,
            )
        )
    )
    async with client:
        response = await gateway.proxy(
            path=request_path(api_kind),
            api_kind=api_kind,
            body=json.dumps({"model": "test", "stream": stream}).encode(),
            headers=_headers(),
            raw_query=b"",
        )
        if stream:
            assert isinstance(response, StreamingResponse)
            assert isinstance(response.body_iterator, ObservedStream)
            received = b"".join([chunk async for chunk in response.body_iterator])
        else:
            received = response.body
        assert "content-encoding" not in response.headers
        assert received == body
        if "content-length" in response.headers:
            assert int(response.headers["content-length"]) == len(received)
        assert response.headers.getlist("set-cookie") == ["a=1", "b=2"]
        assert (await gateway.gateway_calls())[0]["phase"] == "completed"
        assert (await frontier.line_snapshot("job-1", "line-1"))["phase"] == "READY"
        assert source.closed


@pytest.mark.anyio
@pytest.mark.parametrize("stream", [False, True])
async def test_compressed_provider_error_preserves_status_and_body(stream):
    body = b'{"error":{"type":"rate_limit"}}'
    if stream:
        body = b"data: " + body + b"\n\ndata: [DONE]\n\n"
    source = Wire(gzip.compress(body))
    gateway, frontier, _, client = await _gateway(
        httpx.MockTransport(
            lambda _: httpx.Response(
                429, headers={"content-encoding": "gzip"}, stream=source
            )
        )
    )
    async with client:
        response = await gateway.proxy(
            path=request_path("chat"),
            api_kind="chat",
            body=json.dumps({"model": "test", "stream": stream}).encode(),
            headers=_headers(),
            raw_query=b"",
        )
        if stream:
            assert isinstance(response, StreamingResponse)
            assert isinstance(response.body_iterator, ObservedStream)
            received = b"".join([chunk async for chunk in response.body_iterator])
        else:
            received = response.body
        assert response.status_code == 429
        assert received == body
        assert "content-encoding" not in response.headers
        assert (await gateway.gateway_calls())[0]["phase"] == "provider_error"
        assert (await frontier.line_snapshot("job-1", "line-1"))["phase"] == "EMPTY"
        assert source.closed


@pytest.mark.anyio
@pytest.mark.parametrize("encoding", ["custom", "custom, gzip"])
async def test_unknown_encoding_is_not_removed_from_passthrough_error(encoding):
    body = b"opaque provider error"
    wire = gzip.compress(body) if encoding.endswith("gzip") else body
    gateway, _, _, client = await _gateway(
        httpx.MockTransport(
            lambda _: httpx.Response(
                429, headers={"content-encoding": encoding}, stream=Wire(wire)
            )
        )
    )
    async with client:
        response = await gateway.proxy(
            path=request_path("chat"),
            api_kind="chat",
            body=b'{"model":"test"}',
            headers=_headers(),
            raw_query=b"",
        )
        assert response.status_code == 429
        assert response.headers["content-encoding"] == "custom"
        assert response.body == body
        assert int(response.headers["content-length"]) == len(body)


@pytest.mark.anyio
async def test_cancel_during_identity_validation_allows_retry(tmp_path):
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", Fernet.generate_key())
    gateway, frontier, sink, client = await _gateway(
        httpx.MockTransport(
            lambda _: httpx.Response(200, content=response_body("chat", False))
        )
    )
    entered = asyncio.Event()

    async def validate(identity):
        entered.set()
        await manager.authorize_llm_request(identity)

    gateway._identity_validator = validate
    async with client:
        async with manager._lock:
            pending = asyncio.create_task(
                gateway.proxy(
                    path=request_path("chat"),
                    api_kind="chat",
                    body=b'{"model":"test"}',
                    headers=_headers(),
                    raw_query=b"",
                )
            )
            await asyncio.wait_for(entered.wait(), timeout=1)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
        cancelled = (await gateway.gateway_calls())[0]
        assert cancelled["phase"] == "cancelled"
        assert cancelled["authoritative_tail_version"] == 0
        assert (await frontier.line_snapshot("job-1", "line-1"))["phase"] == "EMPTY"
        response = await gateway.proxy(
            path=request_path("chat"),
            api_kind="chat",
            body=b'{"model":"test"}',
            headers=_headers(call_id="retry-call", attempt=2),
            raw_query=b"",
        )
        assert response.status_code == 200
        assert [call["phase"] for call in await gateway.gateway_calls()] == [
            "cancelled",
            "completed",
        ]
        assert (
            sum(record["event_type"] == "llm_cancelled" for record in sink.records) == 1
        )
