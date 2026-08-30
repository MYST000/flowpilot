from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from flowpilot.frontier.store import ToolCallSummary


@dataclass(slots=True)
class CompletionMetadata:
    response_id: str | None
    tool_calls: list[ToolCallSummary]
    usage: dict[str, Any] | None
    finish_reasons: list[str]
    stream_chunks: int
    response_bytes: int
    first_byte_ms: float | None
    client_cancelled: bool = False
    protocol_error: str | None = None


class CompletionAccumulator:
    def __init__(self, api_kind: str) -> None:
        self.api_kind = api_kind
        self.response_id: str | None = None
        self.usage: dict[str, Any] | None = None
        self.finish_reasons: list[str] = []
        self._calls: dict[tuple[int, int], dict[str, Any]] = {}
        self._response_calls: dict[str, dict[str, Any]] = {}
        self._response_item_keys: dict[str, str] = {}
        self._protocol_errors: set[str] = set()

    def feed_json(self, payload: dict[str, Any], event_name: str | None = None) -> None:
        if not isinstance(payload, dict):
            return
        response = payload.get("response")
        if isinstance(response, dict):
            self._feed_response_object(response)
        if isinstance(payload.get("id"), str):
            self.response_id = payload["id"]
        if isinstance(payload.get("usage"), dict):
            self.usage = payload["usage"]
        if self.api_kind == "chat":
            self._feed_chat(payload)
        else:
            self._feed_response_object(payload)
            self._feed_response_event(payload, event_name)

    def finalize(self, *, require_finish_reason: bool = False) -> CompletionMetadata:
        calls: list[ToolCallSummary] = []
        protocol_errors = set(self._protocol_errors)
        values = list(self._calls.values()) + list(self._response_calls.values())
        seen_call_ids: set[str] = set()
        for index, call in enumerate(values):
            tool_call_id = (
                call.get("call_id")
                or call.get("id")
                or call.get("item_id")
                or f"missing-{index}"
            )
            name = call.get("name") or "unknown"
            arguments = call.get("arguments")
            if not isinstance(arguments, str):
                arguments = ""
            if not call.get("id") and not call.get("call_id"):
                protocol_errors.add("missing_tool_call_id")
            elif str(tool_call_id) in seen_call_ids:
                protocol_errors.add("duplicate_tool_call_id")
            seen_call_ids.add(str(tool_call_id))
            if not call.get("name"):
                protocol_errors.add("missing_tool_name")
            if not arguments:
                protocol_errors.add("missing_tool_arguments")
            else:
                try:
                    parsed_arguments = json.loads(arguments)
                except json.JSONDecodeError:
                    protocol_errors.add("malformed_tool_arguments")
                else:
                    if not isinstance(parsed_arguments, dict):
                        protocol_errors.add("malformed_tool_arguments")
            calls.append(
                ToolCallSummary(
                    tool_call_id=str(tool_call_id),
                    tool_name=str(name),
                    arguments_digest=hashlib.sha256(arguments.encode()).hexdigest()
                    if arguments
                    else None,
                    arguments_bytes=len(arguments.encode()) if arguments else None,
                )
            )
        metadata = CompletionMetadata(
            response_id=self.response_id,
            tool_calls=calls,
            usage=self.usage,
            finish_reasons=self.finish_reasons,
            stream_chunks=0,
            response_bytes=0,
            first_byte_ms=None,
            protocol_error=",".join(sorted(set(protocol_errors))) or None,
        )
        if require_finish_reason and not metadata.finish_reasons:
            metadata.protocol_error = metadata.protocol_error or "incomplete_sse"
        return metadata

    def _feed_chat(self, payload: dict[str, Any]) -> None:
        choices = payload.get("choices")
        if not isinstance(choices, list):
            return
        for choice_position, choice in enumerate(choices):
            if not isinstance(choice, dict):
                continue
            choice_index = choice.get("index", choice_position)
            if not isinstance(choice_index, int):
                choice_index = choice_position
            finish = choice.get("finish_reason")
            if isinstance(finish, str):
                self.finish_reasons.append(finish)
            message = choice.get("delta") or choice.get("message") or {}
            if not isinstance(message, dict):
                continue
            calls = message.get("tool_calls")
            if calls is not None and not isinstance(calls, list):
                self._protocol_errors.add("malformed_tool_calls")
            if isinstance(calls, list):
                for item in calls:
                    if not isinstance(item, dict):
                        self._protocol_errors.add("malformed_tool_call")
                        continue
                    index = item.get("index", len(self._calls))
                    if not isinstance(index, int):
                        index = len(self._calls)
                    target = self._calls.setdefault(
                        (choice_index, index),
                        {"id": None, "name": None, "arguments": ""},
                    )
                    if isinstance(item.get("id"), str):
                        target["id"] = item["id"]
                    function = item.get("function")
                    if function is None or not isinstance(function, dict):
                        self._protocol_errors.add("malformed_tool_call_function")
                    else:
                        if isinstance(function.get("name"), str):
                            target["name"] = function["name"]
                        if isinstance(function.get("arguments"), str):
                            target["arguments"] += function["arguments"]
            function_call = message.get("function_call")
            if function_call is not None and not isinstance(function_call, dict):
                self._protocol_errors.add("malformed_function_call")
            if isinstance(function_call, dict):
                target = self._calls.setdefault(
                    (choice_index, 0),
                    {"id": "function-call-0", "name": None, "arguments": ""},
                )
                if isinstance(function_call.get("name"), str):
                    target["name"] = function_call["name"]
                if isinstance(function_call.get("arguments"), str):
                    target["arguments"] += function_call["arguments"]

    def _feed_response_object(self, response: dict[str, Any]) -> None:
        if isinstance(response.get("id"), str):
            self.response_id = response["id"]
        if isinstance(response.get("usage"), dict):
            self.usage = response["usage"]
        output = response.get("output")
        if output is not None and not isinstance(output, list):
            self._protocol_errors.add("malformed_response_output")
        if isinstance(output, list):
            for item in output:
                self._add_response_call(item)

    def _feed_response_event(
        self,
        payload: dict[str, Any],
        event_name: str | None,
    ) -> None:
        if event_name == "response.completed":
            response = payload.get("response")
            if isinstance(response, dict):
                self._feed_response_object(response)
        event_type = payload.get("type") or event_name
        if event_type == "response.output_item.added":
            self._add_response_call(payload.get("item"))
        if event_type == "response.function_call_arguments.delta":
            item_id = payload.get("item_id")
            delta = payload.get("delta")
            if isinstance(item_id, str) and isinstance(delta, str):
                key = self._response_item_keys.get(item_id, item_id)
                target = self._response_calls.setdefault(
                    key,
                    {
                        "id": None,
                        "call_id": None,
                        "item_id": item_id,
                        "name": None,
                        "arguments": "",
                    },
                )
                target["arguments"] += delta
            else:
                self._protocol_errors.add("malformed_tool_arguments_delta")
        if event_type == "response.completed":
            self.finish_reasons.append("completed")

    def _add_response_call(self, item: Any) -> None:
        if not isinstance(item, dict):
            self._protocol_errors.add("malformed_response_output_item")
            return
        if item.get("type") != "function_call":
            return
        item_id = item.get("id")
        call_id = item.get("call_id")
        key = (
            self._response_item_keys.get(item_id) if isinstance(item_id, str) else None
        )
        key = key or call_id or item_id
        if not isinstance(key, str):
            key = f"response-call-{len(self._response_calls)}"
        if isinstance(item_id, str):
            self._response_item_keys[item_id] = key
        target = self._response_calls.setdefault(
            key,
            {
                "id": item_id if isinstance(item_id, str) else None,
                "call_id": call_id if isinstance(call_id, str) else None,
                "item_id": item_id if isinstance(item_id, str) else None,
                "name": None,
                "arguments": "",
            },
        )
        if isinstance(item_id, str):
            target["id"] = item_id
            target["item_id"] = item_id
        if isinstance(call_id, str):
            target["call_id"] = call_id
        if isinstance(item.get("name"), str):
            target["name"] = item["name"]
        if isinstance(item.get("arguments"), str):
            target["arguments"] = item["arguments"]


class SSEFrameParser:
    def __init__(self) -> None:
        self._buffer = b""

    def feed(self, chunk: bytes) -> list[tuple[str | None, bytes]]:
        self._buffer += chunk
        frames: list[tuple[str | None, bytes]] = []
        while True:
            boundary = _find_boundary(self._buffer)
            if boundary is None:
                break
            index, width = boundary
            frame = self._buffer[:index]
            self._buffer = self._buffer[index + width :]
            parsed = _parse_frame(frame)
            if parsed is not None:
                frames.append(parsed)
        return frames

    def finish(self) -> list[tuple[str | None, bytes]]:
        if not self._buffer:
            return []
        parsed = _parse_frame(self._buffer)
        self._buffer = b""
        return [parsed] if parsed is not None else []


class ObservedStream:
    def __init__(
        self,
        source: AsyncIterator[bytes],
        *,
        api_kind: str,
        on_complete: Callable[[CompletionMetadata], Awaitable[None]],
        on_cancel: Callable[[], Awaitable[None]],
        on_error: Callable[[Exception], Awaitable[None]] | None = None,
        close_source: Callable[[], Awaitable[None]] | None = None,
        started_ms: float,
    ) -> None:
        self._source = source
        self._api_kind = api_kind
        self._accumulator = CompletionAccumulator(api_kind)
        self._parser = SSEFrameParser()
        self._on_complete = on_complete
        self._on_cancel = on_cancel
        self._on_error = on_error or _ignore_stream_error
        self._close_source_callback = close_source
        self._started_ms = started_ms
        self._bytes = 0
        self._chunks = 0
        self._first_byte_ms: float | None = None
        self._done = False
        self._source_closed = False
        self._saw_done = False
        self._malformed_frames = 0

    def __aiter__(self) -> ObservedStream:
        return self

    async def __anext__(self) -> bytes:
        try:
            chunk = await self._source.__anext__()
        except StopAsyncIteration:
            await self._finish()
            raise
        except asyncio.CancelledError:
            await self._cancel()
            raise
        except Exception as exc:
            await self._error(exc)
            raise
        self._chunks += 1
        self._bytes += len(chunk)
        if self._first_byte_ms is None:
            self._first_byte_ms = _elapsed_ms(self._started_ms)
        for event_name, data in self._parser.feed(chunk):
            if data == b"[DONE]":
                self._saw_done = True
                continue
            try:
                payload = json.loads(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                self._malformed_frames += 1
                continue
            if isinstance(payload, dict):
                self._accumulator.feed_json(payload, event_name)
        return chunk

    async def aclose(self) -> None:
        await self._cancel()
        await self._close_source()

    async def _finish(self) -> None:
        if self._done:
            return
        self._done = True
        trailing = self._parser.finish()
        for event_name, data in trailing:
            try:
                payload = json.loads(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                self._malformed_frames += 1
                continue
            if isinstance(payload, dict):
                self._accumulator.feed_json(payload, event_name)
        metadata = self._accumulator.finalize(require_finish_reason=True)
        metadata.stream_chunks = self._chunks
        metadata.response_bytes = self._bytes
        metadata.first_byte_ms = self._first_byte_ms
        if trailing:
            metadata.protocol_error = metadata.protocol_error or "incomplete_sse_frame"
        if self._malformed_frames:
            metadata.protocol_error = metadata.protocol_error or "malformed_sse_json"
        if self._api_kind == "chat" and not self._saw_done:
            metadata.protocol_error = metadata.protocol_error or "missing_sse_done"
        if not metadata.finish_reasons:
            metadata.protocol_error = metadata.protocol_error or "incomplete_sse"
        try:
            await self._on_complete(metadata)
        finally:
            await self._close_source()

    async def _cancel(self) -> None:
        if self._done:
            return
        self._done = True
        try:
            await self._on_cancel()
        finally:
            await self._close_source()

    async def _error(self, exc: Exception) -> None:
        if self._done:
            return
        self._done = True
        try:
            await self._on_error(exc)
        finally:
            await self._close_source()

    async def _close_source(self) -> None:
        if self._source_closed:
            return
        self._source_closed = True
        if self._close_source_callback is not None:
            await self._close_source_callback()
            return
        close = getattr(self._source, "aclose", None)
        if close is not None:
            await close()


def _find_boundary(buffer: bytes) -> tuple[int, int] | None:
    candidates = [(buffer.find(b"\n\n"), 2), (buffer.find(b"\r\n\r\n"), 4)]
    candidates = [(index, width) for index, width in candidates if index >= 0]
    return min(candidates) if candidates else None


def _parse_frame(frame: bytes) -> tuple[str | None, bytes] | None:
    event_name: str | None = None
    data_lines: list[bytes] = []
    for line in frame.replace(b"\r\n", b"\n").split(b"\n"):
        if line.startswith(b"event:"):
            event_name = line[6:].strip().decode("utf-8", "replace") or None
        elif line.startswith(b"data:"):
            data_lines.append(line[5:].lstrip())
    if not data_lines:
        return None
    return event_name, b"\n".join(data_lines)


def _elapsed_ms(started_ms: float) -> float:
    import time

    return round(time.monotonic() * 1000 - started_ms, 3)


async def _ignore_stream_error(_exc: Exception) -> None:
    return None
