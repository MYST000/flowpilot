from __future__ import annotations

import asyncio
import time

import pytest

from flowpilot.gateway.stream import (
    CompletionAccumulator,
    CompletionMetadata,
    ObservedStream,
)


def test_responses_tool_argument_delta_uses_item_id_but_preserves_call_id() -> None:
    accumulator = CompletionAccumulator("responses")
    accumulator.feed_json(
        {
            "type": "response.output_item.added",
            "item": {
                "type": "function_call",
                "id": "fc_item_1",
                "call_id": "call_1",
                "name": "web_search",
                "arguments": "",
            },
        },
        "response.output_item.added",
    )
    accumulator.feed_json(
        {
            "type": "response.function_call_arguments.delta",
            "item_id": "fc_item_1",
            "delta": '{"query":"x"}',
        },
        "response.function_call_arguments.delta",
    )

    metadata = accumulator.finalize()

    assert metadata.protocol_error is None
    assert [(call.tool_call_id, call.tool_name) for call in metadata.tool_calls] == [
        ("call_1", "web_search")
    ]


def test_chat_tool_fragments_are_isolated_between_choices() -> None:
    accumulator = CompletionAccumulator("chat")
    accumulator.feed_json(
        {
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call_0",
                                "function": {
                                    "name": "web_search",
                                    "arguments": '{"q":"a"}',
                                },
                            }
                        ]
                    },
                },
                {
                    "index": 1,
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call_1",
                                "function": {
                                    "name": "web_search",
                                    "arguments": '{"q":"b"}',
                                },
                            }
                        ]
                    },
                },
            ]
        }
    )
    accumulator.feed_json(
        {
            "choices": [
                {"index": 0, "delta": {}, "finish_reason": "stop"},
                {"index": 1, "delta": {}, "finish_reason": "stop"},
            ]
        }
    )

    metadata = accumulator.finalize()

    assert metadata.protocol_error is None
    assert [call.tool_call_id for call in metadata.tool_calls] == [
        "call_0",
        "call_1",
    ]


@pytest.mark.anyio
async def test_stream_cancellation_is_reported_without_completion() -> None:
    cancelled = asyncio.Event()
    closed = asyncio.Event()
    completed: list[CompletionMetadata] = []

    async def source():
        await asyncio.Event().wait()
        yield b"unreachable"

    async def on_complete(metadata: CompletionMetadata) -> None:
        completed.append(metadata)

    async def on_cancel() -> None:
        cancelled.set()

    observer = ObservedStream(
        source().__aiter__(),
        api_kind="chat",
        on_complete=on_complete,
        on_cancel=on_cancel,
        close_source=lambda: _set_event(closed),
        started_ms=time.monotonic() * 1000,
    )
    task = asyncio.create_task(observer.__anext__())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    assert closed.is_set()
    assert completed == []


@pytest.mark.anyio
async def test_stream_read_error_is_reported_without_completion() -> None:
    errors: list[str] = []
    closed = asyncio.Event()

    async def source():
        yield b'data: {"choices":[]}\n\n'
        raise RuntimeError("broken upstream")

    async def on_complete(_metadata: CompletionMetadata) -> None:
        raise AssertionError("failed stream must not complete")

    async def on_cancel() -> None:
        raise AssertionError("upstream failure is not client cancellation")

    async def on_error(exc: Exception) -> None:
        errors.append(type(exc).__name__)

    observer = ObservedStream(
        source().__aiter__(),
        api_kind="chat",
        on_complete=on_complete,
        on_cancel=on_cancel,
        on_error=on_error,
        close_source=lambda: _set_event(closed),
        started_ms=time.monotonic() * 1000,
    )
    assert await observer.__anext__()
    with pytest.raises(RuntimeError, match="broken upstream"):
        await observer.__anext__()
    assert errors == ["RuntimeError"]
    assert closed.is_set()


async def _set_event(event: asyncio.Event) -> None:
    event.set()
