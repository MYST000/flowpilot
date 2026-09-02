from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="FlowPilot mock inference")


@app.get("/v1/models")
async def models() -> dict[str, object]:
    return {
        "object": "list",
        "data": [{"id": "flowpilot-mock", "object": "model"}],
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    payload = await request.json()
    if payload.get("stream") is True:
        return StreamingResponse(_chat_stream(), media_type="text/event-stream")
    if payload.get("tools"):
        return JSONResponse(_agent_chat_response(payload))
    return JSONResponse(
        {
            "id": "chatcmpl-flowpilot-mock",
            "object": "chat.completion",
            "created": 0,
            "model": payload.get("model", "flowpilot-mock"),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "mock response"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
            },
        }
    )


@app.post("/v1/responses")
async def responses(request: Request) -> dict[str, object]:
    payload = await request.json()
    if payload.get("tools"):
        return _agent_responses_response(payload)
    return {
        "id": "resp-flowpilot-mock",
        "object": "response",
        "created_at": 0,
        "model": payload.get("model", "flowpilot-mock"),
        "status": "completed",
        "parallel_tool_calls": False,
        "tool_choice": "auto",
        "tools": [],
        "top_p": None,
        "instructions": payload.get("instructions"),
        "output": [
            {
                "id": "message-flowpilot-mock",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [
                    {
                        "type": "output_text",
                        "text": "mock response",
                        "annotations": [],
                    }
                ],
            }
        ],
        "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
    }


def _agent_responses_response(payload: dict[str, object]) -> dict[str, object]:
    inputs = payload.get("input")
    function_outputs = (
        [
            item
            for item in inputs
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        ]
        if isinstance(inputs, list)
        else []
    )
    if not function_outputs:
        output: list[dict[str, object]] = [
            {
                "id": "fc-flowpilot-search-1",
                "type": "function_call",
                "status": "completed",
                "call_id": "tool-call-responses-1",
                "name": "web_search",
                "arguments": '{"query":"flowpilot exact reuse"}',
            },
            {
                "id": "fc-flowpilot-search-2",
                "type": "function_call",
                "status": "completed",
                "call_id": "tool-call-responses-2",
                "name": "web_search",
                "arguments": '{"query":"flowpilot exact reuse"}',
            },
        ]
    else:
        output = [
            {
                "id": "message-flowpilot-agent-final",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [
                    {
                        "type": "output_text",
                        "text": "responses web search complete",
                        "annotations": [],
                    }
                ],
            }
        ]
    return {
        "id": f"resp-flowpilot-agent-{len(function_outputs)}",
        "object": "response",
        "created_at": 0,
        "model": payload.get("model", "flowpilot-mock"),
        "status": "completed",
        "parallel_tool_calls": False,
        "tool_choice": "auto",
        "tools": payload.get("tools", []),
        "top_p": None,
        "instructions": payload.get("instructions"),
        "output": output,
        "usage": {"input_tokens": 2, "output_tokens": 2, "total_tokens": 4},
    }


def _agent_chat_response(payload: dict[str, object]) -> dict[str, object]:
    messages = payload.get("messages")
    tool_results = (
        [
            item
            for item in messages
            if isinstance(item, dict) and item.get("role") == "tool"
        ]
        if isinstance(messages, list)
        else []
    )
    tools = payload.get("tools")
    tool_names = (
        {
            function.get("name")
            for tool in tools
            if isinstance(tool, dict)
            and isinstance((function := tool.get("function")), dict)
        }
        if isinstance(tools, list)
        else set()
    )
    local_barrier_scenario = "dcs-local-barrier-marker" in json.dumps(
        messages, sort_keys=True
    )
    serialized_messages = json.dumps(messages, sort_keys=True)
    if not tool_results and "web_search" in tool_names:
        query = (
            "flowpilot semantic scheduler architecture"
            if "phase3-semantic-source-marker" in serialized_messages
            else "semantic scheduler architecture flowpilot"
            if "phase3-semantic-follower-marker" in serialized_messages
            else "flowpilot in-flight concurrency"
            if "in flight" in serialized_messages
            else "flowpilot exact reuse"
        )
        tool_calls = [
            {
                "id": "tool-call-web-search",
                "type": "function",
                "function": {
                    "name": "web_search",
                    "arguments": json.dumps({"query": query}, separators=(",", ":")),
                },
            }
        ]
    elif (
        local_barrier_scenario
        and "local_read" in tool_names
        and len(tool_results) == 1
    ):
        tool_calls = [
            {
                "id": "tool-call-local-read",
                "type": "function",
                "function": {
                    "name": "local_read",
                    "arguments": '{"query":"authoritative local state"}',
                },
            }
        ]
    elif "web_search" in tool_names:
        return {
            "id": "chatcmpl-agent-web-final",
            "object": "chat.completion",
            "created": 0,
            "model": payload.get("model", "flowpilot-mock"),
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "web search complete",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 2,
                "completion_tokens": 2,
                "total_tokens": 4,
            },
        }
    elif not tool_results:
        tool_calls = [
            {
                "id": "tool-call-think-1",
                "type": "function",
                "function": {
                    "name": "think",
                    "arguments": '{"thought":"first ordered thought"}',
                },
            },
            {
                "id": "tool-call-think-2",
                "type": "function",
                "function": {
                    "name": "think",
                    "arguments": '{"thought":"second ordered thought"}',
                },
            },
        ]
    else:
        tool_calls = [
            {
                "id": "tool-call-finish",
                "type": "function",
                "function": {
                    "name": "finish",
                    "arguments": '{"message":"agent tool e2e complete"}',
                },
            }
        ]
    return {
        "id": f"chatcmpl-agent-{len(tool_results)}",
        "object": "chat.completion",
        "created": 0,
        "model": payload.get("model", "flowpilot-mock"),
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": tool_calls,
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
    }


async def _chat_stream() -> AsyncIterator[bytes]:
    chunks = [
        {
            "id": "chatcmpl-flowpilot-mock",
            "object": "chat.completion.chunk",
            "created": 0,
            "choices": [
                {
                    "index": 0,
                    "delta": {"role": "assistant", "content": "mock "},
                    "finish_reason": None,
                }
            ],
        },
        {
            "id": "chatcmpl-flowpilot-mock",
            "object": "chat.completion.chunk",
            "created": 0,
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": "response"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
            },
        },
    ]
    for chunk in chunks:
        data = json.dumps(chunk, separators=(",", ":"))
        yield f"data: {data}\n\n".encode()
        await asyncio.sleep(0)
    yield b"data: [DONE]\n\n"
