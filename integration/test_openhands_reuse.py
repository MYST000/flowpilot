"""Run with the OpenHands Python environment and this repository on PYTHONPATH."""

import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import mcp.types
import pytest
import uvicorn
from openhands.sdk import Agent, LocalConversation, LocalWorkspace
from openhands.sdk.event import ObservationEvent
from openhands.sdk.flowpilot import FlowPilotConfig, FlowPilotRuntime
from openhands.sdk.flowpilot_reuse import payload_digest
from openhands.sdk.llm import LLM
from openhands.sdk.mcp.definition import MCPToolAction, MCPToolObservation
from openhands.sdk.mcp.tool import MCPToolDefinition
from openhands.sdk.tool import ToolExecutor, register_tool
from openhands.sdk.tool.spec import Tool
from openhands.tools.url_fetch import (
    UrlFetchExecutor,
    UrlFetchObservation,
)
from pydantic import SecretStr

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import ToolRegistryEntry
from flowpilot.reuse.adapters.tavily import TAVILY_SCHEMA_DIGESTS, TAVILY_SCHEMAS
from flowpilot.scheduling.admission import AdmissionConfig


@pytest.mark.parametrize("family", ["url_fetch", "tavily-search", "tavily-extract"])
@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("inflight", [True, False])
@pytest.mark.parametrize("gateway", [True, False])
@pytest.mark.parametrize("admission", [False, True])
def test_agent_gateway_local_commit_then_history(
    tmp_path: Path, monkeypatch, gateway, inflight, family, deferred, admission
):
    if not gateway and deferred:
        pytest.skip("Runtime DCS is covered by the dedicated SDK tests")
    arguments = (
        {"command": "curl https://example.com"}
        if family == "url_fetch"
        else {"query": "FlowPilot research"}
        if family == "tavily-search"
        else {"urls": ["https://example.com/"]}
    )
    expected = (
        UrlFetchObservation.from_text(
            "opaque page body",
            exit_code=0,
            status_code=200,
            complete=True,
            network_policy_validated=True,
            final_url_digest=payload_digest("https://example.com/"),
        )
        if family == "url_fetch"
        else MCPToolObservation.from_call_tool_result(
            family,
            mcp.types.CallToolResult(
                content=[
                    mcp.types.TextContent(
                        type="text", text="opaque page body\nTitle: just content"
                    )
                ],
                isError=False,
            ),
        )
    )
    executions = []
    requests = []
    trace = InMemoryTraceSink()
    waiting = threading.Event()
    original_wait = FlowPilotRuntime._wait_for_reuse

    def wait(runtime, *args, **kwargs):
        waiting.set()
        return original_wait(runtime, *args, **kwargs)

    monkeypatch.setattr(FlowPilotRuntime, "_wait_for_reuse", wait)

    def execute(self, action, conversation=None):
        executions.append(action)
        if inflight:
            assert waiting.wait(10), "follower never attached"
        return expected.model_copy(deep=True)

    if family == "url_fetch":
        monkeypatch.setattr(UrlFetchExecutor, "__call__", execute)
    else:

        class FixtureExecutor(ToolExecutor):
            __call__ = execute

        register_tool(
            family,
            MCPToolDefinition(
                mcp_tool=mcp.types.Tool(
                    name=family, inputSchema=TAVILY_SCHEMAS[family]
                ),
                description="Pinned Tavily fixture with OpenHands schema validation",
                action_type=MCPToolAction,
                observation_type=MCPToolObservation,
                executor=FixtureExecutor(),
            ),
        )
    if not gateway:
        monkeypatch.setattr(
            FlowPilotRuntime, "configure_gateway_reuse", lambda *a, **k: None
        )

    async def upstream(request):
        if request.url.path == "/health":
            return httpx.Response(200)
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": 1})
        body = json.loads(request.content)
        requests.append(body)
        done = any(message.get("role") == "tool" for message in body["messages"])
        message = (
            {"role": "assistant", "content": "done"}
            if done
            else {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call-" + str(len(requests)),
                        "type": "function",
                        "function": {
                            "name": family,
                            "arguments": json.dumps(arguments),
                        },
                    }
                ],
            }
        )
        return httpx.Response(
            200,
            json={
                "id": "response-" + str(len(requests)),
                "object": "chat.completion",
                "model": "gpt-4o",
                "created": 1,
                "choices": [
                    {
                        "index": 0,
                        "message": message,
                        "finish_reason": "stop" if done else "tool_calls",
                    }
                ],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            },
        )

    app = create_app(
        Settings(
            instances=(InferenceInstance("mock", "http://mock"),),
            trace_path=tmp_path / "trace.jsonl",
            ingress_api_key="integration-key",
            admission=AdmissionConfig(enabled=admission, limit=1),
            reuse_enabled=True,
            dcs_enabled=deferred,
            dcs_encryption_key="0ifg6OOv5jfhrCjCMs6d-FpXabEPiIG7Ln86yc49i1o=",
            dcs_wal_path=tmp_path / "dcs.sqlite",
            reuse_cache_path=tmp_path / "reuse.sqlite",
            web_tool_registry=(
                ToolRegistryEntry(
                    tool_name=family,
                    canonical_tool_family=family,
                    tool_version="1" if family == "url_fetch" else "0.2.1",
                    adapter_id={
                        "url_fetch": "curl_url_fetch_v1",
                        "tavily-search": "tavily_search_mcp_v1",
                        "tavily-extract": "tavily_extract_mcp_v1",
                    }[family],
                    input_schema_digest=TAVILY_SCHEMA_DIGESTS.get(family),
                    result_schema_version="observation-v1",
                    url_execution_policy_id="public-pinned-get-v1",
                ),
            ),
        ),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(upstream)),
        trace_sink=trace,
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    base = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True
    )
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started
    try:

        def run(number):
            agent = Agent(
                llm=LLM(
                    model="openai/gpt-4o",
                    base_url=base + "/v1",
                    api_key=SecretStr("integration-key"),
                    caching_prompt=False,
                ),
                tools=[Tool(name=family)],
                include_default_tools=[],
                tool_concurrency_limit=1,
            )
            conversation = LocalConversation(
                agent=agent,
                workspace=LocalWorkspace(working_dir=tmp_path),
                visualizer=None,
                flowpilot=FlowPilotConfig(
                    enabled=True,
                    gateway_url=base,
                    api_key="integration-key",
                    job_id=f"integration-{number}",
                    line_id=f"line-{number}",
                    exact_reuse_enabled=True,
                    deferred_context_enabled=deferred,
                    reusable_web_tools=(family,),
                ),
            )
            try:
                conversation.send_message("Fetch the page")
                conversation.run()
                return [
                    e
                    for e in conversation.state.active_branch()
                    if isinstance(e, ObservationEvent)
                ]
            finally:
                conversation.close()

        if inflight:
            with ThreadPoolExecutor(max_workers=2) as pool:
                histories = list(pool.map(run, range(2)))
            histories.sort(
                key=lambda items: (
                    "FlowPilot reuse provenance" in items[0].observation.text
                )
            )
        else:
            histories = [run(0), run(1)]
        assert len(executions) == 1
        assert len(histories[0]) == len(histories[1]) == 1
        assert histories[0][0].tool_call_id != histories[1][0].tool_call_id
        assert histories[0][0].observation.text == expected.text
        assert expected.text in histories[1][0].observation.text
        assert "FlowPilot reuse provenance" in histories[1][0].observation.text
        assert len(requests) == 4
        admitted = [r for r in trace.records if r["event_type"] == "request_admitted"]
        assert len(admitted) == (len(requests) if admission else 0)
        serialized = json.dumps(trace.records)
        assert (
            "opaque page body" not in serialized and "integration-key" not in serialized
        )
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()
