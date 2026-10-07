"""Run with the OpenHands Python environment and this repository on PYTHONPATH."""

import asyncio
import json
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx
import mcp.types
import pytest
import uvicorn
from fastapi.responses import PlainTextResponse
from openhands.sdk import Agent, LocalConversation, LocalWorkspace
from openhands.sdk.event import ObservationEvent
from openhands.sdk.flowpilot import FlowPilotConfig, FlowPilotRuntime
from openhands.sdk.llm import LLM
from openhands.sdk.mcp.definition import MCPToolAction, MCPToolObservation
from openhands.sdk.mcp.tool import MCPToolDefinition
from openhands.sdk.tool import ToolAnnotations, ToolExecutor, register_tool
from openhands.sdk.tool.spec import Tool
from openhands.tools.terminal import TerminalExecutor, TerminalObservation
from openhands.tools.terminal.metadata import CmdOutputMetadata
from pydantic import SecretStr

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.context.manager import DeferredContextManager
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import ToolRegistryEntry
from flowpilot.reuse.adapters.tavily import TAVILY_SCHEMA_DIGESTS, TAVILY_SCHEMAS
from flowpilot.reuse.semantic import TestHashingEmbedder
from flowpilot.scheduling.admission import AdmissionConfig, BestEffortAdmissionConfig
from flowpilot.scheduling.capacity import AdaptiveAdmissionConfig


@pytest.mark.parametrize(
    "family",
    ["curl", "wget", "tavily-search", "tavily-extract", "tavily-crawl", "tavily-map"],
)
@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("inflight", [True, False])
@pytest.mark.parametrize("gateway", [True, False])
@pytest.mark.parametrize("admission", [False, True])
def test_agent_gateway_local_commit_then_history(
    tmp_path: Path,
    monkeypatch,
    gateway,
    inflight,
    family,
    deferred,
    admission,
    real_terminal=False,
    stream=False,
    tool_rounds=1,
    predictor_probe=None,
    extra_body=None,
    semantic=False,
    semantic_match=False,
    tool_text="opaque page body",
    resume_after_dcs=False,
    async_run=False,
    expected_dcs_barrier=None,
    admission_policy: Literal["prefill_slack", "slo_unexpired_first", "weighted"] = (
        "prefill_slack"
    ),
    capacity_controls=False,
):
    if not gateway and deferred:
        pytest.skip("Runtime DCS is covered by the dedicated SDK tests")
    tool_name = "terminal" if family in {"curl", "wget"} else family
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    base = f"http://127.0.0.1:{sock.getsockname()[1]}"
    arguments = (
        {
            "command": f"{family} {'-sS' if family == 'curl' else '-qO-'} https://example.com"
        }
        if tool_name == "terminal"
        else {"query": "FlowPilot research"}
        if family == "tavily-search"
        else {"urls": ["https://example.com/"]}
        if family == "tavily-extract"
        else {"url": "https://example.com/", "max_depth": 2}
    )
    if real_terminal:
        arguments["command"] = arguments["command"].replace(
            "https://example.com", base + "/fixture/page"
        )
    expected = (
        TerminalObservation.from_text(
            tool_text,
            exit_code=0,
            command=arguments["command"],
            metadata=CmdOutputMetadata(exit_code=0, working_dir="/leader"),
        )
        if tool_name == "terminal"
        else MCPToolObservation.from_call_tool_result(
            family,
            mcp.types.CallToolResult(
                content=[
                    mcp.types.TextContent(
                        type="text", text=tool_text + "\nTitle: just content"
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
    original_execute = TerminalExecutor.__call__
    page_requests = []

    def wait(runtime, *args, **kwargs):
        waiting.set()
        return original_wait(runtime, *args, **kwargs)

    monkeypatch.setattr(FlowPilotRuntime, "_wait_for_reuse", wait)

    def execute(self, action, conversation=None):
        executions.append(action)
        if inflight:
            assert waiting.wait(10), "follower never attached"
        if real_terminal:
            return original_execute(self, action, conversation)
        return expected.model_copy(deep=True)

    if tool_name == "terminal":
        monkeypatch.setattr(TerminalExecutor, "__call__", execute)
    else:

        class FixtureExecutor(ToolExecutor):
            __call__ = execute

        register_tool(
            family,
            MCPToolDefinition(
                mcp_tool=mcp.types.Tool(
                    name=family,
                    inputSchema=TAVILY_SCHEMAS[family],
                ),
                description="Pinned Tavily fixture with OpenHands schema validation",
                action_type=MCPToolAction,
                observation_type=MCPToolObservation,
                annotations=ToolAnnotations(readOnlyHint=True),
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
        if request.url.path == "/metrics":
            assert capacity_controls
            return httpx.Response(
                200,
                text="\n".join(
                    f'vllm:{name}{{engine="0"}} 0'
                    for name in (
                        "num_requests_running",
                        "num_requests_waiting",
                        "num_preemptions_total",
                        "e2e_request_latency_seconds_count",
                    )
                ),
            )
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": 1})
        if predictor_probe is not None:
            assert not any(name.startswith("x-flowpilot-") for name in request.headers)
            await asyncio.sleep(0.02)
        body = json.loads(request.content)
        requests.append(body)
        if extra_body:
            assert "extra_body" not in body
            assert all(body[key] == value for key, value in extra_body.items())
        current_arguments = (
            {"query": "research FlowPilot"}
            if semantic_match and "semantic follower" in json.dumps(body["messages"])
            else arguments
        )
        done = (
            sum(message.get("role") == "tool" for message in body["messages"])
            >= tool_rounds
        )
        message = (
            {"role": "assistant", "content": "done"}
            if done
            else {
                "role": "assistant",
                "content": None,
                "annotations": None,
                "audio": None,
                "function_call": None,
                "reasoning": None,
                "refusal": None,
                "tool_calls": [
                    {
                        "id": "call-" + str(len(requests)),
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "arguments": json.dumps(current_arguments),
                        },
                    }
                ],
            }
        )
        payload = {
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
        }
        if stream:
            assert body["stream"] is True
            payload["object"] = "chat.completion.chunk"
            payload["choices"][0]["delta"] = payload["choices"][0].pop("message")
            for index, tool_call in enumerate(message.get("tool_calls", [])):
                tool_call["index"] = index

            class SSE(httpx.AsyncByteStream):
                async def __aiter__(self):
                    yield b"data: " + json.dumps(payload).encode() + b"\n\n"
                    yield b"data: [DONE]\n\n"

            return httpx.Response(
                200, headers={"content-type": "text/event-stream"}, stream=SSE()
            )
        return httpx.Response(200, json=payload)

    app = create_app(
        Settings(
            instances=(InferenceInstance("mock", "http://mock"),),
            trace_path=tmp_path / "trace.jsonl",
            ingress_api_key="integration-key",
            admission=AdmissionConfig(
                enabled=admission,
                limit=32 if capacity_controls else 1,
                policy=admission_policy,
                best_effort=BestEffortAdmissionConfig(enabled=capacity_controls),
                adaptive=AdaptiveAdmissionConfig(enabled=capacity_controls),
            ),
            reuse_enabled=True,
            dcs_enabled=deferred,
            dcs_encryption_key="0ifg6OOv5jfhrCjCMs6d-FpXabEPiIG7Ln86yc49i1o=",
            dcs_wal_path=tmp_path / "dcs.sqlite",
            reuse_cache_path=tmp_path / "reuse.sqlite",
            web_tool_registry=(
                ToolRegistryEntry(
                    protocol_version="flowpilot-phase3-reuse-v3"
                    if semantic
                    else "flowpilot-phase1-reuse-v3",
                    semantic_reuse_enabled=semantic,
                    semantic_mode="active" if semantic else "shadow",
                    semantic_similarity_threshold=0.9,
                    tool_name=tool_name,
                    canonical_tool_family=family,
                    tool_version="1" if tool_name == "terminal" else "0.2.1",
                    adapter_id={
                        "terminal": "terminal_url_fetch_v1",
                        "tavily-search": "tavily_search_mcp_v1",
                        "tavily-extract": "tavily_extract_mcp_v1",
                        "tavily-crawl": "tavily_crawl_mcp_v1",
                        "tavily-map": "tavily_map_mcp_v1",
                    }[tool_name],
                    input_schema_digest=TAVILY_SCHEMA_DIGESTS.get(family),
                    result_schema_version="observation-v1",
                    command_line_reuse="url_exact"
                    if tool_name == "terminal"
                    else "disabled",
                ),
            ),
        ),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(upstream)),
        trace_sink=trace,
        tool_duration_adapter=predictor_probe,
        semantic_embedder=TestHashingEmbedder() if semantic else None,
    )

    @app.get("/fixture/page")
    async def page():
        page_requests.append(True)
        return PlainTextResponse("opaque page body")

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
                    num_retries=0,
                    stream=stream,
                    litellm_extra_body=extra_body or {},
                    extra_headers={
                        "x-flowpilot-predictor-context": predictor_probe.context
                    }
                    if predictor_probe is not None
                    else {},
                ),
                tools=[
                    Tool(
                        name=tool_name,
                        params={"terminal_type": "subprocess"}
                        if tool_name == "terminal"
                        else {},
                    )
                ],
                include_default_tools=[],
                tool_concurrency_limit=1,
            )
            conversation = LocalConversation(
                agent=agent,
                workspace=LocalWorkspace(working_dir=tmp_path),
                visualizer=None,
                token_callbacks=[lambda _delta: None] if stream else None,
                flowpilot=FlowPilotConfig(
                    enabled=True,
                    gateway_url=base,
                    api_key="integration-key",
                    job_id=f"integration-{number}",
                    line_id=f"line-{number}",
                    exact_reuse_enabled=True,
                    semantic_reuse_enabled=semantic,
                    deferred_context_enabled=deferred,
                    reusable_web_tools=(tool_name,),
                ),
            )
            try:
                conversation.send_message(
                    "Fetch the page: semantic follower"
                    if semantic_match and number == 1
                    else "Fetch the page"
                )
                if async_run:
                    asyncio.run(conversation.arun())
                else:
                    conversation.run()
                if resume_after_dcs and number == 1:
                    conversation.send_message("Confirm the result from the history")
                    if async_run:
                        asyncio.run(conversation.arun())
                    else:
                        conversation.run()
                    assert conversation.state.execution_status.value == "finished"
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
        assert len(histories[0]) == len(histories[1]) == tool_rounds
        assert histories[0][0].tool_call_id != histories[1][0].tool_call_id
        assert expected.text in histories[0][0].observation.text
        # DCS renders separate MCP text blocks with newline separators. Check
        # each complete block, including the generated header and actual body.
        for item in expected.content:
            assert item.text in histories[1][0].observation.text
        assert "FlowPilot reuse provenance" in histories[1][0].observation.text
        if tool_name == "terminal":
            assert histories[1][0].observation.metadata.working_dir is None
            assert histories[1][0].observation.full_output_save_dir is None
        if real_terminal:
            assert len(page_requests) == 1
        assert len(requests) == 2 * (tool_rounds + 1) + int(resume_after_dcs)
        if len(tool_text) > 50_000:
            tool_messages = [
                message
                for message in requests[-1]["messages"]
                if message.get("role") == "tool"
            ]
            assert len(tool_messages) == tool_rounds
            assert tool_text in tool_messages[0]["content"][0]["text"]
            assert any(r["event_type"] == "context_sync_ack" for r in trace.records)
        if tool_rounds > 1:
            resolution_response = httpx.get(
                base + "/flowpilot/v1/tool-resolutions",
                headers={"x-flowpilot-api-key": "integration-key"},
            )
            resolution_response.raise_for_status()
            records = resolution_response.json()["records"]
            assert len(records) == 2 * tool_rounds
            assert all(item["status"] == "ready" for item in records)
            hits = [item for item in records if item["resolution"] == "historical_hit"]
            assert len(hits) == 2 * tool_rounds - 1
        admitted = [r for r in trace.records if r["event_type"] == "request_admitted"]
        assert len(admitted) == (len(requests) if admission else 0)
        assert all(row["fields"]["policy"] == admission_policy for row in admitted)
        if capacity_controls:
            assert all(row["fields"]["effective_limit"] == 24 for row in admitted)
            assert any(
                row["event_type"] == "admission_capacity_observation"
                for row in trace.records
            )
        serialized = json.dumps(trace.records)
        assert (
            "opaque page body" not in serialized and "integration-key" not in serialized
        )
        if expected_dcs_barrier is not None:
            assert any(
                record["event_type"] == "context_sync_request"
                and record["fields"]["barrier_reason"] == expected_dcs_barrier
                for record in trace.records
            )
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("capacity_controls", [False, True])
def test_slo_unexpired_first_with_real_sdk_and_local_tool(
    tmp_path, monkeypatch, deferred, capacity_controls
):
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="curl",
        deferred=deferred,
        admission=True,
        real_terminal=True,
        admission_policy="slo_unexpired_first",
        capacity_controls=capacity_controls,
    )


def test_deferred_gateway_records_inner_tool_hits(tmp_path, monkeypatch):
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="curl",
        deferred=True,
        admission=True,
        tool_rounds=3,
    )


def test_deferred_gateway_preserves_prediction_context_across_rounds(
    tmp_path, monkeypatch
):
    class PredictionProbe:
        context = '{"schema_version":1,"snapshot_age_ms":123}'

        def __init__(self):
            self.records = []

        def bind(self, app):
            pass

        async def close(self):
            pass

        def on_resolution(self, record):
            pass

        def on_response(
            self, identity, version, api_kind, request, response, context, **kwargs
        ):
            self.records.append(
                (identity, context, time.monotonic() * 1000 - kwargs["elapsed_ms"])
            )

    probe = PredictionProbe()
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="tavily-search",
        deferred=True,
        admission=True,
        tool_rounds=3,
        predictor_probe=probe,
    )
    assert len(probe.records) == 8
    anchors = {}
    delegated = []
    for identity, context, anchor in probe.records:
        assert context == probe.context
        if identity.origin == "scheduler_delegated":
            delegated.append(identity.llm_call_id)
            assert anchor == pytest.approx(anchors[identity.job_id], abs=2)
        else:
            anchors[identity.job_id] = anchor
    assert len(delegated) >= 3
    assert len({identity.llm_call_id for identity, _, _ in probe.records}) == 8


@pytest.mark.parametrize("semantic_match", [False, True])
def test_deferred_gateway_preserves_sampling_and_keeps_semantic_delivery_immediate(
    tmp_path, monkeypatch, semantic_match
):
    class Probe:
        context = '{"schema_version":1}'

        def __init__(self):
            self.origins = []

        def bind(self, app):
            pass

        async def close(self):
            pass

        def on_resolution(self, record):
            pass

        def on_response(self, identity, *args, **kwargs):
            self.origins.append(identity.origin)

    probe = Probe()
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="tavily-search",
        deferred=True,
        admission=True,
        predictor_probe=probe,
        semantic=True,
        semantic_match=semantic_match,
        extra_body={
            "chat_template_kwargs": {"enable_thinking": False},
            "top_k": 20,
            "min_p": 0,
            "presence_penalty": 1.5,
            "repetition_penalty": 1,
        },
    )
    assert probe.origins.count("scheduler_delegated") == (0 if semantic_match else 1)


def test_deferred_gateway_preserves_full_long_tool_result(tmp_path, monkeypatch):
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="tavily-search",
        deferred=True,
        admission=True,
        tool_text="HEAD\n" + "完整工具结果\n" * 10_000 + "TAIL",
    )


def test_agent_resumes_after_multiple_gateway_continuations(tmp_path, monkeypatch):
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="tavily-search",
        deferred=True,
        admission=True,
        tool_rounds=3,
        resume_after_dcs=True,
    )


@pytest.mark.parametrize(
    "phase", ["receipt", "later_receipt", "append", "prepare", "authorize"]
)
@pytest.mark.parametrize("async_run", [False, True])
def test_agent_resumes_after_gateway_lease_expiry(
    tmp_path, monkeypatch, phase, async_run
):
    method = {
        "receipt": "_issue_resolution",
        "later_receipt": "_issue_resolution",
        "append": "_append",
        "prepare": "_prepare_continuation",
        "authorize": "_authorize_llm_request",
    }[phase]
    original = getattr(DeferredContextManager, method)
    expired = []

    def expire_once(manager, *args, **kwargs):
        with manager._connect() as connection:
            row = connection.execute(
                "SELECT * FROM dcs_lines WHERE state='open' AND lease_id IS NOT NULL"
            ).fetchone()
            eligible = row is not None and (
                phase != "later_receipt" or row["pending_count"] > 0
            )
            if phase == "authorize":
                eligible = eligible and args[0].origin == "scheduler_delegated"
            if eligible and not expired:
                connection.execute(
                    "UPDATE dcs_lines SET lease_expires_at=? "
                    "WHERE job_id=? AND line_id=?",
                    (
                        (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                        row["job_id"],
                        row["line_id"],
                    ),
                )
                expired.append(True)
        return original(manager, *args, **kwargs)

    monkeypatch.setattr(DeferredContextManager, method, expire_once)
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="tavily-search",
        deferred=True,
        admission=True,
        tool_rounds=3,
        resume_after_dcs=True,
        async_run=async_run,
    )
    assert expired == [True]


def test_gateway_lease_expiry_during_slow_inference(tmp_path, monkeypatch):
    original = httpx.MockTransport.handle_async_request
    delays = []

    async def slow_inference(transport, request):
        if request.url.path == "/v1/chat/completions" and not delays:
            with sqlite3.connect(tmp_path / "dcs.sqlite") as connection:
                pending = connection.execute(
                    "SELECT 1 FROM dcs_lines WHERE state='open' AND pending_count > 0"
                ).fetchone()
            if pending is not None:
                start = time.monotonic()
                await asyncio.sleep(31)
                delays.append(time.monotonic() - start)
        return await original(transport, request)

    monkeypatch.setattr(httpx.MockTransport, "handle_async_request", slow_inference)
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=False,
        family="tavily-search",
        deferred=True,
        admission=True,
        tool_rounds=3,
        resume_after_dcs=True,
        expected_dcs_barrier="lease_expired",
    )
    assert len(delays) == 1 and delays[0] >= 31


@pytest.mark.parametrize("family", ["curl", "wget"])
@pytest.mark.parametrize("inflight", [False, True])
def test_real_terminal_http_reuse(tmp_path, monkeypatch, family, inflight):
    """Real Agent, TerminalExecutor, curl/wget and HTTP; inference is a fixture."""
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=inflight,
        family=family,
        deferred=False,
        admission=False,
        real_terminal=True,
    )


@pytest.mark.parametrize("inflight", [False, True])
@pytest.mark.parametrize("admission", [False, True])
def test_streaming_agent_reuses_at_tool_boundary(
    tmp_path, monkeypatch, inflight, admission
):
    test_agent_gateway_local_commit_then_history(
        tmp_path,
        monkeypatch,
        gateway=True,
        inflight=inflight,
        family="curl",
        deferred=False,
        admission=admission,
        real_terminal=True,
        stream=True,
    )
