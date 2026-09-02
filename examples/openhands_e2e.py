from __future__ import annotations

import asyncio
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from cryptography.fernet import Fernet

ROOT = Path(__file__).resolve().parents[1]
SDK_ROOT = Path("/home/liyachen/openhands/software-agent-sdk")
PYTHON = SDK_ROOT / ".venv/bin/python"
TRACE = Path("/tmp/flowpilot-e2e.jsonl")
CACHE = Path("/tmp/flowpilot-e2e-cache.sqlite")
DCS_WAL = Path("/tmp/flowpilot-e2e-dcs.sqlite")
sys.path.insert(0, str(SDK_ROOT / "openhands-sdk"))


def wait_ready(url: str) -> None:
    for _ in range(100):
        try:
            with urllib.request.urlopen(url, timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"service did not become ready: {url}")


def main() -> None:
    if Path(sys.executable).resolve() != PYTHON.resolve():
        if not PYTHON.is_file():
            raise RuntimeError(f"OpenHands SDK interpreter is missing: {PYTHON}")
        python_path = f"{SDK_ROOT / 'openhands-sdk'}:{ROOT}"
        os.execve(
            str(PYTHON),
            [str(PYTHON), str(Path(__file__).resolve())],
            {**os.environ, "PYTHONPATH": python_path},
        )
    TRACE.unlink(missing_ok=True)
    CACHE.unlink(missing_ok=True)
    DCS_WAL.unlink(missing_ok=True)
    env = {**os.environ, "PYTHONPATH": f"{SDK_ROOT / 'openhands-sdk'}:{ROOT}"}
    mock = subprocess.Popen(
        [
            str(PYTHON),
            "-m",
            "uvicorn",
            "examples.mock_inference:app",
            "--host",
            "127.0.0.1",
            "--port",
            "19101",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    flow_env = {
        **env,
        "FLOWPILOT_UPSTREAMS": "http://127.0.0.1:19101",
        "FLOWPILOT_INGRESS_API_KEY": "test-key",
        "FLOWPILOT_TRACE_PATH": str(TRACE),
        "FLOWPILOT_PORT": "19100",
        "FLOWPILOT_REUSE_ENABLED": "true",
        "FLOWPILOT_REUSE_CACHE_PATH": str(CACHE),
        "FLOWPILOT_DCS_ENABLED": "true",
        "FLOWPILOT_DCS_WAL_PATH": str(DCS_WAL),
        "FLOWPILOT_DCS_ENCRYPTION_KEY": Fernet.generate_key().decode(),
        "FLOWPILOT_WEB_TOOL_REGISTRY_JSON": json.dumps(
            [
                {
                    "protocol_version": "flowpilot-phase3-reuse-v2",
                    "tool_name": "web_search",
                    "canonical_tool_family": "public_web_search",
                    "tool_version": "1",
                    "result_schema_version": "1",
                    "semantic_reuse_enabled": True,
                    "semantic_similarity_threshold": 0.55,
                }
            ]
        ),
    }
    flowpilot = subprocess.Popen(
        [str(PYTHON), "-m", "flowpilot"],
        cwd=ROOT,
        env=flow_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_ready("http://127.0.0.1:19101/v1/models")
        wait_ready("http://127.0.0.1:19100/flowpilot/health")
        result = run_calls()
        result.update(run_agent_tool_conversation())
        result.update(run_exact_reuse_conversations())
        records = [json.loads(line) for line in TRACE.read_text().splitlines()]
        result["trace_events_total"] = len(records)
        result["llm_requests_total"] = sum(
            item["event_type"] == "llm_request" for item in records
        )
        result["dcs_trace_events"] = sum(
            item["event_type"]
            in {
                "context_delegation_grant",
                "context_delta_append",
                "internal_continuation",
                "context_sync_request",
                "context_sync_ack",
                "context_reconcile",
            }
            for item in records
        )
        print(json.dumps(result, sort_keys=True))
    finally:
        flowpilot.terminate()
        mock.terminate()
        flowpilot.wait(timeout=5)
        mock.wait(timeout=5)


def run_calls() -> dict[str, object]:
    from openhands.sdk.llm import LLM, LLMCallContext, Message, TextContent

    direct = LLM(
        model="openai/flowpilot-mock",
        api_key="test",
        base_url="http://127.0.0.1:19101/v1",
    )
    proxy = LLM(
        model="openai/flowpilot-mock",
        api_key="test",
        base_url="http://127.0.0.1:19100/v1",
    )
    message = [Message(role="user", content=[TextContent(text="e2e-secret-marker")])]

    register()
    version = 0

    def context(call: str) -> LLMCallContext:
        nonlocal version
        ctx = LLMCallContext(
            flowpilot_gateway_url="http://127.0.0.1:19100/v1",
            flowpilot_headers={
                "x-flowpilot-api-key": "test-key",
                "x-flowpilot-job-id": "job-e2e",
                "x-flowpilot-line-id": "line-e2e",
                "x-flowpilot-tail-request-id": f"tail-{call}",
                "x-flowpilot-llm-call-id": call,
                "x-flowpilot-tail-version": str(version),
                "x-flowpilot-context-epoch": "1",
                "x-flowpilot-context-sequence": str(version),
                "x-flowpilot-context-cursor": f"cursor-{version}",
                "x-flowpilot-context-digest": "a" * 64,
            },
        )
        version += 1
        return ctx

    chat = proxy.completion(message, call_context=context("chat-sync"))
    responses = proxy.responses(message, call_context=context("responses-sync"))
    assert chat.message.role == "assistant"
    assert responses.message.role == "assistant"

    async def async_calls() -> None:
        chat_context = context("chat-async")
        chat_result = await proxy.acompletion(message, call_context=chat_context)
        responses_context = context("responses-async")
        responses_result = await proxy.aresponses(
            message, call_context=responses_context
        )
        assert chat_result.message.role == "assistant"
        assert responses_result.message.role == "assistant"

    asyncio.run(async_calls())

    direct_ms = latency(lambda: direct.completion(message))
    proxy_ms = latency(
        lambda: proxy.completion(
            message, call_context=context(f"latency-{time.time_ns()}")
        )
    )
    records = [json.loads(line) for line in TRACE.read_text().splitlines()]
    return {
        "direct_median_ms": round(statistics.median(direct_ms), 3),
        "proxy_median_ms": round(statistics.median(proxy_ms), 3),
        "proxy_overhead_ms": round(
            statistics.median(proxy_ms) - statistics.median(direct_ms), 3
        ),
        "trace_events": len(records),
        "llm_requests": sum(r["event_type"] == "llm_request" for r in records),
        "llm_responses": sum(r["event_type"] == "llm_response" for r in records),
        "correlated_calls": sorted(
            r["identity"]["llm_call_id"]
            for r in records
            if r["event_type"] == "llm_request"
        ),
        "secret_leaked": "e2e-secret-marker" in TRACE.read_text(),
    }


def register() -> None:
    for path, payload in (
        ("jobs", {"job_id": "job-e2e"}),
        (
            "lines",
            {
                "job_id": "job-e2e",
                "line_id": "line-e2e",
                "context_epoch": 1,
                "base_context_cursor": "cursor-0",
                "context_digest": "a" * 64,
            },
        ),
    ):
        request = urllib.request.Request(
            f"http://127.0.0.1:19100/flowpilot/v1/{path}",
            data=json.dumps(payload).encode(),
            headers={
                "content-type": "application/json",
                "x-flowpilot-api-key": "test-key",
            },
            method="POST",
        )
        with urllib.request.urlopen(request):
            pass


def run_agent_tool_conversation() -> dict[str, object]:
    from openhands.sdk import Agent, LocalConversation, LocalWorkspace
    from openhands.sdk.event import ObservationEvent
    from openhands.sdk.flowpilot import FlowPilotConfig
    from openhands.sdk.llm import LLM, Message, TextContent

    llm = LLM(
        model="openai/flowpilot-mock",
        api_key="test",
        base_url="http://127.0.0.1:19101/v1",
        stream=False,
    )
    conversation = LocalConversation(
        agent=Agent(
            llm=llm,
            tools=[],
            include_default_tools=["ThinkTool", "FinishTool"],
            tool_concurrency_limit=1,
        ),
        workspace=LocalWorkspace(working_dir=Path("/tmp/flowpilot-agent-e2e")),
        visualizer=None,
        flowpilot=FlowPilotConfig(
            enabled=True,
            gateway_url="http://127.0.0.1:19100",
            api_key="test-key",
            job_id="job-agent-e2e",
            line_id="line-agent-e2e",
        ),
    )
    try:
        conversation.send_message(
            Message(
                role="user",
                content=[TextContent(text="agent-tool-e2e-secret-marker")],
            )
        )
        conversation.run()
        observations = [
            event
            for event in conversation.state.events
            if isinstance(event, ObservationEvent)
        ]
    finally:
        conversation.close()

    records = [json.loads(line) for line in TRACE.read_text().splitlines()]
    tool_records = [
        record
        for record in records
        if record["event_type"].startswith("tool_")
        and record.get("identity", {}).get("job_id") == "job-agent-e2e"
    ]
    tool_terminals = [
        record
        for record in tool_records
        if record["event_type"] in {"tool_finish", "tool_fail", "tool_cancel"}
    ]
    expected_ids = [
        "tool-call-think-1",
        "tool-call-think-2",
        "tool-call-finish",
    ]
    observed_ids = [event.tool_call_id for event in observations]
    terminal_ids = [record["identity"]["tool_call_id"] for record in tool_terminals]
    assert observed_ids == expected_ids
    assert terminal_ids == expected_ids
    assert [record["event_type"] for record in tool_records] == [
        "tool_start",
        "tool_finish",
        "tool_start",
        "tool_finish",
        "tool_start",
        "tool_finish",
    ]
    trace_text = TRACE.read_text()
    return {
        "agent_observation_ids": observed_ids,
        "agent_tool_events": len(tool_records),
        "agent_tool_secret_leaked": "agent-tool-e2e-secret-marker" in trace_text,
    }


def run_exact_reuse_conversations() -> dict[str, object]:
    from collections.abc import Sequence

    from openhands.sdk import Agent, LocalConversation, LocalWorkspace
    from openhands.sdk.event import ObservationEvent
    from openhands.sdk.flowpilot import FlowPilotConfig, context_digest
    from openhands.sdk.llm import LLM, Message, TextContent
    from openhands.sdk.tool import (
        Action,
        Observation,
        Tool,
        ToolAnnotations,
        ToolDefinition,
        ToolExecutor,
        register_tool,
    )
    from pydantic import Field

    class WebSearchAction(Action):
        query: str

    class WebSearchObservation(Observation):
        items: list[dict[str, str]] = Field(default_factory=list)

    class WebSearchExecutor(ToolExecutor[WebSearchAction, WebSearchObservation]):
        executions = 0
        block_next = False
        started = threading.Event()
        release = threading.Event()

        def __call__(
            self,
            action: WebSearchAction,
            conversation=None,  # noqa: ARG002
        ) -> WebSearchObservation:
            type(self).executions += 1
            if type(self).block_next:
                type(self).block_next = False
                type(self).started.set()
                if not type(self).release.wait(timeout=5):
                    raise TimeoutError("in-flight E2E leader was not released")
            return WebSearchObservation(
                content=[TextContent(text="one exact search result")],
                items=[
                    {
                        "title": "FlowPilot",
                        "url": "https://example.test/flowpilot",
                        "query": action.query,
                    }
                ],
            )

    class WebSearchTool(ToolDefinition[WebSearchAction, WebSearchObservation]):
        @classmethod
        def create(cls, conv_state=None, **params) -> Sequence[ToolDefinition]:
            del conv_state, params
            return [
                cls(
                    description="Read-only deterministic Web search.",
                    action_type=WebSearchAction,
                    observation_type=WebSearchObservation,
                    annotations=ToolAnnotations(readOnlyHint=True),
                    executor=WebSearchExecutor(),
                )
            ]

    class LocalReadAction(Action):
        query: str

    class LocalReadObservation(Observation):
        result: str

    class LocalReadExecutor(ToolExecutor[LocalReadAction, LocalReadObservation]):
        executions = 0

        def __call__(
            self,
            action: LocalReadAction,
            conversation=None,  # noqa: ARG002
        ) -> LocalReadObservation:
            type(self).executions += 1
            return LocalReadObservation(
                content=[TextContent(text="local barrier execution complete")],
                result=action.query,
            )

    class LocalReadTool(ToolDefinition[LocalReadAction, LocalReadObservation]):
        @classmethod
        def create(cls, conv_state=None, **params) -> Sequence[ToolDefinition]:
            del conv_state, params
            return [
                cls(
                    description="Read local authoritative state.",
                    action_type=LocalReadAction,
                    observation_type=LocalReadObservation,
                    annotations=ToolAnnotations(readOnlyHint=True),
                    executor=LocalReadExecutor(),
                )
            ]

    register_tool(WebSearchTool.name, WebSearchTool)
    register_tool(LocalReadTool.name, LocalReadTool)
    observation_ids: list[str] = []
    observation_texts: list[str] = []
    dcs_reconciliations: list[dict[str, object]] = []
    jct_ms: dict[int, float] = {}
    control_trace_events: dict[int, int] = {}
    for number in (1, 2, 3, 4, 5, 6, 7):
        enabled_tools = [Tool(name=WebSearchTool.name)]
        if number == 4:
            enabled_tools.append(Tool(name=LocalReadTool.name))
        conversation = LocalConversation(
            agent=Agent(
                llm=LLM(
                    model=(
                        "openai/gpt-5-mini-2025-08-07"
                        if number == 5
                        else "openai/flowpilot-mock"
                    ),
                    api_key="test",
                    base_url="http://127.0.0.1:19101/v1",
                    stream=False,
                    caching_prompt=False,
                ),
                tools=enabled_tools,
                include_default_tools=["FinishTool"],
                tool_concurrency_limit=1,
            ),
            workspace=LocalWorkspace(
                working_dir=Path(f"/tmp/flowpilot-reuse-e2e-{number}")
            ),
            visualizer=None,
            flowpilot=FlowPilotConfig(
                enabled=True,
                gateway_url="http://127.0.0.1:19100",
                api_key="test-key",
                job_id=f"job-reuse-e2e-{number}",
                line_id=f"line-reuse-e2e-{number}",
                exact_reuse_enabled=True,
                semantic_reuse_enabled=number in {6, 7},
                reusable_web_tools=("web_search",),
                deferred_context_enabled=number in {3, 4, 5},
            ),
        )
        try:
            conversation.send_message(
                Message(
                    role="user",
                    content=[
                        TextContent(
                text=(
                    "dcs-agent-e2e-secret-marker"
                    if number == 3
                    else (
                        "dcs-local-barrier-marker"
                        if number == 4
                        else (
                            "dcs-responses-multi-tool-marker"
                            if number == 5
                            else (
                                "phase3-semantic-source-marker"
                                if number == 6
                                else "phase3-semantic-follower-marker"
                                if number == 7
                                else "find flowpilot"
                            )
                        )
                    )
                )
                        )
                    ],
                )
            )
            started = time.perf_counter()
            conversation.run()
            jct_ms[number] = round((time.perf_counter() - started) * 1000, 3)
            web_observations = [
                event
                for event in conversation.state.events
                if isinstance(event, ObservationEvent)
                and event.tool_name == "web_search"
            ]
            expected_count = 2 if number == 5 else 1
            assert len(web_observations) == expected_count
            observation_ids.extend(event.tool_call_id for event in web_observations)
            observation_texts.extend(
                event.observation.text for event in web_observations
            )
            if number in {3, 4, 5}:
                runtime = conversation._flowpilot_runtime
                assert runtime is not None
                events = list(conversation.state.active_branch())
                dcs_reconciliations.append(
                    runtime.reconcile_deferred_context(
                        context_cursor=events[-1].id,
                        context_digest=context_digest(events),
                    )
                )
            records = [json.loads(line) for line in TRACE.read_text().splitlines()]
            control_trace_events[number] = sum(
                record.get("identity", {}).get("line_id")
                == f"line-reuse-e2e-{number}"
                and record["event_type"]
                in {
                    "tool_reuse_resolve",
                    "context_reconcile",
                    "context_delegation_grant",
                    "tool_reuse_deferred_resolve",
                    "context_delta_append",
                    "internal_continuation",
                    "context_sync_request",
                    "context_sync_ack",
                }
                for record in records
            )
        finally:
            conversation.close()

    def inflight_conversation(number: int, *, deferred: bool) -> LocalConversation:
        return LocalConversation(
            agent=Agent(
                llm=LLM(
                    model="openai/flowpilot-mock",
                    api_key="test",
                    base_url="http://127.0.0.1:19101/v1",
                    stream=False,
                    caching_prompt=False,
                ),
                tools=[Tool(name=WebSearchTool.name)],
                include_default_tools=["FinishTool"],
                tool_concurrency_limit=1,
            ),
            workspace=LocalWorkspace(
                working_dir=Path(f"/tmp/flowpilot-inflight-e2e-{number}")
            ),
            visualizer=None,
            flowpilot=FlowPilotConfig(
                enabled=True,
                gateway_url="http://127.0.0.1:19100",
                api_key="test-key",
                job_id=f"job-inflight-e2e-{number}",
                line_id=f"line-inflight-e2e-{number}",
                exact_reuse_enabled=True,
                reusable_web_tools=("web_search",),
                deferred_context_enabled=deferred,
            ),
        )

    leader = inflight_conversation(1, deferred=False)
    follower = inflight_conversation(2, deferred=True)
    WebSearchExecutor.block_next = True
    WebSearchExecutor.started.clear()
    WebSearchExecutor.release.clear()
    errors: list[BaseException] = []

    def run_conversation(conversation: LocalConversation) -> None:
        try:
            conversation.run()
        except BaseException as exc:
            errors.append(exc)

    try:
        leader.send_message("find flowpilot in flight")
        follower.send_message("find flowpilot in flight")
        leader_thread = threading.Thread(target=run_conversation, args=(leader,))
        follower_thread = threading.Thread(target=run_conversation, args=(follower,))
        leader_thread.start()
        if not WebSearchExecutor.started.wait(timeout=5):
            raise TimeoutError("in-flight E2E leader did not start")
        follower_thread.start()
        time.sleep(0.1)
        WebSearchExecutor.release.set()
        leader_thread.join(timeout=10)
        follower_thread.join(timeout=10)
        if leader_thread.is_alive() or follower_thread.is_alive():
            raise TimeoutError("in-flight E2E conversation did not finish")
        if errors:
            raise errors[0]
        follower_observations = [
            event
            for event in follower.state.active_branch()
            if isinstance(event, ObservationEvent) and event.tool_name == "web_search"
        ]
        assert [event.tool_call_id for event in follower_observations] == [
            "tool-call-web-search"
        ]
    finally:
        WebSearchExecutor.release.set()
        leader.close()
        follower.close()

    records = [json.loads(line) for line in TRACE.read_text().splitlines()]
    reuse_records = [
        record
        for record in records
        if record["event_type"] == "tool_reuse_resolve"
        and str(record.get("identity", {}).get("job_id", "")).startswith(
            "job-reuse-e2e-"
        )
    ]
    local_starts = [
        record
        for record in records
        if record["event_type"] == "tool_start"
        and str(record.get("identity", {}).get("job_id", "")).startswith(
            "job-reuse-e2e-"
        )
        and record.get("fields", {}).get("tool_name") == "web_search"
    ]
    decisions = [record["fields"]["decision"] for record in reuse_records]
    dcs_records = [
        record
        for record in records
        if str(record.get("identity", {}).get("job_id", "")).startswith(
            "job-reuse-e2e-"
        )
        and record["event_type"]
        in {
            "context_delegation_grant",
            "tool_reuse_deferred_resolve",
            "context_delta_append",
            "internal_continuation",
            "context_sync_request",
            "context_sync_ack",
            "context_reconcile",
        }
    ]
    assert WebSearchExecutor.executions == 3
    assert LocalReadExecutor.executions == 1
    assert len(local_starts) == 2
    assert decisions == [
        "sync_and_execute_as_leader",
        "sync_with_reused_result",
        "sync_and_execute_as_leader",
        "sync_with_reused_result",
    ]
    assert observation_ids[:4] == ["tool-call-web-search"] * 4
    assert observation_ids[4:6] == [
        "tool-call-responses-1",
        "tool-call-responses-2",
    ]
    assert observation_ids[6:] == ["tool-call-web-search"] * 2
    assert "reuse provenance" not in observation_texts[0]
    assert "historical" in observation_texts[1]
    assert "historical" in observation_texts[2]
    assert "semantic" in observation_texts[7]
    semantic_record = next(
        record
        for record in reuse_records
        if record["fields"].get("match_kind") == "semantic"
    )
    assert semantic_record["fields"]["similarity_score"] >= 0.55
    assert semantic_record["fields"]["semantic_match_id"]
    assert [item["status"] for item in dcs_reconciliations] == [
        "in_sync",
        "agent_ahead_requires_new_delegation",
        "in_sync",
    ]
    assert [record["event_type"] for record in dcs_records] == [
        "context_reconcile",
        "context_delegation_grant",
        "tool_reuse_deferred_resolve",
        "context_delta_append",
        "internal_continuation",
        "context_sync_request",
        "context_sync_ack",
        "context_reconcile",
        "context_reconcile",
        "context_delegation_grant",
        "tool_reuse_deferred_resolve",
        "context_delta_append",
        "internal_continuation",
        "context_sync_request",
        "context_sync_ack",
        "context_reconcile",
        "context_reconcile",
        "context_delegation_grant",
        "tool_reuse_deferred_resolve",
        "tool_reuse_deferred_resolve",
        "context_delta_append",
        "internal_continuation",
        "context_sync_request",
        "context_sync_ack",
        "context_reconcile",
    ]
    local_line_records = [
        record
        for record in records
        if record.get("identity", {}).get("line_id") == "line-reuse-e2e-4"
    ]
    local_event_types = [record["event_type"] for record in local_line_records]
    ack_index = local_event_types.index("context_sync_ack")
    tool_start_index = local_event_types.index("tool_start")
    assert ack_index < tool_start_index
    assert "llm_request" in local_event_types[tool_start_index + 1 :]
    assert "context_sync_fail" not in local_event_types
    local_sync = next(
        record
        for record in local_line_records
        if record["event_type"] == "context_sync_request"
    )
    assert local_sync["fields"]["barrier_reason"] == "local_tool"
    assert local_sync["fields"]["pending_local_tool_call_ids"] == [
        "tool-call-local-read"
    ]
    trace_text = TRACE.read_text()
    wal_bytes = DCS_WAL.read_bytes()
    assert "dcs-agent-e2e-secret-marker" not in trace_text
    assert b"dcs-agent-e2e-secret-marker" not in wal_bytes
    assert b"one exact search result" not in wal_bytes
    inflight_records = [
        record
        for record in records
        if str(record.get("identity", {}).get("job_id", "")).startswith(
            "job-inflight-e2e-"
        )
    ]
    assert any(
        record["event_type"] == "tool_reuse_deferred_resolve"
        and record["fields"]["decision"] == "defer_wait_for_inflight"
        for record in inflight_records
    )
    inflight_append = next(
        record
        for record in inflight_records
        if record["event_type"] == "context_delta_append"
    )
    assert inflight_append["fields"]["reuse_kinds"] == ["exact_inflight"]
    return {
        "reuse_local_executions": WebSearchExecutor.executions,
        "reuse_decisions": decisions,
        "reuse_observation_ids": observation_ids,
        "semantic_historical_hit": True,
        "semantic_similarity_score": semantic_record["fields"][
            "similarity_score"
        ],
        "immediate_return_jct_ms": jct_ms[2],
        "dcs_chat_jct_ms": jct_ms[3],
        "dcs_responses_multi_tool_jct_ms": jct_ms[5],
        "control_trace_events_immediate": control_trace_events[2],
        "control_trace_events_dcs_chat": control_trace_events[3],
        "control_trace_events_dcs_responses": control_trace_events[5],
        "dcs_sync_batch_message_counts": [
            record["fields"]["message_count"]
            for record in dcs_records
            if record["event_type"] == "context_sync_request"
        ],
        "dcs_inflight_follower": True,
        "dcs_event_types": [record["event_type"] for record in dcs_records],
        "dcs_reconcile_status": dcs_reconciliations[0]["status"],
        "dcs_local_reconcile_status": dcs_reconciliations[-1]["status"],
        "dcs_local_barrier_order": [
            "context_sync_ack",
            "tool_start",
            "llm_request",
        ],
        "dcs_local_executions": LocalReadExecutor.executions,
        "dcs_trace_secret_leaked": "dcs-agent-e2e-secret-marker" in trace_text,
        "dcs_wal_plaintext_leaked": (
            b"dcs-agent-e2e-secret-marker" in wal_bytes
            or b"one exact search result" in wal_bytes
        ),
    }


def latency(call) -> list[float]:
    values = []
    for _ in range(8):
        started = time.perf_counter()
        call()
        values.append((time.perf_counter() - started) * 1000)
    return values


if __name__ == "__main__":
    sys.exit(main())
