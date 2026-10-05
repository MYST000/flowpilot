"""Run in the SDK environment with a benchmark adapter's src on PYTHONPATH.

Exercise both the installed adapter and the predictor's delivered snapshot.
"""

import asyncio
import json

import httpx
import pytest
from benchmark_adapters.config import Config
from benchmark_adapters.predictor_integration import (
    PredictorTraceRecorder,
    benchmark_flowpilot_config,
)
from openhands.sdk import Agent, LocalConversation
from openhands.sdk.flowpilot import FlowPilotConfig, FlowPilotRuntime
from openhands.sdk.llm import LLM
from pydantic import SecretStr

from flowpilot.frontier.store import LineTailFrontier
from flowpilot.protocol import JobRegistration, LineRegistration


def test_two_tasks_from_one_run_register_separate_root_jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWPILOT_PREDICTOR_GATEWAY", "http://fixture")
    monkeypatch.setenv("FLOWPILOT_INGRESS_API_KEY", "fixture-key")
    monkeypatch.delenv("FLOWPILOT_EXPERIMENT_PROFILE", raising=False)
    frontier = LineTailFrontier()

    def local_control(runtime, path, payload):
        if path == "/flowpilot/v1/jobs":
            return asyncio.run(
                frontier.register_job(JobRegistration.model_validate(payload))
            )
        if path == "/flowpilot/v1/lines":
            return asyncio.run(
                frontier.register_line(LineRegistration.model_validate(payload))
            )
        raise AssertionError(f"unexpected control route: {path}")

    def local_tail(runtime):
        state = asyncio.run(frontier.snapshot(runtime.config.job_id))
        return next(
            (r for r in state["lines"] if r["line_id"] == runtime.config.line_id), None
        )

    monkeypatch.setattr(FlowPilotRuntime, "_post_control", local_control)
    monkeypatch.setattr(FlowPilotRuntime, "_find_authoritative_tail", local_tail)
    agent = Agent(
        llm=LLM(
            model="openai/fixture",
            api_key=SecretStr("fixture"),
            max_input_tokens=32768,
            max_output_tokens=128,
        ),
        tools=[],
        tool_concurrency_limit=1,
    )
    conversations = []
    try:
        for index in range(2):
            adapter = benchmark_flowpilot_config(
                Config(),
                {
                    "run_id": "one-run",
                    "attempt_id": "attempt-1",
                    "task_id": f"task-{index}",
                },
            )
            workspace = tmp_path / f"task-{index}"
            workspace.mkdir()
            conversation = LocalConversation(
                agent=agent,
                workspace=str(workspace),
                flowpilot=adapter,
                visualizer=None,
                persistence_dir=str(tmp_path / "state"),
            )
            conversations.append(conversation)
            conversation_id = str(conversation.state.id)
            assert conversation._flowpilot.job_id == f"job-{conversation_id}"
            assert conversation._flowpilot.line_id == f"line-{conversation_id}"
            assert conversation._flowpilot.root_conversation_id == conversation_id
        assert len({c._flowpilot.job_id for c in conversations}) == 2
    finally:
        for conversation in conversations:
            conversation._flowpilot_runtime._registered = False
            conversation.close()


@pytest.mark.parametrize("deferred", [False, True])
def test_experiment_mapping_and_feedback_use_the_actual_invocation(
    tmp_path, monkeypatch, deferred
):
    received = []

    def feedback(request):
        received.append(json.loads(request.content))
        return httpx.Response(200, json={"status": "recorded"})

    client_class = httpx.Client
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **kwargs: client_class(
            transport=httpx.MockTransport(feedback), **kwargs
        ),
    )
    recorder = PredictorTraceRecorder(
        tmp_path,
        {
            "run_id": "run-1",
            "task_id": "task-1",
            "attempt_id": "attempt-1",
            "conversation_id": "conversation-1",
            "dataset_revision": "fixture-v1",
        },
        flowpilot_config=FlowPilotConfig(
            enabled=True, gateway_url="http://fixture", api_key="private-key"
        ),
        benchmark="fixture",
    )
    outer = {
        "job_id": "job-conversation-1",
        "line_id": "line-conversation-1",
        "conversation_id": "conversation-1",
        "request_id": "outer-request",
        "tail_request_id": "outer-tail",
        "llm_call_id": "outer-call",
        "context_epoch": 1,
    }
    payload = {
        "extra_headers": {
            "x-flowpilot-" + key.replace("_", "-"): str(value)
            for key, value in outer.items()
        }
    }
    payload["extra_headers"]["x-flowpilot-request-attempt"] = "1"
    final = dict(
        outer,
        request_id="inner-request",
        tail_request_id="inner-tail",
        llm_call_id="inner-call",
        attempt=1,
    )
    try:
        recorder.request("trace-request", payload)
        recorder.response_decision(
            "trace-request",
            {"flowpilot": {"final_identity": final}} if deferred else {},
        )
        recorder.emit(
            "tool_end",
            request_id="trace-request",
            tool_call_id="tool-1",
            tool_name="search",
            round_trip_ms=123.0,
        )
    finally:
        recorder.close()
    assert len(received) == 1
    expected = final if deferred else {**outer, "attempt": 1}
    assert {key: received[0][key] for key in expected} == expected
    assert received[0]["round_trip_ms"] == 123.0
    rows = [
        json.loads(row) for row in (tmp_path / "events.jsonl").read_text().splitlines()
    ]
    mappings = [row for row in rows if row["event"].startswith("flowpilot_")]
    assert len(mappings) == (2 if deferred else 1)
    for row in mappings:
        assert (row["run_id"], row["task_id"], row["attempt_id"]) == (
            "run-1",
            "task-1",
            "attempt-1",
        )
        assert row["flowpilot_identity"]["job_id"] == "job-conversation-1"
        assert row["flowpilot_identity"]["conversation_id"] == "conversation-1"
    assert "private-key" not in json.dumps(rows)
