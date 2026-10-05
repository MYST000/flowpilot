"""CPU-only deadline registration with the real SDK and frontier stores."""

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from benchmark_adapters.config import Config
from benchmark_adapters.contracts import Task
from benchmark_adapters.workflow_slo import register_workflow_slo
from openhands.sdk import Agent, LocalConversation
from openhands.sdk.flowpilot import FlowPilotConfig, FlowPilotRuntime
from openhands.sdk.llm import LLM
from pydantic import SecretStr

from flowpilot.frontier.store import LineTailFrontier
from flowpilot.protocol import JobRegistration, LineRegistration


@pytest.fixture
def experiment(tmp_path, monkeypatch):
    config = Config()
    task = Task(config.dataset.id, config.dataset.revision, "test", "42", "fixture")
    baseline = tmp_path / "baselines.json"
    baseline.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "unit": "seconds",
                "tasks": [
                    {
                        "dataset_id": task.dataset_id,
                        "dataset_revision": task.revision,
                        "task_id": task.task_id,
                        "baseline_seconds": 20.0,
                        "instruction_sha256": hashlib.sha256(
                            task.instruction.encode()
                        ).hexdigest(),
                        "result_path": "fixture/result.json",
                        "result_sha256": "fixture-digest",
                    }
                ],
            }
        )
    )
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "workload": {
                    "baseline_latency_path": "baselines.json",
                    "slo_multiplier": 1.5,
                    "baseline_latency_sha256": hashlib.sha256(
                        baseline.read_bytes()
                    ).hexdigest(),
                }
            }
        )
    )
    monkeypatch.setenv("FLOWPILOT_EXPERIMENT_PROFILE", str(profile))
    adapter = FlowPilotConfig(
        enabled=True,
        gateway_url="http://fixture",
        api_key="fixture-key",
        deployment_id="fixture",
        namespace_id="fixture",
    )
    return config, task, adapter, baseline


def test_slo_survives_sdk_registration_and_repeat_has_its_own_job(
    experiment, tmp_path, monkeypatch
):
    config, task, adapter, _ = experiment
    frontier = LineTailFrontier()

    def control(_runtime, path, payload):
        if path == "/flowpilot/v1/jobs":
            job = asyncio.run(
                frontier.register_job(JobRegistration.model_validate(payload))
            )
            return {"job_id": job.job_id}
        assert path == "/flowpilot/v1/lines"
        return asyncio.run(
            frontier.register_line(LineRegistration.model_validate(payload))
        )

    def tail(runtime):
        snapshot = asyncio.run(frontier.snapshot(runtime.config.job_id))
        return next(
            (r for r in snapshot["lines"] if r["line_id"] == runtime.config.line_id),
            None,
        )

    client_type = httpx.Client
    transport = httpx.MockTransport(
        lambda r: httpx.Response(
            201, json=control(None, r.url.path, json.loads(r.content))
        )
    )
    monkeypatch.setattr(
        httpx, "Client", lambda **kw: client_type(transport=transport, **kw)
    )
    monkeypatch.setattr(FlowPilotRuntime, "_post_control", control)
    monkeypatch.setattr(FlowPilotRuntime, "_find_authoritative_tail", tail)
    agent = Agent(
        llm=LLM(
            model="openai/fixture",
            api_key=SecretStr("fixture"),
            max_input_tokens=32768,
            max_output_tokens=128,
        ),
        tools=[],
    )
    jobs = []
    for iteration in range(2):
        conversation_id = uuid.uuid4()
        started = datetime(2026, 10, 4, 0, iteration, tzinfo=UTC)
        record = register_workflow_slo(
            config, task, adapter, conversation_id=conversation_id, started_at=started
        )
        conversation = LocalConversation(
            agent=agent,
            workspace=str(tmp_path),
            conversation_id=conversation_id,
            persistence_dir=str(tmp_path / "state"),
            flowpilot=adapter,
            visualizer=None,
        )
        try:
            job_id = conversation._flowpilot.job_id
            jobs.append(job_id)
            assert job_id == record["job_id"] == f"job-{conversation_id}"
            state = asyncio.run(frontier.snapshot(job_id))
            assert state["workflow_started_at"] == started.isoformat()
            assert state["deadline"] == (started + timedelta(seconds=30)).isoformat()
            assert record["budget_seconds"] == 30
        finally:
            conversation._flowpilot_runtime._registered = False
            conversation.close()
    assert jobs[0] != jobs[1]


@pytest.mark.parametrize("failure", ["checksum", "missing_task", "http_error"])
def test_slo_registration_errors_are_not_silently_ignored(
    experiment, monkeypatch, failure
):
    config, task, adapter, baseline = experiment
    if failure == "checksum":
        baseline.write_text("{}")
    elif failure == "missing_task":
        task = Task(
            task.dataset_id, task.revision, task.split, "missing", task.instruction
        )
    else:
        client_type = httpx.Client
        monkeypatch.setattr(
            httpx,
            "Client",
            lambda **kw: client_type(
                transport=httpx.MockTransport(lambda r: httpx.Response(503)), **kw
            ),
        )
    with pytest.raises((ValueError, httpx.HTTPStatusError)):
        register_workflow_slo(
            config,
            task,
            adapter,
            conversation_id=uuid.uuid4(),
            started_at=datetime.now(UTC),
        )


def test_no_experiment_profile_keeps_slo_disabled(experiment, monkeypatch):
    config, task, adapter, _ = experiment
    monkeypatch.delenv("FLOWPILOT_EXPERIMENT_PROFILE")
    assert (
        register_workflow_slo(
            config,
            task,
            adapter,
            conversation_id=uuid.uuid4(),
            started_at=datetime.now(UTC),
        )
        is None
    )
