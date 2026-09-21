from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
from test_admission import body, scheduled_gateway
from test_gateway import _headers

from flowpilot.gateway.service import identity_from_headers
from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.protocol import LineFinish
from flowpilot.scheduling.projection import ProjectionCalculator
from flowpilot.scheduling.resolution import ToolResolutionStore
from flowpilot.scheduling.retention import (
    Capabilities,
    PrefixObservation,
    RetentionConfig,
    RetentionController,
    choose_retention,
)

CAPABILITIES = {
    "schema_version": 1,
    "engine": {"engine_epoch": "engine-1"},
    "descriptor_query": True,
    "gpu_retention_preference": True,
    "cpu_backed_eviction_preference": True,
    "safe_direct_drop": True,
    "cpu_store": True,
    "engine_cpu_reuse": True,
    "restore_cost_estimate": False,
    "continuation_proof": False,
}


def observation(**updates):
    return PrefixObservation.model_validate(
        {
            "schema_version": 1,
            "descriptor_id": "d1",
            "engine_epoch": "engine-1",
            "state_version": 1,
            "event_seq": 1,
            "prefix_token_count": 200,
            "gpu_ready_tokens": 0,
            "recoverable_tokens": 160,
            "lookup_state": "COMPLETE",
            "reuse_basis": "DESCRIPTOR_ONLY",
            **updates,
        }
    )


@pytest.mark.parametrize(
    "phase,free,need,expected",
    [
        ("READY", 1024, None, "KEEP"),
        ("BLOCKED", 1024, 0.1, "KEEP"),
        ("BLOCKED", 1024, None, "OFFLOAD"),
        ("READY", 0, None, "OFFLOAD"),
        ("TERMINAL", 1024, None, "DROP"),
    ],
)
def test_retention_uses_factual_readiness_and_real_free_allocations(
    phase,
    free,
    need,
    expected,
):
    decision = choose_retention(
        config=RetentionConfig(),
        capabilities=Capabilities.model_validate(CAPABILITIES),
        observation=observation(),
        phase=phase,
        need_in_seconds=need,
        free_gpu_allocations=free,
    )
    assert decision.action == expected


def test_capabilities_are_independent_and_unknown_recoverability_is_not_zero():
    caps = Capabilities.model_validate(
        {
            **CAPABILITIES,
            "cpu_store": False,
            "gpu_retention_preference": False,
        }
    )
    decision = choose_retention(
        config=RetentionConfig(),
        capabilities=caps,
        observation=observation(recoverable_tokens=None, lookup_state="PENDING"),
        phase="BLOCKED",
        need_in_seconds=None,
        free_gpu_allocations=0,
    )
    assert decision.action is None


class Engine:
    def __init__(self):
        self.paths = []
        self.inference = []
        self.commands: list[dict[str, Any]] = []
        self.version = 0
        self.free = 1024
        self.receipt_status = "APPLIED"
        self.status_result = "APPLIED"
        self.fail_apply_once = False
        self.query_gate: asyncio.Event | None = None
        self.query_started = asyncio.Event()
        self.unsupported = False

    def receipt(self, command, status):
        return {
            "schema_version": 1,
            **{
                k: command[k]
                for k in (
                    "action_id",
                    "idempotency_key",
                    "action",
                    "descriptor_id",
                    "owner_scope",
                )
            },
            "engine_epoch": "engine-1",
            "status": status,
            "operation_id": "op-1",
            "applied_policy_version": self.version,
            "cpu_committed_bytes": 0,
            "gpu_reclaimed_bytes": 0,
            "skipped_reasons": {},
        }

    async def __call__(self, request):
        path = request.url.path
        self.paths.append(path)
        data = json.loads(request.content) if request.content else {}
        if path == "/v1/kv/capabilities":
            return httpx.Response(501 if self.unsupported else 200, json=CAPABILITIES)
        if path == "/tokenize":
            return httpx.Response(200, json={"count": 300})
        if path == "/v1/chat/completions":
            self.inference.append(data)
            return httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]
                },
            )
        if path == "/v1/kv/resolve":
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "status": "READY",
                    "binding": data,
                    "descriptors": [
                        {
                            "descriptor_id": "d1",
                            "binding": data,
                            "engine": CAPABILITIES["engine"],
                        }
                    ],
                },
            )
        if path == "/v1/kv/telemetry":
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "engine_epoch": "engine-1",
                    "event_seq": 1,
                    "events_gap": True,
                    "free_gpu_allocations": self.free,
                },
            )
        if path == "/v1/kv/query":
            self.query_started.set()
            if self.query_gate is not None:
                await self.query_gate.wait()
            return httpx.Response(
                200,
                json=observation(
                    effective_policy_version=self.version,
                ).model_dump(),
            )
        if path == "/v1/kv/apply":
            self.commands.append(data)
            self.version = data["policy_version"]
            if self.fail_apply_once:
                self.fail_apply_once = False
                raise httpx.ReadTimeout("receipt lost")
            return httpx.Response(200, json=self.receipt(data, self.receipt_status))
        if path == "/v1/kv/status":
            return httpx.Response(
                200, json=self.receipt(self.commands[-1], self.status_result)
            )
        raise AssertionError(path)


async def setup(engine):
    gateway, runtime, frontier, client = await scheduled_gateway(engine)
    sink = InMemoryTraceSink()
    retention = RetentionController(
        client,
        "http://inference-a/v1",
        RetentionConfig(enabled=True),
        frontier,
        ProjectionCalculator(frontier, ToolResolutionStore()),
        TraceRecorder(sink),
    )
    runtime.retention = retention
    await retention.negotiate()
    return gateway, runtime, frontier, client, retention


async def complete(gateway, retention):
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=body(),
        headers=_headers(),
        raw_query=b"",
    )
    assert response.status_code == 200
    await asyncio.gather(*retention._tasks)


@pytest.mark.asyncio
async def test_gateway_binding_keep_pressure_offload_terminal_drop():
    engine = Engine()
    gateway, runtime, frontier, client, retention = await setup(engine)
    await complete(gateway, retention)
    binding = engine.inference[0]["kv_transfer_params"]["kv_control_binding"]
    assert binding["llm_call_id"] == "call-1"
    assert binding["request_id"] == "request-1"
    assert engine.commands[-1]["action"] == "KEEP"
    # FlowPilot tail_request_id is distinct; wire expected ID matches engine binding.
    assert engine.commands[-1]["expected_tail_request_id"] == "request-1"
    engine.free = 0
    await retention.refresh()
    assert engine.commands[-1]["action"] == "OFFLOAD"
    await frontier.finish_line(
        LineFinish(
            job_id="job-1",
            line_id="line-1",
            expected_tail_version=1,
        )
    )
    retention.line_finished("job-1", "line-1", 1)
    await retention.refresh()
    assert engine.commands[-1]["action"] == "DROP"
    assert len({x["idempotency_key"] for x in engine.commands}) == 3
    assert [x["policy_version"] for x in engine.commands] == [1, 2, 3]
    assert all("restore" not in path.lower() for path in engine.paths)
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["PARTIAL", "FAILED"])
async def test_accepted_is_polled_and_partial_or_failure_is_not_success(status):
    engine = Engine()
    engine.receipt_status = "ACCEPTED"
    engine.status_result = status
    gateway, runtime, _, client, retention = await setup(engine)
    await complete(gateway, retention)
    assert retention.snapshot()["sources"][0]["receipt_status"] == "ACCEPTED"
    await retention.refresh()
    assert retention.snapshot()["sources"][0]["receipt_status"] == status
    assert "/v1/kv/status" in engine.paths
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_lost_apply_response_retries_same_idempotent_command():
    engine = Engine()
    engine.fail_apply_once = True
    gateway, runtime, _, client, retention = await setup(engine)
    await complete(gateway, retention)
    await retention.refresh()
    assert len(engine.commands) == 2
    assert engine.commands[0] == engine.commands[1]
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_new_tail_invalidates_delayed_policy_and_cpu_only_never_blocks_submit():
    engine = Engine()
    engine.query_gate = asyncio.Event()
    gateway, runtime, _, client, retention = await setup(engine)
    await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=body(),
        headers=_headers(),
        raw_query=b"",
    )
    await engine.query_started.wait()
    second_headers = _headers(1, request_id="request-2", tail_request_id="tail-2")
    response = await gateway.proxy(
        path="/v1/chat/completions",
        api_kind="chat",
        body=body(),
        headers=second_headers,
        raw_query=b"",
    )
    assert response.status_code == 200 and len(engine.inference) == 2
    engine.query_gate.set()
    await asyncio.gather(*retention._tasks)
    assert all(command["source_llm_call_id"] == "call-2" for command in engine.commands)
    assert all("restore" not in path.lower() for path in engine.paths)
    assert (await runtime.snapshot())["admission"]["inflight"] == 0
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_unsupported_engine_keeps_inference_and_body_unmodified():
    engine = Engine()
    engine.unsupported = True
    gateway, runtime, _, client, retention = await setup(engine)
    await complete(gateway, retention)
    assert retention.status == "unsupported"
    assert engine.inference[0] == json.loads(body())
    assert not engine.commands
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_binding_preserves_other_transfer_parameters():
    engine = Engine()
    _, runtime, _, client, retention = await setup(engine)
    identity = identity_from_headers({k: [v] for k, v in _headers().items()})
    payload = {**json.loads(body()), "kv_transfer_params": {"other_parameter": "x"}}
    bound = json.loads(retention.bind(json.dumps(payload).encode(), identity))
    assert bound["messages"] == payload["messages"]
    assert bound["kv_transfer_params"]["other_parameter"] == "x"
    with pytest.raises(ValueError, match="owned by FlowPilot"):
        retention.bind(json.dumps(bound).encode(), identity)
    await runtime.close()
    await client.aclose()


@pytest.fixture
def local_vllm_contract():
    path = Path(
        os.getenv(
            "FLOWPILOT_VLLM_PROTOCOL_PATH",
            str(
                Path(__file__).resolve().parents[3]
                / "vllm/vllm/v1/kv_control/protocol.py"
            ),
        )
    )
    if not path.is_file():
        pytest.skip("local vLLM control protocol source not available")
    spec = importlib.util.spec_from_file_location("_vllm_control_contract", path)
    assert spec and spec.loader
    contract = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = contract
    # Only standalone Pydantic models; no vLLM/GPU initialization.
    spec.loader.exec_module(contract)
    try:
        yield contract
    finally:
        sys.modules.pop(spec.name, None)


@pytest.mark.asyncio
async def test_outbound_contract_matches_local_vllm_protocol_without_gpu(
    local_vllm_contract,
):
    contract = local_vllm_contract
    schemas = {
        "resolve": "CallBinding",
        "query": "QueryRequest",
        "apply": "PolicyRequest",
        "status": "OperationQuery",
        "telemetry": "TelemetryRequest",
    }
    checked = set()

    class CheckedEngine(Engine):
        async def __call__(self, request):
            name = request.url.path.rsplit("/", 1)[-1]
            if name in schemas:
                getattr(contract, schemas[name]).model_validate_json(request.content)
                checked.add(name)
            if request.url.path == "/v1/chat/completions":
                contract.CallBinding.model_validate(
                    json.loads(request.content)["kv_transfer_params"][
                        "kv_control_binding"
                    ]
                )
                checked.add("binding")
            return await super().__call__(request)

    engine = CheckedEngine()
    engine.receipt_status = "ACCEPTED"
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        await retention.refresh()
        assert checked == {*schemas, "binding"}
    finally:
        await runtime.close()
        await client.aclose()
