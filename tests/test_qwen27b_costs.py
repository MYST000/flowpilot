"""Exercise the measured 27B calibration through the experiment and gateway paths."""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace

import httpx
import pytest
from cryptography.fernet import Fernet
from test_benchmark_reuse import entry as benchmark_entry
from test_gateway import _headers
from test_retention import CAPABILITIES
from test_retention import observation as retention_observation

from examples.experiments.qwen35_9b_tp4.profile import gateway_settings, load_profile
from examples.experiments.qwen35_27b_tp4.launch import CONFIG_PATH, main
from flowpilot.app import create_app
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.scheduling.cost import estimate_work
from flowpilot.scheduling.retention import Capabilities, choose_retention


@pytest.fixture
def launch_inputs(tmp_path):
    registry = tmp_path / "registry.json"
    registry.write_text("[" + benchmark_entry().model_dump_json() + "]")
    return {
        "run_dir": tmp_path / "run",
        "registry_path": registry,
        "api_key": "test-key",
        "dcs_key": Fernet.generate_key().decode(),
    }


@pytest.fixture
def settings_27b(launch_inputs):
    return gateway_settings(load_profile(CONFIG_PATH), **launch_inputs)


def target(model, **updates):
    return {
        "engine_epoch": "engine-1",
        "engine_identity_digest": model.engine_identity_digest,
        "state_version": 1,
        "reuse_basis": "TARGET_REQUEST",
        "prompt_tokens": 258048,
        "gpu_ready_tokens": 0,
        "recoverable_tokens": 257936,
        "cpu_load_object_bytes": 17058037760,
        **updates,
    }


def test_27b_entry_loads_current_calibration_without_starting_services(
    launch_inputs, settings_27b, monkeypatch, capsys, tmp_path
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FLOWPILOT_COST_MODEL_PATH", raising=False)
    monkeypatch.setenv("FLOWPILOT_INGRESS_API_KEY", launch_inputs["api_key"])
    monkeypatch.setenv("FLOWPILOT_DCS_ENCRYPTION_KEY", launch_inputs["dcs_key"])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launch",
            "gateway",
            "--check",
            "--run-dir",
            str(launch_inputs["run_dir"]),
            "--registry",
            str(launch_inputs["registry_path"]),
        ],
    )
    main()
    output = capsys.readouterr().out
    assert f"Qwen3.5-27B / {settings_27b.admission.cost_model.version}" in output
    assert "max context 258048" in output
    assert "H2D=calibrated D2H=calibrated" in output
    assert not launch_inputs["run_dir"].exists()


def test_27b_costs_preserve_the_frozen_profile_and_unknown_components(settings_27b):
    profile = load_profile(CONFIG_PATH)
    spec = profile["vllm"]["args"]["kv_transfer_config"]["kv_connector_extra_config"]
    assert spec["cpu_bytes_to_use"] == 64 * 2**30
    assert settings_27b.admission.limit == 4
    assert settings_27b.admission.policy == "prefill_slack"
    assert not settings_27b.forecast_enabled
    assert not settings_27b.synthetic_tool_duration_enabled
    model = settings_27b.admission.cost_model
    assert model.offload is not None and model.restore is not None
    # Residency prices remain policy coefficients, not transfer measurements.
    assert settings_27b.retention.gpu_seconds_per_gib_second == 1
    assert settings_27b.retention.cpu_seconds_per_gib_second == 0.01
    assert model.prefill_seconds(258049, 0) is None


@pytest.mark.parametrize(
    "gpu_hit,all_hit,object_bytes,expected_gpu,expected_cpu,expected_cost",
    [
        (0, 0, 0, 178.828366, 178.828366, 178.828366),
        (128576, 128576, 0, 99.802017, 99.802017, 99.802017),
        (257936, 257936, 0, 0.528532, 0.528532, 0.528532),
        (0, 257936, 17058037760, 178.828366, 0.767614, 0.767614),
        (0, 257936, None, 178.828366, None, 178.828366),
    ],
)
def test_27b_target_costs_use_measured_prefill_and_actual_bytes(
    settings_27b,
    gpu_hit,
    all_hit,
    object_bytes,
    expected_gpu,
    expected_cpu,
    expected_cost,
):
    model = settings_27b.admission.cost_model
    work = estimate_work(
        [
            target(
                model,
                gpu_ready_tokens=gpu_hit,
                recoverable_tokens=all_hit,
                cpu_load_object_bytes=object_bytes,
            )
        ],
        model,
        observed_at=1,
    )
    assert work.gpu_cost_seconds == pytest.approx(expected_gpu, abs=1e-6)
    assert work.cpu_cost_seconds == (
        pytest.approx(expected_cpu, abs=1e-6) if expected_cpu is not None else None
    )
    assert work.cost_seconds == pytest.approx(expected_cost, abs=1e-6)
    assert work.calibration_version == model.version
    assert work.calibration_source == model.source
    incompatible = estimate_work(
        [target(model, engine_identity_digest="different-layout")], model, observed_at=1
    )
    assert incompatible.cost_seconds is None
    assert incompatible.cost_basis == "unknown:calibration_identity_mismatch"


@pytest.mark.parametrize(
    "gpu_hit,cpu_hit,phase,gap,offload_known,expected,calibrated",
    [
        (257936, 257936, "BLOCKED", 1, True, "OFFLOAD", True),
        (257936, None, "BLOCKED", 1, True, "OFFLOAD", True),
        (257936, 128576, "BLOCKED", 1, True, "OFFLOAD", True),
        (0, 257936, "READY", 0, True, "OFFLOAD", True),
        (257936, None, "BLOCKED", None, True, "OFFLOAD", False),
        (257936, None, "BLOCKED", 1, False, "KEEP", True),
        (257936, 128576, "BLOCKED", 1, False, "KEEP", True),
        (257936, 257936, "BLOCKED", 1, False, "OFFLOAD", True),
        (257936, None, "READY", 0, True, "KEEP", True),
        (257936, None, "BLOCKED", 0.001, True, "KEEP", True),
    ],
)
def test_27b_retention_distinguishes_ready_cpu_and_calibrated_or_unknown_copy(
    settings_27b, gpu_hit, cpu_hit, phase, gap, offload_known, expected, calibrated
):
    model = settings_27b.admission.cost_model
    if not offload_known:
        model = model.model_copy(update={"offload": None})
    decision = choose_retention(
        config=settings_27b.retention,
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": model.engine_identity_digest,
                },
            }
        ),
        observation=retention_observation(
            prefix_token_count=258047,
            gpu_ready_tokens=gpu_hit,
            recoverable_tokens=257936,
            cpu_standalone_tokens=cpu_hit,
            gpu_retention_bytes=17058037760 if gpu_hit else 0,
            offload_target_tokens=257936,
            offload_object_bytes=17058037760,
        ),
        phase=phase,
        need_in_seconds=gap,
        free_gpu_allocations=1024,
        remaining_slo_seconds=300,
        cost_model=model,
    )
    assert decision.action == expected
    if calibrated:
        assert decision.reason == "calibrated_slo_and_capacity:assumed_continuation"
    else:
        assert "fallback_cost_unknown" in decision.reason


async def test_27b_gateway_uses_shared_costs_and_submits_cpu_only_request(settings_27b):
    # Exercise the production app wiring with controlled engine facts, no GPU.
    settings = replace(settings_27b, reuse_enabled=False, dcs_enabled=False)
    model = settings.admission.cost_model
    submitted, complete = asyncio.Event(), asyncio.Event()
    paths = []

    async def engine(request):
        path = request.url.path
        paths.append(path)
        data = json.loads(request.content) if request.content else {}
        if path == "/health":
            return httpx.Response(200)
        if path == "/tokenize":
            return httpx.Response(200, json={"count": 258048})
        if path == "/v1/kv/capabilities":
            return httpx.Response(
                200,
                json={
                    **CAPABILITIES,
                    "target_prefix_query": True,
                    "engine": {
                        "engine_epoch": "engine-1",
                        "identity_digest": model.engine_identity_digest,
                    },
                },
            )
        if path == "/v1/kv/query-target":
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "query_id": data["query_id"],
                    "engine_epoch": "engine-1",
                    "inputs": [target(model, query_id=data["query_id"])],
                },
            )
        if path == "/v1/kv/telemetry":
            return httpx.Response(
                200,
                json={
                    "engine_epoch": "engine-1",
                    "event_seq": 1,
                    "events": [],
                    "free_gpu_allocations": 1024,
                },
            )
        if path == "/v1/kv/resolve":
            return httpx.Response(
                200,
                json={
                    "binding": data,
                    "status": "UNKNOWN_BINDING",
                    "observed_at_monotonic": 1,
                    "descriptors": [],
                },
            )
        assert path == "/v1/chat/completions"
        assert data["model"] == "Qwen3.5-27B"
        submitted.set()
        await complete.wait()
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]},
        )

    sink = InMemoryTraceSink()
    async with httpx.AsyncClient(transport=httpx.MockTransport(engine)) as upstream:
        app = create_app(settings, http_client=upstream, trace_sink=sink)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
            ) as client,
        ):
            runtime = app.state.scheduling
            assert runtime.prefix_queries.config.cost_model is model
            assert runtime.retention.cost_model is model
            for path, body in [
                ("jobs", {"job_id": "job-1"}),
                (
                    "lines",
                    {
                        "job_id": "job-1",
                        "line_id": "line-1",
                        "conversation_id": "conversation-line-1",
                        "context_epoch": 1,
                        "base_context_cursor": "cursor-0",
                        "context_digest": "a" * 64,
                    },
                ),
            ]:
                response = await client.post(
                    "/flowpilot/v1/" + path,
                    json=body,
                    headers={"x-flowpilot-api-key": "test-key"},
                )
                assert response.status_code == 201
            request = asyncio.create_task(
                client.post(
                    "/v1/chat/completions",
                    headers=_headers(),
                    json={
                        "model": "Qwen3.5-27B",
                        "messages": [{"role": "user", "content": "test input"}],
                    },
                )
            )
            try:
                await asyncio.wait_for(submitted.wait(), 2)
                assert (await runtime.snapshot())["admission"]["inflight"] == 1
                event = next(
                    r for r in sink.records if r["event_type"] == "request_admitted"
                )
                assert event["fields"]["ordering_basis"] == "prefill_slack"
                work = event["fields"]["work"]
                assert work["gpu_prefix_tokens"] == 0
                assert work["cost_seconds"] == pytest.approx(0.767614, abs=1e-6)
                assert work["calibration_version"] == model.version
            finally:
                complete.set()
                response = await asyncio.wait_for(request, 2)
            assert response.status_code == 200
            assert (await runtime.snapshot())["admission"]["inflight"] == 0
    assert "/v1/kv/query-target" in paths
    assert not any("restore" in path.lower() for path in paths)
