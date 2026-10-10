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
from flowpilot.scheduling.prefill import PrefillLoad
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


def prefill_load(model, **updates):
    return PrefillLoad(
        basis="running_decode_snapshot",
        engine_epoch="engine-1",
        engine_identity_digest=model.engine_identity_digest,
        observed_at_monotonic=1,
        decode_requests=0,
        decode_context_tokens=0,
        active_prefill_requests=0,
        token_budget=2048,
        max_num_seqs=256,
        block_tokens=784,
        max_model_len=262144,
        async_scheduling=True,
        enable_chunked_prefill=True,
        mamba_cache_mode="align",
    ).model_copy(update=updates)


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
        "prefill_load": prefill_load(model).model_dump(),
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
    assert "max context 262144" in output
    assert "H2D=calibrated D2H=calibrated" in output
    assert not launch_inputs["run_dir"].exists()


def test_27b_costs_preserve_the_frozen_profile_and_transfer_calibrations(settings_27b):
    profile = load_profile(CONFIG_PATH)
    spec = profile["vllm"]["args"]["kv_transfer_config"]["kv_connector_extra_config"]
    assert spec["cpu_bytes_to_use"] == 64 * 2**30
    assert settings_27b.admission.limit == 4
    assert settings_27b.admission.policy == "wait_cost"
    assert not settings_27b.forecast_enabled
    assert not settings_27b.synthetic_tool_duration_enabled
    model = settings_27b.admission.cost_model
    assert model.offload.fixed_seconds == 0.004039796055271255
    assert model.offload.seconds_per_byte == 2.044685376438999e-11
    assert model.offload.seconds(0) == 0
    assert model.restore.fixed_seconds == 0.004506207082665055
    assert model.restore.seconds_per_byte == 1.6286942923456314e-11
    assert profile["vllm"]["args"]["max_num_seqs"] == 256
    assert model.prefill_cadence.coefficients == (
        0.09322706889150323,
        0.0002421383133504632,
        -0.0007936305139279601,
        1.2311587355865282e-7,
        -2.561924781527601e-8,
        1.0606758558305914e-9,
        6.332483291982032e-10,
    )
    assert model.prefill == ()
    # Residency prices remain policy coefficients, not transfer measurements.
    assert settings_27b.retention.gpu_seconds_per_gib_second == 1
    assert settings_27b.retention.cpu_seconds_per_gib_second == 0.01
    assert model.prefill_seconds(262145, 0, load=prefill_load(model)) is None


def test_cadence_alignment_uses_history_and_two_residual_chunks(settings_27b):
    model = settings_27b.admission.cost_model
    load = prefill_load(model, decode_requests=3, decode_context_tokens=6000)
    estimate = model.prefill_estimate(2000, 784, load=load)
    # Native alignment gives [784 at h=784, 432 at h=1568].
    expected = (
        2,
        1216,
        6,
        784**2 + 432**2,
        12000,
        784 * 784 + 432 * 1568,
        784 * (6000 + 784) + 432 * (6000 + 1568),
    )
    assert estimate.features == expected
    assert estimate.seconds == pytest.approx(
        sum(
            w * x
            for w, x in zip(model.prefill_cadence.coefficients, expected, strict=True)
        )
    )
    full = model.prefill_estimate(2000, 2000, load=load)
    assert full.features[1] == 1
    assert full.seconds > 0
    assert model.prefill_estimate(2000, 783, load=load).features[1] == 1217
    assert not estimate.extrapolated
    assert model.prefill_estimate(100000, 0, load=load).extrapolated
    assert model.prefill_seconds(262145, 0, load=load) is None


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("missing", "prefill_load_missing"),
        ("epoch", "prefill_load_identity_mismatch"),
        ("identity", "prefill_load_identity_mismatch"),
        ("budget", "prefill_configuration_mismatch"),
        ("sequences", "prefill_configuration_mismatch"),
        ("negative", "nonpositive_or_nonfinite_prefill_prediction"),
    ],
)
def test_cadence_unusable_context_is_unknown_never_zero(settings_27b, fault, reason):
    model = settings_27b.admission.cost_model
    row = target(
        model, prompt_tokens=2000, recoverable_tokens=1568, cpu_load_object_bytes=10000
    )
    if fault == "missing":
        row.pop("prefill_load")
    elif fault == "negative":
        calibration = model.prefill_cadence.model_copy(
            update={"coefficients": (-1.0,) * 7}
        )
        model = model.model_copy(update={"prefill_cadence": calibration})
    else:
        key, value = {
            "epoch": ("engine_epoch", "stale"),
            "identity": ("engine_identity_digest", "other"),
            "budget": ("token_budget", 4096),
            "sequences": ("max_num_seqs", 4),
        }[fault]
        row["prefill_load"][key] = value
    work = estimate_work([row], model, observed_at=1)
    assert work.cost_seconds is None
    assert work.cost_basis == "unknown:" + reason
    assert work.calibration_version == model.version


def test_cadence_uses_decode_context_and_compatible_frozen_restore(settings_27b):
    model = settings_27b.admission.cost_model
    idle = model.prefill_seconds(2000, 784, load=prefill_load(model))
    busy = model.prefill_seconds(
        2000,
        784,
        load=prefill_load(model, decode_requests=8, decode_context_tokens=200000),
    )
    assert busy > idle
    work = estimate_work([target(model)], model, observed_at=1)
    assert work.prefill_estimates[0].load.basis == "running_decode_snapshot"
    assert "candidate_prefill_frozen_decode" in work.prefill_estimates[0].basis
    residual = model.prefill_seconds(258048, 257936, load=prefill_load(model))
    assert work.cpu_cost_seconds == pytest.approx(
        0.004506207082665055 + 17058037760 * 1.6286942923456314e-11 + residual
    )
    assert work.cost_seconds == min(work.cpu_cost_seconds, work.gpu_cost_seconds)
    for no_restore in (
        estimate_work(
            [target(model, cpu_load_object_bytes=None)], model, observed_at=1
        ),
        estimate_work(
            [target(model)], model.model_copy(update={"restore": None}), observed_at=1
        ),
    ):
        assert no_restore.cpu_cost_seconds is None
        assert no_restore.cost_seconds == no_restore.gpu_cost_seconds > 0


def test_cadence_retention_reads_same_load_and_exposes_missing_transfer(settings_27b):
    model = settings_27b.admission.cost_model
    args = dict(
        config=settings_27b.retention,
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "prefill_cost_context": True,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": model.engine_identity_digest,
                },
            }
        ),
        phase="BLOCKED",
        retention_window_seconds=0.1,
        free_gpu_allocations=1024,
        cost_model=model,
    )
    observation = retention_observation(
        prefix_token_count=1999,
        gpu_ready_tokens=1568,
        recoverable_tokens=1568,
        gpu_retention_bytes=4096,
        offload_target_tokens=1568,
        offload_object_bytes=4096,
        offload_new_object_bytes=4096,
        prefill_load=prefill_load(model),
    )
    decision = choose_retention(**args, observation=observation)
    assert decision.action == "KEEP"
    keep = next(c for c in decision.candidates if c.action == "KEEP")
    assert keep.cost_seconds == pytest.approx(
        model.prefill_seconds(2000, 1568, load=prefill_load(model)) + 4096 / 2**30 * 0.1
    )
    offload = next(c for c in decision.candidates if c.action == "OFFLOAD")
    assert offload.reason == "calibrated:new_d2h"
    assert offload.cost_seconds == pytest.approx(
        model.offload.seconds(4096)
        + model.restore.seconds(4096)
        + model.prefill_seconds(2000, 1568, load=prefill_load(model))
        + 4096 / 2**30 * 0.1 * 0.01
    )
    covered = choose_retention(
        **args,
        observation=observation.model_copy(update={"cpu_standalone_tokens": 1568}),
    )
    covered_offload = next(c for c in covered.candidates if c.action == "OFFLOAD")
    assert covered_offload.reason == "calibrated:cpu_already_covered"
    assert covered_offload.cost_seconds == pytest.approx(
        model.restore.seconds(4096)
        + model.prefill_seconds(2000, 1568, load=prefill_load(model))
        + 4096 / 2**30 * 0.1 * 0.01
    )
    missing = choose_retention(
        **args, observation=observation.model_copy(update={"prefill_load": None})
    )
    assert missing.reason.startswith("fallback_cost_unknown")
    assert all(c.cost_seconds is None for c in missing.candidates)
    args["capabilities"] = args["capabilities"].model_copy(
        update={"prefill_cost_context": False}
    )
    undeclared = choose_retention(**args, observation=observation)
    assert undeclared.reason.startswith("fallback_cost_unknown")
    assert all(c.cost_seconds is None for c in undeclared.candidates)


@pytest.mark.parametrize("load_available", [True, False])
async def test_27b_gateway_uses_shared_costs_and_submits_cpu_only_request(
    settings_27b, load_available
):
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
                    "prefill_cost_context": load_available,
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
                    "inputs": [
                        target(
                            model,
                            query_id=data["query_id"],
                            prefill_load=prefill_load(model).model_dump()
                            if load_available
                            else None,
                        )
                    ],
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
                assert event["fields"]["ordering_basis"] == (
                    "wait_cost" if load_available else "fifo:cost_unknown"
                )
                work = event["fields"]["work"]
                assert work["gpu_prefix_tokens"] == 0
                if load_available:
                    assert work["cost_seconds"] == pytest.approx(
                        model.restore.seconds(17058037760)
                        + model.prefill_seconds(
                            258048, 257936, load=prefill_load(model)
                        )
                    )
                    assert work["cost_seconds"] == work["cpu_cost_seconds"]
                    assert work["cpu_cost_seconds"] < work["gpu_cost_seconds"]
                    assert work["prefill_estimates"][0]["load"]["max_num_seqs"] == 256
                else:
                    assert work["cost_seconds"] is None
                    assert work["cost_basis"] == "unknown:prefill_load_missing"
                assert work["calibration_version"] == model.version
            finally:
                complete.set()
                response = await asyncio.wait_for(request, 2)
            assert response.status_code == 200
            assert (await runtime.snapshot())["admission"]["inflight"] == 0
    assert "/v1/kv/query-target" in paths
    assert not any("restore" in path.lower() for path in paths)
