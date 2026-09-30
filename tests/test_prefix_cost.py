from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet
from test_admission import calibrated_model, priority
from test_benchmark_reuse import entry as benchmark_entry
from test_retention import CAPABILITIES
from test_retention import observation as retention_observation

from examples.experiments.qwen35_9b_tp4.profile import gateway_settings, load_profile
from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.protocol import ToolResolutionRecord
from flowpilot.scheduling.admission import AdmissionConfig
from flowpilot.scheduling.cost import (
    OfflineCostModel,
    PrefillCalibration,
    estimate_work,
)
from flowpilot.scheduling.prefix import TargetPrefixQueries
from flowpilot.scheduling.projection import ProjectionCalculator
from flowpilot.scheduling.resolution import ToolResolutionStore
from flowpilot.scheduling.retention import (
    Capabilities,
    RetentionConfig,
    choose_retention,
)
from integration.fit_cost_model import fit, fit_prefill


@pytest.mark.parametrize(
    "capability",
    [
        [],
        None,
        {"schema_version": 1},
        {"schema_version": 1, "target_prefix_query": True, "engine": []},
    ],
)
async def test_malformed_capabilities_return_explicit_unknown_for_entire_queue(
    capability,
):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=capability))
    ) as client:
        queries = TargetPrefixQueries(
            client,
            "http://engine",
            AdmissionConfig(),
            TraceRecorder(InMemoryTraceSink()),
            {},
            "owner",
        )
        requests = [priority("a"), priority("b")]
        result = await queries.refresh(requests)
    assert set(result) == {request.key for request in requests}
    assert queries.status == "unavailable"
    assert all(
        work.cost_seconds is None and work.prefix_basis == "COLD:unavailable"
        for work in result.values()
    )


@pytest.mark.parametrize("fault", [None, "epoch", "identity", "timeout"])
async def test_full_target_queries_validate_identity_and_preserve_unknown(fault):
    queried = []

    async def engine(request):
        if request.url.path.endswith("capabilities"):
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "target_prefix_query": True,
                    "engine": {"engine_epoch": "e", "identity_digest": "test-layout"},
                },
            )
        import json

        payload = json.loads(request.content)
        queried.append(payload)
        if fault == "timeout":
            raise httpx.ReadTimeout("unavailable")
        observation = {
            "query_id": payload["query_id"],
            "engine_epoch": "e",
            "engine_identity_digest": "other" if fault == "identity" else "test-layout",
            "state_version": 2,
            "reuse_basis": "TARGET_REQUEST",
            "prompt_tokens": 100,
            "gpu_ready_tokens": 40,
            "recoverable_tokens": 80,
            "cpu_load_object_bytes": None,
        }
        return httpx.Response(
            200,
            json={
                "schema_version": 1,
                "query_id": payload["query_id"],
                "engine_epoch": "wrong" if fault == "epoch" else "e",
                "inputs": [observation],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(engine)) as client:
        queries = TargetPrefixQueries(
            client,
            "http://engine",
            AdmissionConfig(cost_model=calibrated_model()),
            TraceRecorder(InMemoryTraceSink()),
            {},
            "owner",
        )
        requests = [priority("a"), priority("b")]
        for req in requests:
            queries.payloads[req.key] = ("responses", {"input": req.key[1]})
        result = await queries.refresh(requests)
    assert {p["query_id"] for p in queried} == {"a", "b"}
    assert all(p["api_kind"] == "responses" for p in queried)
    for work in result.values():
        assert work.cpu_cost_seconds is None
        if fault in {"identity", "epoch", "timeout"}:
            assert work.prefix_basis.startswith("COLD:query_failed")
            assert work.gpu_prefix_tokens is None
            assert work.prefill_tokens == 100
            assert work.cost_seconds == pytest.approx(0.1)
        else:
            assert work.prefill_tokens == 60
            assert work.cost_seconds == pytest.approx(0.06)


async def test_serial_tool_readiness_uses_remaining_running_time():
    now = datetime.now(UTC)

    class Frontier:
        async def line_snapshot(self, *_):
            return {"tail_request_id": "tail", "version": 1, "phase": "BLOCKED"}

    store = ToolResolutionStore()
    for name, duration, started in [
        ("a", 2000, now - timedelta(seconds=1)),
        ("b", 3000, None),
    ]:
        await store.update(
            ToolResolutionRecord(
                job_id="j",
                line_id="l",
                tail_request_id="tail",
                llm_call_id="call",
                tool_call_id=name,
                tool_family="web",
                resolution="local_only",
                status="resolving",
                source="local_model",
                confidence=1,
                version=1,
                updated_at=now,
                duration_estimate_ms=duration,
                execution_started_at=started,
            )
        )
    projection = await ProjectionCalculator(Frontier(), store).for_line(
        "j",
        "l",
        now=now,
        downstream_depth=1,
    )
    assert projection.t_need == now + timedelta(seconds=4)
    await store.close()


def test_offline_fitter_recovers_distinct_transfer_overhead_and_rate():
    fixed, slope, error = fit([(100, 0.3), (200, 0.5), (400, 0.9)])
    assert fixed == pytest.approx(0.1)
    assert slope == pytest.approx(0.002)
    assert error < 1e-10
    with pytest.raises(ValueError, match="distinct"):
        fit([(100, 0.3), (100, 0.31)])


def test_piecewise_prefill_uses_uncached_work_and_preserves_high_hit_measurement():
    bucket = PrefillCalibration.model_validate(
        fit_prefill([(10, 0.05), (500, 1.0), (1000, 1.5)], 1000, piecewise=True)
    )
    model = calibrated_model().model_copy(update={"prefill": (bucket,)})
    assert model.prefill_seconds(1000, 990) == pytest.approx(0.05)
    assert model.prefill_seconds(1000, 500) == pytest.approx(1.0)
    assert model.prefill_seconds(1000, 0) == pytest.approx(1.5)
    before = model.prefill_seconds(1000, 501)
    after = model.prefill_seconds(1000, 499)
    assert before is not None and after is not None
    assert before < 1.0 < after
    assert model.prefill_seconds(1001, 0) is None
    # Existing one-line calibrations retain their original behavior.
    assert calibrated_model().prefill_seconds(1000, 990) == pytest.approx(0.01)


@pytest.mark.parametrize("limits", [(1000, 500), (500, 500, 1000), (500,)])
def test_prefill_rejects_unordered_or_incomplete_segments(limits):
    with pytest.raises(ValueError, match="increase and cover"):
        PrefillCalibration.model_validate(
            {
                "max_context_tokens": 1000,
                "seconds_per_token": 0.001,
                "segments": [
                    {"max_uncached_tokens": limit, "seconds_per_token": 0.001}
                    for limit in limits
                ],
            }
        )


@pytest.fixture
def qwen_settings(tmp_path):
    registry = tmp_path / "registry.json"
    registry.write_text("[" + benchmark_entry().model_dump_json() + "]")

    def build(profile=None, cost_model_path=None):
        return gateway_settings(
            profile or load_profile(),
            run_dir=tmp_path,
            registry_path=registry,
            api_key="test-key",
            dcs_key=Fernet.generate_key().decode(),
            cost_model_path=cost_model_path,
        )

    return build


def test_tp4_profile_loads_costs_independent_of_cwd_and_accepts_override(
    qwen_settings, monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    model = qwen_settings().admission.cost_model
    assert isinstance(model, OfflineCostModel)
    assert model.prefill_seconds(131071, 130944) == pytest.approx(0.197, abs=0.003)
    assert model.prefill_seconds(131071, 0) == pytest.approx(29.14, abs=0.05)
    assert model.offload is not None and model.restore is not None
    assert model.offload.seconds(1124204544) == pytest.approx(0.0376, abs=0.001)
    assert model.restore.seconds(1124204544) == pytest.approx(0.0210, abs=0.001)
    override = tmp_path / "override.json"
    replacement = calibrated_model()
    override.write_text(replacement.model_dump_json())
    assert qwen_settings(cost_model_path=override).admission.cost_model == replacement
    profile = load_profile()
    profile["workload"]["cost_model_path"] = None
    assert qwen_settings(profile).admission.cost_model is None
    with pytest.raises(FileNotFoundError):
        qwen_settings(cost_model_path=Path("missing.json"))


def test_tp4_admission_uses_measured_restore_and_residual_prefill(qwen_settings):
    model = qwen_settings().admission.cost_model
    observation = {
        "engine_epoch": "engine",
        "engine_identity_digest": model.engine_identity_digest,
        "state_version": 1,
        "reuse_basis": "TARGET_REQUEST",
        "prompt_tokens": 131071,
        "gpu_ready_tokens": 0,
        "recoverable_tokens": 130944,
        "cpu_load_object_bytes": 4342284288,
    }
    work = estimate_work([observation], model, observed_at=1)
    assert work.gpu_cost_seconds == pytest.approx(29.14, abs=0.05)
    assert work.cpu_cost_seconds == pytest.approx(0.263, abs=0.003)
    assert work.cost_seconds == work.cpu_cost_seconds
    assert work.calibration_version == model.version
    observation["engine_identity_digest"] = "different-layout"
    assert estimate_work([observation], model, observed_at=1).cost_seconds is None


def test_tp4_retention_uses_measured_transfer_cost_instead_of_ready_time_fallback(
    qwen_settings,
):
    model = qwen_settings().admission.cost_model
    args = {
        "config": RetentionConfig(),
        "capabilities": Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": model.engine_identity_digest,
                },
            }
        ),
        "observation": retention_observation(
            prefix_token_count=32736,
            gpu_ready_tokens=32736,
            recoverable_tokens=32736,
            gpu_retention_bytes=1124204544,
            offload_target_tokens=32736,
            offload_object_bytes=1124204544,
        ),
        "phase": "BLOCKED",
        "need_in_seconds": 0.2,
        "free_gpu_allocations": 1024,
        "remaining_slo_seconds": 10.0,
    }
    assert choose_retention(**args).action == "KEEP"
    decision = choose_retention(**args, cost_model=model)
    assert decision.action == "OFFLOAD"
    assert decision.reason == "calibrated_slo_and_capacity:assumed_continuation"
