from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from test_admission import calibrated_model, priority

from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.protocol import ToolResolutionRecord
from flowpilot.scheduling.admission import AdmissionConfig
from flowpilot.scheduling.prefix import TargetPrefixQueries
from flowpilot.scheduling.projection import ProjectionCalculator
from flowpilot.scheduling.resolution import ToolResolutionStore
from integration.fit_cost_model import fit


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
