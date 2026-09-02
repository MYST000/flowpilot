from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import httpx
import pytest

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.gateway.router import (
    InferenceRouter,
    InstanceLoadProfile,
    RoutingPolicy,
    RoutingRequest,
    WeightedFairRequestQueue,
)
from flowpilot.observability.trace import JsonlTraceSink
from flowpilot.protocol import (
    ForecastCandidate,
    ForecastRequest,
    ForecastResult,
    KVActionKind,
    KVLease,
    KVStateFact,
    KVTier,
    SchedulingProjection,
)
from flowpilot.scheduling import (
    DeterministicToolAnalysisAdapter,
    ForecastManager,
    HTTPVLLMKVAdapter,
    KVDirectory,
    KVFact,
    KVRejected,
    KVStale,
    MockVLLMKVAdapter,
    Request2Timing,
    ResourceCapacities,
    RestoreQueue,
    RestoreQueueEntry,
    TemporalKVCoordinator,
    ToolObservation,
    ToolResolutionStore,
    request2_timing,
    to_state_fact,
    wait_age_tier_decision,
)
from flowpilot.state import SharedStateConflict, SQLiteSharedStateBackend


def _forecast_request(job: str, request_id: str = "same-request") -> ForecastRequest:
    return ForecastRequest(
        request_id=request_id,
        job_id=job,
        line_id=f"line-{job}",
        model_id="model",
        history_features_ref="features:1",
        tool_catalog_version="catalog-1",
    )


def _forecast_result(
    request: ForecastRequest, *, lifetime: float = 1.0
) -> ForecastResult:
    return ForecastResult(
        based_on_request_id=request.request_id,
        job_id=request.job_id,
        line_id=request.line_id,
        candidates=(
            ForecastCandidate(
                tool_family="web", probability=1, duration_p50=10, duration_p90=20
            ),
        ),
        confidence=1,
        predictor_version="deterministic-1",
        expires_at=datetime.now(UTC) + timedelta(seconds=lifetime),
    )


class _ScopedAdapter:
    async def forecast(self, request: ForecastRequest) -> ForecastResult:
        return _forecast_result(request, lifetime=0.06)

    async def cancel(self, request_id: str) -> None:
        del request_id


@pytest.mark.anyio
async def test_forecast_identity_collision_and_background_ttl_cleanup() -> None:
    manager = ForecastManager(
        _ScopedAdapter(),
        ttl_seconds=1,
        sweep_interval_seconds=0.01,
    )
    one = _forecast_request("job-a")
    two = _forecast_request("job-b")
    await manager.start(one)
    await manager.start(two)
    await asyncio.sleep(0.01)
    with pytest.raises(ValueError, match="ambiguous"):
        await manager.result(one.request_id)
    assert (
        await manager.result(
            one.request_id, job_id=one.job_id, line_id=one.line_id
        )
        is not None
    )
    assert (
        await manager.result(
            two.request_id, job_id=two.job_id, line_id=two.line_id
        )
        is not None
    )
    await manager.supersede(
        one.request_id, job_id=one.job_id, line_id=one.line_id
    )
    assert (
        await manager.result(
            one.request_id, job_id="job", line_id="line"
        )
        is None
    )
    assert (
        await manager.result(
            two.request_id, job_id=two.job_id, line_id=two.line_id
        )
        is not None
    )
    await asyncio.sleep(0.08)
    assert (await manager.snapshot())["results"] == []
    await manager.close()


@pytest.mark.anyio
async def test_resolution_forecast_scope_capacity_and_ttl() -> None:
    store = ToolResolutionStore(
        forecast_max_entries=1, forecast_sweep_interval_seconds=0.01
    )
    one, two = (
        _forecast_request("job-a", "one"),
        _forecast_request("job-b", "two"),
    )
    await store.save_forecast(one, _forecast_result(one, lifetime=0.05))
    await store.save_forecast(two, _forecast_result(two, lifetime=0.05))
    assert (
        await store.forecast("one", job_id=one.job_id, line_id=one.line_id)
        is None
    )
    assert (
        await store.forecast("two", job_id=two.job_id, line_id=two.line_id)
        is not None
    )
    await asyncio.sleep(0.07)
    assert await store.sweep_forecasts() in {0, 1}
    assert (
        await store.forecast("two", job_id=two.job_id, line_id=two.line_id)
        is None
    )
    await store.close()


@pytest.mark.anyio
async def test_forecast_manager_capacity_is_bounded() -> None:
    manager = ForecastManager(
        _ScopedAdapter(),
        ttl_seconds=1,
        max_entries=1,
    )
    await manager.start(_forecast_request("job-a", "one"))
    await asyncio.sleep(0.01)
    await manager.start(_forecast_request("job-b", "two"))
    await asyncio.sleep(0.01)
    snapshot = await manager.snapshot()
    results = cast(list[dict[str, object]], snapshot["results"])
    assert len(results) == 1
    assert results[0]["job_id"] == "job-b"
    await manager.close()


def _kv_fact(
    *,
    handle: str = "handle-a",
    epoch: str = "epoch-1",
    generation: int = 1,
    sequence: int = 1,
    tier: KVTier = KVTier.CPU,
    observed_at: datetime | None = None,
) -> KVStateFact:
    return KVStateFact(
        job_id="job",
        line_id="line",
        llm_call_id="llm-1",
        instance_id="instance-a",
        engine_epoch=epoch,
        session_id="same-session",
        kv_handle=handle,
        generation=generation,
        tier=tier,
        bytes=4096,
        restore_cost_ms=20,
        migration_cost_ms=30,
        rematerialization_cost_ms=100,
        observed_at=observed_at or datetime.now(UTC),
        sequence=sequence,
    )


@pytest.mark.anyio
async def test_http_kv_adapter_negotiates_auth_and_rejects_epoch_restart() -> None:
    calls: list[httpx.Request] = []
    state = _kv_fact()
    lease = KVLease(
        lease_id="lease-1",
        job_id=state.job_id,
        line_id=state.line_id,
        instance_id=state.instance_id,
        engine_epoch=state.engine_epoch,
        session_id=state.session_id,
        kv_handle=state.kv_handle,
        generation=state.generation,
        owner="worker",
        fencing_token=1,
        expires_at=datetime.now(UTC) + timedelta(seconds=10),
    )
    epoch = "epoch-1"

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal epoch
        calls.append(request)
        if request.url.path.endswith("/capabilities"):
            return httpx.Response(
                200,
                json={
                    "schema_version": "flowpilot-vllm-kv-v2",
                    "enabled": True,
                    "engine_epoch": epoch,
                },
            )
        if request.url.path.endswith("/leases/acquire"):
            return httpx.Response(200, json=lease.model_dump(mode="json"))
        if request.url.path.endswith("/leases/release"):
            return httpx.Response(204)
        return httpx.Response(
            200,
            json={
                "status": "applied",
                "action_id": "action-1",
                "generation": 2,
                "fact": state.model_copy(update={"generation": 2}).model_dump(
                    mode="json"
                ),
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = HTTPVLLMKVAdapter("http://vllm", api_key="secret", client=client)
    assert await adapter.negotiate()
    obtained = await adapter.acquire_lease(state, "worker", 10)
    assert obtained.lease_id == "lease-1"
    await adapter.release_lease(obtained)
    assert all(request.headers["authorization"] == "Bearer secret" for request in calls)
    epoch = "epoch-2"
    with pytest.raises(KVStale, match="engine epoch changed"):
        await adapter.negotiate()
    await client.aclose()


@pytest.mark.anyio
async def test_kv_directory_scope_order_duplicate_and_engine_restart() -> None:
    directory = KVDirectory(fact_ttl_seconds=60)
    first = await directory.record_fact(_kv_fact())
    second = await directory.record_fact(_kv_fact(handle="handle-b", sequence=2))
    assert first.kv_handle == "handle-a"
    assert second.kv_handle == "handle-b"
    assert (await directory.get("instance-a", "same-session")) == second
    assert (
        await directory.get(
            "instance-a",
            "same-session",
            job_id="job",
            line_id="line",
        )
    ) == second
    assert await directory.record_fact(to_state_fact(second)) == second
    with pytest.raises(KVStale):
        await directory.record_fact(_kv_fact(sequence=0))
    with pytest.raises(KVRejected):
        await directory.record_fact(
            _kv_fact(handle="handle-b", sequence=3).model_copy(
                update={"line_id": "line-2"}
            )
        )
    epoch_two = datetime.now(UTC) + timedelta(milliseconds=1)
    await directory.record_fact(
        _kv_fact(
            epoch="epoch-2",
            handle="handle-new",
            generation=0,
            sequence=0,
            observed_at=epoch_two,
        )
    )
    removed = await directory.invalidate_engine("instance-a", "epoch-2")
    assert removed == 0
    current = await directory.get(
        "instance-a", "same-session", job_id="job", line_id="line"
    )
    assert current is not None and current.engine_epoch == "epoch-2"
    with pytest.raises(KVStale):
        await directory.record_fact(
            _kv_fact(epoch="epoch-1", observed_at=epoch_two - timedelta(seconds=1))
        )


def _projection(
    *, weight: float = 1.0, t_need: datetime | None = None, version: int = 1
) -> SchedulingProjection:
    now = datetime.now(UTC)
    return SchedulingProjection(
        job_id="job",
        line_id="line",
        tail_request_id="tail-1",
        tail_version=version,
        ready=False,
        t_need=t_need,
        t2=t_need,
        request_weight=weight,
        dag_importance=2,
        slo_urgency=3,
        blocking_line_count=4,
        wait_age_ms=5,
        computed_at=now,
    )


@pytest.mark.anyio
async def test_kv_lease_fencing_generation_cas_duplicate_and_tail_guard() -> None:
    adapter = MockVLLMKVAdapter()
    state = _kv_fact()
    await adapter.seed(state)
    fact = await KVDirectory().record_fact(state)
    valid = True

    async def validate(_projection: SchedulingProjection) -> bool:
        return valid

    coordinator = TemporalKVCoordinator(adapter, validate, migration_cooldown_seconds=0)
    lease = await coordinator.acquire(fact, "worker-1")
    result = await coordinator.execute(KVActionKind.RESTORE, _projection(), fact, lease)
    duplicate = await coordinator.execute(
        KVActionKind.RESTORE, _projection(), fact, lease
    )
    assert result == duplicate
    newer = await coordinator.acquire(
        await KVDirectory().record_fact(result.fact or state), "worker-2"
    )
    with pytest.raises(KVStale):
        await coordinator.execute(KVActionKind.RESTORE, _projection(), fact, lease)
    with pytest.raises(KVStale):
        await coordinator.execute(KVActionKind.DROP, _projection(), fact, lease)
    assert newer.fencing_token > lease.fencing_token
    valid = False
    with pytest.raises(KVStale):
        await coordinator.execute(
            KVActionKind.KEEP,
            _projection(version=2),
            await KVDirectory().record_fact(result.fact or state),
            newer,
        )


@pytest.mark.anyio
async def test_kv_lease_expiry_and_migration_cooldown_fall_back_safely() -> None:
    adapter = MockVLLMKVAdapter()
    state = _kv_fact(tier=KVTier.GPU)
    await adapter.seed(state)
    fact = await KVDirectory().record_fact(state)

    async def valid(_projection: SchedulingProjection) -> bool:
        return True

    expiring = TemporalKVCoordinator(adapter, valid, lease_ttl_seconds=0.01)
    expired_lease = await expiring.acquire(fact, "expiring-worker")
    await asyncio.sleep(0.02)
    with pytest.raises(KVStale):
        await expiring.execute(KVActionKind.OFFLOAD, _projection(), fact, expired_lease)

    lease = await adapter.acquire_lease(to_state_fact(fact), "worker", 1)
    coordinator = TemporalKVCoordinator(adapter, valid, migration_cooldown_seconds=10)
    offloaded = await coordinator.execute(
        KVActionKind.OFFLOAD, _projection(), fact, lease
    )
    next_fact = await KVDirectory().record_fact(offloaded.fact or state)
    next_lease = await adapter.acquire_lease(to_state_fact(next_fact), "worker", 1)
    with pytest.raises(KVStale, match="cooldown"):
        await coordinator.execute(
            KVActionKind.DROP, _projection(), next_fact, next_lease
        )


@pytest.mark.anyio
async def test_expired_kv_fact_becomes_unsupported() -> None:
    directory = KVDirectory(fact_ttl_seconds=0.01)
    await directory.record_fact(_kv_fact())
    await asyncio.sleep(0.02)
    assert (
        await directory.get(
            "instance-a",
            "same-session",
            job_id="job",
            line_id="line",
        )
        is None
    )


def test_t_need_t_kv_t2_and_unsupported_fallback() -> None:
    now = datetime.now(UTC)
    supported = request2_timing(
        tool_ready_at=now + timedelta(milliseconds=10),
        continuation_cost_ms=5,
        fact=KVFact(
            "job",
            "line",
            "llm",
            "session",
            "instance-a",
            "epoch",
            "handle",
            1,
            KVTier.CPU,
            1,
            20,
            30,
            100,
            now,
            1,
            "flowpilot-vllm-kv-v2",
        ),
        now=now,
    )
    assert isinstance(supported, Request2Timing)
    assert supported.t_need == now + timedelta(milliseconds=15)
    assert supported.t_kv == now + timedelta(milliseconds=20)
    assert supported.t2 == supported.t_kv
    unsupported = request2_timing(
        tool_ready_at=now, continuation_cost_ms=5, fact=None, now=now
    )
    assert unsupported.kv_telemetry == "unsupported"
    assert unsupported.t_kv is None and unsupported.t2 == unsupported.t_need


@pytest.mark.anyio
async def test_restore_queue_laxity_overdue_weight_age_and_cooldown() -> None:
    now = datetime.now(UTC)
    base = await KVDirectory().record_fact(_kv_fact(observed_at=now))
    future_fact = await KVDirectory().record_fact(
        _kv_fact(handle="handle-future", observed_at=now)
    )
    cooling_fact = await KVDirectory().record_fact(
        _kv_fact(handle="handle-cooling", observed_at=now)
    )
    queue = RestoreQueue()
    overdue = RestoreQueueEntry(
        _projection(weight=2, t_need=now + timedelta(milliseconds=10)),
        base,
        now - timedelta(seconds=2),
    )
    future = RestoreQueueEntry(
        _projection(weight=100, t_need=now + timedelta(seconds=1)), future_fact, now
    )
    cooling = RestoreQueueEntry(
        _projection(weight=1000, t_need=now),
        cooling_fact,
        now,
        now + timedelta(seconds=1),
    )
    await queue.upsert(future)
    await queue.upsert(overdue)
    await queue.upsert(cooling)
    ordered = await queue.ordered(now)
    assert ordered[0] == overdue
    assert cooling not in ordered
    assert await queue.overdue(now) == (overdue,)


def test_tool_and_kv_capacities_are_independent() -> None:
    capacities = ResourceCapacities(100, 200, 300, 400)
    assert capacities.tool_admissible(90, 10)
    assert not capacities.tool_admissible(90, 11)
    assert capacities.kv_admissible(KVTier.GPU, 190, 10)
    assert not capacities.kv_admissible(KVTier.GPU, 190, 11)
    cpu_fact = KVFact(
        "job",
        "line",
        "llm",
        "session",
        "instance",
        "epoch",
        "handle",
        1,
        KVTier.CPU,
        1,
        1,
        1,
        1,
        datetime.now(UTC),
        1,
        "flowpilot-vllm-kv-v2",
    )
    tiering = wait_age_tier_decision(
        cpu_fact,
        wait_age_seconds=31,
        cpu_pressure=True,
    )
    assert tiering.action == KVActionKind.OFFLOAD
    assert tiering.target_tier == KVTier.NVME


@pytest.mark.anyio
async def test_queue_slo_blocking_policy_and_soft_affinity() -> None:
    instances = (InferenceInstance("a", "http://a"), InferenceInstance("b", "http://b"))
    router = InferenceRouter(instances, policy=RoutingPolicy.QUEUE_SLO_BLOCKING)
    now = datetime.now(UTC)
    await router.update_load(
        InstanceLoadProfile(
            "a",
            queue_depth=10,
            ttft_ms=100,
            throughput_tokens_per_second=10,
            updated_at=now,
        )
    )
    await router.update_load(
        InstanceLoadProfile(
            "b",
            queue_depth=0,
            ttft_ms=5,
            throughput_tokens_per_second=10,
            updated_at=now,
        )
    )
    request = RoutingRequest(
        "job", "line", 2, now + timedelta(milliseconds=20), 4, "a"
    )
    assert (await router.candidates("model", request))[0].instance_id == "b"
    queue = WeightedFairRequestQueue()
    queue.push(RoutingRequest("job-a", "line-1"))
    queue.push(RoutingRequest("job-a", "line-2"))
    queue.push(RoutingRequest("job-b", "line-1"))
    assert queue.pop().job_id == "job-a"
    assert queue.pop().job_id == "job-b"
    same_job = WeightedFairRequestQueue()
    same_job.push(RoutingRequest("job", "normal"))
    same_job.push(
        RoutingRequest(
            "job",
            "urgent",
            deadline=now - timedelta(milliseconds=1),
            blocking_line_count=2,
        )
    )
    assert same_job.pop().line_id == "urgent"


def test_profile_costs_output_duration_and_hysteresis() -> None:
    adapter = DeterministicToolAnalysisAdapter(minimum_samples=2)
    first = adapter.observe(
        ToolObservation("web", 90, 90, 80, 100, 1000), inference_cost_ms=10
    )
    second = adapter.observe(
        ToolObservation("web", 90, 90, 70, 80, 800), inference_cost_ms=10
    )
    assert not first.heavy and second.heavy
    middle = adapter.observe(
        ToolObservation("web", 50, 50, 40, 60, 600), inference_cost_ms=50
    )
    assert middle.heavy
    light = adapter.observe(
        ToolObservation("web", 10, 10, 5, 20, 100), inference_cost_ms=90
    )
    assert not light.heavy
    assert light.calibration_status == "uncalibrated"
    assert adapter.project("web") == light


@pytest.mark.anyio
async def test_sqlite_shared_writer_crash_expiry_fencing_and_order(
    tmp_path: Path,
) -> None:
    path = tmp_path / "shared.sqlite"
    one = SQLiteSharedStateBackend(path)
    two = SQLiteSharedStateBackend(path)
    claim = await one.acquire(
        "frontier",
        "job/line",
        owner="worker-1",
        schema_version="v1",
        generation=1,
        ttl_seconds=0.05,
    )
    with pytest.raises(SharedStateConflict):
        await two.acquire(
            "frontier",
            "job/line",
            owner="worker-2",
            schema_version="v1",
            generation=1,
            ttl_seconds=1,
        )
    committed = await one.commit(
        claim, expected_state_version=0, sequence=1, payload_digest="a" * 64
    )
    assert (
        await one.commit(
            committed, expected_state_version=1, sequence=1, payload_digest="a" * 64
        )
        == committed
    )
    with pytest.raises(SharedStateConflict):
        await one.commit(
            committed, expected_state_version=1, sequence=3, payload_digest="b" * 64
        )
    await asyncio.sleep(0.06)
    replacement = await two.acquire(
        "frontier",
        "job/line",
        owner="worker-2",
        schema_version="v1",
        generation=1,
        ttl_seconds=1,
    )
    assert replacement.fencing_token > claim.fencing_token
    with pytest.raises(SharedStateConflict):
        await one.commit(
            committed, expected_state_version=1, sequence=2, payload_digest="b" * 64
        )
    with pytest.raises(SharedStateConflict):
        await one.acquire(
            "frontier",
            "job/line",
            owner="worker-3",
            schema_version="v2",
            generation=2,
            ttl_seconds=1,
        )
    await two.release(replacement)
    upgraded = await one.acquire(
        "frontier",
            "job/line",
        owner="worker-3",
        schema_version="v2",
        generation=2,
        ttl_seconds=1,
    )
    assert upgraded.generation == 2 and upgraded.last_sequence == 0
    await one.close()
    await two.close()


@pytest.mark.anyio
async def test_trace_rotation_and_restart_append(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    sink = JsonlTraceSink(path, max_bytes=80, backup_count=2)
    await sink.write({"event": "one", "padding": "x" * 50})
    await sink.write({"event": "two", "padding": "x" * 50})
    assert (tmp_path / "trace.jsonl.1").exists()
    restarted = JsonlTraceSink(path, max_bytes=1_000, backup_count=2)
    await restarted.write({"event": "after-restart"})
    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert [item["event"] for item in records] == ["two", "after-restart"]


def test_multi_worker_uses_explicit_backend_contract_and_fails_closed(
    tmp_path: Path,
) -> None:
    settings = Settings(
        instances=(InferenceInstance("a", "http://a"),),
        trace_path=tmp_path / "trace.jsonl",
        ingress_api_key="key",
        workers=2,
        shared_state_path=tmp_path / "shared.sqlite",
    )
    with pytest.raises(ValueError, match="multi-worker serving is fail-closed"):
        create_app(settings)
