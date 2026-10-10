from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from test_admission import body, calibrated_model, scheduled_gateway
from test_gateway import _headers

from flowpilot.gateway.service import identity_from_headers
from flowpilot.observability.trace import InMemoryTraceSink, TraceRecorder
from flowpilot.protocol import LineFinish
from flowpilot.scheduling import retention as retention_module
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
    "metadata_ttl_seconds": 300,
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
        ("READY", 1024, None, "OFFLOAD"),
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
        retention_window_seconds=need,
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
        retention_window_seconds=None,
        free_gpu_allocations=0,
    )
    assert decision.action is None


@pytest.mark.parametrize(
    "gap,expected",
    [
        (0.001, "KEEP"),
        (1, "OFFLOAD"),
        (100000, "DROP"),
    ],
)
def test_calibrated_retention_compares_window_and_capacity(gap, expected):
    decision = choose_retention(
        config=RetentionConfig(),
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": "test-layout",
                },
            }
        ),
        observation=observation(
            gpu_ready_tokens=192,
            recoverable_tokens=192,
            gpu_retention_bytes=2**30,
            offload_target_tokens=192,
            offload_object_bytes=1000000,
            offload_new_object_bytes=1000000,
        ),
        phase="BLOCKED",
        retention_window_seconds=gap,
        free_gpu_allocations=1024,
        cost_model=calibrated_model(),
    )
    assert decision.action == expected
    assert decision.reason == "calibrated_cost:assumed_continuation"
    costs = {c.action: c.cost_seconds for c in decision.candidates}
    assert costs["DROP"] == pytest.approx(0.201)
    assert costs["KEEP"] == pytest.approx(0.009 + gap)
    assert costs["OFFLOAD"] == pytest.approx(0.012 + 1000000 / 2**30 * gap * 0.01)


@pytest.mark.parametrize("has_offload_model", [True, False])
@pytest.mark.parametrize("policy_action", [None, "OFFLOAD"])
def test_ready_successor_preserves_already_resident_cpu_prefix(
    has_offload_model, policy_action
):
    model = calibrated_model()
    if not has_offload_model:
        model = model.model_copy(update={"offload": None})
    decision = choose_retention(
        config=RetentionConfig(),
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": model.engine_identity_digest,
                },
            }
        ),
        observation=observation(
            prefix_token_count=1000,
            gpu_ready_tokens=0,
            recoverable_tokens=990,
            cpu_standalone_tokens=990,
            gpu_retention_bytes=0,
            offload_target_tokens=990,
            offload_object_bytes=1000000,
            effective_policy_action=policy_action,
        ),
        phase="READY",
        retention_window_seconds=0,
        free_gpu_allocations=1024,
        cost_model=model,
    )
    assert decision.action == "OFFLOAD"
    assert decision.reason == "calibrated_cost:assumed_continuation"


@pytest.mark.parametrize("cpu_tokens", [None, 0, 160])
def test_offload_policy_does_not_prove_cpu_copy_is_ready(cpu_tokens):
    decision = choose_retention(
        config=RetentionConfig(),
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": "test-layout",
                },
            }
        ),
        observation=observation(
            gpu_ready_tokens=192,
            recoverable_tokens=192,
            cpu_standalone_tokens=cpu_tokens,
            gpu_retention_bytes=2**30,
            offload_target_tokens=192,
            offload_object_bytes=1000000,
            offload_new_object_bytes=1000000,
            effective_policy_action="OFFLOAD",
        ),
        phase="READY",
        retention_window_seconds=0,
        free_gpu_allocations=0,
        cost_model=calibrated_model(),
    )
    assert decision.action == "KEEP"


@pytest.mark.parametrize(
    "new_bytes,action,reason",
    [
        (100000, "OFFLOAD", "calibrated:new_d2h"),
        (1000000, "KEEP", "estimated_d2h_exceeds_window"),
        (None, "KEEP", "offload_new_bytes_unknown"),
    ],
)
def test_partial_cpu_replica_uses_missing_bytes_for_d2h_window_only(
    new_bytes, action, reason
):
    model = calibrated_model()
    decision = choose_retention(
        config=RetentionConfig(gpu_seconds_per_gib_second=100),
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": model.engine_identity_digest,
                },
            }
        ),
        observation=observation(
            gpu_ready_tokens=192,
            recoverable_tokens=192,
            cpu_standalone_tokens=160,
            gpu_retention_bytes=2**30,
            offload_target_tokens=192,
            offload_object_bytes=1000000,
            offload_new_object_bytes=new_bytes,
        ),
        phase="BLOCKED",
        retention_window_seconds=0.0005,
        free_gpu_allocations=1024,
        cost_model=model,
    )
    assert decision.action == action
    candidate = next(c for c in decision.candidates if c.action == "OFFLOAD")
    assert candidate.reason == reason
    if new_bytes == 100000:
        # H2D and CPU residency still use the complete target, independently of D2H.
        assert candidate.cost_seconds == pytest.approx(
            0.0001 + 0.002 + 0.009 + 1000000 / 2**30 * 0.0005 * 0.01
        )
    else:
        assert candidate.cost_seconds is None


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
        self.engine_clock = 10000.0
        self.descriptor_expires_at = self.engine_clock + 300
        self.events = []
        self.event_seq = 1
        self.events_gap = False
        self.telemetry_gate: asyncio.Event | None = None
        self.telemetry_started = asyncio.Event()

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
                    "observed_at_monotonic": self.engine_clock,
                    "descriptors": [
                        {
                            "descriptor_id": "d1",
                            "binding": data,
                            "engine": CAPABILITIES["engine"],
                            "expires_at_monotonic": self.descriptor_expires_at,
                        }
                    ],
                },
            )
        if path == "/v1/kv/telemetry":
            self.telemetry_started.set()
            if self.telemetry_gate is not None:
                await self.telemetry_gate.wait()
            return httpx.Response(
                200,
                json={
                    "schema_version": 1,
                    "engine_epoch": "engine-1",
                    "event_seq": self.event_seq,
                    "events": self.events,
                    "events_gap": self.events_gap,
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
    assert runtime.queue is not None
    retention.set_queue_wait_provider(runtime.queue.queue_wait_estimate)
    gateway._resolution_store = retention.projections.resolutions
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
async def test_capability_timeout_preserves_binding_but_disables_actions():
    class FlakyEngine(Engine):
        fail_capabilities = False

        async def __call__(self, request):
            if request.url.path.endswith("capabilities") and self.fail_capabilities:
                raise httpx.ReadTimeout("busy engine")
            return await super().__call__(request)

    engine = FlakyEngine()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        engine.fail_capabilities = True
        await retention.negotiate()
        assert retention.status == "unavailable"
        await complete(gateway, retention)
        assert "kv_control_binding" in engine.inference[0]["kv_transfer_params"]
        assert "d1" in retention._sources
        assert not engine.commands
        engine.fail_capabilities = False
        await retention.negotiate()
        await retention.refresh()
        assert engine.commands[-1]["action"] == "KEEP"
        engine.unsupported = True
        await retention.negotiate()
        identity = identity_from_headers({k: [v] for k, v in _headers().items()})
        assert retention.bind(body(), identity) == body()
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ["recover", "tail_removed", "epoch_changed", "deadline"]
)
async def test_resolve_retries_transport_failure_within_engine_lifetime(outcome):
    class FlakyEngine(Engine):
        attempts = 0

        async def __call__(self, request):
            if request.url.path.endswith("resolve"):
                self.attempts += 1
                if self.attempts == 1 or outcome == "deadline":
                    if outcome == "tail_removed":
                        await frontier.finish_line(
                            LineFinish(
                                job_id="job-1",
                                line_id="line-1",
                                expected_tail_version=1,
                            )
                        )
                    if outcome == "epoch_changed":
                        retention._engine_epoch = "new-engine"
                    raise httpx.ReadTimeout("busy engine")
            return await super().__call__(request)

    engine = FlakyEngine()
    gateway, runtime, frontier, client, retention = await setup(engine)
    retention.config = retention.config.model_copy(update={"refresh_seconds": 0.01})
    retention._metadata_ttl_seconds = 0.06
    try:
        await complete(gateway, retention)
        assert bool(retention._sources) == (outcome == "recover")
        assert bool(engine.commands) == (outcome == "recover")
        if outcome == "recover":
            assert engine.attempts == 2
        elif outcome == "deadline":
            assert engine.attempts > 1
            assert retention.snapshot()["last_error"] == "TimeoutError"
        else:
            assert engine.attempts == 1
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["PARTIAL", "FAILED"])
async def test_offload_failure_requeries_and_retries_new_version_after_delay(
    monkeypatch, status
):
    clock = [100.0]
    monkeypatch.setattr(retention_module, "monotonic", lambda: clock[0])
    engine = Engine()
    engine.free = 0
    engine.receipt_status = "ACCEPTED"
    engine.status_result = status
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        await retention.refresh()
        assert retention.snapshot()["sources"][0]["receipt_status"] == status
        await retention.refresh()
        assert len(engine.commands) == 1
        query_count = engine.paths.count("/v1/kv/query")
        clock[0] += retention.config.refresh_seconds
        engine.free = 1024  # The selected OFFLOAD survives a pressure change.
        engine.receipt_status = "APPLIED"
        await retention.refresh()
        assert engine.paths.count("/v1/kv/query") == query_count + 1
        assert [c["policy_version"] for c in engine.commands] == [1, 2]
        assert (
            engine.commands[0]["idempotency_key"]
            != engine.commands[1]["idempotency_key"]
        )
        assert retention.snapshot()["sources"][0]["receipt_status"] == "APPLIED"
        await retention.refresh()
        assert len(engine.commands) == 2
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
async def test_one_source_timeout_does_not_skip_other_sources():
    class FlakyEngine(Engine):
        fail_first = False

        async def __call__(self, request):
            if request.url.path.endswith("query"):
                did = json.loads(request.content)["descriptor_id"]
                if self.fail_first and did == "d1":
                    raise httpx.ReadTimeout("one descriptor unavailable")
                response = await super().__call__(request)
                return httpx.Response(
                    200, json={**response.json(), "descriptor_id": did}
                )
            return await super().__call__(request)

    engine = FlakyEngine()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        first = retention._sources["d1"]
        first.decision = None
        first.dirty = True
        retention._register_source(
            replace(
                first,
                descriptor_id="d2",
                expiry_handle=None,
                last_action=None,
                last_status=None,
                observation=None,
            )
        )
        engine.fail_first = True
        await retention.refresh()
        assert first.dirty
        assert engine.commands[-1]["descriptor_id"] == "d2"
        assert retention._sources["d2"].last_status == "APPLIED"
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
async def test_tool_events_during_rpc_do_not_reselect_placement():
    engine = Engine()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        engine.telemetry_started.clear()
        engine.telemetry_gate = asyncio.Event()
        before = engine.paths.count("/v1/kv/query")
        retention.line_changed("job-1", "line-1")
        await asyncio.wait_for(engine.telemetry_started.wait(), 1)
        for _ in range(50):
            retention.line_changed("job-1", "line-1")
        assert len(retention._tasks) == 1
        engine.telemetry_gate.set()
        await asyncio.gather(*retention._tasks)
        assert engine.paths.count("/v1/kv/query") == before
        assert len(engine.commands) == 1
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
async def test_gateway_binding_frozen_keep_then_terminal_cleanup():
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
    assert len(engine.commands) == 1
    assert engine.commands[-1]["action"] == "KEEP"
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
    assert len({x["idempotency_key"] for x in engine.commands}) == 2
    assert [x["policy_version"] for x in engine.commands] == [1, 2]
    assert all("restore" not in path.lower() for path in engine.paths)
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_unchanged_tail_is_not_queried_on_each_refresh():
    engine = Engine()
    gateway, runtime, _, client, retention = await setup(engine)
    await complete(gateway, retention)
    queries = engine.paths.count("/v1/kv/query")
    await retention.refresh()
    await retention.refresh()
    assert engine.paths.count("/v1/kv/query") == queries
    engine.free = 0
    await retention.refresh()
    assert engine.paths.count("/v1/kv/query") == queries
    assert len(engine.commands) == 1
    assert engine.commands[-1]["action"] == "KEEP"
    await runtime.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_resolve_converts_clock_domain_and_does_not_renew_local_ttl(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(retention_module, "monotonic", lambda: clock[0])
    engine = Engine()  # Engine clock starts at 10000, independently of FlowPilot.
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        source = retention._sources["d1"]
        assert source.expires_at_monotonic == 400.0
        first_timer = source.expiry_handle
        assert first_timer is not None
        assert source.inputs is not None
        queries = engine.paths.count("/v1/kv/query")
        clock[0] += 10
        # Even a delayed/repeated clock sample cannot renew the existing deadline.
        await retention._resolve(source.identity, source.tail_version, source.inputs)
        assert retention._sources["d1"] is source
        assert source.expires_at_monotonic == 400.0
        assert first_timer.cancelled()
        assert engine.paths.count("/v1/kv/query") == queries
        assert len(engine.commands) == 1
        assert retention.snapshot()["sources"][0]["remaining_ttl_seconds"] == 290
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", ["none", "operation", "lost_receipt"])
async def test_idle_expiry_runs_while_control_rpc_is_blocked(monkeypatch, pending):
    engine = Engine()
    engine.descriptor_expires_at = engine.engine_clock + 0.5
    engine.receipt_status = "ACCEPTED" if pending == "operation" else "APPLIED"
    engine.fail_apply_once = pending == "lost_receipt"
    engine.events_gap = True  # No expiry event will be delivered.
    gateway, runtime, _, client, retention = await setup(engine)
    expired = asyncio.Event()
    expire_source = retention._expire_source

    def observed_expiry(source, reason):
        expire_source(source, reason)
        expired.set()

    monkeypatch.setattr(retention, "_expire_source", observed_expiry)
    refreshing = None
    try:
        await complete(gateway, retention)
        before_queries = engine.paths.count("/v1/kv/query")
        before_commands = len(engine.commands)
        engine.telemetry_started.clear()
        engine.telemetry_gate = asyncio.Event()
        refreshing = asyncio.create_task(retention.refresh())
        await asyncio.wait_for(engine.telemetry_started.wait(), 1)
        # Capability failure does not disable the local expiry timer.
        engine.unsupported = True
        await retention.negotiate()
        await asyncio.wait_for(expired.wait(), 2)
        assert not retention._sources  # No snapshot/refresh needed to prune it.
        assert not refreshing.done()
        engine.telemetry_gate.set()
        await refreshing
        assert engine.paths.count("/v1/kv/query") == before_queries
        assert len(engine.commands) == before_commands
        assert retention.snapshot()["source_expirations"] == {"deadline": 1}
    finally:
        if refreshing is not None:
            refreshing.cancel()
            await asyncio.gather(refreshing, return_exceptions=True)
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
async def test_expiry_during_prefix_query_prevents_late_policy(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(retention_module, "monotonic", lambda: clock[0])
    engine = Engine()
    engine.query_gate = asyncio.Event()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        response = await gateway.proxy(
            path="/v1/chat/completions",
            api_kind="chat",
            body=body(),
            headers=_headers(),
            raw_query=b"",
        )
        assert response.status_code == 200
        await asyncio.wait_for(engine.query_started.wait(), 1)
        clock[0] = 401.0
        engine.query_gate.set()
        await asyncio.gather(*retention._tasks)
        assert not retention._sources
        assert not engine.commands
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch", [None, "owner_scope", "engine_epoch", "descriptor_id"]
)
async def test_expiry_event_removes_only_matching_source_without_query(mismatch):
    engine = Engine()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        source = retention._sources["d1"]
        timer = source.expiry_handle
        assert timer is not None
        queries = engine.paths.count("/v1/kv/query")
        event = dict(
            kind="DESCRIPTOR_EXPIRED",
            owner_scope="flowpilot-local",
            engine_epoch="engine-1",
            descriptor_id="d1",
            event_seq=2,
        )
        if mismatch:
            event[mismatch] = "other"
        engine.events = [event]
        engine.event_seq = 2
        engine.events_gap = True
        await retention.refresh()
        assert bool(retention._sources) == (mismatch is not None)
        assert timer.cancelled() == (mismatch is None)
        assert engine.paths.count("/v1/kv/query") == queries
        assert len(engine.commands) == 1
        if mismatch is None:
            assert retention.snapshot()["source_expirations"] == {"engine_event": 1}
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["close", "engine_restart"])
async def test_close_or_epoch_change_cancels_descriptor_timers(monkeypatch, cause):
    engine = Engine()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        timer = retention._sources["d1"].expiry_handle
        assert timer is not None
        if cause == "close":
            await retention.close()
        else:
            engine.unsupported = True
            await retention.negotiate()

            async def restarted(*_):
                return {**CAPABILITIES, "engine": {"engine_epoch": "engine-2"}}

            monkeypatch.setattr(retention, "_rpc", restarted)
            await retention.negotiate()
            assert retention._event_seq == 0
        assert timer.cancelled()
        assert not retention._sources
    finally:
        await runtime.close()
        await client.aclose()


@pytest.mark.asyncio
async def test_resolve_latency_does_not_extend_expiry_and_missing_clock_is_visible(
    monkeypatch,
):
    clock = [100.0]
    monkeypatch.setattr(retention_module, "monotonic", lambda: clock[0])

    class DelayedEngine(Engine):
        omit_clock = False

        async def __call__(self, request):
            response = await super().__call__(request)
            if request.url.path.endswith("/resolve"):
                clock[0] += 2
                if self.omit_clock:
                    data = response.json()
                    del data["observed_at_monotonic"]
                    return httpx.Response(200, json=data)
            return response

    engine = DelayedEngine()
    gateway, runtime, _, client, retention = await setup(engine)
    try:
        await complete(gateway, retention)
        source = retention._sources["d1"]
        assert source.expires_at_monotonic == 400.0
        assert retention.snapshot()["sources"][0]["remaining_ttl_seconds"] == 298
        engine.omit_clock = True
        assert source.inputs is not None
        await retention._resolve(source.identity, source.tail_version, source.inputs)
        assert retention.snapshot()["last_error"] == "ValidationError"
        assert source.expires_at_monotonic == 400.0
    finally:
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


@pytest.mark.parametrize("phase", ["READY", "BLOCKED"])
def test_ready_phase_never_erases_positive_retention_window(phase):
    model = calibrated_model()
    decision = choose_retention(
        config=RetentionConfig(),
        capabilities=Capabilities.model_validate(
            {
                **CAPABILITIES,
                "engine": {
                    "engine_epoch": "engine-1",
                    "identity_digest": model.engine_identity_digest,
                },
            }
        ),
        observation=observation(
            gpu_ready_tokens=192,
            recoverable_tokens=192,
            gpu_retention_bytes=2**30,
            offload_target_tokens=192,
            offload_object_bytes=1000000,
            offload_new_object_bytes=1000000,
        ),
        phase=phase,
        retention_window_seconds=1,
        free_gpu_allocations=1024,
        cost_model=model,
    )
    assert decision.action == "OFFLOAD"
    assert {c.action: c.cost_seconds for c in decision.candidates}[
        "KEEP"
    ] == pytest.approx(1.009)


async def test_queue_feedback_await_revalidates_replaced_source():
    engine = Engine()
    gateway, runtime, frontier, client, retention = await setup(engine)
    started, finish = asyncio.Event(), asyncio.Event()
    original = retention._queue_wait_provider
    assert original is not None

    async def feedback():
        started.set()
        await finish.wait()
        return await original()

    retention.set_queue_wait_provider(feedback)
    try:
        pending = asyncio.create_task(complete(gateway, retention))
        await started.wait()
        retention.forget("job-1", "call-1")
        identity = identity_from_headers(
            {
                k: [v]
                for k, v in _headers(
                    1,
                    call_id="call-2",
                    request_id="request-2",
                    tail_request_id="tail-2",
                ).items()
            }
        )
        await frontier.begin_request(identity, model="m")
        finish.set()
        await pending
        assert not engine.commands
    finally:
        finish.set()
        await runtime.close()
        await client.aclose()
