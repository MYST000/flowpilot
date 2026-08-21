from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from flowpilot.protocol import (
    BindingFailureReport,
    FalseReuseReport,
    LeaderProgressReport,
    LeaderResultPublish,
    ReuseDecisionKind,
    ReuseMatchKind,
    ReuseProtocolVersion,
    ReuseScope,
    ReuseType,
    SemanticReusePolicyUpdate,
    ToolRegistryEntry,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
)
from flowpilot.reuse import WebReuseController
from flowpilot.reuse.controller import ReuseConflict


def _registry() -> tuple[ToolRegistryEntry, ...]:
    return (
        ToolRegistryEntry(
            tool_name="web_search",
            canonical_tool_family="public_web_search",
            tool_version="1",
            result_schema_version="1",
            default_ttl_seconds=60,
        ),
    )


def _semantic_registry(*, threshold: float = 0.55) -> tuple[ToolRegistryEntry, ...]:
    return (
        _registry()[0].model_copy(
            update={
                "protocol_version": "flowpilot-phase3-reuse-v1",
                "semantic_reuse_enabled": True,
                "semantic_similarity_threshold": threshold,
            }
        ),
    )


def _identity(line: str, call: str = "tool-1") -> ToolReuseIdentity:
    return ToolReuseIdentity(
        tenant_id="tenant-1",
        job_id="job-1",
        line_id=line,
        tail_request_id=f"tail-{line}",
        llm_call_id=f"llm-{line}",
        action_id=f"action-{line}",
        tool_call_id=call,
    )


def _request(
    line: str,
    *,
    query: str = "flowpilot",
    auth_scope: str = "anonymous",
    budget: int | None = None,
    protocol_version: ReuseProtocolVersion = "flowpilot-phase1-reuse-v1",
    time_sensitivity_class: str = "standard",
) -> ToolReuseResolveRequest:
    return ToolReuseResolveRequest(
        protocol_version=protocol_version,
        identity=_identity(line),
        tool_name="web_search",
        arguments={"query": query},
        scope=ReuseScope(
            tenant_id="tenant-1",
            auth_scope=auth_scope,
            time_sensitivity_class=time_sensitivity_class,
        ),
        output_budget_bytes=budget,
    )


@pytest.mark.anyio
async def test_exact_inflight_has_one_leader_and_independent_follower_identity(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(_registry(), tmp_path / "cache.sqlite")
    leader, follower = await asyncio.gather(
        controller.resolve(_request("line-1")),
        controller.resolve(_request("line-2", budget=45)),
    )
    decisions = {leader.decision, follower.decision}
    assert decisions == {
        ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER,
        ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT,
    }
    if leader.decision != ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER:
        leader, follower = follower, leader
        leader_identity = _identity("line-2")
        follower_identity = _identity("line-1")
    else:
        leader_identity = _identity("line-1")
        follower_identity = _identity("line-2")
    assert leader.binding_id == follower.binding_id
    result = {
        "items": [
            {"title": "first", "url": "https://one"},
            {"title": "second", "url": "https://two"},
        ]
    }
    await controller.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=leader_identity,
            result=result,
        )
    )
    reused = await controller.poll(follower.binding_id or "", follower_identity)
    assert reused.decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
    assert reused.provenance is not None
    assert reused.provenance.reuse_type == ReuseType.INFLIGHT
    assert reused.provenance.truncation_policy == "items_prefix_v1"
    assert reused.provenance.returned_size <= 45


@pytest.mark.anyio
async def test_repeated_resolve_preserves_leader_and_follower_roles(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(_registry(), tmp_path / "cache.sqlite")
    leader_request = _request("line-1")
    leader = await controller.resolve(leader_request)
    repeated_leader = await controller.resolve(leader_request, defer_allowed=True)
    assert repeated_leader.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    assert repeated_leader.binding_id == leader.binding_id

    follower_request = _request("line-2", budget=45)
    follower = await controller.resolve(follower_request, defer_allowed=True)
    repeated_follower = await controller.resolve(
        follower_request.model_copy(update={"output_budget_bytes": 1000})
    )
    assert follower.decision == ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
    assert repeated_follower.decision == ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
    assert repeated_follower.binding_id == follower.binding_id

    snapshot = await controller.snapshot()
    assert snapshot["bindings"][0]["follower_count"] == 1
    await controller.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=leader_request.identity,
            result={
                "items": [
                    {"title": "first", "url": "https://one"},
                    {"title": "second", "url": "https://two"},
                ]
            },
        )
    )
    reused = await controller.poll(
        follower.binding_id or "", follower_request.identity, defer_allowed=True
    )
    assert reused.provenance is not None
    assert reused.provenance.returned_size <= 45


@pytest.mark.anyio
async def test_phase2_defer_is_explicit_and_does_not_change_phase1(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(_registry(), tmp_path / "cache.sqlite")
    leader = await controller.resolve(_request("line-1"), defer_allowed=True)
    assert leader.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER

    deferred_follower = await controller.resolve(_request("line-2"), defer_allowed=True)
    immediate_follower = await controller.resolve(_request("line-3"))
    assert deferred_follower.decision == ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
    assert immediate_follower.decision == ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT

    await controller.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=_identity("line-1"),
            result={"items": [{"title": "exact"}]},
        )
    )
    deferred = await controller.poll(
        deferred_follower.binding_id or "",
        _identity("line-2"),
        defer_allowed=True,
    )
    immediate = await controller.poll(
        immediate_follower.binding_id or "", _identity("line-3")
    )
    assert deferred.decision == ReuseDecisionKind.DEFER_WITH_CACHED_RESULT
    assert immediate.decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT

    historical_deferred = await controller.resolve(
        _request("line-4"), defer_allowed=True
    )
    historical_immediate = await controller.resolve(_request("line-5"))
    assert historical_deferred.decision == ReuseDecisionKind.DEFER_WITH_CACHED_RESULT
    assert historical_immediate.decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT


@pytest.mark.anyio
async def test_hard_scope_prevents_binding_and_historical_reuse(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(_registry(), tmp_path / "cache.sqlite")
    leader = await controller.resolve(_request("line-1", auth_scope="scope-a"))
    other = await controller.resolve(_request("line-2", auth_scope="scope-b"))
    assert leader.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    assert other.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    assert leader.binding_id != other.binding_id
    await controller.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=_identity("line-1"),
            result={"items": [{"title": "private"}]},
        )
    )
    hit = await controller.resolve(_request("line-3", auth_scope="scope-a"))
    miss = await controller.resolve(_request("line-4", auth_scope="scope-b"))
    assert hit.decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
    assert hit.provenance is not None
    assert hit.provenance.reuse_type == ReuseType.HISTORICAL
    assert miss.decision == ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT


@pytest.mark.anyio
async def test_leader_failure_and_lease_expiry_release_followers(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _registry(), tmp_path / "cache.sqlite", lease_seconds=0.1
    )
    leader = await controller.resolve(_request("line-1"))
    follower = await controller.resolve(_request("line-2"))
    await controller.fail(
        BindingFailureReport(
            binding_id=leader.binding_id or "",
            identity=_identity("line-1"),
            error_class="SearchError",
        )
    )
    fallback = await controller.poll(follower.binding_id or "", _identity("line-2"))
    assert fallback.decision == ReuseDecisionKind.EXECUTE_LOCALLY
    elected = await controller.resolve(_request("line-3"))
    assert elected.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    waiting = await controller.resolve(_request("line-4"))
    await asyncio.sleep(0.12)
    expired = await controller.poll(waiting.binding_id or "", _identity("line-4"))
    assert expired.decision == ReuseDecisionKind.EXECUTE_LOCALLY


@pytest.mark.anyio
async def test_historical_cache_survives_restart_but_binding_does_not(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cache.sqlite"
    first = WebReuseController(_registry(), path)
    leader = await first.resolve(_request("line-1"))
    await first.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=_identity("line-1"),
            result={"items": [{"title": "persisted"}]},
        )
    )
    restarted = WebReuseController(_registry(), path)
    historical = await restarted.resolve(_request("line-2"))
    assert historical.decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
    assert historical.provenance is not None
    assert historical.provenance.reuse_type == ReuseType.HISTORICAL

    running = await first.resolve(_request("line-3", query="not-persisted"))
    after_restart = await restarted.resolve(_request("line-4", query="not-persisted"))
    assert running.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    assert after_restart.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    assert running.binding_id != after_restart.binding_id


@pytest.mark.anyio
async def test_leader_ttl_cannot_extend_registry_freshness(tmp_path: Path) -> None:
    path = tmp_path / "cache.sqlite"
    registry = (_registry()[0].model_copy(update={"default_ttl_seconds": 1}),)
    controller = WebReuseController(registry, path)
    leader = await controller.resolve(_request("line-1"))
    await controller.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=_identity("line-1"),
            result={"items": [{"title": "short-lived"}]},
            ttl_seconds=60,
        )
    )

    with sqlite3.connect(path) as connection:
        created_at, freshness_deadline = connection.execute(
            "SELECT created_at, freshness_deadline FROM exact_results"
        ).fetchone()
    lifetime = datetime.fromisoformat(freshness_deadline) - datetime.fromisoformat(
        created_at
    )
    assert lifetime.total_seconds() == 1


@pytest.mark.anyio
async def test_terminal_bindings_are_garbage_collected(tmp_path: Path) -> None:
    controller = WebReuseController(
        _registry(), tmp_path / "cache.sqlite", lease_seconds=0.05
    )
    leader = await controller.resolve(_request("line-1"))
    await controller.publish(
        LeaderResultPublish(
            binding_id=leader.binding_id or "",
            identity=_identity("line-1"),
            result={"items": [{"title": "complete"}]},
            cacheable=False,
        )
    )
    assert len((await controller.snapshot())["bindings"]) == 1

    await asyncio.sleep(0.06)
    assert (await controller.snapshot())["bindings"] == []


@pytest.mark.anyio
async def test_unregistered_and_public_tools_fall_back_locally(tmp_path: Path) -> None:
    controller = WebReuseController(_registry(), tmp_path / "cache.sqlite")
    unregistered = _request("line-1").model_copy(update={"tool_name": "terminal"})
    assert (
        await controller.resolve(unregistered)
    ).decision == ReuseDecisionKind.EXECUTE_LOCALLY
    public = _request("line-2").model_copy(
        update={
            "scope": ReuseScope(
                tenant_id="tenant-1", auth_scope="anonymous", public_scope=True
            )
        }
    )
    assert (
        await controller.resolve(public)
    ).decision == ReuseDecisionKind.EXECUTE_LOCALLY


@pytest.mark.anyio
async def test_sensitive_result_fields_are_rejected(tmp_path: Path) -> None:
    controller = WebReuseController(_registry(), tmp_path / "cache.sqlite")
    leader = await controller.resolve(_request("line-1"))
    with pytest.raises(ReuseConflict, match="sensitive field"):
        await controller.publish(
            LeaderResultPublish(
                binding_id=leader.binding_id or "",
                identity=_identity("line-1"),
                result={"items": [{"title": "bad", "api_key": "secret"}]},
            )
        )

    text_leader = await controller.resolve(_request("line-2", query="text-secret"))
    with pytest.raises(ReuseConflict, match="sensitive text"):
        await controller.publish(
            LeaderResultPublish(
                binding_id=text_leader.binding_id or "",
                identity=_identity("line-2"),
                result={
                    "items": [
                        {
                            "title": "unsafe",
                            "content": "Authorization: Bearer abcdefghijklmnop",
                        }
                    ]
                },
            )
        )

    safe_leader = await controller.resolve(_request("line-3", query="author"))
    await controller.publish(
        LeaderResultPublish(
            binding_id=safe_leader.binding_id or "",
            identity=_identity("line-3"),
            result={"items": [{"author": "Alice"}]},
        )
    )
    assert (
        await controller.resolve(_request("line-3", query="author"))
    ).decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT


def test_scope_tenant_must_match_identity() -> None:
    with pytest.raises(ValueError, match="scope tenant"):
        ToolReuseResolveRequest.model_validate(
            {
                **_request("line-1").model_dump(mode="json"),
                "scope": {"tenant_id": "tenant-2", "auth_scope": "anonymous"},
            }
        )


@pytest.mark.anyio
async def test_phase3_semantic_historical_hit_is_auditable_and_exact_stays_first(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _semantic_registry(), tmp_path / "cache.sqlite"
    )
    source = _request(
        "line-1",
        query="flowpilot semantic scheduler design",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    leader = await controller.resolve(source)
    await controller.publish(
        LeaderResultPublish(
            protocol_version="flowpilot-phase3-reuse-v1",
            binding_id=leader.binding_id or "",
            identity=source.identity,
            result={"items": [{"title": "semantic"}]},
        )
    )

    exact = await controller.resolve(
        source.model_copy(update={"identity": _identity("line-2")})
    )
    assert exact.provenance is not None
    assert exact.provenance.match_kind == ReuseMatchKind.EXACT
    assert exact.semantic_match_id is None

    semantic = await controller.resolve(
        _request(
            "line-3",
            query="semantic scheduler design flowpilot",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    assert semantic.decision == ReuseDecisionKind.SYNC_WITH_REUSED_RESULT
    assert semantic.provenance is not None
    assert semantic.provenance.match_kind == ReuseMatchKind.SEMANTIC
    assert semantic.similarity_score is not None
    assert semantic.similarity_score >= 0.55
    assert semantic.semantic_match_id

    report = FalseReuseReport(
        semantic_match_id=semantic.semantic_match_id,
        tenant_id="tenant-1",
        reason="not_equivalent",
        evidence_digest=hashlib.sha256(b"human-label-17").hexdigest(),
        observed_at=datetime.now(UTC),
    )
    assert await controller.report_false_reuse(report) is False
    assert await controller.report_false_reuse(report) is True
    snapshot = await controller.snapshot()
    assert snapshot["semantic"]["counters"]["semantic_historical_matches"] >= 1
    assert snapshot["semantic"]["counters"]["false_reuse_reports"] == 1


@pytest.mark.anyio
async def test_phase3_hard_filters_threshold_and_phase1_opt_in_fail_closed(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _semantic_registry(threshold=0.9), tmp_path / "cache.sqlite"
    )
    source = _request(
        "line-1",
        query="flowpilot semantic scheduler design",
        auth_scope="scope-a",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    leader = await controller.resolve(source)
    await controller.publish(
        LeaderResultPublish(
            protocol_version="flowpilot-phase3-reuse-v1",
            binding_id=leader.binding_id or "",
            identity=source.identity,
            result={"items": [{"title": "source"}]},
        )
    )

    phase1 = await controller.resolve(
        _request("line-2", query="semantic scheduler design flowpilot")
    )
    wrong_auth = await controller.resolve(
        _request(
            "line-3",
            query="flowpilot semantic scheduler design",
            auth_scope="scope-b",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    below_threshold = await controller.resolve(
        _request(
            "line-4",
            query="semantic scheduler design flowpilot",
            auth_scope="scope-a",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    temporal = await controller.resolve(
        _request(
            "line-5",
            query="latest flowpilot semantic scheduler design",
            auth_scope="scope-a",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    assert all(
        item.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
        for item in (phase1, wrong_auth, below_threshold, temporal)
    )
    assert len(
        {item.binding_id for item in (phase1, wrong_auth, below_threshold, temporal)}
    ) == 4


@pytest.mark.anyio
async def test_phase3_semantic_inflight_progress_failure_and_retry(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _semantic_registry(), tmp_path / "cache.sqlite", lease_seconds=10
    )
    source = _request(
        "line-1",
        query="flowpilot semantic scheduler design",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    follower_request = _request(
        "line-2",
        query="semantic scheduler design flowpilot",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    leader = await controller.resolve(source)
    follower = await controller.resolve(follower_request, defer_allowed=True)
    assert follower.decision == ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
    assert follower.match_kind == ReuseMatchKind.SEMANTIC
    assert follower.semantic_match_id

    progress = LeaderProgressReport(
        binding_id=leader.binding_id or "",
        identity=source.identity,
        sequence=1,
        observed_at=datetime.now(UTC),
        estimated_remaining_ms=250,
    )
    assert await controller.progress(progress) is False
    assert await controller.progress(progress) is True
    waiting = await controller.poll_deferred(
        follower.binding_id or "", follower_request
    )
    assert waiting.leader_estimated_remaining_ms == 250

    await controller.fail(
        BindingFailureReport(
            protocol_version="flowpilot-phase3-reuse-v1",
            binding_id=leader.binding_id or "",
            identity=source.identity,
            error_class="SearchError",
        )
    )
    fallback = await controller.poll_deferred(
        follower.binding_id or "", follower_request
    )
    assert fallback.decision == ReuseDecisionKind.EXECUTE_LOCALLY
    retry = await controller.resolve(follower_request)
    assert retry.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    assert retry.binding_id != leader.binding_id


@pytest.mark.anyio
async def test_phase3_semantic_inflight_completion_preserves_follower_identity(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _semantic_registry(), tmp_path / "cache.sqlite"
    )
    source = _request(
        "line-1",
        query="flowpilot semantic scheduler design",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    follower_request = _request(
        "line-2",
        query="semantic scheduler design flowpilot",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    leader = await controller.resolve(source)
    follower = await controller.resolve(follower_request)
    await controller.publish(
        LeaderResultPublish(
            protocol_version="flowpilot-phase3-reuse-v1",
            binding_id=leader.binding_id or "",
            identity=source.identity,
            result={"items": [{"title": "inflight"}]},
        )
    )
    completed = await controller.poll(
        follower.binding_id or "", follower_request.identity
    )
    assert completed.provenance is not None
    assert completed.provenance.reuse_type == ReuseType.INFLIGHT
    assert completed.provenance.match_kind == ReuseMatchKind.SEMANTIC
    assert completed.descriptor_digest != leader.descriptor_digest
    assert completed.semantic_match_id == follower.semantic_match_id


@pytest.mark.anyio
async def test_phase3_concurrent_semantic_misses_atomically_choose_one_leader(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _semantic_registry(), tmp_path / "cache.sqlite"
    )
    left, right = await asyncio.gather(
        controller.resolve(
            _request(
                "line-1",
                query="flowpilot semantic scheduler design",
                protocol_version="flowpilot-phase3-reuse-v1",
            )
        ),
        controller.resolve(
            _request(
                "line-2",
                query="semantic scheduler design flowpilot",
                protocol_version="flowpilot-phase3-reuse-v1",
            )
        ),
    )
    assert {left.decision, right.decision} == {
        ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER,
        ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT,
    }
    assert left.binding_id == right.binding_id
    follower = (
        right
        if right.decision == ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT
        else left
    )
    assert follower.match_kind == ReuseMatchKind.SEMANTIC


@pytest.mark.anyio
async def test_phase3_stale_semantic_candidate_is_deleted_and_not_reused(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cache.sqlite"
    controller = WebReuseController(_semantic_registry(), path)
    source = _request(
        "line-1",
        query="flowpilot semantic scheduler design",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    leader = await controller.resolve(source)
    await controller.publish(
        LeaderResultPublish(
            protocol_version="flowpilot-phase3-reuse-v1",
            binding_id=leader.binding_id or "",
            identity=source.identity,
            result={"items": [{"title": "stale"}]},
        )
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE exact_results SET freshness_deadline=?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(),),
        )
    decision = await controller.resolve(
        _request(
            "line-2",
            query="semantic scheduler design flowpilot",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    assert decision.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    counters = (await controller.snapshot())["semantic"]["counters"]
    assert counters["semantic_historical_stale_rejections"] == 1


@pytest.mark.anyio
async def test_phase3_runtime_kill_switch_is_versioned_by_tool_and_tenant(
    tmp_path: Path,
) -> None:
    controller = WebReuseController(
        _semantic_registry(), tmp_path / "cache.sqlite"
    )
    source = _request(
        "line-1",
        query="flowpilot semantic scheduler design",
        protocol_version="flowpilot-phase3-reuse-v1",
    )
    leader = await controller.resolve(source)
    await controller.publish(
        LeaderResultPublish(
            protocol_version="flowpilot-phase3-reuse-v1",
            binding_id=leader.binding_id or "",
            identity=source.identity,
            result={"items": [{"title": "source"}]},
        )
    )
    disabled = await controller.update_semantic_policy(
        SemanticReusePolicyUpdate(
            version=1,
            expected_version=0,
            enabled=False,
            tool_name="web_search",
        )
    )
    assert disabled["disabled_tools"] == ["web_search"]
    tool_blocked = await controller.resolve(
        _request(
            "line-2",
            query="semantic scheduler design flowpilot",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    assert tool_blocked.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER

    await controller.update_semantic_policy(
        SemanticReusePolicyUpdate(
            version=2,
            expected_version=1,
            enabled=True,
            tool_name="web_search",
        )
    )
    await controller.update_semantic_policy(
        SemanticReusePolicyUpdate(
            version=3,
            expected_version=2,
            enabled=False,
            tenant_id="tenant-1",
        )
    )
    tenant_blocked = await controller.resolve(
        _request(
            "line-3",
            query="design semantic scheduler flowpilot",
            protocol_version="flowpilot-phase3-reuse-v1",
        )
    )
    assert tenant_blocked.decision == ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER
    with pytest.raises(ReuseConflict, match="expected semantic policy version 3"):
        await controller.update_semantic_policy(
            SemanticReusePolicyUpdate(
                version=2,
                expected_version=1,
                enabled=True,
                tenant_id="tenant-1",
            )
        )
