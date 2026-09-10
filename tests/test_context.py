from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from flowpilot.context import DCSConflict, DeferredContextManager
from flowpilot.protocol import (
    ContextDeltaAppend,
    ContextReconcileRequest,
    ContextSyncAck,
    ContextSyncBegin,
    DCSBarrierReason,
    DCSReference,
    DelegationPolicy,
    InternalContinuationRequest,
    RequestIdentity,
    ResultProvenance,
    ReuseDecisionKind,
    ReuseMatchKind,
    ReuseScope,
    ReuseType,
    ToolReuseDecision,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
)

BASE_DIGEST = "a" * 64
ENCRYPTION_KEY = Fernet.generate_key()


def _policy(**updates: object) -> DelegationPolicy:
    now = datetime.now(UTC)
    values: dict[str, object] = {
        "policy_version": 1,
        "expected_policy_version": 0,
        "lease_id": "lease-1",
        "job_id": "job-1",
        "line_id": "line-1",
        "context_epoch": 1,
        "base_context_cursor": "cursor-0",
        "base_context_digest": BASE_DIGEST,
        "issued_at": now - timedelta(seconds=1),
        "expires_at": now + timedelta(minutes=5),
        "allowed_tool_names": ("web_search",),
        "api_kind": "chat",
        "request_snapshot": {
            "model": "model-a",
            "messages": [{"role": "user", "content": "question"}],
            "stream": False,
        },
    }
    values.update(updates)
    return DelegationPolicy.model_validate(values)


def _reference(snapshot: dict[str, object]) -> DCSReference:
    return DCSReference(
        job_id="job-1",
        line_id="line-1",
        context_epoch=1,
        lease_id="lease-1",
        base_context_cursor=str(snapshot["base_context_cursor"]),
        delta_digest=str(snapshot["delta_digest"]),
    )


def _chat_batch(call_id: str = "call-1") -> tuple[dict[str, object], ...]:
    return (
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": "web_search",
                        "arguments": '{"query":"weather"}',
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": call_id,
            "content": '{"items":[{"title":"sunny"}]}',
        },
    )


async def _receipt(
    manager: DeferredContextManager,
    snapshot: dict[str, object],
    *,
    call_id: str,
    arguments: dict[str, object],
    result: dict[str, object],
    reuse_type: ReuseType = ReuseType.HISTORICAL,
    match_kind: ReuseMatchKind = ReuseMatchKind.EXACT,
    parent_llm_call_id: str = "llm-1",
) -> dict[str, str]:
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    request = ToolReuseResolveRequest(
        protocol_version=(
            "flowpilot-phase3-reuse-v2"
            if match_kind == ReuseMatchKind.SEMANTIC
            else "flowpilot-phase1-reuse-v2"
        ),
        identity=ToolReuseIdentity(
            job_id="job-1",
            line_id="line-1",
            tail_request_id="tail-1",
            llm_call_id=parent_llm_call_id,
            action_id=f"action-{call_id}",
            tool_call_id=call_id,
        ),
        tool_name="web_search",
        arguments=arguments,
        scope=ReuseScope(),
    )
    decision = ToolReuseDecision(
        protocol_version=(
            "flowpilot-phase3-reuse-v2"
            if match_kind == ReuseMatchKind.SEMANTIC
            else "flowpilot-phase1-reuse-v2"
        ),
        decision=ReuseDecisionKind.DEFER_WITH_CACHED_RESULT,
        descriptor_digest=hashlib.sha256(call_id.encode()).hexdigest(),
        result=result,
        provenance=ResultProvenance(
            reuse_type=reuse_type,
            match_kind=match_kind,
            observed_at=datetime.now(UTC),
            result_schema_version="1",
            source_query_digest=hashlib.sha256(b"query").hexdigest(),
            original_size=len(encoded),
            returned_size=len(encoded),
            truncation_policy="none",
            similarity_score=(0.97 if match_kind == ReuseMatchKind.SEMANTIC else None),
            semantic_match_id=(
                "semantic-match-1" if match_kind == ReuseMatchKind.SEMANTIC else None
            ),
        ),
    )
    return await manager.issue_resolution(_reference(snapshot), request, decision)


@pytest.mark.anyio
async def test_phase2_dcs_rejects_semantic_resolution(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY.decode())
    snapshot = await manager.grant(_policy())
    with pytest.raises(DCSConflict, match="exact reuse results only"):
        await _receipt(
            manager,
            snapshot,
            call_id="semantic-call",
            arguments={"query": "semantic scheduler design flowpilot"},
            result={"items": [{"title": "semantic"}]},
            match_kind=ReuseMatchKind.SEMANTIC,
        )


async def _append_exact(
    manager: DeferredContextManager,
    snapshot: dict[str, object],
    *,
    messages: tuple[dict[str, object], ...],
    call_ids: tuple[str, ...],
    arguments: tuple[dict[str, object], ...],
    results: tuple[dict[str, object], ...],
    reuse_type: ReuseType = ReuseType.HISTORICAL,
    parent_llm_call_id: str = "llm-1",
) -> dict[str, object]:
    receipts = [
        await _receipt(
            manager,
            snapshot,
            call_id=call_id,
            arguments=call_arguments,
            result=result,
            reuse_type=reuse_type,
            parent_llm_call_id=parent_llm_call_id,
        )
        for call_id, call_arguments, result in zip(
            call_ids, arguments, results, strict=True
        )
    ]
    provider_messages = [dict(item) for item in messages]
    if provider_messages[0].get("role") == "assistant":
        for item, receipt in zip(provider_messages[1:], receipts, strict=True):
            item["content"] = receipt["provider_content"]
    else:
        outputs = [
            item
            for item in provider_messages
            if item.get("type") == "function_call_output"
        ]
        for item, receipt in zip(outputs, receipts, strict=True):
            item["output"] = receipt["provider_content"]
    last_seq = snapshot["last_seq"]
    assert isinstance(last_seq, int)
    return await manager.append(
        ContextDeltaAppend(
            reference=_reference(snapshot),
            expected_last_seq=last_seq,
            parent_llm_call_id=parent_llm_call_id,
            messages=tuple(provider_messages),
            tool_call_ids=call_ids,
            resolution_receipts=tuple(item["resolution_receipt"] for item in receipts),
            result_digests=tuple(item["result_digest"] for item in receipts),
        )
    )


@pytest.mark.anyio
async def test_wal_restart_continuation_fragmented_sync_and_idempotent_ack(
    tmp_path: Path,
) -> None:
    wal = tmp_path / "dcs.sqlite"
    manager = DeferredContextManager(wal, ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    continuation = await manager.prepare_continuation(
        InternalContinuationRequest(
            reference=_reference(appended), parent_llm_call_id="llm-1"
        )
    )
    assert continuation["body"]["messages"][:2] == [
        {"role": "user", "content": "question"},
        _chat_batch()[0],
    ]
    assert "sunny" in continuation["body"]["messages"][2]["content"]
    assert (
        "FlowPilot reuse provenance" in continuation["body"]["messages"][2]["content"]
    )
    assert continuation["delta_seq"] == 2

    restarted = DeferredContextManager(wal, ENCRYPTION_KEY)
    reconciled = await restarted.reconcile(
        ContextReconcileRequest(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            context_cursor="cursor-0",
            context_digest=BASE_DIGEST,
        )
    )
    assert reconciled["status"] == "sync_required"

    first = await restarted.begin_sync(
        ContextSyncBegin(
            reference=_reference(appended),
            barrier_reason=DCSBarrierReason.TERMINAL_RESPONSE,
            max_messages=1,
            parent_llm_call_id="llm-final",
            barrier_messages=({"role": "assistant", "content": "done"},),
        )
    )
    assert (first["first_seq"], first["last_seq"], first["more"]) == (1, 2, True)
    repeated = await restarted.begin_sync(
        ContextSyncBegin(
            reference=_reference(appended),
            barrier_reason=DCSBarrierReason.TERMINAL_RESPONSE,
            max_messages=1,
            parent_llm_call_id="llm-final",
            barrier_messages=({"role": "assistant", "content": "done"},),
        )
    )
    assert repeated == first
    wal_bytes = wal.read_bytes()
    for marker in (b"question", b"weather", b"sunny", b"done"):
        assert marker not in wal_bytes
    ack = ContextSyncAck(
        reference=_reference(appended),
        first_seq=1,
        last_seq=2,
        delta_digest=first["delta_digest"],
        new_context_cursor="cursor-1",
        new_context_digest="b" * 64,
    )
    accepted = await restarted.acknowledge(ack)
    assert accepted["pending_message_count"] == 1
    assert (await restarted.acknowledge(ack))["duplicate"] is True

    next_reference = _reference(appended).model_copy(
        update={
            "base_context_cursor": "cursor-1",
            "delta_digest": first["wal_delta_digest"],
        }
    )
    second = await restarted.next_sync_chunk(next_reference)
    assert second["messages"][-1] == {"role": "assistant", "content": "done"}
    assert second["parent_llm_call_id"] == "llm-final"
    assert second["pending_local_tool_call_ids"] == []
    final = await restarted.acknowledge(
        ContextSyncAck(
            reference=next_reference,
            first_seq=second["first_seq"],
            last_seq=second["last_seq"],
            delta_digest=second["delta_digest"],
            new_context_cursor="cursor-3",
            new_context_digest="c" * 64,
        )
    )
    assert final["state"] == "acked"
    assert final["pending_message_count"] == 0
    assert (await restarted.acknowledge(ack))["duplicate"] is True
    in_sync = await restarted.reconcile(
        ContextReconcileRequest(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            context_cursor="cursor-3",
            context_digest="c" * 64,
        )
    )
    assert in_sync["status"] == "in_sync"
    agent_ahead = await restarted.reconcile(
        ContextReconcileRequest(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            context_cursor="cursor-after-local-tool",
            context_digest="d" * 64,
        )
    )
    assert agent_ahead == {
        "status": "agent_ahead_requires_new_delegation",
        "sync_required": False,
    }
    assert (await restarted.snapshot())["lines"][0]["state"] == "acked"
    with sqlite3.connect(wal) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM dcs_continuations").fetchone()[0]
            == 0
        )


@pytest.mark.anyio
async def test_local_tool_barrier_rejects_incomplete_call_arguments(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    with pytest.raises(DCSConflict, match="valid JSON"):
        await manager.begin_sync(
            ContextSyncBegin(
                reference=_reference(granted),
                barrier_reason=DCSBarrierReason.LOCAL_TOOL,
                parent_llm_call_id="llm-1",
                barrier_messages=(
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "type": "function",
                                "function": {
                                    "name": "terminal",
                                    "arguments": "{",
                                },
                            }
                        ],
                    },
                ),
                pending_local_tool_call_ids=("call-1",),
            )
        )


@pytest.mark.anyio
async def test_conflicting_ack_marks_context_diverged(tmp_path: Path) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
        reuse_type=ReuseType.INFLIGHT,
    )
    chunk = await manager.begin_sync(
        ContextSyncBegin(
            reference=_reference(appended),
            barrier_reason=DCSBarrierReason.LOCAL_TOOL,
            parent_llm_call_id="llm-2",
            barrier_messages=(
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "local-1",
                            "type": "function",
                            "function": {"name": "terminal", "arguments": "{}"},
                        }
                    ],
                },
            ),
            pending_local_tool_call_ids=("local-1",),
        )
    )
    assert chunk["messages"][-1] == {
        "role": "assistant",
        "tool_calls": [
            {
                "id": "local-1",
                "type": "function",
                "function": {"name": "terminal", "arguments": "{}"},
            }
        ],
    }
    assert chunk["parent_llm_call_id"] == "llm-2"
    assert chunk["pending_local_tool_call_ids"] == ["local-1"]
    assert b"local-1" not in (tmp_path / "dcs.sqlite").read_bytes()
    with pytest.raises(DCSConflict, match="ACK range or digest"):
        await manager.acknowledge(
            ContextSyncAck(
                reference=_reference(appended),
                first_seq=chunk["first_seq"],
                last_seq=chunk["last_seq"],
                delta_digest="f" * 64,
                new_context_cursor="cursor-bad",
                new_context_digest="f" * 64,
            )
        )
    assert (await manager.snapshot())["lines"][0]["state"] == "diverged"


@pytest.mark.anyio
async def test_ack_from_stale_delegation_marks_context_diverged(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    chunk = await manager.begin_sync(
        ContextSyncBegin(
            reference=_reference(appended),
            barrier_reason=DCSBarrierReason.FAILURE,
        )
    )
    stale = _reference(appended).model_copy(update={"lease_id": "stale-lease"})
    with pytest.raises(DCSConflict, match="ACK reference"):
        await manager.acknowledge(
            ContextSyncAck(
                reference=stale,
                first_seq=chunk["first_seq"],
                last_seq=chunk["last_seq"],
                delta_digest=chunk["delta_digest"],
                new_context_cursor="cursor-stale",
                new_context_digest="f" * 64,
            )
        )
    assert (await manager.snapshot())["lines"][0]["state"] == "diverged"


@pytest.mark.anyio
async def test_ack_cannot_split_provider_batch_and_divergence_is_fail_closed(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    await manager.begin_sync(
        ContextSyncBegin(
            reference=_reference(appended),
            barrier_reason=DCSBarrierReason.FAILURE,
        )
    )
    with sqlite3.connect(tmp_path / "dcs.sqlite") as connection:
        first_digest = connection.execute(
            "SELECT digest FROM dcs_messages WHERE seq=1"
        ).fetchone()[0]
    with pytest.raises(DCSConflict, match="splits a provider message batch"):
        await manager.acknowledge(
            ContextSyncAck(
                reference=_reference(appended),
                first_seq=1,
                last_seq=1,
                delta_digest=first_digest,
                new_context_cursor="cursor-partial",
                new_context_digest="b" * 64,
            )
        )
    reconciled = await manager.reconcile(
        ContextReconcileRequest(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            context_cursor="cursor-0",
            context_digest=BASE_DIGEST,
        )
    )
    assert reconciled == {"status": "context_diverged", "sync_required": False}
    with pytest.raises(DCSConflict, match="context diverged"):
        await manager.authorize_llm_request(
            RequestIdentity(
                job_id="job-1",
                line_id="line-1",
                request_id=f"request-{'tail-2'}",
                attempt=1,
                conversation_id=f"conversation-{'line-1'}",
                tail_request_id="tail-2",
                llm_call_id="llm-agent",
                expected_tail_version=1,
                context_epoch=1,
                context_sequence=0,
                base_context_cursor="cursor-0",
                context_digest=BASE_DIGEST,
            )
        )
    with pytest.raises(DCSConflict, match="pending context"):
        await manager.grant(
            _policy(
                policy_version=2,
                expected_policy_version=1,
                lease_id="lease-2",
                context_epoch=2,
            )
        )


@pytest.mark.anyio
async def test_capacity_and_lease_force_early_sync(tmp_path: Path) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy(max_messages=2))
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    assert appended["state"] == "syncing"
    assert appended["barrier_reason"] == "capacity"
    capacity_chunk = await manager.next_sync_chunk(_reference(appended))
    assert capacity_chunk["messages"]
    with pytest.raises(DCSConflict, match="syncing"):
        await manager.prepare_continuation(
            InternalContinuationRequest(
                reference=_reference(appended), parent_llm_call_id="llm-1"
            )
        )

    expired = DeferredContextManager(tmp_path / "expired.sqlite", ENCRYPTION_KEY)
    now = datetime.now(UTC)
    policy = _policy(
        issued_at=now - timedelta(seconds=2),
        expires_at=now + timedelta(milliseconds=20),
    )
    grant = await expired.grant(policy)
    await asyncio.sleep(0.03)
    with pytest.raises(DCSConflict, match="lease expired"):
        await expired.validate_reference(_reference(grant))
    line = (await expired.snapshot())["lines"][0]
    assert (line["state"], line["barrier_reason"]) == (
        "aborted",
        "lease_expired",
    )


@pytest.mark.anyio
async def test_append_rejects_batches_that_exceed_delta_capacity(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy(max_messages=1))
    with pytest.raises(DCSConflict, match="message capacity"):
        await _append_exact(
            manager,
            granted,
            messages=_chat_batch(),
            call_ids=("call-1",),
            arguments=({"query": "weather"},),
            results=({"items": [{"title": "sunny"}]},),
        )
    line = (await manager.snapshot())["lines"][0]
    assert line["state"] == "open"
    assert line["pending_message_count"] == 0


@pytest.mark.anyio
async def test_barrier_capacity_rejection_leaves_wal_unchanged(tmp_path: Path) -> None:
    wal = tmp_path / "dcs.sqlite"
    manager = DeferredContextManager(wal, ENCRYPTION_KEY)
    granted = await manager.grant(_policy(max_messages=3))
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    before = (await manager.snapshot())["lines"][0]
    with pytest.raises(DCSConflict, match="exceed delta capacity"):
        await manager.begin_sync(
            ContextSyncBegin(
                reference=_reference(appended),
                barrier_reason=DCSBarrierReason.LOCAL_TOOL,
                parent_llm_call_id="llm-2",
                barrier_messages=(
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "local-1",
                                "type": "function",
                                "function": {
                                    "name": "terminal",
                                    "arguments": "{}",
                                },
                            },
                            {
                                "id": "resolved-1",
                                "type": "function",
                                "function": {
                                    "name": "web_search",
                                    "arguments": '{"query":"cached"}',
                                },
                            },
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": "resolved-1",
                        "content": "cached",
                    },
                ),
                pending_local_tool_call_ids=("local-1",),
            )
        )
    after = (await manager.snapshot())["lines"][0]
    assert after == before
    with sqlite3.connect(wal) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM dcs_messages").fetchone()[0] == 2
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "barrier_message",
    [
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "not terminal"}],
        },
        {
            "type": "function_call",
            "call_id": "call-1",
            "name": "web_search",
            "arguments": "{}",
        },
        {"type": "message", "role": "assistant", "content": []},
        {
            "type": "message",
            "role": "assistant",
            "content": [{"type": "input_text", "text": "wrong direction"}],
        },
    ],
)
async def test_terminal_responses_barrier_rejects_non_assistant_items(
    tmp_path: Path, barrier_message: dict[str, object]
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(
        _policy(
            api_kind="responses",
            request_snapshot={
                "model": "model-a",
                "input": [{"role": "user", "content": "question"}],
            },
        )
    )
    with pytest.raises(DCSConflict, match="terminal Responses barrier"):
        await manager.begin_sync(
            ContextSyncBegin(
                reference=_reference(granted),
                barrier_reason=DCSBarrierReason.TERMINAL_RESPONSE,
                parent_llm_call_id="llm-final",
                barrier_messages=(barrier_message,),
            )
        )


@pytest.mark.anyio
async def test_responses_barriers_accept_openhands_provider_items(
    tmp_path: Path,
) -> None:
    local = DeferredContextManager(tmp_path / "local.sqlite", ENCRYPTION_KEY)
    local_grant = await local.grant(
        _policy(
            api_kind="responses",
            request_snapshot={"model": "model-a", "input": []},
        )
    )
    local_chunk = await local.begin_sync(
        ContextSyncBegin(
            reference=_reference(local_grant),
            barrier_reason=DCSBarrierReason.LOCAL_TOOL,
            parent_llm_call_id="llm-local",
            barrier_messages=(
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "I will search."}],
                },
                {
                    "type": "function_call",
                    "id": "fc_call-local",
                    "call_id": "call-local",
                    "name": "web_search",
                    "arguments": '{"query":"flowpilot"}',
                },
            ),
            pending_local_tool_call_ids=("call-local",),
        )
    )
    assert [item["type"] for item in local_chunk["messages"]] == [
        "message",
        "function_call",
    ]

    terminal = DeferredContextManager(tmp_path / "terminal.sqlite", ENCRYPTION_KEY)
    terminal_grant = await terminal.grant(
        _policy(
            api_kind="responses",
            request_snapshot={"model": "model-a", "input": []},
        )
    )
    terminal_chunk = await terminal.begin_sync(
        ContextSyncBegin(
            reference=_reference(terminal_grant),
            barrier_reason=DCSBarrierReason.TERMINAL_RESPONSE,
            parent_llm_call_id="llm-final",
            barrier_messages=(
                {
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": [{"type": "summary_text", "text": "Done"}],
                },
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Final answer"}],
                },
            ),
        )
    )
    assert [item["type"] for item in terminal_chunk["messages"]] == [
        "reasoning",
        "message",
    ]


@pytest.mark.anyio
async def test_delta_ttl_forces_sync_before_lease_expiry(tmp_path: Path) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy(delta_ttl_seconds=0.02))
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    await asyncio.sleep(0.03)
    with pytest.raises(DCSConflict, match="TTL expired"):
        await manager.prepare_continuation(
            InternalContinuationRequest(
                reference=_reference(appended), parent_llm_call_id="llm-1"
            )
        )
    line = (await manager.snapshot())["lines"][0]
    assert (line["state"], line["barrier_reason"]) == ("syncing", "ttl")
    ttl_chunk = await manager.next_sync_chunk(_reference(appended))
    assert ttl_chunk["messages"]


@pytest.mark.anyio
async def test_continuation_limit_preserves_recoverable_wal(tmp_path: Path) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy(max_internal_continuations=1))
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    request = InternalContinuationRequest(
        reference=_reference(appended), parent_llm_call_id="llm-1"
    )
    first = await manager.prepare_continuation(request)
    repeated = await manager.prepare_continuation(request)
    assert repeated["body"] == first["body"]
    assert repeated["idempotent"] is True
    assert (await manager.snapshot())["lines"][0]["internal_continuation_count"] == 1
    second = await _append_exact(
        manager,
        appended,
        messages=_chat_batch("call-2"),
        call_ids=("call-2",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "cloudy"}]},),
        parent_llm_call_id="llm-2",
    )
    with pytest.raises(DCSConflict, match="continuation limit"):
        await manager.prepare_continuation(
            InternalContinuationRequest(
                reference=_reference(second), parent_llm_call_id="llm-2"
            )
        )
    chunk = await manager.next_sync_chunk(_reference(second))
    assert chunk["barrier_reason"] == "capacity"
    assert chunk["messages"]


@pytest.mark.anyio
async def test_continuation_rejects_a_stale_parent(tmp_path: Path) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    appended = await _append_exact(
        manager,
        granted,
        messages=_chat_batch(),
        call_ids=("call-1",),
        arguments=({"query": "weather"},),
        results=({"items": [{"title": "sunny"}]},),
    )
    with pytest.raises(DCSConflict, match="latest deferred batch"):
        await manager.prepare_continuation(
            InternalContinuationRequest(
                reference=_reference(appended), parent_llm_call_id="llm-stale"
            )
        )


@pytest.mark.anyio
async def test_responses_batch_preserves_call_order_in_continuation(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(
        _policy(
            api_kind="responses",
            request_snapshot={
                "model": "model-a",
                "input": [{"role": "user", "content": "question"}],
            },
        )
    )
    messages: tuple[dict[str, object], ...] = (
        {
            "type": "function_call",
            "call_id": "call-1",
            "name": "web_search",
            "arguments": '{"query":"one"}',
        },
        {
            "type": "function_call",
            "call_id": "call-2",
            "name": "web_search",
            "arguments": '{"query":"two"}',
        },
        {
            "type": "function_call_output",
            "call_id": "call-1",
            "output": '{"items":[{"title":"one"}]}',
        },
        {
            "type": "function_call_output",
            "call_id": "call-2",
            "output": '{"items":[{"title":"two"}]}',
        },
    )
    appended = await _append_exact(
        manager,
        granted,
        messages=messages,
        call_ids=("call-1", "call-2"),
        arguments=({"query": "one"}, {"query": "two"}),
        results=(
            {"items": [{"title": "one"}]},
            {"items": [{"title": "two"}]},
        ),
        reuse_type=ReuseType.INFLIGHT,
    )
    continuation = await manager.prepare_continuation(
        InternalContinuationRequest(
            reference=_reference(appended), parent_llm_call_id="llm-1"
        )
    )
    assert continuation["body"]["input"][1:3] == list(messages[:2])
    assert all(
        "FlowPilot reuse provenance" in item["output"]
        for item in continuation["body"]["input"][3:]
    )


@pytest.mark.anyio
async def test_parallel_batch_identity_and_agent_fork_are_rejected(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    with pytest.raises(DCSConflict, match="identity/order"):
        await manager.append(
            ContextDeltaAppend(
                reference=_reference(granted),
                expected_last_seq=0,
                parent_llm_call_id="llm-1",
                messages=_chat_batch("call-other"),
                tool_call_ids=("call-1",),
                resolution_receipts=("invalid",),
                result_digests=("f" * 64,),
            )
        )
    fork = await manager.reconcile(
        ContextReconcileRequest(
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            context_cursor="forked-cursor",
            context_digest="b" * 64,
        )
    )
    assert fork["status"] == "context_diverged"


@pytest.mark.anyio
async def test_append_requires_and_consumes_exact_resolution_receipt(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    messages = _chat_batch()
    result: dict[str, object] = {"items": [{"title": "sunny"}]}
    receipt = await _receipt(
        manager,
        granted,
        call_id="call-1",
        arguments={"query": "weather"},
        result=result,
    )
    base = {
        "reference": _reference(granted),
        "expected_last_seq": 0,
        "parent_llm_call_id": "llm-1",
        "messages": (
            messages[0],
            {
                **messages[1],
                "content": receipt["provider_content"],
            },
        ),
        "tool_call_ids": ("call-1",),
        "result_digests": (receipt["result_digest"],),
    }
    with pytest.raises(DCSConflict, match="unknown resolution receipt"):
        await manager.append(
            ContextDeltaAppend(
                **base,
                resolution_receipts=("not-issued",),
            )
        )
    tampered = (
        base["messages"][0],
        {
            "role": "tool",
            "tool_call_id": "call-1",
            "content": '{"items":[{"title":"stormy"}]}',
        },
    )
    with pytest.raises(DCSConflict, match="provider messages conflict"):
        await manager.append(
            ContextDeltaAppend(
                **{**base, "messages": tampered},
                resolution_receipts=(receipt["resolution_receipt"],),
            )
        )
    appended = await manager.append(
        ContextDeltaAppend(
            **base,
            resolution_receipts=(receipt["resolution_receipt"],),
        )
    )
    assert appended["reuse_kinds"] == ["exact_historical"]
    with sqlite3.connect(tmp_path / "dcs.sqlite") as connection:
        consumed_at = connection.execute(
            "SELECT consumed_at FROM dcs_resolutions"
        ).fetchone()[0]
    assert consumed_at is not None


@pytest.mark.anyio
async def test_append_accepts_equivalent_chat_text_content_block(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    receipt = await _receipt(
        manager,
        granted,
        call_id="call-1",
        arguments={"query": "weather"},
        result={"items": [{"title": "sunny"}]},
    )
    messages = [dict(item) for item in _chat_batch()]
    messages[1]["content"] = [{"type": "text", "text": receipt["provider_content"]}]
    updated = await manager.append(
        ContextDeltaAppend(
            reference=_reference(granted),
            expected_last_seq=0,
            parent_llm_call_id="llm-1",
            messages=tuple(messages),
            tool_call_ids=("call-1",),
            resolution_receipts=(receipt["resolution_receipt"],),
            result_digests=(receipt["result_digest"],),
        )
    )
    assert updated["last_seq"] == 2


def test_legacy_tenant_wal_is_rejected(tmp_path: Path) -> None:
    wal = tmp_path / "legacy.sqlite"
    policy_json = json.dumps(
        _policy().model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    message_json = json.dumps(
        {"role": "assistant", "content": "legacy-result-private"},
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(wal) as connection:
        connection.executescript(
            """
            CREATE TABLE dcs_lines (
                tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
                line_id TEXT NOT NULL, context_epoch INTEGER NOT NULL,
                policy_version INTEGER NOT NULL, policy_json TEXT NOT NULL,
                lease_id TEXT, lease_expires_at TEXT,
                base_context_cursor TEXT NOT NULL,
                base_context_digest TEXT NOT NULL, state TEXT NOT NULL,
                last_seq INTEGER NOT NULL, last_digest TEXT NOT NULL,
                pending_count INTEGER NOT NULL, pending_bytes INTEGER NOT NULL,
                internal_continuations INTEGER NOT NULL, barrier_reason TEXT,
                last_ack_first_seq INTEGER, last_ack_last_seq INTEGER,
                last_ack_digest TEXT, last_ack_cursor TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, job_id, line_id)
            );
            CREATE TABLE dcs_messages (
                tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
                line_id TEXT NOT NULL, context_epoch INTEGER NOT NULL,
                seq INTEGER NOT NULL, previous_digest TEXT NOT NULL,
                digest TEXT NOT NULL, message_json TEXT NOT NULL,
                message_bytes INTEGER NOT NULL,
                parent_llm_call_id TEXT NOT NULL, reuse_kind TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, job_id, line_id, context_epoch, seq)
            );
            PRAGMA user_version = 1;
            """
        )
        connection.execute(
            "INSERT INTO dcs_lines VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "tenant-1",
                "job-1",
                "line-1",
                1,
                1,
                policy_json,
                "lease-1",
                _policy().expires_at.isoformat(),
                "cursor-0",
                BASE_DIGEST,
                "open",
                1,
                "b" * 64,
                1,
                len(message_json.encode()),
                0,
                None,
                None,
                None,
                None,
                None,
                datetime.now(UTC).isoformat(),
            ),
        )
        connection.execute(
            "INSERT INTO dcs_messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "tenant-1",
                "job-1",
                "line-1",
                1,
                1,
                BASE_DIGEST,
                "b" * 64,
                message_json,
                len(message_json.encode()),
                "llm-legacy",
                "exact_historical",
                datetime.now(UTC).isoformat(),
            ),
        )

    with pytest.raises(DCSConflict, match="unsupported DCS WAL schema version 1"):
        DeferredContextManager(wal, ENCRYPTION_KEY)


def test_unversioned_legacy_tenant_wal_is_rejected(tmp_path: Path) -> None:
    wal = tmp_path / "legacy-unversioned.sqlite"
    with sqlite3.connect(wal) as connection:
        connection.execute(
            "CREATE TABLE dcs_lines (tenant_id TEXT NOT NULL, job_id TEXT NOT NULL)"
        )

    with pytest.raises(
        DCSConflict, match="legacy DCS WAL schema requires explicit migration"
    ):
        DeferredContextManager(wal, ENCRYPTION_KEY)


def test_v2_wal_is_rejected(tmp_path: Path) -> None:
    wal = tmp_path / "v2.sqlite"
    DeferredContextManager(wal, ENCRYPTION_KEY)
    with sqlite3.connect(wal) as connection:
        connection.execute("DROP TABLE dcs_acks")
        connection.execute("PRAGMA user_version = 2")

    with pytest.raises(DCSConflict, match="unsupported DCS WAL schema version 2"):
        DeferredContextManager(wal, ENCRYPTION_KEY)


@pytest.mark.anyio
async def test_active_empty_delegation_blocks_agent_until_release(
    tmp_path: Path,
) -> None:
    manager = DeferredContextManager(tmp_path / "dcs.sqlite", ENCRYPTION_KEY)
    granted = await manager.grant(_policy())
    identity = RequestIdentity(
        job_id="job-1",
        line_id="line-1",
        request_id=f"request-{'tail-1'}",
        attempt=1,
        conversation_id=f"conversation-{'line-1'}",
        tail_request_id="tail-1",
        llm_call_id="llm-1",
        expected_tail_version=0,
        context_epoch=1,
        context_sequence=0,
        base_context_cursor="cursor-0",
        context_digest=BASE_DIGEST,
    )

    with pytest.raises(DCSConflict, match="active DCS writer"):
        await manager.authorize_llm_request(identity)

    released = await manager.release(_reference(granted))
    assert released["state"] == "aborted"
    await manager.authorize_llm_request(identity)
