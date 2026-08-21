from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier, ToolCallSummary
from flowpilot.protocol import (
    DependencyUpdate,
    JobRegistration,
    LineFinish,
    LineRegistration,
    RequestIdentity,
    ToolClass,
    ToolEventKind,
    ToolTelemetryEvent,
)


def _digest(char: str = "a") -> str:
    return char * 64


def _identity(
    expected: int,
    *,
    line: str = "line-1",
    call: str = "call-1",
    context_sequence: int = 1,
) -> RequestIdentity:
    return RequestIdentity(
        tenant_id="tenant-1",
        job_id="job-1",
        line_id=line,
        tail_request_id=f"tail-{call}",
        llm_call_id=call,
        expected_tail_version=expected,
        context_epoch=1,
        context_sequence=context_sequence,
        base_context_cursor="cursor-1",
        context_digest=_digest(),
    )


@pytest.mark.anyio
async def test_tail_replacement_is_atomic_and_stale_response_is_ignored() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )

    first = _identity(0)
    await frontier.begin_request(first, "model-a")
    assert await frontier.complete_response(
        first, response_id="response-1", tool_calls=[]
    )
    second = _identity(1, call="call-2")
    await frontier.begin_request(second, "model-a")
    assert not await frontier.complete_response(
        first, response_id="late", tool_calls=[]
    )
    assert await frontier.complete_response(
        second, response_id="response-2", tool_calls=[]
    )
    snapshot = await frontier.snapshot("tenant-1", "job-1")
    line = snapshot["lines"][0]
    assert line["version"] == 2
    assert line["tail_request_id"] == "tail-call-2"
    assert line["response_id"] == "response-2"


@pytest.mark.anyio
async def test_dependency_updates_are_versioned_and_cycle_checked() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    for line_id in ("line-1", "line-2"):
        await frontier.register_line(
            LineRegistration(
                tenant_id="tenant-1",
                job_id="job-1",
                line_id=line_id,
                context_epoch=1,
                base_context_cursor="cursor-0",
                context_digest=_digest(),
            )
        )
    await frontier.replace_dependencies(
        DependencyUpdate(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-2",
            version=1,
            prerequisite_line_ids=("line-1",),
        )
    )
    with pytest.raises(FrontierConflict, match="cycle"):
        await frontier.replace_dependencies(
            DependencyUpdate(
                tenant_id="tenant-1",
                job_id="job-1",
                line_id="line-1",
                version=1,
                prerequisite_line_ids=("line-2",),
            )
        )
    with pytest.raises(FrontierConflict, match="dependency version"):
        await frontier.replace_dependencies(
            DependencyUpdate(
                tenant_id="tenant-1",
                job_id="job-1",
                line_id="line-2",
                version=1,
                prerequisite_line_ids=(),
            )
        )
    snapshot = await frontier.snapshot("tenant-1", "job-1")
    line = next(item for item in snapshot["lines"] if item["line_id"] == "line-1")
    assert line["blocking_line_count"] == 1


@pytest.mark.anyio
async def test_finishing_a_line_releases_current_dependents() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    for line_id in ("line-1", "line-2"):
        await frontier.register_line(
            LineRegistration(
                tenant_id="tenant-1",
                job_id="job-1",
                line_id=line_id,
                context_epoch=1,
                base_context_cursor="cursor-0",
                context_digest=_digest(),
            )
        )
    await frontier.replace_dependencies(
        DependencyUpdate(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-2",
            version=1,
            prerequisite_line_ids=("line-1",),
        )
    )
    finished, released = await frontier.finish_line(
        LineFinish(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            expected_tail_version=0,
        )
    )
    assert finished.state == "FINISHED"
    assert released == ("line-2",)
    snapshot = await frontier.snapshot("tenant-1", "job-1")
    line_2 = next(item for item in snapshot["lines"] if item["line_id"] == "line-2")
    assert line_2["dependencies"] == []
    assert line_2["dependency_version"] == 2
    assert all(item["line_id"] != "line-1" for item in snapshot["lines"])


@pytest.mark.anyio
async def test_frontier_history_and_finished_state_are_bounded() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    for version in range(80):
        identity = _identity(
            version,
            call=f"call-{version}",
            context_sequence=version + 1,
        ).model_copy(update={"base_context_cursor": f"cursor-{version + 1}"})
        await frontier.begin_request(identity, "model-a")
        await frontier.complete_response(identity, response_id=None, tool_calls=[])

    key = ("tenant-1", "job-1", "line-1")
    assert len(frontier._lines[key].context_history) == 64
    finished, _released = await frontier.finish_line(
        LineFinish(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            expected_tail_version=80,
        )
    )
    assert finished.state == "FINISHED"
    assert key not in frontier._lines
    assert ("tenant-1", "job-1") not in frontier._jobs
    with pytest.raises(FrontierConflict, match="unknown job"):
        await frontier.snapshot("tenant-1", "job-1")


@pytest.mark.anyio
async def test_failed_request_rolls_back_version_and_can_retry() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    identity = _identity(0)
    await frontier.begin_request(identity, "model-a")
    assert await frontier.abort_request(identity, "ConnectError") == 0
    retry = await frontier.begin_request(identity, "model-a")
    assert retry.version == 1


@pytest.mark.anyio
async def test_context_rejects_same_cursor_conflict_and_backward_move() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest("a"),
        )
    )
    first = _identity(0, context_sequence=1)
    await frontier.begin_request(first, "model-a")
    await frontier.complete_response(first, response_id="r1", tool_calls=[])
    conflict = _identity(1, call="call-2", context_sequence=1).model_copy(
        update={"context_digest": _digest("b")}
    )
    with pytest.raises(FrontierConflict, match="same sequence"):
        await frontier.begin_request(conflict, "model-a")
    backwards = _identity(1, call="call-3", context_sequence=0).model_copy(
        update={
            "base_context_cursor": "different-cursor",
            "context_digest": _digest("c"),
        }
    )
    with pytest.raises(FrontierConflict, match="move backwards"):
        await frontier.begin_request(backwards, "model-a")

    cursor_conflict = _identity(1, call="call-4", context_sequence=2).model_copy(
        update={"context_digest": _digest("d")}
    )
    with pytest.raises(FrontierConflict, match="previously observed base cursor"):
        await frontier.begin_request(cursor_conflict, "model-a")


@pytest.mark.anyio
async def test_tool_telemetry_is_idempotent_and_terminal_is_unique() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    identity = _identity(0)
    await frontier.begin_request(identity, "model-a")
    await frontier.complete_response(
        identity,
        response_id="r1",
        tool_calls=[ToolCallSummary("tc-1", "terminal", None, None)],
    )
    common: dict[str, Any] = dict(
        tenant_id="tenant-1",
        job_id="job-1",
        line_id="line-1",
        tail_request_id="tail-call-1",
        llm_call_id="call-1",
        action_id="action-1",
        tool_call_id="tc-1",
        tool_name="terminal",
        tool_class=ToolClass.NON_WEB,
        execution_attempt=1,
        observed_at=datetime.now(UTC),
    )
    start = ToolTelemetryEvent(
        **common, event_id="event-start", sequence=1, event_kind=ToolEventKind.START
    )
    _tail, duplicate = await frontier.record_tool_event(start)
    assert not duplicate
    _tail, duplicate = await frontier.record_tool_event(start)
    assert duplicate
    conflicting_start = start.model_copy(update={"observed_at": datetime.now(UTC)})
    with pytest.raises(FrontierConflict, match="event_id conflicts"):
        await frontier.record_tool_event(conflicting_start)
    blocked = ToolTelemetryEvent(
        **common,
        event_id="event-blocked",
        sequence=2,
        event_kind=ToolEventKind.BLOCKED,
        error_class="HookBlocked",
    )
    with pytest.raises(FrontierConflict, match="must terminate with"):
        await frontier.record_tool_event(blocked)
    finish = ToolTelemetryEvent(
        **common,
        event_id="event-finish",
        sequence=2,
        event_kind=ToolEventKind.FINISH,
        result_size_bytes=10,
        measured_latency_ms=1,
    )
    await frontier.record_tool_event(finish)
    fail = ToolTelemetryEvent(
        **common,
        event_id="event-fail",
        sequence=3,
        event_kind=ToolEventKind.FAIL,
        error_class="RuntimeError",
    )
    with pytest.raises(FrontierConflict, match="already terminated"):
        await frontier.record_tool_event(fail)


@pytest.mark.anyio
async def test_blocked_tool_event_is_terminal_without_start() -> None:
    frontier = LineTailFrontier()
    await frontier.register_job(JobRegistration(tenant_id="tenant-1", job_id="job-1"))
    await frontier.register_line(
        LineRegistration(
            tenant_id="tenant-1",
            job_id="job-1",
            line_id="line-1",
            context_epoch=1,
            base_context_cursor="cursor-0",
            context_digest=_digest(),
        )
    )
    identity = _identity(0)
    await frontier.begin_request(identity, "model-a")
    await frontier.complete_response(
        identity,
        response_id="r1",
        tool_calls=[ToolCallSummary("tc-1", "terminal", None, None)],
    )
    common: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "job_id": "job-1",
        "line_id": "line-1",
        "tail_request_id": "tail-call-1",
        "llm_call_id": "call-1",
        "action_id": "action-1",
        "tool_call_id": "tc-1",
        "tool_name": "terminal",
        "tool_class": ToolClass.NON_WEB,
        "execution_attempt": 1,
        "observed_at": datetime.now(UTC),
    }
    blocked = ToolTelemetryEvent(
        **common,
        event_id="event-blocked",
        sequence=1,
        event_kind=ToolEventKind.BLOCKED,
        error_class="HookBlocked",
    )
    _tail, duplicate = await frontier.record_tool_event(blocked)
    assert not duplicate
    finish = ToolTelemetryEvent(
        **common,
        event_id="event-finish",
        sequence=2,
        event_kind=ToolEventKind.FINISH,
        result_size_bytes=1,
        measured_latency_ms=1,
    )
    with pytest.raises(FrontierConflict, match="already terminated"):
        await frontier.record_tool_event(finish)
