from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from flowpilot.protocol import (
    DependencyUpdate,
    JobRegistration,
    LineFinish,
    LineRegistration,
    RequestIdentity,
    ToolEventKind,
    ToolTelemetryEvent,
)


class FrontierConflict(ValueError):
    """The caller attempted an invalid state transition."""


class StaleEvent(FrontierConflict):
    """An event no longer belongs to the current line tail."""


_CONTEXT_HISTORY_LIMIT = 64


@dataclass(slots=True)
class ToolCallSummary:
    tool_call_id: str
    tool_name: str
    arguments_digest: str | None
    arguments_bytes: int | None


@dataclass(slots=True)
class LineTail:
    tenant_id: str
    job_id: str
    line_id: str
    context_epoch: int
    context_sequence: int
    base_context_cursor: str
    context_digest: str
    version: int = 0
    state: str = "EMPTY"
    tail_request_id: str | None = None
    llm_call_id: str | None = None
    model: str | None = None
    instance_id: str | None = None
    response_id: str | None = None
    tool_calls: list[ToolCallSummary] = field(default_factory=list)
    dependencies: tuple[str, ...] = ()
    dependency_version: int = 0
    deadline: datetime | None = None
    weight: float = 1.0
    last_error: str | None = None
    context_history: list[tuple[int, str, str]] = field(default_factory=list)
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def identity_key(self) -> tuple[str, str, str]:
        return self.tenant_id, self.job_id, self.line_id


@dataclass(slots=True)
class JobState:
    tenant_id: str
    job_id: str
    default_slo_ms: int | None
    lines: set[str] = field(default_factory=set)


@dataclass(slots=True)
class ToolExecutionState:
    action_id: str
    tool_call_id: str
    tool_name: str
    execution_attempt: int
    last_sequence: int
    terminal_kind: ToolEventKind | None = None
    events: dict[str, ToolTelemetryEvent] = field(default_factory=dict)


@dataclass(slots=True)
class RequestBackup:
    version: int
    state: str
    tail_request_id: str | None
    llm_call_id: str | None
    model: str | None
    instance_id: str | None
    response_id: str | None
    tool_calls: list[ToolCallSummary]
    context_sequence: int
    base_context_cursor: str
    context_digest: str
    context_history: list[tuple[int, str, str]]
    last_error: str | None


class LineTailFrontier:
    """Process-local phase 0 tail table with atomic line/dependency updates."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._jobs: dict[tuple[str, str], JobState] = {}
        self._lines: dict[tuple[str, str, str], LineTail] = {}
        self._request_backups: dict[tuple[str, str, str], RequestBackup] = {}
        self._aborted_retries: dict[tuple[str, str, str], tuple[str, str, int]] = {}
        self._tool_executions: dict[
            tuple[str, str, str, str, int], ToolExecutionState
        ] = {}

    async def register_job(self, registration: JobRegistration) -> JobState:
        async with self._lock:
            key = (registration.tenant_id, registration.job_id)
            existing = self._jobs.get(key)
            if existing is not None:
                if existing.default_slo_ms != registration.default_slo_ms:
                    raise FrontierConflict(
                        "job registration conflicts with existing job"
                    )
                return existing
            state = JobState(
                tenant_id=registration.tenant_id,
                job_id=registration.job_id,
                default_slo_ms=registration.default_slo_ms,
            )
            self._jobs[key] = state
            return state

    async def register_line(self, registration: LineRegistration) -> LineTail:
        async with self._lock:
            job_key = (registration.tenant_id, registration.job_id)
            job = self._jobs.get(job_key)
            if job is None:
                raise FrontierConflict("job must be registered before a line")
            key = (*job_key, registration.line_id)
            if key in self._lines:
                raise FrontierConflict("line is already registered")
            tail = LineTail(
                tenant_id=registration.tenant_id,
                job_id=registration.job_id,
                line_id=registration.line_id,
                context_epoch=registration.context_epoch,
                context_sequence=registration.context_sequence,
                base_context_cursor=registration.base_context_cursor,
                context_digest=registration.context_digest,
                deadline=registration.deadline,
                weight=registration.weight,
                context_history=[
                    (
                        registration.context_sequence,
                        registration.base_context_cursor,
                        registration.context_digest,
                    )
                ],
            )
            self._lines[key] = tail
            job.lines.add(registration.line_id)
            return tail

    async def begin_request(self, identity: RequestIdentity, model: str) -> LineTail:
        async with self._lock:
            tail = self._require_line(
                identity.tenant_id, identity.job_id, identity.line_id
            )
            key = tail.identity_key()
            retry = self._aborted_retries.get(key)
            retry_matches = retry == (
                identity.tail_request_id,
                identity.llm_call_id,
                identity.expected_tail_version,
            )
            if tail.version != identity.expected_tail_version and not retry_matches:
                raise FrontierConflict(
                    f"tail version {tail.version} does not match expected "
                    f"{identity.expected_tail_version}"
                )
            if tail.state not in {"EMPTY", "NEXT_READY"}:
                raise FrontierConflict(
                    f"line tail is not ready for a new request: {tail.state}"
                )
            if tail.context_epoch != identity.context_epoch:
                raise FrontierConflict(
                    "context epoch transition is unsupported in phase 0; "
                    "restart/resume requires a new line"
                )
            self._validate_context_transition(tail, identity)
            self._aborted_retries.pop(key, None)
            self._prune_tool_executions(key)
            self._request_backups[key] = RequestBackup(
                version=tail.version,
                state=tail.state,
                tail_request_id=tail.tail_request_id,
                llm_call_id=tail.llm_call_id,
                model=tail.model,
                instance_id=tail.instance_id,
                response_id=tail.response_id,
                tool_calls=list(tail.tool_calls),
                context_sequence=tail.context_sequence,
                base_context_cursor=tail.base_context_cursor,
                context_digest=tail.context_digest,
                context_history=list(tail.context_history),
                last_error=tail.last_error,
            )
            tail.version += 1
            tail.state = "LLM_RUNNING"
            tail.tail_request_id = identity.tail_request_id
            tail.llm_call_id = identity.llm_call_id
            tail.model = model
            tail.instance_id = None
            tail.response_id = None
            tail.tool_calls = []
            tail.context_sequence = identity.context_sequence
            tail.base_context_cursor = identity.base_context_cursor
            tail.context_digest = identity.context_digest
            if not tail.context_history or tail.context_history[-1] != (
                identity.context_sequence,
                identity.base_context_cursor,
                identity.context_digest,
            ):
                tail.context_history.append(
                    (
                        identity.context_sequence,
                        identity.base_context_cursor,
                        identity.context_digest,
                    )
                )
                del tail.context_history[:-_CONTEXT_HISTORY_LIMIT]
            tail.last_error = None
            tail.updated_at = datetime.now(UTC)
            return tail

    async def abort_request(self, identity: RequestIdentity, error: str) -> int:
        """Roll back an uncommitted request and return the authoritative version."""
        async with self._lock:
            tail = self._require_current(identity)
            backup = self._request_backups.pop(tail.identity_key(), None)
            if backup is None:
                raise StaleEvent("request is no longer rollbackable")
            visible_version = tail.version
            tail.version = backup.version
            tail.state = backup.state
            tail.tail_request_id = backup.tail_request_id
            tail.llm_call_id = backup.llm_call_id
            tail.model = backup.model
            tail.instance_id = backup.instance_id
            tail.response_id = backup.response_id
            tail.tool_calls = backup.tool_calls
            tail.context_sequence = backup.context_sequence
            tail.base_context_cursor = backup.base_context_cursor
            tail.context_digest = backup.context_digest
            tail.context_history = backup.context_history
            tail.last_error = error
            tail.updated_at = datetime.now(UTC)
            self._aborted_retries[tail.identity_key()] = (
                identity.tail_request_id,
                identity.llm_call_id,
                visible_version,
            )
            return tail.version

    async def mark_routed(
        self,
        identity: RequestIdentity,
        instance_id: str,
    ) -> LineTail:
        async with self._lock:
            tail = self._require_current(identity)
            tail.instance_id = instance_id
            tail.updated_at = datetime.now(UTC)
            return tail

    async def complete_response(
        self,
        identity: RequestIdentity,
        *,
        response_id: str | None,
        tool_calls: list[ToolCallSummary],
        error: str | None = None,
    ) -> bool:
        async with self._lock:
            try:
                tail = self._require_current(identity)
            except StaleEvent:
                return False
            tail.response_id = response_id
            tail.tool_calls = tool_calls
            tail.last_error = error
            tail.state = "NEXT_READY"
            self._request_backups.pop(tail.identity_key(), None)
            tail.updated_at = datetime.now(UTC)
            return True

    async def record_tool_event(
        self, event: ToolTelemetryEvent
    ) -> tuple[LineTail, bool]:
        async with self._lock:
            tail = self._require_line(event.tenant_id, event.job_id, event.line_id)
            if tail.tail_request_id != event.tail_request_id:
                raise StaleEvent(
                    "tool event does not belong to the current tail request"
                )
            if tail.llm_call_id != event.llm_call_id:
                raise StaleEvent("tool event does not belong to the current LLM call")
            call = next(
                (
                    item
                    for item in tail.tool_calls
                    if item.tool_call_id == event.tool_call_id
                ),
                None,
            )
            if call is None:
                raise FrontierConflict("tool event references an unknown tool call")
            if call.tool_name != event.tool_name:
                raise FrontierConflict("tool event name does not match tool call")
            key = (
                event.tenant_id,
                event.job_id,
                event.line_id,
                event.tool_call_id,
                event.execution_attempt,
            )
            execution = self._tool_executions.get(key)
            if execution is None:
                if event.sequence != 1 or event.event_kind not in {
                    ToolEventKind.START,
                    ToolEventKind.BLOCKED,
                }:
                    raise FrontierConflict(
                        "tool lifecycle must begin with sequence 1 START or BLOCKED"
                    )
                execution = ToolExecutionState(
                    action_id=event.action_id,
                    tool_call_id=event.tool_call_id,
                    tool_name=event.tool_name,
                    execution_attempt=event.execution_attempt,
                    last_sequence=event.sequence,
                    terminal_kind=(
                        ToolEventKind.BLOCKED
                        if event.event_kind == ToolEventKind.BLOCKED
                        else None
                    ),
                )
                execution.events[event.event_id] = event
                self._tool_executions[key] = execution
                tail.updated_at = datetime.now(UTC)
                return tail, False
            existing_event = execution.events.get(event.event_id)
            if existing_event is not None:
                if existing_event != event:
                    raise FrontierConflict(
                        "tool event_id conflicts with an earlier payload"
                    )
                return tail, True
            if execution.action_id != event.action_id:
                raise FrontierConflict("tool event action_id conflicts with execution")
            if execution.terminal_kind is not None:
                raise FrontierConflict(
                    "tool execution already terminated as "
                    f"{execution.terminal_kind.value}"
                )
            if event.sequence != execution.last_sequence + 1:
                raise FrontierConflict("tool event sequence is not contiguous")
            if event.event_kind not in {
                ToolEventKind.FINISH,
                ToolEventKind.FAIL,
                ToolEventKind.CANCEL,
            }:
                raise FrontierConflict(
                    "START must terminate with FINISH, FAIL, or CANCEL"
                )
            execution.last_sequence = event.sequence
            execution.terminal_kind = event.event_kind
            execution.events[event.event_id] = event
            tail.updated_at = datetime.now(UTC)
            return tail, False

    async def replace_dependencies(self, update: DependencyUpdate) -> LineTail:
        async with self._lock:
            job_key = (update.tenant_id, update.job_id)
            tail = self._require_line(*job_key, update.line_id)
            if update.version != tail.dependency_version + 1:
                raise FrontierConflict(
                    f"dependency version {tail.dependency_version} does not precede "
                    f"{update.version}"
                )
            job = self._jobs[job_key]
            unknown = set(update.prerequisite_line_ids) - job.lines
            if unknown:
                raise FrontierConflict(f"unknown prerequisite lines: {sorted(unknown)}")
            if update.line_id in update.prerequisite_line_ids:
                raise FrontierConflict("a line cannot depend on itself")
            proposed = {
                line_id: item.dependencies
                for line_id, item in self._line_items(job_key)
            }
            proposed[update.line_id] = update.prerequisite_line_ids
            if _has_cycle(proposed):
                raise FrontierConflict("dependency update would create a cycle")
            tail.dependencies = update.prerequisite_line_ids
            tail.dependency_version = update.version
            tail.updated_at = datetime.now(UTC)
            return tail

    async def finish_line(self, event: LineFinish) -> tuple[LineTail, tuple[str, ...]]:
        async with self._lock:
            tail = self._require_line(event.tenant_id, event.job_id, event.line_id)
            if tail.version != event.expected_tail_version:
                raise FrontierConflict(
                    f"tail version {tail.version} does not match expected "
                    f"{event.expected_tail_version}"
                )
            if tail.state not in {"EMPTY", "NEXT_READY"}:
                raise FrontierConflict(
                    f"line tail cannot finish from state {tail.state}"
                )
            if event.tail_request_id is not None:
                if tail.tail_request_id != event.tail_request_id:
                    raise StaleEvent("line finish does not belong to the current tail")
            tail.state = "FINISHED"
            tail.updated_at = datetime.now(UTC)
            key = tail.identity_key()
            self._request_backups.pop(key, None)
            self._aborted_retries.pop(key, None)
            self._prune_tool_executions(key)

            released: list[str] = []
            for line_id, dependent in self._line_items((event.tenant_id, event.job_id)):
                if event.line_id not in dependent.dependencies:
                    continue
                dependent.dependencies = tuple(
                    item for item in dependent.dependencies if item != event.line_id
                )
                dependent.dependency_version += 1
                dependent.updated_at = datetime.now(UTC)
                released.append(line_id)
            job_key = (event.tenant_id, event.job_id)
            job = self._jobs[job_key]
            self._lines.pop(key, None)
            job.lines.discard(event.line_id)
            if not job.lines:
                self._jobs.pop(job_key, None)
            return tail, tuple(sorted(released))

    async def snapshot(self, tenant_id: str, job_id: str) -> dict[str, Any]:
        async with self._lock:
            job = self._jobs.get((tenant_id, job_id))
            if job is None:
                raise FrontierConflict("unknown job")
            lines = list(self._line_items((tenant_id, job_id)))
            dependents: dict[str, int] = {line_id: 0 for line_id in job.lines}
            for _line_id, tail in lines:
                for prerequisite in tail.dependencies:
                    dependents[prerequisite] += 1
            return {
                "tenant_id": tenant_id,
                "job_id": job_id,
                "default_slo_ms": job.default_slo_ms,
                "lines": [
                    _tail_to_dict(tail, dependents[tail.line_id]) for _, tail in lines
                ],
            }

    async def require_line(self, tenant_id: str, job_id: str, line_id: str) -> LineTail:
        async with self._lock:
            return self._require_line(tenant_id, job_id, line_id)

    def _line_items(
        self,
        job_key: tuple[str, str],
    ) -> list[tuple[str, LineTail]]:
        return [
            (line_id, tail)
            for (tenant_id, job_id, line_id), tail in self._lines.items()
            if (tenant_id, job_id) == job_key
        ]

    def _require_line(self, tenant_id: str, job_id: str, line_id: str) -> LineTail:
        try:
            return self._lines[(tenant_id, job_id, line_id)]
        except KeyError as exc:
            raise FrontierConflict("unknown line") from exc

    def _require_current(self, identity: RequestIdentity) -> LineTail:
        tail = self._require_line(identity.tenant_id, identity.job_id, identity.line_id)
        backup = self._request_backups.get(tail.identity_key())
        if (
            backup is None
            or tail.version != backup.version + 1
            or tail.tail_request_id != identity.tail_request_id
            or tail.llm_call_id != identity.llm_call_id
        ):
            raise StaleEvent("event does not belong to the current tail")
        return tail

    def _prune_tool_executions(self, line_key: tuple[str, str, str]) -> None:
        stale = [key for key in self._tool_executions if key[:3] == line_key]
        for key in stale:
            self._tool_executions.pop(key, None)

    @staticmethod
    def _validate_context_transition(tail: LineTail, identity: RequestIdentity) -> None:
        if identity.context_sequence < tail.context_sequence:
            raise FrontierConflict("context sequence would move backwards")
        if identity.context_sequence == tail.context_sequence and (
            identity.base_context_cursor != tail.base_context_cursor
            or identity.context_digest != tail.context_digest
        ):
            raise FrontierConflict(
                "context cursor or digest conflicts at the same sequence"
            )
        if identity.origin == "scheduler_delegated":
            if identity.context_sequence <= tail.context_sequence:
                raise FrontierConflict(
                    "delegated context sequence must advance the current context"
                )
            return
        if any(
            cursor == identity.base_context_cursor and digest != identity.context_digest
            for _sequence, cursor, digest in tail.context_history
        ):
            raise FrontierConflict(
                "context digest conflicts with the previously observed base cursor"
            )


def _has_cycle(graph: dict[str, tuple[str, ...]]) -> bool:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        if any(visit(child) for child in graph.get(node, ())):
            return True
        visiting.remove(node)
        visited.add(node)
        return False

    return any(visit(node) for node in graph)


def _tail_to_dict(tail: LineTail, blocking_count: int) -> dict[str, Any]:
    return {
        "tenant_id": tail.tenant_id,
        "job_id": tail.job_id,
        "line_id": tail.line_id,
        "context_epoch": tail.context_epoch,
        "context_sequence": tail.context_sequence,
        "base_context_cursor": tail.base_context_cursor,
        "context_digest": tail.context_digest,
        "version": tail.version,
        "state": tail.state,
        "tail_request_id": tail.tail_request_id,
        "llm_call_id": tail.llm_call_id,
        "model": tail.model,
        "instance_id": tail.instance_id,
        "response_id": tail.response_id,
        "tool_calls": [
            {
                "tool_call_id": item.tool_call_id,
                "tool_name": item.tool_name,
                "arguments_digest": item.arguments_digest,
                "arguments_bytes": item.arguments_bytes,
            }
            for item in tail.tool_calls
        ],
        "dependencies": list(tail.dependencies),
        "dependency_version": tail.dependency_version,
        "blocking_line_count": blocking_count,
        "deadline": tail.deadline.isoformat() if tail.deadline else None,
        "weight": tail.weight,
        "last_error": tail.last_error,
        "updated_at": tail.updated_at.isoformat(),
    }
