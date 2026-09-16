from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from flowpilot.identity import (
    IdentityConflict,
    Namespace,
    validate_line_parent,
    validate_namespace,
    validate_request_line,
)
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


class LinePhase:
    EMPTY = "EMPTY"
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    READY = "READY"
    TERMINAL = "TERMINAL"


@dataclass(slots=True, frozen=True)
class ToolCallSummary:
    tool_call_id: str
    tool_name: str
    arguments_digest: str | None
    arguments_bytes: int | None


@dataclass(slots=True)
class LineTail:
    """The bounded frontier record; detailed facts live in external stores."""

    job_id: str
    line_id: str
    context_epoch: int
    base_context_cursor: str
    version: int = 0
    phase: str = LinePhase.EMPTY
    tail_request_id: str | None = None
    delta_ref: str | None = None
    delegation_ref: str | None = None

    def identity_key(self) -> tuple[str, str]:
        return self.job_id, self.line_id

    @property
    def state(self) -> str:
        """Read-only projection used by the frontier snapshot API."""
        return self.phase


@dataclass(slots=True)
class JobState:
    job_id: str
    default_slo_ms: int | None
    workflow_started_at: datetime
    deadline: datetime | None
    root_conversation_id: str | None = None
    deployment_id: str | None = None
    namespace_id: str | None = None
    finished_at: datetime | None = None
    lines: set[str] = field(default_factory=set)


@dataclass(slots=True)
class RequestRecord:
    tail_request_id: str
    request_id: str
    llm_call_id: str
    attempt: int
    model: str
    gateway_received_at: datetime
    conversation_id: str | None = None
    parent_conversation_id: str | None = None
    parent_line_id: str | None = None
    spawn_id: str | None = None
    instance_id: str | None = None
    response_id: str | None = None
    tool_calls: tuple[ToolCallSummary, ...] = ()


@dataclass(slots=True, frozen=True)
class LineMetadata:
    deadline: datetime | None
    weight: float
    conversation_id: str | None
    parent_conversation_id: str | None
    parent_line_id: str | None
    spawn_id: str | None
    task_id: str | None
    agent_id: str | None
    parent_action_id: str | None


@dataclass(slots=True)
class ContextState:
    sequence: int
    digest: str
    history: list[tuple[int, str, str]] = field(default_factory=list)


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
    phase: str
    tail_request_id: str | None
    base_context_cursor: str
    context: ContextState
    request_record: RequestRecord | None


_CONTEXT_HISTORY_LIMIT = 64


class LineTailFrontier:
    """Process-local Phase 0 frontier with separate fact stores."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._jobs: dict[str, JobState] = {}
        self._lines: dict[tuple[str, str], LineTail] = {}
        self._contexts: dict[tuple[str, str], ContextState] = {}
        self._line_metadata: dict[tuple[str, str], LineMetadata] = {}
        self._requests: dict[tuple[str, str], RequestRecord] = {}
        self._request_backups: dict[tuple[str, str], RequestBackup] = {}
        self._aborted_retries: dict[
            tuple[str, str], tuple[str, str, str, int, int]
        ] = {}
        self._job_aliases: dict[tuple[str, str | None, str], str] = {}
        self._dependencies: dict[tuple[str, str], tuple[str, ...]] = {}
        self._dependency_versions: dict[tuple[str, str], int] = {}
        self._tool_executions: dict[
            tuple[str, str, str, str, int], ToolExecutionState
        ] = {}
        # ``event_id`` is scoped to a line/tail request.  The protocol's
        # idempotency tuple includes the execution identity, so the same UUID
        # on another line must not be treated as a conflicting global event.
        # Keep a bounded receipt window independent of execution pruning: a
        # late duplicate after a completed/replaced tail remains idempotent,
        # while the frontier cannot grow without bound.
        self._tool_event_ids: dict[tuple[str, str, int, str], ToolTelemetryEvent] = {}
        self._tool_event_order: deque[tuple[str, str, int, str]] = deque()
        self._tool_event_receipt_limit = 16_384
        self._reuse_receipt_times: dict[tuple[str, str, int, str], datetime] = {}
        self._reuse_receipt_pins: set[str] = set()

    async def register_job(self, registration: JobRegistration) -> JobState:
        async with self._lock:
            key = registration.job_id
            existing = self._jobs.get(key)
            if existing is not None:
                if (
                    existing.default_slo_ms != registration.default_slo_ms
                    or existing.root_conversation_id
                    != registration.root_conversation_id
                    or existing.deployment_id != registration.deployment_id
                    or existing.namespace_id != registration.namespace_id
                ):
                    raise FrontierConflict(
                        "job registration conflicts with existing job"
                    )
                return existing
            if registration.root_conversation_id is not None:
                alias_key = (
                    registration.root_conversation_id,
                    registration.deployment_id,
                    registration.namespace_id or "",
                )
                prior_job = self._job_aliases.get(alias_key)
                if prior_job is not None and prior_job != registration.job_id:
                    raise FrontierConflict(
                        "root conversation is already bound to another job"
                    )
                self._job_aliases[alias_key] = registration.job_id
            state = JobState(
                registration.job_id,
                registration.default_slo_ms,
                registration.workflow_started_at,
                registration.deadline,
                registration.root_conversation_id,
                registration.deployment_id,
                registration.namespace_id,
            )
            self._jobs[key] = state
            return state

    async def register_line(self, registration: LineRegistration) -> LineTail:
        async with self._lock:
            job_key = registration.job_id
            if job_key not in self._jobs:
                raise FrontierConflict("job must be registered before a line")
            job = self._jobs[job_key]
            if (
                job.root_conversation_id is not None
                and registration.parent_line_id is None
                and registration.conversation_id != job.root_conversation_id
            ):
                raise FrontierConflict("root line conversation does not match the job")
            key = (job_key, registration.line_id)
            if key in self._lines:
                existing = self._line_metadata[key]
                if existing != LineMetadata(
                    registration.deadline,
                    registration.weight,
                    registration.conversation_id,
                    registration.parent_conversation_id,
                    registration.parent_line_id,
                    registration.spawn_id,
                    registration.task_id,
                    registration.agent_id,
                    registration.parent_action_id,
                ):
                    raise FrontierConflict(
                        "line registration conflicts with existing line"
                    )
                return self._lines[key]
            if registration.parent_line_id is not None:
                parent_key = (job_key, registration.parent_line_id)
                if parent_key not in self._lines:
                    raise FrontierConflict(
                        "parent line must belong to the same registered job"
                    )
                parent_metadata = self._line_metadata[parent_key]
                try:
                    validate_line_parent(
                        registration,
                        parent_conversation_id=parent_metadata.conversation_id,
                    )
                except IdentityConflict as exc:
                    raise FrontierConflict(str(exc)) from exc
            tail = LineTail(
                job_id=registration.job_id,
                line_id=registration.line_id,
                context_epoch=registration.context_epoch,
                base_context_cursor=registration.base_context_cursor,
            )
            self._lines[key] = tail
            self._contexts[key] = ContextState(
                registration.context_sequence,
                registration.context_digest,
                [
                    (
                        registration.context_sequence,
                        registration.base_context_cursor,
                        registration.context_digest,
                    )
                ],
            )
            self._line_metadata[key] = LineMetadata(
                registration.deadline,
                registration.weight,
                registration.conversation_id,
                registration.parent_conversation_id,
                registration.parent_line_id,
                registration.spawn_id,
                registration.task_id,
                registration.agent_id,
                registration.parent_action_id,
            )
            self._dependencies[key] = ()
            self._dependency_versions[key] = 0
            self._jobs[job_key].lines.add(registration.line_id)
            return tail

    async def begin_request(self, identity: RequestIdentity, model: str) -> LineTail:
        async with self._lock:
            tail = self._require_line(identity.job_id, identity.line_id)
            key = tail.identity_key()
            retry = self._aborted_retries.get(key)
            retry_matches = retry is not None and (
                retry[0] == identity.request_id
                and retry[1] == identity.tail_request_id
                and retry[3] == identity.expected_tail_version
                and retry[2] != identity.llm_call_id
                and identity.attempt > retry[4]
            )
            if tail.version != identity.expected_tail_version and not retry_matches:
                raise FrontierConflict(
                    f"tail version {tail.version} does not match expected "
                    f"{identity.expected_tail_version}"
                )
            can_continue_delegated = (
                tail.phase == LinePhase.BLOCKED
                and identity.origin == "scheduler_delegated"
                and not self._dependencies[key]
            )
            can_continue_from_agent_history = (
                tail.phase == LinePhase.BLOCKED
                and identity.origin == "agent"
                and not self._dependencies[key]
                and identity.context_sequence > self._contexts[key].sequence
            )
            if tail.phase not in {LinePhase.EMPTY, LinePhase.READY} and not (
                can_continue_delegated or can_continue_from_agent_history
            ):
                raise FrontierConflict(
                    f"line tail is not ready for a new request: {tail.phase}"
                )
            context = self._contexts[key]
            job = self._jobs[identity.job_id]
            try:
                validate_namespace(
                    identity, Namespace(job.deployment_id, job.namespace_id)
                )
            except IdentityConflict as exc:
                raise FrontierConflict(str(exc)) from exc
            if tail.context_epoch != identity.context_epoch:
                raise FrontierConflict(
                    "context epoch transition requires a new line in phase 0"
                )
            self._validate_context_transition(tail, context, identity)
            metadata = self._line_metadata[key]
            try:
                validate_request_line(
                    identity,
                    conversation_id=metadata.conversation_id,
                    parent_conversation_id=metadata.parent_conversation_id,
                    parent_line_id=metadata.parent_line_id,
                    spawn_id=metadata.spawn_id,
                )
            except IdentityConflict as exc:
                raise FrontierConflict(str(exc)) from exc
            self._aborted_retries.pop(key, None)
            self._request_backups[key] = RequestBackup(
                tail.version,
                tail.phase,
                tail.tail_request_id,
                tail.base_context_cursor,
                ContextState(
                    context.sequence,
                    context.digest,
                    list(context.history),
                ),
                self._requests.get(key),
            )
            tail.version += 1
            tail.phase = LinePhase.ACTIVE
            tail.tail_request_id = identity.tail_request_id
            tail.base_context_cursor = identity.base_context_cursor
            history = list(context.history)
            marker = (
                identity.context_sequence,
                identity.base_context_cursor,
                identity.context_digest,
            )
            if not history or history[-1] != marker:
                history.append(marker)
            self._contexts[key] = ContextState(
                identity.context_sequence,
                identity.context_digest,
                history[-_CONTEXT_HISTORY_LIMIT:],
            )
            assert identity.tail_request_id is not None
            self._requests[key] = RequestRecord(
                identity.tail_request_id,
                identity.request_id,
                identity.llm_call_id,
                identity.attempt,
                model,
                datetime.now(UTC),
                identity.conversation_id,
                identity.parent_conversation_id,
                identity.parent_line_id,
                identity.spawn_id,
            )
            return tail

    async def abort_request(self, identity: RequestIdentity, _error: str) -> int:
        async with self._lock:
            terminal = self._require_line(identity.job_id, identity.line_id)
            if terminal.phase == LinePhase.TERMINAL:
                return terminal.version
            tail = self._require_current(identity)
            key = tail.identity_key()
            backup = self._request_backups.pop(key, None)
            if backup is None:
                raise StaleEvent("request is no longer rollbackable")
            visible_version = tail.version
            tail.version = backup.version
            tail.phase = backup.phase
            tail.tail_request_id = backup.tail_request_id
            tail.base_context_cursor = backup.base_context_cursor
            self._contexts[key] = backup.context
            if backup.request_record is None:
                self._requests.pop(key, None)
            else:
                self._requests[key] = backup.request_record
            assert identity.tail_request_id is not None
            self._aborted_retries[key] = (
                identity.request_id,
                identity.tail_request_id,
                identity.llm_call_id,
                visible_version,
                identity.attempt,
            )
            return tail.version

    async def mark_routed(
        self, identity: RequestIdentity, instance_id: str
    ) -> LineTail:
        async with self._lock:
            tail = self._require_current(identity)
            self._requests[tail.identity_key()].instance_id = instance_id
            return tail

    async def complete_response(
        self,
        identity: RequestIdentity,
        *,
        response_id: str | None,
        tool_calls: list[ToolCallSummary],
    ) -> int | None:
        async with self._lock:
            try:
                tail = self._require_current(identity)
            except StaleEvent:
                return None
            request = self._requests[tail.identity_key()]
            request.response_id = response_id
            request.tool_calls = tuple(tool_calls)
            tail.phase = self._resolved_phase(tail.identity_key())
            self._request_backups.pop(tail.identity_key(), None)
            self._prune_tool_executions(
                tail.identity_key(), keep_tail_request_id=tail.tail_request_id
            )
            return tail.version

    async def record_tool_event(
        self, event: ToolTelemetryEvent
    ) -> tuple[LineTail, bool]:
        async with self._lock:
            tail = self._require_line(event.job_id, event.line_id)
            previous = self._tool_event_ids.get(_tool_event_key(event))
            if previous is not None:
                if previous != event:
                    raise FrontierConflict(
                        "tool event_id conflicts with an earlier payload"
                    )
                return tail, True
            key = tail.identity_key()
            request = self._requests.get(key)
            if request is None or tail.tail_request_id != event.tail_request_id:
                raise StaleEvent(
                    "tool event does not belong to the current tail request"
                )
            if tail.context_epoch != event.context_epoch:
                raise StaleEvent("tool event context epoch does not match the line")
            if request.llm_call_id != event.llm_call_id:
                raise StaleEvent("tool event does not belong to the current LLM call")
            if event.request_id != request.request_id:
                raise StaleEvent("tool event does not belong to the current request")
            if event.attempt != request.attempt:
                raise StaleEvent(
                    "tool event attempt does not match the current request"
                )
            if event.conversation_id != request.conversation_id:
                raise StaleEvent("tool event conversation does not match the line")
            call = next(
                (
                    item
                    for item in request.tool_calls
                    if item.tool_call_id == event.tool_call_id
                ),
                None,
            )
            if call is None:
                raise FrontierConflict("tool event references an unknown tool call")
            if call.tool_name != event.tool_name:
                raise FrontierConflict("tool event name does not match tool call")
            event_key = _tool_event_key(event)
            prior_event = self._tool_event_ids.get(event_key)
            if prior_event is not None:
                if prior_event != event:
                    raise FrontierConflict(
                        "tool event_id conflicts with an earlier payload"
                    )
                return tail, True
            execution_key = (
                *key,
                event.tail_request_id,
                event.tool_call_id,
                event.execution_attempt,
            )
            execution = self._tool_executions.get(execution_key)
            if execution is None:
                prior_attempts = [
                    attempt
                    for (
                        *candidate_line,
                        tail_request_id,
                        tool_call_id,
                        attempt,
                    ) in self._tool_executions
                    if tuple(candidate_line) == key
                    and tail_request_id == event.tail_request_id
                    and tool_call_id == event.tool_call_id
                ]
                if prior_attempts and event.execution_attempt <= max(prior_attempts):
                    raise FrontierConflict(
                        "execution_attempt must increase for a retry"
                    )
                if event.sequence != 1 or event.event_kind not in {
                    ToolEventKind.START,
                    ToolEventKind.BLOCKED,
                }:
                    raise FrontierConflict(
                        "tool lifecycle must begin with sequence 1 START or BLOCKED"
                    )
                self._tool_executions[execution_key] = ToolExecutionState(
                    event.action_id,
                    event.tool_call_id,
                    event.tool_name,
                    event.execution_attempt,
                    event.sequence,
                    ToolEventKind.BLOCKED
                    if event.event_kind == ToolEventKind.BLOCKED
                    else None,
                    {event.event_id: event},
                )
                self._remember_tool_event(event_key, event)
                if event.binding_id:
                    self._reuse_receipt_pins.add(event.binding_id)
                    self._reuse_receipt_times.setdefault(
                        _tool_event_key(event), datetime.now(UTC)
                    )
                tail.phase = self._resolved_phase(key)
                return tail, False
            existing = execution.events.get(event.event_id)
            if existing is not None:
                if existing != event:
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
            self._remember_tool_event(event_key, event)
            if event.binding_id:
                self._reuse_receipt_times.setdefault(
                    _tool_event_key(event), datetime.now(UTC)
                )
            tail.phase = self._resolved_phase(key)
            return tail, False

    async def reuse_execution_receipt(
        self,
        binding_id: str,
        identity: Any,
        execution_attempt: int,
    ) -> tuple[ToolTelemetryEvent, ToolTelemetryEvent, datetime]:
        async with self._lock:
            key = (
                identity.job_id,
                identity.line_id,
                identity.tail_request_id,
                identity.tool_call_id,
                execution_attempt,
            )
            execution = self._tool_executions.get(key)
            if execution is None or execution.terminal_kind != ToolEventKind.FINISH:
                raise FrontierConflict(
                    "reuse origin requires accepted START and FINISH"
                )
            events = sorted(execution.events.values(), key=lambda e: e.sequence)
            if len(events) != 2 or any(e.binding_id != binding_id for e in events):
                raise FrontierConflict("reuse execution receipt does not match binding")
            return (
                events[0],
                events[1],
                self._reuse_receipt_times[_tool_event_key(events[1])],
            )

    async def accepted_tool_event(self, event: ToolTelemetryEvent) -> bool:
        async with self._lock:
            previous = self._tool_event_ids.get(_tool_event_key(event))
            if previous is not None and previous != event:
                raise FrontierConflict(
                    "tool event_id conflicts with an earlier payload"
                )
            return previous is not None

    async def release_reuse_receipt(self, binding_id: str) -> None:
        async with self._lock:
            self._reuse_receipt_pins.discard(binding_id)
            for key, execution in list(self._tool_executions.items()):
                if any(e.binding_id == binding_id for e in execution.events.values()):
                    for event in execution.events.values():
                        self._reuse_receipt_times.pop(_tool_event_key(event), None)
                    tail = self._lines.get(key[:2])
                    if tail is None or tail.tail_request_id != key[2]:
                        del self._tool_executions[key]

    async def reuse_namespace(
        self, job_id: str, line_id: str
    ) -> tuple[str | None, str | None]:
        async with self._lock:
            self._require_line(job_id, line_id)
            job = self._jobs[job_id]
            return job.deployment_id, job.namespace_id

    async def replace_dependencies(self, update: DependencyUpdate) -> LineTail:
        async with self._lock:
            job_key = update.job_id
            tail = self._require_line(job_key, update.line_id)
            key = tail.identity_key()
            current = self._dependency_versions[key]
            if update.version != current + 1:
                raise FrontierConflict(
                    f"dependency version {current} does not precede {update.version}"
                )
            job = self._jobs[job_key]
            unknown = set(update.prerequisite_line_ids) - job.lines
            if unknown:
                raise FrontierConflict(f"unknown prerequisite lines: {sorted(unknown)}")
            if update.line_id in update.prerequisite_line_ids:
                raise FrontierConflict("a line cannot depend on itself")
            proposed = {
                line_id: self._dependencies[(job_key, line_id)] for line_id in job.lines
            }
            proposed[update.line_id] = update.prerequisite_line_ids
            if _has_cycle(proposed):
                raise FrontierConflict("dependency update would create a cycle")
            self._dependencies[key] = update.prerequisite_line_ids
            self._dependency_versions[key] = update.version
            if tail.phase in {
                LinePhase.EMPTY,
                LinePhase.READY,
                LinePhase.BLOCKED,
            }:
                tail.phase = self._resolved_phase(key)
            return tail

    async def finish_line(self, event: LineFinish) -> tuple[LineTail, tuple[str, ...]]:
        async with self._lock:
            tail = self._require_line(event.job_id, event.line_id)
            if tail.version != event.expected_tail_version:
                raise FrontierConflict(
                    f"tail version {tail.version} does not match expected "
                    f"{event.expected_tail_version}"
                )
            if tail.phase not in {LinePhase.EMPTY, LinePhase.READY}:
                raise FrontierConflict(
                    f"line tail cannot finish from phase {tail.phase}"
                )
            if (
                event.tail_request_id is not None
                and tail.tail_request_id != event.tail_request_id
            ):
                raise StaleEvent("line finish does not belong to the current tail")
            tail.phase = LinePhase.TERMINAL
            key = tail.identity_key()
            self._request_backups.pop(key, None)
            self._aborted_retries.pop(key, None)
            self._requests.pop(key, None)
            self._prune_tool_executions(key)
            released: list[str] = []
            job = self._jobs[event.job_id]
            for dependent_id in tuple(job.lines):
                dependent_key = (event.job_id, dependent_id)
                dependencies = self._dependencies[dependent_key]
                if event.line_id not in dependencies:
                    continue
                self._dependencies[dependent_key] = tuple(
                    item for item in dependencies if item != event.line_id
                )
                self._dependency_versions[dependent_key] += 1
                dependent = self._lines[dependent_key]
                if dependent.phase == LinePhase.BLOCKED:
                    dependent.phase = self._resolved_phase(dependent_key)
                released.append(dependent_id)
            self._lines.pop(key, None)
            self._contexts.pop(key, None)
            self._line_metadata.pop(key, None)
            self._dependencies.pop(key, None)
            self._dependency_versions.pop(key, None)
            job.lines.discard(event.line_id)
            if not job.lines:
                job.finished_at = datetime.now(UTC)
            return tail, tuple(sorted(released))

    async def mark_terminal(self, job_id: str, line_id: str, _reason: str) -> LineTail:
        """Fail closed while retaining the line for terminal-state inspection."""
        async with self._lock:
            tail = self._require_line(job_id, line_id)
            if tail.phase == LinePhase.TERMINAL:
                return tail
            tail.phase = LinePhase.TERMINAL
            key = tail.identity_key()
            self._request_backups.pop(key, None)
            self._aborted_retries.pop(key, None)
            self._requests.pop(key, None)
            self._prune_tool_executions(key)
            return tail

    async def snapshot(self, job_id: str) -> dict[str, Any]:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise FrontierConflict("unknown job")
            lines = list(self._line_items(job_id))
            dependents: dict[str, int] = {line_id: 0 for line_id in job.lines}
            for _line_id, dependencies in self._dependency_items(job_id):
                for prerequisite in dependencies:
                    dependents[prerequisite] += 1
            return {
                "job_id": job_id,
                "default_slo_ms": job.default_slo_ms,
                "root_conversation_id": job.root_conversation_id,
                "deployment_id": job.deployment_id,
                "namespace_id": job.namespace_id,
                "workflow_started_at": job.workflow_started_at.isoformat(),
                "deadline": job.deadline.isoformat() if job.deadline else None,
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                "lines": [
                    self._tail_to_dict(tail, dependents[line_id])
                    for line_id, tail in lines
                ],
            }

    async def require_line(self, job_id: str, line_id: str) -> LineTail:
        async with self._lock:
            return self._require_line(job_id, line_id)

    async def line_snapshot(self, job_id: str, line_id: str) -> dict[str, Any]:
        async with self._lock:
            tail = self._require_line(job_id, line_id)
            blocking_count = sum(
                line_id in dependencies
                for dependent_id, dependencies in self._dependency_items(job_id)
                if dependent_id != line_id
            )
            return self._tail_to_dict(tail, blocking_count)

    async def dependency_snapshot(
        self, job_id: str, line_id: str
    ) -> tuple[int, tuple[str, ...]]:
        async with self._lock:
            key = (job_id, line_id)
            self._require_line(job_id, line_id)
            return self._dependency_versions[key], self._dependencies[key]

    async def context_evidence(self, job_id: str, line_id: str) -> dict[str, Any]:
        async with self._lock:
            key = (job_id, line_id)
            self._require_line(job_id, line_id)
            context = self._contexts[key]
            tail = self._lines[key]
            return {
                "context_epoch": tail.context_epoch,
                "context_sequence": context.sequence,
                "base_context_cursor": tail.base_context_cursor,
                "context_digest": context.digest,
                "history": tuple(context.history),
            }

    def _line_items(self, job_key: str) -> list[tuple[str, LineTail]]:
        return [
            (line_id, tail)
            for (job_id, line_id), tail in self._lines.items()
            if job_id == job_key
        ]

    def _dependency_items(self, job_key: str) -> list[tuple[str, tuple[str, ...]]]:
        return [
            (line_id, values)
            for (job_id, line_id), values in self._dependencies.items()
            if job_id == job_key
        ]

    def _require_line(self, job_id: str, line_id: str) -> LineTail:
        try:
            return self._lines[(job_id, line_id)]
        except KeyError as exc:
            raise FrontierConflict("unknown line") from exc

    def _require_current(self, identity: RequestIdentity) -> LineTail:
        tail = self._require_line(identity.job_id, identity.line_id)
        request = self._requests.get(tail.identity_key())
        if (
            request is None
            or tail.phase != LinePhase.ACTIVE
            or tail.tail_request_id != identity.tail_request_id
            or request.request_id != identity.request_id
            or request.llm_call_id != identity.llm_call_id
            or request.attempt != identity.attempt
        ):
            raise StaleEvent("event does not belong to the current tail")
        return tail

    def _prune_tool_executions(
        self,
        line_key: tuple[str, str],
        *,
        keep_tail_request_id: str | None = None,
    ) -> None:
        for key in tuple(self._tool_executions):
            if any(
                e.binding_id in self._reuse_receipt_pins
                for e in self._tool_executions[key].events.values()
            ):
                continue
            if key[:2] == line_key and (
                keep_tail_request_id is None or key[2] != keep_tail_request_id
            ):
                self._tool_executions.pop(key, None)

    def _resolved_phase(self, line_key: tuple[str, str]) -> str:
        if self._dependencies[line_key]:
            return LinePhase.BLOCKED
        if self._requests.get(line_key) is None:
            # A registered line with no request is still EMPTY.  READY means
            # that a request has completed (or its Tool facts have resolved),
            # and must not be manufactured merely by clearing dependencies.
            return LinePhase.EMPTY
        if self._has_unresolved_tool_calls(line_key):
            return LinePhase.BLOCKED
        return LinePhase.READY

    def _has_unresolved_tool_calls(self, line_key: tuple[str, str]) -> bool:
        request = self._requests.get(line_key)
        if request is None or not request.tool_calls:
            return False
        # A failed/cancelled attempt may be retried with a higher
        # ``execution_attempt``. Only the latest attempt is authoritative for
        # readiness; an older terminal event must not make a newly active retry
        # look resolved.
        latest: dict[str, tuple[int, ToolExecutionState]] = {}
        for (
            *candidate_line,
            tail_request_id,
            tool_call_id,
            attempt,
        ), execution in self._tool_executions.items():
            if (
                tuple(candidate_line) == line_key
                and tail_request_id == request.tail_request_id
            ):
                prior = latest.get(tool_call_id)
                if prior is None or attempt > prior[0]:
                    latest[tool_call_id] = (attempt, execution)
        resolved = {
            tool_call_id
            for tool_call_id, (_attempt, execution) in latest.items()
            if execution.terminal_kind is not None
        }
        return any(call.tool_call_id not in resolved for call in request.tool_calls)

    def _remember_tool_event(
        self,
        event_key: tuple[str, str, int, str],
        event: ToolTelemetryEvent,
    ) -> None:
        self._tool_event_ids[event_key] = event
        self._tool_event_order.append(event_key)
        for _ in range(len(self._tool_event_order)):
            if len(self._tool_event_ids) <= self._tool_event_receipt_limit:
                break
            oldest = self._tool_event_order.popleft()
            prior = self._tool_event_ids.get(oldest)
            if prior is not None and prior.binding_id in self._reuse_receipt_pins:
                self._tool_event_order.append(oldest)
                continue
            # A key can occur more than once only if a caller replays an event
            # after it was evicted and re-accepted; don't remove a newer value.
            if oldest != event_key:
                self._tool_event_ids.pop(oldest, None)

    @staticmethod
    def _validate_context_transition(
        tail: LineTail, context: ContextState, identity: RequestIdentity
    ) -> None:
        if identity.context_sequence < context.sequence:
            raise FrontierConflict("context sequence would move backwards")
        if identity.context_sequence == context.sequence and (
            identity.base_context_cursor != tail.base_context_cursor
            or identity.context_digest != context.digest
        ):
            raise FrontierConflict(
                "context cursor or digest conflicts at the same sequence"
            )
        if identity.origin == "scheduler_delegated":
            if identity.context_sequence <= context.sequence:
                raise FrontierConflict(
                    "delegated context sequence must advance the current context"
                )
            return
        if any(
            cursor == identity.base_context_cursor and digest != identity.context_digest
            for _sequence, cursor, digest in context.history
        ):
            raise FrontierConflict(
                "context digest conflicts with the previously observed base cursor"
            )

    def _tail_to_dict(self, tail: LineTail, blocking_count: int) -> dict[str, Any]:
        key = tail.identity_key()
        context = self._contexts[key]
        line_metadata = self._line_metadata[key]
        request = self._requests.get(key)
        job = self._jobs[tail.job_id]
        return {
            "job_id": tail.job_id,
            "line_id": tail.line_id,
            "context_epoch": tail.context_epoch,
            "context_sequence": context.sequence,
            "base_context_cursor": tail.base_context_cursor,
            "context_digest": context.digest,
            "version": tail.version,
            "phase": tail.phase,
            "state": tail.phase,
            "tail_request_id": tail.tail_request_id,
            "request_id": request.request_id if request else None,
            "llm_call_id": request.llm_call_id if request else None,
            "attempt": request.attempt if request else None,
            "model": request.model if request else None,
            "instance_id": request.instance_id if request else None,
            "response_id": request.response_id if request else None,
            "tool_calls": [
                {
                    "tool_call_id": item.tool_call_id,
                    "tool_name": item.tool_name,
                    "arguments_digest": item.arguments_digest,
                    "arguments_bytes": item.arguments_bytes,
                }
                for item in (request.tool_calls if request else ())
            ],
            "delta_ref": tail.delta_ref,
            "delegation_ref": tail.delegation_ref,
            "dependencies": list(self._dependencies[key]),
            "dependency_version": self._dependency_versions[key],
            "blocking_line_count": blocking_count,
            "deadline": (
                line_metadata.deadline.isoformat()
                if line_metadata.deadline is not None
                else None
            ),
            "weight": line_metadata.weight,
            "workflow_started_at": job.workflow_started_at.isoformat(),
            "job_deadline": job.deadline.isoformat() if job.deadline else None,
            "conversation_id": line_metadata.conversation_id,
            "parent_conversation_id": line_metadata.parent_conversation_id,
            "parent_line_id": line_metadata.parent_line_id,
            "spawn_id": line_metadata.spawn_id,
            "task_id": line_metadata.task_id,
            "agent_id": line_metadata.agent_id,
            "parent_action_id": line_metadata.parent_action_id,
            "gateway_received_at": (
                request.gateway_received_at.isoformat() if request else None
            ),
        }


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


def _tool_event_key(event: ToolTelemetryEvent) -> tuple[str, str, int, str]:
    """Return the scoped idempotency key for a telemetry event.

    Event IDs are client-generated and only have protocol meaning within the
    line/context epoch that accepted them.  The protocol idempotency contract
    is exactly ``(job_id, line_id, context_epoch, event_id)``.
    """
    return (
        event.job_id,
        event.line_id,
        event.context_epoch,
        event.event_id,
    )
