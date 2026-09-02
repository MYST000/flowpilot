from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from flowpilot.protocol import RequestIdentity


class GatewayCallPhase(StrEnum):
    ACTIVE = "active"
    ROUTED = "routed"
    COMPLETED = "completed"
    PROVIDER_ERROR = "provider_error"
    PROTOCOL_ERROR = "protocol_error"
    UPSTREAM_FAILED = "upstream_failed"
    CANCELLED = "cancelled"
    STREAM_ERROR = "stream_error"


_TERMINAL_PHASES = {
    GatewayCallPhase.COMPLETED,
    GatewayCallPhase.PROVIDER_ERROR,
    GatewayCallPhase.PROTOCOL_ERROR,
    GatewayCallPhase.UPSTREAM_FAILED,
    GatewayCallPhase.CANCELLED,
    GatewayCallPhase.STREAM_ERROR,
}


@dataclass(slots=True)
class GatewayCallRecord:
    job_id: str
    line_id: str
    tail_request_id: str
    llm_call_id: str
    attempt: int
    phase: GatewayCallPhase
    stream: bool
    api_kind: str
    instance_id: str | None
    authoritative_tail_version: int | None
    status_code: int | None
    terminal_reason: str | None
    started_at: datetime
    updated_at: datetime
    gateway_received_at: datetime
    scheduler_queue_entered_at: datetime
    upstream_sent_at: datetime | None = None
    upstream_first_byte_at: datetime | None = None
    response_completed_at: datetime | None = None


class GatewayCallConflict(RuntimeError):
    pass


class GatewayCallStore:
    """Owns per-call proxy lifecycle independently from ``LineTail.phase``."""

    def __init__(self, *, max_records: int = 4096) -> None:
        if max_records <= 0:
            raise ValueError("max_records must be positive")
        self._max_records = max_records
        self._lock = asyncio.Lock()
        self._records: dict[tuple[str, str, str, int], GatewayCallRecord] = {}
        self._latest_attempt: dict[tuple[str, str, str], int] = {}

    async def start(
        self, identity: RequestIdentity, *, stream: bool, api_kind: str
    ) -> GatewayCallRecord:
        async with self._lock:
            call_key = (
                identity.job_id,
                identity.line_id,
                identity.llm_call_id,
            )
            latest_attempt = self._latest_attempt.get(call_key, 0)
            existing = self._records.get((*call_key, latest_attempt))
            if existing is not None and existing.phase not in _TERMINAL_PHASES:
                raise GatewayCallConflict(
                    "llm_call_id already has an active gateway call"
                )
            attempt = latest_attempt + 1 if existing is not None else 1
            now = datetime.now(UTC)
            record = GatewayCallRecord(
                identity.job_id,
                identity.line_id,
                identity.tail_request_id,
                identity.llm_call_id,
                attempt,
                GatewayCallPhase.ACTIVE,
                stream,
                api_kind,
                None,
                None,
                None,
                None,
                now,
                now,
                now,
                now,
                None,
                None,
                None,
            )
            self._records[(*call_key, attempt)] = record
            self._latest_attempt[call_key] = attempt
            self._trim()
            return record

    async def routed(self, call: GatewayCallRecord, instance_id: str) -> None:
        async with self._lock:
            record = self._require_active(call)
            record.phase = GatewayCallPhase.ROUTED
            record.instance_id = instance_id
            record.updated_at = datetime.now(UTC)

    async def sent(self, call: GatewayCallRecord) -> None:
        """Record the wall-clock instant immediately before upstream send."""
        async with self._lock:
            record = self._require_active(call)
            now = datetime.now(UTC)
            record.upstream_sent_at = record.upstream_sent_at or now
            record.updated_at = now

    async def first_byte(self, call: GatewayCallRecord) -> None:
        async with self._lock:
            record = self._require_active(call)
            now = datetime.now(UTC)
            record.upstream_first_byte_at = record.upstream_first_byte_at or now
            record.updated_at = now

    async def terminal(
        self,
        call: GatewayCallRecord,
        phase: GatewayCallPhase,
        *,
        authoritative_tail_version: int | None,
        status_code: int | None,
        reason: str | None = None,
    ) -> None:
        if phase not in _TERMINAL_PHASES:
            raise ValueError("terminal phase required")
        async with self._lock:
            record = self._record_for(call)
            if record is None:
                raise GatewayCallConflict("unknown gateway call")
            if record.phase in _TERMINAL_PHASES:
                if (
                    record.phase == phase
                    and record.authoritative_tail_version == authoritative_tail_version
                    and record.status_code == status_code
                    and record.terminal_reason == reason
                ):
                    return
                raise GatewayCallConflict(
                    "gateway call already has a different terminal outcome"
                )
            record.phase = phase
            record.authoritative_tail_version = authoritative_tail_version
            record.status_code = status_code
            record.terminal_reason = reason
            now = datetime.now(UTC)
            record.response_completed_at = now
            record.updated_at = now

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [
                {
                    "job_id": record.job_id,
                    "line_id": record.line_id,
                    "tail_request_id": record.tail_request_id,
                    "llm_call_id": record.llm_call_id,
                    "attempt": record.attempt,
                    "phase": record.phase.value,
                    "stream": record.stream,
                    "api_kind": record.api_kind,
                    "instance_id": record.instance_id,
                    "authoritative_tail_version": record.authoritative_tail_version,
                    "status_code": record.status_code,
                    "terminal_reason": record.terminal_reason,
                    "started_at": record.started_at.isoformat(),
                    "updated_at": record.updated_at.isoformat(),
                    "gateway_received_at": record.gateway_received_at.isoformat(),
                    "scheduler_queue_entered_at": (
                        record.scheduler_queue_entered_at.isoformat()
                    ),
                    "upstream_sent_at": (
                        record.upstream_sent_at.isoformat()
                        if record.upstream_sent_at else None
                    ),
                    "upstream_first_byte_at": (
                        record.upstream_first_byte_at.isoformat()
                        if record.upstream_first_byte_at else None
                    ),
                    "response_completed_at": (
                        record.response_completed_at.isoformat()
                        if record.response_completed_at else None
                    ),
                }
                for record in self._records.values()
            ]

    def _record_for(self, call: GatewayCallRecord) -> GatewayCallRecord | None:
        return self._records.get(
            (
                call.job_id,
                call.line_id,
                call.llm_call_id,
                call.attempt,
            )
        )

    def _require_active(self, call: GatewayCallRecord) -> GatewayCallRecord:
        record = self._record_for(call)
        if record is None:
            raise GatewayCallConflict("unknown gateway call")
        if record.phase in _TERMINAL_PHASES:
            raise GatewayCallConflict("gateway call is already terminal")
        return record

    def _trim(self) -> None:
        while len(self._records) > self._max_records:
            terminal_id = next(
                (
                    call_id
                    for call_id, record in self._records.items()
                    if record.phase in _TERMINAL_PHASES
                ),
                None,
            )
            if terminal_id is None:
                return
            key = terminal_id
            self._records.pop(key, None)
            call_key = key[:3]
            if self._latest_attempt.get(call_key) == key[3]:
                remaining = [
                    candidate[3]
                    for candidate in self._records
                    if candidate[:3] == call_key
                ]
                if remaining:
                    self._latest_attempt[call_key] = max(remaining)
                else:
                    self._latest_attempt.pop(call_key, None)
