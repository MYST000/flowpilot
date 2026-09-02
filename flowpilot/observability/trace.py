from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from flowpilot.protocol import TRACE_SCHEMA_VERSION


class TraceSink(Protocol):
    async def write(self, record: dict[str, Any]) -> None: ...


class JsonlTraceSink:
    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int = 64 * 1024 * 1024,
        backup_count: int = 3,
    ) -> None:
        if max_bytes <= 0 or backup_count < 0:
            raise ValueError("invalid trace rotation configuration")
        self.path = path
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self._lock = asyncio.Lock()

    async def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
        async with self._lock:
            # Trace records are bounded metadata-only writes.  Keeping the
            # append in the locked critical section avoids executor-dependent
            # filesystem stalls while preserving ordering and rotation.
            self._append(line)

    def _append(self, line: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded_bytes = len(line.encode("utf-8")) + 1
        if (
            self.path.exists()
            and self.path.stat().st_size + encoded_bytes > self.max_bytes
        ):
            self._rotate()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")
            handle.flush()

    def _rotate(self) -> None:
        if self.backup_count == 0:
            self.path.unlink(missing_ok=True)
            return
        oldest = self.path.with_name(f"{self.path.name}.{self.backup_count}")
        oldest.unlink(missing_ok=True)
        for index in range(self.backup_count - 1, 0, -1):
            source = self.path.with_name(f"{self.path.name}.{index}")
            if source.exists():
                source.replace(self.path.with_name(f"{self.path.name}.{index + 1}"))
        self.path.replace(self.path.with_name(f"{self.path.name}.1"))


class InMemoryTraceSink:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def write(self, record: dict[str, Any]) -> None:
        async with self._lock:
            self.records.append(record)


class TraceRecorder:
    def __init__(self, sink: TraceSink) -> None:
        self._sink = sink
        self._lock = asyncio.Lock()
        self._counters: Counter[str] = Counter()

    async def emit(
        self,
        event_type: str,
        *,
        identity: Mapping[str, Any] | None = None,
        fields: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": TRACE_SCHEMA_VERSION,
            "event_id": str(uuid4()),
            "event_type": event_type,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        if identity:
            record["identity"] = dict(identity)
        if fields:
            record["fields"] = _json_safe(fields)
        try:
            await self._sink.write(record)
        except Exception:
            async with self._lock:
                self._counters["trace_write_failures"] += 1
                self._counters["trace_dropped_events"] += 1
            return record
        async with self._lock:
            self._counters[event_type] += 1
        return record

    async def increment(self, name: str, value: int = 1) -> None:
        async with self._lock:
            self._counters[name] += value

    async def snapshot(self) -> dict[str, int]:
        async with self._lock:
            return dict(self._counters)

    async def healthy(self) -> bool:
        async with self._lock:
            return self._counters["trace_write_failures"] == 0

    async def prometheus(self) -> str:
        values = await self.snapshot()
        lines = [
            "# HELP flowpilot_events_total Phase 0 trace events by event type.",
            "# TYPE flowpilot_events_total counter",
        ]
        for name, value in sorted(values.items()):
            metric_name = _metric_name(name)
            lines.append(f'flowpilot_events_total{{event="{metric_name}"}} {value}')
        return "\n".join(lines) + "\n"


def _metric_name(value: str) -> str:
    return "".join(char if char.isalnum() or char == "_" else "_" for char in value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return {"bytes": len(value)}
    return str(value)


def monotonic_ms() -> float:
    return time.monotonic() * 1000
