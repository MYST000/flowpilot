"""Feedback for external credits, using measured single-engine counters only."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AdaptiveAdmissionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    enabled: bool = False
    initial_limit: int = Field(default=24, gt=0)
    min_limit: int = Field(default=16, gt=0)
    sample_interval_seconds: float = Field(default=10, gt=0)
    window_seconds: float = Field(default=30, gt=0)
    pressure_windows: int = Field(default=2, gt=0)
    recovery_windows: int = Field(default=3, gt=0)
    decrease_step: int = Field(default=4, gt=0)
    increase_step: int = Field(default=1, gt=0)
    waiting_threshold: float = Field(default=2, gt=0)
    throughput_tolerance: float = Field(default=0.05, ge=0, lt=1)

    @model_validator(mode="after")
    def windows(self) -> AdaptiveAdmissionConfig:
        if self.min_limit > self.initial_limit:
            raise ValueError("adaptive minimum exceeds initial limit")
        if self.window_seconds < self.sample_interval_seconds:
            raise ValueError("adaptive window must include a sampling interval")
        return self


@dataclass(frozen=True)
class EngineLoad:
    running: float
    waiting: float
    preemptions: float
    completed: float

    @classmethod
    def from_prometheus(cls, text: str) -> EngineLoad:
        names = {
            "vllm:num_requests_running": "running",
            "vllm:num_requests_waiting": "waiting",
            "vllm:num_preemptions_total": "preemptions",
            "vllm:e2e_request_latency_seconds_count": "completed",
        }
        found: dict[str, float] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name = line.split("{", 1)[0].split()[0]
            if name not in names:
                continue
            # Labels may contain spaces; the value starts after the label set.
            rest = line.rsplit("}", 1)[1] if "{" in line else line[len(name) :]
            value = float(rest.split()[0])
            if name in found or not math.isfinite(value) or value < 0:
                raise ValueError("ambiguous or invalid single-engine metric")
            found[name] = value
        if found.keys() != names.keys():
            raise ValueError("required engine admission metrics unavailable")
        return cls(**{names[name]: value for name, value in found.items()})


class CapacityFeedback:
    """Hysteresis over actual waiting, preemption and completion observations.

    A missing/reset sample breaks the comparison window and retains the last
    credit limit explicitly. Neither a missing metric nor a reset is zero load.
    """

    def __init__(self, config: AdaptiveAdmissionConfig, maximum: int) -> None:
        self.config = config
        self.maximum = maximum
        self.limit = config.initial_limit if config.enabled else maximum
        self.status = "awaiting_metrics" if config.enabled else "disabled"
        self.reason = "initial_limit" if config.enabled else "configured_limit"
        self.last_observed_at: float | None = None
        self.last_load: EngineLoad | None = None
        self.last_window: dict[str, float] | None = None
        self._baseline: tuple[float, EngineLoad] | None = None
        self._previous: EngineLoad | None = None
        self._samples: list[EngineLoad] = []
        self._demand = False
        self._pressure = 0
        self._recovery = 0
        self.changes = 0

    def _reset_window(self) -> None:
        self._baseline = None
        self._previous = None
        self._samples.clear()
        self._demand = False
        self.last_window = None
        self._pressure = self._recovery = 0

    def unavailable(self, reason: str) -> None:
        self.status = "metrics_unavailable"
        self.reason = reason
        self._reset_window()

    def observe(self, load: EngineLoad, now: float, *, demand: bool) -> None:
        if not self.config.enabled:
            return
        if any(not math.isfinite(x) or x < 0 for x in vars(load).values()):
            raise ValueError("invalid engine load")
        old_at = self.last_observed_at
        previous = self._previous
        if old_at is not None and now <= old_at:
            raise ValueError("engine observation clock must advance")
        reset = previous is not None and (
            load.preemptions < previous.preemptions
            or load.completed < previous.completed
        )
        stale = (
            old_at is not None
            and now - old_at > 3 * self.config.sample_interval_seconds
        )
        if reset or stale:
            self._reset_window()
        self.last_load, self.last_observed_at = load, now
        self._previous = load
        self.status = "observing"
        if self._baseline is None:
            self._baseline = (now, load)
            self.reason = (
                "counter_reset" if reset else "sampling_gap" if stale else "warmup"
            )
            return
        self._samples.append(load)
        self._demand |= demand
        began, first = self._baseline
        elapsed = now - began
        if elapsed < self.config.window_seconds:
            return
        waiting = sum(s.waiting for s in self._samples) / len(self._samples)
        preemptions = load.preemptions - first.preemptions
        rate = (load.completed - first.completed) / elapsed
        prior = self.last_window
        pressured = (
            prior is not None
            and waiting >= self.config.waiting_threshold
            and (preemptions > 0 or waiting >= prior["waiting_mean"])
            and rate
            <= prior["completion_rate"] * (1 + self.config.throughput_tolerance)
        )
        recovery = (
            waiting < self.config.waiting_threshold
            and preemptions == 0
            and rate > 0
            and self._demand
        )
        self._pressure = self._pressure + 1 if pressured else 0
        self._recovery = self._recovery + 1 if recovery else 0
        old = self.limit
        if self._pressure >= self.config.pressure_windows:
            self.limit = max(
                self.config.min_limit, self.limit - self.config.decrease_step
            )
            self.reason = "engine_pressure" if self.limit < old else "minimum_limit"
            self._pressure = self._recovery = 0
        elif self._recovery >= self.config.recovery_windows:
            self.limit = min(self.maximum, self.limit + self.config.increase_step)
            self.reason = "engine_recovered" if self.limit > old else "maximum_limit"
            self._pressure = self._recovery = 0
        else:
            self.reason = "pressure_observation" if pressured else "hold"
        self.changes += self.limit != old
        self.last_window = {
            "seconds": elapsed,
            "waiting_mean": waiting,
            "preemptions": preemptions,
            "completion_rate": rate,
        }
        self._baseline = (now, load)
        self._samples.clear()
        self._demand = False

    def snapshot(self, now: float) -> dict[str, Any]:
        age = now - self.last_observed_at if self.last_observed_at is not None else None
        stale = age is not None and age > 3 * self.config.sample_interval_seconds
        return {
            "enabled": self.config.enabled,
            "status": "metrics_stale" if stale and self.config.enabled else self.status,
            "reason": self.reason,
            "effective_limit": self.limit,
            "min_limit": self.config.min_limit if self.config.enabled else self.maximum,
            "max_limit": self.maximum,
            "sample_age_seconds": age,
            "load": vars(self.last_load) if self.last_load is not None else None,
            "window": self.last_window,
            "pressure_windows": self._pressure,
            "recovery_windows": self._recovery,
            "changes": self.changes,
        }
