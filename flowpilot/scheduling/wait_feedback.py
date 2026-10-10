"""Measured external admission waits, independent of engine and Tool time."""

from collections import deque

from pydantic import BaseModel, ConfigDict, Field


class WaitFeedbackConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    # Initial experimental window; deployments must calibrate against their load.
    window_seconds: float = Field(default=30.0, gt=0)


class QueueWaitEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    schema_version: str = "admission-wait-v1"
    estimate_ms: float | None = Field(default=None, ge=0)
    source: str
    sample_count: int = Field(default=0, ge=0)
    window_seconds: float | None = Field(default=None, gt=0)
    observed_at_monotonic: float
    last_sample_at_monotonic: float | None = None


class AdmissionWaitFeedback:
    def __init__(self, config: WaitFeedbackConfig) -> None:
        self.config = config
        self._samples: deque[tuple[float, float]] = deque()
        self._last_sample_at: float | None = None

    def record(self, *, dispatched_at: float, entered_at: float) -> None:
        if dispatched_at < entered_at:
            raise ValueError("dispatch precedes queue entry")
        self._samples.append((dispatched_at, (dispatched_at - entered_at) * 1000))
        self._last_sample_at = dispatched_at
        self._expire(dispatched_at)

    def _expire(self, now: float) -> None:
        cutoff = now - self.config.window_seconds
        while self._samples and self._samples[0][0] <= cutoff:
            self._samples.popleft()

    def estimate(self, *, now: float, idle_capacity: bool) -> QueueWaitEstimate:
        self._expire(now)
        count = len(self._samples)
        return QueueWaitEstimate(
            estimate_ms=0.0
            if idle_capacity
            else (sum(wait for _, wait in self._samples) / count if count else None),
            source="idle_capacity"
            if idle_capacity
            else (
                "measured"
                if count
                else ("no_samples" if self._last_sample_at is None else "expired")
            ),
            sample_count=count,
            window_seconds=self.config.window_seconds,
            observed_at_monotonic=now,
            last_sample_at_monotonic=self._last_sample_at,
        )
