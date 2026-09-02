from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class CalibrationStatus(StrEnum):
    UNSUPPORTED = "unsupported"
    UNCALIBRATED = "uncalibrated"
    CALIBRATED = "calibrated"


@dataclass(frozen=True, slots=True)
class ToolObservation:
    tool_family: str
    intrinsic_cost_ms: float
    effective_cost_ms: float
    remaining_cost_ms: float
    duration_ms: float
    output_bytes: int

    def __post_init__(self) -> None:
        if (
            min(
                self.intrinsic_cost_ms,
                self.effective_cost_ms,
                self.remaining_cost_ms,
                self.duration_ms,
                self.output_bytes,
            )
            < 0
        ):
            raise ValueError("Tool cost/profile observations cannot be negative")


@dataclass(frozen=True, slots=True)
class ToolAnalysis:
    tool_family: str
    intrinsic_cost_ms: float
    effective_cost_ms: float
    remaining_cost_ms: float
    duration_profile_ms: float
    output_profile_bytes: float
    tool_share: float
    heavy: bool
    calibration_status: CalibrationStatus
    sample_count: int
    duration_residual_ms: float | None = None
    output_residual_bytes: float | None = None


class DeterministicToolAnalysisAdapter:
    """Deterministic E14 closure with explicit calibration provenance.

    It uses measured inputs only.  Until an external calibrated dataset is
    installed the status remains ``uncalibrated`` and no benefit is claimed.
    """

    def __init__(
        self,
        *,
        alpha: float = 0.25,
        heavy_enter_share: float = 0.60,
        heavy_exit_share: float = 0.40,
        minimum_samples: int = 3,
        calibrated: bool = False,
    ) -> None:
        if not 0 < alpha <= 1 or not 0 <= heavy_exit_share <= heavy_enter_share <= 1:
            raise ValueError("invalid profile smoothing/hysteresis configuration")
        self.alpha = alpha
        self.heavy_enter_share = heavy_enter_share
        self.heavy_exit_share = heavy_exit_share
        self.minimum_samples = minimum_samples
        self.calibrated = calibrated
        self._profiles: dict[str, ToolAnalysis] = {}

    def observe(
        self, observation: ToolObservation, *, inference_cost_ms: float
    ) -> ToolAnalysis:
        if inference_cost_ms < 0:
            raise ValueError("inference cost cannot be negative")
        prior = self._profiles.get(observation.tool_family)
        count = 1 if prior is None else prior.sample_count + 1
        duration = self._ewma(
            prior.duration_profile_ms if prior else None, observation.duration_ms
        )
        output = self._ewma(
            prior.output_profile_bytes if prior else None,
            float(observation.output_bytes),
        )
        effective = observation.effective_cost_ms
        share = effective / max(effective + inference_cost_ms, 1e-9)
        heavy = prior.heavy if prior else False
        if count >= self.minimum_samples:
            if heavy and share <= self.heavy_exit_share:
                heavy = False
            elif not heavy and share >= self.heavy_enter_share:
                heavy = True
        analysis = ToolAnalysis(
            observation.tool_family,
            observation.intrinsic_cost_ms,
            effective,
            observation.remaining_cost_ms,
            duration,
            output,
            share,
            heavy,
            CalibrationStatus.CALIBRATED
            if self.calibrated
            else CalibrationStatus.UNCALIBRATED,
            count,
            (
                observation.duration_ms - prior.duration_profile_ms
                if prior is not None
                else None
            ),
            (
                float(observation.output_bytes) - prior.output_profile_bytes
                if prior is not None
                else None
            ),
        )
        self._profiles[observation.tool_family] = analysis
        return analysis

    def project(self, tool_family: str) -> ToolAnalysis | None:
        """Return a short-lived projection; callers must not write it to LineTail."""
        return self._profiles.get(tool_family)

    def snapshot(self) -> tuple[ToolAnalysis, ...]:
        return tuple(self._profiles.values())

    def observe_resolution(
        self,
        tool_family: str,
        *,
        ready_latency_ms: float,
        result_bytes: int = 0,
        cache_hit: bool = False,
        inference_cost_ms: float = 0.0,
    ) -> ToolAnalysis:
        """Record a factual Tool ready event, including cache hits.

        A hit has zero effective Tool execution cost but still contributes its
        measured lookup/delivery latency to the duration profile.  This keeps
        forecast updates tied to facts while leaving readiness semantics owned
        by the resolution store.
        """
        if ready_latency_ms < 0 or result_bytes < 0 or inference_cost_ms < 0:
            raise ValueError("resolution measurements cannot be negative")
        effective = 0.0 if cache_hit else ready_latency_ms
        return self.observe(
            ToolObservation(
                tool_family=tool_family,
                intrinsic_cost_ms=ready_latency_ms,
                effective_cost_ms=effective,
                remaining_cost_ms=0.0,
                duration_ms=ready_latency_ms,
                output_bytes=result_bytes,
            ),
            inference_cost_ms=inference_cost_ms,
        )

    def residual(
        self,
        tool_family: str,
        *,
        duration_ms: float,
        output_bytes: int,
    ) -> tuple[float, float] | None:
        """Return signed duration/output residual against the current profile."""
        profile = self._profiles.get(tool_family)
        if profile is None:
            return None
        return (
            duration_ms - profile.duration_profile_ms,
            float(output_bytes) - profile.output_profile_bytes,
        )

    def _ewma(self, prior: float | None, value: float) -> float:
        return value if prior is None else self.alpha * value + (1 - self.alpha) * prior
