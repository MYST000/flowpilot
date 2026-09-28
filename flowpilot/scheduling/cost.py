"""Offline calibration used for conditional prefill and transfer estimates."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PrefillCalibration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    max_context_tokens: int = Field(gt=0)
    seconds_per_token: float = Field(gt=0)
    fixed_seconds: float = Field(default=0, ge=0)
    uncertainty_seconds: float = Field(default=0, ge=0)


class TransferCalibration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    seconds_per_byte: float = Field(gt=0)
    fixed_seconds: float = Field(default=0, ge=0)
    uncertainty_seconds: float = Field(default=0, ge=0)

    def seconds(self, object_bytes: int) -> float:
        if object_bytes < 0:
            raise ValueError("object bytes must be nonnegative")
        return (
            self.fixed_seconds + object_bytes * self.seconds_per_byte
            if object_bytes
            else 0.0
        )


class OfflineCostModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    source: str = Field(min_length=1)
    version: str = Field(min_length=1)
    measured_at: datetime
    model: str = Field(min_length=1)
    engine_identity_digest: str = Field(min_length=1)
    measurement_basis: str = Field(min_length=1)
    prefill: tuple[PrefillCalibration, ...]
    offload: TransferCalibration | None = None
    restore: TransferCalibration | None = None

    @model_validator(mode="after")
    def validate_buckets(self) -> OfflineCostModel:
        limits = [bucket.max_context_tokens for bucket in self.prefill]
        if not limits or limits != sorted(set(limits)):
            raise ValueError(
                "prefill buckets must have increasing unique context limits"
            )
        if self.measured_at.tzinfo is None:
            raise ValueError("calibration timestamp must be timezone aware")
        return self

    def prefill_seconds(self, prompt: int, hit: int) -> float | None:
        if not 0 <= hit <= prompt:
            raise ValueError("prefix must be within the target prompt")
        bucket = next((b for b in self.prefill if prompt <= b.max_context_tokens), None)
        if bucket is None:
            return None
        return bucket.fixed_seconds + (prompt - hit) * bucket.seconds_per_token


class RequestWork(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    prompt_tokens: int | None = Field(default=None, ge=0)
    gpu_prefix_tokens: int | None = Field(default=None, ge=0)
    recoverable_tokens: int | None = Field(default=None, ge=0)
    prefill_tokens: int | None = Field(default=None, ge=0)
    gpu_cost_seconds: float | None = Field(default=None, ge=0)
    cpu_cost_seconds: float | None = Field(default=None, ge=0)
    cost_seconds: float | None = Field(default=None, ge=0)
    prefix_basis: str = "COLD:no_target_proof"
    cost_basis: str = "unknown:no_calibration"
    engine_epoch: str | None = None
    state_version: int | None = None
    observed_at_monotonic: float | None = None
    calibration_source: str | None = None
    calibration_version: str | None = None


def estimate_work(
    observations: list[dict], model: OfflineCostModel | None, *, observed_at: float
) -> RequestWork:
    if not observations:
        raise ValueError("target lookup returned no input observations")
    epochs = {o["engine_epoch"] for o in observations}
    if len(epochs) != 1 or any(
        o["reuse_basis"] != "TARGET_REQUEST" for o in observations
    ):
        raise ValueError("inconsistent target prefix observations")
    prompt = sum(o["prompt_tokens"] for o in observations)
    gpu = sum(o["gpu_ready_tokens"] for o in observations)
    all_known = all(o["recoverable_tokens"] is not None for o in observations)
    total = sum(o["recoverable_tokens"] for o in observations) if all_known else None
    for o in observations:
        if not 0 <= o["gpu_ready_tokens"] <= o["prompt_tokens"] or (
            o["recoverable_tokens"] is not None
            and not o["gpu_ready_tokens"]
            <= o["recoverable_tokens"]
            <= o["prompt_tokens"]
        ):
            raise ValueError("invalid target prefix range")
    base = RequestWork(
        prompt_tokens=prompt,
        gpu_prefix_tokens=gpu,
        recoverable_tokens=total,
        prefill_tokens=prompt - gpu,
        prefix_basis="TARGET_REQUEST",
        engine_epoch=next(iter(epochs)),
        state_version=max(o["state_version"] for o in observations),
        observed_at_monotonic=observed_at,
    )
    if model is None:
        return base
    if any(
        o["engine_identity_digest"] != model.engine_identity_digest
        for o in observations
    ):
        return base.model_copy(
            update={"cost_basis": "unknown:calibration_identity_mismatch"}
        )
    gpu_costs = [
        model.prefill_seconds(o["prompt_tokens"], o["gpu_ready_tokens"])
        for o in observations
    ]
    if any(cost is None for cost in gpu_costs):
        return base.model_copy(
            update={"cost_basis": "unknown:outside_calibration_range"}
        )
    gpu_cost = sum(cost for cost in gpu_costs if cost is not None)
    cpu_cost = None
    if (
        all_known
        and model.restore is not None
        and all(o["cpu_load_object_bytes"] is not None for o in observations)
    ):
        cpu_cost = 0.0
        for o in observations:
            prefill = model.prefill_seconds(o["prompt_tokens"], o["recoverable_tokens"])
            assert prefill is not None  # Same context buckets as the GPU costs.
            cpu_cost += model.restore.seconds(o["cpu_load_object_bytes"]) + prefill
    # CPU time is conditional on the engine choosing that plan. GPU-only
    # recomputation remains the comparison basis when no restore model exists.
    return base.model_copy(
        update={
            "gpu_cost_seconds": gpu_cost,
            "cpu_cost_seconds": cpu_cost,
            "cost_seconds": min(gpu_cost, cpu_cost)
            if cpu_cost is not None
            else gpu_cost,
            "cost_basis": "calibrated:conditional_best_available_plan"
            if cpu_cost is not None
            else "calibrated:gpu_recompute_only",
            "calibration_source": model.source,
            "calibration_version": model.version,
        }
    )
