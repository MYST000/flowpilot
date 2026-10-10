"""Frozen seven-feature cadence estimates from engine-owned load observations.

The candidate has the prefill budget after observed decodes. Other prefills and
future changes of batch membership are not simulated. This is a pre-admission
scenario, not knowledge of the candidate's first batch or an engine-wait ETA.
"""

from __future__ import annotations

import math
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

Features = tuple[float, float, float, float, float, float, float]


class PrefillLoad(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    schema_version: Literal[1] = 1
    basis: Literal["running_decode_snapshot"]
    engine_epoch: str = Field(min_length=1)
    engine_identity_digest: str = Field(min_length=1)
    observed_at_monotonic: float = Field(ge=0)
    decode_requests: int = Field(ge=0, strict=True)
    decode_context_tokens: int = Field(ge=0, strict=True)
    active_prefill_requests: int = Field(ge=0, strict=True)
    token_budget: int = Field(gt=0, strict=True)
    max_num_seqs: int = Field(gt=0, strict=True)
    block_tokens: int = Field(gt=0, strict=True)
    max_model_len: int = Field(gt=0, strict=True)
    async_scheduling: bool
    enable_chunked_prefill: bool
    mamba_cache_mode: str

    @model_validator(mode="after")
    def validate_counts(self) -> PrefillLoad:
        if self.decode_context_tokens < self.decode_requests or (
            not self.decode_requests and self.decode_context_tokens
        ):
            raise ValueError("decode context must include one token per decode")
        if self.decode_requests + self.active_prefill_requests > self.max_num_seqs:
            raise ValueError("running requests exceed engine max_num_seqs")
        return self


class PrefillEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    seconds: float | None = Field(default=None, ge=0)
    basis: str
    features: Features | None = None
    load: PrefillLoad | None = None
    extrapolated: bool = False


class SevenFeaturePrefillCalibration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    kind: Literal["seven_feature_cadence_frozen"]
    coefficients: Features
    max_context_tokens: int = Field(gt=0)
    max_num_seqs: int = Field(gt=0)
    token_budget: int = Field(gt=0)
    block_tokens: int = Field(gt=0)
    training_feature_max: Features
    max_profiled_prompt_tokens: int = Field(gt=0)
    fit_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scenario: Literal["candidate_prefill_frozen_decode"] = (
        "candidate_prefill_frozen_decode"
    )

    def estimate(
        self, prompt: int, hit: int, load: PrefillLoad | None
    ) -> PrefillEstimate:
        if load is None:
            return PrefillEstimate(basis="unknown:prefill_load_missing")
        missing = (
            "outside_calibration_range"
            if prompt <= 0 or prompt > self.max_context_tokens
            else "prefill_configuration_mismatch"
            if (
                load.token_budget != self.token_budget
                or load.max_num_seqs != self.max_num_seqs
                or load.block_tokens != self.block_tokens
                or load.max_model_len != self.max_context_tokens
                or not load.async_scheduling
                or not load.enable_chunked_prefill
                or load.mamba_cache_mode != "align"
            )
            else None
        )
        if missing:
            return PrefillEstimate(basis="unknown:" + missing, load=load)
        # A full cache hit still needs the last prompt token for logits.
        h = min(hit, prompt - 1)
        d = self.token_budget - load.decode_requests
        x = [0.0] * 7
        extrapolated = prompt > self.max_profiled_prompt_tokens
        while h < prompt:
            end = min(prompt, h + d)
            if end < prompt:
                end = end // self.block_tokens * self.block_tokens
            boundaries = [
                (h // self.block_tokens + 1) * self.block_tokens
                if h % self.block_tokens
                else 0,
                prompt // self.block_tokens * self.block_tokens,
            ]
            end = min([end] + [s for s in boundaries if h < s < end])
            if d <= 0 or end <= h:
                return PrefillEstimate(
                    basis="unknown:no_aligned_prefill_budget", load=load
                )
            q, b, c = end - h, load.decode_requests, load.decode_context_tokens
            row = (1, q, b, q * q, c, q * h, q * (c + h))
            extrapolated |= any(
                v > m for v, m in zip(row, self.training_feature_max, strict=True)
            )
            x = [a + v for a, v in zip(x, row, strict=True)]
            h = end
        seconds = sum(w * v for w, v in zip(self.coefficients, x, strict=True))
        # Preserve an invalid model result as unknown; never clip it to zero.
        return PrefillEstimate(
            seconds=seconds if math.isfinite(seconds) and seconds > 0 else None,
            basis="calibrated:seven_feature_cadence_frozen:candidate_prefill_frozen_decode"
            if math.isfinite(seconds) and seconds > 0
            else "unknown:nonpositive_or_nonfinite_prefill_prediction",
            features=cast(Features, tuple(x)),
            load=load,
            extrapolated=extrapolated,
        )
