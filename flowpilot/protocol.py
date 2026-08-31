from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, overload

from pydantic import BaseModel, Field, field_validator, model_validator

PROTOCOL_VERSION = "flowpilot-phase0-v1"
TRACE_SCHEMA_VERSION = "flowpilot-trace-v1"
REUSE_PROTOCOL_VERSION = "flowpilot-phase1-reuse-v1"
DCS_PROTOCOL_VERSION = "flowpilot-phase2-dcs-v1"
SEMANTIC_REUSE_PROTOCOL_VERSION = "flowpilot-phase3-reuse-v1"
PHASE4_PROTOCOL_VERSION = "flowpilot-phase4-scheduling-v1"
HEX_DIGEST_PATTERN = r"^[0-9a-f]{64}$"
ID_PATTERN = r"^[A-Za-z0-9_.:@/-]+$"
ReuseProtocolVersion = Literal["flowpilot-phase1-reuse-v1", "flowpilot-phase3-reuse-v1"]


@overload
def _require_aware_datetime(value: datetime, field_name: str) -> datetime: ...


@overload
def _require_aware_datetime(value: None, field_name: str) -> None: ...


def _require_aware_datetime(value: datetime | None, field_name: str) -> datetime | None:
    """Reject ambiguous wall-clock timestamps at the protocol boundary."""
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must include a timezone offset")
    return value


class RequestIdentity(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    expected_tail_version: int = Field(ge=0)
    context_epoch: int = Field(ge=1)
    context_sequence: int = Field(ge=0)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    origin: Literal["agent", "scheduler_delegated"] = "agent"
    delegation_lease_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )

    @model_validator(mode="after")
    def validate_origin(self) -> RequestIdentity:
        if self.origin == "scheduler_delegated" and self.delegation_lease_id is None:
            raise ValueError("delegated request requires delegation_lease_id")
        if self.origin == "agent" and self.delegation_lease_id is not None:
            raise ValueError("agent request cannot carry a delegation lease")
        return self


class JobRegistration(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    default_slo_ms: int | None = Field(default=None, gt=0)


class LineRegistration(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    context_sequence: int = Field(default=0, ge=0)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    deadline: datetime | None = None
    weight: float = Field(default=1.0, gt=0)

    @field_validator("deadline")
    @classmethod
    def require_aware_deadline(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "deadline")


class DependencyUpdate(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    version: int = Field(ge=1)
    prerequisite_line_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def reject_duplicates(self) -> DependencyUpdate:
        if len(set(self.prerequisite_line_ids)) != len(self.prerequisite_line_ids):
            raise ValueError("prerequisite_line_ids must be unique")
        return self


class LineFinish(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    expected_tail_version: int = Field(ge=0)
    tail_request_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )


class ToolEventKind(StrEnum):
    START = "start"
    FINISH = "finish"
    FAIL = "fail"
    CANCEL = "cancel"
    BLOCKED = "blocked"


class ToolClass(StrEnum):
    WEB = "web"
    NON_WEB = "non_web"


class ToolTelemetryEvent(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    event_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    sequence: int = Field(ge=1)
    execution_attempt: int = Field(ge=1)
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    action_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_name: str = Field(min_length=1, max_length=256)
    tool_class: ToolClass
    event_kind: ToolEventKind
    input_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    result_size_bytes: int | None = Field(default=None, ge=0)
    measured_latency_ms: float | None = Field(default=None, ge=0)
    error_class: str | None = Field(default=None, max_length=256)
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")

    @model_validator(mode="after")
    def validate_terminal_fields(self) -> ToolTelemetryEvent:
        if self.event_kind == ToolEventKind.START:
            if any(
                value is not None
                for value in (
                    self.result_size_bytes,
                    self.measured_latency_ms,
                    self.error_class,
                )
            ):
                raise ValueError("start events cannot contain terminal fields")
        if self.event_kind == ToolEventKind.FINISH:
            if self.result_size_bytes is None or self.measured_latency_ms is None:
                raise ValueError(
                    "finish events require result_size_bytes and measured_latency_ms"
                )
        if self.event_kind == ToolEventKind.FAIL and not self.error_class:
            raise ValueError("fail events require error_class")
        if self.event_kind == ToolEventKind.CANCEL and not self.error_class:
            raise ValueError("cancel events require error_class")
        if self.event_kind == ToolEventKind.BLOCKED:
            if not self.error_class:
                raise ValueError("blocked events require error_class")
            if (
                self.result_size_bytes is not None
                or self.measured_latency_ms is not None
            ):
                raise ValueError("blocked events cannot contain result fields")
        return self


class KVTier(StrEnum):
    GPU = "gpu"
    CPU = "cpu"
    NVME = "nvme"
    DROPPED = "dropped"


class KVStateEvent(BaseModel):
    protocol_version: Literal["flowpilot-phase0-v1"] = PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    session_id: str = Field(min_length=1, max_length=256)
    instance_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tier: KVTier
    bytes: int = Field(ge=0)
    restore_cost_ms: float | None = Field(default=None, ge=0)
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")


class ForecastRequest(BaseModel):
    """Metadata-only request sent to an externally owned Tool predictor."""

    schema_version: Literal["flowpilot-phase4-scheduling-v1"] = (
        PHASE4_PROTOCOL_VERSION
    )
    request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    model_id: str = Field(min_length=1, max_length=256)
    history_features_ref: str = Field(min_length=1, max_length=256)
    tool_catalog_version: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    deadline: datetime | None = None
    requested_top_n: int = Field(default=3, gt=0, le=32)

    @field_validator("deadline")
    @classmethod
    def require_aware_deadline(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "deadline")


class ForecastCandidate(BaseModel):
    tool_family: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    probability: float = Field(ge=0, le=1)
    duration_p50: float = Field(ge=0)
    duration_p90: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_quantiles(self) -> ForecastCandidate:
        if self.duration_p90 < self.duration_p50:
            raise ValueError("duration_p90 must be >= duration_p50")
        return self


class ForecastResult(BaseModel):
    """Versioned, expiring predictor output; never a Tool execution fact."""

    schema_version: Literal["flowpilot-phase4-scheduling-v1"] = (
        PHASE4_PROTOCOL_VERSION
    )
    based_on_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    candidates: tuple[ForecastCandidate, ...] = Field(max_length=32)
    confidence: float = Field(ge=0, le=1)
    predictor_version: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def require_aware_expiry(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "expires_at")


class ToolResolutionKind(StrEnum):
    HISTORICAL_HIT = "historical_hit"
    INFLIGHT_FOLLOWER = "inflight_follower"
    LOCAL_LEADER = "local_leader"
    LOCAL_ONLY = "local_only"


class ToolResolutionStatus(StrEnum):
    RESOLVING = "resolving"
    WAITING = "waiting"
    READY = "ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ToolResolutionSource(StrEnum):
    CACHE_FACT = "cache_fact"
    INFLIGHT_STATE = "inflight_state"
    WEB_HISTORY = "web_history"
    LOCAL_MODEL = "local_model"


class ToolResolutionRecord(BaseModel):
    """Authoritative Tool readiness fact kept outside the line tail."""

    schema_version: Literal["flowpilot-phase4-scheduling-v1"] = (
        PHASE4_PROTOCOL_VERSION
    )
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_family: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    resolution: ToolResolutionKind
    status: ToolResolutionStatus
    ready_at_estimate: datetime | None = None
    actual_latency_ms: float | None = Field(default=None, ge=0)
    actual_result_bytes: int | None = Field(default=None, ge=0)
    source: ToolResolutionSource
    confidence: float = Field(ge=0, le=1)
    version: int = Field(ge=1)
    updated_at: datetime

    @field_validator("ready_at_estimate", "updated_at")
    @classmethod
    def require_aware_times(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "resolution timestamp")


class SchedulingProjection(BaseModel):
    """Short-lived scheduling view derived from current owner facts."""

    schema_version: Literal["flowpilot-phase4-scheduling-v1"] = (
        PHASE4_PROTOCOL_VERSION
    )
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )
    tail_version: int = Field(ge=0)
    ready: bool
    t_need: datetime | None = None
    estimated_inference_ms: float | None = Field(default=None, ge=0)
    request_weight: float = Field(ge=0)
    kv_restore_laxity_ms: float | None = None
    dag_importance: float = Field(ge=0)
    slo_urgency: float = Field(ge=0)
    blocking_line_count: int = Field(ge=0)
    wait_age_ms: float = Field(ge=0)
    kv_telemetry: Literal["supported", "unsupported"] = "unsupported"
    computed_at: datetime

    @field_validator("t_need", "computed_at")
    @classmethod
    def require_aware_projection_times(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "projection timestamp")


ControlEvent = Annotated[ToolTelemetryEvent | KVStateEvent, Field(discriminator=None)]


class ReuseDecisionKind(StrEnum):
    EXECUTE_LOCALLY = "execute_locally"
    DEFER_WITH_CACHED_RESULT = "defer_with_cached_result"
    DEFER_WAIT_FOR_INFLIGHT = "defer_wait_for_inflight"
    SYNC_AND_EXECUTE_AS_LEADER = "sync_and_execute_as_leader"
    WAIT_AND_SYNC_REUSED_RESULT = "wait_and_sync_reused_result"
    SYNC_WITH_REUSED_RESULT = "sync_with_reused_result"


class ReuseType(StrEnum):
    HISTORICAL = "historical"
    INFLIGHT = "inflight"


class ReuseMatchKind(StrEnum):
    EXACT = "exact"
    SEMANTIC = "semantic"


class ToolRegistryEntry(BaseModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    tool_name: str = Field(min_length=1, max_length=256)
    canonical_tool_family: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_version: str = Field(min_length=1, max_length=64, pattern=ID_PATTERN)
    result_schema_version: str = Field(min_length=1, max_length=64, pattern=ID_PATTERN)
    read_only: bool = True
    exact_reuse_enabled: bool = True
    semantic_reuse_enabled: bool = False
    semantic_query_fields: tuple[str, ...] = ("query",)
    semantic_similarity_threshold: float = Field(default=0.92, ge=0, le=1)
    semantic_candidate_limit: int = Field(default=100, gt=0, le=10_000)
    semantic_time_sensitivity_classes: tuple[str, ...] = ("standard",)
    allow_public_scope: bool = False
    default_ttl_seconds: int = Field(default=300, gt=0, le=86400)
    max_result_bytes: int = Field(default=1_000_000, gt=0)

    @model_validator(mode="after")
    def validate_semantic_policy(self) -> ToolRegistryEntry:
        if not self.semantic_query_fields:
            raise ValueError("semantic_query_fields cannot be empty")
        if len(set(self.semantic_query_fields)) != len(self.semantic_query_fields):
            raise ValueError("semantic_query_fields must be unique")
        if any(not item or "." in item for item in self.semantic_query_fields):
            raise ValueError("semantic_query_fields must be top-level field names")
        if not self.semantic_time_sensitivity_classes:
            raise ValueError("semantic_time_sensitivity_classes cannot be empty")
        if (
            self.semantic_reuse_enabled
            and self.protocol_version != SEMANTIC_REUSE_PROTOCOL_VERSION
        ):
            raise ValueError(
                "semantic reuse registry entries require flowpilot-phase3-reuse-v1"
            )
        if self.semantic_reuse_enabled and not self.exact_reuse_enabled:
            raise ValueError("semantic reuse requires exact reuse to remain enabled")
        return self


class ReuseScope(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    auth_scope: str = Field(min_length=1, max_length=256)
    locale: str = Field(default="und", min_length=1, max_length=64)
    language: str = Field(default="und", min_length=1, max_length=64)
    region: str = Field(default="global", min_length=1, max_length=64)
    safe_search_policy: str = Field(default="default", min_length=1, max_length=64)
    time_sensitivity_class: str = Field(default="standard", min_length=1, max_length=64)
    data_source_constraints: tuple[str, ...] = ()
    public_scope: bool = False


class ToolReuseIdentity(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    action_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)


class ToolReuseResolveRequest(BaseModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    identity: ToolReuseIdentity
    tool_name: str = Field(min_length=1, max_length=256)
    arguments: dict[str, Any]
    scope: ReuseScope
    output_budget_bytes: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def require_tenant_scope_match(self) -> ToolReuseResolveRequest:
        if self.scope.tenant_id != self.identity.tenant_id:
            raise ValueError("reuse scope tenant must match active identity")
        return self


class ResultProvenance(BaseModel):
    reuse_type: ReuseType
    match_kind: ReuseMatchKind = ReuseMatchKind.EXACT
    observed_at: datetime
    result_schema_version: str
    source_query_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    original_size: int = Field(ge=0)
    returned_size: int = Field(ge=0)
    truncation_policy: str
    similarity_score: float | None = Field(default=None, ge=-1, le=1)
    semantic_match_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")

    @model_validator(mode="after")
    def validate_semantic_fields(self) -> ResultProvenance:
        semantic_fields = self.similarity_score is not None or (
            self.semantic_match_id is not None
        )
        if self.match_kind == ReuseMatchKind.SEMANTIC and not (
            self.similarity_score is not None and self.semantic_match_id is not None
        ):
            raise ValueError("semantic provenance requires score and match id")
        if self.match_kind == ReuseMatchKind.EXACT and semantic_fields:
            raise ValueError("exact provenance cannot contain semantic fields")
        return self


class ToolReuseDecision(BaseModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    decision: ReuseDecisionKind
    binding_id: str | None = Field(default=None, pattern=ID_PATTERN)
    descriptor_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    retry_after_ms: int | None = Field(default=None, ge=0)
    result: dict[str, Any] | None = None
    provenance: ResultProvenance | None = None
    match_kind: ReuseMatchKind | None = None
    similarity_score: float | None = Field(default=None, ge=-1, le=1)
    semantic_match_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )
    leader_estimated_remaining_ms: float | None = Field(default=None, ge=0)


class LeaderResultPublish(BaseModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity
    result: dict[str, Any]
    cacheable: bool = True
    ttl_seconds: int | None = Field(default=None, gt=0, le=86400)


class BindingFailureReport(BaseModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity
    error_class: str = Field(min_length=1, max_length=256)


class FollowerCancellation(BaseModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity


class LeaderProgressReport(BaseModel):
    protocol_version: Literal["flowpilot-phase3-reuse-v1"] = (
        SEMANTIC_REUSE_PROTOCOL_VERSION
    )
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity
    sequence: int = Field(ge=1)
    observed_at: datetime
    estimated_remaining_ms: float | None = Field(default=None, ge=0)

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")


class FalseReuseReport(BaseModel):
    protocol_version: Literal["flowpilot-phase3-reuse-v1"] = (
        SEMANTIC_REUSE_PROTOCOL_VERSION
    )
    semantic_match_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    reason: Literal[
        "not_equivalent",
        "stale",
        "scope_mismatch",
        "result_incompatible",
        "other",
    ]
    evidence_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")


class SemanticReusePolicyUpdate(BaseModel):
    protocol_version: Literal["flowpilot-phase3-reuse-v1"] = (
        SEMANTIC_REUSE_PROTOCOL_VERSION
    )
    version: int = Field(ge=1)
    expected_version: int = Field(ge=0)
    enabled: bool
    tool_name: str | None = Field(default=None, min_length=1, max_length=256)
    tenant_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )

    @model_validator(mode="after")
    def validate_policy_update(self) -> SemanticReusePolicyUpdate:
        if self.version != self.expected_version + 1:
            raise ValueError("semantic policy version must follow expected version")
        if (self.tool_name is None) == (self.tenant_id is None):
            raise ValueError("semantic policy must target one Tool or one tenant")
        return self


class DCSState(StrEnum):
    OPEN = "open"
    SYNCING = "syncing"
    ACKED = "acked"
    ABORTED = "aborted"
    DIVERGED = "diverged"


class DCSBarrierReason(StrEnum):
    LOCAL_TOOL = "local_tool"
    TERMINAL_RESPONSE = "terminal_response"
    CAPACITY = "capacity"
    TTL = "ttl"
    LEASE_EXPIRED = "lease_expired"
    FAILURE = "failure"
    ROLLING_UPGRADE = "rolling_upgrade"


class DCSReuseKind(StrEnum):
    EXACT_HISTORICAL = "exact_historical"
    EXACT_INFLIGHT = "exact_inflight"
    SEMANTIC_HISTORICAL = "semantic_historical"
    SEMANTIC_INFLIGHT = "semantic_inflight"


class DelegationPolicy(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    policy_version: int = Field(ge=1)
    expected_policy_version: int = Field(default=0, ge=0)
    lease_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    base_context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    issued_at: datetime
    expires_at: datetime
    allowed_tool_names: tuple[str, ...] = Field(min_length=1)
    max_messages: int = Field(default=32, gt=0, le=1024)
    max_bytes: int = Field(default=1_000_000, gt=0)
    max_internal_continuations: int = Field(default=8, gt=0, le=128)
    delta_ttl_seconds: float = Field(default=300.0, gt=0, le=86400)
    api_kind: Literal["chat", "responses"]
    request_snapshot: dict[str, Any]

    @field_validator("issued_at", "expires_at")
    @classmethod
    def require_aware_policy_time(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "delegation timestamp")

    @model_validator(mode="after")
    def validate_policy(self) -> DelegationPolicy:
        if self.expires_at <= self.issued_at:
            raise ValueError("delegation expiry must follow issuance")
        if self.policy_version != self.expected_policy_version + 1:
            raise ValueError("policy_version must atomically follow expected version")
        if len(set(self.allowed_tool_names)) != len(self.allowed_tool_names):
            raise ValueError("allowed_tool_names must be unique")
        return self


class DCSReference(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    lease_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    delta_digest: str = Field(pattern=HEX_DIGEST_PATTERN)


class DeferredReuseResolveRequest(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    reuse: ToolReuseResolveRequest
    delegation: DCSReference

    @model_validator(mode="after")
    def validate_identity(self) -> DeferredReuseResolveRequest:
        identity = self.reuse.identity
        reference = self.delegation
        if (identity.tenant_id, identity.job_id, identity.line_id) != (
            reference.tenant_id,
            reference.job_id,
            reference.line_id,
        ):
            raise ValueError("delegation and reuse identities must match")
        return self


class DeferredBindingPoll(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    reuse: ToolReuseResolveRequest
    delegation: DCSReference

    @model_validator(mode="after")
    def validate_identity(self) -> DeferredBindingPoll:
        identity = self.reuse.identity
        reference = self.delegation
        if (identity.tenant_id, identity.job_id, identity.line_id) != (
            reference.tenant_id,
            reference.job_id,
            reference.line_id,
        ):
            raise ValueError("delegation and reuse identities must match")
        return self


class ContextDeltaAppend(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    reference: DCSReference
    expected_last_seq: int = Field(ge=0)
    parent_llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    messages: tuple[dict[str, Any], ...] = Field(min_length=2)
    tool_call_ids: tuple[str, ...] = Field(min_length=1)
    resolution_receipts: tuple[str, ...] = Field(min_length=1)
    result_digests: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_resolution_alignment(self) -> ContextDeltaAppend:
        if len(set(self.tool_call_ids)) != len(self.tool_call_ids):
            raise ValueError("tool_call_ids must be unique")
        count = len(self.tool_call_ids)
        if len(self.resolution_receipts) != count or len(self.result_digests) != count:
            raise ValueError(
                "resolution_receipts and result_digests must align with tool_call_ids"
            )
        if len(set(self.resolution_receipts)) != count:
            raise ValueError("resolution_receipts must be unique")
        if any(
            not re.fullmatch(HEX_DIGEST_PATTERN, digest)
            for digest in self.result_digests
        ):
            raise ValueError("result_digests must contain SHA-256 hex digests")
        return self


class ContextSyncBegin(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    reference: DCSReference
    barrier_reason: DCSBarrierReason
    max_messages: int | None = Field(default=None, gt=0, le=1024)
    parent_llm_call_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )
    barrier_messages: tuple[dict[str, Any], ...] = ()
    pending_local_tool_call_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_barrier_envelope(self) -> ContextSyncBegin:
        if self.barrier_messages and self.parent_llm_call_id is None:
            raise ValueError("barrier messages require parent_llm_call_id")
        if len(set(self.pending_local_tool_call_ids)) != len(
            self.pending_local_tool_call_ids
        ):
            raise ValueError("pending_local_tool_call_ids must be unique")
        return self


class ContextSyncAck(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    reference: DCSReference
    first_seq: int = Field(ge=1)
    last_seq: int = Field(ge=1)
    delta_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    new_context_cursor: str = Field(min_length=1, max_length=256)
    new_context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_range(self) -> ContextSyncAck:
        if self.last_seq < self.first_seq:
            raise ValueError("last_seq must not precede first_seq")
        return self


class ContextReconcileRequest(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    tenant_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    context_cursor: str = Field(min_length=1, max_length=256)
    context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)


class InternalContinuationRequest(BaseModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v1"] = DCS_PROTOCOL_VERSION
    reference: DCSReference
    parent_llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
