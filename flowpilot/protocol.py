from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, overload

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PROTOCOL_VERSION = "flowpilot-phase0-v2"
TRACE_SCHEMA_VERSION = "flowpilot-trace-v2"
REUSE_PROTOCOL_VERSION = "flowpilot-phase1-reuse-v3"
DCS_PROTOCOL_VERSION = "flowpilot-phase2-dcs-v2"
SEMANTIC_REUSE_PROTOCOL_VERSION = "flowpilot-phase3-reuse-v3"
PHASE4_PROTOCOL_VERSION = "flowpilot-phase4-scheduling-v2"
HEX_DIGEST_PATTERN = r"^[0-9a-f]{64}$"
ID_PATTERN = r"^[A-Za-z0-9_.:@/-]+$"
ReuseProtocolVersion = Literal["flowpilot-phase1-reuse-v3", "flowpilot-phase3-reuse-v3"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_identity_fields(cls, value: Any) -> Any:
        if _contains_legacy_identity_field(value):
            raise ValueError(
                "legacy tenant identity fields are not accepted by the canonical "
                "FlowPilot protocol"
            )
        return value


def _contains_legacy_identity_field(value: Any) -> bool:
    if isinstance(value, Mapping):
        if any(
            str(key).lower()
            in {"tenant", "tenant_id", "tenant_api_keys", "semantic_disabled_tenants"}
            for key in value
        ):
            return True
        return any(_contains_legacy_identity_field(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_legacy_identity_field(item) for item in value)
    return False


@overload
def _require_aware_datetime(value: datetime, field_name: str) -> datetime: ...


@overload
def _require_aware_datetime(value: None, field_name: str) -> None: ...


def _require_aware_datetime(value: datetime | None, field_name: str) -> datetime | None:
    """Reject ambiguous wall-clock timestamps at the protocol boundary."""
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must include a timezone offset")
    return value


class RequestIdentity(StrictModel):
    protocol_version: Literal["flowpilot-phase0-v2"] = PROTOCOL_VERSION
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    # ``request_id`` is the logical request identity.  ``tail_request_id`` is
    # the frontier reference and remains a separate field so a retry can keep
    # the logical request while a new GatewayCall receives a new call id.
    request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    attempt: int = Field(ge=1)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    expected_tail_version: int = Field(ge=0)
    context_epoch: int = Field(ge=1)
    context_sequence: int = Field(ge=0)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    conversation_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    parent_conversation_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )
    parent_line_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    spawn_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    deployment_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    namespace_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
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


class JobRegistration(StrictModel):
    protocol_version: Literal["flowpilot-phase0-v2"] = PROTOCOL_VERSION
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    default_slo_ms: int | None = Field(default=None, gt=0)
    workflow_started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    deadline: datetime | None = None
    # Root conversation aliases are external lineage evidence.  They may be
    # reused as a job only when the caller supplies a stable namespace.
    root_conversation_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )
    deployment_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    namespace_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)

    @field_validator("workflow_started_at", "deadline")
    @classmethod
    def require_aware_job_times(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "job timestamp")


class LineRegistration(StrictModel):
    protocol_version: Literal["flowpilot-phase0-v2"] = PROTOCOL_VERSION
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    context_sequence: int = Field(default=0, ge=0)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    deadline: datetime | None = None
    weight: float = Field(default=1.0, gt=0)
    conversation_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )
    parent_conversation_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )
    parent_line_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    spawn_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    task_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    agent_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    parent_action_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )

    @field_validator("deadline")
    @classmethod
    def require_aware_deadline(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "deadline")


class DependencyUpdate(StrictModel):
    protocol_version: Literal["flowpilot-phase0-v2"] = PROTOCOL_VERSION
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    version: int = Field(ge=1)
    prerequisite_line_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def reject_duplicates(self) -> DependencyUpdate:
        if len(set(self.prerequisite_line_ids)) != len(self.prerequisite_line_ids):
            raise ValueError("prerequisite_line_ids must be unique")
        return self


class LineFinish(StrictModel):
    protocol_version: Literal["flowpilot-phase0-v2"] = PROTOCOL_VERSION
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


class ToolTelemetryEvent(StrictModel):
    protocol_version: Literal["flowpilot-phase0-v2"] = PROTOCOL_VERSION
    event_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    sequence: int = Field(ge=1)
    execution_attempt: int = Field(ge=1)
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    attempt: int = Field(ge=1)
    conversation_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
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
    # Present for reuse-leader telemetry.  Ordinary local Tool telemetry may
    # omit these metadata-only correlation fields.
    binding_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    result_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    result_schema_version: str | None = Field(default=None, max_length=64)
    adapter_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    adapter_version: str | None = Field(default=None, max_length=64, pattern=ID_PATTERN)
    executor_kind: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    final_url_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    reuse_receipt_version: Literal["flowpilot-execution-v1"] | None = None

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")

    @model_validator(mode="after")
    def validate_terminal_fields(self) -> ToolTelemetryEvent:
        if self.binding_id is not None:
            if not all(
                (
                    self.reuse_receipt_version,
                    self.input_digest,
                    self.adapter_id,
                    self.adapter_version,
                    self.result_schema_version,
                    self.executor_kind,
                )
            ):
                raise ValueError(
                    "reuse leader requires versioned execution credentials"
                )
            if self.event_kind == ToolEventKind.FINISH and not self.result_digest:
                raise ValueError("reuse FINISH requires result_digest")
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


class ForecastRequest(StrictModel):
    """Metadata-only request sent to an externally owned Tool predictor."""

    schema_version: Literal["flowpilot-phase4-scheduling-v2"] = PHASE4_PROTOCOL_VERSION
    request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
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


class ForecastCandidate(StrictModel):
    tool_family: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    probability: float = Field(ge=0, le=1)
    duration_p50: float = Field(ge=0)
    duration_p90: float = Field(ge=0)

    @model_validator(mode="after")
    def validate_quantiles(self) -> ForecastCandidate:
        if self.duration_p90 < self.duration_p50:
            raise ValueError("duration_p90 must be >= duration_p50")
        return self


class ForecastResult(StrictModel):
    """Versioned, expiring predictor output; never a Tool execution fact."""

    schema_version: Literal["flowpilot-phase4-scheduling-v2"] = PHASE4_PROTOCOL_VERSION
    based_on_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    # Scope is optional for backwards-compatible phase-4 replay fixtures.  A
    # production result is always persisted together with the originating
    # ForecastRequest scope by ForecastManager/ToolResolutionStore.
    job_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
    line_id: str | None = Field(default=None, max_length=128, pattern=ID_PATTERN)
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


class ToolResolutionRecord(StrictModel):
    """Authoritative Tool readiness fact kept outside the line tail."""

    schema_version: Literal["flowpilot-phase4-scheduling-v2"] = PHASE4_PROTOCOL_VERSION
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


class SchedulingProjection(StrictModel):
    """Short-lived scheduling view derived from current owner facts."""

    schema_version: Literal["flowpilot-phase4-scheduling-v2"] = PHASE4_PROTOCOL_VERSION
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
    dag_importance: float = Field(ge=0)
    slo_urgency: float = Field(ge=0)
    blocking_line_count: int = Field(ge=0)
    wait_age_ms: float = Field(ge=0)
    workflow_age_ms: float = Field(default=0, ge=0)
    scheduler_queue_wait_ms: float | None = Field(default=None, ge=0)
    upstream_queue_wait_ms: float | None = Field(default=None, ge=0)
    critical_path_elapsed_ms: float | None = Field(default=None, ge=0)
    critical_path_remaining_ms: float | None = Field(default=None, ge=0)
    deadline_slack_ms: float | None = None
    computed_at: datetime

    @field_validator("t_need", "computed_at")
    @classmethod
    def require_aware_projection_times(cls, value: datetime | None) -> datetime | None:
        return _require_aware_datetime(value, "projection timestamp")


class InstanceLoadEvent(StrictModel):
    schema_version: Literal["flowpilot-phase5-routing-v2"] = (
        "flowpilot-phase5-routing-v2"
    )
    instance_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    queue_depth: int = Field(ge=0)
    running_requests: int = Field(ge=0)
    ttft_ms: float = Field(ge=0)
    throughput_tokens_per_second: float = Field(gt=0)
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_aware_load_time(cls, value: datetime) -> datetime:
        return _require_aware_datetime(value, "observed_at")


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


class ToolRegistryEntry(StrictModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    tool_name: str = Field(min_length=1, max_length=256)
    canonical_tool_family: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tool_version: str = Field(min_length=1, max_length=64, pattern=ID_PATTERN)
    result_schema_version: str = Field(min_length=1, max_length=64, pattern=ID_PATTERN)
    tool_schema_version: str = Field(
        default="1", min_length=1, max_length=64, pattern=ID_PATTERN
    )
    adapter_id: str = Field(
        default="generic_v1", min_length=1, max_length=128, pattern=ID_PATTERN
    )
    adapter_version: str = Field(
        default="1", min_length=1, max_length=64, pattern=ID_PATTERN
    )
    freshness_policy_id: str = Field(
        default="default", min_length=1, max_length=128, pattern=ID_PATTERN
    )
    url_execution_policy_id: str | None = Field(
        default=None, max_length=128, pattern=ID_PATTERN
    )
    input_schema_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    security_policy_id: str = "masked-observation-v1"
    policy_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    scope_max_ttl_seconds: int | None = Field(default=None, gt=0, le=86400)
    semantic_mode: Literal["shadow", "candidate", "active"] = "shadow"
    read_only: bool = True
    exact_reuse_enabled: bool = True
    semantic_reuse_enabled: bool = False
    # Optional adapter for a terminal-like Tool.  The adapter is deliberately
    # explicit: arbitrary shell commands are never eligible for reuse.
    command_line_reuse: Literal["disabled", "curl_url_exact"] = "disabled"
    semantic_query_fields: tuple[str, ...] = ("query",)
    semantic_similarity_threshold: float = Field(default=0.92, ge=0, le=1)
    semantic_candidate_limit: int = Field(default=100, gt=0, le=10_000)
    semantic_time_sensitivity_classes: tuple[str, ...] = ("standard",)
    default_ttl_seconds: int = Field(default=300, gt=0, le=86400)
    max_ttl_seconds: int | None = Field(default=None, gt=0, le=86400)
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
                "semantic reuse registry entries require flowpilot-phase3-reuse-v3"
            )
        if self.semantic_reuse_enabled and not self.exact_reuse_enabled:
            raise ValueError("semantic reuse requires exact reuse to remain enabled")
        if (
            self.semantic_reuse_enabled
            and self.tool_name == "tavily-search"
            and self.semantic_query_fields != ("query",)
        ):
            raise ValueError("Tavily semantic reuse may soften only the query field")
        return self


class ReuseScope(StrictModel):
    # Authentication is ingress-only; scope is limited to Tool hard constraints.
    locale: str = Field(default="und", min_length=1, max_length=64)
    language: str = Field(default="und", min_length=1, max_length=64)
    region: str = Field(default="global", min_length=1, max_length=64)
    safe_search_policy: str = Field(default="default", min_length=1, max_length=64)
    time_sensitivity_class: str = Field(default="standard", min_length=1, max_length=64)
    data_source_constraints: tuple[str, ...] = ()


class ToolReuseIdentity(StrictModel):
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    tail_request_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    action_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=ID_PATTERN
    )
    tool_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)


class ToolReuseResolveRequest(StrictModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    identity: ToolReuseIdentity
    tool_name: str = Field(min_length=1, max_length=256)
    arguments: dict[str, Any]
    scope: ReuseScope
    output_budget_bytes: int | None = Field(default=None, gt=0)
    input_schema_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)


class ResultProvenance(StrictModel):
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
    expires_at: datetime | None = None
    result_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    origin_id: str | None = Field(
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


class ToolReuseDecision(StrictModel):
    input_schema_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
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
    input_digest: str | None = Field(default=None, pattern=HEX_DIGEST_PATTERN)
    adapter_id: str | None = None
    adapter_version: str | None = None
    result_schema_version: str | None = None
    executor_kind: str | None = None
    reason: str | None = None
    publication: dict[str, Any] | None = None
    semantic_candidates: tuple[dict[str, Any], ...] = ()


class LeaderResultPublish(StrictModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity
    result: dict[str, Any]
    cacheable: bool = True
    ttl_seconds: int | None = Field(default=None, gt=0, le=86400)
    start_event_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    finish_event_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    execution_attempt: int = Field(ge=1)
    input_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    result_digest: str = Field(pattern=HEX_DIGEST_PATTERN)
    result_schema_version: str = Field(min_length=1, max_length=64)
    result_size_bytes: int = Field(ge=0)


class BindingFailureReport(StrictModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity
    error_class: str = Field(min_length=1, max_length=256)


class FollowerCancellation(StrictModel):
    protocol_version: ReuseProtocolVersion = REUSE_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    identity: ToolReuseIdentity


class LeaderProgressReport(StrictModel):
    protocol_version: Literal["flowpilot-phase3-reuse-v3"] = (
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


class FalseReuseReport(StrictModel):
    protocol_version: Literal["flowpilot-phase3-reuse-v3"] = (
        SEMANTIC_REUSE_PROTOCOL_VERSION
    )
    semantic_match_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
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


class SemanticReusePolicyUpdate(StrictModel):
    protocol_version: Literal["flowpilot-phase3-reuse-v3"] = (
        SEMANTIC_REUSE_PROTOCOL_VERSION
    )
    version: int = Field(ge=1)
    expected_version: int = Field(ge=0)
    enabled: bool
    tool_name: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_policy_update(self) -> SemanticReusePolicyUpdate:
        if self.version != self.expected_version + 1:
            raise ValueError("semantic policy version must follow expected version")
        if self.tool_name is None:
            raise ValueError("semantic policy must target one Tool")
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


class DelegationPolicy(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
    policy_version: int = Field(ge=1)
    expected_policy_version: int = Field(default=0, ge=0)
    lease_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
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


class DCSReference(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    lease_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    base_context_cursor: str = Field(min_length=1, max_length=256)
    delta_digest: str = Field(pattern=HEX_DIGEST_PATTERN)


class DeferredReuseResolveRequest(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
    reuse: ToolReuseResolveRequest
    delegation: DCSReference

    @model_validator(mode="after")
    def validate_identity(self) -> DeferredReuseResolveRequest:
        identity = self.reuse.identity
        reference = self.delegation
        if (identity.job_id, identity.line_id) != (reference.job_id, reference.line_id):
            raise ValueError("delegation and reuse identities must match")
        return self


class DeferredBindingPoll(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
    binding_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    reuse: ToolReuseResolveRequest
    delegation: DCSReference

    @model_validator(mode="after")
    def validate_identity(self) -> DeferredBindingPoll:
        identity = self.reuse.identity
        reference = self.delegation
        if (identity.job_id, identity.line_id) != (reference.job_id, reference.line_id):
            raise ValueError("delegation and reuse identities must match")
        return self


class ContextDeltaAppend(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
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


class ContextSyncBegin(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
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


class ContextSyncAck(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
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


class ContextReconcileRequest(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
    job_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    line_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
    context_epoch: int = Field(ge=1)
    context_cursor: str = Field(min_length=1, max_length=256)
    context_digest: str = Field(pattern=HEX_DIGEST_PATTERN)


class InternalContinuationRequest(StrictModel):
    protocol_version: Literal["flowpilot-phase2-dcs-v2"] = DCS_PROTOCOL_VERSION
    reference: DCSReference
    parent_llm_call_id: str = Field(min_length=1, max_length=128, pattern=ID_PATTERN)
