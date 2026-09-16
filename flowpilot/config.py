from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowpilot.protocol import ToolRegistryEntry


@dataclass(frozen=True, slots=True)
class InferenceInstance:
    instance_id: str
    base_url: str
    models: frozenset[str] = frozenset()
    kv_telemetry_schema: str | None = None
    kv_endpoint: str | None = None
    kv_api_key: str | None = None
    kv_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.kv_telemetry_schema not in {None, "flowpilot-vllm-kv-v2"}:
            raise ValueError(
                "inference instance kv_telemetry_schema must be flowpilot-vllm-kv-v2"
            )
        if self.kv_endpoint is not None and not self.kv_endpoint.startswith(
            ("http://", "https://")
        ):
            raise ValueError("inference instance kv_endpoint must be HTTP(S)")
        if self.kv_timeout_seconds <= 0:
            raise ValueError("inference instance kv_timeout_seconds must be positive")

    def supports(self, model: str) -> bool:
        return not self.models or model in self.models


@dataclass(frozen=True, slots=True)
class Settings:
    instances: tuple[InferenceInstance, ...]
    trace_path: Path
    request_timeout_seconds: float = 120.0
    ingress_api_key: str | None = None
    require_ingress_auth: bool = True
    host: str = "0.0.0.0"
    port: int = 9000
    workers: int = 1
    reuse_enabled: bool = False
    reuse_cache_path: Path = Path("data/reuse-v4.sqlite")
    reuse_deployment_id: str = "local"
    reuse_default_namespace: str | None = "default"
    reuse_maintenance_interval_seconds: float = 60.0
    reuse_max_payload_bytes: int = 512 * 1024 * 1024
    reuse_embedding_model_path: str = "/docker/data/HF_MODELS/Qwen3-Embedding-0.6B"
    reuse_lease_seconds: float = 30.0
    web_tool_registry: tuple[ToolRegistryEntry, ...] = ()
    dcs_enabled: bool = False
    dcs_wal_path: Path = Path("data/flowpilot_dcs.sqlite")
    dcs_encryption_key: str | None = None
    forecast_timeout_seconds: float = 0.25
    forecast_ttl_seconds: float = 30.0
    forecast_min_confidence: float = 0.0
    forecast_top_n: int = 3
    # Forecast consumption is an M4 capability and is explicitly opt-in;
    # M0 forwards requests immediately without starting a predictor task.
    forecast_enabled: bool = False
    tool_catalog_version: str = "default-v1"
    routing_policy: str = "round-robin"
    shared_state_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.instances:
            raise ValueError("at least one inference instance is required")
        if self.require_ingress_auth and not self.ingress_api_key:
            raise ValueError(
                "FLOWPILOT_INGRESS_API_KEY is required when ingress auth is enabled"
            )
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if self.workers != 1 and self.shared_state_path is None:
            raise ValueError(
                "workers > 1 require FLOWPILOT_SHARED_STATE_PATH; fail closed"
            )
        if self.routing_policy not in {
            "round-robin",
            "queue-aware",
            "queue+slo",
            "queue+slo+blocking",
        }:
            raise ValueError("unsupported routing policy")
        if self.reuse_lease_seconds <= 0:
            raise ValueError("reuse_lease_seconds must be positive")
        if (
            self.reuse_maintenance_interval_seconds <= 0
            or self.reuse_max_payload_bytes <= 0
        ):
            raise ValueError("reuse maintenance interval and capacity must be positive")
        if self.reuse_enabled and not self.web_tool_registry:
            raise ValueError(
                "FLOWPILOT_WEB_TOOL_REGISTRY_JSON is required when reuse is enabled"
            )
        if self.dcs_enabled and not self.reuse_enabled:
            raise ValueError("Phase 2 DCS requires exact reuse to be enabled")
        if self.dcs_enabled and not self.dcs_encryption_key:
            raise ValueError(
                "FLOWPILOT_DCS_ENCRYPTION_KEY is required when DCS is enabled"
            )
        if self.forecast_timeout_seconds <= 0 or self.forecast_ttl_seconds <= 0:
            raise ValueError("forecast timeout and TTL must be positive")
        if not 0 <= self.forecast_min_confidence <= 1:
            raise ValueError("forecast_min_confidence must be between 0 and 1")
        if self.forecast_top_n <= 0 or self.forecast_top_n > 32:
            raise ValueError("forecast_top_n must be between 1 and 32")
        if not self.tool_catalog_version:
            raise ValueError("tool_catalog_version must be non-empty")
        identifiers = [item.instance_id for item in self.instances]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("inference instance IDs must be unique")

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            instances=_instances_from_env(),
            trace_path=Path(
                os.getenv("FLOWPILOT_TRACE_PATH", "traces/flowpilot.jsonl")
            ),
            request_timeout_seconds=float(
                os.getenv("FLOWPILOT_REQUEST_TIMEOUT_SECONDS", "120")
            ),
            ingress_api_key=os.getenv("FLOWPILOT_INGRESS_API_KEY") or None,
            require_ingress_auth=_bool_env("FLOWPILOT_REQUIRE_INGRESS_AUTH", True),
            host=os.getenv("FLOWPILOT_HOST", "0.0.0.0"),
            port=int(os.getenv("FLOWPILOT_PORT", "9000")),
            workers=int(os.getenv("FLOWPILOT_WORKERS", "1")),
            reuse_enabled=_bool_env("FLOWPILOT_REUSE_ENABLED", False),
            reuse_cache_path=Path(
                os.getenv("FLOWPILOT_REUSE_CACHE_PATH", "data/reuse-v4.sqlite")
            ),
            reuse_deployment_id=os.getenv("FLOWPILOT_REUSE_DEPLOYMENT_ID", "local"),
            reuse_default_namespace=os.getenv(
                "FLOWPILOT_REUSE_DEFAULT_NAMESPACE", "default"
            )
            or None,
            reuse_lease_seconds=float(os.getenv("FLOWPILOT_REUSE_LEASE_SECONDS", "30")),
            reuse_maintenance_interval_seconds=float(
                os.getenv("FLOWPILOT_REUSE_MAINTENANCE_INTERVAL_SECONDS", "60")
            ),
            reuse_max_payload_bytes=int(
                os.getenv("FLOWPILOT_REUSE_MAX_PAYLOAD_BYTES", "536870912")
            ),
            reuse_embedding_model_path=os.getenv(
                "FLOWPILOT_REUSE_EMBEDDING_MODEL_PATH",
                "/docker/data/HF_MODELS/Qwen3-Embedding-0.6B",
            ),
            web_tool_registry=_registry_from_env(),
            dcs_enabled=_bool_env("FLOWPILOT_DCS_ENABLED", False),
            dcs_wal_path=Path(
                os.getenv("FLOWPILOT_DCS_WAL_PATH", "data/flowpilot_dcs.sqlite")
            ),
            dcs_encryption_key=os.getenv("FLOWPILOT_DCS_ENCRYPTION_KEY") or None,
            forecast_timeout_seconds=float(
                os.getenv("FLOWPILOT_FORECAST_TIMEOUT_SECONDS", "0.25")
            ),
            forecast_ttl_seconds=float(
                os.getenv("FLOWPILOT_FORECAST_TTL_SECONDS", "30")
            ),
            forecast_min_confidence=float(
                os.getenv("FLOWPILOT_FORECAST_MIN_CONFIDENCE", "0")
            ),
            forecast_top_n=int(os.getenv("FLOWPILOT_FORECAST_TOP_N", "3")),
            forecast_enabled=_bool_env("FLOWPILOT_FORECAST_ENABLED", False),
            tool_catalog_version=os.getenv(
                "FLOWPILOT_TOOL_CATALOG_VERSION", "default-v1"
            ),
            routing_policy=os.getenv("FLOWPILOT_ROUTING_POLICY", "round-robin"),
            shared_state_path=(
                Path(value)
                if (value := os.getenv("FLOWPILOT_SHARED_STATE_PATH"))
                else None
            ),
        )


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _instances_from_env() -> tuple[InferenceInstance, ...]:
    raw_json = os.getenv("FLOWPILOT_INSTANCES_JSON")
    if raw_json:
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError("FLOWPILOT_INSTANCES_JSON is invalid JSON") from exc
        if not isinstance(payload, list):
            raise ValueError("FLOWPILOT_INSTANCES_JSON must be a JSON array")
        return tuple(_parse_instance(item) for item in payload)

    urls = [
        item.strip().rstrip("/")
        for item in os.getenv("FLOWPILOT_UPSTREAMS", "").split(",")
        if item.strip()
    ]
    return tuple(
        InferenceInstance(instance_id=f"instance-{index}", base_url=url)
        for index, url in enumerate(urls, start=1)
    )


def _parse_instance(value: Any) -> InferenceInstance:
    if not isinstance(value, dict):
        raise ValueError("each inference instance must be an object")
    instance_id = value.get("id")
    base_url = value.get("base_url")
    models = value.get("models", [])
    kv_telemetry_schema = value.get("kv_telemetry_schema")
    kv_endpoint = value.get("kv_endpoint")
    kv_api_key = value.get("kv_api_key")
    kv_timeout_seconds = value.get("kv_timeout_seconds", 5.0)
    if not isinstance(instance_id, str) or not instance_id:
        raise ValueError("inference instance id must be a non-empty string")
    if not isinstance(base_url, str) or not base_url.startswith(
        ("http://", "https://")
    ):
        raise ValueError("inference instance base_url must be HTTP(S)")
    if not isinstance(models, list) or not all(
        isinstance(item, str) for item in models
    ):
        raise ValueError("inference instance models must be a string array")
    if (
        kv_telemetry_schema is not None
        and kv_telemetry_schema != "flowpilot-vllm-kv-v2"
    ):
        raise ValueError(
            "inference instance kv_telemetry_schema must be flowpilot-vllm-kv-v2"
        )
    if kv_endpoint is not None and (
        not isinstance(kv_endpoint, str)
        or not kv_endpoint.startswith(("http://", "https://"))
    ):
        raise ValueError("inference instance kv_endpoint must be HTTP(S)")
    if kv_api_key is not None and not isinstance(kv_api_key, str):
        raise ValueError("inference instance kv_api_key must be a string")
    if not isinstance(kv_timeout_seconds, (int, float)) or kv_timeout_seconds <= 0:
        raise ValueError("inference instance kv_timeout_seconds must be positive")
    return InferenceInstance(
        instance_id=instance_id,
        base_url=base_url.rstrip("/"),
        models=frozenset(models),
        kv_telemetry_schema=kv_telemetry_schema,
        kv_endpoint=kv_endpoint.rstrip("/") if kv_endpoint else None,
        kv_api_key=kv_api_key,
        kv_timeout_seconds=float(kv_timeout_seconds),
    )


def _registry_from_env() -> tuple[ToolRegistryEntry, ...]:
    raw = os.getenv("FLOWPILOT_WEB_TOOL_REGISTRY_JSON", "[]")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("FLOWPILOT_WEB_TOOL_REGISTRY_JSON is invalid JSON") from exc
    if not isinstance(payload, list):
        raise ValueError("FLOWPILOT_WEB_TOOL_REGISTRY_JSON must be a JSON array")
    return tuple(ToolRegistryEntry.model_validate(item) for item in payload)
