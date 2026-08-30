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

    def __post_init__(self) -> None:
        if self.kv_telemetry_schema not in {None, "flowpilot-vllm-kv-v1"}:
            raise ValueError(
                "inference instance kv_telemetry_schema must be flowpilot-vllm-kv-v1"
            )

    def supports(self, model: str) -> bool:
        return not self.models or model in self.models


@dataclass(frozen=True, slots=True)
class Settings:
    instances: tuple[InferenceInstance, ...]
    trace_path: Path
    request_timeout_seconds: float = 120.0
    ingress_api_key: str | None = None
    tenant_api_keys: tuple[tuple[str, str], ...] = ()
    require_ingress_auth: bool = True
    host: str = "0.0.0.0"
    port: int = 9000
    workers: int = 1
    reuse_enabled: bool = False
    reuse_cache_path: Path = Path("data/flowpilot_exact_cache.sqlite")
    reuse_lease_seconds: float = 30.0
    semantic_disabled_tenants: frozenset[str] = frozenset()
    web_tool_registry: tuple[ToolRegistryEntry, ...] = ()
    dcs_enabled: bool = False
    dcs_wal_path: Path = Path("data/flowpilot_dcs.sqlite")
    dcs_encryption_key: str | None = None

    def __post_init__(self) -> None:
        if not self.instances:
            raise ValueError("at least one inference instance is required")
        if self.require_ingress_auth and not (
            self.ingress_api_key or self.tenant_api_keys
        ):
            raise ValueError(
                "FLOWPILOT_INGRESS_API_KEY or FLOWPILOT_TENANT_API_KEYS_JSON is "
                "required when ingress auth is enabled"
            )
        if self.ingress_api_key and self.tenant_api_keys:
            raise ValueError(
                "global and tenant-bound ingress API keys cannot be combined"
            )
        if any(not key or not tenant for key, tenant in self.tenant_api_keys):
            raise ValueError("tenant API key mappings cannot contain empty values")
        keys = [key for key, _tenant in self.tenant_api_keys]
        if len(keys) != len(set(keys)):
            raise ValueError("tenant API keys must be unique")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if self.workers != 1:
            raise ValueError(
                "frontier and in-flight state are process-local and require one worker"
            )
        if self.reuse_lease_seconds <= 0:
            raise ValueError("reuse_lease_seconds must be positive")
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
            tenant_api_keys=_tenant_api_keys_from_env(),
            require_ingress_auth=_bool_env("FLOWPILOT_REQUIRE_INGRESS_AUTH", True),
            host=os.getenv("FLOWPILOT_HOST", "0.0.0.0"),
            port=int(os.getenv("FLOWPILOT_PORT", "9000")),
            workers=int(os.getenv("FLOWPILOT_WORKERS", "1")),
            reuse_enabled=_bool_env("FLOWPILOT_REUSE_ENABLED", False),
            reuse_cache_path=Path(
                os.getenv(
                    "FLOWPILOT_REUSE_CACHE_PATH", "data/flowpilot_exact_cache.sqlite"
                )
            ),
            reuse_lease_seconds=float(os.getenv("FLOWPILOT_REUSE_LEASE_SECONDS", "30")),
            semantic_disabled_tenants=frozenset(
                item.strip()
                for item in os.getenv("FLOWPILOT_SEMANTIC_DISABLED_TENANTS", "").split(
                    ","
                )
                if item.strip()
            ),
            web_tool_registry=_registry_from_env(),
            dcs_enabled=_bool_env("FLOWPILOT_DCS_ENABLED", False),
            dcs_wal_path=Path(
                os.getenv("FLOWPILOT_DCS_WAL_PATH", "data/flowpilot_dcs.sqlite")
            ),
            dcs_encryption_key=os.getenv("FLOWPILOT_DCS_ENCRYPTION_KEY") or None,
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
        and kv_telemetry_schema != "flowpilot-vllm-kv-v1"
    ):
        raise ValueError(
            "inference instance kv_telemetry_schema must be flowpilot-vllm-kv-v1"
        )
    return InferenceInstance(
        instance_id=instance_id,
        base_url=base_url.rstrip("/"),
        models=frozenset(models),
        kv_telemetry_schema=kv_telemetry_schema,
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


def _tenant_api_keys_from_env() -> tuple[tuple[str, str], ...]:
    raw = os.getenv("FLOWPILOT_TENANT_API_KEYS_JSON")
    if not raw:
        return ()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("FLOWPILOT_TENANT_API_KEYS_JSON is invalid JSON") from exc
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(tenant, str)
        for key, tenant in payload.items()
    ):
        raise ValueError(
            "FLOWPILOT_TENANT_API_KEYS_JSON must map API keys to tenant IDs"
        )
    return tuple(payload.items())
