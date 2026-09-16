from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any


class ReuseConflict(ValueError):
    pass


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ReuseConflict("value is not canonical JSON") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def provider_reuse_content(result: dict[str, Any], provenance: dict[str, Any]) -> str:
    allowed = {
        key: provenance[key]
        for key in ("reuse_type", "observed_at", "result_schema_version")
    }
    content = result.get("content")
    if isinstance(content, list) and all(
        isinstance(block, dict)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
        for block in content
    ):
        body = "\n".join(block["text"] for block in content)
    else:
        body = canonical_json(result)
    return body + "\n[FlowPilot reuse provenance: " + canonical_json(allowed) + "]"


@dataclass(frozen=True)
class TrustedContext:
    deployment_id: str
    namespace_id: str

    def __post_init__(self) -> None:
        if not self.deployment_id or not self.namespace_id:
            raise ReuseConflict("reuse namespace is unavailable")


def secret_dependent(value: Any) -> bool:
    if isinstance(value, dict):
        return any(secret_dependent(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(secret_dependent(v) for v in value)
    return isinstance(value, str) and bool(re.search(r"\$(?:\{|[A-Za-z_])", value))


def reject_sensitive(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in {
                "authorization",
                "cookie",
                "api_key",
                "apikey",
                "access_token",
                "refresh_token",
                "password",
                "private_key",
                "credential",
                "secret",
                "session",
            }:
                raise ReuseConflict("sensitive_field")
            if normalized in {"provenance", "reuse_provenance"}:
                raise ReuseConflict("replayed_result")
            reject_sensitive(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            reject_sensitive(item)
    elif isinstance(value, str) and re.search(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/=-]{12,}"
        r"|\bAKIA[0-9A-Z]{16}\b|\b(?:sk|rk)-[A-Za-z0-9_-]{16,}\b"
        r"|\b(?:api[_-]?key|access[_-]?token|password|secret)\s*[:=]\s*[^\s,;]{6,}"
        r"|\[FlowPilot reuse provenance:",
        value,
        re.I,
    ):
        raise ReuseConflict("sensitive_or_replayed_text")


def tool_call_key(identity: Any) -> str:
    return canonical_json(identity.model_dump(mode="json", exclude={"action_id"}))
