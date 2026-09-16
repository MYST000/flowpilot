from __future__ import annotations

from typing import Any

from flowpilot.reuse.command_line import normalize_curl_url_command
from flowpilot.reuse.contracts import canonical_json


class CurlUrlFetchAdapter:
    adapter_id = "curl_url_fetch_v1"
    adapter_version = "1"
    executor_kind = "isolated_curl_argv"

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        if tool_name not in {"terminal", "curl", "url_fetch"}:
            return None
        return normalize_curl_url_command(arguments)

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        return dict(parsed)

    def build_semantic_text(self, canonical: dict[str, Any]) -> None:
        return None

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        if not isinstance(observation, dict) or execution_receipt is None:
            return False
        return (
            observation.get("kind") == "UrlFetchObservation"
            and observation.get("is_error") is False
            and observation.get("exit_code") == 0
            and observation.get("timeout") is False
            and observation.get("executor_kind") == self.executor_kind
            and observation.get("network_policy_id") == "public-pinned-get-v1"
            and observation.get("network_policy_validated") is True
            and observation.get("complete") is True
            and isinstance(observation.get("status_code"), int)
            and 200 <= observation["status_code"] < 300
            and observation.get("final_url_digest")
            == execution_receipt.final_url_digest
            and isinstance(observation.get("content"), list)
            and all(
                isinstance(v, dict)
                and v.get("type") == "text"
                and isinstance(v.get("text"), str)
                for v in observation["content"]
            )
        )

    def adapt_result(
        self, observation: dict[str, Any], output_budget: int | None = None
    ) -> dict[str, Any]:
        if (
            output_budget is not None
            and len(canonical_json(observation).encode()) > output_budget
        ):
            raise ValueError("budget_exceeded")
        return observation
