"""Contracts for Tool-specific reuse adapters.

Adapters own vendor envelopes and execution eligibility; the controller only
handles descriptors, leases and persistence.
"""

from __future__ import annotations

from typing import Any, Protocol


class ReuseAdapter(Protocol):
    adapter_id: str
    adapter_version: str
    executor_kind: str

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None: ...
    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]: ...
    def build_semantic_text(self, canonical: dict[str, Any]) -> str | None: ...
    def validate_result(
        self, observation: Any, execution_receipt: Any = None
    ) -> bool: ...
    def adapt_result(
        self, observation: dict[str, Any], output_budget: int | None = None
    ) -> dict[str, Any]: ...
