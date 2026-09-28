"""BrowseComp-Plus's corpus search, executed through OpenHands' MCP client."""

from __future__ import annotations

import json
import math
from typing import Any

from flowpilot.reuse.contracts import canonical_json

from .tavily import validate_mcp_observation


class BrowseCompSearchAdapter:
    adapter_id = "browsecomp_search_mcp_v1"
    adapter_version = "1"
    executor_kind = "mcp"

    def __init__(self, tool_name: str = "search") -> None:
        self.tool_name = tool_name

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        if (
            tool_name != self.tool_name
            or set(arguments) != {"query"}
            or not isinstance(arguments["query"], str)
            or not arguments["query"].strip()
        ):
            return None
        return dict(arguments)

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        # Lucene and dense retrievers interpret the original query themselves.
        # Even punctuation/case/whitespace normalization is not exact equivalence.
        return dict(parsed)

    def build_semantic_text(self, canonical: dict[str, Any]) -> str:
        return canonical["query"]

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        if not validate_mcp_observation(observation, self.tool_name):
            return False
        blocks = observation["content"]
        if blocks[0]["text"] == f"[Tool '{self.tool_name}' executed.]":
            blocks = blocks[1:]
        if not blocks:
            return False
        for block in blocks:
            try:
                value = json.loads(block["text"])
            except (ValueError, TypeError):
                return False
            # FastMCP 2 returns one JSON array; 3 may emit one block per hit.
            hits = value if isinstance(value, list) else [value]
            for hit in hits:
                if (
                    not isinstance(hit, dict)
                    or set(hit) - {"docid", "score", "snippet"}
                    or not isinstance(hit.get("docid"), str)
                    or not isinstance(hit.get("snippet"), str)
                ):
                    return False
                if "score" in hit and (
                    type(hit["score"]) not in (int, float)
                    or not math.isfinite(hit["score"])
                ):
                    return False
        return True

    def adapt_result(
        self, observation: dict[str, Any], output_budget: int | None = None
    ) -> dict[str, Any]:
        if (
            output_budget is not None
            and len(canonical_json(observation).encode()) > output_budget
        ):
            raise ValueError("budget_exceeded")
        return observation
