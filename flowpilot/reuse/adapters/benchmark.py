"""Reuse contracts for OpenHands benchmark RetrievalObservation tools."""

from __future__ import annotations

import json
import math
from typing import Any

from flowpilot.reuse.contracts import canonical_json


class BenchmarkRetrievalAdapter:
    adapter_version = "1"
    executor_kind = "openhands_local"

    def __init__(self, tool_name: str, *, native: bool = False) -> None:
        self.tool_name = tool_name
        self.native = native
        self.adapter_id = f"benchmark_{'native_' if native else ''}{tool_name}_v1"

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        if tool_name != self.tool_name:
            return None
        # OpenHands removes these provider annotations before constructing Action.
        values = {
            k: v for k, v in arguments.items() if k not in {"summary", "security_risk"}
        }
        field = (
            "query"
            if tool_name == "search"
            else ("doc_id" if tool_name == "read_document" else "docid")
        )
        if not isinstance(values.get(field), str) or not values[field]:
            return None
        defaults: dict[str, int] = {}
        if tool_name == "search" and not self.native:
            defaults = {"top_k": 5}
        elif tool_name == "read_document":
            defaults = {"start_sentence": 0, "max_sentences": 20}
        elif tool_name == "get_document" and not self.native:
            defaults = {"offset": 0}
        if set(values) - {field, *defaults}:
            return None
        values = {**defaults, **values}
        for key in defaults:
            value = values[key]
            if type(value) is not int or value < (
                1 if key in {"top_k", "max_sentences"} else 0
            ):
                return None
            if key == "top_k" and value > 20:
                return None
            if key == "max_sentences" and value > 100:
                return None
        return values

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        # The backend interprets the original query/ID bytes, including case.
        return dict(parsed)

    def build_semantic_text(self, canonical: dict[str, Any]) -> str | None:
        return canonical["query"] if self.tool_name == "search" else None

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        if (
            not isinstance(observation, dict)
            or observation.get("kind") != "RetrievalObservation"
            or observation.get("is_error") is not False
            or set(observation) - {"kind", "content", "is_error"}
        ):
            return False
        blocks = observation.get("content")
        if not isinstance(blocks, list) or len(blocks) != 1:
            return False
        block = blocks[0]
        if (
            not isinstance(block, dict)
            or block.get("type") != "text"
            or not isinstance(block.get("text"), str)
        ):
            return False
        try:
            text = block["text"].strip()
            values = []
            decoder = json.JSONDecoder()
            while text:
                value, end = decoder.raw_decode(text)
                values.append(value)
                text = text[end:].lstrip()
        except (ValueError, TypeError):
            return False
        if not values:
            return False
        if self.tool_name == "search":
            if len(values) == 1 and isinstance(values[0], list):
                hits = values[0]
            elif self.native:
                # FastMCP may emit one JSON text block per hit; the wrapper joins
                # those blocks with newlines without changing their contents.
                hits = values
            else:
                return False
            return all(self._valid_hit(hit) for hit in hits)
        if len(values) != 1:
            return False
        document = values[0]
        if self.native and document is None:
            return True
        if not isinstance(document, dict) or not isinstance(document.get("docid"), str):
            return False
        if self.tool_name == "read_document":
            sentences = document.get("sentences")
            return (
                isinstance(document.get("title"), str)
                and isinstance(sentences, list)
                and all(
                    isinstance(row, list)
                    and len(row) == 2
                    and type(row[0]) is int
                    and row[0] >= 0
                    and isinstance(row[1], str)
                    for row in sentences
                )
                and "next_sentence" in document
                and _optional_offset(document["next_sentence"])
            )
        if not isinstance(document.get("text"), str):
            return False
        return self.native or (
            all(isinstance(document.get(key), str) for key in ("title", "url"))
            and type(document.get("offset")) is int
            and document["offset"] >= 0
            and type(document.get("truncated")) is bool
            and "next_offset" in document
            and _optional_offset(document["next_offset"])
        )

    def _valid_hit(self, hit: Any) -> bool:
        fields = (
            ("docid", "snippet")
            if self.native
            else ("docid", "title", "url", "snippet")
        )
        if not isinstance(hit, dict) or not all(
            isinstance(hit.get(key), str) for key in fields
        ):
            return False
        if "score" in hit and (
            type(hit["score"]) not in (int, float) or not math.isfinite(hit["score"])
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


def _optional_offset(value: Any) -> bool:
    return value is None or (type(value) is int and value >= 0)


BENCHMARK_ADAPTERS = {
    adapter.adapter_id: adapter
    for adapter in (
        BenchmarkRetrievalAdapter("search"),
        BenchmarkRetrievalAdapter("search", native=True),
        BenchmarkRetrievalAdapter("read_document"),
        BenchmarkRetrievalAdapter("get_document"),
        BenchmarkRetrievalAdapter("get_document", native=True),
    )
}
