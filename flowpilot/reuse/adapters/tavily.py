from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from flowpilot.reuse.command_line import normalize_url
from flowpilot.reuse.contracts import canonical_json, digest, secret_dependent

TAVILY_SCHEMAS = json.loads(Path(__file__).with_name("tavily_schema.json").read_text())
TAVILY_SCHEMA_DIGESTS = {
    name: digest(schema) for name, schema in TAVILY_SCHEMAS.items()
}

SEARCH_FIELDS = {
    "query",
    "search_depth",
    "topic",
    "days",
    "time_range",
    "max_results",
    "include_images",
    "include_image_descriptions",
    "include_raw_content",
    "include_domains",
    "exclude_domains",
}
EXTRACT_FIELDS = {"urls", "extract_depth", "include_images"}


def validate_mcp_observation(observation: Any, tool_name: str) -> bool:
    if not isinstance(observation, dict) or observation.get("tool_name") != tool_name:
        return False
    if (
        observation.get("is_error") is not False
        or observation.get("kind") != "MCPToolObservation"
    ):
        return False
    content = observation.get("content")
    if not isinstance(content, list) or not content:
        return False
    return all(
        isinstance(block, dict)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
        for block in content
    )


class TavilySearchAdapter:
    adapter_id = "tavily_search_mcp_v1"
    adapter_version = "1"
    executor_kind = "mcp"

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        if (
            tool_name != "tavily-search"
            or set(arguments) - SEARCH_FIELDS
            or secret_dependent(arguments)
        ):
            return None
        if (
            not isinstance(arguments.get("query"), str)
            or not arguments["query"].strip()
        ):
            return None
        for key, values in (
            ("search_depth", ("basic", "advanced")),
            ("topic", ("general", "news")),
            ("time_range", ("day", "week", "month", "year", "d", "w", "m", "y")),
        ):
            if key in arguments and arguments[key] not in values:
                return None
        for key in ("days", "max_results"):
            if key in arguments:
                value = arguments[key]
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    return None
                if key == "max_results" and not 5 <= value <= 20:
                    return None
        for key in (
            "include_images",
            "include_image_descriptions",
            "include_raw_content",
        ):
            if key in arguments and not isinstance(arguments[key], bool):
                return None
        for key in ("include_domains", "exclude_domains"):
            if key in arguments and (
                not isinstance(arguments[key], list)
                or any(not isinstance(v, str) for v in arguments[key])
            ):
                return None
        return dict(arguments)

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        result = dict(parsed)
        for key in ("days", "max_results"):
            if key in result:
                result[key] = float(result[key])
        for key in ("include_domains", "exclude_domains"):
            result[key] = sorted({v.casefold() for v in result.get(key, [])})
        return result

    def build_semantic_text(self, canonical: dict[str, Any]) -> str | None:
        if canonical.get("topic", "general") != "general" or "time_range" in canonical:
            return None
        # 0.2.1 overrides topic when the query contains this substring.
        if "news" in canonical["query"].lower():
            return None
        return canonical["query"]

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        return validate_mcp_observation(observation, "tavily-search")

    def adapt_result(
        self, observation: dict[str, Any], output_budget: int | None = None
    ) -> dict[str, Any]:
        if (
            output_budget is not None
            and len(canonical_json(observation).encode()) > output_budget
        ):
            raise ValueError("budget_exceeded")
        return observation


class TavilyExtractAdapter(TavilySearchAdapter):
    adapter_id = "tavily_extract_mcp_v1"

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        if (
            tool_name != "tavily-extract"
            or set(arguments) - EXTRACT_FIELDS
            or secret_dependent(arguments)
        ):
            return None
        urls = arguments.get("urls")
        if (
            not isinstance(urls, list)
            or not urls
            or any(not isinstance(v, str) or normalize_url(v) is None for v in urls)
        ):
            return None
        if "extract_depth" in arguments and arguments["extract_depth"] not in (
            "basic",
            "advanced",
        ):
            return None
        if "include_images" in arguments and not isinstance(
            arguments["include_images"], bool
        ):
            return None
        return dict(arguments)

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        return {**parsed, "urls": [normalize_url(v) for v in parsed["urls"]]}

    def build_semantic_text(self, canonical: dict[str, Any]) -> None:
        return None

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        return validate_mcp_observation(observation, "tavily-extract")


class TavilySiteAdapter(TavilySearchAdapter):
    """Pinned 0.2.1 Crawl/Map; retain every traversal parameter for exact reuse.

    Crawl's MCP formatter exposes only 200-character previews per page. Reuse
    preserves that actual Observation and never claims to recover full pages.
    """

    def __init__(self, tool_name: str) -> None:
        self.tool_name = tool_name
        self.adapter_id = tool_name.replace("-", "_") + "_mcp_v1"

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        fields = TAVILY_SCHEMAS[self.tool_name]["properties"]
        if (
            tool_name != self.tool_name
            or set(arguments) - fields.keys()
            or secret_dependent(arguments)
        ):
            return None
        if (
            not isinstance(arguments.get("url"), str)
            or normalize_url(arguments["url"]) is None
        ):
            return None
        for key, value in arguments.items():
            schema = fields[key]
            kind = schema["type"]
            if kind == "integer" and (
                type(value) is not int or value < schema["minimum"]
            ):
                return None
            if kind == "boolean" and type(value) is not bool:
                return None
            if kind == "string" and (
                not isinstance(value, str)
                or ("enum" in schema and value not in schema["enum"])
            ):
                return None
            if kind == "array" and (
                not isinstance(value, list)
                or any(
                    not isinstance(item, str)
                    or (
                        "enum" in schema["items"]
                        and item not in schema["items"]["enum"]
                    )
                    for item in value
                )
            ):
                return None
        return dict(arguments)

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        return {**parsed, "url": normalize_url(parsed["url"])}

    def build_semantic_text(self, canonical: dict[str, Any]) -> None:
        return None

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        return validate_mcp_observation(observation, self.tool_name)


def parse_tavily_search(arguments: dict[str, Any]) -> dict[str, Any] | None:
    return TavilySearchAdapter().parse_tool_call("tavily-search", arguments)


def canonicalize_tavily_search(arguments: dict[str, Any]) -> dict[str, Any] | None:
    parsed = parse_tavily_search(arguments)
    return (
        None if parsed is None else TavilySearchAdapter().canonicalize_arguments(parsed)
    )


def parse_tavily_extract(arguments: dict[str, Any]) -> dict[str, Any] | None:
    return TavilyExtractAdapter().parse_tool_call("tavily-extract", arguments)


def canonicalize_tavily_extract(arguments: dict[str, Any]) -> dict[str, Any] | None:
    parsed = parse_tavily_extract(arguments)
    return (
        None
        if parsed is None
        else TavilyExtractAdapter().canonicalize_arguments(parsed)
    )
