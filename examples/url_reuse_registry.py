"""Print FLOWPILOT_WEB_TOOL_REGISTRY_JSON for existing Terminal and Tavily tools."""

import json

from flowpilot.protocol import ToolRegistryEntry
from flowpilot.reuse.adapters.tavily import TAVILY_SCHEMA_DIGESTS


def url_reuse_registry() -> list[dict]:
    entries = [
        ToolRegistryEntry(
            tool_name="terminal",
            canonical_tool_family="terminal_url_fetch",
            tool_version="1",
            result_schema_version="terminal-observation-v1",
            adapter_id="terminal_url_fetch_v1",
            command_line_reuse="url_exact",
        )
    ]
    entries.extend(
        ToolRegistryEntry(
            tool_name=name,
            canonical_tool_family=name.replace("-", "_"),
            tool_version="0.2.1",
            result_schema_version="mcp-observation-v1",
            adapter_id=name.replace("-", "_") + "_mcp_v1",
            input_schema_digest=TAVILY_SCHEMA_DIGESTS[name],
        )
        for name in ("tavily-search", "tavily-extract", "tavily-crawl", "tavily-map")
    )
    return [entry.model_dump(mode="json", exclude_defaults=True) for entry in entries]


if __name__ == "__main__":
    print(json.dumps(url_reuse_registry(), indent=2))
