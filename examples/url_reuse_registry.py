"""Optional Terminal URL profile; benchmark profiles use reuse_profile export."""

import json

from flowpilot.protocol import ToolRegistryEntry


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
    return [entry.model_dump(mode="json", exclude_defaults=True) for entry in entries]


if __name__ == "__main__":
    print(json.dumps(url_reuse_registry(), indent=2))
