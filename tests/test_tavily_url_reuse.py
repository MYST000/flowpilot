from pathlib import Path

import pytest
from reuse_support import execution, observation, registry, request, service

from flowpilot.reuse.adapters.registry import get_adapter
from flowpilot.reuse.adapters.tavily import TAVILY_SCHEMA_DIGESTS
from flowpilot.reuse.contracts import digest


@pytest.mark.parametrize("family", ["tavily-crawl", "tavily-map"])
@pytest.mark.parametrize("historical", [True, False])
async def test_tavily_site_url_exact_reuse(tmp_path: Path, family, historical):
    svc = service(
        tmp_path / "reuse.sqlite",
        entry=registry(
            tool_name=family,
            canonical_tool_family=family,
            adapter_id=family.replace("-", "_") + "_mcp_v1",
            input_schema_digest=TAVILY_SCHEMA_DIGESTS[family],
        ),
    )
    args = {
        "url": "https://EXAMPLE.com:443",
        "max_depth": 2,
        "limit": 7,
        "select_paths": ["/docs/.*"],
        "instructions": "API documentation",
    }
    leader = await request(svc, "a", tool_name=family, arguments=args)
    first = await svc.resolve(leader)
    assert first.decision == "sync_and_execute_as_leader"
    assert first.input_digest == digest(args)
    report = await execution(
        svc, leader, first, result=observation(family, "MCP output preview")
    )
    if historical:
        await svc.publish(report)
    follower = await request(
        svc, "b", tool_name=family, arguments={**args, "url": "https://example.com/"}
    )
    second = await svc.resolve(follower)
    assert second.descriptor_digest == first.descriptor_digest
    if not historical:
        assert second.binding_id == first.binding_id
        await svc.publish(report)
        second = await svc.poll(second.binding_id, follower.identity)
    assert second.result == report.result
    assert second.match_kind == "exact"
    for key, value in {
        "limit": 8,
        "max_depth": 3,
        "instructions": "other",
        "select_paths": ["/blog/.*"],
        "allow_external": True,
    }.items():
        different = await svc.resolve(
            await request(svc, key, tool_name=family, arguments={**args, key: value})
        )
        assert different.decision == "sync_and_execute_as_leader"


@pytest.mark.parametrize("family", ["tavily-crawl", "tavily-map"])
@pytest.mark.parametrize(
    "extra",
    [
        {"limit": True},
        {"max_depth": 0},
        {"max_breadth": 1.5},
        {"allow_external": "false"},
        {"categories": ["fake"]},
        {"select_paths": [1]},
        {"instructions": "$TOKEN"},
        {"unknown": 1},
    ],
)
def test_tavily_site_pinned_schema_validation(family, extra):
    adapter = get_adapter(family)
    assert (
        adapter.parse_tool_call(family, {"url": "https://example.com", **extra}) is None
    )


def test_tavily_map_is_distinct_from_page_content_extraction():
    adapter = get_adapter("tavily-map")
    assert (
        adapter.parse_tool_call(
            "tavily-map", {"url": "https://example.com", "extract_depth": "advanced"}
        )
        is None
    )
    assert not adapter.validate_result(observation("tavily-crawl"))
    assert (
        adapter.build_semantic_text(
            {"url": "https://example.com", "instructions": "same topic"}
        )
        is None
    )
