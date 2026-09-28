import json

import pytest
from reuse_support import execution, observation, registry, request, service

from flowpilot.protocol import ReuseScope
from flowpilot.reuse.adapters.browsecomp import BrowseCompSearchAdapter
from flowpilot.reuse.adapters.registry import get_adapter
from flowpilot.reuse.contracts import ReuseConflict, digest
from flowpilot.reuse.semantic import TestHashingEmbedder

SCHEMA = {
    "properties": {"query": {"title": "Query", "type": "string"}},
    "required": ["query"],
    "type": "object",
}
PROFILE = digest({"corpus": "test-v1", "retriever": "bm25", "k": 5})


def search_registry(**updates):
    return registry(
        **{
            "tool_name": "search",
            "canonical_tool_family": "browsecomp_corpus_search",
            "tool_version": "1",
            "adapter_id": BrowseCompSearchAdapter.adapter_id,
            "input_schema_digest": digest(SCHEMA),
            "policy_digest": PROFILE,
            **updates,
        }
    )


async def search_request(svc, line, *, profile=PROFILE, **kwargs):
    req = await request(svc, line, tool_name="search", **kwargs)
    return req.model_copy(
        update={
            "input_schema_digest": digest(SCHEMA),
            "scope": ReuseScope(
                data_source_constraints=(f"browsecomp-search:{profile}",)
            ),
        }
    )


def search_result():
    result = observation(
        "search",
        json.dumps(
            [
                {"docid": "10", "score": 1.25, "snippet": "Evidence [10]"},
                {"docid": "20", "snippet": "Evidence without score"},
            ]
        ),
    )
    result["content"].insert(0, {"type": "text", "text": "[Tool 'search' executed.]"})
    return result


@pytest.mark.parametrize("historical", [False, True])
async def test_search_exact_history_and_inflight(tmp_path, historical):
    svc = service(tmp_path / "reuse.sqlite", entry=search_registry())
    leader = await search_request(svc, "a", query='Who wrote "Book A"?')
    first = await svc.resolve(leader)
    assert first.decision == "sync_and_execute_as_leader"
    assert first.input_digest == digest(leader.arguments)
    report = await execution(svc, leader, first, result=search_result())
    if historical:
        await svc.publish(report)
    follower = await search_request(svc, "b", query=leader.arguments["query"])
    second = await svc.resolve(follower)
    if not historical:
        assert second.decision == "wait_and_sync_reused_result"
        assert second.binding_id == first.binding_id
        await svc.publish(report)
        second = await svc.poll(second.binding_id, follower.identity)
    assert second.result == report.result
    assert second.match_kind == "exact"
    assert second.provenance.result_digest == digest(report.result)
    assert follower.identity.tool_call_id != leader.identity.tool_call_id


@pytest.mark.parametrize("query", ["query", "Query ", " Query", '"Query"'])
async def test_search_exact_does_not_rewrite_retriever_query(tmp_path, query):
    svc = service(tmp_path / "reuse.sqlite", entry=search_registry())
    original = await search_request(svc, "a", query="Query")
    await svc.publish(
        await execution(
            svc, original, await svc.resolve(original), result=search_result()
        )
    )
    changed = await svc.resolve(await search_request(svc, "b", query=query))
    assert changed.decision == "sync_and_execute_as_leader"


@pytest.mark.parametrize("mismatch", ["profile", "missing_schema", "schema"])
async def test_search_profile_and_schema_must_match(tmp_path, mismatch):
    svc = service(tmp_path / "reuse.sqlite", entry=search_registry())
    req = await search_request(svc, "a")
    if mismatch == "profile":
        req = req.model_copy(
            update={
                "scope": ReuseScope(
                    data_source_constraints=("browsecomp-search:" + "0" * 64,)
                )
            }
        )
    else:
        req = req.model_copy(
            update={
                "input_schema_digest": None
                if mismatch == "missing_schema"
                else "0" * 64
            }
        )
    assert (await svc.resolve(req)).decision == "execute_locally"


@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("mode", ["candidate", "active"])
async def test_search_semantic_matches_use_query_and_keep_profile(
    tmp_path, mode, historical
):
    svc = service(
        tmp_path / "reuse.sqlite",
        entry=search_registry(
            protocol_version="flowpilot-phase3-reuse-v3",
            semantic_reuse_enabled=True,
            semantic_mode=mode,
            semantic_similarity_threshold=0.9,
        ),
        embedder=TestHashingEmbedder(),
    )
    leader = await search_request(svc, "a", query="alpha beta", semantic=True)
    first = await svc.resolve(leader)
    report = await execution(svc, leader, first, result=search_result())
    if historical:
        await svc.publish(report)
        await svc.controller.rebuild_vectors()
    follower = await search_request(svc, "b", query="beta alpha", semantic=True)
    second = await svc.resolve(follower)
    if mode == "candidate":
        assert second.decision == "sync_and_execute_as_leader"
        assert second.semantic_candidates
    else:
        if not historical:
            assert second.decision == "wait_and_sync_reused_result"
            await svc.publish(report)
            second = await svc.poll(second.binding_id, follower.identity)
        assert second.match_kind == "semantic"
        assert second.result == report.result
    different = await search_request(
        svc, "c", query="beta alpha", semantic=True, profile=digest({"k": 10})
    )
    assert (await svc.resolve(different)).decision == "execute_locally"


@pytest.mark.parametrize(
    "text",
    [
        "tool failed",
        '{"error":"bad query"}',
        '[{"docid":"a"}]',
        '[{"docid":1,"snippet":"s"}]',
        '[{"docid":"a","snippet":"s","score":NaN}]',
    ],
)
async def test_search_rejects_malformed_results(tmp_path, text):
    svc = service(tmp_path / "reuse.sqlite", entry=search_registry())
    req = await search_request(svc, "a")
    report = await execution(
        svc, req, await svc.resolve(req), result=observation("search", text)
    )
    with pytest.raises(ReuseConflict, match="adapter rejected"):
        await svc.publish(report)


def test_search_adapter_handles_native_fastmcp_shapes_and_explicit_alias():
    assert get_adapter("search") is None
    adapter = get_adapter("corpus_search", BrowseCompSearchAdapter.adapter_id)
    assert adapter is not None
    result = observation("corpus_search", "[]")
    assert adapter.validate_result(result)
    result["content"] = [
        {"type": "text", "text": json.dumps({"docid": docid, "snippet": "s"})}
        for docid in ("1", "2")
    ]
    assert adapter.validate_result(result)
    result["is_error"] = True
    assert not adapter.validate_result(result)
    assert adapter.parse_tool_call("corpus_search", {"query": "x", "k": 10}) is None
