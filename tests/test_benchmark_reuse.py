import json

import pytest
from cryptography.fernet import Fernet
from reuse_support import execution, request, service

from examples.experiments.qwen35_9b_tp4.profile import gateway_settings, load_profile
from flowpilot.protocol import ReuseScope, ToolRegistryEntry
from flowpilot.reuse.adapters.benchmark import BENCHMARK_ADAPTERS
from flowpilot.reuse.contracts import ReuseConflict, digest
from flowpilot.reuse.controller import WebReuseController
from flowpilot.reuse.semantic import TestHashingEmbedder


def entry(name="search", *, native=False, corpus="hotpot-v1", **updates):
    profile = digest({"corpus": corpus, "native": native})
    return ToolRegistryEntry.model_validate(
        {
            "protocol_version": "flowpilot-phase3-reuse-v3",
            "tool_name": name,
            "canonical_tool_family": "benchmark_" + name,
            "tool_version": "1",
            "adapter_id": f"benchmark_{'native_' if native else ''}{name}_v1",
            "input_schema_digest": digest({"name": name, "native": native}),
            "result_schema_version": "retrieval-observation-v1",
            "policy_digest": profile,
            "required_data_source_constraints": (f"benchmark-retrieval:{profile}",),
            "semantic_reuse_enabled": name == "search",
            "semantic_mode": "active",
            **updates,
        }
    )


async def call(svc, line, registry, arguments):
    req = await request(
        svc, line, tool_name=registry.tool_name, arguments=arguments, semantic=True
    )
    return req.model_copy(
        update={
            "input_schema_digest": registry.input_schema_digest,
            "scope": ReuseScope(
                data_source_constraints=registry.required_data_source_constraints
            ),
        }
    )


def observation(value):
    return {
        "kind": "RetrievalObservation",
        "is_error": False,
        "content": [{"type": "text", "cache_prompt": False, "text": json.dumps(value)}],
    }


CASES = [
    (
        "search",
        False,
        {"query": "Alpha"},
        {"query": "Alpha", "top_k": 5},
        [{"docid": "1", "title": "Alpha", "url": "", "snippet": "Alpha."}],
    ),
    (
        "search",
        True,
        {"query": "Alpha"},
        {"query": "Alpha"},
        [{"docid": "1", "snippet": "Alpha.", "score": 1.25}],
    ),
    (
        "read_document",
        False,
        {"doc_id": "1"},
        {"doc_id": "1", "start_sentence": 0, "max_sentences": 20},
        {
            "docid": "1",
            "title": "Alpha",
            "sentences": [[0, "Alpha."]],
            "next_sentence": None,
        },
    ),
    (
        "get_document",
        False,
        {"docid": "1"},
        {"docid": "1", "offset": 0},
        {
            "docid": "1",
            "title": "Alpha",
            "url": "",
            "text": "Alpha.",
            "offset": 0,
            "truncated": False,
            "next_offset": None,
        },
    ),
    (
        "get_document",
        True,
        {"docid": "1"},
        {"docid": "1"},
        {"docid": "1", "text": "Alpha."},
    ),
]


@pytest.mark.parametrize("name,native,raw,effective,value", CASES)
@pytest.mark.parametrize("historical", [False, True])
async def test_benchmark_history_and_inflight_preserve_observation(
    tmp_path, name, native, raw, effective, value, historical
):
    registry = entry(name, native=native)
    svc = service(tmp_path / "reuse.sqlite", entry=registry)
    leader = await call(svc, "leader", registry, {**raw, "summary": "Find evidence"})
    first = await svc.resolve(leader)
    assert first.input_digest == digest(effective)
    report = await execution(svc, leader, first, result=observation(value))
    if historical:
        await svc.publish(report)
    follower = await call(svc, "follower", registry, effective)
    result = await svc.resolve(follower)
    if not historical:
        assert result.decision == "wait_and_sync_reused_result"
        await svc.publish(report)
        result = await svc.poll(result.binding_id, follower.identity)
    assert result.result == report.result
    assert result.match_kind == "exact"
    assert leader.identity.tool_call_id != follower.identity.tool_call_id


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("mode", ["shadow", "candidate", "active"])
async def test_search_semantics_only_replace_in_active_mode(
    tmp_path, native, historical, mode
):
    registry = entry(native=native, semantic_mode=mode)
    svc = service(
        tmp_path / "reuse.sqlite", entry=registry, embedder=TestHashingEmbedder()
    )
    leader = await call(svc, "leader", registry, {"query": "alpha beta"})
    first = await svc.resolve(leader)
    report = await execution(svc, leader, first, result=observation([]))
    if historical:
        await svc.publish(report)
        await svc.controller.rebuild_vectors()
    follower = await call(svc, "follower", registry, {"query": "beta alpha"})
    result = await svc.resolve(follower)
    if mode == "active":
        if not historical:
            assert result.decision == "wait_and_sync_reused_result"
            await svc.publish(report)
            result = await svc.poll(result.binding_id, follower.identity)
        assert result.match_kind == "semantic"
        assert result.result == report.result
    else:
        assert result.decision == "sync_and_execute_as_leader"
        assert bool(result.semantic_candidates) == (mode == "candidate")
    if not native:
        changed = await call(svc, "topk", registry, {"query": "beta alpha", "top_k": 3})
        assert (await svc.resolve(changed)).decision == "sync_and_execute_as_leader"


async def test_same_names_select_by_profile_and_schema_and_isolate_results(tmp_path):
    registries = (
        entry(),
        entry(corpus="browsecomp-v1"),
        entry(native=True),
        entry(corpus="hotpot-v2"),
    )
    svc = service(tmp_path / "unused.sqlite")
    svc.controller = WebReuseController(
        registries,
        tmp_path / "reuse.sqlite",
        frontier=svc.frontier,
        embedder=TestHashingEmbedder(),
    )
    for index, registry in enumerate(registries):
        leader = await call(svc, str(index), registry, {"query": "alpha beta"})
        first = await svc.resolve(leader)
        assert first.decision == "sync_and_execute_as_leader"
        await svc.publish(await execution(svc, leader, first, result=observation([])))
        await svc.controller.rebuild_vectors()
    req = await call(svc, "missing", registries[0], {"query": "alpha beta"})
    assert (
        await svc.resolve(req.model_copy(update={"scope": ReuseScope()}))
    ).decision == "execute_locally"
    assert (
        await svc.resolve(req.model_copy(update={"input_schema_digest": None}))
    ).decision == "execute_locally"
    ambiguous = req.model_copy(
        update={
            "scope": ReuseScope(
                data_source_constraints=(
                    *registries[0].required_data_source_constraints,
                    *registries[1].required_data_source_constraints,
                )
            )
        }
    )
    assert (await svc.resolve(ambiguous)).decision == "execute_locally"


@pytest.mark.parametrize("name,native,raw,effective,value", CASES[2:])
async def test_document_ids_and_ranges_are_exact_only(
    tmp_path, name, native, raw, effective, value
):
    registry = entry(name, native=native)
    svc = service(
        tmp_path / "reuse.sqlite", entry=registry, embedder=TestHashingEmbedder()
    )
    leader = await call(svc, "leader", registry, effective)
    await svc.publish(
        await execution(
            svc, leader, await svc.resolve(leader), result=observation(value)
        )
    )
    fields = ["doc_id" if name == "read_document" else "docid"]
    fields += [key for key in effective if isinstance(effective[key], int)]
    for field in fields:
        changed = dict(effective)
        changed[field] = (
            effective[field] + 1 if isinstance(effective[field], int) else "2"
        )
        req = await call(svc, field, registry, changed)
        assert (await svc.resolve(req)).decision == "sync_and_execute_as_leader"
    with pytest.raises(ValueError, match="exact reuse only"):
        entry(name, native=native, semantic_reuse_enabled=True)


@pytest.mark.parametrize(
    "bad",
    [
        "failure",
        {"error": "failed"},
        [{"docid": "1"}],
        [{"docid": "1", "snippet": "s", "score": float("nan")}],
    ],
)
async def test_invalid_result_is_not_published(tmp_path, bad):
    registry = entry(native=True)
    svc = service(tmp_path / "reuse.sqlite", entry=registry)
    leader = await call(svc, "leader", registry, {"query": "alpha"})
    report = await execution(
        svc, leader, await svc.resolve(leader), result=observation(bad)
    )
    with pytest.raises(ReuseConflict, match="adapter rejected"):
        await svc.publish(report)


def test_native_wrapper_json_lines_and_no_reader_embeddings():
    adapter = BENCHMARK_ADAPTERS["benchmark_native_search_v1"]
    result = observation([])
    result["content"][0]["text"] = "\n".join(
        json.dumps({"docid": str(i), "snippet": "text"}) for i in range(2)
    )
    assert adapter.validate_result(result)
    result["is_error"] = True
    assert not adapter.validate_result(result)
    for name, adapter in BENCHMARK_ADAPTERS.items():
        if "search" not in name:
            assert adapter.build_semantic_text({"docid": "Alpha"}) is None


def test_exact_query_is_not_normalized_and_unknown_parameters_are_ineligible():
    adapter = BENCHMARK_ADAPTERS["benchmark_search_v1"]
    assert adapter.parse_tool_call("search", {"query": " Alpha! "}) == {
        "query": " Alpha! ",
        "top_k": 5,
    }
    for arguments in ({"query": "a", "top_k": True}, {"query": "a", "offset": 0}):
        assert adapter.parse_tool_call("search", arguments) is None


def test_experiment_retains_both_search_profiles_and_exact_readers(tmp_path):
    registry = [
        entry(),
        entry(corpus="browsecomp-v1"),
        entry("read_document"),
        entry("get_document", corpus="browsecomp-v1"),
    ]
    path = tmp_path / "registry.json"
    path.write_text(json.dumps([item.model_dump(mode="json") for item in registry]))
    profile = load_profile()
    profile["workload"]["cost_model_path"] = None
    settings = gateway_settings(
        profile,
        run_dir=tmp_path,
        registry_path=path,
        api_key="test-key",
        dcs_key=Fernet.generate_key().decode(),
    )
    assert [item.tool_name for item in settings.web_tool_registry].count("search") == 2
    assert len(settings.web_tool_registry) == 4
    for item in settings.web_tool_registry:
        assert item.semantic_reuse_enabled == (item.tool_name == "search")
        assert item.semantic_mode == "shadow"
