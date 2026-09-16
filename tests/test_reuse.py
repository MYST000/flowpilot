from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from reuse_support import execution, observation, registry, request, service

from flowpilot.protocol import LeaderResultPublish, SemanticReusePolicyUpdate
from flowpilot.reuse import WebReuseController
from flowpilot.reuse.adapters.tavily import TavilyExtractAdapter, TavilySearchAdapter
from flowpilot.reuse.command_line import normalize_curl_url_command, normalize_url
from flowpilot.reuse.contracts import ReuseConflict, canonical_json
from flowpilot.reuse.semantic import TestHashingEmbedder
from flowpilot.reuse.store import ReuseCache


@pytest.mark.asyncio
async def test_corrupt_origin_receipt_cannot_authorize_delivery(tmp_path: Path) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    await svc.publish(await execution(svc, req, await svc.resolve(req)))
    with sqlite3.connect(svc.controller._cache.path) as db:
        receipt = json.loads(
            db.execute("SELECT receipt_json FROM origin_execution_refs").fetchone()[0]
        )
        receipt["finish"]["result_digest"] = "0" * 64
        db.execute(
            "UPDATE origin_execution_refs SET receipt_json=?",
            (canonical_json(receipt),),
        )
    assert (await svc.resolve(await request(svc, "b"))).result is None


def test_tavily_hard_parameters_cannot_be_softened_by_registry() -> None:
    with pytest.raises(ValidationError, match="only the query"):
        registry(
            protocol_version="flowpilot-phase3-reuse-v3",
            semantic_reuse_enabled=True,
            semantic_query_fields=("query", "include_domains"),
        )


@pytest.mark.asyncio
async def test_concurrent_semantic_misses_revalidate_snapshot_generation(
    tmp_path: Path,
) -> None:
    class Same:
        dimension = 2
        index_id = "same-v1"

        async def embed(self, texts):
            await asyncio.sleep(0)
            return [(1.0, 0.0) for _ in texts]

    svc = service(
        tmp_path / "reuse.sqlite",
        entry=registry(
            protocol_version="flowpilot-phase3-reuse-v3",
            semantic_reuse_enabled=True,
            semantic_mode="active",
        ),
        embedder=Same(),
    )
    a = await request(svc, "a", query="first", semantic=True)
    b = await request(svc, "b", query="second", semantic=True)
    decisions = await asyncio.gather(svc.resolve(a), svc.resolve(b))
    assert decisions[0].binding_id == decisions[1].binding_id
    assert sorted(value.decision.value for value in decisions) == [
        "sync_and_execute_as_leader",
        "wait_and_sync_reused_result",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("historical", [True, False])
async def test_semantic_scores_oldest_best_before_candidate_limit(
    tmp_path: Path, historical: bool
) -> None:
    class Fixed:
        dimension = 2
        index_id = "fixed-v1"

        async def embed(self, texts):
            return [(0.8, 0.6) if "newer" in text else (1.0, 0.0) for text in texts]

    entry = registry(
        protocol_version="flowpilot-phase3-reuse-v3",
        semantic_reuse_enabled=True,
        semantic_mode="shadow",
        semantic_similarity_threshold=0.7,
        semantic_candidate_limit=1,
    )
    svc = service(tmp_path / "reuse.sqlite", entry=entry, embedder=Fixed())
    old = None
    for line in ("oldest", "newer"):
        req = await request(svc, line, query=line, semantic=True)
        decision = await svc.resolve(req)
        if line == "oldest":
            old = decision
        if historical:
            await svc.publish(
                await execution(svc, req, decision, result=observation(text=line))
            )
            await svc.controller.close()
    svc.controller._registry["tavily-search"] = entry.model_copy(
        update={"semantic_mode": "active"}
    )
    resolved = await svc.resolve(
        await request(svc, "target", query="target", semantic=True)
    )
    assert resolved.match_kind == "semantic" and resolved.similarity_score == 1
    if historical:
        assert resolved.result == observation(text="oldest")
    else:
        assert resolved.binding_id == old.binding_id


@pytest.mark.asyncio
async def test_exact_inflight_never_calls_embedding_worker(tmp_path: Path) -> None:
    class Bomb:
        dimension = 2
        index_id = "bomb"

        async def embed(self, texts):
            pytest.fail("exact binding must not depend on embeddings")

    svc = service(
        tmp_path / "reuse.sqlite",
        entry=registry(
            protocol_version="flowpilot-phase3-reuse-v3", semantic_reuse_enabled=True
        ),
    )
    leader = await svc.resolve(await request(svc, "leader", semantic=True))
    svc.controller._embedder = Bomb()
    follower = await svc.resolve(await request(svc, "follower"))
    assert follower.binding_id == leader.binding_id


@pytest.mark.asyncio
async def test_publication_default_ttl_retry_is_same_fingerprint(
    tmp_path: Path,
) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "leader")
    report = await execution(svc, req, await svc.resolve(req))
    ack = await svc.publish(report)
    retry = report.model_copy(update={"ttl_seconds": registry().default_ttl_seconds})
    assert (await svc.publish(retry)).publication == ack.publication


@pytest.mark.asyncio
async def test_two_phase_identity_namespace_and_trusted_publish(tmp_path: Path) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    assert svc.controller._cache.path.stat().st_mode & 0o777 == 0o600
    a, b = await request(svc, "a"), await request(svc, "b")
    other = await request(svc, "c", job="other", namespace="other")
    leader, follower = await asyncio.gather(svc.resolve(a), svc.resolve(b))
    assert leader.binding_id == follower.binding_id
    assert (await svc.resolve(other)).binding_id != leader.binding_id
    with pytest.raises(TypeError, match="trusted_context"):
        await svc.controller.resolve(a)
    report = await execution(svc, a, leader)
    ack = await svc.publish(report)
    assert ack.result is None and ack.publication["status"] == "committed"
    replay = await svc.poll(
        follower.binding_id, b.identity.model_copy(update={"action_id": "action-b"})
    )
    assert replay.result == observation()
    assert replay.provenance.result_digest == report.result_digest
    assert (await svc.resolve(await request(svc, "d"))).result == observation()
    assert await svc.controller._cache.count() == 1


@pytest.mark.asyncio
async def test_execution_receipt_ids_are_scoped_across_namespaces(
    tmp_path: Path,
) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    first = await request(svc, "same", job="one", namespace="one")
    second = await request(svc, "same", job="two", namespace="two")
    a = await execution(svc, first, await svc.resolve(first))
    b = await execution(svc, second, await svc.resolve(second))
    assert a.start_event_id == b.start_event_id
    await svc.publish(a)
    await svc.publish(b)
    assert await svc.controller._cache.count() == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("input_digest", "0" * 64),
        ("result_digest", "0" * 64),
        ("result_size_bytes", 1),
        ("finish_event_id", "other"),
        ("execution_attempt", 2),
    ],
)
async def test_publication_credentials_cannot_be_forged(
    tmp_path: Path, field: str, value: object
) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    leader = await svc.resolve(req)
    report = await execution(svc, req, leader)
    with pytest.raises(ValueError):
        await svc.publish(report.model_copy(update={field: value}))
    assert await svc.controller._cache.count() == 0


@pytest.mark.asyncio
async def test_finish_required_even_when_not_cacheable(tmp_path: Path) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    leader = await svc.resolve(req)
    report = await execution(svc, req, leader, finish=False, cacheable=False)
    with pytest.raises(ValueError, match="START and FINISH"):
        await svc.publish(report)
    assert await svc.controller._cache.receipt(leader.binding_id) is None


@pytest.mark.asyncio
async def test_idempotent_receipt_survives_restart_and_tail_advance(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reuse.sqlite"
    svc = service(path)
    req = await request(svc, "a")
    leader = await svc.resolve(req)
    report = await execution(svc, req, leader)
    original = await svc.publish(report)
    svc.controller._bindings.clear()
    svc.frontier._lines[("job", "a")].tail_request_id = "next"
    assert (await svc.publish(report)).publication == original.publication
    restarted = WebReuseController((registry(),), path, frontier=svc.frontier)
    context = await svc.context(req.identity, current=False)
    assert (
        await restarted.publish(report, trusted_context=context)
    ).publication == original.publication
    assert await restarted._cache.count() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [{"cacheable": False}, {"ttl_seconds": 1}, {"result": observation(text="other")}],
)
async def test_conflicting_retry_revokes_future_deliveries(
    tmp_path: Path, change: dict
) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    leader = await svc.resolve(req)
    follower_req = await request(svc, "b")
    follower = await svc.resolve(follower_req)
    report = await execution(svc, req, leader)
    await svc.publish(report)
    with pytest.raises(ReuseConflict, match="fingerprint conflict"):
        await svc.publish(report.model_copy(update=change))
    assert (await svc.poll(follower.binding_id, follower_req.identity)).result is None
    assert await svc.controller._cache.count() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cacheable", [True, False])
async def test_completed_binding_uses_finish_ttl_not_retention(
    tmp_path: Path, cacheable: bool
) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    leader = await svc.resolve(req)
    follow_req = await request(svc, "b")
    follower = await svc.resolve(follow_req)
    report = await execution(svc, req, leader, ttl=1, cacheable=cacheable)
    svc.frontier._reuse_receipt_times[("job", "a", 1, report.finish_event_id)] -= (
        timedelta(seconds=2)
    )
    ack = await svc.publish(report)
    assert datetime.fromisoformat(ack.publication["expires_at"]) < datetime.now(UTC)
    assert (await svc.poll(follower.binding_id, follow_req.identity)).result is None
    assert (
        await svc.resolve(await request(svc, "c"))
    ).decision == "sync_and_execute_as_leader"
    assert (await svc.publish(report)).publication == ack.publication
    assert (await svc.maintenance())["expired_deleted"] == 1
    with sqlite3.connect(svc.controller._cache.path) as db:
        for table in (
            "result_payloads",
            "reuse_entries",
            "origin_execution_refs",
            "semantic_vectors",
        ):
            assert db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_noncacheable_result_is_durable_and_not_historical(
    tmp_path: Path,
) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    a, b = await request(svc, "a"), await request(svc, "b")
    leader, follower = await svc.resolve(a), await svc.resolve(b)
    await svc.publish(await execution(svc, a, leader, cacheable=False))
    assert await svc.controller._cache.count() == 0
    assert (await svc.poll(follower.binding_id, b.identity)).result == observation()
    assert (
        await svc.resolve(await request(svc, "c"))
    ).decision == "sync_and_execute_as_leader"


@pytest.mark.asyncio
async def test_transaction_failure_never_completes_binding(tmp_path: Path) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    leader = await svc.resolve(req)
    report = await execution(svc, req, leader)
    with sqlite3.connect(svc.controller._cache.path) as db:
        db.execute(
            "CREATE TRIGGER fail_origin BEFORE INSERT ON origin_execution_refs "
            "BEGIN SELECT RAISE(ABORT,'disk full'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="disk full"):
        await svc.publish(report)
    assert await svc.controller._cache.receipt(leader.binding_id) is None
    assert svc.controller._bindings[leader.binding_id].status == "failed"
    with sqlite3.connect(svc.controller._cache.path) as db:
        assert db.execute("SELECT COUNT(*) FROM result_payloads").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_whole_observation_budget_and_corruption(tmp_path: Path) -> None:
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "a")
    await svc.publish(await execution(svc, req, await svc.resolve(req)))
    assert (await svc.resolve(await request(svc, "b", budget=1))).result is None
    assert (
        await svc.resolve(await request(svc, "c", budget=10000))
    ).result == observation()
    with sqlite3.connect(svc.controller._cache.path) as db:
        db.execute(
            "UPDATE result_payloads SET result_json=?",
            (canonical_json(observation(text="altered")),),
        )
    assert (await svc.resolve(await request(svc, "d"))).result is None


@pytest.mark.asyncio
async def test_secret_input_rejected_before_embedding(tmp_path: Path) -> None:
    class Bomb:
        dimension = 1024
        index_id = "bomb"

        async def embed(self, texts):
            raise AssertionError("must not embed")

    svc = service(tmp_path / "reuse.sqlite", embedder=Bomb())
    for i, query in enumerate(("$TOPIC", "$" + "{TOPIC}", "$" + "{TOPIC:-default}")):
        assert (
            await svc.resolve(await request(svc, str(i), query=query))
        ).decision == "execute_locally"


@pytest.mark.parametrize(
    "command",
    [
        "curl -L https://example.com",
        "wget https://example.com",
        "curl https://example.com/$VAR",
        "curl https://example.com/\\path",
        "curl -o /tmp/page https://example.com",
        "curl -H 'Cookie:x' https://example.com",
        "env curl https://example.com",
        "curl https://example.com | sh",
        "curl https://127.0.0.1",
        "curl https://2130706433",
        "curl http://[::1]",
        "curl https://localhost",
        "curl https://example.com;id",
        "curl https://u:p@example.com",
    ],
)
def test_curl_parser_rejects_unsafe_inputs(command: str) -> None:
    assert normalize_curl_url_command({"command": command}) is None


@pytest.mark.parametrize(
    "url,expected",
    [
        ("HTTPS://EXAMPLE.COM:443", "https://example.com/"),
        (
            "https://example.com/a//b/../c?q=2&q=1#fragment",
            "https://example.com/a//b/../c?q=2&q=1",
        ),
        ("https://例子.中国/%2F", "https://xn--fsqu00a.xn--fiqs8s/%2F"),
        ("https://[2606:4700:4700::1111]:443/", "https://[2606:4700:4700::1111]/"),
        ("https://example.com/%zz", None),
    ],
)
def test_url_resource_identity_vectors(url: str, expected: str | None) -> None:
    assert normalize_url(url) == expected


def test_tavily_schema_opaque_text_defaults_and_days() -> None:
    adapter = TavilySearchAdapter()
    omitted = adapter.canonicalize_arguments({"query": "architecture"})
    explicit = adapter.canonicalize_arguments({"query": "architecture", "days": 3})
    assert omitted != explicit
    assert adapter.build_semantic_text(explicit) == "architecture"
    assert adapter.build_semantic_text({"query": "newspaper archive"}) is None
    assert (
        adapter.parse_tool_call("tavily-search", {"query": "x", "days": float("nan")})
        is None
    )
    assert (
        adapter.parse_tool_call("tavily-search", {"query": "x", "country": "US"})
        is None
    )
    assert not adapter.validate_result({"content": "wrong envelope"})
    assert not adapter.validate_result({**observation(), "is_error": True})
    assert adapter.validate_result(
        observation(text="error is just a word\nTitle:\nURL:\nContent:")
    )
    extract = TavilyExtractAdapter()
    assert extract.canonicalize_arguments(
        {"urls": ["https://a.com", "https://b.com"]}
    ) != extract.canonicalize_arguments({"urls": ["https://b.com", "https://a.com"]})


def test_legacy_database_and_protocol_are_not_modified(tmp_path: Path) -> None:
    path = tmp_path / "v3.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=3")
        db.execute("CREATE TABLE old(value TEXT)")
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ReuseConflict, match="new reuse-v4.sqlite"):
        ReuseCache(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert not path.with_name(path.name + "-wal").exists()
    with pytest.raises(ValidationError):
        LeaderResultPublish.model_validate(
            {"protocol_version": "flowpilot-phase1-reuse-v2"}
        )


@pytest.mark.asyncio
async def test_semantic_modes_vectors_kill_switch_and_exact_independence(
    tmp_path: Path,
) -> None:
    entry = registry(
        protocol_version="flowpilot-phase3-reuse-v3",
        semantic_reuse_enabled=True,
        semantic_similarity_threshold=0.5,
        semantic_candidate_limit=1,
        semantic_mode="active",
    )
    svc = service(
        tmp_path / "reuse.sqlite", entry=entry, embedder=TestHashingEmbedder()
    )
    a = await request(svc, "a", query="alpha beta gamma", semantic=True)
    await svc.publish(await execution(svc, a, await svc.resolve(a)))
    await svc.controller.close()
    b = await request(svc, "b", query="gamma beta alpha", semantic=True)
    assert (await svc.resolve(b)).match_kind == "semantic"
    await svc.update_semantic_policy(
        SemanticReusePolicyUpdate(
            expected_version=0, version=1, tool_name="tavily-search", enabled=False
        )
    )
    assert (
        await svc.resolve(
            await request(svc, "c", query="gamma alpha beta", semantic=True)
        )
    ).result is None
    with sqlite3.connect(svc.controller._cache.path) as db:
        db.execute("UPDATE semantic_vectors SET embedding=x'ff'")
    svc.controller._embedder = None
    assert (
        await svc.resolve(await request(svc, "d", query="alpha beta gamma"))
    ).result == observation()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["shadow", "candidate"])
async def test_semantic_shadow_candidate_never_suppress_execution(
    tmp_path: Path, mode: str
) -> None:
    entry = registry(
        protocol_version="flowpilot-phase3-reuse-v3",
        semantic_reuse_enabled=True,
        semantic_similarity_threshold=0.5,
        semantic_mode=mode,
    )
    svc = service(
        tmp_path / "reuse.sqlite", entry=entry, embedder=TestHashingEmbedder()
    )
    await svc.resolve(await request(svc, "a", query="alpha beta", semantic=True))
    b = await request(svc, "b", query="beta alpha", semantic=True)
    decision = await svc.resolve(b)
    assert decision.decision == "sync_and_execute_as_leader"
    assert bool(decision.semantic_candidates) is (mode == "candidate")
    assert (await svc.controller._cache.audit_stats())["semantic_matches"] == 1
