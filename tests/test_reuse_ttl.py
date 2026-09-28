from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from reuse_support import execution, observation, registry, request, service

from flowpilot.reuse.semantic import TestHashingEmbedder


@pytest.fixture
def clock(monkeypatch):
    class Clock(datetime):
        current = datetime.now(UTC)

        @classmethod
        def now(cls, tz=None):
            return (
                cls.current.astimezone(tz)
                if tz is not None
                else cls.current.replace(tzinfo=None)
            )

    for module in (
        "flowpilot.reuse.store",
        "flowpilot.reuse.controller",
        "flowpilot.frontier.store",
        "reuse_support",
    ):
        monkeypatch.setattr(f"{module}.datetime", Clock)
    return Clock


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "settings,requested_ttl,effective_ttl",
    [
        ({}, None, 300),
        ({}, 20, 20),
        ({}, 600, 300),
        ({"default_ttl_seconds": 120, "max_ttl_seconds": 90}, None, 90),
        ({"scope_max_ttl_seconds": 45}, 90, 45),
        ({"max_ttl_seconds": 600}, 450, 450),
    ],
)
async def test_history_renews_fixed_ttl_across_restart(
    tmp_path: Path, clock, settings, requested_ttl, effective_ttl
):
    path = tmp_path / "reuse.sqlite"
    entry = registry(**settings)
    svc = service(path, entry=entry)
    initial = clock.current
    window = timedelta(seconds=effective_ttl)
    leader = await request(svc, "leader")
    report = await execution(svc, leader, await svc.resolve(leader), ttl=requested_ttl)
    published = await svc.publish(report)
    assert (
        datetime.fromisoformat(published.publication["expires_at"]) == initial + window
    )

    clock.current = initial + window * 0.6
    first = await svc.resolve(await request(svc, "first"))
    assert first.result == observation()
    assert first.provenance.observed_at == initial
    assert first.provenance.expires_at == clock.current + window
    assert (await svc.publish(report)).publication == published.publication
    await svc.close()

    # The original FINISH TTL is now past; the persisted sliding expiry is live.
    clock.current = initial + window * 1.2
    restarted = service(path, entry=entry)
    assert (await restarted.maintenance())["expired_deleted"] == 0
    second = await restarted.resolve(await request(restarted, "second"))
    assert second.result == observation()
    assert second.provenance.origin_id == first.provenance.origin_id
    assert second.provenance.observed_at == initial
    assert second.provenance.expires_at == clock.current + window

    # A full idle window ends reuse, and neither a hit nor a touch revives it.
    clock.current = second.provenance.expires_at
    assert await restarted.controller._cache.touch(second.provenance.origin_id) is None
    assert not (await restarted.resolve(await request(restarted, "expired"))).result
    assert (await restarted.maintenance())["expired_deleted"] == 1
    await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("rejection", ["budget", "invalid_payload"])
async def test_rejected_delivery_does_not_renew(tmp_path: Path, clock, rejection):
    svc = service(tmp_path / "reuse.sqlite")
    initial = clock.current
    leader = await request(svc, "leader")
    await svc.publish(await execution(svc, leader, await svc.resolve(leader), ttl=10))
    clock.current = initial + timedelta(seconds=5)
    if rejection == "invalid_payload":
        with sqlite3.connect(svc.controller._cache.path) as db:
            db.execute("UPDATE result_payloads SET result_json='{}'")
    rejected = await svc.resolve(
        await request(svc, "rejected", budget=1 if rejection == "budget" else None)
    )
    assert rejected.result is None
    with sqlite3.connect(svc.controller._cache.path) as db:
        expires, hits = db.execute(
            "SELECT expires_at,hit_count FROM reuse_entries"
        ).fetchone()
    assert datetime.fromisoformat(expires) == initial + timedelta(seconds=10)
    assert hits == 0
    await svc.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["expiry", "revocation"])
async def test_result_invalidated_during_delivery_cannot_renew(
    tmp_path: Path, clock, monkeypatch, terminal
):
    svc = service(tmp_path / "reuse.sqlite")
    initial = clock.current
    leader = await request(svc, "leader")
    await svc.publish(await execution(svc, leader, await svc.resolve(leader), ttl=10))
    cache = svc.controller._cache
    touch = cache.touch

    async def invalidate_before_touch(origin_id):
        if terminal == "expiry":
            clock.current = initial + timedelta(seconds=10)
        else:
            await cache.revoke(origin_id)
        return await touch(origin_id)

    monkeypatch.setattr(cache, "touch", invalidate_before_touch)
    assert not (await svc.resolve(await request(svc, "racing"))).result
    assert (await svc.maintenance())["payload_bytes"] == 0
    await svc.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cacheable", [True, False])
@pytest.mark.parametrize("deferred", [True, False])
async def test_follower_poll_renews_only_cacheable_results(
    tmp_path: Path, clock, cacheable, deferred
):
    svc = service(tmp_path / "reuse.sqlite")
    initial = clock.current
    leader = await request(svc, "leader")
    follower = await request(svc, "follower")
    leader_decision = await svc.resolve(leader)
    waiting = await svc.resolve(follower, defer_allowed=deferred)
    assert waiting.binding_id == leader_decision.binding_id
    await svc.publish(
        await execution(svc, leader, leader_decision, ttl=10, cacheable=cacheable)
    )

    async def poll():
        if deferred:
            return await svc.poll_deferred(waiting.binding_id, follower)
        return await svc.poll(waiting.binding_id, follower.identity)

    clock.current = initial + timedelta(seconds=2)
    first = await poll()
    assert first.result == observation()
    assert first.provenance.observed_at == initial
    assert first.provenance.expires_at == initial + timedelta(
        seconds=12 if cacheable else 10
    )
    clock.current = initial + timedelta(seconds=11)
    second = await poll()
    if cacheable:
        assert second.result == observation()
        assert second.provenance.expires_at == initial + timedelta(seconds=21)
        clock.current = second.provenance.expires_at
        assert (await poll()).result is None
    else:
        assert second.result is None
    assert (await svc.maintenance())["expired_deleted"] == 1
    await svc.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["active", "shadow", "candidate"])
async def test_only_active_semantic_delivery_renews(tmp_path: Path, clock, mode):
    svc = service(
        tmp_path / "reuse.sqlite",
        entry=registry(
            protocol_version="flowpilot-phase3-reuse-v3",
            semantic_reuse_enabled=True,
            semantic_mode=mode,
            semantic_similarity_threshold=0.5,
        ),
        embedder=TestHashingEmbedder(),
    )
    initial = clock.current
    leader = await request(svc, "leader", query="alpha beta gamma", semantic=True)
    await svc.publish(await execution(svc, leader, await svc.resolve(leader), ttl=10))
    # Drain the asynchronous vector writer before testing semantic history.
    await svc.controller.close()
    clock.current = initial + timedelta(seconds=8)
    matched = await svc.resolve(
        await request(svc, "semantic", query="gamma beta alpha", semantic=True)
    )
    if mode == "active":
        assert matched.result == observation()
        assert matched.match_kind == "semantic"
        assert matched.provenance.expires_at == initial + timedelta(seconds=18)
    else:
        assert matched.result is None
    clock.current = initial + timedelta(seconds=11)
    exact = await svc.resolve(
        await request(svc, "exact", query="alpha beta gamma", semantic=True)
    )
    assert bool(exact.result) is (mode == "active")
    await svc.close()


@pytest.mark.asyncio
async def test_retention_value_uses_original_window_after_repeated_hits(
    tmp_path: Path, clock
):
    svc = service(tmp_path / "reuse.sqlite")
    initial = clock.current
    leader = await request(svc, "old", query="old result")
    await svc.publish(await execution(svc, leader, await svc.resolve(leader), ttl=10))
    for elapsed in (9, 18, 27):
        clock.current = initial + timedelta(seconds=elapsed)
        assert (
            await svc.resolve(await request(svc, f"hit-{elapsed}", query="old result"))
        ).result
    newer = await request(svc, "new", query="new result")
    await svc.publish(
        await execution(svc, newer, await svc.resolve(newer), ttl=10, latency_ms=3)
    )
    size = (await svc.maintenance())["payload_bytes"]
    # Equal payloads and full windows: old saves 1*(1+3)=4 ms, new saves 3 ms.
    # Dividing by the origin's age plus TTL would incorrectly evict the old one.
    assert (await svc.maintenance(max_payload_bytes=size // 2))["capacity_evicted"] == 1
    assert (await svc.resolve(await request(svc, "kept", query="old result"))).result
    assert not (
        await svc.resolve(await request(svc, "evicted", query="new result"))
    ).result
    await svc.close()


@pytest.mark.asyncio
async def test_existing_v4_entry_with_hits_can_start_sliding_expiry(
    tmp_path: Path, clock
):
    svc = service(tmp_path / "reuse.sqlite")
    initial = clock.current
    leader = await request(svc, "leader")
    ack = await svc.publish(
        await execution(svc, leader, await svc.resolve(leader), ttl=10)
    )
    cache = svc.controller._cache
    # Older v4 writers recorded hits without moving the entry's expiry.
    with sqlite3.connect(cache.path) as db:
        db.execute(
            "UPDATE reuse_entries SET hit_count=4,last_used_at=?",
            ((initial + timedelta(seconds=3)).isoformat(),),
        )
        original_receipt = db.execute(
            "SELECT receipt_json FROM origin_execution_refs"
        ).fetchone()[0]
    clock.current = initial + timedelta(seconds=5)
    hit = await svc.resolve(await request(svc, "upgraded"))
    assert hit.result == observation()
    assert hit.provenance.expires_at == initial + timedelta(seconds=15)
    receipt = await cache.receipt(ack.binding_id)
    assert receipt is not None
    assert receipt["expires_at"] == ack.publication["expires_at"]
    row = await cache.result(hit.provenance.origin_id)
    assert row is not None
    assert json.loads(row["receipt_json"]) == json.loads(original_receipt)
    await svc.close()
