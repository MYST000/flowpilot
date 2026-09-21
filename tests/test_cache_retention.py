from __future__ import annotations

from pathlib import Path

import pytest
from reuse_support import execution, observation, request, service

from flowpilot.protocol import FollowerCancellation


@pytest.mark.asyncio
async def test_value_eviction_keeps_expensive_old_result_over_recent_cheap_result(
    tmp_path,
):
    svc = service(tmp_path / "reuse.sqlite")
    expensive = await request(svc, "expensive", query="expensive query")
    cheap = await request(svc, "cheap", query="cheap query")
    for req, latency in ((expensive, 1000), (cheap, 1)):
        await svc.publish(
            await execution(
                svc,
                req,
                await svc.resolve(req),
                latency_ms=latency,
            )
        )
    cache = svc.controller._cache
    stats = await svc.maintenance()
    # Equal sized payloads. The expensive result is older, so pure LRU
    # would make the opposite decision under the same capacity.
    result = await svc.maintenance(max_payload_bytes=stats["payload_bytes"] // 2)
    assert result["capacity_evicted"] == 1
    assert (
        await svc.resolve(await request(svc, "again1", query="expensive query"))
    ).result
    assert not (
        await svc.resolve(await request(svc, "again2", query="cheap query"))
    ).result
    assert await cache.count() == 1
    await svc.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cacheable", [True, False])
async def test_pending_follower_payload_survives_capacity_until_cancel(
    tmp_path, cacheable
):
    svc = service(tmp_path / "reuse.sqlite")
    svc.controller._max_payload_bytes = 1
    leader = await request(svc, "leader")
    follower = await request(svc, "follower")
    leader_decision = await svc.resolve(leader)
    follower_decision = await svc.resolve(follower)
    assert follower_decision.binding_id == leader_decision.binding_id
    await svc.publish(
        await execution(
            svc,
            leader,
            leader_decision,
            cacheable=cacheable,
        )
    )
    stats = await svc.maintenance(max_payload_bytes=1)
    assert stats["over_capacity_bytes"] > 0
    assert stats["protected_origins"] == 1
    assert leader_decision.binding_id is not None
    delivered = await svc.poll(leader_decision.binding_id, follower.identity)
    assert delivered.result == observation()
    # No delivery ACK exists. Retry polls retain the same payload for the
    # binding's existing lifetime; cancellation explicitly releases its claim.
    assert (await svc.poll(leader_decision.binding_id, follower.identity)).result
    await svc.cancel_follower(
        FollowerCancellation(
            binding_id=leader_decision.binding_id,
            identity=follower.identity,
        )
    )
    stats = await svc.maintenance(max_payload_bytes=1)
    assert stats["payload_bytes"] == 0
    assert stats["capacity_evicted"] == 1
    await svc.close()


@pytest.mark.asyncio
async def test_active_delivery_has_a_capacity_reference(tmp_path: Path):
    svc = service(tmp_path / "reuse.sqlite")
    req = await request(svc, "leader")
    decision = await svc.resolve(req)
    await svc.publish(await execution(svc, req, decision))
    receipt = await svc.controller._cache.receipt(decision.binding_id)
    assert receipt is not None
    with svc.controller._cache.protect_delivery(receipt["origin_id"]):
        stats = await svc.maintenance(max_payload_bytes=1)
        assert stats["capacity_evicted"] == 0
        assert stats["over_capacity_bytes"] > 0
    stats = await svc.maintenance(max_payload_bytes=1)
    assert stats["capacity_evicted"] == 1
    await svc.close()
