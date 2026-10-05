import asyncio
import threading

import pytest
from reuse_support import registry

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.reuse.semantic import Qwen3Embedding


async def test_distinct_queries_queue_and_cancel_without_extra_native_jobs(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    calls = []

    def encode(texts):
        calls.append(list(texts))
        if texts == ["first"]:
            entered.set()
            assert release.wait(3)
        return [(float(len(texts[0])),)]

    embedder = Qwen3Embedding()
    monkeypatch.setattr(embedder, "_encode", encode)
    first = asyncio.create_task(embedder.embed(["first"]))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        duplicate = asyncio.create_task(embedder.embed(["first"]))
        second = asyncio.create_task(embedder.embed(["second"]))
        cancelled = asyncio.create_task(embedder.embed(["cancelled"]))
        await asyncio.sleep(0)
        assert not second.done()
        first.cancel()
        cancelled.cancel()
        for task in (first, cancelled):
            with pytest.raises(asyncio.CancelledError):
                await task
        assert calls == [["first"]]
    finally:
        release.set()
    assert await asyncio.wait_for(duplicate, 1) == [(5.0,)]
    assert await asyncio.wait_for(second, 1) == [(6.0,)]
    assert calls == [["first"], ["second"]]


async def test_failed_query_does_not_fail_the_next_queued_query(monkeypatch):
    entered, release = threading.Event(), threading.Event()

    def encode(texts):
        if texts == ["bad"]:
            entered.set()
            assert release.wait(3)
            raise ValueError("invalid input")
        return [(1.0,)]

    embedder = Qwen3Embedding()
    monkeypatch.setattr(embedder, "_encode", encode)
    first = asyncio.create_task(embedder.embed(["bad"]))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        second = asyncio.create_task(embedder.embed(["good"]))
        await asyncio.sleep(0)
        assert not second.done()
    finally:
        release.set()
    with pytest.raises(ValueError, match="invalid input"):
        await first
    assert await asyncio.wait_for(second, 1) == [(1.0,)]


async def test_gateway_startup_waits_for_native_embedding(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    ready = asyncio.Event()
    calls = []

    def encode(self, texts):
        calls.append(list(texts))
        if len(calls) == 1:
            entered.set()
            assert release.wait(3)
        return [(1.0,) + (0.0,) * 1023]

    monkeypatch.setattr(Qwen3Embedding, "_encode", encode)
    app = create_app(
        Settings(
            instances=(InferenceInstance("test", "http://inference"),),
            ingress_api_key="test-key",
            trace_path=tmp_path / "trace.jsonl",
            reuse_enabled=True,
            reuse_cache_path=tmp_path / "reuse.sqlite",
            web_tool_registry=(
                registry(
                    protocol_version="flowpilot-phase3-reuse-v3",
                    semantic_reuse_enabled=True,
                    semantic_mode="active",
                ),
            ),
        )
    )

    async def serve():
        async with app.router.lifespan_context(app):
            ready.set()
            embedder = app.state.reuse.controller._embedder
            result = await asyncio.wait_for(embedder.embed(["first query"]), 1)
            assert len(result[0]) == 1024

    startup = asyncio.create_task(serve())
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        assert not ready.is_set()
    finally:
        release.set()
    await asyncio.wait_for(startup, 2)
    assert ready.is_set()
    assert len(calls) == 2
