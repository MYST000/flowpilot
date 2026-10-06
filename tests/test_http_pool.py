from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink


def test_http_pool_setting_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOWPILOT_REQUIRE_INGRESS_AUTH", "0")
    monkeypatch.setenv("FLOWPILOT_UPSTREAMS", "http://inference")
    monkeypatch.delenv("FLOWPILOT_HTTP_MAX_CONNECTIONS", raising=False)
    assert Settings.from_env().http_max_connections == 100
    monkeypatch.setenv("FLOWPILOT_HTTP_MAX_CONNECTIONS", "1000")
    assert Settings.from_env().http_max_connections == 1000
    monkeypatch.setenv("FLOWPILOT_HTTP_MAX_CONNECTIONS", "0")
    with pytest.raises(ValueError, match="http_max_connections must be positive"):
        Settings.from_env()


@pytest.mark.anyio
async def test_owned_pool_allows_more_than_100_active_connections(
    tmp_path: Path,
) -> None:
    arrived = 0
    all_arrived = asyncio.Event()
    release = asyncio.Event()

    async def upstream(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal arrived
        try:
            await reader.readuntil(b"\r\n\r\n")
            arrived += 1
            if arrived >= 101:
                all_arrived.set()
            await release.wait()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    address = f"http://127.0.0.1:{port}"
    settings = Settings(
        instances=(InferenceInstance("inference", address),),
        trace_path=tmp_path / "trace.jsonl",
        require_ingress_auth=False,
        http_max_connections=1000,
    )
    app = create_app(settings, trace_sink=InMemoryTraceSink())
    async with server, app.router.lifespan_context(app):
        pool = app.state.llm_gateway._client
        requests = [asyncio.create_task(pool.get(address)) for _ in range(101)]
        try:
            # A pool still capped at 100 deadlocks here until this timeout.
            await asyncio.wait_for(all_arrived.wait(), timeout=10)
        finally:
            release.set()
            responses = await asyncio.gather(*requests)
        assert all(response.status_code == 200 for response in responses)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            health = (await client.get("/flowpilot/health")).json()
            assert health["http_pool"] == {
                "owner": "flowpilot",
                "max_connections": 1000,
            }
    assert pool.is_closed


@pytest.mark.anyio
async def test_injected_pool_remains_caller_owned(tmp_path: Path) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as injected:
        settings = Settings(
            instances=(InferenceInstance("inference", "http://inference"),),
            trace_path=tmp_path / "trace.jsonl",
            require_ingress_auth=False,
            http_max_connections=1000,
        )
        app = create_app(settings, http_client=injected, trace_sink=InMemoryTraceSink())
        async with app.router.lifespan_context(app):
            assert app.state.llm_gateway._client is injected
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
            ) as client:
                health = (await client.get("/flowpilot/health")).json()
                assert health["http_pool"] == {
                    "owner": "injected",
                    "max_connections": None,
                }
        assert not injected.is_closed
