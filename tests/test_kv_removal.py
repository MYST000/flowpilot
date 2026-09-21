from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink


@pytest.mark.anyio
async def test_retired_kv_control_plane_cannot_call_upstream(tmp_path: Path) -> None:
    upstream_paths: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        upstream_paths.append(request.url.path)
        assert request.method == "GET" and request.url.path == "/v1/models"
        return httpx.Response(200, json={"data": []})

    sink = InMemoryTraceSink()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream)
    ) as upstream_client:
        app = create_app(
            Settings(
                instances=(InferenceInstance("engine", "http://engine"),),
                trace_path=tmp_path / "trace.jsonl",
                ingress_api_key="test-key",
            ),
            http_client=upstream_client,
            trace_sink=sink,
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://flowpilot",
                headers={"x-flowpilot-api-key": "test-key"},
            ) as client:
                for method, path in (
                    ("GET", "/flowpilot/v1/kv"),
                    ("POST", "/flowpilot/v1/kv/engine/capabilities"),
                    ("POST", "/flowpilot/v1/kv/engine/actions"),
                    ("POST", "/flowpilot/v1/events/kv"),
                    ("GET", "/flowpilot/v1/scheduling/kv-action/line"),
                    ("GET", "/flowpilot/v1/scheduling/alignment"),
                ):
                    response = await client.request(method, path, json={})
                    assert response.status_code == 404, path
                assert upstream_paths == []
                health = await client.get("/flowpilot/health")
                assert health.status_code == 200
                assert health.json()["kv_telemetry"] == "unsupported"

                schema = (await client.get("/openapi.json")).json()
                assert not any("/kv" in path for path in schema["paths"])
                properties = schema["components"]["schemas"]
                assert not any(name.startswith("KV") for name in properties)

    assert upstream_paths == ["/v1/models"]
    assert not any(item["event_type"] == "kv_state" for item in sink.records)


@pytest.mark.parametrize(
    "key,value",
    [
        ("kv_telemetry_schema", "flowpilot-vllm-kv-v2"),
        ("kv_endpoint", "http://retired-engine"),
        ("kv_api_key", "private-test-value"),
        ("kv_timeout_seconds", 5),
    ],
)
def test_retired_kv_configuration_is_rejected(
    monkeypatch: pytest.MonkeyPatch, key: str, value: str | int
) -> None:
    monkeypatch.setenv("FLOWPILOT_INGRESS_API_KEY", "test-key")
    monkeypatch.setenv(
        "FLOWPILOT_INSTANCES_JSON",
        json.dumps([{"id": "engine", "base_url": "http://engine", key: value}]),
    )
    with pytest.raises(
        ValueError, match="legacy KV configuration has been removed"
    ) as exc:
        Settings.from_env()
    assert key in str(exc.value)
    assert "private-test-value" not in str(exc.value)
