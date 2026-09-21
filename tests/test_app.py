from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import ToolRegistryEntry


def _digest() -> str:
    return "b" * 64


@pytest.mark.anyio
async def test_deployment_ingress_key_and_legacy_tenant_field_rejection() -> None:
    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "unexpected"})

    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    settings = Settings(
        instances=(InferenceInstance("inference-a", "http://inference-a"),),
        trace_path=Path("/tmp/flowpilot-ingress-auth-test.jsonl"),
        ingress_api_key="key-a",
    )
    app = create_app(settings, http_client=upstream_client)
    auth = {"x-flowpilot-api-key": "key-a"}
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            accepted = await client.post(
                "/flowpilot/v1/jobs",
                headers=auth,
                json={"job_id": "job-a"},
            )
            accepted_other_job = await client.post(
                "/flowpilot/v1/jobs",
                headers=auth,
                json={"job_id": "job-b"},
            )
            legacy = await client.post(
                "/flowpilot/v1/jobs",
                headers={
                    **auth,
                },
                json={"tenant_id": "legacy", "job_id": "job-legacy"},
            )

    assert accepted.status_code == 201
    assert accepted_other_job.status_code == 201
    assert legacy.status_code == 422
    await upstream_client.aclose()


@pytest.mark.anyio
async def test_phase0_control_plane_collects_frontier_and_tool_events() -> None:
    async def upstream(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "response-1",
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "call-tool-1",
                                    "type": "function",
                                    "function": {
                                        "name": "web_search",
                                        "arguments": '{"query":"phase zero"}',
                                    },
                                }
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
            },
        )

    sink = InMemoryTraceSink()
    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    settings = Settings(
        instances=(InferenceInstance("inference-a", "http://inference-a"),),
        trace_path=Path("/tmp/flowpilot-test.jsonl"),
        ingress_api_key="test-key",
    )
    app = create_app(settings, http_client=upstream_client, trace_sink=sink)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            auth = {"x-flowpilot-api-key": "test-key"}
            assert (
                await client.post(
                    "/flowpilot/v1/jobs",
                    headers=auth,
                    json={"job_id": "job-1"},
                )
            ).status_code == 201
            assert (
                await client.post(
                    "/flowpilot/v1/lines",
                    headers=auth,
                    json={
                        "job_id": "job-1",
                        "line_id": "line-1",
                        "context_epoch": 1,
                        "base_context_cursor": "cursor-0",
                        "context_digest": _digest(),
                        "conversation_id": "conversation-line-1",
                    },
                )
            ).status_code == 201
            assert (
                await client.post(
                    "/flowpilot/v1/lines",
                    headers=auth,
                    json={
                        "job_id": "job-1",
                        "line_id": "line-2",
                        "context_epoch": 1,
                        "base_context_cursor": "cursor-0",
                        "context_digest": _digest(),
                        "conversation_id": "conversation-line-1",
                    },
                )
            ).status_code == 201
            dependency = await client.put(
                "/flowpilot/v1/lines/line-2/dependencies",
                headers=auth,
                json={
                    "job_id": "job-1",
                    "line_id": "line-2",
                    "version": 1,
                    "prerequisite_line_ids": ["line-1"],
                },
            )
            assert dependency.status_code == 200

            llm_headers = {
                **auth,
                "x-flowpilot-protocol-version": "flowpilot-phase0-v2",
                "x-flowpilot-job-id": "job-1",
                "x-flowpilot-line-id": "line-1",
                "x-flowpilot-tail-request-id": "tail-1",
                "x-flowpilot-llm-call-id": "call-1",
                "x-flowpilot-tail-version": "0",
                "x-flowpilot-context-epoch": "1",
                "x-flowpilot-context-sequence": "0",
                "x-flowpilot-context-cursor": "cursor-0",
                "x-flowpilot-context-digest": _digest(),
                "x-flowpilot-request-id": "request-1",
                "x-flowpilot-request-attempt": "1",
                "x-flowpilot-conversation-id": "conversation-line-1",
            }
            response = await client.post(
                "/v1/chat/completions",
                headers=llm_headers,
                json={"model": "model-a", "messages": []},
            )
            assert response.status_code == 200
            assert response.headers["x-flowpilot-tail-version"] == "1"
            denied_calls = await client.get("/flowpilot/v1/gateway-calls")
            assert denied_calls.status_code == 401
            calls = await client.get("/flowpilot/v1/gateway-calls", headers=auth)
            assert calls.status_code == 200
            assert calls.json()["calls"][0]["phase"] == "completed"

            tool_payload = {
                "event_id": "tool-event-start-1",
                "sequence": 1,
                "execution_attempt": 1,
                "job_id": "job-1",
                "line_id": "line-1",
                "context_epoch": 1,
                "tail_request_id": "tail-1",
                "llm_call_id": "call-1",
                "action_id": "action-1",
                "tool_call_id": "call-tool-1",
                "tool_name": "web_search",
                "tool_class": "web",
                "event_kind": "start",
                "observed_at": "2026-08-10T00:00:00Z",
                "request_id": "request-1",
                "attempt": 1,
                "conversation_id": "conversation-line-1",
            }
            assert (
                await client.post(
                    "/flowpilot/v1/events/tools", headers=auth, json=tool_payload
                )
            ).status_code == 202
            tool_event = await client.post(
                "/flowpilot/v1/events/tools",
                headers=auth,
                json={
                    **tool_payload,
                    "event_id": "tool-event-finish-1",
                    "sequence": 2,
                    "event_kind": "finish",
                    "result_size_bytes": 1234,
                    "measured_latency_ms": 40,
                    "observed_at": "2026-08-10T00:00:00Z",
                },
            )
            assert tool_event.status_code == 202

            projection = await client.get(
                "/flowpilot/v1/scheduling/projections/line-1",
                headers=auth,
                params={"job_id": "job-1"},
            )
            assert projection.status_code == 200
            assert projection.json()["t_need"] == "2026-08-10T00:00:00Z"
            assert not {"t_kv", "t2", "kv_restore_laxity_ms"}.intersection(
                projection.json()
            )

    event_types = [item["event_type"] for item in sink.records]
    assert "llm_request" in event_types
    assert "llm_response" in event_types
    assert "tool_finish" in event_types
    assert "kv_state" not in event_types
    await upstream_client.aclose()


@pytest.mark.anyio
async def test_trace_write_failure_degrades_health_and_counts_drop() -> None:
    class FailingTraceSink:
        async def write(self, record: dict[str, Any]) -> None:
            del record
            raise OSError("disk full")

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/models"
        return httpx.Response(200, json={"data": []})

    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    settings = Settings(
        instances=(InferenceInstance("inference-a", "http://inference-a"),),
        trace_path=Path("/tmp/flowpilot-test.jsonl"),
        ingress_api_key="test-key",
    )
    app = create_app(
        settings, http_client=upstream_client, trace_sink=FailingTraceSink()
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            response = await client.post(
                "/flowpilot/v1/jobs",
                headers={"x-flowpilot-api-key": "test-key"},
                json={"job_id": "job-1"},
            )
            assert response.status_code == 201
            health = await client.get("/flowpilot/health")
            metrics = await client.get("/flowpilot/metrics")
    assert health.status_code == 503
    assert health.json()["trace"]["status"] == "degraded"
    assert metrics.json()["trace_write_failures"] == 1
    assert metrics.json()["trace_dropped_events"] == 1
    await upstream_client.aclose()


@pytest.mark.anyio
async def test_phase1_reuse_api_requires_active_tail_and_omits_payload_from_trace(
    tmp_path: Path,
) -> None:
    async def upstream(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/models":
            return httpx.Response(200, json={"data": []})
        assert request.url.path == "/v1/chat/completions"
        return httpx.Response(
            200,
            json={
                "id": "response-1",
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "tool-1",
                                    "type": "function",
                                    "function": {
                                        "name": "web_search",
                                        "arguments": '{"query":"private-query"}',
                                    },
                                }
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
            },
        )

    sink = InMemoryTraceSink()
    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    settings = Settings(
        instances=(InferenceInstance("inference-a", "http://inference-a"),),
        trace_path=tmp_path / "trace.jsonl",
        ingress_api_key="test-key",
        reuse_enabled=True,
        reuse_cache_path=tmp_path / "cache.sqlite",
        web_tool_registry=(
            ToolRegistryEntry(
                tool_name="web_search",
                canonical_tool_family="public_web_search",
                tool_version="1",
                result_schema_version="1",
            ),
        ),
    )
    app = create_app(settings, http_client=upstream_client, trace_sink=sink)
    auth = {"x-flowpilot-api-key": "test-key"}
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            await client.post(
                "/flowpilot/v1/jobs",
                headers=auth,
                json={"job_id": "job-1"},
            )
            await client.post(
                "/flowpilot/v1/lines",
                headers=auth,
                json={
                    "job_id": "job-1",
                    "line_id": "line-1",
                    "context_epoch": 1,
                    "base_context_cursor": "cursor-0",
                    "context_digest": _digest(),
                    "conversation_id": "conversation-line-1",
                },
            )
            identity = {
                "job_id": "job-1",
                "line_id": "line-1",
                "tail_request_id": "tail-1",
                "llm_call_id": "llm-1",
                "action_id": "action-1",
                "tool_call_id": "tool-1",
            }
            reuse_payload = {
                "protocol_version": "flowpilot-phase1-reuse-v3",
                "identity": identity,
                "tool_name": "web_search",
                "arguments": {"query": "private-query"},
                "scope": {},
            }
            stale = await client.post(
                "/flowpilot/v1/reuse/resolve", headers=auth, json=reuse_payload
            )
            assert stale.status_code == 409
            llm_headers = {
                **auth,
                "x-flowpilot-protocol-version": "flowpilot-phase0-v2",
                "x-flowpilot-job-id": "job-1",
                "x-flowpilot-line-id": "line-1",
                "x-flowpilot-tail-request-id": "tail-1",
                "x-flowpilot-llm-call-id": "llm-1",
                "x-flowpilot-tail-version": "0",
                "x-flowpilot-context-epoch": "1",
                "x-flowpilot-context-sequence": "0",
                "x-flowpilot-context-cursor": "cursor-0",
                "x-flowpilot-context-digest": _digest(),
                "x-flowpilot-request-id": "request-1",
                "x-flowpilot-request-attempt": "1",
                "x-flowpilot-conversation-id": "conversation-line-1",
            }
            assert (
                await client.post(
                    "/v1/chat/completions",
                    headers=llm_headers,
                    json={"model": "model-a", "messages": []},
                )
            ).status_code == 200
            leader = await client.post(
                "/flowpilot/v1/reuse/resolve", headers=auth, json=reuse_payload
            )
            assert leader.status_code == 200
            assert leader.json()["decision"] == "sync_and_execute_as_leader"
            health = await client.get("/flowpilot/health")
            assert health.json()["reuse_mode"] == "exact"
    serialized = str(sink.records)
    assert "private-query" not in serialized
    assert "authorization" not in serialized.lower()
    await upstream_client.aclose()
