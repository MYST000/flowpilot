from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import ToolRegistryEntry


@pytest.mark.anyio
async def test_phase3_api_semantic_audit_progress_and_privacy(tmp_path: Path) -> None:
    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "response-1",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
            },
        )

    sink = InMemoryTraceSink()
    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app = create_app(
        Settings(
            instances=(InferenceInstance("inference-a", "http://inference-a"),),
            trace_path=tmp_path / "trace.jsonl",
            ingress_api_key="test-key",
            reuse_enabled=True,
            reuse_cache_path=tmp_path / "cache.sqlite",
            web_tool_registry=(
                ToolRegistryEntry(
                    protocol_version="flowpilot-phase3-reuse-v2",
                    tool_name="web_search",
                    canonical_tool_family="public_web_search",
                    tool_version="1",
                    result_schema_version="1",
                    semantic_reuse_enabled=True,
                    semantic_similarity_threshold=0.55,
                ),
            ),
        ),
        http_client=upstream_client,
        trace_sink=sink,
    )
    auth = {"x-flowpilot-api-key": "test-key"}
    digest = "a" * 64

    async def register_tail(client: httpx.AsyncClient, line: str) -> dict[str, str]:
        await client.post(
            "/flowpilot/v1/lines",
            headers=auth,
            json={
                "job_id": "job-1",
                "line_id": line,
                "context_epoch": 1,
                "base_context_cursor": "cursor-0",
                "context_digest": digest,
                "conversation_id": "conversation-line-1",
            },
        )
        headers = {
            **auth,
            "x-flowpilot-protocol-version": "flowpilot-phase0-v2",
            "x-flowpilot-job-id": "job-1",
            "x-flowpilot-line-id": line,
            "x-flowpilot-tail-request-id": f"tail-{line}",
            "x-flowpilot-llm-call-id": f"llm-{line}",
            "x-flowpilot-tail-version": "0",
            "x-flowpilot-context-epoch": "1",
            "x-flowpilot-context-sequence": "0",
            "x-flowpilot-context-cursor": "cursor-0",
            "x-flowpilot-context-digest": digest,
            "x-flowpilot-request-id": "request-1",
            "x-flowpilot-request-attempt": "1",
            "x-flowpilot-conversation-id": "conversation-line-1",
        }
        response = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": "model-a", "messages": []},
        )
        assert response.status_code == 200
        return {
            "job_id": "job-1",
            "line_id": line,
            "tail_request_id": f"tail-{line}",
            "llm_call_id": f"llm-{line}",
            "action_id": f"action-{line}",
            "tool_call_id": f"tool-{line}",
        }

    def reuse(identity: dict[str, str], query: str) -> dict[str, object]:
        return {
            "protocol_version": "flowpilot-phase3-reuse-v2",
            "identity": identity,
            "tool_name": "web_search",
            "arguments": {"query": query},
            "scope": {},
        }

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            await client.post(
                "/flowpilot/v1/jobs",
                headers=auth,
                json={"job_id": "job-1"},
            )
            leader_identity = await register_tail(client, "line-1")
            leader = await client.post(
                "/flowpilot/v1/reuse/resolve",
                headers=auth,
                json=reuse(
                    leader_identity, "flowpilot semantic scheduler private-query"
                ),
            )
            assert leader.json()["decision"] == "sync_and_execute_as_leader"
            binding_id = leader.json()["binding_id"]
            progress = await client.post(
                f"/flowpilot/v1/reuse/bindings/{binding_id}/progress",
                headers=auth,
                json={
                    "protocol_version": "flowpilot-phase3-reuse-v2",
                    "binding_id": binding_id,
                    "identity": leader_identity,
                    "sequence": 1,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "estimated_remaining_ms": 25,
                },
            )
            assert progress.status_code == 202
            published = await client.post(
                f"/flowpilot/v1/reuse/bindings/{binding_id}/result",
                headers=auth,
                json={
                    "protocol_version": "flowpilot-phase3-reuse-v2",
                    "binding_id": binding_id,
                    "identity": leader_identity,
                    "result": {"items": [{"title": "private-result"}]},
                },
            )
            assert published.status_code == 200

            follower_identity = await register_tail(client, "line-2")
            semantic = await client.post(
                "/flowpilot/v1/reuse/resolve",
                headers=auth,
                json=reuse(
                    follower_identity, "semantic scheduler private-query flowpilot"
                ),
            )
            assert semantic.json()["match_kind"] == "semantic"
            match_id = semantic.json()["semantic_match_id"]
            feedback = await client.post(
                "/flowpilot/v1/reuse/semantic/false-reuse",
                headers=auth,
                json={
                    "semantic_match_id": match_id,
                    "reason": "not_equivalent",
                    "evidence_digest": hashlib.sha256(b"label-1").hexdigest(),
                    "observed_at": datetime.now(UTC).isoformat(),
                },
            )
            assert feedback.status_code == 202
            health = await client.get("/flowpilot/health")
            assert health.json()["reuse_mode"] == "exact+semantic"
            snapshot = await client.get("/flowpilot/v1/reuse", headers=auth)
            assert snapshot.json()["semantic"]["counters"]["false_reuse_reports"] == 1
            policy = await client.put(
                "/flowpilot/v1/reuse/semantic/policy",
                headers=auth,
                json={
                    "version": 1,
                    "expected_version": 0,
                    "enabled": False,
                    "tool_name": "web_search",
                },
            )
            assert policy.status_code == 200
            assert policy.json()["disabled_tools"] == ["web_search"]

    trace = json.dumps(sink.records)
    assert "private-query" not in trace
    assert "private-result" not in trace
    assert "authorization" not in trace.casefold()
    assert "tool_reuse_false_reuse" in trace
    assert "tool_reuse_semantic_policy" in trace
    await upstream_client.aclose()
