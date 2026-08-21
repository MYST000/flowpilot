from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import ToolRegistryEntry


@pytest.mark.anyio
async def test_phase2_api_round_trip_is_durable_and_metadata_only(
    tmp_path: Path,
) -> None:
    upstream_calls = 0

    async def upstream(_request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        if upstream_calls == 2:
            return httpx.Response(
                200,
                json={
                    "id": "response-2",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "final-private",
                            },
                        }
                    ],
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "response-1",
                "choices": [
                    {
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "tool-1",
                                    "type": "function",
                                    "function": {
                                        "name": "web_search",
                                        "arguments": '{"q":"private"}',
                                    },
                                }
                            ],
                        },
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
                    tool_name="web_search",
                    canonical_tool_family="web_search",
                    tool_version="1",
                    result_schema_version="1",
                ),
            ),
            dcs_enabled=True,
            dcs_wal_path=tmp_path / "dcs.sqlite",
            dcs_encryption_key=Fernet.generate_key().decode(),
        ),
        http_client=upstream_client,
        trace_sink=sink,
    )
    auth = {"x-flowpilot-api-key": "test-key"}
    digest = "a" * 64
    now = datetime.now(UTC)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://flowpilot"
        ) as client:
            await client.post(
                "/flowpilot/v1/jobs",
                headers=auth,
                json={"tenant_id": "tenant-1", "job_id": "job-1"},
            )
            await client.post(
                "/flowpilot/v1/lines",
                headers=auth,
                json={
                    "tenant_id": "tenant-1",
                    "job_id": "job-1",
                    "line_id": "line-1",
                    "context_epoch": 1,
                    "base_context_cursor": "cursor-0",
                    "context_digest": digest,
                },
            )
            llm_headers = {
                **auth,
                "x-flowpilot-protocol-version": "flowpilot-phase0-v1",
                "x-flowpilot-tenant-id": "tenant-1",
                "x-flowpilot-job-id": "job-1",
                "x-flowpilot-line-id": "line-1",
                "x-flowpilot-tail-request-id": "tail-1",
                "x-flowpilot-llm-call-id": "llm-1",
                "x-flowpilot-tail-version": "0",
                "x-flowpilot-context-epoch": "1",
                "x-flowpilot-context-sequence": "0",
                "x-flowpilot-context-cursor": "cursor-0",
                "x-flowpilot-context-digest": digest,
            }
            llm = await client.post(
                "/v1/chat/completions",
                headers=llm_headers,
                json={"model": "model-a", "messages": []},
            )
            assert llm.status_code == 200
            reuse = {
                "identity": {
                    "tenant_id": "tenant-1",
                    "job_id": "job-1",
                    "line_id": "line-1",
                    "tail_request_id": "tail-1",
                    "llm_call_id": "llm-1",
                    "action_id": "action-1",
                    "tool_call_id": "tool-1",
                },
                "tool_name": "web_search",
                "arguments": {"q": "private"},
                "scope": {"tenant_id": "tenant-1", "auth_scope": "anonymous"},
            }
            leader = await client.post(
                "/flowpilot/v1/reuse/resolve", headers=auth, json=reuse
            )
            assert leader.json()["decision"] == "sync_and_execute_as_leader"
            published = await client.post(
                f"/flowpilot/v1/reuse/bindings/{leader.json()['binding_id']}/result",
                headers=auth,
                json={
                    "binding_id": leader.json()["binding_id"],
                    "identity": reuse["identity"],
                    "result": {"items": [{"title": "sunny-private"}]},
                },
            )
            assert published.status_code == 200
            grant = await client.post(
                "/flowpilot/v1/dcs/delegations",
                headers=auth,
                json={
                    "policy_version": 1,
                    "expected_policy_version": 0,
                    "lease_id": "lease-1",
                    "tenant_id": "tenant-1",
                    "job_id": "job-1",
                    "line_id": "line-1",
                    "context_epoch": 1,
                    "base_context_cursor": "cursor-0",
                    "base_context_digest": digest,
                    "issued_at": (now - timedelta(seconds=1)).isoformat(),
                    "expires_at": (now + timedelta(minutes=1)).isoformat(),
                    "allowed_tool_names": ["web_search"],
                    "api_kind": "chat",
                    "request_snapshot": {
                        "model": "model-a",
                        "messages": [{"role": "user", "content": "question-private"}],
                        "stream": False,
                    },
                },
            )
            assert grant.status_code == 201
            reference = {
                "tenant_id": "tenant-1",
                "job_id": "job-1",
                "line_id": "line-1",
                "context_epoch": 1,
                "lease_id": "lease-1",
                "base_context_cursor": "cursor-0",
                "delta_digest": grant.json()["delta_digest"],
            }
            messages = [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tool-1",
                            "type": "function",
                            "function": {
                                "name": "web_search",
                                "arguments": '{"q":"private"}',
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "tool-1",
                    "content": '{"items":[{"title":"sunny-private"}]}',
                },
            ]
            deferred = await client.post(
                "/flowpilot/v1/dcs/reuse/resolve",
                headers=auth,
                json={"reuse": reuse, "delegation": reference},
            )
            assert deferred.json()["decision"] == "defer_with_cached_result"
            assert deferred.json()["reuse_kind"] == "exact_historical"
            messages[1]["content"] = deferred.json()["provider_content"]
            append = await client.post(
                "/flowpilot/v1/dcs/deltas/append",
                headers=auth,
                json={
                    "reference": reference,
                    "expected_last_seq": 0,
                    "parent_llm_call_id": "llm-1",
                    "messages": messages,
                    "tool_call_ids": ["tool-1"],
                    "resolution_receipts": [deferred.json()["resolution_receipt"]],
                    "result_digests": [deferred.json()["result_digest"]],
                },
            )
            assert append.status_code == 200
            reference["delta_digest"] = append.json()["delta_digest"]
            continuation = await client.post(
                "/flowpilot/v1/dcs/continuations",
                headers=auth,
                json={"reference": reference, "parent_llm_call_id": "llm-1"},
            )
            assert continuation.status_code == 200
            assert continuation.json()["body"]["messages"][-2:] == messages
            pending_wal = (tmp_path / "dcs.sqlite").read_bytes()
            assert b"question-private" not in pending_wal
            assert b"sunny-private" not in pending_wal

            conflicting_agent_headers = {
                **llm_headers,
                "x-flowpilot-tail-request-id": "tail-conflict",
                "x-flowpilot-llm-call-id": "llm-conflict",
                "x-flowpilot-tail-version": "1",
                "x-flowpilot-context-sequence": "1",
            }
            conflicting_agent = await client.post(
                "/v1/chat/completions",
                headers=conflicting_agent_headers,
                json={"model": "model-a", "messages": []},
            )
            assert conflicting_agent.status_code == 409
            assert "active DCS writer" in conflicting_agent.text

            delegated_headers = {
                **llm_headers,
                "x-flowpilot-tail-request-id": "tail-2",
                "x-flowpilot-llm-call-id": "llm-2",
                "x-flowpilot-tail-version": "1",
                "x-flowpilot-context-sequence": str(append.json()["last_seq"]),
                "x-flowpilot-context-digest": reference["delta_digest"],
                "x-flowpilot-request-origin": "scheduler_delegated",
                "x-flowpilot-delegation-lease-id": "wrong-lease",
            }
            invalid_delegated = await client.post(
                "/v1/chat/completions",
                headers=delegated_headers,
                json=continuation.json()["body"],
            )
            assert invalid_delegated.status_code == 409
            assert "active DCS writer" in invalid_delegated.text

            delegated_headers["x-flowpilot-delegation-lease-id"] = "lease-1"
            delegated = await client.post(
                "/v1/chat/completions",
                headers=delegated_headers,
                json=continuation.json()["body"],
            )
            assert delegated.status_code == 200
            assert delegated.headers["x-flowpilot-tail-version"] == "2"
            assert delegated.json()["choices"][0]["message"]["content"] == (
                "final-private"
            )

            sync = await client.post(
                "/flowpilot/v1/dcs/sync",
                headers=auth,
                json={
                    "reference": reference,
                    "barrier_reason": "terminal_response",
                    "parent_llm_call_id": "llm-2",
                    "barrier_messages": [
                        {"role": "assistant", "content": "final-private"}
                    ],
                },
            )
            assert sync.status_code == 200
            ack = await client.post(
                "/flowpilot/v1/dcs/sync/ack",
                headers=auth,
                json={
                    "reference": reference,
                    "first_seq": sync.json()["first_seq"],
                    "last_seq": sync.json()["last_seq"],
                    "delta_digest": sync.json()["delta_digest"],
                    "new_context_cursor": "cursor-2",
                    "new_context_digest": "b" * 64,
                },
            )
            assert ack.status_code == 200
            assert ack.json()["state"] == "acked"
            health = await client.get("/flowpilot/health")
            assert health.json()["context_sync"] == "phase2-dcs-v1"

    trace = json.dumps(sink.records)
    assert "question-private" not in trace
    assert "sunny-private" not in trace
    assert "final-private" not in trace
    assert "context_delta_append" in trace
    assert "context_sync_ack" in trace
    wal_bytes = (tmp_path / "dcs.sqlite").read_bytes()
    assert b"question-private" not in wal_bytes
    assert b"sunny-private" not in wal_bytes
    await upstream_client.aclose()
