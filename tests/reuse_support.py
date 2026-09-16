from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from flowpilot.frontier.store import LineTailFrontier, ToolCallSummary
from flowpilot.protocol import (
    JobRegistration,
    LeaderResultPublish,
    LineRegistration,
    RequestIdentity,
    ReuseScope,
    ToolRegistryEntry,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
    ToolTelemetryEvent,
)
from flowpilot.reuse.adapters.tavily import TAVILY_SCHEMA_DIGESTS
from flowpilot.reuse.contracts import canonical_json, digest
from flowpilot.reuse.controller import WebReuseController
from flowpilot.reuse.service import ReuseService


async def api_execution(
    client: httpx.AsyncClient,
    auth: dict[str, str],
    identity: dict[str, Any],
    decision: dict[str, Any],
    result: dict[str, Any],
    *,
    tool_name: str,
    conversation_id: str,
    request_id: str = "request-1",
) -> dict[str, Any]:
    common = {
        **identity,
        "request_id": request_id,
        "conversation_id": conversation_id,
        "context_epoch": 1,
        "attempt": 1,
        "execution_attempt": 1,
        "tool_name": tool_name,
        "tool_class": "web",
        "reuse_receipt_version": "flowpilot-execution-v1",
        **{
            key: decision[key]
            for key in (
                "binding_id",
                "input_digest",
                "adapter_id",
                "adapter_version",
                "result_schema_version",
                "executor_kind",
            )
        },
    }
    size = len(canonical_json(result).encode())
    for sequence, kind in ((1, "start"), (2, "finish")):
        response = await client.post(
            "/flowpilot/v1/events/tools",
            headers=auth,
            json={
                **common,
                "event_id": kind + "-" + identity["line_id"],
                "sequence": sequence,
                "event_kind": kind,
                "observed_at": datetime.now(UTC).isoformat(),
                **(
                    {
                        "result_digest": digest(result),
                        "result_size_bytes": size,
                        "measured_latency_ms": 1,
                    }
                    if kind == "finish"
                    else {}
                ),
            },
        )
        assert response.status_code == 202, response.text
    return {
        "binding_id": decision["binding_id"],
        "identity": identity,
        "result": result,
        "start_event_id": "start-" + identity["line_id"],
        "finish_event_id": "finish-" + identity["line_id"],
        "execution_attempt": 1,
        "input_digest": decision["input_digest"],
        "result_digest": digest(result),
        "result_size_bytes": size,
        "result_schema_version": decision["result_schema_version"],
    }


def registry(**updates: Any) -> ToolRegistryEntry:
    return ToolRegistryEntry(
        **{
            "tool_name": "tavily-search",
            "canonical_tool_family": "tavily_public_search",
            "tool_version": "0.2.1",
            "adapter_id": "tavily_search_mcp_v1",
            "input_schema_digest": TAVILY_SCHEMA_DIGESTS["tavily-search"],
            "result_schema_version": "mcp-observation-v1",
            **updates,
        }
    )


def service(
    path: Path, *, entry: ToolRegistryEntry | None = None, embedder: Any = None
) -> ReuseService:
    frontier = LineTailFrontier()
    controller = WebReuseController(
        (entry or registry(),), path, frontier=frontier, embedder=embedder
    )
    return ReuseService(
        controller, frontier, deployment_id="test", default_namespace="default"
    )


async def request(
    svc: ReuseService,
    line: str,
    *,
    job: str = "job",
    namespace: str = "ns",
    query: str = "flowpilot research",
    action: str | None = None,
    arguments: dict[str, Any] | None = None,
    tool_name: str = "tavily-search",
    semantic: bool = False,
    budget: int | None = None,
) -> ToolReuseResolveRequest:
    frontier = svc.frontier
    await frontier.register_job(
        JobRegistration(job_id=job, deployment_id="test", namespace_id=namespace)
    )
    await frontier.register_line(
        LineRegistration(
            job_id=job,
            line_id=line,
            conversation_id=line,
            context_epoch=1,
            base_context_cursor="root",
            context_digest="a" * 64,
        )
    )
    identity = RequestIdentity(
        job_id=job,
        line_id=line,
        request_id="request-" + line,
        tail_request_id="tail-" + line,
        llm_call_id="llm-" + line,
        conversation_id=line,
        attempt=1,
        expected_tail_version=0,
        context_epoch=1,
        context_sequence=0,
        base_context_cursor="root",
        context_digest="a" * 64,
        deployment_id="test",
        namespace_id=namespace,
    )
    await frontier.begin_request(identity, "test-model")
    await frontier.complete_response(
        identity,
        response_id="response",
        tool_calls=[ToolCallSummary("tool-" + line, tool_name, None, None)],
    )
    return ToolReuseResolveRequest(
        protocol_version="flowpilot-phase3-reuse-v3"
        if semantic
        else "flowpilot-phase1-reuse-v3",
        identity=ToolReuseIdentity(
            job_id=job,
            line_id=line,
            tail_request_id=identity.tail_request_id,
            llm_call_id=identity.llm_call_id,
            tool_call_id="tool-" + line,
            action_id=action,
        ),
        tool_name=tool_name,
        arguments=arguments if arguments is not None else {"query": query},
        scope=ReuseScope(),
        input_schema_digest=TAVILY_SCHEMA_DIGESTS.get(tool_name),
        output_budget_bytes=budget,
    )


def observation(
    tool_name: str = "tavily-search", text: str = "完整文本\nTitle: is content"
) -> dict[str, Any]:
    return {
        "kind": "MCPToolObservation",
        "tool_name": tool_name,
        "is_error": False,
        "content": [{"type": "text", "text": text, "cache_prompt": False}],
    }


async def execution(
    svc: ReuseService,
    req: ToolReuseResolveRequest,
    decision: Any,
    *,
    result: dict[str, Any] | None = None,
    finish: bool = True,
    cacheable: bool = True,
    ttl: int | None = None,
) -> LeaderResultPublish:
    result = result if result is not None else observation(req.tool_name)
    identity = req.identity.model_copy(
        update={"action_id": req.identity.action_id or "action-" + req.identity.line_id}
    )
    common = {
        **identity.model_dump(),
        "protocol_version": "flowpilot-phase0-v2",
        "job_id": identity.job_id,
        "line_id": identity.line_id,
        "request_id": "request-" + identity.line_id,
        "conversation_id": identity.line_id,
        "context_epoch": 1,
        "attempt": 1,
        "execution_attempt": 1,
        "tool_name": req.tool_name,
        "tool_class": "web",
        "binding_id": decision.binding_id,
        "input_digest": decision.input_digest,
        "adapter_id": decision.adapter_id,
        "adapter_version": decision.adapter_version,
        "executor_kind": decision.executor_kind,
        "result_schema_version": decision.result_schema_version,
        "reuse_receipt_version": "flowpilot-execution-v1",
    }
    start = ToolTelemetryEvent(
        **common,
        event_id="start-" + identity.line_id,
        sequence=1,
        event_kind="start",
        observed_at=datetime.now(UTC),
    )
    await svc.record_execution(start)
    size = len(canonical_json(result).encode())
    if finish:
        await svc.record_execution(
            ToolTelemetryEvent(
                **common,
                event_id="finish-" + identity.line_id,
                sequence=2,
                event_kind="finish",
                result_digest=digest(result),
                result_size_bytes=size,
                measured_latency_ms=1,
                observed_at=datetime.now(UTC),
                final_url_digest=result.get("final_url_digest"),
            )
        )
    return LeaderResultPublish(
        binding_id=decision.binding_id,
        identity=identity,
        result=result,
        cacheable=cacheable,
        ttl_seconds=ttl,
        start_event_id=start.event_id,
        finish_event_id="finish-" + identity.line_id,
        execution_attempt=1,
        input_digest=decision.input_digest,
        result_digest=digest(result),
        result_size_bytes=size,
        result_schema_version=decision.result_schema_version,
    )
