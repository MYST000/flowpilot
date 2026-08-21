from __future__ import annotations

import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.responses import Response

from flowpilot.config import Settings
from flowpilot.context import DCSConflict, DeferredContextManager
from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.gateway.router import InferenceRouter
from flowpilot.gateway.service import (
    GatewayAuthenticationError,
    GatewayUpstreamError,
    LLMGateway,
)
from flowpilot.observability.trace import JsonlTraceSink, TraceRecorder, TraceSink
from flowpilot.protocol import (
    BindingFailureReport,
    ContextDeltaAppend,
    ContextReconcileRequest,
    ContextSyncAck,
    ContextSyncBegin,
    DCSReference,
    DeferredBindingPoll,
    DeferredReuseResolveRequest,
    DelegationPolicy,
    DependencyUpdate,
    FalseReuseReport,
    FollowerCancellation,
    InternalContinuationRequest,
    JobRegistration,
    KVStateEvent,
    LeaderProgressReport,
    LeaderResultPublish,
    LineFinish,
    LineRegistration,
    ReuseDecisionKind,
    SemanticReusePolicyUpdate,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
    ToolTelemetryEvent,
)
from flowpilot.reuse import ReuseConflict, WebReuseController
from flowpilot.reuse.semantic import SemanticEmbedder


def create_app(
    settings: Settings | None = None,
    *,
    http_client: httpx.AsyncClient | None = None,
    trace_sink: TraceSink | None = None,
    semantic_embedder: SemanticEmbedder | None = None,
) -> FastAPI:
    resolved = settings or Settings.from_env()
    frontier = LineTailFrontier()
    router = InferenceRouter(resolved.instances)
    recorder = TraceRecorder(trace_sink or JsonlTraceSink(resolved.trace_path))
    reuse = (
        WebReuseController(
            resolved.web_tool_registry,
            resolved.reuse_cache_path,
            lease_seconds=resolved.reuse_lease_seconds,
            embedder=semantic_embedder,
            semantic_disabled_tenants=resolved.semantic_disabled_tenants,
        )
        if resolved.reuse_enabled
        else None
    )
    dcs = (
        DeferredContextManager(
            resolved.dcs_wal_path,
            resolved.dcs_encryption_key or "",
        )
        if resolved.dcs_enabled
        else None
    )
    owns_client = http_client is None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = http_client or httpx.AsyncClient(
            timeout=resolved.request_timeout_seconds
        )
        app.state.llm_gateway = LLMGateway(
            client,
            router,
            frontier,
            recorder,
            ingress_api_key=resolved.ingress_api_key,
            tenant_api_keys=resolved.tenant_api_keys,
            require_ingress_auth=resolved.require_ingress_auth,
            identity_validator=(dcs.authorize_llm_request if dcs is not None else None),
        )
        try:
            yield
        finally:
            if owns_client:
                await client.aclose()

    app = FastAPI(title="FlowPilot phase 3", lifespan=lifespan)
    app.state.frontier = frontier
    app.state.recorder = recorder
    app.state.reuse = reuse
    app.state.dcs = dcs

    @app.get("/flowpilot/health")
    async def health(request: Request) -> JSONResponse:
        gateway: LLMGateway = request.app.state.llm_gateway
        upstreams = await gateway.health()
        trace_healthy = await request.app.state.recorder.healthy()
        ready = any(upstreams.values()) and trace_healthy
        return JSONResponse(
            {
                "status": "ok" if ready else "degraded",
                "protocol_version": "flowpilot-phase0-v1",
                "llm_instances": upstreams,
                "state_backend": (
                    "process-local-frontier+sqlite-dcs-wal"
                    if dcs is not None
                    else "process-local-single-worker"
                ),
                "tool_execution": "local-agent-only",
                "reuse_enabled": reuse is not None,
                "reuse_mode": (
                    "exact+semantic"
                    if reuse is not None
                    and any(
                        item.semantic_reuse_enabled
                        for item in resolved.web_tool_registry
                    )
                    else "exact"
                    if reuse is not None
                    else "disabled"
                ),
                "kv_telemetry": "unsupported",
                "context_sync": "phase2-dcs-v1" if dcs is not None else "disabled",
                "restart_resume": (
                    "frontier-and-no-pending-dcs-only"
                    if dcs is not None
                    else "unsupported"
                ),
                "trace": {
                    "status": "ok" if trace_healthy else "degraded",
                    "rotation": "unsupported",
                    "restart_continuity": "unsupported",
                },
            },
            status_code=200 if ready else 503,
        )

    @app.get("/flowpilot/metrics")
    async def metrics(request: Request) -> JSONResponse:
        recorder: TraceRecorder = request.app.state.recorder
        return JSONResponse(await recorder.snapshot())

    @app.get("/metrics")
    async def prometheus_metrics(request: Request) -> PlainTextResponse:
        recorder: TraceRecorder = request.app.state.recorder
        return PlainTextResponse(await recorder.prometheus())

    @app.post("/flowpilot/v1/jobs", status_code=201)
    async def register_job(
        payload: JobRegistration, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            job = await request.app.state.frontier.register_job(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.recorder.emit(
            "job_submit",
            identity={"tenant_id": payload.tenant_id, "job_id": payload.job_id},
            fields={"default_slo_ms": payload.default_slo_ms},
        )
        return {
            "tenant_id": job.tenant_id,
            "job_id": job.job_id,
            "default_slo_ms": job.default_slo_ms,
        }

    @app.post("/flowpilot/v1/lines", status_code=201)
    async def register_line(
        payload: LineRegistration, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            tail = await request.app.state.frontier.register_line(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.recorder.emit(
            "line_register",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": payload.line_id,
            },
            fields={
                "context_epoch": payload.context_epoch,
                "context_sequence": payload.context_sequence,
                "base_context_cursor": payload.base_context_cursor,
                "context_digest": payload.context_digest,
            },
        )
        return {"line_id": tail.line_id, "version": tail.version, "state": tail.state}

    @app.put("/flowpilot/v1/lines/{line_id}/dependencies")
    async def update_dependencies(
        line_id: str,
        payload: DependencyUpdate,
        request: Request,
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        if payload.line_id != line_id:
            raise HTTPException(status_code=400, detail="line_id does not match path")
        try:
            tail = await request.app.state.frontier.replace_dependencies(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.recorder.emit(
            "line_dependencies",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": line_id,
            },
            fields={
                "version": payload.version,
                "prerequisite_line_ids": list(payload.prerequisite_line_ids),
            },
        )
        return {
            "line_id": line_id,
            "dependency_version": tail.dependency_version,
            "prerequisite_line_ids": list(tail.dependencies),
        }

    @app.post("/flowpilot/v1/lines/{line_id}/finish")
    async def finish_line(
        line_id: str,
        payload: LineFinish,
        request: Request,
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        if payload.line_id != line_id:
            raise HTTPException(status_code=400, detail="line_id does not match path")
        try:
            tail, released = await request.app.state.frontier.finish_line(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.recorder.emit(
            "line_finish",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": line_id,
                "tail_request_id": payload.tail_request_id,
            },
            fields={
                "tail_version": tail.version,
                "released_line_ids": list(released),
            },
        )
        return {
            "line_id": line_id,
            "state": tail.state,
            "released_line_ids": list(released),
        }

    @app.get("/flowpilot/v1/jobs/{job_id}/frontier")
    async def frontier_snapshot(
        job_id: str,
        tenant_id: str,
        request: Request,
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, tenant_id)
        try:
            return await request.app.state.frontier.snapshot(tenant_id, job_id)
        except FrontierConflict as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/flowpilot/v1/events/tools", status_code=202)
    async def tool_event(
        payload: ToolTelemetryEvent,
        request: Request,
    ) -> dict[str, str]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            _tail, duplicate = await request.app.state.frontier.record_tool_event(
                payload
            )
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if duplicate:
            await request.app.state.recorder.increment("tool_duplicate_events")
            return {"status": "duplicate"}
        await request.app.state.recorder.emit(
            f"tool_{payload.event_kind.value}",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "tail_request_id": payload.tail_request_id,
                "llm_call_id": payload.llm_call_id,
                "action_id": payload.action_id,
                "tool_call_id": payload.tool_call_id,
                "telemetry_event_id": payload.event_id,
            },
            fields={
                "tool_name": payload.tool_name,
                "tool_class": payload.tool_class.value,
                "input_digest": payload.input_digest,
                "result_size_bytes": payload.result_size_bytes,
                "measured_latency_ms": payload.measured_latency_ms,
                "error_class": payload.error_class,
                "sequence": payload.sequence,
                "execution_attempt": payload.execution_attempt,
            },
        )
        return {"status": "accepted"}

    @app.post("/flowpilot/v1/events/kv", status_code=202)
    async def kv_event(payload: KVStateEvent, request: Request) -> dict[str, str]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            await request.app.state.frontier.require_line(
                payload.tenant_id, payload.job_id, payload.line_id
            )
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if not router.contains(payload.instance_id):
            raise HTTPException(status_code=400, detail="unknown inference instance")
        await request.app.state.recorder.emit(
            "kv_state",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "session_id": payload.session_id,
            },
            fields={
                "instance_id": payload.instance_id,
                "tier": payload.tier.value,
                "bytes": payload.bytes,
                "restore_cost_ms": payload.restore_cost_ms,
            },
        )
        return {"status": "accepted"}

    @app.post("/flowpilot/v1/reuse/resolve")
    async def resolve_tool(
        payload: ToolReuseResolveRequest, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.identity.tenant_id)
        controller = _require_reuse(request)
        await _require_reuse_tail(request, payload.identity)
        try:
            decision = await controller.resolve(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_resolve",
            identity=payload.identity.model_dump(mode="json"),
            fields={
                "tool_name": payload.tool_name,
                "decision": decision.decision.value,
                "descriptor_digest": decision.descriptor_digest,
                "binding_id": decision.binding_id,
                "result_size_bytes": (
                    decision.provenance.returned_size if decision.provenance else None
                ),
                "match_kind": (
                    decision.match_kind.value if decision.match_kind else None
                ),
                "similarity_score": decision.similarity_score,
                "semantic_match_id": decision.semantic_match_id,
            },
        )
        return decision.model_dump(mode="json", exclude_none=True)

    @app.post(
        "/flowpilot/v1/reuse/bindings/{binding_id}/progress", status_code=202
    )
    async def report_binding_progress(
        binding_id: str, payload: LeaderProgressReport, request: Request
    ) -> dict[str, str]:
        _authorize_control(request, resolved, payload.identity.tenant_id)
        if payload.binding_id != binding_id:
            raise HTTPException(
                status_code=400, detail="binding_id does not match path"
            )
        await _require_reuse_tail(request, payload.identity)
        try:
            duplicate = await _require_reuse(request).progress(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_leader_progress",
            identity=payload.identity.model_dump(mode="json"),
            fields={
                "binding_id": binding_id,
                "sequence": payload.sequence,
                "estimated_remaining_ms": payload.estimated_remaining_ms,
                "duplicate": duplicate,
            },
        )
        return {"status": "duplicate" if duplicate else "accepted"}

    @app.post("/flowpilot/v1/reuse/semantic/false-reuse", status_code=202)
    async def report_false_reuse(
        payload: FalseReuseReport, request: Request
    ) -> dict[str, str]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            duplicate = await _require_reuse(request).report_false_reuse(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_false_reuse",
            identity={
                "tenant_id": payload.tenant_id,
                "semantic_match_id": payload.semantic_match_id,
            },
            fields={
                "reason": payload.reason,
                "evidence_digest": payload.evidence_digest,
                "duplicate": duplicate,
            },
        )
        return {"status": "duplicate" if duplicate else "accepted"}

    @app.put("/flowpilot/v1/reuse/semantic/policy")
    async def update_semantic_policy(
        payload: SemanticReusePolicyUpdate, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            result = await _require_reuse(request).update_semantic_policy(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_semantic_policy",
            identity={"tenant_id": payload.tenant_id} if payload.tenant_id else None,
            fields={
                "version": payload.version,
                "enabled": payload.enabled,
                "tool_name": payload.tool_name,
            },
        )
        return result

    @app.post("/flowpilot/v1/reuse/bindings/{binding_id}/result")
    async def publish_result(
        binding_id: str, payload: LeaderResultPublish, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.identity.tenant_id)
        if payload.binding_id != binding_id:
            raise HTTPException(
                status_code=400, detail="binding_id does not match path"
            )
        controller = _require_reuse(request)
        await _require_reuse_tail(request, payload.identity)
        try:
            decision = await controller.publish(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if decision.provenance is None:
            raise HTTPException(
                status_code=500, detail="published result lacks provenance"
            )
        await recorder.emit(
            "tool_reuse_leader_finish",
            identity=payload.identity.model_dump(mode="json"),
            fields={
                "binding_id": binding_id,
                "cacheable": payload.cacheable,
                "result_size_bytes": decision.provenance.original_size,
            },
        )
        return decision.model_dump(mode="json", exclude_none=True)

    @app.get("/flowpilot/v1/reuse/bindings/{binding_id}")
    async def poll_binding(
        binding_id: str,
        request: Request,
        tenant_id: str,
        job_id: str,
        line_id: str,
        tail_request_id: str,
        llm_call_id: str,
        action_id: str,
        tool_call_id: str,
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, tenant_id)
        identity = ToolReuseIdentity(
            tenant_id=tenant_id,
            job_id=job_id,
            line_id=line_id,
            tail_request_id=tail_request_id,
            llm_call_id=llm_call_id,
            action_id=action_id,
            tool_call_id=tool_call_id,
        )
        await _require_reuse_tail(request, identity)
        try:
            decision = await _require_reuse(request).poll(binding_id, identity)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return decision.model_dump(mode="json", exclude_none=True)

    @app.post("/flowpilot/v1/reuse/bindings/{binding_id}/fail", status_code=202)
    async def fail_binding(
        binding_id: str, payload: BindingFailureReport, request: Request
    ) -> dict[str, str]:
        _authorize_control(request, resolved, payload.identity.tenant_id)
        if payload.binding_id != binding_id:
            raise HTTPException(
                status_code=400, detail="binding_id does not match path"
            )
        await _require_reuse_tail(request, payload.identity)
        try:
            await _require_reuse(request).fail(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_leader_fail",
            identity=payload.identity.model_dump(mode="json"),
            fields={"binding_id": binding_id, "error_class": payload.error_class},
        )
        return {"status": "accepted"}

    @app.post("/flowpilot/v1/reuse/bindings/{binding_id}/cancel", status_code=202)
    async def cancel_follower(
        binding_id: str, payload: FollowerCancellation, request: Request
    ) -> dict[str, str]:
        _authorize_control(request, resolved, payload.identity.tenant_id)
        if payload.binding_id != binding_id:
            raise HTTPException(
                status_code=400, detail="binding_id does not match path"
            )
        try:
            await _require_reuse(request).cancel_follower(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"status": "accepted"}

    @app.get("/flowpilot/v1/reuse")
    async def reuse_snapshot(request: Request) -> dict[str, Any]:
        _authorize_control(request, resolved)
        return await _require_reuse(request).snapshot()

    @app.post("/flowpilot/v1/dcs/delegations", status_code=201)
    async def grant_delegation(
        payload: DelegationPolicy, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        await _require_dcs_line(request, payload)
        try:
            result = await _require_dcs(request).grant(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        snapshot_json = json.dumps(
            payload.request_snapshot,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        await recorder.emit(
            "context_delegation_grant",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "context_epoch": str(payload.context_epoch),
            },
            fields={
                "policy_version": payload.policy_version,
                "lease_id": payload.lease_id,
                "expires_at": payload.expires_at.isoformat(),
                "allowed_tool_names": list(payload.allowed_tool_names),
                "request_snapshot_bytes": len(snapshot_json),
                "request_snapshot_digest": hashlib.sha256(snapshot_json).hexdigest(),
            },
        )
        return result

    @app.post("/flowpilot/v1/dcs/delegations/release")
    async def release_delegation(
        payload: DCSReference, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            result = await _require_dcs(request).release(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "context_delegation_release",
            identity=_dcs_identity(payload),
            fields={"state": result["state"], "pending_message_count": 0},
        )
        return result

    @app.post("/flowpilot/v1/dcs/reuse/resolve")
    async def resolve_deferred_tool(
        payload: DeferredReuseResolveRequest, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.reuse.identity.tenant_id)
        await _require_reuse_tail(request, payload.reuse.identity)
        manager = _require_dcs(request)
        try:
            await manager.authorize_reuse(payload.delegation, payload.reuse.tool_name)
            decision = await _require_reuse(request).resolve(
                payload.reuse, defer_allowed=True
            )
            receipt = None
            if decision.decision == ReuseDecisionKind.DEFER_WITH_CACHED_RESULT:
                receipt = await manager.issue_resolution(
                    payload.delegation, payload.reuse, decision
                )
        except (DCSConflict, ReuseConflict) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_deferred_resolve",
            identity=payload.reuse.identity.model_dump(mode="json"),
            fields={
                "tool_name": payload.reuse.tool_name,
                "decision": decision.decision.value,
                "descriptor_digest": decision.descriptor_digest,
                "binding_id": decision.binding_id,
            },
        )
        result = decision.model_dump(mode="json", exclude_none=True)
        if receipt is not None:
            result.update(receipt)
        return result

    @app.post("/flowpilot/v1/dcs/reuse/bindings/poll")
    async def poll_deferred_binding(
        payload: DeferredBindingPoll, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.reuse.identity.tenant_id)
        await _require_reuse_tail(request, payload.reuse.identity)
        try:
            manager = _require_dcs(request)
            await manager.validate_reference(payload.delegation)
            decision = await _require_reuse(request).poll_deferred(
                payload.binding_id, payload.reuse
            )
            receipt = None
            if decision.decision == ReuseDecisionKind.DEFER_WITH_CACHED_RESULT:
                receipt = await manager.issue_resolution(
                    payload.delegation, payload.reuse, decision
                )
        except (DCSConflict, ReuseConflict) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        result = decision.model_dump(mode="json", exclude_none=True)
        if receipt is not None:
            result.update(receipt)
        return result

    @app.post("/flowpilot/v1/dcs/deltas/append")
    async def append_context_delta(
        payload: ContextDeltaAppend, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.reference.tenant_id)
        try:
            result = await _require_dcs(request).append(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "context_delta_append",
            identity=_dcs_identity(payload.reference),
            fields={
                "first_seq": payload.expected_last_seq + 1,
                "last_seq": result["last_seq"],
                "delta_digest": result["delta_digest"],
                "message_count": len(payload.messages),
                "pending_bytes": result["pending_bytes"],
                "reuse_kinds": result["reuse_kinds"],
                "tool_call_ids": list(payload.tool_call_ids),
            },
        )
        return result

    @app.post("/flowpilot/v1/dcs/sync")
    async def begin_context_sync(
        payload: ContextSyncBegin, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.reference.tenant_id)
        try:
            result = await _require_dcs(request).begin_sync(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "context_sync_request",
            identity=_dcs_identity(payload.reference),
            fields={
                "first_seq": result["first_seq"],
                "last_seq": result["last_seq"],
                "delta_digest": result["delta_digest"],
                "message_count": len(result["messages"]),
                "barrier_reason": payload.barrier_reason.value,
                "pending_local_tool_call_ids": list(
                    payload.pending_local_tool_call_ids
                ),
            },
        )
        return result

    @app.post("/flowpilot/v1/dcs/sync/next")
    async def next_context_sync_chunk(
        payload: DCSReference, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        try:
            return await _require_dcs(request).next_sync_chunk(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/flowpilot/v1/dcs/sync/ack")
    async def acknowledge_context_sync(
        payload: ContextSyncAck, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.reference.tenant_id)
        try:
            result = await _require_dcs(request).acknowledge(payload)
        except DCSConflict as exc:
            await recorder.emit(
                "context_sync_fail",
                identity=_dcs_identity(payload.reference),
                fields={"error_class": "ContextDiverged"},
            )
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "context_sync_ack",
            identity=_dcs_identity(payload.reference),
            fields={
                "first_seq": payload.first_seq,
                "last_seq": payload.last_seq,
                "delta_digest": payload.delta_digest,
                "duplicate": result["duplicate"],
                "remaining_messages": result["pending_message_count"],
            },
        )
        return result

    @app.post("/flowpilot/v1/dcs/reconcile")
    async def reconcile_context(
        payload: ContextReconcileRequest, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.tenant_id)
        result = await _require_dcs(request).reconcile(payload)
        await recorder.emit(
            "context_reconcile",
            identity={
                "tenant_id": payload.tenant_id,
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "context_epoch": str(payload.context_epoch),
            },
            fields={"status": result["status"]},
        )
        return result

    @app.post("/flowpilot/v1/dcs/continuations")
    async def prepare_internal_continuation(
        payload: InternalContinuationRequest, request: Request
    ) -> dict[str, Any]:
        _authorize_control(request, resolved, payload.reference.tenant_id)
        try:
            result = await _require_dcs(request).prepare_continuation(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        body = json.dumps(
            result["body"], sort_keys=True, separators=(",", ":")
        ).encode()
        await recorder.emit(
            "internal_continuation",
            identity=_dcs_identity(payload.reference),
            fields={
                "parent_llm_call_id": payload.parent_llm_call_id,
                "delta_seq": result["delta_seq"],
                "delta_digest": result["delta_digest"],
                "request_bytes": len(body),
                "request_digest": hashlib.sha256(body).hexdigest(),
            },
        )
        return result

    @app.get("/flowpilot/v1/dcs")
    async def dcs_snapshot(request: Request) -> dict[str, Any]:
        _authorize_control(request, resolved)
        return await _require_dcs(request).snapshot()

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        return await _proxy_request(request, "/v1/chat/completions", "chat")

    @app.post("/v1/responses")
    async def responses(request: Request) -> Response:
        return await _proxy_request(request, "/v1/responses", "responses")

    return app


async def _proxy_request(request: Request, path: str, api_kind: str) -> Response:
    gateway: LLMGateway = request.app.state.llm_gateway
    try:
        return await gateway.proxy(
            path=path,
            api_kind=api_kind,
            body=await request.body(),
            headers=request.headers,
            raw_query=request.scope.get("query_string", b""),
        )
    except GatewayAuthenticationError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GatewayUpstreamError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _authorize_control(
    request: Request, settings: Settings, tenant_id: str | None = None
) -> None:
    if not settings.require_ingress_auth:
        return
    supplied = request.headers.get("x-flowpilot-api-key")
    if supplied:
        for api_key, bound_tenant in settings.tenant_api_keys:
            if hmac.compare_digest(supplied, api_key):
                if tenant_id is None or tenant_id != bound_tenant:
                    raise HTTPException(
                        status_code=403,
                        detail="FlowPilot API key is not authorized for this tenant",
                    )
                return
    if (
        not supplied
        or not settings.ingress_api_key
        or not hmac.compare_digest(supplied, settings.ingress_api_key)
    ):
        raise HTTPException(
            status_code=401, detail="FlowPilot ingress authentication rejected"
        )


def _require_reuse(request: Request) -> WebReuseController:
    controller: WebReuseController | None = request.app.state.reuse
    if controller is None:
        raise HTTPException(status_code=503, detail="exact reuse is disabled")
    return controller


def _require_dcs(request: Request) -> DeferredContextManager:
    manager: DeferredContextManager | None = request.app.state.dcs
    if manager is None:
        raise HTTPException(status_code=503, detail="Phase 2 DCS is disabled")
    return manager


async def _require_dcs_line(request: Request, policy: DelegationPolicy) -> None:
    try:
        tail = await request.app.state.frontier.require_line(
            policy.tenant_id, policy.job_id, policy.line_id
        )
    except FrontierConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if (
        tail.context_epoch != policy.context_epoch
        or tail.base_context_cursor != policy.base_context_cursor
        or tail.context_digest != policy.base_context_digest
    ):
        raise HTTPException(
            status_code=409, detail="delegation base does not match authoritative line"
        )


def _dcs_identity(reference: DCSReference) -> dict[str, str]:
    return {
        "tenant_id": reference.tenant_id,
        "job_id": reference.job_id,
        "line_id": reference.line_id,
        "context_epoch": str(reference.context_epoch),
        "lease_id": reference.lease_id,
    }


async def _require_reuse_tail(request: Request, identity: ToolReuseIdentity) -> None:
    try:
        tail = await request.app.state.frontier.require_line(
            identity.tenant_id, identity.job_id, identity.line_id
        )
    except FrontierConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if (
        tail is None
        or tail.tail_request_id != identity.tail_request_id
        or tail.llm_call_id != identity.llm_call_id
        or tail.state != "NEXT_READY"
    ):
        raise HTTPException(status_code=409, detail="reuse identity is not active tail")
