from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.responses import Response

from flowpilot.config import Settings
from flowpilot.context import DCSConflict, DeferredContextManager
from flowpilot.frontier.store import FrontierConflict, LinePhase, LineTailFrontier
from flowpilot.gateway.router import InferenceRouter, InstanceLoadProfile
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
    ForecastRequest,
    ForecastResult,
    InstanceLoadEvent,
    InternalContinuationRequest,
    JobRegistration,
    LeaderProgressReport,
    LeaderResultPublish,
    LineFinish,
    LineRegistration,
    ReuseDecisionKind,
    SemanticReusePolicyUpdate,
    ToolResolutionKind,
    ToolResolutionRecord,
    ToolResolutionSource,
    ToolResolutionStatus,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
    ToolTelemetryEvent,
)
from flowpilot.reuse import ReuseConflict, WebReuseController
from flowpilot.reuse.semantic import Qwen3Embedding, SemanticEmbedder
from flowpilot.reuse.service import ReuseService
from flowpilot.scheduling import (
    DeterministicToolAnalysisAdapter,
    ForecastAdapter,
    ForecastManager,
    NoOpForecastAdapter,
    ProjectionCalculator,
    ToolObservation,
    ToolResolutionStore,
)
from flowpilot.scheduling.duration import SyntheticToolDurationPrior
from flowpilot.scheduling.retention import RetentionController
from flowpilot.scheduling.runtime import SchedulingRuntime
from flowpilot.state import SQLiteSharedStateBackend

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    http_client: httpx.AsyncClient | None = None,
    trace_sink: TraceSink | None = None,
    semantic_embedder: SemanticEmbedder | None = None,
    forecast_adapter: ForecastAdapter | None = None,
    tool_duration_adapter: Any | None = None,
) -> FastAPI:
    resolved = settings or Settings.from_env()
    if resolved.workers != 1:
        raise ValueError(
            "multi-worker serving is fail-closed until the shared backend is "
            "wired into the complete frontier/DCS/reuse transaction; the "
            "SQLite backend currently validates the contract only"
        )
    frontier = LineTailFrontier()
    router = InferenceRouter(resolved.instances, policy=resolved.routing_policy)
    recorder = TraceRecorder(trace_sink or JsonlTraceSink(resolved.trace_path))
    native_embedder = (
        Qwen3Embedding(model_path=resolved.reuse_embedding_model_path)
        if resolved.reuse_enabled
        and semantic_embedder is None
        and any(entry.semantic_reuse_enabled for entry in resolved.web_tool_registry)
        else None
    )
    reuse = (
        ReuseService(
            WebReuseController(
                resolved.web_tool_registry,
                resolved.reuse_cache_path,
                lease_seconds=resolved.reuse_lease_seconds,
                embedder=semantic_embedder or native_embedder,
                frontier=frontier,
                max_payload_bytes=resolved.reuse_max_payload_bytes,
            ),
            frontier,
            deployment_id=resolved.reuse_deployment_id,
            default_namespace=resolved.reuse_default_namespace,
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
    resolution_store = ToolResolutionStore(
        duration_prior=(
            SyntheticToolDurationPrior(resolved.synthetic_tool_duration_seed)
            if resolved.synthetic_tool_duration_enabled
            else None
        )
    )
    projection_calculator = ProjectionCalculator(frontier, resolution_store)
    tool_analysis = DeterministicToolAnalysisAdapter()
    shared_state = (
        SQLiteSharedStateBackend(resolved.shared_state_path)
        if resolved.shared_state_path is not None
        else None
    )

    async def _forecast_event(
        event_type: str,
        forecast_request: ForecastRequest,
        result: ForecastResult | None,
        reason: str | None,
    ) -> None:
        fields: dict[str, Any] = {
            "request_id": forecast_request.request_id,
            "schema_version": forecast_request.schema_version,
            "model_id": forecast_request.model_id,
            "history_features_ref": forecast_request.history_features_ref,
            "requested_top_n": forecast_request.requested_top_n,
            "tool_catalog_version": forecast_request.tool_catalog_version,
            "deadline": (
                forecast_request.deadline.isoformat()
                if forecast_request.deadline is not None
                else None
            ),
            "reason": reason,
        }
        if result is not None:
            fields.update(
                {
                    "candidate_count": len(result.candidates),
                    "confidence": result.confidence,
                    "predictor_version": result.predictor_version,
                    "expires_at": result.expires_at.isoformat(),
                    "candidates": [
                        {
                            "tool_family": item.tool_family,
                            "probability": item.probability,
                            "duration_p50": item.duration_p50,
                            "duration_p90": item.duration_p90,
                        }
                        for item in result.candidates
                    ],
                }
            )
        await recorder.emit(
            event_type,
            identity={
                "job_id": forecast_request.job_id,
                "line_id": forecast_request.line_id,
            },
            fields=fields,
        )

    async def _forecast_prewarm(
        forecast_request: ForecastRequest, result: ForecastResult
    ) -> None:
        # Phase 4 only stores versioned metadata.  A deployment may replace
        # this callback with a Tool Cache index prewarmer; no payload is sent.
        await resolution_store.save_forecast(forecast_request, result)

    async def _notify_duration_resolution(record: ToolResolutionRecord) -> None:
        if tool_duration_adapter is None:
            return
        try:
            tool_duration_adapter.on_resolution(record)
        except (Exception, asyncio.CancelledError) as exc:
            if (
                isinstance(exc, asyncio.CancelledError)
                and (task := asyncio.current_task()) is not None
                and task.cancelling()
            ):
                raise
            await recorder.increment("tool_duration_resolution_failures")
            logger.warning(
                "Tool duration resolution feedback unavailable: %s", type(exc).__name__
            )

    async def _record_reuse_resolution(
        identity: ToolReuseIdentity,
        tool_name: str,
        decision: Any,
    ) -> None:
        kind_by_decision = {
            ReuseDecisionKind.DEFER_WITH_CACHED_RESULT: (
                ToolResolutionKind.HISTORICAL_HIT
            ),
            ReuseDecisionKind.SYNC_WITH_REUSED_RESULT: (
                ToolResolutionKind.HISTORICAL_HIT
            ),
            ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT: (
                ToolResolutionKind.INFLIGHT_FOLLOWER
            ),
            ReuseDecisionKind.WAIT_AND_SYNC_REUSED_RESULT: (
                ToolResolutionKind.INFLIGHT_FOLLOWER
            ),
            ReuseDecisionKind.SYNC_AND_EXECUTE_AS_LEADER: (
                ToolResolutionKind.LOCAL_LEADER
            ),
            ReuseDecisionKind.EXECUTE_LOCALLY: ToolResolutionKind.LOCAL_ONLY,
        }
        kind = kind_by_decision.get(decision.decision)
        if kind is None:
            return
        status = (
            ToolResolutionStatus.READY
            if decision.result is not None
            else ToolResolutionStatus.WAITING
            if kind == ToolResolutionKind.INFLIGHT_FOLLOWER
            else ToolResolutionStatus.RESOLVING
        )
        source = (
            ToolResolutionSource.WEB_HISTORY
            if kind == ToolResolutionKind.HISTORICAL_HIT
            else ToolResolutionSource.INFLIGHT_STATE
            if kind == ToolResolutionKind.INFLIGHT_FOLLOWER
            else ToolResolutionSource.LOCAL_MODEL
        )
        ready_at = None
        if decision.result is not None:
            ready_at = datetime.now(UTC)
        elif decision.leader_estimated_remaining_ms is not None:
            ready_at = datetime.now(UTC) + timedelta(
                milliseconds=decision.leader_estimated_remaining_ms
            )
        duration_record = await resolution_store.resolve_reuse(
            identity=identity,
            tool_family=tool_name,
            resolution=kind,
            status=status,
            source=source,
            ready_at_estimate=ready_at,
            confidence=1.0 if decision.result is not None else 0.5,
        )
        await _notify_duration_resolution(duration_record)
        if app.state.scheduling.retention is not None:
            app.state.scheduling.retention.tool_resolved(identity)
        if decision.result is not None:
            tool_analysis.observe_resolution(
                tool_name,
                ready_latency_ms=0.0,
                result_bytes=len(str(decision.result).encode("utf-8")),
                cache_hit=True,
                inference_cost_ms=0.0,
            )

    forecast_manager = ForecastManager(
        forecast_adapter or NoOpForecastAdapter(),
        timeout_seconds=resolved.forecast_timeout_seconds,
        ttl_seconds=resolved.forecast_ttl_seconds,
        min_confidence=resolved.forecast_min_confidence,
        on_event=_forecast_event,
        on_prewarm=_forecast_prewarm,
    )
    forecast_active = resolved.forecast_enabled or forecast_adapter is not None
    owns_client = http_client is None

    async def maintain_reuse() -> None:
        assert reuse is not None
        while True:
            try:
                await reuse.maintenance(
                    max_payload_bytes=resolved.reuse_max_payload_bytes
                )
                await reuse.controller.rebuild_vectors()
            except Exception:
                await recorder.increment("reuse_maintenance_failures")
            await asyncio.sleep(resolved.reuse_maintenance_interval_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = http_client or httpx.AsyncClient(
            timeout=resolved.request_timeout_seconds,
            limits=httpx.Limits(
                max_connections=resolved.http_max_connections,
                max_keepalive_connections=20,
            ),
        )
        scheduling = SchedulingRuntime(
            client,
            resolved.instances[0].base_url,
            resolved.admission,
            frontier,
            recorder,
            retention=RetentionController(
                client,
                resolved.instances[0].base_url,
                resolved.retention,
                frontier,
                projection_calculator,
                recorder,
                api_key=resolved.upstream_control_api_key,
                cost_model=resolved.admission.cost_model,
            )
            if resolved.retention.enabled
            else None,
            api_key=resolved.upstream_control_api_key,
        )
        await scheduling.start()
        app.state.scheduling = scheduling
        if tool_duration_adapter is not None:
            tool_duration_adapter.bind(app)
        app.state.llm_gateway = LLMGateway(
            client,
            router,
            frontier,
            recorder,
            ingress_api_key=resolved.ingress_api_key,
            require_ingress_auth=resolved.require_ingress_auth,
            identity_validator=(dcs.authorize_llm_request if dcs is not None else None),
            forecast_manager=forecast_manager if forecast_active else None,
            resolution_store=resolution_store,
            tool_catalog_version=resolved.tool_catalog_version,
            forecast_top_n=resolved.forecast_top_n,
            reuse=reuse,
            dcs=dcs,
            on_reuse_resolution=_record_reuse_resolution,
            scheduling=scheduling,
            tool_duration_adapter=tool_duration_adapter,
        )
        maintenance_task = None
        try:
            # Load weights before serving or rebuilding persisted vectors;
            # cold initialization is not part of the five-second query budget.
            if native_embedder is not None:
                await native_embedder.embed(["FlowPilot semantic readiness"])
            maintenance_task = asyncio.create_task(maintain_reuse()) if reuse else None
            yield
        finally:
            await scheduling.close()
            await app.state.llm_gateway.close()
            if tool_duration_adapter is not None:
                await tool_duration_adapter.close()
            if maintenance_task is not None:
                maintenance_task.cancel()
                await asyncio.gather(maintenance_task, return_exceptions=True)
            if reuse is not None:
                await reuse.close()
            await forecast_manager.close()
            await resolution_store.close()
            if shared_state is not None:
                await shared_state.close()
            if owns_client:
                await client.aclose()

    app = FastAPI(title="FlowPilot", lifespan=lifespan)
    app.state.frontier = frontier
    app.state.recorder = recorder
    app.state.reuse = reuse
    app.state.dcs = dcs
    app.state.forecast_manager = forecast_manager
    app.state.tool_resolutions = resolution_store
    app.state.tool_duration_adapter = tool_duration_adapter
    app.state.projection_calculator = projection_calculator
    app.state.tool_analysis = tool_analysis
    app.state.shared_state = shared_state

    async def _mark_frontier_terminal(job_id: str, line_id: str, reason: str) -> None:
        try:
            await frontier.mark_terminal(job_id, line_id, reason)
        except FrontierConflict:
            # DCS remains durably diverged even if the process-local frontier
            # has already been lost or was never registered in this worker.
            await recorder.increment("context_terminal_mark_failures")

    @app.get("/flowpilot/health")
    async def health(request: Request) -> JSONResponse:
        gateway: LLMGateway = request.app.state.llm_gateway
        upstreams = await gateway.health()
        trace_healthy = await request.app.state.recorder.healthy()
        ready = any(upstreams.values()) and trace_healthy
        return JSONResponse(
            {
                "status": "ok" if ready else "degraded",
                "protocol_version": "flowpilot-phase0-v2",
                "scheduling_protocol_version": "flowpilot-phase4-scheduling-v2",
                "llm_instances": upstreams,
                "http_pool": {
                    "owner": "flowpilot" if owns_client else "injected",
                    "max_connections": resolved.http_max_connections
                    if owns_client
                    else None,
                },
                "state_backend": (
                    "sqlite-shared-contract+process-local-frontier-production-blocked"
                    if shared_state is not None
                    else "process-local-frontier+sqlite-dcs-wal"
                    if dcs is not None
                    else "process-local-single-worker"
                ),
                "tool_execution": "local-agent-only",
                "reuse_enabled": reuse is not None,
                "phase4_forecast": "enabled" if forecast_active else "disabled:m0",
                "tool_duration_prior": (
                    "synthetic_factual_family_v1"
                    if resolved.synthetic_tool_duration_enabled
                    else "disabled"
                ),
                "tool_analysis": "uncalibrated:deterministic",
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
                "kv_telemetry": (
                    request.app.state.scheduling.retention.status
                    if request.app.state.scheduling.retention
                    else "unsupported"
                ),
                "admission": (
                    resolved.admission.policy
                    if resolved.admission.enabled
                    else "disabled"
                ),
                "context_sync": "phase2-dcs-v2" if dcs is not None else "disabled",
                "restart_resume": (
                    "frontier-and-no-pending-dcs-only"
                    if dcs is not None
                    else "unsupported"
                ),
                "trace": {
                    "status": "ok" if trace_healthy else "degraded",
                    "rotation": "local-size-bounded-v1",
                    "restart_continuity": "append-only-current-file",
                },
            },
            status_code=200 if ready else 503,
        )

    @app.get("/flowpilot/metrics")
    async def metrics(request: Request) -> JSONResponse:
        recorder: TraceRecorder = request.app.state.recorder
        return JSONResponse(await recorder.snapshot())

    @app.get("/flowpilot/v1/scheduling/state")
    async def scheduling_state(request: Request) -> dict[str, Any]:
        return await request.app.state.scheduling.snapshot()

    @app.middleware("http")
    async def authenticate_control(request: Request, call_next: Any) -> Response:
        if request.url.path.startswith("/flowpilot/v1/"):
            try:
                _authorize_control(request, resolved)
            except HTTPException as exc:
                return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        return await call_next(request)

    @app.get("/flowpilot/v1/forecast")
    async def forecast_snapshot(request: Request) -> dict[str, Any]:
        _authorize_control(request, resolved)
        return await forecast_manager.snapshot()

    @app.post("/flowpilot/v1/forecast/{request_id}/cancel", status_code=202)
    async def cancel_forecast(
        request_id: str,
        request: Request,
        job_id: str,
        line_id: str,
    ) -> dict[str, str]:
        await forecast_manager.cancel(
            request_id,
            job_id=job_id,
            line_id=line_id,
        )
        return {"status": "accepted", "request_id": request_id}

    @app.get("/flowpilot/v1/tool-resolutions")
    async def tool_resolution_snapshot(request: Request) -> dict[str, Any]:
        _authorize_control(request, resolved)
        return {"records": await resolution_store.snapshot()}

    @app.get("/flowpilot/v1/tool-analysis")
    async def tool_analysis_snapshot(request: Request) -> dict[str, Any]:
        _authorize_control(request, resolved)
        return {
            "calibration_status": "uncalibrated",
            "profiles": [
                {
                    "tool_family": item.tool_family,
                    "intrinsic_cost_ms": item.intrinsic_cost_ms,
                    "effective_cost_ms": item.effective_cost_ms,
                    "remaining_cost_ms": item.remaining_cost_ms,
                    "duration_profile_ms": item.duration_profile_ms,
                    "output_profile_bytes": item.output_profile_bytes,
                    "tool_share": item.tool_share,
                    "heavy": item.heavy,
                    "sample_count": item.sample_count,
                }
                for item in tool_analysis.snapshot()
            ],
        }

    @app.post("/flowpilot/v1/tool-resolutions")
    async def update_tool_resolution(
        payload: ToolResolutionRecord, request: Request
    ) -> dict[str, Any]:
        try:
            line = await frontier.line_snapshot(payload.job_id, payload.line_id)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if (
            line.get("tail_request_id") != payload.tail_request_id
            or line.get("llm_call_id") != payload.llm_call_id
            or payload.tool_call_id
            not in {item["tool_call_id"] for item in line.get("tool_calls", [])}
        ):
            raise HTTPException(
                status_code=409, detail="resolution is not current tail"
            )
        try:
            record = await resolution_store.update(payload)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_resolution_update",
            identity={
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "tail_request_id": payload.tail_request_id,
                "tool_call_id": payload.tool_call_id,
            },
            fields={
                "resolution": payload.resolution.value,
                "status": payload.status.value,
                "source": payload.source.value,
                "ready_at_estimate": (
                    payload.ready_at_estimate.isoformat()
                    if payload.ready_at_estimate
                    else None
                ),
                "actual_latency_ms": payload.actual_latency_ms,
                "actual_result_bytes": payload.actual_result_bytes,
                "version": payload.version,
            },
        )
        return record.model_dump(mode="json")

    @app.get("/flowpilot/v1/scheduling/projections/{line_id}")
    async def scheduling_projection(
        line_id: str,
        request: Request,
        job_id: str,
        continuation_cost_ms: float = 0.0,
    ) -> dict[str, Any]:
        if {"estimated_inference_ms", "downstream_depth"} & request.query_params.keys():
            raise HTTPException(
                status_code=422,
                detail=(
                    "Readiness projection migration: estimated_inference_ms "
                    "and downstream_depth are removed"
                ),
            )
        try:
            projection = await projection_calculator.for_line(
                job_id,
                line_id,
                continuation_cost_ms=continuation_cost_ms,
            )
        except FrontierConflict as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return projection.model_dump(mode="json")

    @app.get("/flowpilot/v1/gateway-calls")
    async def gateway_calls(request: Request) -> JSONResponse:
        _authorize_control(request, resolved)
        return JSONResponse(
            {"calls": await request.app.state.llm_gateway.gateway_calls()}
        )

    @app.post("/flowpilot/v1/routing/load", status_code=202)
    async def update_instance_load(
        payload: InstanceLoadEvent, request: Request
    ) -> dict[str, str]:
        _authorize_control(request, resolved)
        try:
            await router.update_load(
                InstanceLoadProfile(
                    instance_id=payload.instance_id,
                    queue_depth=payload.queue_depth,
                    running_requests=payload.running_requests,
                    ttft_ms=payload.ttft_ms,
                    throughput_tokens_per_second=(payload.throughput_tokens_per_second),
                    updated_at=payload.observed_at,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await recorder.emit(
            "instance_load",
            identity={"instance_id": payload.instance_id},
            fields={
                "queue_depth": payload.queue_depth,
                "running_requests": payload.running_requests,
                "ttft_ms": payload.ttft_ms,
                "throughput_tokens_per_second": (payload.throughput_tokens_per_second),
                "routing_policy": resolved.routing_policy,
            },
        )
        return {"status": "accepted"}

    @app.get("/metrics")
    async def prometheus_metrics(request: Request) -> PlainTextResponse:
        recorder: TraceRecorder = request.app.state.recorder
        return PlainTextResponse(await recorder.prometheus())

    @app.post("/flowpilot/v1/jobs", status_code=201)
    async def register_job(
        payload: JobRegistration, request: Request
    ) -> dict[str, Any]:
        try:
            job = await request.app.state.frontier.register_job(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.recorder.emit(
            "job_submit",
            identity={
                "job_id": payload.job_id,
                "root_conversation_id": payload.root_conversation_id,
            },
            fields={"default_slo_ms": payload.default_slo_ms},
        )
        return {
            "job_id": job.job_id,
            "default_slo_ms": job.default_slo_ms,
        }

    @app.post("/flowpilot/v1/lines", status_code=201)
    async def register_line(
        payload: LineRegistration, request: Request
    ) -> dict[str, Any]:
        try:
            tail = await request.app.state.frontier.register_line(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.recorder.emit(
            "line_register",
            identity={
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "conversation_id": payload.conversation_id,
                "parent_conversation_id": payload.parent_conversation_id,
                "parent_line_id": payload.parent_line_id,
                "spawn_id": payload.spawn_id,
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
        if payload.line_id != line_id:
            raise HTTPException(status_code=400, detail="line_id does not match path")
        try:
            await request.app.state.frontier.replace_dependencies(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await request.app.state.scheduling.dependencies_changed(payload.job_id)
        await request.app.state.recorder.emit(
            "line_dependencies",
            identity={
                "job_id": payload.job_id,
                "line_id": line_id,
            },
            fields={
                "version": payload.version,
                "prerequisite_line_ids": list(payload.prerequisite_line_ids),
            },
        )
        (
            dependency_version,
            prerequisites,
        ) = await request.app.state.frontier.dependency_snapshot(
            payload.job_id, payload.line_id
        )
        return {
            "line_id": line_id,
            "dependency_version": dependency_version,
            "prerequisite_line_ids": list(prerequisites),
        }

    @app.post("/flowpilot/v1/lines/{line_id}/finish")
    async def finish_line(
        line_id: str,
        payload: LineFinish,
        request: Request,
    ) -> dict[str, Any]:
        if payload.line_id != line_id:
            raise HTTPException(status_code=400, detail="line_id does not match path")
        try:
            tail, released = await request.app.state.frontier.finish_line(payload)
        except FrontierConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if request.app.state.scheduling.retention is not None:
            request.app.state.scheduling.retention.line_finished(
                payload.job_id, line_id, payload.expected_tail_version
            )
        await request.app.state.scheduling.dependencies_changed(payload.job_id)
        await request.app.state.recorder.emit(
            "line_finish",
            identity={
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
            "state": tail.phase,
            "released_line_ids": list(released),
        }

    @app.get("/flowpilot/v1/jobs/{job_id}/frontier")
    async def frontier_snapshot(
        job_id: str,
        request: Request,
    ) -> dict[str, Any]:
        try:
            return await request.app.state.frontier.snapshot(job_id)
        except FrontierConflict as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/flowpilot/v1/predictor/feedback")
    async def predictor_feedback(payload: dict[str, Any]) -> dict[str, Any]:
        if tool_duration_adapter is None:
            raise HTTPException(503, "tool duration predictor disabled")
        try:
            return await tool_duration_adapter.feedback(payload)
        except (ValueError, KeyError, TypeError) as exc:
            raise HTTPException(422, "invalid predictor feedback") from exc

    @app.get("/flowpilot/v1/predictor")
    async def predictor_status() -> dict[str, Any]:
        return (
            tool_duration_adapter.snapshot()
            if tool_duration_adapter
            else {"enabled": False}
        )

    @app.post("/flowpilot/v1/events/tools", status_code=202)
    async def tool_event(
        payload: ToolTelemetryEvent,
        request: Request,
    ) -> dict[str, str]:
        try:
            if payload.binding_id is not None:
                _tail, duplicate = await _require_reuse(request).record_execution(
                    payload
                )
            else:
                _tail, duplicate = await request.app.state.frontier.record_tool_event(
                    payload
                )
        except (FrontierConflict, ReuseConflict) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if duplicate:
            await request.app.state.recorder.increment("tool_duplicate_events")
            return {"status": "duplicate"}
        resolution_status = {
            "start": ToolResolutionStatus.RESOLVING,
            "blocked": ToolResolutionStatus.WAITING,
            "finish": ToolResolutionStatus.READY,
            "fail": ToolResolutionStatus.FAILED,
            "cancel": ToolResolutionStatus.CANCELLED,
        }[payload.event_kind.value]
        resolution_kind = (
            ToolResolutionKind.LOCAL_LEADER
            if payload.tool_class.value == "web"
            else ToolResolutionKind.LOCAL_ONLY
        )
        try:
            duration_record = await resolution_store.resolve_reuse(
                identity=payload,
                tool_family=payload.tool_name,
                resolution=resolution_kind,
                status=resolution_status,
                source=ToolResolutionSource.LOCAL_MODEL,
                ready_at_estimate=(
                    payload.observed_at
                    if resolution_status == ToolResolutionStatus.READY
                    else None
                ),
                actual_latency_ms=payload.measured_latency_ms,
                execution_started_at=payload.observed_at
                if payload.event_kind.value == "start"
                else None,
                actual_result_bytes=payload.result_size_bytes,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await _notify_duration_resolution(duration_record)
        analysis = None
        if request.app.state.scheduling.retention is not None:
            # A real local START also settles reuse for SDK/streaming paths.
            request.app.state.scheduling.retention.tool_resolved(payload)
        if (
            resolution_status == ToolResolutionStatus.READY
            and payload.measured_latency_ms is not None
            and payload.result_size_bytes is not None
        ):
            analysis = tool_analysis.observe(
                ToolObservation(
                    tool_family=payload.tool_name,
                    intrinsic_cost_ms=payload.measured_latency_ms,
                    effective_cost_ms=payload.measured_latency_ms,
                    remaining_cost_ms=0.0,
                    duration_ms=payload.measured_latency_ms,
                    output_bytes=payload.result_size_bytes,
                ),
                inference_cost_ms=0.0,
            )
        await request.app.state.recorder.emit(
            f"tool_{payload.event_kind.value}",
            identity={
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
                "profile_calibration_status": (
                    analysis.calibration_status.value if analysis else None
                ),
                "tool_share": analysis.tool_share if analysis else None,
                "heavy": analysis.heavy if analysis else None,
            },
        )
        return {"status": "accepted"}

    @app.post("/flowpilot/v1/reuse/resolve")
    async def resolve_tool(
        payload: ToolReuseResolveRequest, request: Request
    ) -> dict[str, Any]:
        controller = _require_reuse(request)
        await _require_reuse_tail(request, payload.identity)
        try:
            decision = await controller.resolve(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await _record_reuse_resolution(payload.identity, payload.tool_name, decision)
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

    @app.post("/flowpilot/v1/reuse/bindings/{binding_id}/progress", status_code=202)
    async def report_binding_progress(
        binding_id: str, payload: LeaderProgressReport, request: Request
    ) -> dict[str, str]:
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
        try:
            duplicate = await _require_reuse(request).report_false_reuse(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_false_reuse",
            identity={
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
        try:
            result = await _require_reuse(request).update_semantic_policy(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await recorder.emit(
            "tool_reuse_semantic_policy",
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
        if payload.binding_id != binding_id:
            raise HTTPException(
                status_code=400, detail="binding_id does not match path"
            )
        controller = _require_reuse(request)
        try:
            decision = await controller.publish(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if decision.publication is None:
            raise HTTPException(
                status_code=500, detail="published result lacks commit receipt"
            )
        await recorder.emit(
            "tool_reuse_leader_finish",
            identity=payload.identity.model_dump(mode="json"),
            fields={
                "binding_id": binding_id,
                "cacheable": payload.cacheable,
                "result_size_bytes": decision.publication["result_size"],
            },
        )
        return decision.model_dump(mode="json", exclude_none=True)

    @app.get("/flowpilot/v1/reuse/bindings/{binding_id}")
    async def poll_binding(
        binding_id: str,
        request: Request,
        job_id: str,
        line_id: str,
        tail_request_id: str,
        llm_call_id: str,
        action_id: str,
        tool_call_id: str,
    ) -> dict[str, Any]:
        identity = ToolReuseIdentity(
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
        if payload.binding_id != binding_id:
            raise HTTPException(
                status_code=400, detail="binding_id does not match path"
            )
        await _require_reuse_tail(request, payload.identity)
        try:
            await _require_reuse(request).cancel_follower(payload)
        except ReuseConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"status": "accepted"}

    @app.get("/flowpilot/v1/reuse")
    async def reuse_snapshot(request: Request) -> dict[str, Any]:
        _authorize_control(request, resolved)
        return await _require_reuse(request).snapshot()

    @app.post("/flowpilot/v1/reuse/maintenance", status_code=200)
    async def reuse_maintenance(request: Request) -> dict[str, int]:
        _authorize_control(request, resolved)
        try:
            result = await _require_reuse(request).maintenance()
        except ReuseConflict as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await recorder.emit("tool_reuse_maintenance", fields=result)
        return result

    @app.post("/flowpilot/v1/dcs/delegations", status_code=201)
    async def grant_delegation(
        payload: DelegationPolicy, request: Request
    ) -> dict[str, Any]:
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
        await _require_reuse_tail(request, payload.reuse.identity)
        manager = _require_dcs(request)
        try:
            await manager.authorize_reuse(payload.delegation, payload.reuse.tool_name)
            decision = await _require_reuse(request).resolve(
                payload.reuse, defer_allowed=True, exact_only=True
            )
            await _record_reuse_resolution(
                payload.reuse.identity, payload.reuse.tool_name, decision
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
        await _require_reuse_tail(request, payload.reuse.identity)
        try:
            manager = _require_dcs(request)
            await manager.validate_reference(payload.delegation)
            decision = await _require_reuse(request).poll_deferred(
                payload.binding_id, payload.reuse, exact_only=True
            )
            await _record_reuse_resolution(
                payload.reuse.identity, payload.reuse.tool_name, decision
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
        try:
            return await _require_dcs(request).next_sync_chunk(payload)
        except DCSConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/flowpilot/v1/dcs/sync/ack")
    async def acknowledge_context_sync(
        payload: ContextSyncAck, request: Request
    ) -> dict[str, Any]:
        try:
            result = await _require_dcs(request).acknowledge(payload)
        except DCSConflict as exc:
            await _mark_frontier_terminal(
                payload.reference.job_id,
                payload.reference.line_id,
                "context_sync_conflict",
            )
            await recorder.emit(
                "context_sync_fail",
                identity=_dcs_identity(payload.reference),
                fields={
                    "error_class": "ContextDiverged",
                    "terminal_reason": "context_sync_conflict",
                },
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
        result = await _require_dcs(request).reconcile(payload)
        if result.get("status") == "context_diverged":
            await _mark_frontier_terminal(
                payload.job_id,
                payload.line_id,
                "context_reconcile_conflict",
            )
        reconcile_fields: dict[str, str] = {"status": result["status"]}
        if result.get("status") == "context_diverged":
            reconcile_fields["terminal_reason"] = "context_reconcile_conflict"
        await recorder.emit(
            "context_reconcile",
            identity={
                "job_id": payload.job_id,
                "line_id": payload.line_id,
                "context_epoch": str(payload.context_epoch),
            },
            fields=reconcile_fields,
        )
        return result

    @app.post("/flowpilot/v1/dcs/continuations")
    async def prepare_internal_continuation(
        payload: InternalContinuationRequest, request: Request
    ) -> dict[str, Any]:
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
        body = await request.body()
        proxy = gateway.proxy(
            path=path,
            api_kind=api_kind,
            body=body,
            headers=request.headers,
            raw_query=request.scope.get("query_string", b""),
        )
        if request.app.state.scheduling.queue is None:
            return await proxy

        async def disconnected() -> None:
            while (await request.receive())["type"] != "http.disconnect":
                pass

        pending = asyncio.create_task(proxy)
        watcher = asyncio.create_task(disconnected())
        delivered = False
        try:
            done, _ = await asyncio.wait(
                (pending, watcher), return_when=asyncio.FIRST_COMPLETED
            )
            if pending in done and watcher not in done:
                delivered = True
                return pending.result()
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            return Response(status_code=499)
        finally:
            watcher.cancel()
            if not pending.done():
                pending.cancel()
            await asyncio.gather(watcher, pending, return_exceptions=True)
            if (
                not delivered
                and not pending.cancelled()
                and pending.exception() is None
            ):
                iterator = getattr(pending.result(), "body_iterator", None)
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    await close()
    except GatewayAuthenticationError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except GatewayUpstreamError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _authorize_control(request: Request, settings: Settings) -> None:
    if not settings.require_ingress_auth:
        return
    supplied = request.headers.get("x-flowpilot-api-key")
    if len(request.headers.getlist("x-flowpilot-api-key")) != 1:
        raise HTTPException(
            status_code=401, detail="FlowPilot ingress authentication rejected"
        )
    if (
        not supplied
        or not settings.ingress_api_key
        or not hmac.compare_digest(
            supplied.encode("utf-8"), settings.ingress_api_key.encode("utf-8")
        )
    ):
        raise HTTPException(
            status_code=401, detail="FlowPilot ingress authentication rejected"
        )


def _require_reuse(request: Request) -> ReuseService:
    controller: ReuseService | None = request.app.state.reuse
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
        line = await request.app.state.frontier.line_snapshot(
            policy.job_id, policy.line_id
        )
    except FrontierConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if (
        line["context_epoch"] != policy.context_epoch
        or line["base_context_cursor"] != policy.base_context_cursor
        or line["context_digest"] != policy.base_context_digest
    ):
        raise HTTPException(
            status_code=409, detail="delegation base does not match authoritative line"
        )


def _dcs_identity(reference: DCSReference) -> dict[str, str]:
    return {
        "job_id": reference.job_id,
        "line_id": reference.line_id,
        "context_epoch": str(reference.context_epoch),
        "lease_id": reference.lease_id,
    }


async def _require_reuse_tail(request: Request, identity: ToolReuseIdentity) -> None:
    try:
        line = await request.app.state.frontier.line_snapshot(
            identity.job_id, identity.line_id
        )
    except FrontierConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if (
        line["tail_request_id"] != identity.tail_request_id
        or line["llm_call_id"] != identity.llm_call_id
        or line["phase"] not in {LinePhase.BLOCKED, LinePhase.READY}
    ):
        raise HTTPException(status_code=409, detail="reuse identity is not active tail")
