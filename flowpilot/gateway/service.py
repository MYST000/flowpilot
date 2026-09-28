from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

import httpx
from fastapi import HTTPException
from starlette.background import BackgroundTask
from starlette.datastructures import Headers
from starlette.responses import Response, StreamingResponse

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.gateway.call_state import (
    GatewayCallConflict,
    GatewayCallPhase,
    GatewayCallRecord,
    GatewayCallStore,
)
from flowpilot.gateway.router import (
    InferenceRouter,
    NoCompatibleInstance,
    RoutingRequest,
)
from flowpilot.gateway.stream import (
    CompletionAccumulator,
    CompletionMetadata,
    ObservedStream,
)
from flowpilot.observability.trace import TraceRecorder
from flowpilot.protocol import (
    ContextDeltaAppend,
    DCSReference,
    DelegationPolicy,
    ForecastRequest,
    InternalContinuationRequest,
    RequestIdentity,
    ReuseDecisionKind,
    ToolReuseDecision,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
)
from flowpilot.reuse.contracts import provider_reuse_content


class GatewayAuthenticationError(ValueError):
    pass


class GatewayUpstreamError(RuntimeError):
    pass


class ClosingStreamingResponse(StreamingResponse):
    async def stream_response(self, send: Any) -> None:
        try:
            await super().stream_response(send)
        finally:
            close = getattr(self.body_iterator, "aclose", None)
            if close is not None:
                await close()


_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}
_REQUEST_MANAGED_HEADERS = {"content-length"}
_RESPONSE_SERVER_HEADERS = {"date", "server"}
_PRIVATE_PREFIX = "x-flowpilot-"
_API_KEY_HEADER = "x-flowpilot-api-key"


class LLMGateway:
    """Transparent OpenAI-compatible proxy with phase 0 correlation tracing."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        router: InferenceRouter,
        frontier: LineTailFrontier,
        recorder: TraceRecorder,
        *,
        ingress_api_key: str | None,
        require_ingress_auth: bool,
        identity_validator: Callable[[RequestIdentity], Awaitable[None]] | None = None,
        call_store: GatewayCallStore | None = None,
        forecast_manager: Any | None = None,
        resolution_store: Any | None = None,
        reuse: Any | None = None,
        dcs: Any | None = None,
        on_reuse_resolution: Callable[..., Awaitable[None]] | None = None,
        tool_catalog_version: str = "default-v1",
        forecast_top_n: int = 3,
        scheduling: Any | None = None,
    ) -> None:
        self._client = client
        self._router = router
        self._frontier = frontier
        self._recorder = recorder
        self._ingress_api_key = ingress_api_key
        self._require_ingress_auth = require_ingress_auth
        self._identity_validator = identity_validator
        self._scheduling = scheduling
        self._call_store = call_store or GatewayCallStore(
            on_terminal=scheduling.terminal if scheduling else None
        )
        self._forecast_manager = forecast_manager
        self._resolution_store = resolution_store
        self._reuse = reuse
        self._dcs = dcs
        self._on_reuse_resolution = on_reuse_resolution
        self._tool_catalog_version = tool_catalog_version
        self._forecast_top_n = forecast_top_n

    async def gateway_calls(self) -> list[dict[str, Any]]:
        return await self._call_store.snapshot()

    async def health(self) -> dict[str, bool]:
        return await self._router.health(self._client)

    async def proxy(
        self,
        *,
        path: str,
        api_kind: str,
        body: bytes,
        headers: Iterable[tuple[str, str]] | Mapping[str, str] | Headers,
        raw_query: bytes,
    ) -> Response:
        header_values = _header_values(headers)
        gateway_policy = _gateway_reuse_policy(header_values)
        if gateway_policy is not None:
            if self._reuse is None:
                raise HTTPException(status_code=503, detail="reuse is disabled")
            if _first_header(header_values, "x-flowpilot-request-origin") == (
                "scheduler_delegated"
            ):
                raise HTTPException(
                    status_code=400, detail="delegated origin is internal"
                )
            if gateway_policy.get("api_kind", api_kind) != api_kind:
                raise HTTPException(status_code=400, detail="api kind mismatch")
            if payload := _parse_json_object(body):
                gateway_policy["request_snapshot"] = payload
                gateway_policy["stream"] = payload.get("stream") is True
            if gateway_policy.get("deferred") and self._dcs is None:
                raise HTTPException(status_code=503, detail="DCS is disabled")
        result = await self._proxy_once(
            path=path,
            api_kind=api_kind,
            body=body,
            headers=headers,
            raw_query=raw_query,
            gateway_policy=gateway_policy,
        )
        if gateway_policy is None or gateway_policy.get("stream"):
            return result
        return await self._drive_gateway_reuse(
            result,
            path=path,
            api_kind=api_kind,
            request_body=body,
            request_headers=header_values,
            raw_query=raw_query,
            policy=gateway_policy,
        )

    async def _drive_gateway_reuse(
        self,
        response: Response,
        *,
        path: str,
        api_kind: str,
        request_body: bytes,
        request_headers: Mapping[str, list[str]],
        raw_query: bytes,
        policy: dict[str, Any],
    ) -> Response:
        """Resolve complete Tool Calls after the provider response.

        The normal OpenAI response remains the boundary for local Tool
        execution.  Only a fully reusable, deferred batch is continued inside
        FlowPilot; all other decisions are attached as control metadata for
        the OpenHands adapter to consume without issuing a resolve request.
        """
        if response.status_code >= 400:
            return response
        assert self._reuse is not None
        content = bytes(response.body or b"")
        payload = _parse_json_object(content)
        calls = _provider_tool_calls(payload, api_kind)
        if not calls:
            return _with_gateway_metadata(
                content,
                response,
                {"decisions": [], "policy_version": int(policy["policy_version"])},
            )
        identity = identity_from_headers(request_headers)
        scope = _reuse_scope(policy)
        decisions: list[dict[str, Any]] = []
        reference: DCSReference | None = None
        if policy.get("deferred"):
            reference = await self._ensure_gateway_delegation(
                identity, api_kind, _parse_json_object(request_body), policy
            )
        for call in calls:
            reuse_identity = ToolReuseIdentity(
                job_id=identity.job_id,
                line_id=identity.line_id,
                tail_request_id=identity.tail_request_id,
                llm_call_id=identity.llm_call_id,
                action_id=None,
                tool_call_id=call["id"],
            )
            request = ToolReuseResolveRequest(
                protocol_version=(
                    "flowpilot-phase1-reuse-v3"
                    if policy.get("deferred")
                    else policy.get(
                        "reuse_protocol_version", "flowpilot-phase1-reuse-v3"
                    )
                ),
                identity=reuse_identity,
                tool_name=call["name"],
                arguments=call["arguments"],
                scope=scope,
                output_budget_bytes=policy.get("output_budget_bytes"),
                input_schema_digest=policy.get("tool_schema_digests", {}).get(
                    call["name"]
                ),
            )
            decision = (
                await self._reuse.resolve(
                    request,
                    defer_allowed=bool(policy.get("deferred")),
                    exact_only=bool(policy.get("deferred")),
                )
                if call["name"] in policy["allowed_tool_names"]
                else ToolReuseDecision(decision=ReuseDecisionKind.EXECUTE_LOCALLY)
            )
            if self._on_reuse_resolution is not None:
                await self._on_reuse_resolution(
                    reuse_identity,
                    call["name"],
                    decision,
                )
            decisions.append(
                {
                    "tool_call_id": call["id"],
                    "tool_name": call["name"],
                    "arguments_digest": hashlib.sha256(
                        json.dumps(
                            call["arguments"], sort_keys=True, separators=(",", ":")
                        ).encode()
                    ).hexdigest(),
                    "identity": reuse_identity.model_dump(mode="json"),
                    **decision.model_dump(mode="json", exclude_none=True),
                }
            )
        all_deferred = bool(reference) and all(
            item["decision"] == ReuseDecisionKind.DEFER_WITH_CACHED_RESULT.value
            for item in decisions
        )
        if not all_deferred:
            if reference is not None:
                assert self._dcs is not None
                await self._dcs.release(reference)
            return _with_gateway_metadata(
                content,
                response,
                {
                    "decisions": decisions,
                    "policy_version": int(policy["policy_version"]),
                },
            )

        assert reference is not None
        assert self._dcs is not None
        current = response
        batches: list[dict[str, Any]] = []
        parent_llm_call_id = identity.llm_call_id
        current_identity = identity

        async def barrier() -> Response:
            assert self._dcs is not None and reference is not None
            metadata: dict[str, Any] = {
                "decisions": [item for item in decisions if "identity" in item],
                "policy_version": int(policy["policy_version"]),
            }
            if batches:
                metadata.update(
                    batches=batches,
                    final_identity=current_identity.model_dump(mode="json"),
                    dcs_reference=reference.model_dump(mode="json"),
                    delta_seq=await self._dcs_last_seq(reference),
                )
            else:
                await self._dcs.release(reference)
            return _with_gateway_metadata(bytes(current.body or b""), current, metadata)

        for _ in range(int(policy["max_internal_continuations"])):
            current_payload = _parse_json_object(bytes(current.body or b""))
            current_calls = _provider_tool_calls(current_payload, api_kind)
            if any("result" not in item for item in decisions):
                refreshed: list[dict[str, Any]] = []
                for call in current_calls:
                    request = ToolReuseResolveRequest(
                        identity=ToolReuseIdentity(
                            job_id=current_identity.job_id,
                            line_id=current_identity.line_id,
                            tail_request_id=current_identity.tail_request_id,
                            llm_call_id=current_identity.llm_call_id,
                            tool_call_id=call["id"],
                        ),
                        tool_name=call["name"],
                        arguments=call["arguments"],
                        scope=scope,
                        output_budget_bytes=policy.get("output_budget_bytes"),
                        input_schema_digest=policy.get("tool_schema_digests", {}).get(
                            call["name"]
                        ),
                    )
                    decision = await self._reuse.resolve(
                        request, defer_allowed=True, exact_only=True
                    )
                    for _poll in range(120):
                        if (
                            decision.decision
                            != ReuseDecisionKind.DEFER_WAIT_FOR_INFLIGHT
                        ):
                            break
                        await asyncio.sleep(0.05)
                        decision = await self._reuse.poll_deferred(
                            decision.binding_id or "", request, exact_only=True
                        )
                    if decision.decision != ReuseDecisionKind.DEFER_WITH_CACHED_RESULT:
                        refreshed.append(
                            {
                                "identity": request.identity.model_dump(mode="json"),
                                "tool_call_id": call["id"],
                                "tool_name": call["name"],
                                **decision.model_dump(mode="json", exclude_none=True),
                            }
                        )
                        decisions = refreshed
                        return await barrier()
                    refreshed.append(
                        {
                            "tool_call_id": call["id"],
                            "tool_name": call["name"],
                            "identity": request.identity.model_dump(mode="json"),
                            **decision.model_dump(mode="json", exclude_none=True),
                        }
                    )
                decisions = refreshed
            receipts: list[str] = []
            result_digests: list[str] = []
            for item, call in zip(decisions, current_calls, strict=True):
                request = ToolReuseResolveRequest(
                    identity=ToolReuseIdentity.model_validate(item["identity"]),
                    tool_name=call["name"],
                    arguments=call["arguments"],
                    scope=scope,
                    output_budget_bytes=policy.get("output_budget_bytes"),
                    input_schema_digest=policy.get("tool_schema_digests", {}).get(
                        call["name"]
                    ),
                )
                decision = await self._reuse.resolve(
                    request, defer_allowed=True, exact_only=True
                )
                item.update(decision.model_dump(mode="json", exclude_none=True))
                if decision.decision != ReuseDecisionKind.DEFER_WITH_CACHED_RESULT:
                    return await barrier()
                receipt = await self._dcs.issue_resolution(reference, request, decision)
                receipts.append(receipt["resolution_receipt"])
                result_digests.append(receipt["result_digest"])
                item.update(receipt)
            current_messages = _provider_reuse_messages(
                current_payload, current_calls, decisions, api_kind
            )
            appended = await self._dcs.append(
                ContextDeltaAppend(
                    reference=reference,
                    expected_last_seq=await self._dcs_last_seq(reference),
                    parent_llm_call_id=parent_llm_call_id,
                    messages=tuple(current_messages),
                    tool_call_ids=tuple(call["id"] for call in current_calls),
                    resolution_receipts=tuple(receipts),
                    result_digests=tuple(result_digests),
                )
            )
            reference = DCSReference.model_validate(
                {
                    **reference.model_dump(mode="json"),
                    "delta_digest": appended["delta_digest"],
                }
            )
            batches.append(
                {
                    "response": current_payload,
                    "decisions": decisions,
                    "parent_llm_call_id": parent_llm_call_id,
                }
            )
            continuation = await self._dcs.prepare_continuation(
                InternalContinuationRequest(
                    reference=reference,
                    parent_llm_call_id=parent_llm_call_id,
                )
            )
            next_identity = _delegated_identity(
                current_identity, reference, continuation
            )
            current = await self._proxy_once(
                path=path,
                api_kind=api_kind,
                body=json.dumps(continuation["body"], separators=(",", ":")).encode(),
                headers=_identity_headers(next_identity, request_headers),
                raw_query=raw_query,
            )
            parent_llm_call_id = next_identity.llm_call_id
            current_identity = next_identity
            next_payload = _parse_json_object(bytes(current.body or b""))
            next_calls = _provider_tool_calls(next_payload, api_kind)
            if not next_calls:
                return _with_gateway_metadata(
                    bytes(current.body or b""),
                    current,
                    {
                        "batches": batches,
                        "final_identity": next_identity.model_dump(mode="json"),
                        "dcs_reference": reference.model_dump(mode="json"),
                        "delta_seq": continuation["delta_seq"],
                        "policy_version": int(policy["policy_version"]),
                    },
                )
            decisions = []
            for call in next_calls:
                decisions.append(
                    {
                        "tool_call_id": call["id"],
                        "tool_name": call["name"],
                        "arguments": call["arguments"],
                    }
                )
            if not all(
                call["name"] in policy["allowed_tool_names"] for call in next_calls
            ):
                break
        return await barrier()

    async def _ensure_gateway_delegation(
        self,
        identity: RequestIdentity,
        api_kind: str,
        request_snapshot: dict[str, Any],
        policy: dict[str, Any],
    ) -> DCSReference:
        assert self._dcs is not None
        issued = datetime.now().astimezone()
        lease_id = str(policy["lease_id"])
        delegation = DelegationPolicy.model_validate(
            {
                "policy_version": int(policy["policy_version"]),
                "expected_policy_version": int(policy["expected_policy_version"]),
                "lease_id": lease_id,
                "job_id": identity.job_id,
                "line_id": identity.line_id,
                "context_epoch": identity.context_epoch,
                "base_context_cursor": identity.base_context_cursor,
                "base_context_digest": identity.context_digest,
                "issued_at": issued,
                "expires_at": issued
                + timedelta(seconds=float(policy["lease_seconds"])),
                "allowed_tool_names": tuple(policy["allowed_tool_names"]),
                "max_messages": int(policy["max_messages"]),
                "max_bytes": int(policy["max_bytes"]),
                "max_internal_continuations": int(policy["max_internal_continuations"]),
                "delta_ttl_seconds": float(policy["delta_ttl_seconds"]),
                "api_kind": api_kind,
                "request_snapshot": request_snapshot,
            }
        )
        result = await self._dcs.grant(delegation)
        return DCSReference(
            job_id=identity.job_id,
            line_id=identity.line_id,
            context_epoch=identity.context_epoch,
            lease_id=lease_id,
            base_context_cursor=identity.base_context_cursor,
            delta_digest=str(result["delta_digest"]),
        )

    async def _dcs_last_seq(self, reference: DCSReference) -> int:
        assert self._dcs is not None
        snapshot = await self._dcs.snapshot()
        for item in snapshot.get("lines", []):
            if (
                item.get("job_id") == reference.job_id
                and item.get("line_id") == reference.line_id
            ):
                return int(item["last_seq"])
        raise GatewayUpstreamError("DCS line disappeared during reuse")

    async def _proxy_once(
        self,
        *,
        path: str,
        api_kind: str,
        body: bytes,
        headers: Iterable[tuple[str, str]] | Mapping[str, str] | Headers,
        raw_query: bytes,
        gateway_policy: dict[str, Any] | None = None,
    ) -> Response:
        header_values = _header_values(headers)
        self._authenticate(header_values)
        identity = identity_from_headers(header_values)
        payload = _parse_json_object(body)
        if self._scheduling is not None and self._scheduling.retention is not None:
            transfer = payload.get("kv_transfer_params")
            if transfer is not None and (
                not isinstance(transfer, dict) or "kv_control_binding" in transfer
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "kv_transfer_params must leave kv_control_binding to FlowPilot"
                    ),
                )
        model = _model_from_payload(payload, header_values)
        stream = payload.get("stream") is True
        started_ms = time.monotonic() * 1000
        body_digest = hashlib.sha256(body).hexdigest()

        try:
            call = await self._call_store.start(
                identity, stream=stream, api_kind=api_kind
            )
        except GatewayCallConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        try:
            if self._identity_validator is not None:
                await self._identity_validator(identity)
            tail = await self._frontier.begin_request(identity, model)
        except (FrontierConflict, ValueError) as exc:
            await self._call_store.terminal(
                call,
                GatewayCallPhase.PROTOCOL_ERROR,
                authoritative_tail_version=None,
                status_code=409,
                reason=str(exc),
            )
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        identity_fields = _identity_fields(identity)
        try:
            await self._recorder.emit(
                "llm_request",
                identity=identity_fields,
                fields={
                    "api_kind": api_kind,
                    "path": path,
                    "model": model,
                    "stream": stream,
                    "request_bytes": len(body),
                    "request_digest": body_digest,
                    "expected_tail_version": identity.expected_tail_version,
                    "tail_version": tail.version,
                },
            )
        except asyncio.CancelledError:
            await self._cancel_before_response(
                identity, identity_fields, started_ms, call
            )
            raise

        deadline = None
        line_snapshot: dict[str, Any] = {}
        try:
            line_snapshot = await self._frontier.line_snapshot(
                identity.job_id, identity.line_id
            )
            raw_deadline = line_snapshot.get("deadline")
            if isinstance(raw_deadline, str):
                deadline = datetime.fromisoformat(raw_deadline)
        except (FrontierConflict, ValueError):
            pass
        routing_request = RoutingRequest(
            job_id=identity.job_id,
            line_id=identity.line_id,
            request_weight=float(line_snapshot.get("weight", 1.0)),
            deadline=deadline,
            blocking_line_count=int(line_snapshot.get("blocking_line_count", 0)),
        )

        if self._forecast_manager is not None:
            forecast_request = ForecastRequest(
                request_id=identity.request_id,
                tail_request_id=identity.tail_request_id,
                job_id=identity.job_id,
                line_id=identity.line_id,
                model_id=model,
                history_features_ref=f"tail:{identity.tail_request_id}",
                tool_catalog_version=self._tool_catalog_version,
                deadline=deadline,
                requested_top_n=self._forecast_top_n,
            )
            # Forecast is an optional side channel.  Start it after the
            # request is accepted and never await it on the inference path.
            await self._forecast_manager.start(forecast_request)

        response: httpx.Response | None = None
        try:
            if self._scheduling is not None:
                await self._scheduling.admit(
                    identity,
                    call,
                    line_snapshot,
                    payload,
                    tail.version,
                    api_kind=api_kind,
                )
                if self._scheduling.retention is not None:
                    body = self._scheduling.retention.bind(body, identity)
            instance, response = await self._send_with_failover(
                path=path,
                body=body,
                headers=header_values,
                raw_query=raw_query,
                model=model,
                routing_request=routing_request,
                call=call,
            )
        except asyncio.CancelledError:
            await self._cancel_before_response(
                identity, identity_fields, started_ms, call
            )
            raise
        except Exception as exc:
            await self._cancel_forecast(identity)
            authoritative_version = await self._abort_request(
                identity, type(exc).__name__
            )
            await self._recorder.emit(
                "llm_upstream_failed",
                identity=identity_fields,
                fields={
                    "error_class": type(exc).__name__,
                    "authoritative_tail_version": authoritative_version,
                },
            )
            await self._call_store.terminal(
                call,
                GatewayCallPhase.UPSTREAM_FAILED,
                authoritative_tail_version=authoritative_version,
                status_code=None,
                reason=type(exc).__name__,
            )
            raise GatewayUpstreamError(
                "all compatible inference instances failed"
            ) from exc

        try:
            await self._frontier.mark_routed(identity, instance.instance_id)
            await self._call_store.routed(call, instance.instance_id)
            await self._recorder.emit(
                "llm_routed",
                identity=identity_fields,
                fields={"instance_id": instance.instance_id, "model": model},
            )
        except asyncio.CancelledError:
            await self._close_upstream(response)
            await self._cancel_before_response(
                identity, identity_fields, started_ms, call
            )
            raise
        except Exception as exc:
            await self._close_upstream(response)
            version = await self._abort_request(
                identity, f"gateway_state_{type(exc).__name__}"
            )
            await self._call_store.terminal(
                call,
                GatewayCallPhase.PROTOCOL_ERROR,
                authoritative_tail_version=version,
                status_code=None,
                reason=f"gateway_state_{type(exc).__name__}",
            )
            raise
        response_headers = _response_headers(response.headers)
        await self._call_store.first_byte(call)

        if stream:
            observer = ObservedStream(
                response.aiter_raw(),
                api_kind=api_kind,
                on_complete=lambda metadata: self._complete_stream(
                    identity,
                    identity_fields,
                    metadata,
                    started_ms,
                    response.status_code,
                    call,
                ),
                on_cancel=lambda: self._cancel_stream(
                    identity,
                    identity_fields,
                    started_ms,
                    response.status_code,
                    call,
                ),
                on_error=lambda exc: self._fail_stream(
                    identity,
                    identity_fields,
                    started_ms,
                    response.status_code,
                    exc,
                    call,
                ),
                close_source=lambda: self._close_upstream(response),
                started_ms=started_ms,
            )
            result = ClosingStreamingResponse(
                observer,
                status_code=response.status_code,
                background=BackgroundTask(self._close_upstream, response),
            )
            _append_flowpilot_headers(
                response_headers, identity, tail.version, instance.instance_id
            )
            result.raw_headers = response_headers
            return result

        try:
            content = await response.aread()
        except asyncio.CancelledError:
            await self._close_upstream(response)
            await self._cancel_stream(
                identity,
                identity_fields,
                started_ms,
                response.status_code,
                call,
            )
            raise
        except Exception as exc:
            await self._close_upstream(response)
            await self._fail_stream(
                identity,
                identity_fields,
                started_ms,
                response.status_code,
                exc,
                call,
            )
            raise GatewayUpstreamError("upstream response read failed") from exc
        try:
            await self._close_upstream(response)
            metadata = _metadata_from_body(api_kind, content)
            if response.status_code >= 400:
                authoritative_version = await self._abort_request(
                    identity, f"upstream_http_{response.status_code}"
                )
                await self._recorder.emit(
                    "llm_provider_error",
                    identity=identity_fields,
                    fields={
                        "status_code": response.status_code,
                        "response_bytes": len(content),
                        "authoritative_tail_version": authoritative_version,
                    },
                )
                await self._call_store.terminal(
                    call,
                    GatewayCallPhase.PROVIDER_ERROR,
                    authoritative_tail_version=authoritative_version,
                    status_code=response.status_code,
                    reason=f"upstream_http_{response.status_code}",
                )
            elif metadata.protocol_error:
                authoritative_version = await self._abort_request(
                    identity, metadata.protocol_error
                )
                await self._recorder.emit(
                    "llm_response_failed",
                    identity=identity_fields,
                    fields={
                        "status_code": response.status_code,
                        "response_bytes": len(content),
                        "error_class": "UpstreamProtocolError",
                        "protocol_error": metadata.protocol_error,
                        "authoritative_tail_version": authoritative_version,
                    },
                )
                await self._call_store.terminal(
                    call,
                    GatewayCallPhase.PROTOCOL_ERROR,
                    authoritative_tail_version=authoritative_version,
                    status_code=response.status_code,
                    reason=metadata.protocol_error,
                )
            else:
                completed_version = await self._complete_response(
                    identity,
                    identity_fields,
                    metadata,
                    started_ms,
                    response.status_code,
                    call,
                )
                authoritative_version = (
                    completed_version
                    if completed_version is not None
                    else await self._authoritative_version(identity)
                )
            _append_flowpilot_headers(
                response_headers,
                identity,
                authoritative_version,
                instance.instance_id,
            )
            result = Response(content=content, status_code=response.status_code)
            result.raw_headers = response_headers
            return result
        except asyncio.CancelledError:
            await self._cancel_stream(
                identity, identity_fields, started_ms, response.status_code, call
            )
            raise
        except Exception as exc:
            await self._fail_stream(
                identity, identity_fields, started_ms, response.status_code, exc, call
            )
            raise

    async def _send_with_failover(
        self,
        *,
        path: str,
        body: bytes,
        headers: Mapping[str, list[str]],
        raw_query: bytes,
        model: str,
        routing_request: RoutingRequest,
        call: GatewayCallRecord,
    ) -> tuple[Any, httpx.Response]:
        try:
            candidates = await self._router.candidates(model, request=routing_request)
        except NoCompatibleInstance as exc:
            raise GatewayUpstreamError(str(exc)) from exc
        last_error: Exception | None = None
        for instance in candidates:
            suffix = (
                path.removeprefix("/v1") if instance.base_url.endswith("/v1") else path
            )
            query = raw_query.decode("latin-1")
            url = f"{instance.base_url}{suffix}"
            if query:
                url = f"{url}?{query}"
            request = self._client.build_request(
                "POST",
                url,
                content=body,
                headers=_forward_headers(headers),
            )
            try:
                await self._call_store.sent(call)
                response = await self._client.send(request, stream=True)
            except httpx.HTTPError as exc:
                last_error = exc
                await self._recorder.emit(
                    "llm_route_attempt_failed",
                    identity={
                        "job_id": call.job_id,
                        "line_id": call.line_id,
                        "request_id": call.request_id,
                        "llm_call_id": call.llm_call_id,
                        "attempt": str(call.attempt),
                    },
                    fields={
                        "instance_id": instance.instance_id,
                        "model": model,
                        "error_class": type(exc).__name__,
                    },
                )
                continue
            return instance, response
        if last_error is None:
            raise GatewayUpstreamError("no compatible inference instance")
        raise GatewayUpstreamError(
            "all inference route attempts failed"
        ) from last_error

    async def _complete_stream(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        metadata: CompletionMetadata,
        started_ms: float,
        status_code: int,
        call: GatewayCallRecord,
    ) -> None:
        if status_code >= 400:
            version = await self._abort_request(
                identity, f"upstream_http_{status_code}"
            )
            await self._recorder.emit(
                "llm_provider_error",
                identity=identity_fields,
                fields={
                    "status_code": status_code,
                    "response_bytes": metadata.response_bytes,
                    "authoritative_tail_version": version,
                },
            )
            await self._call_store.terminal(
                call,
                GatewayCallPhase.PROVIDER_ERROR,
                authoritative_tail_version=version,
                status_code=status_code,
                reason=f"upstream_http_{status_code}",
            )
            return
        if metadata.protocol_error:
            version = await self._abort_request(identity, metadata.protocol_error)
            await self._recorder.emit(
                "llm_stream_failed",
                identity=identity_fields,
                fields={
                    "status_code": status_code,
                    "latency_ms": _elapsed_ms(started_ms),
                    "error_class": "UpstreamProtocolError",
                    "protocol_error": metadata.protocol_error,
                    "authoritative_tail_version": version,
                },
            )
            await self._call_store.terminal(
                call,
                GatewayCallPhase.PROTOCOL_ERROR,
                authoritative_tail_version=version,
                status_code=status_code,
                reason=metadata.protocol_error,
            )
            return
        await self._complete_response(
            identity, identity_fields, metadata, started_ms, status_code, call
        )

    async def _cancel_stream(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        started_ms: float,
        status_code: int,
        call: GatewayCallRecord,
    ) -> None:
        if call.phase not in {GatewayCallPhase.ACTIVE, GatewayCallPhase.ROUTED}:
            return
        await self._cancel_forecast(identity)
        version = await self._abort_request(identity, "client_cancelled")
        await self._recorder.emit(
            "llm_cancelled",
            identity=identity_fields,
            fields={
                "status_code": status_code,
                "latency_ms": _elapsed_ms(started_ms),
                "client_cancelled": True,
                "authoritative_tail_version": version,
            },
        )
        await self._call_store.terminal(
            call,
            GatewayCallPhase.CANCELLED,
            authoritative_tail_version=version,
            status_code=status_code,
            reason="client_cancelled",
        )

    async def _cancel_before_response(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        started_ms: float,
        call: GatewayCallRecord,
    ) -> None:
        await self._cancel_forecast(identity)
        version = await self._abort_request(identity, "client_cancelled")
        await self._recorder.emit(
            "llm_cancelled",
            identity=identity_fields,
            fields={
                "status_code": None,
                "latency_ms": _elapsed_ms(started_ms),
                "client_cancelled": True,
                "authoritative_tail_version": version,
            },
        )
        await self._call_store.terminal(
            call,
            GatewayCallPhase.CANCELLED,
            authoritative_tail_version=version,
            status_code=None,
            reason="client_cancelled",
        )

    async def _fail_stream(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        started_ms: float,
        status_code: int,
        exc: Exception,
        call: GatewayCallRecord,
    ) -> None:
        if call.phase not in {GatewayCallPhase.ACTIVE, GatewayCallPhase.ROUTED}:
            return
        await self._cancel_forecast(identity)
        version = await self._abort_request(identity, f"stream_{type(exc).__name__}")
        await self._recorder.emit(
            "llm_stream_failed",
            identity=identity_fields,
            fields={
                "status_code": status_code,
                "latency_ms": _elapsed_ms(started_ms),
                "error_class": type(exc).__name__,
                "authoritative_tail_version": version,
            },
        )
        await self._call_store.terminal(
            call,
            GatewayCallPhase.STREAM_ERROR,
            authoritative_tail_version=version,
            status_code=status_code,
            reason=f"stream_{type(exc).__name__}",
        )

    async def _complete_response(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        metadata: CompletionMetadata,
        started_ms: float,
        status_code: int,
        call: GatewayCallRecord,
    ) -> int | None:
        if metadata.tool_calls:
            await self._supersede_forecast(identity)
            if self._resolution_store is not None:
                for item in metadata.tool_calls:
                    await self._resolution_store.observe_tool_call(
                        job_id=identity.job_id,
                        line_id=identity.line_id,
                        tail_request_id=identity.tail_request_id,
                        llm_call_id=identity.llm_call_id,
                        tool_call_id=item.tool_call_id,
                        tool_family=item.tool_name,
                    )
        completed_version = await self._frontier.complete_response(
            identity,
            response_id=metadata.response_id,
            tool_calls=metadata.tool_calls,
        )
        await self._recorder.emit(
            "llm_response",
            identity=identity_fields,
            fields={
                "status_code": status_code,
                "response_id": metadata.response_id,
                "tool_calls": [
                    {
                        "tool_call_id": item.tool_call_id,
                        "tool_name": item.tool_name,
                        "arguments_digest": item.arguments_digest,
                        "arguments_bytes": item.arguments_bytes,
                    }
                    for item in metadata.tool_calls
                ],
                "tool_call_count": len(metadata.tool_calls),
                "finish_reasons": metadata.finish_reasons,
                "usage": _usage_metadata(metadata.usage),
                "response_bytes": metadata.response_bytes,
                "stream_chunks": metadata.stream_chunks,
                "first_byte_ms": metadata.first_byte_ms,
                "latency_ms": _elapsed_ms(started_ms),
                "tail_updated": completed_version is not None,
                "protocol_error": metadata.protocol_error,
            },
        )
        authoritative_version = (
            completed_version
            if completed_version is not None
            else await self._safe_authoritative_version(identity)
        )
        if self._scheduling is not None and self._scheduling.retention is not None:
            self._scheduling.retention.finished(identity, authoritative_version)
        await self._call_store.terminal(
            call,
            GatewayCallPhase.COMPLETED,
            authoritative_tail_version=authoritative_version,
            status_code=status_code,
            reason=None,
        )
        return authoritative_version

    async def _cancel_forecast(self, identity: RequestIdentity) -> None:
        if self._forecast_manager is not None:
            await self._forecast_manager.cancel(
                identity.request_id,
                job_id=identity.job_id,
                line_id=identity.line_id,
            )

    async def _supersede_forecast(self, identity: RequestIdentity) -> None:
        if self._forecast_manager is not None:
            await self._forecast_manager.supersede(
                identity.request_id,
                job_id=identity.job_id,
                line_id=identity.line_id,
            )

    async def _authoritative_version(self, identity: RequestIdentity) -> int:
        snapshot = await self._frontier.snapshot(identity.job_id)
        line = next(
            item for item in snapshot["lines"] if item["line_id"] == identity.line_id
        )
        return int(line["version"])

    async def _safe_authoritative_version(
        self, identity: RequestIdentity
    ) -> int | None:
        try:
            return await self._authoritative_version(identity)
        except (FrontierConflict, StopIteration):
            return None

    async def _abort_request(
        self, identity: RequestIdentity, reason: str
    ) -> int | None:
        """Abort only this uncommitted replacement and tolerate stale callbacks.

        A stream can finish or be cancelled after the Agent has already
        submitted a newer request (or after a context-conflict terminal mark).
        In that case ``abort_request`` must not roll back the newer tail.  The
        call still needs an explicit GatewayCall terminal outcome, so expose
        the current authoritative version when it is available and otherwise
        return ``None`` without turning cleanup into a second failure.
        """
        try:
            return await self._frontier.abort_request(identity, reason)
        except FrontierConflict:
            try:
                return await self._authoritative_version(identity)
            except (FrontierConflict, StopIteration):
                return None

    async def _close_upstream(self, response: httpx.Response) -> None:
        try:
            await response.aclose()
        except Exception:
            await self._recorder.increment("upstream_close_failures")

    def _authenticate(self, headers: Mapping[str, list[str]]) -> None:
        supplied = _first_header(headers, _API_KEY_HEADER)
        if len(headers.get(_API_KEY_HEADER, [])) != 1:
            raise GatewayAuthenticationError(
                "FlowPilot ingress authentication rejected"
            )
        if not self._require_ingress_auth:
            return
        if (
            not self._ingress_api_key
            or not supplied
            or not hmac.compare_digest(
                supplied.encode("utf-8"), self._ingress_api_key.encode("utf-8")
            )
        ):
            raise GatewayAuthenticationError(
                "FlowPilot ingress authentication rejected"
            )
        return


def _gateway_reuse_policy(
    headers: Mapping[str, list[str]],
) -> dict[str, Any] | None:
    raw = _first_header(headers, "x-flowpilot-reuse-policy")
    if raw is None:
        return None
    try:
        policy = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        raise GatewayAuthenticationError("invalid FlowPilot reuse policy") from None
    if not isinstance(policy, dict):
        raise GatewayAuthenticationError("invalid FlowPilot reuse policy")
    required = {
        "allowed_tool_names",
        "deferred",
        "policy_version",
        "expected_policy_version",
        "lease_id",
        "lease_seconds",
        "max_messages",
        "max_bytes",
        "max_internal_continuations",
        "delta_ttl_seconds",
    }
    optional = {
        "reuse_protocol_version",
        "tool_schema_digests",
        "locale",
        "language",
        "region",
        "safe_search_policy",
        "time_sensitivity_class",
        "data_source_constraints",
        "output_budget_bytes",
        "api_kind",
        "issued_at",
    }
    if set(policy) - required - optional or not required <= set(policy):
        raise GatewayAuthenticationError("incomplete FlowPilot reuse policy")
    schemas = policy.get("tool_schema_digests", {})
    if not isinstance(schemas, dict) or any(
        not isinstance(name, str)
        or not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
        for name, value in schemas.items()
    ):
        raise GatewayAuthenticationError("invalid Tool schema digests")
    if policy.get("reuse_protocol_version", "flowpilot-phase1-reuse-v3") not in {
        "flowpilot-phase1-reuse-v3",
        "flowpilot-phase3-reuse-v3",
    }:
        raise GatewayAuthenticationError("unsupported reuse protocol")
    if (
        not isinstance(policy["allowed_tool_names"], list)
        or not policy["allowed_tool_names"]
        or not all(
            isinstance(item, str) and item for item in policy["allowed_tool_names"]
        )
        or not isinstance(policy["deferred"], bool)
    ):
        raise GatewayAuthenticationError("invalid FlowPilot reuse policy")
    if (
        not isinstance(policy["policy_version"], int)
        or policy["policy_version"] < 1
        or not isinstance(policy["expected_policy_version"], int)
        or policy["expected_policy_version"] < 0
        or not isinstance(policy["lease_id"], str)
        or not policy["lease_id"]
    ):
        raise GatewayAuthenticationError("invalid FlowPilot reuse policy")
    if "api_kind" in policy and policy["api_kind"] not in {"chat", "responses"}:
        raise GatewayAuthenticationError("invalid FlowPilot reuse policy")
    for name in (
        "lease_seconds",
        "max_messages",
        "max_bytes",
        "max_internal_continuations",
        "delta_ttl_seconds",
    ):
        value = policy[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise GatewayAuthenticationError("invalid FlowPilot reuse policy")
    return policy


def _reuse_scope(policy: Mapping[str, Any]) -> Any:
    from flowpilot.protocol import ReuseScope

    return ReuseScope(
        locale=str(policy.get("locale", "und")),
        language=str(policy.get("language", "und")),
        region=str(policy.get("region", "global")),
        safe_search_policy=str(policy.get("safe_search_policy", "default")),
        time_sensitivity_class=str(policy.get("time_sensitivity_class", "standard")),
        data_source_constraints=tuple(policy.get("data_source_constraints", ())),
    )


def _provider_tool_calls(
    payload: Mapping[str, Any], api_kind: str
) -> list[dict[str, Any]]:
    if api_kind == "chat":
        return _chat_tool_calls(payload)
    output = payload.get("output")
    if not isinstance(output, list):
        return []
    result: list[dict[str, Any]] = []
    for item in output:
        if (
            not isinstance(item, dict)
            or item.get("type") != "function_call"
            or not isinstance(item.get("call_id"), str)
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("arguments"), str)
        ):
            continue
        try:
            arguments = json.loads(item["arguments"])
        except json.JSONDecodeError:
            return []
        if not isinstance(arguments, dict):
            return []
        result.append(
            {
                "id": item["call_id"],
                "name": item["name"],
                "arguments": arguments,
            }
        )
    return result


def _chat_tool_calls(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        return []
    choice = choices[0]
    message = choice.get("message") if isinstance(choice, dict) else None
    calls = message.get("tool_calls") if isinstance(message, dict) else None
    if not isinstance(calls, list) or not calls:
        return []
    result: list[dict[str, Any]] = []
    for item in calls:
        function = item.get("function") if isinstance(item, dict) else None
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("id"), str)
            or not isinstance(function, dict)
            or not isinstance(function.get("name"), str)
            or not isinstance(function.get("arguments"), str)
        ):
            return []
        try:
            arguments = json.loads(function["arguments"])
        except json.JSONDecodeError:
            return []
        if not isinstance(arguments, dict):
            return []
        result.append(
            {"id": item["id"], "name": function["name"], "arguments": arguments}
        )
    return result


def _provider_reuse_messages(
    payload: Mapping[str, Any],
    calls: list[dict[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    api_kind: str,
) -> list[dict[str, Any]]:
    if api_kind == "chat":
        return _chat_reuse_messages(payload, calls, decisions)
    output = payload.get("output")
    if not isinstance(output, list):
        raise GatewayUpstreamError("provider Responses response is malformed")
    messages = [
        item
        for item in output
        if isinstance(item, dict) and item.get("type") == "function_call"
    ]
    if len(messages) != len(calls):
        raise GatewayUpstreamError("provider Responses Tool response is malformed")
    result: list[dict[str, Any]] = []
    for call, decision in zip(calls, decisions, strict=True):
        matching = next(
            (item for item in messages if item.get("call_id") == call["id"]), None
        )
        if matching is None:
            raise GatewayUpstreamError("Responses function call identity changed")
        reused = decision.get("result")
        provenance = decision.get("provenance")
        if not isinstance(reused, dict) or not isinstance(provenance, dict):
            raise GatewayUpstreamError("deferred reuse result is incomplete")
        output_value = provider_reuse_content(reused, provenance)
        result.extend(
            [
                {
                    "type": "function_call",
                    "call_id": call["id"],
                    "name": call["name"],
                    "arguments": matching["arguments"],
                },
                {
                    "type": "function_call_output",
                    "call_id": call["id"],
                    "output": output_value,
                },
            ]
        )
    return result


def _chat_reuse_messages(
    payload: Mapping[str, Any],
    calls: list[dict[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    choices = payload.get("choices")
    message = choices[0].get("message") if isinstance(choices, list) else None
    if not isinstance(message, dict):
        raise GatewayUpstreamError("provider Tool response is malformed")
    assistant = dict(message)
    if assistant.get("content") is None:
        assistant.pop("content", None)
    elif isinstance(assistant.get("content"), str):
        assistant["content"] = [{"type": "text", "text": assistant["content"]}]
    messages: list[dict[str, Any]] = [{"role": "assistant", **assistant}]
    for call, decision in zip(calls, decisions, strict=True):
        result = decision.get("result")
        provenance = decision.get("provenance")
        if not isinstance(result, dict) or not isinstance(provenance, dict):
            raise GatewayUpstreamError("deferred reuse result is incomplete")
        provider_content = provider_reuse_content(result, provenance)
        messages.append(
            {
                "role": "tool",
                "name": call["name"],
                "tool_call_id": call["id"],
                "content": [{"type": "text", "text": provider_content}],
            }
        )
    return messages


def _with_gateway_metadata(
    content: bytes, response: Response, metadata: dict[str, Any]
) -> Response:
    payload = _parse_json_object(content)
    payload["flowpilot"] = metadata
    result = Response(
        content=json.dumps(payload, separators=(",", ":")).encode(),
        status_code=response.status_code,
        media_type=response.media_type,
    )
    result.raw_headers = [
        (name, value)
        for name, value in response.raw_headers
        if name.lower()
        not in {b"content-length", b"content-encoding", b"etag", b"content-md5"}
    ] + [(b"content-length", str(len(result.body)).encode())]
    return result


def _delegated_identity(
    parent: RequestIdentity,
    reference: DCSReference,
    continuation: Mapping[str, Any],
) -> RequestIdentity:
    import uuid

    return RequestIdentity(
        job_id=parent.job_id,
        line_id=parent.line_id,
        request_id=str(uuid.uuid4()),
        tail_request_id=str(uuid.uuid4()),
        attempt=1,
        llm_call_id=str(uuid.uuid4()),
        expected_tail_version=parent.expected_tail_version + 1,
        context_epoch=reference.context_epoch,
        context_sequence=parent.context_sequence + int(continuation["delta_seq"]),
        base_context_cursor=reference.base_context_cursor,
        context_digest=reference.delta_digest,
        conversation_id=parent.conversation_id,
        parent_conversation_id=parent.parent_conversation_id,
        parent_line_id=parent.parent_line_id,
        spawn_id=parent.spawn_id,
        deployment_id=parent.deployment_id,
        namespace_id=parent.namespace_id,
        origin="scheduler_delegated",
        delegation_lease_id=reference.lease_id,
    )


def _identity_headers(
    identity: RequestIdentity, original: Mapping[str, list[str]]
) -> dict[str, str]:
    headers: dict[str, str] = {
        "x-flowpilot-api-key": _first_header(original, _API_KEY_HEADER) or "",
    }
    # Retain provider authentication and tenant headers for the internal
    # continuation. FlowPilot identity headers are rebuilt below so a stale
    # request cannot cross the DCS boundary, and the reuse policy is omitted
    # to prevent recursive Scheduler handling.
    for name, values in original.items():
        lower = name.lower()
        if lower.startswith("x-flowpilot-") or lower in {
            "content-length",
            "host",
        }:
            continue
        if values:
            headers[lower] = values[-1]
    values = {
        "x-flowpilot-protocol-version": identity.protocol_version,
        "x-flowpilot-job-id": identity.job_id,
        "x-flowpilot-line-id": identity.line_id,
        "x-flowpilot-request-id": identity.request_id,
        "x-flowpilot-tail-request-id": identity.tail_request_id,
        "x-flowpilot-request-attempt": str(identity.attempt),
        "x-flowpilot-llm-call-id": identity.llm_call_id,
        "x-flowpilot-tail-version": str(identity.expected_tail_version),
        "x-flowpilot-context-epoch": str(identity.context_epoch),
        "x-flowpilot-context-sequence": str(identity.context_sequence),
        "x-flowpilot-context-cursor": identity.base_context_cursor,
        "x-flowpilot-context-digest": identity.context_digest,
        "x-flowpilot-conversation-id": identity.conversation_id,
        "x-flowpilot-request-origin": identity.origin,
        "x-flowpilot-delegation-lease-id": identity.delegation_lease_id or "",
    }
    for name, value in values.items():
        if value:
            headers[name] = value
    for name in (
        "parent-conversation-id",
        "parent-line-id",
        "spawn-id",
        "deployment-id",
        "namespace-id",
    ):
        value = getattr(identity, name.replace("-", "_"))
        if value is not None:
            headers[f"x-flowpilot-{name}"] = value
    return headers


def identity_from_headers(headers: Mapping[str, list[str]]) -> RequestIdentity:
    for name, values in headers.items():
        if name.lower().startswith("x-flowpilot-") and name.lower() != _API_KEY_HEADER:
            if len(values) != 1:
                raise GatewayAuthenticationError(
                    f"FlowPilot identity header must appear exactly once: {name}"
                )
    values = {
        "protocol_version": _required_header(headers, "x-flowpilot-protocol-version"),
        "job_id": _required_header(headers, "x-flowpilot-job-id"),
        "line_id": _required_header(headers, "x-flowpilot-line-id"),
        "request_id": _required_header(headers, "x-flowpilot-request-id"),
        "tail_request_id": _required_header(headers, "x-flowpilot-tail-request-id"),
        "attempt": _required_header(headers, "x-flowpilot-request-attempt"),
        "llm_call_id": _required_header(headers, "x-flowpilot-llm-call-id"),
        "expected_tail_version": _required_header(headers, "x-flowpilot-tail-version"),
        "context_epoch": _required_header(headers, "x-flowpilot-context-epoch"),
        "context_sequence": _required_header(headers, "x-flowpilot-context-sequence"),
        "base_context_cursor": _required_header(headers, "x-flowpilot-context-cursor"),
        "context_digest": _required_header(headers, "x-flowpilot-context-digest"),
        "conversation_id": _required_header(headers, "x-flowpilot-conversation-id"),
        "parent_conversation_id": _first_header(
            headers, "x-flowpilot-parent-conversation-id"
        ),
        "parent_line_id": _first_header(headers, "x-flowpilot-parent-line-id"),
        "spawn_id": _first_header(headers, "x-flowpilot-spawn-id"),
        "deployment_id": _first_header(headers, "x-flowpilot-deployment-id"),
        "namespace_id": _first_header(headers, "x-flowpilot-namespace-id"),
        "origin": _first_header(headers, "x-flowpilot-request-origin") or "agent",
        "delegation_lease_id": _first_header(
            headers, "x-flowpilot-delegation-lease-id"
        ),
    }
    try:
        return RequestIdentity.model_validate(values)
    except ValueError as exc:
        raise GatewayAuthenticationError(
            f"invalid FlowPilot request identity: {exc}"
        ) from exc


def _header_values(
    headers: Iterable[tuple[str, str]] | Mapping[str, str] | Headers,
) -> dict[str, list[str]]:
    if isinstance(headers, Headers):
        items = headers.raw
        return _header_values(
            [(key.decode("latin-1"), value.decode("latin-1")) for key, value in items]
        )
    if isinstance(headers, Mapping):
        return {str(key).lower(): [str(value)] for key, value in headers.items()}
    result: dict[str, list[str]] = {}
    for key, value in headers:
        result.setdefault(key.lower(), []).append(value)
    return result


def _required_header(headers: Mapping[str, list[str]], name: str) -> str:
    value = _first_header(headers, name)
    if value is None or not value:
        raise GatewayAuthenticationError(f"missing required header {name}")
    return value


def _first_header(headers: Mapping[str, list[str]], name: str) -> str | None:
    values = headers.get(name.lower())
    return values[0] if values else None


def _identity_fields(identity: RequestIdentity) -> dict[str, str]:
    fields = {
        "job_id": identity.job_id,
        "line_id": identity.line_id,
        "request_id": identity.request_id,
        "tail_request_id": identity.tail_request_id,
        "attempt": str(identity.attempt),
        "llm_call_id": identity.llm_call_id,
        "context_epoch": str(identity.context_epoch),
        "context_sequence": str(identity.context_sequence),
        "base_context_cursor": identity.base_context_cursor,
        "context_digest": identity.context_digest,
    }
    for name in (
        "conversation_id",
        "parent_conversation_id",
        "parent_line_id",
        "spawn_id",
        "deployment_id",
        "namespace_id",
    ):
        value = getattr(identity, name)
        if value is not None:
            fields[name] = value
    return fields


def _forward_headers(headers: Mapping[str, list[str]]) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for key, values in headers.items():
        lower = key.lower()
        if (
            lower in _HOP_BY_HOP
            or lower in _REQUEST_MANAGED_HEADERS
            or lower == _API_KEY_HEADER
            or lower.startswith(_PRIVATE_PREFIX)
        ):
            continue
        result.extend((key, value) for value in values)
    return result


def _response_headers(headers: httpx.Headers) -> list[tuple[bytes, bytes]]:
    return [
        (key.encode("latin-1"), value.encode("latin-1"))
        for key, value in headers.multi_items()
        if key.lower() not in _HOP_BY_HOP
        and key.lower() not in _RESPONSE_SERVER_HEADERS
        and not key.lower().startswith(_PRIVATE_PREFIX)
    ]


def _append_flowpilot_headers(
    headers: list[tuple[bytes, bytes]],
    identity: RequestIdentity,
    authoritative_version: int | None,
    instance_id: str,
) -> None:
    headers.extend(
        [
            (b"x-flowpilot-protocol-version", identity.protocol_version.encode()),
            (b"x-flowpilot-job-id", identity.job_id.encode()),
            (b"x-flowpilot-line-id", identity.line_id.encode()),
            (b"x-flowpilot-request-id", identity.request_id.encode()),
            (b"x-flowpilot-request-attempt", str(identity.attempt).encode()),
            (b"x-flowpilot-llm-call-id", identity.llm_call_id.encode()),
            (b"x-flowpilot-instance-id", instance_id.encode()),
        ]
    )
    if identity.conversation_id is not None:
        headers.append(
            (b"x-flowpilot-conversation-id", identity.conversation_id.encode())
        )
    for header, value in (
        (b"x-flowpilot-parent-conversation-id", identity.parent_conversation_id),
        (b"x-flowpilot-parent-line-id", identity.parent_line_id),
        (b"x-flowpilot-spawn-id", identity.spawn_id),
        (b"x-flowpilot-deployment-id", identity.deployment_id),
        (b"x-flowpilot-namespace-id", identity.namespace_id),
    ):
        if value is not None:
            headers.append((header, value.encode()))
    if authoritative_version is not None:
        headers.append(
            (b"x-flowpilot-tail-version", str(authoritative_version).encode())
        )


def _parse_json_object(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _model_from_payload(
    payload: dict[str, Any], headers: Mapping[str, list[str]]
) -> str:
    model = payload.get("model")
    if isinstance(model, str) and model:
        return model
    return _first_header(headers, "x-flowpilot-model") or "unknown"


def _metadata_from_body(api_kind: str, content: bytes) -> CompletionMetadata:
    accumulator = CompletionAccumulator(api_kind)
    try:
        value = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError):
        value = None
    payload = value if isinstance(value, dict) else {}
    accumulator.feed_json(payload)
    # A non-streaming Responses object does not carry the SSE-only
    # ``finish_reason`` field.  Keep structural/tool validation, but do not
    # classify a valid JSON response as an incomplete stream.
    metadata = accumulator.finalize(require_finish_reason=False)
    if isinstance(value, dict):
        expected_field = "choices" if api_kind == "chat" else "output"
        if expected_field not in value:
            metadata.protocol_error = metadata.protocol_error or (
                f"missing_{expected_field}"
            )
    metadata.response_bytes = len(content)
    if not isinstance(value, dict):
        metadata.protocol_error = "invalid_response_json"
    return metadata


def _usage_metadata(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    safe: dict[str, Any] = {}
    for key in (
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "input_tokens",
        "output_tokens",
    ):
        item = value.get(key)
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            safe[key] = item
    return safe or None


def _elapsed_ms(started_ms: float) -> float:
    return round(time.monotonic() * 1000 - started_ms, 3)
