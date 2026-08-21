from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

import httpx
from fastapi import HTTPException
from starlette.background import BackgroundTask
from starlette.datastructures import Headers
from starlette.responses import Response, StreamingResponse

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.gateway.router import InferenceRouter, NoCompatibleInstance
from flowpilot.gateway.stream import (
    CompletionAccumulator,
    CompletionMetadata,
    ObservedStream,
)
from flowpilot.observability.trace import TraceRecorder
from flowpilot.protocol import RequestIdentity


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
        tenant_api_keys: tuple[tuple[str, str], ...] = (),
        identity_validator: Callable[[RequestIdentity], Awaitable[None]] | None = None,
    ) -> None:
        self._client = client
        self._router = router
        self._frontier = frontier
        self._recorder = recorder
        self._ingress_api_key = ingress_api_key
        self._tenant_api_keys = tenant_api_keys
        self._require_ingress_auth = require_ingress_auth
        self._identity_validator = identity_validator

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
        authenticated_tenant = self._authenticate(header_values)
        identity = identity_from_headers(header_values)
        if (
            authenticated_tenant is not None
            and identity.tenant_id != authenticated_tenant
        ):
            raise GatewayAuthenticationError(
                "FlowPilot API key is not authorized for this tenant"
            )
        payload = _parse_json_object(body)
        model = _model_from_payload(payload, header_values)
        stream = payload.get("stream") is True
        started_ms = time.monotonic() * 1000
        body_digest = hashlib.sha256(body).hexdigest()

        try:
            if self._identity_validator is not None:
                await self._identity_validator(identity)
            tail = await self._frontier.begin_request(identity, model)
        except (FrontierConflict, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        identity_fields = _identity_fields(identity)
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

        response: httpx.Response | None = None
        try:
            instance, response = await self._send_with_failover(
                path=path,
                body=body,
                headers=header_values,
                raw_query=raw_query,
                model=model,
            )
        except asyncio.CancelledError:
            await self._cancel_before_response(identity, identity_fields, started_ms)
            raise
        except (httpx.HTTPError, GatewayUpstreamError) as exc:
            authoritative_version = await self._frontier.abort_request(
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
            raise GatewayUpstreamError(
                "all compatible inference instances failed"
            ) from exc

        try:
            await self._frontier.mark_routed(identity, instance.instance_id)
            await self._recorder.emit(
                "llm_routed",
                identity=identity_fields,
                fields={"instance_id": instance.instance_id, "model": model},
            )
        except asyncio.CancelledError:
            await response.aclose()
            await self._cancel_before_response(identity, identity_fields, started_ms)
            raise
        response_headers = _response_headers(response.headers)

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
                ),
                on_cancel=lambda: self._cancel_stream(
                    identity, identity_fields, started_ms, response.status_code
                ),
                on_error=lambda exc: self._fail_stream(
                    identity,
                    identity_fields,
                    started_ms,
                    response.status_code,
                    exc,
                ),
                close_source=response.aclose,
                started_ms=started_ms,
            )
            result = ClosingStreamingResponse(
                observer,
                status_code=response.status_code,
                background=BackgroundTask(response.aclose),
            )
            _append_flowpilot_headers(
                response_headers, identity, tail.version, instance.instance_id
            )
            result.raw_headers = response_headers
            return result

        try:
            content = await response.aread()
        except asyncio.CancelledError:
            await response.aclose()
            await self._cancel_stream(
                identity, identity_fields, started_ms, response.status_code
            )
            raise
        except (httpx.HTTPError, httpx.RemoteProtocolError) as exc:
            await response.aclose()
            await self._fail_stream(
                identity, identity_fields, started_ms, response.status_code, exc
            )
            raise GatewayUpstreamError("upstream response read failed") from exc
        await response.aclose()
        metadata = _metadata_from_body(api_kind, content)
        if response.status_code >= 400:
            authoritative_version = await self._frontier.abort_request(
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
        elif metadata.protocol_error:
            authoritative_version = await self._frontier.abort_request(
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
        else:
            await self._complete_response(
                identity,
                identity_fields,
                metadata,
                started_ms,
                response.status_code,
            )
            authoritative_version = tail.version
        _append_flowpilot_headers(
            response_headers,
            identity,
            authoritative_version,
            instance.instance_id,
        )
        result = Response(content=content, status_code=response.status_code)
        result.raw_headers = response_headers
        return result

    async def _send_with_failover(
        self,
        *,
        path: str,
        body: bytes,
        headers: Mapping[str, list[str]],
        raw_query: bytes,
        model: str,
    ) -> tuple[Any, httpx.Response]:
        try:
            candidates = await self._router.candidates(model)
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
                response = await self._client.send(request, stream=True)
            except httpx.HTTPError as exc:
                last_error = exc
                await self._recorder.emit(
                    "llm_route_attempt_failed",
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
    ) -> None:
        if status_code >= 400:
            version = await self._frontier.abort_request(
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
            return
        if metadata.protocol_error:
            version = await self._frontier.abort_request(
                identity, metadata.protocol_error
            )
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
            return
        await self._complete_response(
            identity, identity_fields, metadata, started_ms, status_code
        )

    async def _cancel_stream(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        started_ms: float,
        status_code: int,
    ) -> None:
        version = await self._frontier.abort_request(identity, "client_cancelled")
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

    async def _cancel_before_response(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        started_ms: float,
    ) -> None:
        version = await self._frontier.abort_request(identity, "client_cancelled")
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

    async def _fail_stream(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        started_ms: float,
        status_code: int,
        exc: Exception,
    ) -> None:
        version = await self._frontier.abort_request(
            identity, f"stream_{type(exc).__name__}"
        )
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

    async def _complete_response(
        self,
        identity: RequestIdentity,
        identity_fields: dict[str, str],
        metadata: CompletionMetadata,
        started_ms: float,
        status_code: int,
    ) -> None:
        completed = await self._frontier.complete_response(
            identity,
            response_id=metadata.response_id,
            tool_calls=metadata.tool_calls,
            error=(
                metadata.protocol_error
                or (f"upstream_http_{status_code}" if status_code >= 400 else None)
            ),
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
                "tail_updated": completed,
                "protocol_error": metadata.protocol_error,
            },
        )

    def _authenticate(self, headers: Mapping[str, list[str]]) -> str | None:
        supplied = _first_header(headers, _API_KEY_HEADER)
        if not self._require_ingress_auth:
            return None
        if supplied:
            for key, tenant_id in self._tenant_api_keys:
                if hmac.compare_digest(supplied, key):
                    return tenant_id
        if (
            not self._ingress_api_key
            or not supplied
            or not hmac.compare_digest(supplied, self._ingress_api_key)
        ):
            raise GatewayAuthenticationError(
                "FlowPilot ingress authentication rejected"
            )
        return None


def identity_from_headers(headers: Mapping[str, list[str]]) -> RequestIdentity:
    values = {
        "protocol_version": _first_header(headers, "x-flowpilot-protocol-version")
        or "flowpilot-phase0-v1",
        "tenant_id": _required_header(headers, "x-flowpilot-tenant-id"),
        "job_id": _required_header(headers, "x-flowpilot-job-id"),
        "line_id": _required_header(headers, "x-flowpilot-line-id"),
        "tail_request_id": _required_header(headers, "x-flowpilot-tail-request-id"),
        "llm_call_id": _required_header(headers, "x-flowpilot-llm-call-id"),
        "expected_tail_version": _required_header(headers, "x-flowpilot-tail-version"),
        "context_epoch": _required_header(headers, "x-flowpilot-context-epoch"),
        "context_sequence": _required_header(headers, "x-flowpilot-context-sequence"),
        "base_context_cursor": _required_header(headers, "x-flowpilot-context-cursor"),
        "context_digest": _required_header(headers, "x-flowpilot-context-digest"),
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
    return {
        "tenant_id": identity.tenant_id,
        "job_id": identity.job_id,
        "line_id": identity.line_id,
        "tail_request_id": identity.tail_request_id,
        "llm_call_id": identity.llm_call_id,
        "context_epoch": str(identity.context_epoch),
        "context_sequence": str(identity.context_sequence),
        "base_context_cursor": identity.base_context_cursor,
        "context_digest": identity.context_digest,
    }


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
    authoritative_version: int,
    instance_id: str,
) -> None:
    headers.extend(
        [
            (b"x-flowpilot-protocol-version", identity.protocol_version.encode()),
            (b"x-flowpilot-llm-call-id", identity.llm_call_id.encode()),
            (b"x-flowpilot-tail-version", str(authoritative_version).encode()),
            (b"x-flowpilot-instance-id", instance_id.encode()),
        ]
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
    metadata = accumulator.finalize()
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
