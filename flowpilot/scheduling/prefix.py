"""Full target-request sweeps, independent of descriptor retention."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from flowpilot.observability.trace import TraceRecorder
from flowpilot.scheduling.admission import AdmissionConfig, RequestPriority
from flowpilot.scheduling.cost import RequestWork, estimate_work

logger = logging.getLogger(__name__)


class TargetPrefixQueries:
    def __init__(
        self,
        client: httpx.AsyncClient,
        root_url: str,
        config: AdmissionConfig,
        recorder: TraceRecorder,
        headers: dict[str, str],
        owner_scope: str,
    ) -> None:
        self.client, self.root_url, self.config = client, root_url, config
        self.recorder, self.headers, self.owner_scope = recorder, headers, owner_scope
        self.payloads: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self.status = "unnegotiated"
        self.capability: dict[str, Any] | None = None

    async def refresh(
        self, requests: list[RequestPriority]
    ) -> dict[tuple[str, str], RequestWork]:
        if not requests:
            return {}
        try:
            response = await self.client.get(
                self.root_url + "/v1/kv/capabilities",
                headers=self.headers,
                timeout=self.config.probe_timeout_seconds,
            )
            response.raise_for_status()
            capability = response.json()
            if (
                not isinstance(capability, dict)
                or capability.get("schema_version") != 1
            ):
                raise ValueError("incompatible KV capability schema")
            engine = capability.get("engine")
            if (
                not isinstance(engine, dict)
                or not isinstance(engine.get("engine_epoch"), str)
                or not engine["engine_epoch"]
            ):
                raise ValueError("missing KV engine identity")
            if not isinstance(capability.get("target_prefix_query", False), bool):
                raise ValueError("invalid target prefix capability")
            if not isinstance(capability.get("prefill_cost_context", False), bool):
                raise ValueError("invalid prefill load capability")
            self.capability = capability
            self.status = (
                "supported" if capability.get("target_prefix_query") else "unsupported"
            )
        except (httpx.HTTPError, ValueError) as exc:
            self.capability = None
            self.status = (
                "unsupported"
                if isinstance(exc, httpx.HTTPStatusError)
                and exc.response.status_code in {404, 501}
                else "unavailable"
            )
            await self.recorder.increment("target_prefix_capability_failures")
            logger.warning(
                "Target prefix capabilities unavailable: %s", type(exc).__name__
            )
        works = await asyncio.gather(*(self._query(request) for request in requests))
        await self.recorder.increment("target_prefix_full_sweeps")
        return dict(zip((r.key for r in requests), works, strict=True))

    def _cold(self, request: RequestPriority, reason: str) -> RequestWork:
        p = request.prompt_tokens
        model = self.config.cost_model
        engine = (self.capability or {}).get("engine", {})
        cost = (
            model.prefill_seconds(p, 0)
            if model is not None
            and p is not None
            and engine.get("identity_digest") == model.engine_identity_digest
            else None
        )
        return RequestWork(
            prompt_tokens=p,
            prefill_tokens=p,
            gpu_cost_seconds=cost,
            cost_seconds=cost,
            prefix_basis="COLD:" + reason,
            cost_basis="calibrated:cold" if cost is not None else "unknown:" + reason,
            calibration_source=model.source if cost is not None and model else None,
            calibration_version=model.version if cost is not None and model else None,
        )

    async def _query(self, request: RequestPriority) -> RequestWork:
        if self.status != "supported" or self.capability is None:
            return self._cold(request, self.status)
        target = self.payloads.get(request.key)
        if target is None:
            return self._cold(request, "cancelled")
        api_kind, payload = target
        epoch = self.capability["engine"]["engine_epoch"]
        query_id = request.key[1]
        started = time.monotonic()
        try:
            response = await self.client.post(
                self.root_url + "/v1/kv/query-target",
                headers=self.headers,
                timeout=self.config.probe_timeout_seconds,
                json={
                    "schema_version": 1,
                    "owner_scope": self.owner_scope,
                    "expected_engine_epoch": epoch,
                    "query_id": query_id,
                    "api_kind": api_kind,
                    "payload": payload,
                },
            )
            response.raise_for_status()
            result = response.json()
            if not isinstance(result, dict) or (
                result.get("schema_version") != 1
                or result.get("query_id") != query_id
                or result.get("engine_epoch") != epoch
            ):
                raise ValueError("target prefix query identity mismatch")
            inputs = result["inputs"]
            if (
                not isinstance(inputs, list)
                or not inputs
                or any(not isinstance(o, dict) for o in inputs)
            ):
                raise ValueError("invalid target prefix inputs")
            if any(
                o["query_id"] != query_id or o["engine_epoch"] != epoch for o in inputs
            ):
                raise ValueError("target input observation identity mismatch")
            expected_layout = self.capability["engine"].get("identity_digest")
            if expected_layout is not None and any(
                o.get("engine_identity_digest") != expected_layout for o in inputs
            ):
                raise ValueError("target input layout mismatch")
            if not self.capability.get("prefill_cost_context", False):
                inputs = [{**o, "prefill_load": None} for o in inputs]
            work = estimate_work(inputs, self.config.cost_model, observed_at=started)
            await self.recorder.increment("target_prefix_queries")
            return work
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            await self.recorder.increment("target_prefix_query_failures")
            logger.warning("Target prefix lookup failed: %s", type(exc).__name__)
            return self._cold(request, "query_failed:" + type(exc).__name__)
