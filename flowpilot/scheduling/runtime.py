from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.gateway.call_state import GatewayCallPhase, GatewayCallRecord
from flowpilot.observability.trace import TraceRecorder
from flowpilot.protocol import RequestIdentity
from flowpilot.scheduling.admission import (
    AdmissionConfig,
    AdmissionQueue,
    priority_from_snapshot,
)
from flowpilot.scheduling.retention import RetentionController

logger = logging.getLogger(__name__)


class SchedulingRuntime:
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        config: AdmissionConfig,
        frontier: LineTailFrontier,
        recorder: TraceRecorder,
        *,
        retention: RetentionController | None = None,
        api_key: str | None = None,
    ) -> None:
        self.client = client
        self.root_url = base_url.removesuffix("/v1")
        self.queue = AdmissionQueue(config) if config.enabled else None
        self.config = config
        self.frontier = frontier
        self.recorder = recorder
        self.retention = retention
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self.retention:
            await self.retention.negotiate()
            self._tasks.append(asyncio.create_task(self.retention.run()))
        if self.queue:
            await self._heartbeat()
            self._tasks.append(asyncio.create_task(self._heartbeats()))

    async def _heartbeat(self) -> None:
        assert self.queue is not None
        try:
            response = await self.client.get(
                self.root_url + "/health",
                headers=self.headers,
                timeout=self.config.probe_timeout_seconds,
            )
            response.raise_for_status()
            healthy = True
        except httpx.HTTPError:
            healthy = False
            await self.recorder.increment("admission_heartbeat_failures")
        await self.queue.heartbeat(healthy)

    async def _heartbeats(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            await self._heartbeat()

    async def _prompt_tokens(self, payload: dict[str, Any]) -> int | None:
        # Token IDs are exact; no prompt-byte/token or KV-byte/token conversion.
        prompt = payload.get("prompt")
        if isinstance(prompt, list) and all(type(x) is int for x in prompt):
            return len(prompt)
        if "messages" not in payload and not isinstance(prompt, str):
            await self.recorder.increment("admission_prompt_work_unknown")
            return None
        supported = {
            "model",
            "messages",
            "prompt",
            "tools",
            "add_generation_prompt",
            "continue_final_message",
            "add_special_tokens",
            "chat_template",
            "chat_template_kwargs",
            "media_io_kwargs",
            "mm_processor_kwargs",
        }
        try:
            response = await self.client.post(
                self.root_url + "/tokenize",
                headers=self.headers,
                json={k: v for k, v in payload.items() if k in supported},
                timeout=self.config.probe_timeout_seconds,
            )
            response.raise_for_status()
            count = response.json()["count"]
            if type(count) is not int or count < 0:
                raise ValueError("invalid tokenizer count")
            return count
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            await self.recorder.increment("admission_prompt_work_unknown")
            logger.warning("Prompt work unavailable: %s", type(exc).__name__)
            return None

    async def admit(
        self,
        identity: RequestIdentity,
        call: GatewayCallRecord,
        snapshot: dict[str, Any],
        payload: dict[str, Any],
        tail_version: int,
    ) -> None:
        if self.queue is None:
            return
        projection = await self.queue.acquire(
            priority_from_snapshot(
                key=(identity.job_id, identity.llm_call_id),
                snapshot=snapshot,
                arrived_at=call.gateway_received_at,
                prompt_tokens=await self._prompt_tokens(payload),
            )
        )
        try:
            current = await self.frontier.line_snapshot(
                identity.job_id, identity.line_id
            )
            if (
                current["version"] != tail_version
                or current["tail_request_id"] != identity.tail_request_id
                or current["llm_call_id"] != identity.llm_call_id
                or current["phase"] != "ACTIVE"
                or current["dependencies"]
            ):
                raise FrontierConflict("queued request no longer matches ready tail")
            await self.recorder.emit(
                "request_admitted",
                identity={
                    "job_id": identity.job_id,
                    "line_id": identity.line_id,
                    "request_id": identity.request_id,
                    "llm_call_id": identity.llm_call_id,
                },
                fields=projection,
            )
        except BaseException:
            await self.queue.release((identity.job_id, identity.llm_call_id))
            raise

    async def terminal(self, call: GatewayCallRecord) -> None:
        if self.queue:
            await self.queue.release((call.job_id, call.llm_call_id))
        if self.retention and call.phase != GatewayCallPhase.COMPLETED:
            self.retention.forget(call.job_id, call.llm_call_id)

    async def dependencies_changed(self, job_id: str) -> None:
        if self.queue is not None:
            job = await self.frontier.snapshot(job_id)
            await self.queue.refresh_dependencies(job_id, job["lines"])

    async def snapshot(self) -> dict[str, Any]:
        return {
            "admission": await self.queue.snapshot()
            if self.queue
            else {"enabled": False},
            "kv": self.retention.snapshot()
            if self.retention
            else {"status": "unsupported"},
        }

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.queue:
            await self.queue.close()
        if self.retention:
            await self.retention.close()
