from __future__ import annotations

from typing import Any

from flowpilot.frontier.store import FrontierConflict, LineTailFrontier
from flowpilot.protocol import (
    BindingFailureReport,
    FollowerCancellation,
    LeaderProgressReport,
    LeaderResultPublish,
    ToolReuseIdentity,
    ToolReuseResolveRequest,
    ToolTelemetryEvent,
)

from .contracts import ReuseConflict, TrustedContext
from .controller import WebReuseController


class ReuseService:
    """All authenticated ingress paths resolve the same authoritative namespace."""

    def __init__(
        self,
        controller: WebReuseController,
        frontier: LineTailFrontier,
        *,
        deployment_id: str,
        default_namespace: str | None,
    ) -> None:
        self.controller, self.frontier = controller, frontier
        self.deployment_id, self.default_namespace = deployment_id, default_namespace

    async def context(
        self, identity: ToolReuseIdentity, *, current: bool = True
    ) -> TrustedContext:
        try:
            deployment, namespace = await self.frontier.reuse_namespace(
                identity.job_id, identity.line_id
            )
            if current:
                line = await self.frontier.line_snapshot(
                    identity.job_id, identity.line_id
                )
                if (
                    line["tail_request_id"] != identity.tail_request_id
                    or line["llm_call_id"] != identity.llm_call_id
                    or line["phase"] not in {"BLOCKED", "READY"}
                ):
                    raise ReuseConflict("reuse identity is not active tail")
            return TrustedContext(
                deployment or self.deployment_id,
                namespace or self.default_namespace or "",
            )
        except FrontierConflict as exc:
            raise ReuseConflict(str(exc)) from exc

    async def resolve(self, request: ToolReuseResolveRequest, **kwargs: Any) -> Any:
        return await self.controller.resolve(
            request, trusted_context=await self.context(request.identity), **kwargs
        )

    async def poll(
        self, binding_id: str, identity: ToolReuseIdentity, **kwargs: Any
    ) -> Any:
        return await self.controller.poll(
            binding_id, identity, trusted_context=await self.context(identity), **kwargs
        )

    async def poll_deferred(
        self, binding_id: str, request: ToolReuseResolveRequest, **kwargs: Any
    ) -> Any:
        return await self.controller.poll_deferred(
            binding_id,
            request,
            trusted_context=await self.context(request.identity),
            **kwargs,
        )

    async def publish(self, report: LeaderResultPublish) -> Any:
        # Committed retries must remain legal after the tail advances.
        return await self.controller.publish(
            report, trusted_context=await self.context(report.identity, current=False)
        )

    async def record_execution(self, event: ToolTelemetryEvent) -> Any:
        identity = ToolReuseIdentity(
            **{
                key: getattr(event, key)
                for key in (
                    "job_id",
                    "line_id",
                    "tail_request_id",
                    "llm_call_id",
                    "action_id",
                    "tool_call_id",
                )
            }
        )
        context = await self.context(identity, current=False)
        if await self.frontier.accepted_tool_event(event):
            return await self.frontier.record_tool_event(event)
        await self.context(identity)
        return await self.controller.record_execution(event, trusted_context=context)

    async def progress(self, report: LeaderProgressReport) -> bool:
        return await self.controller.progress(
            report, trusted_context=await self.context(report.identity)
        )

    async def fail(self, report: BindingFailureReport) -> None:
        await self.controller.fail(
            report, trusted_context=await self.context(report.identity)
        )

    async def cancel_follower(self, report: FollowerCancellation) -> None:
        await self.controller.cancel_follower(
            report, trusted_context=await self.context(report.identity)
        )

    async def snapshot(self) -> dict[str, Any]:
        return await self.controller.snapshot()

    async def maintenance(self, **kwargs: Any) -> dict[str, int]:
        return await self.controller.maintenance(**kwargs)

    async def report_false_reuse(self, report: Any) -> bool:
        return await self.controller.report_false_reuse(report)

    async def update_semantic_policy(self, update: Any) -> dict[str, Any]:
        return await self.controller.update_semantic_policy(update)

    async def close(self) -> None:
        await self.controller.close()
