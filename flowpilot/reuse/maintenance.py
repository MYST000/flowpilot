from __future__ import annotations

from .controller import WebReuseController


class ToolCacheMaintenance:
    """Small explicit maintenance facade; never mutates live bindings."""

    def __init__(self, controller: WebReuseController) -> None:
        self.controller = controller

    async def run_once(self) -> dict[str, int]:
        return await self.controller.maintenance()


async def purge_expired(controller: WebReuseController) -> dict[str, int]:
    return await controller.maintenance()
