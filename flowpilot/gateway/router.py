from __future__ import annotations

import asyncio
from collections.abc import Iterable

import httpx

from flowpilot.config import InferenceInstance


class NoCompatibleInstance(RuntimeError):
    pass


class InferenceRouter:
    """Round-robin phase 0 placement with model compatibility and health probes."""

    def __init__(self, instances: Iterable[InferenceInstance]) -> None:
        self.instances = tuple(instances)
        if not self.instances:
            raise ValueError("at least one inference instance is required")
        self._cursor = 0
        self._lock = asyncio.Lock()

    async def candidates(self, model: str) -> tuple[InferenceInstance, ...]:
        compatible = tuple(item for item in self.instances if item.supports(model))
        if not compatible:
            raise NoCompatibleInstance(f"no instance supports model {model!r}")
        async with self._lock:
            start = self._cursor % len(compatible)
            self._cursor += 1
        return compatible[start:] + compatible[:start]

    async def health(self, client: httpx.AsyncClient) -> dict[str, bool]:
        async def check(instance: InferenceInstance) -> tuple[str, bool]:
            suffix = "/models" if instance.base_url.endswith("/v1") else "/v1/models"
            try:
                response = await client.get(f"{instance.base_url}{suffix}", timeout=5)
            except httpx.HTTPError:
                return instance.instance_id, False
            return instance.instance_id, response.is_success

        return dict(await asyncio.gather(*(check(item) for item in self.instances)))

    def contains(self, instance_id: str) -> bool:
        return any(item.instance_id == instance_id for item in self.instances)
