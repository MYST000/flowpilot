"""Explicit synthetic Tool-duration prior for local workflow experiments."""

from __future__ import annotations

from random import Random


class SyntheticToolDurationPrior:
    """Draw a duration after a factual Tool name is known; never delay execution."""

    def __init__(self, seed: int | None = None) -> None:
        self._random = Random(seed)

    def estimate_ms(self, tool_family: str) -> float:
        if "search" in tool_family.casefold():
            return self._random.uniform(1_000.0, 2_000.0)
        return self._random.uniform(100.0, 200.0)
