"""Deterministic offline metrics for queue/SLO experiments."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime


def jain_fairness(shares: Iterable[float]) -> float:
    """Return Jain's fairness index, with zero for an empty/all-zero sample."""
    values = [float(value) for value in shares]
    if any(value < 0 for value in values):
        raise ValueError("fairness shares cannot be negative")
    total = sum(values)
    if not values or total == 0:
        return 0.0
    return total * total / (len(values) * sum(value * value for value in values))


def deadline_miss_rate(
    completions: Iterable[tuple[datetime, datetime | None, datetime | None]],
) -> float:
    """Compute misses from ``(arrival, deadline, completed_at)`` tuples."""
    rows = list(completions)
    if not rows:
        return 0.0
    misses = sum(
        1
        for _arrival, deadline, completed_at in rows
        if deadline is not None and (completed_at is None or completed_at > deadline)
    )
    return misses / len(rows)


def slo_goodput(
    completions: Iterable[tuple[datetime, datetime | None, datetime | None]],
) -> float:
    """Return the fraction of completed requests meeting their deadline."""
    rows = list(completions)
    if not rows:
        return 0.0
    return 1.0 - deadline_miss_rate(rows)


__all__ = ["deadline_miss_rate", "jain_fairness", "slo_goodput"]
