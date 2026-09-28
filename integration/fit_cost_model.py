"""Fit offline cost models from measured CSV rows; never measure HTTP as prefill.

Columns: kind,prompt_tokens,cached_tokens,bytes,seconds.
Kinds: prefill,offload,restore. Transfer seconds must be wall-clock completion,
and bytes must be the actual objects transferred across the declared workers.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path

from flowpilot.scheduling.cost import OfflineCostModel


def fit(samples: list[tuple[float, float]]) -> tuple[float, float, float]:
    if len({x for x, _ in samples}) < 2:
        raise ValueError("each fitted bucket needs at least two distinct work sizes")
    if any(x <= 0 or y <= 0 or not math.isfinite(x + y) for x, y in samples):
        raise ValueError(
            "calibration work and measured time must be finite and positive"
        )
    n = len(samples)
    sx, sy = sum(x for x, _ in samples), sum(y for _, y in samples)
    xx, xy = sum(x * x for x, _ in samples), sum(x * y for x, y in samples)
    slope = (n * xy - sx * sy) / (n * xx - sx * sx)
    intercept = (sy - slope * sx) / n
    if slope <= 0:
        raise ValueError("measurements do not support a positive throughput model")
    if intercept < 0:
        intercept, slope = 0.0, xy / xx
    error = max(abs(y - intercept - slope * x) for x, y in samples)
    return intercept, slope, error


def fit_prefill(
    samples: list[tuple[float, float]], upper: int, *, piecewise: bool = False
) -> dict:
    fixed, slope, error = fit(samples)
    bucket = {
        "max_context_tokens": upper,
        "fixed_seconds": fixed,
        "seconds_per_token": slope,
        "uncertainty_seconds": error,
    }
    if piecewise:
        work_sizes = sorted({x for x, _ in samples})
        segments = []
        for index, (left, right) in enumerate(pairwise(work_sizes)):
            fixed, slope, error = fit(
                [(x, y) for x, y in samples if left <= x <= right]
            )
            segments.append(
                {
                    "max_uncached_tokens": upper
                    if index == len(work_sizes) - 2
                    else int(right),
                    "fixed_seconds": fixed,
                    "seconds_per_token": slope,
                    "uncertainty_seconds": error,
                }
            )
        bucket["segments"] = segments
        bucket["uncertainty_seconds"] = max(
            abs(y - segment["fixed_seconds"] - x * segment["seconds_per_token"])
            for x, y in samples
            for segment in [next(s for s in segments if x <= s["max_uncached_tokens"])]
        )
    return bucket


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measurements", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--engine-identity-digest", required=True)
    parser.add_argument("--measurement-basis", required=True)
    parser.add_argument(
        "--measured-at", required=True, help="ISO8601 measurement timestamp"
    )
    parser.add_argument("--context-bounds", default="1024,2048,4096,8192,16384,32768")
    parser.add_argument(
        "--piecewise-prefill",
        action="store_true",
        help="Fit adjacent measured uncached-token sizes within each context bucket",
    )
    args = parser.parse_args()
    raw = args.measurements.read_bytes()
    rows = list(csv.DictReader(raw.decode().splitlines()))
    bounds = [int(value) for value in args.context_bounds.split(",")]
    if not bounds or bounds != sorted(set(bounds)) or bounds[0] <= 0:
        raise ValueError("context bounds must be positive and strictly increasing")
    for row in rows:
        if row["kind"] not in {"prefill", "offload", "restore"}:
            raise ValueError("unknown measurement kind")
        if row["kind"] == "prefill" and not (
            0 <= int(row["cached_tokens"]) < int(row["prompt_tokens"]) <= bounds[-1]
        ):
            raise ValueError("prefill sample is outside the declared context coverage")
    buckets = []
    lower = 0
    for upper in bounds:
        samples = [
            (
                float(row["prompt_tokens"]) - float(row["cached_tokens"]),
                float(row["seconds"]),
            )
            for row in rows
            if row["kind"] == "prefill" and lower < int(row["prompt_tokens"]) <= upper
        ]
        if samples:
            buckets.append(
                fit_prefill(samples, upper, piecewise=args.piecewise_prefill)
            )
        elif any(
            row["kind"] == "prefill" and int(row["prompt_tokens"]) > upper
            for row in rows
        ):
            raise ValueError(
                f"missing calibration coverage below context bound {upper}"
            )
        lower = upper
    transfers = {}
    for kind in ("offload", "restore"):
        samples = [
            (float(row["bytes"]), float(row["seconds"]))
            for row in rows
            if row["kind"] == kind
        ]
        if samples:
            fixed, slope, error = fit(samples)
            transfers[kind] = {
                "fixed_seconds": fixed,
                "seconds_per_byte": slope,
                "uncertainty_seconds": error,
            }
    model = OfflineCostModel(
        source=f"{args.measurements.resolve()}#sha256={hashlib.sha256(raw).hexdigest()}",
        version=datetime.now(UTC).strftime("offline-%Y%m%dT%H%M%SZ"),
        measured_at=datetime.fromisoformat(args.measured_at),
        model=args.model,
        engine_identity_digest=args.engine_identity_digest,
        measurement_basis=args.measurement_basis,
        prefill=tuple(buckets),
        **transfers,
    )
    args.output.write_text(model.model_dump_json(indent=2) + "\n")


if __name__ == "__main__":
    main()
