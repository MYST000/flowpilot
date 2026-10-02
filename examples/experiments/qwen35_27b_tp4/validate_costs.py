"""Same-session holdout checks for measured costs, separate from workflow evidence."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median

from flowpilot.scheduling.cost import PrefillCalibration
from integration.fit_cost_model import fit, fit_prefill


def errors(values: list[tuple[float, float]]) -> dict:
    absolute = [abs(predicted - actual) for actual, predicted in values]
    relative = [abs(predicted - actual) / actual for actual, predicted in values]
    return {
        "samples": len(values),
        "median_absolute_seconds": median(absolute),
        "max_absolute_seconds": max(absolute),
        "median_relative_error": median(relative),
        "max_relative_error": max(relative),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    root = args.output
    requests = {
        row["sample"]: row["repeat"]
        for line in (root / f"requests-{args.run_id}.jsonl").read_text().splitlines()
        for row in [json.loads(line)]
    }
    with (root / "request-costs.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    buckets = defaultdict(list)
    transfers = []
    for row in rows:
        if row["phase"] not in {"cold", "gpu_prefix", "gpu_full_prefix", "cpu_restore"}:
            continue
        repeat = requests[row["sample"]]
        work = int(row["prompt_tokens"]) - int(row["cached_tokens"])
        buckets[int(row["prompt_tokens"])].append(
            (work, float(row["engine_prefill_seconds"]), repeat)
        )
        if row["phase"] == "cpu_restore":
            transfers.append(
                (int(row["restore_bytes"]), float(row["restore_seconds"]), repeat)
            )
    prefill_errors = []
    by_context = []
    for context, samples in sorted(buckets.items()):
        calibration = PrefillCalibration.model_validate(
            fit_prefill(
                [(x, y) for x, y, repeat in samples if repeat < 2],
                context,
                piecewise=True,
            )
        )
        held_out = [
            (y, calibration.seconds(x)) for x, y, repeat in samples if repeat == 2
        ]
        prefill_errors.extend(held_out)
        by_context.append({"prompt_tokens": context, **errors(held_out)})

    def transfer_errors(samples: list[tuple[int, float, int]]) -> dict:
        fixed, slope, _ = fit([(x, y) for x, y, repeat in samples if repeat < 2])
        return errors(
            [(y, fixed + x * slope) for x, y, repeat in samples if repeat == 2]
        )

    report = {
        "basis": "repeats 0/1 train; repeat 2 held out in the same engine session",
        "limitations": "not independent workload validation or a p95/p99 bound",
        "prefill": errors(prefill_errors),
        "prefill_by_context": by_context,
        "restore": transfer_errors(transfers),
    }
    offload_path = root / "isolated-offload-costs.csv"
    if offload_path.exists():
        with offload_path.open() as stream:
            offloads = list(csv.DictReader(stream))
        report["offload"] = transfer_errors(
            [
                (
                    int(r["bytes"]),
                    float(r["seconds"]),
                    int(r["repeat"]) if "repeat" in r else requests[r["sample"]],
                )
                for r in offloads
            ]
        )
    (root / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
