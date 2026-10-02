"""Join metadata-only engine/worker samples without summing TP time as latency."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median


def read_rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def write_csv(path, rows):
    with path.open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--include-residual-prefill", action="store_true")
    args = parser.parse_args()
    root = args.output
    manifest = json.loads((root / f"manifest-{args.run_id}.json").read_text())
    requests = read_rows(root / f"requests-{args.run_id}.jsonl")
    repeat_by_sample = {row["sample"]: row["repeat"] for row in requests}
    engine = [row for path in root.glob("engine-*.jsonl") for row in read_rows(path)]
    assert not any(row["kind"] == "transfer_failure" for row in engine)
    by_call = {
        row["binding"]["llm_call_id"]: row for row in engine if row["kind"] == "prefill"
    }
    by_job = defaultdict(list)
    for row in engine:
        if row["kind"] == "worker_transfer":
            by_job[row["job_id"]].append(row)
    transfers = []
    for row in engine:
        if row["kind"] != "engine_transfer":
            continue
        workers = by_job[row["job_id"]]
        assert len(workers) == manifest["engine"]["required_worker_count"]
        assert (
            len({w["rank"] for w in workers})
            == manifest["engine"]["required_worker_count"]
        )
        assert all(w["success"] and w["direction"] == row["direction"] for w in workers)
        transfers.append(
            {
                **row,
                "bytes": sum(w["transfer_size"] for w in workers),
                "cuda_max_rank_seconds": max(w["transfer_time"] for w in workers),
                "cuda_sum_rank_seconds": sum(w["transfer_time"] for w in workers),
            }
        )
    measured = []
    calibration = []
    groups = defaultdict(list)
    for request in requests:
        if request["phase"] in {"warmup", "seed"}:
            continue
        row = by_call[request["binding"]["llm_call_id"]]
        stats = row["stats"]
        phase = request["phase"]
        if phase in {"cold", "concurrent_cold"}:
            assert stats["num_cached_tokens"] == 0, (phase, stats)
        elif phase in {"gpu_prefix", "gpu_full_prefix"}:
            assert (
                stats["num_local_cached_tokens"] > 0
                and stats["num_external_cached_tokens"] == 0
            )
        elif phase == "cpu_restore":
            assert (
                stats["num_local_cached_tokens"] == 0
                and stats["num_external_cached_tokens"] > 0
            )
        loads = [
            job
            for job in transfers
            if job["engine_request_id"] == row["engine_request_id"]
            and job["direction"] == "restore"
        ]
        restore_seconds = (
            max(job["ended"] for job in loads) - min(job["started"] for job in loads)
            if loads
            else 0
        )
        load_bytes = sum(job["bytes"] for job in loads)
        sample = {
            "sample": request["sample"],
            "phase": phase,
            "concurrency": request["concurrency"],
            "prompt_tokens": request["prompt_tokens"],
            "cached_tokens": stats["num_cached_tokens"],
            "engine_prefill_seconds": row["seconds"],
            "http_seconds": request["http_seconds"],
            "restore_bytes": load_bytes,
            "restore_seconds": restore_seconds,
        }
        if phase == "cpu_restore":
            assert loads and load_bytes > 0
            assert max(job["ended"] for job in loads) <= row["started"], (
                "restore overlapped the measured prefill window",
                request["sample"],
            )
            calibration.append(
                {
                    "kind": "restore",
                    "prompt_tokens": 0,
                    "cached_tokens": 0,
                    "bytes": load_bytes,
                    "seconds": restore_seconds,
                }
            )
        if phase in {"cold", "gpu_prefix", "gpu_full_prefix"} or (
            phase == "cpu_restore" and args.include_residual_prefill
        ):
            calibration.append(
                {
                    "kind": "prefill",
                    "prompt_tokens": sample["prompt_tokens"],
                    "cached_tokens": sample["cached_tokens"],
                    "bytes": 0,
                    "seconds": row["seconds"],
                }
            )
        measured.append(sample)
        groups[(phase, request["prompt_tokens"])].append(sample)
    summary = []
    for (phase, length), samples in groups.items():
        summary.append(
            {
                "phase": phase,
                "prompt_tokens": length,
                "samples": len(samples),
                **{
                    key + "_median": median(sample[key] for sample in samples)
                    for key in (
                        "cached_tokens",
                        "engine_prefill_seconds",
                        "http_seconds",
                        "restore_bytes",
                        "restore_seconds",
                    )
                },
                "engine_prefill_seconds_min": min(
                    s["engine_prefill_seconds"] for s in samples
                ),
                "engine_prefill_seconds_max": max(
                    s["engine_prefill_seconds"] for s in samples
                ),
            }
        )
    isolated_path = root / "isolated-offload.jsonl"
    isolated_samples = []
    if isolated_path.exists():
        for sample in read_rows(isolated_path):
            if sample["status"] == "NO_NEW_COPY_CANDIDATE":
                continue
            assert sample["status"] == "APPLIED"
            jobs = [
                job
                for job in transfers
                if job["engine_request_id"] == sample["descriptor_id"]
                and job["direction"] == "offload"
            ]
            actual_bytes = sum(job["bytes"] for job in jobs)
            assert jobs and actual_bytes == sample["operation"]["cpu_committed_bytes"]
            seconds = max(job["ended"] for job in jobs) - min(
                job["started"] for job in jobs
            )
            calibration.append(
                {
                    "kind": "offload",
                    "prompt_tokens": 0,
                    "cached_tokens": 0,
                    "bytes": actual_bytes,
                    "seconds": seconds,
                }
            )
            isolated_samples.append(
                {
                    "sample": sample["sample"],
                    "repeat": sample.get(
                        "repeat", repeat_by_sample.get(sample["sample"])
                    ),
                    "prompt_tokens": sample["prompt_tokens"],
                    "bytes": actual_bytes,
                    "seconds": seconds,
                    "jobs": len(jobs),
                    "rpc_seconds": sample["ended"] - sample["started"],
                }
            )
        if isolated_samples:
            write_csv(root / "isolated-offload-costs.csv", isolated_samples)
    write_csv(root / "request-costs.csv", measured)
    write_csv(root / "measurements.csv", calibration)
    write_csv(root / "transfer-jobs.csv", transfers)
    write_csv(root / "summary.csv", summary)
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(
        json.dumps(
            {
                "request_samples": len(measured),
                "transfer_jobs": len(transfers),
                "fit_rows": len(calibration),
                "run_id": args.run_id,
            }
        )
    )


if __name__ == "__main__":
    main()
