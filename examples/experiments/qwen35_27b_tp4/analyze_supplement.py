"""Join one supplemental engine epoch and extend the existing measured model."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median

from examples.experiments.qwen35_9b_tp4.analyze_costs import read_rows, write_csv
from flowpilot.scheduling.cost import OfflineCostModel
from integration.fit_cost_model import fit

from .validate_costs import errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--retention-run", required=True)
    args = parser.parse_args()
    root = args.root
    output = root / "analysis"
    output.mkdir(exist_ok=True)
    model = OfflineCostModel.model_validate_json(
        (args.baseline / "cost-model.json").read_text()
    )
    baseline_csv = args.baseline / "measurements.csv"
    source_path, source_hash = model.source.rsplit("#sha256=", 1)
    assert Path(source_path).resolve() == baseline_csv.resolve()
    assert hashlib.sha256(baseline_csv.read_bytes()).hexdigest() == source_hash
    caps = json.loads((root / "capabilities.json").read_text())
    assert caps["engine"]["identity_digest"] == model.engine_identity_digest
    assert (
        json.loads((root / "profile.json").read_text())["vllm"]
        == json.loads((args.baseline / "profile.json").read_text())["vllm"]
    )
    engine = [row for path in root.glob("engine-*.jsonl") for row in read_rows(path)]
    assert not any(row["kind"] == "transfer_failure" for row in engine)
    workers = defaultdict(list)
    for row in engine:
        if row["kind"] == "worker_transfer":
            workers[row["job_id"]].append(row)
    transfers = []
    for row in engine:
        if row["kind"] != "engine_transfer":
            continue
        shards = workers[row["job_id"]]
        assert len(shards) == caps["engine"]["required_worker_count"]
        assert len({shard["rank"] for shard in shards}) == len(shards)
        assert all(s["success"] and s["direction"] == row["direction"] for s in shards)
        transfers.append(
            {
                **row,
                "bytes": sum(s["transfer_size"] for s in shards),
                "cuda_max_rank_seconds": max(s["transfer_time"] for s in shards),
            }
        )
    assert len({row["job_id"] for row in transfers}) == len(transfers)
    assert set(workers) == {row["job_id"] for row in transfers}
    write_csv(output / "transfer-jobs.csv", transfers)
    prefill = [row for row in engine if row["kind"] == "prefill"]
    by_call = {row["binding"]["llm_call_id"]: row for row in prefill}

    offloads = []
    skipped = []
    for sample in read_rows(root / "isolated-offload.jsonl"):
        if sample["status"] == "NO_NEW_COPY_CANDIDATE":
            skipped.append(
                {
                    "sample": sample["sample"],
                    "descriptor_id": sample["descriptor_id"],
                    "gpu_ready_tokens": sample["before"]["gpu_ready_tokens"],
                    "cpu_standalone_tokens": sample["before"]["cpu_standalone_tokens"],
                }
            )
            continue
        assert sample["status"] == "APPLIED"
        jobs = [
            row
            for row in transfers
            if row["engine_request_id"] == sample["descriptor_id"]
            and row["direction"] == "offload"
            and sample["started"] <= row["started"] <= row["ended"] <= sample["ended"]
        ]
        actual_bytes = sum(job["bytes"] for job in jobs)
        assert jobs and actual_bytes == sample["operation"]["cpu_committed_bytes"] > 0
        started = min(job["started"] for job in jobs)
        ended = max(job["ended"] for job in jobs)
        assert not any(r["started"] < ended and r["ended"] > started for r in prefill)
        offloads.append(
            {
                "sample": sample["sample"],
                "repeat": sample["repeat"],
                "prompt_tokens": sample["prompt_tokens"],
                "bytes": actual_bytes,
                "object_bytes": sample["before"]["offload_object_bytes"],
                "seconds": ended - started,
                "jobs": len(jobs),
                "rpc_seconds": sample["ended"] - sample["started"],
            }
        )
    assert offloads, "no measured independent D2H copies"
    write_csv(output / "isolated-offload-costs.csv", offloads)
    fixed, slope, uncertainty = fit(
        [(row["bytes"], row["seconds"]) for row in offloads]
    )
    train_fixed, train_slope, _ = fit(
        [(row["bytes"], row["seconds"]) for row in offloads if row["repeat"] < 2]
    )
    offload_holdout = errors(
        [
            (row["seconds"], train_fixed + train_slope * row["bytes"])
            for row in offloads
            if row["repeat"] == 2
        ]
    )

    requests = read_rows(root / f"requests-{args.retention_run}.jsonl")
    retained = []

    def request_cost(sample):
        row = by_call[sample["binding"]["llm_call_id"]]
        stats = row["stats"]
        jobs = [
            job
            for job in transfers
            if job["engine_request_id"] == row["engine_request_id"]
            and job["direction"] == "restore"
        ]
        load_bytes = sum(job["bytes"] for job in jobs)
        load_seconds = (
            max(job["ended"] for job in jobs) - min(job["started"] for job in jobs)
            if jobs
            else 0.0
        )
        if jobs:
            assert max(job["ended"] for job in jobs) <= row["started"]
        predicted_prefill = model.prefill_seconds(
            sample["prompt_tokens"], stats["num_cached_tokens"]
        )
        return {
            "sample": sample["sample"],
            "phase": sample["phase"],
            "prompt_tokens": sample["prompt_tokens"],
            "local_cached_tokens": stats["num_local_cached_tokens"],
            "external_cached_tokens": stats["num_external_cached_tokens"],
            "prefill_seconds": row["seconds"],
            "predicted_prefill_seconds": predicted_prefill,
            "restore_bytes": load_bytes,
            "restore_seconds": load_seconds,
            "predicted_restore_seconds": model.restore.seconds(load_bytes)
            if model.restore is not None
            else None,
            "http_seconds": sample["http_seconds"],
        }

    retained = [request_cost(sample) for sample in requests]
    write_csv(output / "retention-requests.csv", retained)
    states = read_rows(root / f"retained-prefixes-{args.retention_run}.jsonl")
    before_resumes = next(row for row in states if row["stage"] == "before_resume_0")
    observed_prefixes = [item["observation"] for item in before_resumes["observations"]]
    resumes = [row for row in retained if row["phase"] == "retained_resume"]
    restored = [row for row in resumes if row["restore_bytes"] > 0]
    long_restores = [
        request_cost(row)
        for path in root.glob("requests-*.jsonl")
        for row in read_rows(path)
        if row["phase"] == "long_candidate"
    ]
    assert len(long_restores) == 3
    assert all(
        row["local_cached_tokens"] == 0
        and row["external_cached_tokens"] > 0
        and row["restore_bytes"] > 0
        for row in long_restores
    )
    write_csv(output / "long-prefix-restores.csv", long_restores)

    query_groups = defaultdict(list)
    for path in root.glob("query-costs-*.jsonl"):
        for row in read_rows(path):
            if row["warmup"]:
                continue
            obs = row["observation"]["inputs"][0]
            assert obs["engine_epoch"] == caps["engine"]["engine_epoch"]
            assert obs["engine_identity_digest"] == model.engine_identity_digest
            assert obs["gpu_ready_tokens"] == obs["recoverable_tokens"] == 0
            query_groups[(obs["prompt_tokens"], row["concurrency"])].append(row)
    query_summary = []
    probe_timeout = json.loads((root / "profile.json").read_text())["flowpilot"][
        "admission"
    ]["probe_timeout_seconds"]
    for (tokens, concurrency), rows in sorted(query_groups.items()):
        sweeps = {(row["run_id"], row["repeat"]): row["sweep_seconds"] for row in rows}
        query_summary.append(
            {
                "prompt_tokens": tokens,
                "concurrency": concurrency,
                "requests": len(rows),
                "sweeps": len(sweeps),
                "http_seconds_median": median(row["http_seconds"] for row in rows),
                "http_seconds_max": max(row["http_seconds"] for row in rows),
                "probe_timeout_seconds": probe_timeout,
                "requests_over_probe_timeout": sum(
                    row["http_seconds"] > probe_timeout for row in rows
                ),
                "sweep_seconds_median": median(sweeps.values()),
                "sweep_seconds_max": max(sweeps.values()),
            }
        )
    assert query_summary, "target query samples are missing"
    write_csv(output / "query-summary.csv", query_summary)

    with baseline_csv.open() as stream:
        calibration = list(csv.DictReader(stream))
    assert not any(row["kind"] == "offload" for row in calibration)
    calibration += [
        {
            "kind": "offload",
            "prompt_tokens": 0,
            "cached_tokens": 0,
            "bytes": row["bytes"],
            "seconds": row["seconds"],
        }
        for row in offloads
    ]
    combined = output / "combined-measurements.csv"
    write_csv(combined, calibration)
    updated = model.model_dump()
    updated.update(
        source=f"{combined.resolve()}#sha256={hashlib.sha256(combined.read_bytes()).hexdigest()}",
        version=datetime.now(UTC).strftime("offline-%Y%m%dT%H%M%SZ"),
        measured_at=datetime.now(UTC),
        measurement_basis=(
            model.measurement_basis.replace("Offload not calibrated. ", "")
            + f"; D2H supplement: {root.name}, same engine identity/configuration, "
            + f"{len(offloads)} isolated copies, actual bytes "
            + f"{min(r['bytes'] for r in offloads)}.."
            + f"{max(r['bytes'] for r in offloads)}; "
            + "all four workers complete; no overlapping inference; "
            + "prefill/H2D coefficients preserved from original cpu64 session"
        ),
        offload={
            "fixed_seconds": fixed,
            "seconds_per_byte": slope,
            "uncertainty_seconds": uncertainty,
        },
    )
    updated_model = OfflineCostModel.model_validate(updated)
    assert (
        updated_model.prefill == model.prefill
        and updated_model.restore == model.restore
    )
    (output / "cost-model.json").write_text(
        updated_model.model_dump_json(indent=2) + "\n"
    )
    summary = {
        "engine": caps["engine"],
        "completed_transfer_jobs": len(transfers),
        "independent_D2H": {
            "samples": len(offloads),
            "skipped_candidates": skipped,
            "min_bytes": min(row["bytes"] for row in offloads),
            "max_bytes": max(row["bytes"] for row in offloads),
            "calibration": updated["offload"],
            "holdout": offload_holdout,
            "holdout_basis": (
                "repeats 0/1 train; repeat 2 held out; same engine session"
            ),
        },
        "retention": {
            "created_prefixes": len(observed_prefixes),
            "cpu_tokens_before_any_resume": [
                o["cpu_standalone_tokens"] for o in observed_prefixes
            ],
            "gpu_tokens_before_any_resume": [
                o["gpu_ready_tokens"] for o in observed_prefixes
            ],
            "object_bytes_before_any_resume": [
                o["offload_object_bytes"] for o in observed_prefixes
            ],
            "completed_resumes": len(resumes),
            "resumes_with_cpu_load": len(restored),
            "restore_prediction_error": errors(
                [
                    (r["restore_seconds"], r["predicted_restore_seconds"])
                    for r in restored
                ]
            )
            if restored
            else None,
            "residual_prefill_prediction_error": errors(
                [
                    (r["prefill_seconds"], r["predicted_prefill_seconds"])
                    for r in restored
                ]
            )
            if restored
            else None,
        },
        "target_queries": query_summary,
        "long_prefix_ordinary_restores": {
            "samples": len(long_restores),
            "restore_prediction_error": errors(
                [
                    (r["restore_seconds"], r["predicted_restore_seconds"])
                    for r in long_restores
                ]
            ),
            "prefill_prediction_error": errors(
                [
                    (r["prefill_seconds"], r["predicted_prefill_seconds"])
                    for r in long_restores
                ]
            ),
        },
        "capacity": json.loads((root / "capacity-analysis.json").read_text()),
        "model_version": updated_model.version,
        "limitations": [
            "No long-term residency guarantee or workflow SLO claim",
            "D2H outside measured actual bytes is extrapolation",
            "Cold synthetic Chat targets; no Tool schema; idle engine",
            "Query RPC latency is not added to prefill/transfer coefficients",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
