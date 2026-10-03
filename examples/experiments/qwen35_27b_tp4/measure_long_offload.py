"""Measure longest-prefix D2H after observed native CPU LRU eviction."""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--seed-run", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:18851")
    args = parser.parse_args()
    root = args.root
    config = json.loads((root / "profile.json").read_text())
    seed = json.loads((root / f"manifest-{args.seed_run}.json").read_text())
    assert seed["retention_seed_only"] and not seed["contexts"]
    rng = random.Random(seed["seed"])
    tokens = []
    for _ in range(seed["retained_prefixes"]):
        tokens = [
            rng.randrange(1000, 20000) for _ in range(seed["retained_prefix_length"])
        ]
    tag = uuid4().hex
    pressure_rng = random.Random(20261002)

    def save(name, row):
        with (root / name).open("a") as stream:
            stream.write(json.dumps(row) + "\n")

    with httpx.Client(base_url=args.url, timeout=900, trust_env=False) as client:

        def post(path, body):
            response = client.post(path, json=body)
            response.raise_for_status()
            return response.json()

        response = client.get("/v1/kv/capabilities")
        response.raise_for_status()
        capability = response.json()
        assert capability["engine"] == seed["engine"]
        common = {
            "schema_version": 1,
            "owner_scope": "cost-calibration",
            "expected_engine_epoch": capability["engine"]["engine_epoch"],
        }
        manifest = {
            "run_id": tag,
            "measured_at": datetime.now(UTC).isoformat(),
            "engine": capability["engine"],
            "seed_run": args.seed_run,
            "seed_index": seed["retained_prefixes"] - 1,
            "contexts": [len(tokens)],
            "repeats": 3,
            "pressure_context": 65536,
            "max_pressure_requests_per_repeat": 8,
            "purpose": "longest-prefix idle D2H after native CPU eviction",
            "input_kind": "synthetic token IDs from seed; no content saved",
        }
        (root / f"manifest-{tag}.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )

        def query(did):
            return post("/v1/kv/query", {**common, "descriptor_id": did})

        def infer(prompt, sample, repeat, salt, phase):
            call = f"{tag}-{sample}"
            binding = {
                "schema_version": 1,
                "owner_scope": common["owner_scope"],
                "job_id": tag,
                "line_id": sample,
                "request_id": call,
                "llm_call_id": call,
                "attempt": 1,
                "context_epoch": 1,
            }
            started = time.monotonic()
            result = post(
                "/v1/completions",
                {
                    **config["profiling"]["sampling"],
                    "model": config["vllm"]["args"]["served_model_name"],
                    "prompt": prompt,
                    "max_tokens": 1,
                    "ignore_eos": True,
                    "cache_salt": salt,
                    "kv_transfer_params": {"kv_control_binding": binding},
                },
            )
            ended = time.monotonic()
            assert result["usage"]["prompt_tokens"] == len(prompt)
            row = {
                "sample": sample,
                "phase": phase,
                "repeat": repeat,
                "concurrency": 1,
                "prompt_tokens": len(prompt),
                "started": started,
                "ended": ended,
                "http_seconds": ended - started,
                "binding": binding,
                "usage": result["usage"],
            }
            save(f"requests-{tag}.jsonl", row)
            resolved = post("/v1/kv/resolve", binding)
            assert resolved["status"] == "READY", resolved
            did = resolved["descriptors"][0]["descriptor_id"]
            print(
                json.dumps({"sample": sample, "http_seconds": ended - started}),
                flush=True,
            )
            return binding, did

        def apply(binding, did, action_name):
            before = query(did)
            version = before["effective_policy_version"] or 0
            action_id = uuid4().hex
            started = time.monotonic()
            operation = post(
                "/v1/kv/apply",
                {
                    **common,
                    "descriptor_id": did,
                    "action_id": action_id,
                    "idempotency_key": action_id,
                    "action": action_name,
                    "expected_policy_version": version,
                    "policy_version": version + 1,
                    "source_llm_call_id": binding["llm_call_id"],
                    "expected_tail_request_id": binding["request_id"],
                    "expected_tail_version": 1,
                    "decision_ref": "long-idle-offload-cost",
                },
            )
            while operation["status"] == "ACCEPTED":
                assert time.monotonic() - started < 120, operation
                time.sleep(0.002)
                operation = post(
                    "/v1/kv/status",
                    {**common, "operation_id": operation["operation_id"]},
                )
            assert operation["status"] == "APPLIED", operation
            ended = time.monotonic()
            record = {
                "descriptor_id": did,
                "before": before,
                "operation": operation,
                "status": operation["status"],
                "started": started,
                "ended": ended,
            }
            save(f"controls-{tag}.jsonl", {**record, "binding": binding})
            return record

        for repeat in range(3):
            sample = f"offload-p{len(tokens)}-r{repeat}"
            binding, did = infer(
                tokens, sample, repeat, args.seed_run, "long_candidate"
            )
            apply(binding, did, "KEEP")
            expected_gpu = query(did)["gpu_ready_tokens"]
            assert expected_gpu > 0
            for index in range(manifest["max_pressure_requests_per_repeat"]):
                observation = query(did)
                save(
                    f"long-pressure-{tag}.jsonl",
                    {"repeat": repeat, "index": index, "observation": observation},
                )
                assert observation["gpu_ready_tokens"] == expected_gpu, observation
                if not any(observation["cpu_ready_blocks_by_group"].values()):
                    break
                pressure = [
                    pressure_rng.randrange(1000, 20000)
                    for _ in range(manifest["pressure_context"])
                ]
                pressure_binding, pressure_did = infer(
                    pressure, f"pressure-r{repeat}-i{index}", repeat, tag, "pressure"
                )
                apply(pressure_binding, pressure_did, "OFFLOAD")
            observation = query(did)
            assert observation["gpu_ready_tokens"] == expected_gpu, observation
            assert not any(observation["cpu_ready_blocks_by_group"].values()), (
                observation
            )
            record = apply(binding, did, "OFFLOAD")
            assert (
                record["operation"]["cpu_committed_bytes"]
                == observation["offload_object_bytes"]
                > 0
            )
            save(
                "isolated-offload.jsonl",
                {
                    **record,
                    "sample": sample,
                    "repeat": repeat,
                    "prompt_tokens": len(tokens),
                    "run_id": tag,
                },
            )
            print(
                json.dumps(
                    {
                        "sample": sample,
                        "D2H_bytes": record["operation"]["cpu_committed_bytes"],
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    main()
