"""Prepare small GPU KEEP preferences before an independent CPU-pressure run."""

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
    parser.add_argument("output", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:18851")
    parser.add_argument("--compact-gpu-prefix", action="store_true")
    parser.add_argument("--contexts", default="1024,2048,4096,8192,16384,32768")
    parser.add_argument(
        "--config", type=Path, default=Path(__file__).with_name("config.json")
    )
    args = parser.parse_args()
    contexts = [int(value) for value in args.contexts.split(",")]
    if not contexts or any(value < 1 for value in contexts):
        parser.error("contexts must be positive integers")
    config = json.loads(args.config.read_text())
    tag = uuid4().hex
    rng = random.Random(41)
    with httpx.Client(base_url=args.url, timeout=900, trust_env=False) as client:

        def post(path, body):
            response = client.post(path, json=body)
            response.raise_for_status()
            return response.json()

        response = client.get("/v1/kv/capabilities")
        response.raise_for_status()
        capability = response.json()
        common = {
            "schema_version": 1,
            "owner_scope": "cost-calibration",
            "expected_engine_epoch": capability["engine"]["engine_epoch"],
        }
        manifest = {
            "run_id": tag,
            "measured_at": datetime.now(UTC).isoformat(),
            "engine": capability["engine"],
            "purpose": (
                "GPU KEEP preferences for later idle OFFLOAD after CPU LRU pressure"
            ),
            "contexts": contexts,
            "compact_gpu_prefix": args.compact_gpu_prefix,
            "repeats": 3,
            "sampling": config["profiling"]["sampling"],
            "input_kind": "synthetic token IDs; no content retained",
        }
        (args.output / f"manifest-{tag}.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )

        def infer(tokens, sample, repeat, phase):
            call = f"{tag}-{sample}-{phase}"
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
                    "prompt": tokens,
                    "max_tokens": 1,
                    "ignore_eos": True,
                    "cache_salt": tag,
                    "kv_transfer_params": {"kv_control_binding": binding},
                },
            )
            ended = time.monotonic()
            assert result["usage"]["prompt_tokens"] == len(tokens)
            row = {
                "sample": sample,
                "phase": phase,
                "repeat": repeat,
                "concurrency": 1,
                "prompt_tokens": len(tokens),
                "started": started,
                "ended": ended,
                "http_seconds": ended - started,
                "binding": binding,
                "usage": result["usage"],
            }
            with (args.output / f"requests-{tag}.jsonl").open("a") as stream:
                stream.write(json.dumps(row) + "\n")
            resolved = post("/v1/kv/resolve", binding)
            assert resolved["status"] == "READY", resolved
            return binding, resolved["descriptors"][0]["descriptor_id"]

        def query(did):
            return post("/v1/kv/query", {**common, "descriptor_id": did})

        def apply(binding, did, action_name, sample):
            before = query(did)
            assert before["gpu_ready_tokens"] > 0, before
            version = before["effective_policy_version"] or 0
            action = uuid4().hex
            receipt = post(
                "/v1/kv/apply",
                {
                    **common,
                    "descriptor_id": did,
                    "action_id": action,
                    "idempotency_key": action,
                    "action": action_name,
                    "expected_policy_version": version,
                    "policy_version": version + 1,
                    "source_llm_call_id": binding["llm_call_id"],
                    "expected_tail_request_id": binding["request_id"],
                    "expected_tail_version": 1,
                    "decision_ref": "idle-offload-preparation",
                },
            )
            deadline = time.monotonic() + 120
            while receipt["status"] == "ACCEPTED":
                assert time.monotonic() < deadline, receipt
                time.sleep(0.002)
                receipt = post(
                    "/v1/kv/status", {**common, "operation_id": receipt["operation_id"]}
                )
            assert receipt["status"] == "APPLIED", receipt
            after = query(did)
            if action_name == "OFFLOAD":
                while after["gpu_ready_tokens"]:
                    assert time.monotonic() < deadline, after
                    time.sleep(0.002)
                    after = query(did)
                assert after["cpu_standalone_tokens"] > 0, after
            filename = (
                f"keep-{tag}.jsonl" if action_name == "KEEP" else f"prime-{tag}.jsonl"
            )
            with (args.output / filename).open("a") as stream:
                stream.write(
                    json.dumps(
                        {
                            "sample": sample,
                            "binding": binding,
                            "before": before,
                            "after": after,
                            "receipt": receipt,
                        }
                    )
                    + "\n"
                )
            return receipt

        for length in manifest["contexts"]:
            for repeat in range(3):
                sample = f"offload-p{length}-r{repeat}"
                tokens = [rng.randrange(1000, 20000) for _ in range(length)]
                binding, did = infer(
                    tokens,
                    sample,
                    repeat,
                    "seed" if args.compact_gpu_prefix else "cold",
                )
                if args.compact_gpu_prefix:
                    apply(binding, did, "OFFLOAD", sample)
                    binding, did = infer(tokens, sample, repeat, "offload_candidate")
                receipt = apply(binding, did, "KEEP", sample)
                print(
                    json.dumps(
                        {"sample": sample, "status": receipt["status"], "run_id": tag}
                    ),
                    flush=True,
                )


if __name__ == "__main__":
    main()
