"""Measure real prefill and engine-owned KV recovery; save metadata only."""

from __future__ import annotations

import argparse
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18831")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", default="Qwen3.5-9B")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--sampling-json", default='{"temperature": 0}')
    parser.add_argument("--full-gpu-prefix", action="store_true")
    parser.add_argument("--retention-only", action="store_true")
    parser.add_argument(
        "--retention-seed-only",
        action="store_true",
        help="Create/offload prefixes without resuming, for CPU pressure preparation",
    )
    parser.add_argument("--retained-prefixes", type=int, default=0)
    parser.add_argument("--retained-prefix-length", type=int, default=258048)
    parser.add_argument(
        "--contexts", default="1024,2048,4096,8192,16384,32768,65536,131071"
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--concurrency-probe", action="store_true")
    args = parser.parse_args()
    sampling = json.loads(args.sampling_json)
    allowed_sampling = {
        "temperature",
        "top_p",
        "top_k",
        "presence_penalty",
        "repetition_penalty",
        "min_p",
        "seed",
    }
    if not isinstance(sampling, dict) or set(sampling) - allowed_sampling:
        parser.error("sampling-json must contain only sampling parameters")
    contexts = (
        []
        if args.retention_only
        else [int(value) for value in args.contexts.split(",")]
    )
    tag = uuid4().hex
    client = httpx.Client(base_url=args.url, timeout=args.timeout, trust_env=False)

    def post(path, body):
        response = client.post(path, json=body)
        response.raise_for_status()
        return response.json()

    response = client.get("/v1/kv/capabilities")
    response.raise_for_status()
    capability = response.json()
    assert capability["offload_gpu_reclaim"] and capability["transfer_measurement"]
    common = {
        "schema_version": 1,
        "owner_scope": "cost-calibration",
        "expected_engine_epoch": capability["engine"]["engine_epoch"],
    }
    manifest = {
        "measured_at": datetime.now(UTC).isoformat(),
        "run_id": tag,
        "contexts": contexts,
        "repeats": args.repeats,
        "engine": capability["engine"],
        "capabilities": capability,
        "input_kind": "synthetic token IDs; exact length; no content saved",
        "max_output_tokens": 1,
        "seed": 41,
        "model": args.model,
        "sampling": sampling,
        "full_gpu_prefix": args.full_gpu_prefix,
        "retained_prefixes": args.retained_prefixes,
        "retained_prefix_length": args.retained_prefix_length,
        "retention_seed_only": args.retention_seed_only,
    }
    (args.output / f"manifest-{tag}.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )

    def save(row):
        with (args.output / f"requests-{tag}.jsonl").open("a") as stream:
            stream.write(json.dumps(row) + "\n")
        print(
            json.dumps(
                {
                    key: row[key]
                    for key in ("sample", "phase", "prompt_tokens", "http_seconds")
                }
            ),
            flush=True,
        )

    def infer(tokens, sample, phase, repeat, concurrency=1):
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
                **sampling,
                "model": args.model,
                "prompt": tokens,
                "max_tokens": 1,
                "ignore_eos": True,
                "cache_salt": tag,
                "kv_transfer_params": {"kv_control_binding": binding},
            },
        )
        ended = time.monotonic()
        row = {
            "sample": sample,
            "phase": phase,
            "repeat": repeat,
            "concurrency": concurrency,
            "prompt_tokens": len(tokens),
            "started": started,
            "ended": ended,
            "http_seconds": ended - started,
            "binding": binding,
            "usage": result["usage"],
        }
        assert row["usage"]["prompt_tokens"] == len(tokens)
        save(row)
        return binding

    def resolve(binding):
        deadline = time.monotonic() + 60
        while True:
            result = post("/v1/kv/resolve", binding)
            if result["status"] == "READY":
                return result["descriptors"][0]["descriptor_id"]
            assert time.monotonic() < deadline, result
            time.sleep(0.01)

    def offload(binding, sample):
        did = resolve(binding)
        observation = post("/v1/kv/query", {**common, "descriptor_id": did})
        version = observation["effective_policy_version"] or 0
        action = uuid4().hex
        started = time.monotonic()
        operation = post(
            "/v1/kv/apply",
            {
                **common,
                "descriptor_id": did,
                "action_id": action,
                "idempotency_key": action,
                "action": "OFFLOAD",
                "expected_policy_version": version,
                "policy_version": version + 1,
                "source_llm_call_id": binding["llm_call_id"],
                "expected_tail_request_id": binding["request_id"],
                "expected_tail_version": 1,
                "decision_ref": "cost-calibration",
            },
        )
        deadline = started + 60
        while operation["status"] == "ACCEPTED":
            assert time.monotonic() < deadline, operation
            time.sleep(0.01)
            operation = post(
                "/v1/kv/status", {**common, "operation_id": operation["operation_id"]}
            )
        assert operation["status"] == "APPLIED", operation
        applied = time.monotonic()
        while True:
            cpu = post("/v1/kv/query", {**common, "descriptor_id": did})
            if cpu["gpu_ready_tokens"] == 0:
                break
            assert time.monotonic() < deadline, cpu
            time.sleep(0.01)
        assert cpu["recoverable_tokens"] > 0, cpu
        with (args.output / f"controls-{tag}.jsonl").open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        "sample": sample,
                        "binding": binding,
                        "before": observation,
                        "after": cpu,
                        "operation": operation,
                        "rpc_to_applied_seconds": applied - started,
                        "rpc_to_gpu_evicted_seconds": time.monotonic() - started,
                    }
                )
                + "\n"
            )
        return did

    rng = random.Random(41)

    def prompt(length):
        return [rng.randrange(1000, 20000) for _ in range(length)]

    try:
        for index in range(0 if args.retention_only else 2):
            infer(prompt(8192), f"warmup-{index}", "warmup", -1)
        for length in contexts:
            for repeat in range(args.repeats):
                sample = f"p{length}-r{repeat}"
                infer(prompt(length), sample, "cold", repeat)
                tokens = prompt(length)
                seed_length = max(
                    capability["layout"]["group_block_tokens"][0] + 1,
                    length // 2,
                )
                infer(tokens[:seed_length], sample, "seed", repeat)
                binding = infer(tokens, sample, "gpu_prefix", repeat)
                if args.full_gpu_prefix:
                    binding = infer(tokens, sample, "gpu_full_prefix", repeat)
                offload(binding, sample)
                infer(tokens, sample, "cpu_restore", repeat)
        if args.concurrency_probe:
            for length in (4096, 16384, 32768):
                for repeat in range(args.repeats):
                    prompts = [prompt(length) for _ in range(4)]
                    with ThreadPoolExecutor(max_workers=4) as pool:
                        futures = [
                            pool.submit(
                                infer,
                                tokens,
                                f"batch-p{length}-r{repeat}-i{index}",
                                "concurrent_cold",
                                repeat,
                                4,
                            )
                            for index, tokens in enumerate(prompts)
                        ]
                        for future in futures:
                            future.result()
        retained = []

        def observe_retained(stage):
            observations = []
            for _, sample, descriptor_id in retained:
                observation = post(
                    "/v1/kv/query", {**common, "descriptor_id": descriptor_id}
                )
                observations.append({"sample": sample, "observation": observation})
            with (args.output / f"retained-prefixes-{tag}.jsonl").open("a") as stream:
                stream.write(
                    json.dumps({"stage": stage, "observations": observations}) + "\n"
                )

        for index in range(args.retained_prefixes):
            tokens = prompt(args.retained_prefix_length)
            sample = f"retained-p{args.retained_prefix_length}-i{index}"
            binding = infer(tokens, sample, "retained_seed", index)
            did = offload(binding, sample)
            retained.append((tokens, sample, did))
            observe_retained(f"after_offload_{index}")
        for index, (tokens, sample, _) in enumerate(
            [] if args.retention_seed_only else retained
        ):
            observe_retained(f"before_resume_{index}")
            infer(tokens, sample, "retained_resume", index)
            observe_retained(f"after_resume_{index}")
        time.sleep(1)  # Allow final native transfer completions to be observed.
    finally:
        client.close()


if __name__ == "__main__":
    main()
