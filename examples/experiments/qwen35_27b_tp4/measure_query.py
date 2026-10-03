"""Measure target-prefix query HTTP cost; retain observations, never input text."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:18851")
    parser.add_argument("--model", default="Qwen3.5-27B")
    parser.add_argument("--concurrencies", default="1")
    args = parser.parse_args()
    concurrencies = [int(value) for value in args.concurrencies.split(",")]
    if not concurrencies or any(value < 1 for value in concurrencies):
        parser.error("concurrencies must be positive integers")
    run_id = uuid4().hex
    with httpx.Client(base_url=args.url, timeout=900, trust_env=False) as client:
        response = client.get("/v1/kv/capabilities")
        response.raise_for_status()
        capability = response.json()
        assert capability["target_prefix_query"]
        (args.output / f"query-manifest-{run_id}.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "engine": capability["engine"],
                    "measured_at": datetime.now(UTC).isoformat(),
                    "concurrencies": concurrencies,
                    "repeats": 3,
                    "measurement_basis": (
                        "cold Chat target HTTP including render/tokenize/hash/lookup; "
                        "per-query and whole-sweep wall time; idle inference engine"
                    ),
                },
                indent=2,
            )
            + "\n"
        )
        for words in (8, 1024, 8192, 32768, 131072, 258000):
            payload = {
                "model": args.model,
                "messages": [{"role": "user", "content": " alpha" * words}],
                "max_tokens": 1,
                "chat_template_kwargs": {"enable_thinking": False},
                "cache_salt": run_id,
            }

            def query(slot, concurrency, repeat, words, payload):
                started = time.monotonic()
                query_id = f"{run_id}-{words}-{concurrency}-{repeat}-{slot}"
                response = client.post(
                    "/v1/kv/query-target",
                    json={
                        "schema_version": 1,
                        "owner_scope": "cost-calibration",
                        "expected_engine_epoch": capability["engine"]["engine_epoch"],
                        "query_id": query_id,
                        "api_kind": "chat",
                        "payload": payload,
                    },
                )
                elapsed = time.monotonic() - started
                response.raise_for_status()
                observation = response.json()
                assert observation["query_id"] == query_id
                assert (
                    observation["engine_epoch"] == capability["engine"]["engine_epoch"]
                )
                assert len(observation["inputs"]) == 1
                target = observation["inputs"][0]
                assert (
                    target["engine_identity_digest"]
                    == capability["engine"]["identity_digest"]
                )
                assert target["gpu_ready_tokens"] == 0
                assert target["recoverable_tokens"] == 0
                return {
                    "run_id": run_id,
                    "measured_at": datetime.now(UTC).isoformat(),
                    "input_kind": "synthetic repeated text; cold Chat target; no tools",
                    "warmup": words == 8,
                    "words": words,
                    "repeat": repeat,
                    "concurrency": concurrency,
                    "slot": slot,
                    "http_seconds": elapsed,
                    "observation": observation,
                }

            for concurrency in [1] if words == 8 else concurrencies:
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    for repeat in range(1 if words == 8 else 3):
                        sweep_started = time.monotonic()
                        futures = [
                            executor.submit(
                                query, slot, concurrency, repeat, words, payload
                            )
                            for slot in range(concurrency)
                        ]
                        rows = [future.result() for future in futures]
                        sweep_seconds = time.monotonic() - sweep_started
                        with (args.output / f"query-costs-{run_id}.jsonl").open(
                            "a"
                        ) as stream:
                            for row in rows:
                                stream.write(
                                    json.dumps({**row, "sweep_seconds": sweep_seconds})
                                    + "\n"
                                )
                        print(
                            json.dumps(
                                {
                                    "prompt_tokens": rows[0]["observation"]["inputs"][
                                        0
                                    ]["prompt_tokens"],
                                    "repeat": repeat,
                                    "concurrency": concurrency,
                                    "sweep_seconds": sweep_seconds,
                                }
                            ),
                            flush=True,
                        )


if __name__ == "__main__":
    main()
