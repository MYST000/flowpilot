"""Measure target-prefix query HTTP cost; retain observations, never input text."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:18851")
    parser.add_argument("--model", default="Qwen3.5-27B")
    args = parser.parse_args()
    run_id = uuid4().hex
    with httpx.Client(base_url=args.url, timeout=900, trust_env=False) as client:
        response = client.get("/v1/kv/capabilities")
        response.raise_for_status()
        capability = response.json()
        assert capability["target_prefix_query"]
        for words in (8, 1024, 8192, 32768, 131072, 258000):
            payload = {
                "model": args.model,
                "messages": [{"role": "user", "content": " alpha" * words}],
                "max_tokens": 1,
                "chat_template_kwargs": {"enable_thinking": False},
                "cache_salt": run_id,
            }
            for repeat in range(1 if words == 8 else 3):
                started = time.monotonic()
                response = client.post(
                    "/v1/kv/query-target",
                    json={
                        "schema_version": 1,
                        "owner_scope": "cost-calibration",
                        "expected_engine_epoch": capability["engine"]["engine_epoch"],
                        "query_id": f"{run_id}-{words}-{repeat}",
                        "api_kind": "chat",
                        "payload": payload,
                    },
                )
                elapsed = time.monotonic() - started
                response.raise_for_status()
                observation = response.json()
                assert len(observation["inputs"]) == 1
                target = observation["inputs"][0]
                assert target["gpu_ready_tokens"] == 0
                assert target["recoverable_tokens"] == 0
                row = {
                    "run_id": run_id,
                    "measured_at": datetime.now(UTC).isoformat(),
                    "input_kind": "synthetic repeated text; cold Chat target; no tools",
                    "warmup": words == 8,
                    "words": words,
                    "repeat": repeat,
                    "http_seconds": elapsed,
                    "observation": observation,
                }
                with (args.output / f"query-costs-{run_id}.jsonl").open("a") as stream:
                    stream.write(json.dumps(row) + "\n")
                print(
                    json.dumps(
                        {
                            "prompt_tokens": target["prompt_tokens"],
                            "repeat": repeat,
                            "http_seconds": elapsed,
                        }
                    ),
                    flush=True,
                )


if __name__ == "__main__":
    main()
