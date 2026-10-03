"""Measure explicit OFFLOAD after native CPU LRU eviction, with no inference."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from uuid import uuid4

import httpx

from .analyze_costs import read_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--phase",
        default="concurrent_cold",
        choices=("cold", "concurrent_cold", "offload_candidate"),
    )
    parser.add_argument("--url", default="http://127.0.0.1:18831")
    args = parser.parse_args()
    rows = read_rows(args.output / f"requests-{args.run_id}.jsonl")
    with httpx.Client(base_url=args.url, timeout=120, trust_env=False) as client:

        def post(path, body):
            response = client.post(path, json=body)
            response.raise_for_status()
            return response.json()

        capability = client.get("/v1/kv/capabilities").json()
        common = {
            "schema_version": 1,
            "owner_scope": "cost-calibration",
            "expected_engine_epoch": capability["engine"]["engine_epoch"],
        }
        for row in rows:
            if row["phase"] != args.phase:
                continue
            binding = row["binding"]
            resolved = post("/v1/kv/resolve", binding)
            assert resolved["status"] == "READY", resolved
            did = resolved["descriptors"][0]["descriptor_id"]
            before = post("/v1/kv/query", {**common, "descriptor_id": did})
            result = {
                "sample": row["sample"],
                "repeat": row["repeat"],
                "descriptor_id": did,
                "prompt_tokens": row["prompt_tokens"],
                "before": before,
            }
            if (
                before["gpu_ready_tokens"] == 0
                or before["cpu_standalone_tokens"] == before["gpu_ready_tokens"]
            ):
                result["status"] = "NO_NEW_COPY_CANDIDATE"
            else:
                version = before["effective_policy_version"] or 0
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
                        "decision_ref": "isolated-offload-cost",
                    },
                )
                deadline = started + 120
                while operation["status"] == "ACCEPTED":
                    assert time.monotonic() < deadline, operation
                    time.sleep(0.002)
                    operation = post(
                        "/v1/kv/status",
                        {**common, "operation_id": operation["operation_id"]},
                    )
                result.update(
                    status=operation["status"],
                    operation=operation,
                    started=started,
                    ended=time.monotonic(),
                )
            with (args.output / "isolated-offload.jsonl").open("a") as stream:
                stream.write(json.dumps(result) + "\n")
            print(
                json.dumps(
                    {
                        "sample": row["sample"],
                        "status": result["status"],
                        "new_bytes": result.get("operation", {}).get(
                            "cpu_committed_bytes"
                        ),
                    }
                ),
                flush=True,
            )
            assert result["status"] in {"APPLIED", "NO_NEW_COPY_CANDIDATE"}, result


if __name__ == "__main__":
    main()
