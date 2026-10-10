"""Real target lookup and CPU-only continuation; outputs metadata only.

The filename is retained for existing commands; no workflow SLO is evaluated.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from uuid import uuid4

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18831")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with httpx.Client(base_url=args.url, timeout=120, trust_env=False) as client:

        def post(path, data):
            response = client.post(path, json=data)
            response.raise_for_status()
            return response.json()

        response = client.get("/v1/kv/capabilities")
        response.raise_for_status()
        capability = response.json()
        assert capability["target_prefix_query"] and capability["offload_gpu_reclaim"]
        epoch = capability["engine"]["engine_epoch"]
        common = {
            "schema_version": 1,
            "owner_scope": "prefix-verification",
            "expected_engine_epoch": epoch,
        }
        tag = uuid4().hex
        binding = {
            "schema_version": 1,
            "owner_scope": common["owner_scope"],
            "job_id": tag,
            "line_id": "line",
            "request_id": "first",
            "llm_call_id": "first",
            "attempt": 1,
            "context_epoch": 1,
        }
        payload = {
            "model": "flowpilot-real",
            "max_tokens": 24,
            "temperature": 0,
            "chat_template_kwargs": {"enable_thinking": False},
            "messages": [
                {
                    "role": "user",
                    "content": tag
                    + "\n"
                    + "实验记录：需要保持对话中的数字与事实一致。" * 40
                    + "\n请简短确认。",
                }
            ],
        }
        first = post(
            "/v1/chat/completions",
            {**payload, "kv_transfer_params": {"kv_control_binding": binding}},
        )
        deadline = time.monotonic() + 10
        while True:
            resolved = post("/v1/kv/resolve", binding)
            if resolved["status"] == "READY":
                break
            assert time.monotonic() < deadline, resolved["status"]
            time.sleep(0.02)
        descriptor = resolved["descriptors"][0]
        did = descriptor["descriptor_id"]
        payload["messages"] += [
            first["choices"][0]["message"],
            {"role": "user", "content": "请用一句话重述记录要求。"},
        ]

        def target(kind, body):
            return post(
                "/v1/kv/query-target",
                {**common, "query_id": tag, "api_kind": kind, "payload": body},
            )["inputs"][0]

        gpu = target("chat", payload)
        observed = post("/v1/kv/query", {**common, "descriptor_id": did})
        version = observed["effective_policy_version"] or 0
        operation = post(
            "/v1/kv/apply",
            {
                **common,
                "descriptor_id": did,
                "action_id": tag,
                "idempotency_key": tag,
                "action": "OFFLOAD",
                "expected_policy_version": version,
                "policy_version": version + 1,
                "source_llm_call_id": "first",
                "expected_tail_request_id": "first",
                "expected_tail_version": 1,
                "decision_ref": "real-verification",
            },
        )
        deadline = time.monotonic() + 10
        while operation["status"] == "ACCEPTED":
            assert time.monotonic() < deadline
            time.sleep(0.02)
            operation = post(
                "/v1/kv/status", {**common, "operation_id": operation["operation_id"]}
            )
        assert operation["status"] == "APPLIED", operation["status"]
        time.sleep(0.3)  # Permit the configured finish GRACE to expire.
        cpu = target("chat", payload)
        assert cpu["gpu_ready_tokens"] == 0, cpu
        assert cpu["recoverable_tokens"] > 0 and cpu["cpu_load_object_bytes"] > 0, cpu

        def load_bytes():
            text = client.get("/metrics").text
            return sum(
                float(line.rsplit(" ", 1)[1])
                for line in text.splitlines()
                if line.startswith("vllm:kv_offload_load_bytes_total{")
            )

        before_load = load_bytes()
        second = post(
            "/v1/chat/completions",
            {
                **payload,
                "kv_transfer_params": {
                    "kv_control_binding": {
                        **binding,
                        "llm_call_id": "second",
                        "request_id": "second",
                    }
                },
            },
        )
        assert second["choices"][0]["message"]["content"]
        deadline = time.monotonic() + 15
        while (after_load := load_bytes()) <= before_load:
            assert time.monotonic() < deadline, (
                "ordinary inference did not report CPU load"
            )
            time.sleep(0.2)
        responses = target(
            "responses",
            {"model": "flowpilot-real", "input": "简短确认。", "max_output_tokens": 8},
        )
        result = {
            "engine": capability["engine"],
            "gpu_target": gpu,
            "cpu_target": cpu,
            "offload_status": operation["status"],
            "cpu_load_bytes": after_load - before_load,
            "successor_usage": second["usage"],
            "responses_target": responses,
        }
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(
            json.dumps(
                {
                    "offload_status": operation["status"],
                    "cpu_load_bytes": after_load - before_load,
                    "gpu_prefix_after_offload": cpu["gpu_ready_tokens"],
                    "recoverable_tokens": cpu["recoverable_tokens"],
                }
            )
        )


if __name__ == "__main__":
    main()
