"""Exercise the live FlowPilot admission queue with concurrent vLLM requests."""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import uvicorn

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.scheduling.admission import AdmissionConfig

API_KEY = "real-admission-key"


def wait_for_health(url: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    with httpx.Client(timeout=2, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                if client.get(url).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
    raise RuntimeError(f"service did not become healthy: {url}")


def identity_headers(name: str) -> dict[str, str]:
    return {
        "x-flowpilot-api-key": API_KEY,
        "x-flowpilot-protocol-version": "flowpilot-phase0-v2",
        "x-flowpilot-job-id": f"admission-{name}",
        "x-flowpilot-line-id": f"line-{name}",
        "x-flowpilot-conversation-id": f"conversation-{name}",
        "x-flowpilot-request-id": f"request-{name}",
        "x-flowpilot-tail-request-id": f"tail-{name}",
        "x-flowpilot-request-attempt": "1",
        "x-flowpilot-llm-call-id": f"call-{name}",
        "x-flowpilot-tail-version": "0",
        "x-flowpilot-context-epoch": "1",
        "x-flowpilot-context-sequence": "0",
        "x-flowpilot-context-cursor": "root",
        "x-flowpilot-context-digest": "a" * 64,
    }


async def wait_for_state(
    client: httpx.AsyncClient,
    gateway_url: str,
    condition,
    *,
    seconds: float = 20,
) -> dict[str, object]:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        response = await client.get(
            gateway_url + "/flowpilot/v1/scheduling/state",
            headers={"x-flowpilot-api-key": API_KEY},
        )
        response.raise_for_status()
        state = response.json()["admission"]
        if condition(state):
            return state
        await asyncio.sleep(0.05)
    raise AssertionError("admission queue did not reach the expected state")


async def exercise(gateway_url: str, trace_path: Path) -> dict[str, object]:
    now = datetime.now(UTC)
    jobs = {
        "occupied": (now, None),
        "low": (now, now + timedelta(minutes=10)),
        "high": (now - timedelta(seconds=120), now + timedelta(seconds=5)),
    }
    async with httpx.AsyncClient(timeout=120, trust_env=False) as client:
        for name, (started, deadline) in jobs.items():
            response = await client.post(
                gateway_url + "/flowpilot/v1/jobs",
                headers={"x-flowpilot-api-key": API_KEY},
                json={
                    "job_id": f"admission-{name}",
                    "workflow_started_at": started.isoformat(),
                    "deadline": deadline.isoformat() if deadline else None,
                },
            )
            response.raise_for_status()
            response = await client.post(
                gateway_url + "/flowpilot/v1/lines",
                headers={"x-flowpilot-api-key": API_KEY},
                json={
                    "job_id": f"admission-{name}",
                    "line_id": f"line-{name}",
                    "conversation_id": f"conversation-{name}",
                    "context_epoch": 1,
                    "base_context_cursor": "root",
                    "context_digest": "a" * 64,
                },
            )
            response.raise_for_status()

        async def complete(name: str) -> dict[str, object]:
            prompt = (
                "List the integers from 1 to 500, each on a separate line, "
                f"without explanation. This request identifier is {uuid.uuid4()}."
                if name == "occupied"
                else f"Answer with the word {name}."
            )
            response = await client.post(
                gateway_url + "/v1/chat/completions",
                headers=identity_headers(name),
                json={
                    "model": "flowpilot-real",
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "max_tokens": 512 if name == "occupied" else 24,
                    "temperature": 0,
                    "chat_template_kwargs": {"enable_thinking": False},
                },
            )
            response.raise_for_status()
            payload = response.json()
            if not payload.get("choices"):
                raise AssertionError(f"{name} produced no real vLLM choice")
            return payload

        first = asyncio.create_task(complete("occupied"))
        await wait_for_state(client, gateway_url, lambda s: s["inflight"] == 1)
        low = asyncio.create_task(complete("low"))
        await wait_for_state(
            client,
            gateway_url,
            lambda s: [q["llm_call_id"] for q in s["queued"]] == ["call-low"],
        )
        high = asyncio.create_task(complete("high"))
        queued = await wait_for_state(
            client,
            gateway_url,
            lambda s: (
                {q["llm_call_id"] for q in s["queued"]} == {"call-high", "call-low"}
            ),
        )
        await asyncio.gather(first, low, high)
        final = await wait_for_state(
            client,
            gateway_url,
            lambda s: s["inflight"] == 0 and s["free"] == 1,
        )

    trace_text = await asyncio.to_thread(trace_path.read_text)
    trace = [json.loads(line) for line in trace_text.splitlines()]
    admitted = [
        event["identity"]["llm_call_id"]
        for event in trace
        if event["event_type"] == "request_admitted"
    ]
    if len(admitted) != 3 or set(admitted) != {
        "call-occupied",
        "call-high",
        "call-low",
    }:
        raise AssertionError(f"missing or duplicate admission: {admitted}")
    evidence = [
        event["fields"] for event in trace if event["event_type"] == "request_admitted"
    ]
    for fields in evidence:
        cost = fields["kv_start_cost_ms"]
        if cost is not None:
            assert abs(fields["score_ms"] - (fields["queue_wait_ms"] - cost)) < 1e-6
        if fields["ordering_basis"].startswith("fifo:"):
            assert fields["sequence"] == min(fields["candidate_sequences"])
        if fields["ordering_basis"] == "fifo:cost_unknown":
            assert fields["cost_unknown_reasons"]
    # This probe has no cost file: every sweep must explicitly use FIFO.
    assert all(row["ordering_basis"] == "fifo:cost_unknown" for row in evidence)
    assert [row["sequence"] for row in evidence] == sorted(
        row["sequence"] for row in evidence
    )
    return {
        "queued_order": [item["llm_call_id"] for item in queued["queued"]],
        "admitted_order": admitted,
        "admission_evidence": evidence,
        "admission_limit": final["limit"],
        "final_inflight": final["inflight"],
        "final_free": final["free"],
        "work_basis": [
            event["fields"]["work_basis"]
            for event in trace
            if event["event_type"] == "request_admitted"
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vllm-url", default="http://127.0.0.1:18801")
    parser.add_argument("--trace-path", type=Path, required=True)
    args = parser.parse_args()
    wait_for_health(args.vllm_url + "/health", 10)
    args.trace_path.parent.mkdir(parents=True, exist_ok=True)
    app = create_app(
        Settings(
            instances=(InferenceInstance("local-vllm", args.vllm_url),),
            trace_path=args.trace_path,
            ingress_api_key=API_KEY,
            admission=AdmissionConfig(enabled=True, limit=1),
        )
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    gateway_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True
    )
    thread.start()
    try:
        wait_for_health(gateway_url + "/flowpilot/health", 30)
        print(json.dumps(asyncio.run(exercise(gateway_url, args.trace_path)), indent=2))
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


if __name__ == "__main__":
    main()
