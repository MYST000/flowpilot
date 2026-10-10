"""Real OpenHands -> FlowPilot -> vLLM + local Terminal HTTP workflow.

This is an opt-in smoke executable. It fails unless the LLM, Tool, reuse,
admission, and KV-control observations come from actual local services.
"""

from __future__ import annotations

import argparse
import json
import socket
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import mcp.types
import uvicorn
from fastapi.responses import PlainTextResponse
from openhands.sdk import Agent, LocalConversation, LocalWorkspace
from openhands.sdk.event import ObservationEvent
from openhands.sdk.flowpilot import FlowPilotConfig
from openhands.sdk.llm import LLM
from openhands.sdk.mcp.definition import MCPToolAction, MCPToolObservation
from openhands.sdk.mcp.tool import MCPToolDefinition
from openhands.sdk.tool import ToolExecutor, register_tool
from openhands.sdk.tool.spec import Tool
from openhands.tools.terminal import TerminalTool
from pydantic import SecretStr

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.protocol import ToolRegistryEntry
from flowpilot.scheduling.admission import AdmissionConfig
from flowpilot.scheduling.cost import OfflineCostModel
from flowpilot.scheduling.retention import RetentionConfig


def wait_for_health(url: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    with httpx.Client(timeout=2, trust_env=False) as client:
        while time.monotonic() < deadline:
            try:
                response = client.get(url)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
    raise RuntimeError(f"service did not become healthy: {url}")


def run_agent(
    number: int,
    gateway_url: str,
    model: str,
    page_url: str,
    root: Path,
    *,
    search: bool = False,
) -> dict[str, object]:
    llm = LLM(
        model=f"openai/{model}",
        base_url=gateway_url + "/v1",
        api_key=SecretStr("real-workflow-key"),
        temperature=0,
        max_output_tokens=384,
        stream=False,
        caching_prompt=False,
        litellm_extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    conversation = LocalConversation(
        agent=Agent(
            llm=llm,
            tools=(
                [Tool(name="web_search", params={})]
                if search
                else [
                    Tool(
                        name=TerminalTool.name,
                        params={"terminal_type": "subprocess"},
                    )
                ]
            ),
            include_default_tools=[],
            tool_concurrency_limit=1,
        ),
        workspace=LocalWorkspace(working_dir=root / f"agent-{number}"),
        visualizer=None,
        max_iteration_per_run=4,
        flowpilot=FlowPilotConfig(
            enabled=True,
            gateway_url=gateway_url,
            api_key="real-workflow-key",
            job_id=f"real-workflow-{number}",
            line_id=f"line-{number}",
            exact_reuse_enabled=not search,
            reusable_web_tools=("terminal",) if not search else (),
            timeout=10,
        ),
    )
    try:
        if search:
            conversation.send_message(
                "Use web_search exactly once with query 'FlowPilot integration page'. "
                "After its result, answer with the page title."
            )
        else:
            conversation.send_message(
                "Use the terminal tool exactly once to run this exact command: "
                f"`curl -sS {page_url}`. "
                "After its result, answer with the page title. "
                "Do not run another command."
            )
        conversation.run()
        observations = [
            event
            for event in conversation.state.active_branch()
            if isinstance(event, ObservationEvent)
            and event.tool_name == ("web_search" if search else "terminal")
        ]
        if len(observations) != 1:
            raise AssertionError(
                f"agent {number} produced {len(observations)} Tool observations"
            )
        observation = observations[0]
        if "FlowPilot integration page" not in observation.observation.text:
            raise AssertionError(f"agent {number} did not receive the page")
        return {
            "job_id": f"real-workflow-{number}",
            "tool_call_id": observation.tool_call_id,
            "reused": "FlowPilot reuse provenance" in observation.observation.text,
        }
    finally:
        conversation.close()


def run_pressure_agent(number: int, gateway_url: str, model: str, root: Path) -> None:
    conversation = LocalConversation(
        agent=Agent(
            llm=LLM(
                model=f"openai/{model}",
                base_url=gateway_url + "/v1",
                api_key=SecretStr("real-workflow-key"),
                temperature=0,
                max_output_tokens=48,
                stream=False,
                caching_prompt=False,
                litellm_extra_body={"chat_template_kwargs": {"enable_thinking": False}},
            ),
            tools=[],
            include_default_tools=[],
            tool_concurrency_limit=1,
        ),
        workspace=LocalWorkspace(working_dir=root / f"pressure-{number}"),
        visualizer=None,
        max_iteration_per_run=1,
        flowpilot=FlowPilotConfig(
            enabled=True,
            gateway_url=gateway_url,
            api_key="real-workflow-key",
            job_id=f"pressure-{number}",
            line_id=f"pressure-line-{number}",
            timeout=10,
        ),
    )
    try:
        unique_text = " ".join(f"pressure{number}record{index}" for index in range(260))
        conversation.send_message(
            "Read these unique records and reply with exactly DONE: " + unique_text
        )
        conversation.run()
    finally:
        conversation.close()


def metric_value(metrics: str, name: str) -> float:
    values = [
        float(line.rsplit(" ", 1)[1])
        for line in metrics.splitlines()
        if line.startswith(name + "{")
    ]
    return sum(values)


def kv_transfer_bytes(vllm_url: str) -> tuple[float, float]:
    with httpx.Client(timeout=10, trust_env=False) as client:
        response = client.get(vllm_url + "/metrics")
        response.raise_for_status()
    return (
        metric_value(response.text, "vllm:kv_offload_store_bytes_total"),
        metric_value(response.text, "vllm:kv_offload_load_bytes_total"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vllm-url", default="http://127.0.0.1:18801")
    parser.add_argument("--model", default="flowpilot-real")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--pressure-jobs", type=int, default=0)
    parser.add_argument("--require-cpu-reuse", action="store_true")
    parser.add_argument(
        "--admission-policy", choices=("wait_cost", "fifo"), default="wait_cost"
    )
    parser.add_argument(
        "--retention-window-basis",
        choices=("tool_and_queue", "tool_only"),
        default="tool_and_queue",
    )
    parser.add_argument("--cost-model", type=Path)

    args = parser.parse_args()
    cost_model = (
        OfflineCostModel.model_validate_json(args.cost_model.read_text())
        if args.cost_model
        else None
    )
    root = args.work_dir or Path(tempfile.mkdtemp(prefix="flowpilot-real-"))
    root.mkdir(parents=True, exist_ok=True)
    wait_for_health(args.vllm_url + "/health", 10)
    with httpx.Client(timeout=5, trust_env=False) as client:
        capability = client.get(args.vllm_url + "/v1/kv/capabilities")
        capability.raise_for_status()
        if not capability.json().get("descriptor_query"):
            raise AssertionError("vLLM KV descriptor query is unavailable")
    initial_store_bytes, initial_load_bytes = kv_transfer_bytes(args.vllm_url)

    page_requests: list[float] = []
    search_requests: list[str] = []
    gateway = create_app(
        Settings(
            instances=(InferenceInstance("local-vllm", args.vllm_url),),
            trace_path=root / "trace.jsonl",
            ingress_api_key="real-workflow-key",
            reuse_enabled=True,
            reuse_cache_path=root / "reuse-v4.sqlite",
            web_tool_registry=(
                ToolRegistryEntry(
                    tool_name="terminal",
                    canonical_tool_family="terminal_url_fetch",
                    tool_version="1",
                    result_schema_version="terminal-observation-v1",
                    adapter_id="terminal_url_fetch_v1",
                    command_line_reuse="url_exact",
                ),
            ),
            admission=AdmissionConfig(
                enabled=True,
                limit=1,
                policy=args.admission_policy,
                cost_model=cost_model,
            ),
            retention=RetentionConfig(
                enabled=True,
                owner_scope="flowpilot-real-workflow",
                window_basis=args.retention_window_basis,
                timeout_seconds=5,
                refresh_seconds=0.05,
                keep_horizon_seconds=0.25,
                gpu_free_reserve_allocations=1_000_000,
            ),
            synthetic_tool_duration_enabled=True,
            synthetic_tool_duration_seed=23,
        )
    )

    @gateway.get("/fixture/page")
    async def page() -> PlainTextResponse:
        page_requests.append(time.monotonic())
        return PlainTextResponse("FlowPilot integration page\nTitle: Real workflow\n")

    @gateway.get("/fixture/search")
    async def search_page(q: str) -> PlainTextResponse:
        search_requests.append(q)
        return PlainTextResponse("FlowPilot integration page\nTitle: Real workflow\n")

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    gateway_url = f"http://127.0.0.1:{sock.getsockname()[1]}"

    class LocalSearchExecutor(ToolExecutor):
        def __call__(self, action, conversation=None):
            del conversation
            with httpx.Client(timeout=10, trust_env=False) as client:
                response = client.get(
                    gateway_url + "/fixture/search",
                    params={"q": action.to_mcp_arguments()["query"]},
                )
                response.raise_for_status()
            return MCPToolObservation.from_call_tool_result(
                "web_search",
                mcp.types.CallToolResult(
                    content=[mcp.types.TextContent(type="text", text=response.text)],
                    isError=False,
                ),
            )

    register_tool(
        "web_search",
        MCPToolDefinition(
            mcp_tool=mcp.types.Tool(
                name="web_search",
                inputSchema={
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
            ),
            description="Search the local integration page index over HTTP",
            action_type=MCPToolAction,
            observation_type=MCPToolObservation,
            executor=LocalSearchExecutor(),
        ),
    )
    server = uvicorn.Server(uvicorn.Config(gateway, log_level="error"))
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True
    )
    thread.start()
    try:
        wait_for_health(gateway_url + "/flowpilot/health", 30)
        page_url = gateway_url + "/fixture/page"
        first = run_agent(1, gateway_url, args.model, page_url, root)
        if args.pressure_jobs:
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(
                    pool.map(
                        lambda number: run_pressure_agent(
                            number, gateway_url, args.model, root
                        ),
                        range(args.pressure_jobs),
                    )
                )
        before_second_store_bytes, before_second_load_bytes = kv_transfer_bytes(
            args.vllm_url
        )
        second = run_agent(2, gateway_url, args.model, page_url, root)
        after_second_store_bytes, after_second_load_bytes = kv_transfer_bytes(
            args.vllm_url
        )
        if first["reused"] or not second["reused"]:
            raise AssertionError("historical reuse was not observed in agent history")
        if len(page_requests) != 1:
            raise AssertionError("exact reuse did not suppress the second HTTP fetch")
        # Two real OpenHands conversations compete for a single admission credit.
        with ThreadPoolExecutor(max_workers=2) as pool:
            concurrent = list(
                pool.map(
                    lambda number: run_agent(
                        number, gateway_url, args.model, page_url, root
                    ),
                    (3, 4),
                )
            )
        if len(page_requests) != 1 or not all(item["reused"] for item in concurrent):
            raise AssertionError("concurrent conversations lost the cached result")
        search_result = run_agent(
            5, gateway_url, args.model, page_url, root, search=True
        )
        if search_result["reused"] or len(search_requests) != 1:
            raise AssertionError("local search was not executed exactly once")
        with httpx.Client(timeout=10, trust_env=False) as client:
            headers = {"x-flowpilot-api-key": "real-workflow-key"}
            for number in range(1, 6):
                frontier = client.get(
                    gateway_url + f"/flowpilot/v1/jobs/real-workflow-{number}/frontier",
                    headers=headers,
                )
                frontier.raise_for_status()
                if frontier.json()["lines"]:
                    raise AssertionError(
                        "explicitly closed conversation retained its line"
                    )
            state = client.get(
                gateway_url + "/flowpilot/v1/scheduling/state", headers=headers
            ).json()
            resolutions = client.get(
                gateway_url + "/flowpilot/v1/tool-resolutions", headers=headers
            ).json()["records"]
            calls = client.get(
                gateway_url + "/flowpilot/v1/gateway-calls", headers=headers
            ).json()["calls"]
        trace = [
            json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()
        ]
        events = Counter(item["event_type"] for item in trace)
        if events["line_finish"] != 5 + args.pressure_jobs:
            raise AssertionError(
                "explicit conversation close did not report line finish"
            )
        receipts = [
            item["fields"]
            for item in trace
            if item["event_type"] == "kv_policy_receipt"
        ]
        if state["admission"]["inflight"] != 0:
            raise AssertionError("admission credit was not returned")
        if len(calls) < 10 + args.pressure_jobs or events["request_admitted"] != len(
            calls
        ):
            raise AssertionError("real LLM calls bypassed admission")
        if not any(
            item["duration_estimate_basis"] == "synthetic_factual_family_v1"
            and 100 <= item["duration_estimate_ms"] <= 200
            for item in resolutions
            if item["tool_family"] == "terminal"
        ):
            raise AssertionError("synthetic non-search duration was not observed")
        if not any(
            item["duration_estimate_basis"] == "synthetic_factual_family_v1"
            and 1000 <= item["duration_estimate_ms"] <= 2000
            for item in resolutions
            if item["tool_family"] == "web_search"
        ):
            raise AssertionError("synthetic search duration was not observed")
        if not any(
            item["action"] == "OFFLOAD" and item["status"] == "APPLIED"
            for item in receipts
        ):
            raise AssertionError("vLLM did not apply an OFFLOAD retention action")
        if "FlowPilot integration page" in (root / "trace.jsonl").read_text():
            raise AssertionError("trace contains Tool payload")
        final_store_bytes, final_load_bytes = kv_transfer_bytes(args.vllm_url)
        cpu_store_bytes = final_store_bytes - initial_store_bytes
        cpu_load_bytes = final_load_bytes - initial_load_bytes
        second_load_bytes = after_second_load_bytes - before_second_load_bytes
        if cpu_store_bytes < 0 or cpu_load_bytes < 0 or second_load_bytes < 0:
            raise AssertionError("vLLM KV transfer counters decreased during workflow")
        if args.require_cpu_reuse and cpu_load_bytes <= 0:
            raise AssertionError("workflow inference did not load CPU KV")
        print(
            json.dumps(
                {
                    "work_dir": str(root),
                    "model": args.model,
                    "conversations": 5,
                    "pressure_jobs": args.pressure_jobs,
                    "real_page_fetches": len(page_requests),
                    "real_search_fetches": len(search_requests),
                    "gateway_calls": len(calls),
                    "admitted": events["request_admitted"],
                    "ordering_basis": Counter(
                        r["fields"]["ordering_basis"]
                        for r in trace
                        if r["event_type"] == "request_admitted"
                    ),
                    "retention_window_basis": args.retention_window_basis,
                    "cost_model_version": cost_model.version if cost_model else None,
                    "line_finishes": events["line_finish"],
                    "kv_receipts": Counter(
                        f"{item['action']}:{item['status']}" for item in receipts
                    ),
                    "admission": state["admission"],
                    "kv_status": state["kv"]["status"],
                    "cpu_store_bytes": cpu_store_bytes,
                    "cpu_load_bytes": cpu_load_bytes,
                    "cpu_load_bytes_before_second": (
                        before_second_load_bytes - initial_load_bytes
                    ),
                    "cpu_load_bytes_during_second_conversation": second_load_bytes,
                    "cpu_store_bytes_before_second": (
                        before_second_store_bytes - initial_store_bytes
                    ),
                    "cpu_store_bytes_after_second": (
                        after_second_store_bytes - initial_store_bytes
                    ),
                },
                default=dict,
                indent=2,
            )
        )
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


if __name__ == "__main__":
    main()
