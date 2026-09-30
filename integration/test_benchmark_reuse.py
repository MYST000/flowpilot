"""Run in OpenHands' venv with FlowPilot and benchmark src on PYTHONPATH."""

import asyncio
import json
import socket
import threading
import time
from dataclasses import replace

import httpx
import pytest
import uvicorn
from benchmark_adapters import native_browsecomp
from benchmark_adapters.config import (
    Config,
    DatasetConfig,
    LLMConfig,
    RetrievalConfig,
    RuntimeConfig,
)
from benchmark_adapters.contracts import Task
from benchmark_adapters.retrieval import build_index
from benchmark_adapters.reuse_profile import export_registry, retrieval_scope
from benchmark_adapters.runner import run_task
from cryptography.fernet import Fernet
from fastmcp import Client, FastMCP
from openhands.sdk.flowpilot import FlowPilotConfig

from flowpilot.app import create_app
from flowpilot.config import InferenceInstance, Settings
from flowpilot.observability.trace import InMemoryTraceSink
from flowpilot.protocol import ToolRegistryEntry


class FeedbackRecorder:
    def __init__(self):
        self.feedbacks = []

    def bind(self, app):
        pass

    def on_response(self, *args, **kwargs):
        pass

    def on_resolution(self, record):
        pass

    async def feedback(self, payload):
        self.feedbacks.append(payload)
        return {"status": "accepted"}

    def snapshot(self):
        return {"enabled": True}

    async def close(self):
        pass


@pytest.mark.parametrize(
    "kind,backend",
    [
        ("hotpot", "sqlite"),
        ("browsecomp", "sqlite"),
        ("browsecomp", "browsecomp_mcp"),
    ],
)
@pytest.mark.parametrize("deferred", [False, True])
def test_real_retrieval_execution_then_reuse_and_corpus_isolation(
    tmp_path, monkeypatch, kind, backend, deferred
):
    reader = "read_document" if kind == "hotpot" else "get_document"
    arguments = {"doc_id" if kind == "hotpot" else "docid": "1"}
    contents = {"text": "Alpha original evidence."}
    native = FastMCP("benchmark-reuse-fixture")

    @native.tool
    def search(query: str) -> list[dict]:
        """Search the fixed fixture corpus."""
        return [{"docid": "1", "snippet": contents["text"], "score": 1.0}]

    @native.tool
    def get_document(docid: str) -> dict:
        """Return the complete fixture document."""
        return {"docid": docid, "text": contents["text"]}

    monkeypatch.setattr(
        native_browsecomp, "Client", lambda url, **kw: Client(native, **kw)
    )
    configurations = []
    for revision in ("original", "changed"):
        corpus = tmp_path / f"{revision}.jsonl"
        corpus.write_text(
            json.dumps(
                {
                    "docid": "1",
                    "title": "Alpha",
                    "sentences": [f"Alpha {revision} evidence."],
                }
            )
            + "\n"
        )
        index = tmp_path / f"{revision}.sqlite"
        build_index(corpus, index, revision)
        configurations.append(
            Config(
                dataset=DatasetConfig(kind=kind, id=kind, revision="questions-v1"),
                runtime=replace(RuntimeConfig(), max_iterations=4, task_timeout=30),
                retrieval=RetrievalConfig(
                    backend=backend,
                    index_path=str(index),
                    corpus_revision=revision,
                    server_policy_revision="fixture-v1",
                ),
            )
        )
    registry = tuple(
        ToolRegistryEntry.model_validate({**entry, "semantic_reuse_enabled": False})
        for config in configurations
        for entry in export_registry(config)
    )
    requests = []

    def inference(request):
        assert not any(key.startswith("x-flowpilot-") for key in request.headers)
        body = json.loads(request.content)
        requests.append(body)
        number = len(requests)
        done = any(message.get("role") == "tool" for message in body["messages"])
        calls = (
            [
                (
                    "finish",
                    {
                        "message": json.dumps(
                            {
                                "answer": "Alpha",
                                "supporting_facts": [["Alpha", 0]],
                            }
                        )
                    },
                )
            ]
            if done
            else [("search", {"query": "Alpha"}), (reader, arguments)]
        )
        return httpx.Response(
            200,
            json={
                "id": f"response-{number}",
                "object": "chat.completion",
                "created": 1,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": f"call-{number}-{index}",
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": json.dumps(
                                            {**args, "summary": "Get evidence"}
                                        ),
                                    },
                                }
                                for index, (name, args) in enumerate(calls)
                            ],
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 10,
                    "total_tokens": 30,
                },
            },
        )

    feedback = FeedbackRecorder()
    trace = InMemoryTraceSink()
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(inference))
    app = create_app(
        Settings(
            instances=(InferenceInstance("fixture", "http://inference"),),
            ingress_api_key="test-key",
            trace_path=tmp_path / "gateway.jsonl",
            reuse_enabled=True,
            reuse_cache_path=tmp_path / "reuse.sqlite",
            web_tool_registry=registry,
            dcs_enabled=deferred,
            dcs_encryption_key=Fernet.generate_key().decode(),
            dcs_wal_path=tmp_path / "dcs.sqlite",
        ),
        http_client=upstream,
        tool_duration_adapter=feedback,
        trace_sink=trace,
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    base = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True
    )
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        assert server.started
        for index, config in enumerate(
            [configurations[0], configurations[0], configurations[1]]
        ):
            if index == 2:
                contents["text"] = "Alpha changed evidence."
            config = replace(
                config,
                llm=LLMConfig(
                    model="openai/test-model",
                    base_url=base + "/v1",
                    num_retries=0,
                ),
            )
            runtime = FlowPilotConfig(
                enabled=True,
                gateway_url=base,
                api_key="test-key",
                exact_reuse_enabled=True,
                deferred_context_enabled=deferred,
                reusable_web_tools=("search", reader),
                data_source_constraints=retrieval_scope(config),
            )
            directory = tmp_path / f"attempt-{index}"
            result = run_task(
                config,
                Task(kind, "questions-v1", "dev", "1", "Find Alpha."),
                directory,
                flowpilot_config=runtime,
                run_id="same-single-agent-campaign",
            )
            assert result["execution_status"] == "completed", result
            assert result["artifact_status"] == "valid", result
            request_records = [
                record
                for record in trace.records
                if record["event_type"] == "llm_request"
                and record["identity"]["conversation_id"] == result["conversation_id"]
            ]
            assert len(request_records) == 2
            for record in request_records:
                assert (
                    record["identity"]["job_id"] == "job-" + result["conversation_id"]
                )
                assert (
                    record["identity"]["line_id"] == "line-" + result["conversation_id"]
                )
            events = [
                json.loads(line)
                for line in (directory / "events.jsonl").read_text().splitlines()
            ]
            executed = [
                event["tool_name"] for event in events if event["event"] == "tool_end"
            ]
            assert executed == ([] if index == 1 else ["search", reader])
            assert len(feedback.feedbacks) == (4 if index == 2 else 2)
        assert len(requests) == 6
        observations = []
        for body in requests[1::2]:
            messages = body["messages"]
            tool_messages = [
                message for message in messages if message.get("role") == "tool"
            ]
            assert len(tool_messages) == 2
            assistant = next(
                message for message in messages if message.get("tool_calls")
            )
            assert [m["tool_call_id"] for m in tool_messages] == [
                c["id"] for c in assistant["tool_calls"]
            ]
            observations.append([message["content"][0] for message in tool_messages])
            if len(observations) == 2:
                assert all(
                    "FlowPilot reuse provenance" in message["content"][1]["text"]
                    for message in tool_messages
                )
        assert observations[0] == observations[1]
        assert observations[2] != observations[1]
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()
        asyncio.run(upstream.aclose())
