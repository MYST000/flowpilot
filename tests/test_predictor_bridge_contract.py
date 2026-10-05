"""Optional real predictor bridge against gateway, resolution and retention stores.

Run with flowpilot_predictor and its pinned ML dependencies on PYTHONPATH.
"""

from __future__ import annotations

import asyncio
import json
import threading

import pytest
import test_response_retention as retention_tests
from test_gateway import _headers

bridge_module = pytest.importorskip("flowpilot_predictor_bridge.framework")


def predictor_context(replica_id):
    return json.dumps(
        {
            "schema_version": 1,
            "benchmark": "fixture",
            "dataset_revision": "fixture-v1",
            "replica_id": replica_id,
            "environment_tools": ["web_search"],
            "features": {
                "tool_execution_profile": {"tool_timeout_seconds": 120},
                "environment_known_at_t0": {"backend": "fixture"},
                "prior_tool_executions": [],
                "budget_at_t0": {"remaining_seconds": 120},
            },
        }
    )


class GatedModel:
    name = "fixture"
    version = "fixture-v1"

    def __init__(self, fail_second):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.fail_second = fail_second

    def __deepcopy__(self, memo):
        return self

    def predict(self, context):
        assert context["backend_version"] == bridge_module.backend_signature(
            {"backend": "fixture"}, "fixture-v1", {"tool_timeout_seconds": 120}
        )
        self.entered.set()
        if not self.release.wait(5):
            raise TimeoutError("fixture gate was not released")
        if self.fail_second and context["batch_index"] == 1:
            raise ValueError("fixture second prediction failed")
        return {
            "duration_ms": dict(q10=30.0, q50=50.0, q90=90.0, q99=99.0),
            "support": {"status": "supported"},
            "fallback": {"used": False, "reason": None},
        }


@pytest.mark.parametrize("api_kind", ["chat", "responses"])
@pytest.mark.parametrize("fail_second", [False, True])
@pytest.mark.parametrize("replica_id", ["stock-model", "flowpilot-model"])
async def test_real_bridge_collects_writes_before_retention(
    tmp_path, monkeypatch, api_kind, fail_second, replica_id
):
    model = GatedModel(fail_second)
    bridge = bridge_module.FrameworkPredictor(
        bridge_module.PredictorRuntime(model, timeout_seconds=3)
    )
    monkeypatch.setattr(retention_tests, "Predictor", lambda *_args: bridge)
    async with retention_tests.scenario(
        tmp_path, durations={"a": 50, "b": 50}, api_kind=api_kind
    ) as (app, engine, cache, _, sink):
        try:
            cache.release.set()
            headers = _headers()
            headers["x-flowpilot-predictor-context"] = predictor_context(replica_id)
            headers["x-flowpilot-reuse-policy"] = json.dumps(
                {
                    "allowed_tool_names": ["web_search"],
                    "deferred": False,
                    "policy_version": 1,
                    "expected_policy_version": 0,
                    "lease_id": "lease-1",
                    "lease_seconds": 60,
                    "max_messages": 20,
                    "max_bytes": 10000,
                    "max_internal_continuations": 3,
                    "delta_ttl_seconds": 60,
                }
            )
            payload = {
                "model": "m",
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "web_search",
                            "parameters": {"type": "object"},
                        },
                    }
                ],
                **(
                    {"messages": [{"role": "user", "content": "fixture"}]}
                    if api_kind == "chat"
                    else {"input": "fixture"}
                ),
            }
            response = await app.state.llm_gateway.proxy(
                path="/v1/chat/completions" if api_kind == "chat" else "/v1/responses",
                api_kind=api_kind,
                body=json.dumps(payload).encode(),
                headers=headers,
                raw_query=b"",
            )
            assert response.status_code == 200
            assert await asyncio.to_thread(model.entered.wait, 1)
            await asyncio.wait_for(engine.query_started.wait(), 1)
            retention = app.state.scheduling.retention
            await retention.refresh()
            assert not engine.commands
            assert (await app.state.scheduling.queue.snapshot())["inflight"] == 0
        finally:
            model.release.set()
        await asyncio.wait_for(asyncio.gather(*retention._tasks), 2)
        decisions = retention_tests.decisions(sink)
        assert len(decisions) == len(engine.commands) == 1
        assert decisions[0]["prediction_failed"] is fail_second
        if fail_second:
            assert decisions[0]["tool_gap_seconds"] is None
            assert engine.commands[0]["action"] == "OFFLOAD"
        else:
            assert 0 <= decisions[0]["tool_gap_seconds"] <= 0.1
            assert bridge.metrics["scheduler_handoffs"] == 2
            assert engine.commands[0]["action"] == "KEEP"
