"""Read-only engine observations for the local vLLM fork, not serving policy.

All timestamps use this host's monotonic clock. No token content is persisted.
Transfer latency ends when the scheduler observes completion from every rank;
it includes native dispatch/polling delay, unlike worker CUDA-event time.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import cast

from vllm.distributed.kv_transfer.kv_connector.v1.offloading.common import (
    OffloadingConnectorMetadata,
    OffloadingWorkerMetadata,
)
from vllm.v1.core.sched.async_scheduler import AsyncScheduler
from vllm.v1.kv_offload.cpu.gpu_worker import SingleDirectionOffloadingHandler
from vllm.v1.worker.gpu_worker import Worker


def record(kind, **values):
    path = Path(os.environ["FLOWPILOT_COST_EVIDENCE"]) / f"engine-{os.getpid()}.jsonl"
    with path.open("a") as stream:
        stream.write(json.dumps({"kind": kind, **values}) + "\n")


class ObservedWorker(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        original = SingleDirectionOffloadingHandler.get_finished
        rank = self.rank

        def get_finished(self):
            results = original(self)
            now = time.monotonic()
            for result in results:
                record(
                    "worker_transfer",
                    rank=rank,
                    direction="offload" if self.gpu_to_cpu else "restore",
                    observed_at=now,
                    **asdict(result),
                )
            return results

        SingleDirectionOffloadingHandler.get_finished = get_finished


class ObservedScheduler(AsyncScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert self.scheduler_config.async_scheduling, "Keep native async scheduling"
        self.cost_requests = {}
        self.cost_jobs = {}
        self.cost_workers = self.parallel_config.world_size
        record("observer_config", workers=self.cost_workers, async_scheduling=True)

    def schedule(self, *args, **kwargs):
        started = time.monotonic()
        output = super().schedule(*args, **kwargs)
        for request_id in output.num_scheduled_tokens:
            if request_id not in self.cost_requests:
                request = self.requests[request_id]
                binding = (request.kv_transfer_params or {}).get(
                    "kv_control_binding", {}
                )
                self.cost_requests[request_id] = {
                    "started": started,
                    "binding": binding,
                    "prompt_tokens": request.num_prompt_tokens,
                }
        meta = cast(OffloadingConnectorMetadata | None, output.kv_connector_metadata)
        if meta is not None:
            for direction, jobs in (
                ("offload", meta.store_jobs),
                ("restore", meta.load_jobs),
            ):
                for job_id, job in jobs.items():
                    assert job_id not in self.cost_jobs
                    self.cost_jobs[job_id] = {
                        "direction": direction,
                        "engine_request_id": job.req_id,
                        "started": started,
                        "completed_workers": 0,
                    }
        return output

    def update_from_output(self, scheduler_output, model_runner_output):
        connector_output = model_runner_output.kv_connector_output
        if connector_output and connector_output.kv_connector_worker_meta:
            meta = cast(
                OffloadingWorkerMetadata, connector_output.kv_connector_worker_meta
            )
            if meta.failed_jobs:
                record("transfer_failure", jobs=meta.failed_jobs)
            for job_id, count in meta.completed_jobs.items():
                job = self.cost_jobs[job_id]
                job["completed_workers"] += count
                if job["completed_workers"] == self.cost_workers:
                    ended = time.monotonic()
                    record(
                        "engine_transfer",
                        job_id=job_id,
                        ended=ended,
                        seconds=ended - job["started"],
                        **job,
                    )
                    del self.cost_jobs[job_id]
        outputs = super().update_from_output(scheduler_output, model_runner_output)
        ended = time.monotonic()
        for batch in outputs.values():
            for output in batch.outputs:
                if output.new_token_ids and output.prefill_stats is not None:
                    request = self.cost_requests.pop(output.request_id)
                    record(
                        "prefill",
                        engine_request_id=output.request_id,
                        ended=ended,
                        seconds=ended - request["started"],
                        stats=asdict(output.prefill_stats),
                        **request,
                    )
        return outputs
