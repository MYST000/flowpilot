"""Launch the frozen profile with read-only calibration observers."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from .profile import CONFIG_PATH, load_profile, vllm_arguments


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    profile = load_profile(args.config)
    os.environ.update(profile["vllm"]["environment"])
    os.environ["FLOWPILOT_COST_EVIDENCE"] = str(args.output.resolve())
    namespace = vllm_arguments(profile)
    module = "examples.experiments.qwen35_9b_tp4.cost_observer"
    namespace.scheduler_cls = module + ".ObservedScheduler"
    namespace.worker_cls = module + ".ObservedWorker"
    (args.output / "profile.json").write_text(json.dumps(profile, indent=2) + "\n")
    (args.output / "observer.json").write_text(
        json.dumps(
            {
                "scheduler_cls": namespace.scheduler_cls,
                "worker_cls": namespace.worker_cls,
                "measurement_basis": (
                    "isolated engine wall clock with native async dispatch; "
                    "transfer completion requires all workers; "
                    "CUDA-event worker times separate"
                ),
            },
            indent=2,
        )
        + "\n"
    )
    from vllm.entrypoints.launchers.api_server.entry import run_server

    asyncio.run(run_server(namespace))


if __name__ == "__main__":
    main()
