"""Explicit service launch or configuration-only validation in each owner's venv."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from .profile import (
    CONFIG_PATH,
    gateway_settings,
    load_profile,
    openhands_options,
    vllm_arguments,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("component", choices=("vllm", "gateway", "openhands"))
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--check", action="store_true", help="Validate without serving")
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument(
        "--registry", type=Path, help="Current BrowseComp registry export"
    )
    parser.add_argument("--cost-model", type=Path)
    args = parser.parse_args()
    profile = load_profile(args.config)
    if args.component == "vllm":
        os.environ.update(profile["vllm"]["environment"])
        engine_args = vllm_arguments(profile)
        if args.check:
            print("vLLM arguments validated; no engine or GPU workload started")
            return
        from vllm.entrypoints.launchers.api_server.entry import run_server

        asyncio.run(run_server(engine_args))
    elif args.component == "gateway":
        if args.run_dir is None or args.registry is None:
            parser.error("gateway requires --run-dir and --registry")
        cost_value = os.getenv("FLOWPILOT_COST_MODEL_PATH")
        cost_path = args.cost_model or (Path(cost_value) if cost_value else None)
        settings = gateway_settings(
            profile,
            run_dir=args.run_dir.resolve(),
            registry_path=args.registry,
            api_key=os.environ["FLOWPILOT_INGRESS_API_KEY"],
            dcs_key=os.environ["FLOWPILOT_DCS_ENCRYPTION_KEY"],
            cost_model_path=cost_path,
        )
        print(
            "Cost model:",
            settings.admission.cost_model.version
            if settings.admission.cost_model
            else "unknown:no_calibration (deadline-only / retention fallback)",
        )
        if args.check:
            print("FlowPilot settings validated; no service or database opened")
            return
        import uvicorn

        from flowpilot.app import create_app

        args.run_dir.mkdir(parents=True, exist_ok=True)
        uvicorn.run(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            workers=settings.workers,
        )
    else:
        if not args.check:
            parser.error(
                "openhands supports --check; the benchmark runner owns real Tools"
            )
        openhands_options(
            profile,
            api_key=os.environ["FLOWPILOT_INGRESS_API_KEY"],
            job_id="config-validation-job",
            line_id="config-validation-line",
            root_conversation_id="config-validation-conversation",
            seed=profile["workload"]["seeds"][0],
        )
        print("OpenHands LLM and adapter validated; no conversation or request started")


if __name__ == "__main__":
    main()
