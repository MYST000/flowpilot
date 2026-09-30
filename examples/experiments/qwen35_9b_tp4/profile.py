"""Load the frozen experiment settings without starting any service or Tool."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = Path(__file__).with_name("config.json")


def load_profile(path: Path = CONFIG_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def service_url(profile: dict[str, Any], component: str) -> str:
    settings = profile["vllm"]["args"] if component == "vllm" else profile["flowpilot"]
    return f"http://{settings['host']}:{settings['port']}"


def vllm_arguments(profile: dict[str, Any]) -> argparse.Namespace:
    # vLLM lives in a separate environment; import only for engine validation/launch.
    from vllm.entrypoints.launchers.cli_args import make_arg_parser
    from vllm.utils.argparse_utils import FlexibleArgumentParser

    options = []
    for name, value in profile["vllm"]["args"].items():
        flag = "--" + name.replace("_", "-")
        if isinstance(value, bool):
            options.append(flag if value else "--no-" + name.replace("_", "-"))
        else:
            if name == "chat_template":
                value = str(ROOT / value)
            options.extend(
                [flag, json.dumps(value) if isinstance(value, dict) else str(value)]
            )
    args = cast(
        argparse.Namespace,
        make_arg_parser(FlexibleArgumentParser()).parse_args(options),
    )
    for name, value in profile["vllm"]["namespace"].items():
        getattr(args, name)  # Reject misspelled Namespace fields.
        setattr(args, name, value)
    return args


def gateway_settings(
    profile: dict[str, Any],
    *,
    run_dir: Path,
    registry_path: Path,
    api_key: str,
    dcs_key: str,
    cost_model_path: Path | None = None,
) -> Any:
    from cryptography.fernet import Fernet

    from flowpilot.config import InferenceInstance, Settings
    from flowpilot.protocol import ToolRegistryEntry
    from flowpilot.scheduling.admission import AdmissionConfig
    from flowpilot.scheduling.cost import OfflineCostModel
    from flowpilot.scheduling.retention import RetentionConfig

    settings = dict(profile["flowpilot"])
    admission = dict(settings.pop("admission"))
    retention = RetentionConfig.model_validate(settings.pop("retention"))
    if cost_model_path is None and (value := profile["workload"]["cost_model_path"]):
        cost_model_path = ROOT / value
    if cost_model_path is not None:
        admission["cost_model"] = OfflineCostModel.model_validate_json(
            cost_model_path.read_text()
        )
    if settings["dcs_enabled"]:
        Fernet(dcs_key.encode())
    allowed = profile["openhands"]["flowpilot"]["reusable_web_tools"]
    exported = [
        row
        for row in json.loads(registry_path.read_text())
        if row["tool_name"] in allowed
    ]
    if any(not row["adapter_id"].startswith("benchmark_") for row in exported):
        raise ValueError("Export registry with benchmark_adapters.reuse_profile")
    registry = tuple(
        ToolRegistryEntry.model_validate(
            {
                **row,
                **profile["reuse_policy"],
                "semantic_reuse_enabled": (
                    row["semantic_reuse_enabled"]
                    and profile["reuse_policy"]["semantic_reuse_enabled"]
                ),
            }
        )
        for row in exported
    )
    return Settings(
        **settings,
        instances=(InferenceInstance("qwen35-tp4", service_url(profile, "vllm")),),
        ingress_api_key=api_key,
        dcs_encryption_key=dcs_key,
        trace_path=run_dir / "gateway.jsonl",
        reuse_cache_path=run_dir / "reuse-v4.sqlite",
        dcs_wal_path=run_dir / "dcs-v4.sqlite",
        web_tool_registry=registry,
        admission=AdmissionConfig.model_validate(admission),
        retention=retention,
    )


def openhands_options(
    profile: dict[str, Any],
    *,
    api_key: str,
    job_id: str,
    line_id: str,
    root_conversation_id: str,
    seed: int,
) -> dict[str, Any]:
    from openhands.sdk.flowpilot import FlowPilotConfig
    from openhands.sdk.llm import LLM
    from pydantic import SecretStr

    options = profile["openhands"]
    gateway = service_url(profile, "flowpilot")
    adapter = FlowPilotConfig(
        **{
            **options["flowpilot"],
            "reusable_web_tools": tuple(options["flowpilot"]["reusable_web_tools"]),
        },
        gateway_url=gateway,
        api_key=api_key,
        job_id=job_id,
        line_id=line_id,
        root_conversation_id=root_conversation_id,
        deployment_id=profile["flowpilot"]["reuse_deployment_id"],
        namespace_id=profile["flowpilot"]["reuse_default_namespace"],
    )
    adapter.validate(tool_concurrency_limit=options["agent"]["tool_concurrency_limit"])
    return {
        "llm": LLM(
            **options["llm"],
            model="openai/" + profile["vllm"]["args"]["served_model_name"],
            base_url=gateway + "/v1",
            api_key=SecretStr(api_key),
            seed=seed,
        ),
        "agent": dict(options["agent"]),
        "conversation": {**options["conversation"], "flowpilot": adapter},
    }


def job_registration(
    profile: dict[str, Any],
    *,
    job_id: str,
    root_conversation_id: str,
    started_at: datetime,
    baseline_seconds: float,
) -> dict[str, Any]:
    """Build the initial registration; the runner sends it before SDK registration."""
    from flowpilot.protocol import JobRegistration

    return JobRegistration(
        job_id=job_id,
        root_conversation_id=root_conversation_id,
        deployment_id=profile["flowpilot"]["reuse_deployment_id"],
        namespace_id=profile["flowpilot"]["reuse_default_namespace"],
        workflow_started_at=started_at,
        deadline=started_at
        + timedelta(seconds=baseline_seconds * profile["workload"]["slo_multiplier"]),
    ).model_dump(mode="json")
