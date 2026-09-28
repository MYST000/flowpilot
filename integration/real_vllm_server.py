"""Launch the local vLLM KV-control build for the real workflow smoke test."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from vllm.entrypoints.launchers.api_server.entry import run_server
from vllm.entrypoints.launchers.cli_args import make_arg_parser
from vllm.utils.argparse_utils import FlexibleArgumentParser


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18801)
    parser.add_argument("--model", default="/docker/data/HF_MODELS/Qwen3.5-9B")
    parser.add_argument("--gpu-blocks", type=int, default=64)
    parser.add_argument("--grace-ms", type=int, default=30_000)
    config = parser.parse_args()
    template = (
        Path(__file__).resolve().parents[1]
        / "examples/chat_templates/qwen3.5-preserve-prefix.jinja"
    )
    options = [
        "--model",
        config.model,
        "--served-model-name",
        "flowpilot-real",
        "--host",
        "127.0.0.1",
        "--port",
        str(config.port),
        "--tensor-parallel-size",
        "4",
        "--dtype",
        "bfloat16",
        "--max-model-len",
        "6144",
        "--max-num-seqs",
        "4",
        "--max-num-batched-tokens",
        "4096",
        "--gpu-memory-utilization",
        "0.5",
        "--num-gpu-blocks-override",
        str(config.gpu_blocks),
        "--chat-template",
        str(template),
        "--enforce-eager",
        "--enable-prefix-caching",
        "--mamba-cache-mode",
        "align",
        "--language-model-only",
        "--enable-auto-tool-choice",
        "--tool-call-parser",
        "qwen3_coder",
        "--reasoning-parser",
        "qwen3",
        "--generation-config",
        "vllm",
        "--kv-transfer-config",
        json.dumps(
            {
                "kv_connector": "OffloadingConnector",
                "kv_role": "kv_both",
                "kv_connector_extra_config": {
                    "spec_name": "CPUOffloadingSpec",
                    "cpu_bytes_to_use": 1_073_741_824,
                    "store_threshold": 0,
                    "offload_prompt_only": False,
                },
            }
        ),
        "--additional-config",
        json.dumps(
            {
                "kv_control": {
                    "enabled": True,
                    "finish_grace_ttl_ms": config.grace_ms,
                    "metadata_ttl_seconds": 1800,
                }
            }
        ),
    ]
    args = make_arg_parser(FlexibleArgumentParser()).parse_args(options)
    args.prefix_cache_retention_interval = None
    args.enable_prompt_tokens_details = True
    args.enable_log_requests = False
    args.enable_log_outputs = False
    asyncio.run(run_server(args))


if __name__ == "__main__":
    main()
