"""Launch the Qwen3.5-27B TP=4 / CPU KV 64 GiB experiment profile."""

from pathlib import Path

from examples.experiments.qwen35_9b_tp4.launch import main as launch_profile

CONFIG_PATH = Path(__file__).with_name("config.json")


def main() -> None:
    launch_profile(default_config=CONFIG_PATH)


if __name__ == "__main__":
    main()
