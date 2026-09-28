"""Scheduler-side URL families carried by the ordinary OpenHands terminal Tool.

This module never executes or rewrites commands. The registry's deployment and
policy scope must describe compatible executables, configuration and environment.
"""

from __future__ import annotations

import math
import re
import shlex
from typing import Any
from urllib.parse import urlsplit

from flowpilot.reuse.command_line import normalize_url
from flowpilot.reuse.contracts import canonical_json

_CURL_FLAGS = {
    "s": "--silent",
    "S": "--show-error",
    "f": "--fail",
    "L": "--location",
    "I": "--head",
    "i": "--include",
    "g": "--globoff",
    "G": "--get",
    "q": "--disable",
    "4": "--ipv4",
    "6": "--ipv6",
}
_CURL_LONG_FLAGS = set(_CURL_FLAGS.values()) | {
    "--compressed",
    "--fail-with-body",
    "--path-as-is",
}
_CURL_VALUES = {
    "X": "--request",
    "o": "--output",
    "m": "--max-time",
}
_CURL_LONG_VALUES = set(_CURL_VALUES.values()) | {
    "--url",
    "--connect-timeout",
    "--max-redirs",
}
_WGET_FLAGS = {"q": "--quiet", "4": "--inet4-only", "6": "--inet6-only"}
_WGET_LONG_FLAGS = set(_WGET_FLAGS.values()) | {
    "--no-verbose",
    "--no-config",
    "--no-hsts",
    "--content-on-error",
}
_WGET_VALUES = {"O": "--output-document", "T": "--timeout", "t": "--tries"}
_WGET_LONG_VALUES = set(_WGET_VALUES.values()) | {
    "--connect-timeout",
    "--read-timeout",
    "--dns-timeout",
    "--max-redirect",
}


def terminal_input(arguments: dict[str, Any]) -> dict[str, Any]:
    """Match TerminalAction's serialized defaults, without parsing its command."""
    result = {"is_input": False, "reset": False, "timeout": None, **arguments}
    if result["timeout"] is not None:
        result["timeout"] = float(result["timeout"])
    return result


def _literal_argv(command: str) -> list[str] | None:
    # Preserve shell quoting. In particular an unquoted '&' in a URL starts a
    # background command, whereas the same character inside quotes is URL data.
    quote = ""
    for char in command:
        if ord(char) < 32 or ord(char) == 127 or char in "\\$`":
            return None
        if quote:
            if quote == '"' and char == "!":
                return None
            if char == quote:
                quote = ""
        elif char in "\"'":
            quote = char
        elif char in ";&|<>()*?[]{}~#!":
            return None
    if quote:
        return None
    try:
        return shlex.split(command)
    except ValueError:
        return None


def normalize_terminal_url_command(arguments: dict[str, Any]) -> dict[str, Any] | None:
    if set(arguments) - {"command", "is_input", "reset", "timeout"}:
        return None
    if (
        arguments.get("is_input", False) is not False
        or arguments.get("reset", False) is not False
    ):
        return None
    timeout = arguments.get("timeout")
    if timeout is not None and (
        type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout < 0
    ):
        return None
    command = arguments.get("command")
    if not isinstance(command, str):
        return None
    argv = _literal_argv(command)
    if not argv or argv[0] not in {
        "curl",
        "wget",
        "/usr/bin/curl",
        "/usr/bin/wget",
        "/bin/curl",
        "/bin/wget",
    }:
        return None
    executable = argv[0]
    family = executable.rsplit("/", 1)[-1]
    short_flags = _CURL_FLAGS if family == "curl" else _WGET_FLAGS
    short_values = _CURL_VALUES if family == "curl" else _WGET_VALUES
    long_flags = _CURL_LONG_FLAGS if family == "curl" else _WGET_LONG_FLAGS
    long_values = _CURL_LONG_VALUES if family == "curl" else _WGET_LONG_VALUES
    canonical: list[str] = [executable]
    urls: list[str] = []
    stdout = family == "curl"
    positional = False
    index = 1
    while index < len(argv):
        item = argv[index]
        index += 1
        options: list[tuple[str, str | None]] = []
        if item == "--" and not positional:
            positional = True
            canonical.append(item)
            continue
        if positional or not item.startswith("-"):
            options.append(("--url", item))
        elif item.startswith("--"):
            option, sep, value = item.partition("=")
            if option in long_flags and not sep:
                options.append((option, None))
            elif option in long_values:
                if not sep:
                    if index == len(argv):
                        return None
                    value = argv[index]
                    index += 1
                options.append((option, value))
            else:
                return None
        else:
            letters = item[1:]
            if not letters:
                return None
            while letters:
                char, letters = letters[0], letters[1:]
                if char in short_flags:
                    options.append((short_flags[char], None))
                elif char in short_values:
                    if letters:
                        value, letters = letters, ""
                    else:
                        if index == len(argv):
                            return None
                        value = argv[index]
                        index += 1
                    options.append((short_values[char], value))
                else:
                    return None
        for option, value in options:
            if option == "--url":
                if value is None:
                    return None
                url = normalize_url(value)
                if url is None:
                    return None
                urls.append(url)
                canonical.append(url)
            else:
                if option in {"--output", "--output-document"}:
                    if value != "-":
                        return None
                    stdout = True
                elif option == "--request":
                    if value not in {"GET", "HEAD"}:
                        return None
                elif value is not None and not re.fullmatch(r"\d+(?:\.\d+)?", value):
                    return None
                canonical.append(option)
                if value is not None:
                    canonical.append(value)
    if len(urls) != 1 or not stdout:
        return None
    # curl expands ranges even inside shell quotes; IPv6 host brackets are
    # ordinary address syntax. wget has no corresponding URL glob expansion.
    parsed_url = urlsplit(urls[0])
    if (
        family == "curl"
        and "--globoff" not in canonical
        and re.search(r"[{}\[\]]", parsed_url.path + parsed_url.query)
    ):
        return None
    return {
        "executable_family": family,
        "argv": canonical,
        "url": urls[0],
        "timeout": float(timeout) if timeout is not None else None,
    }


class TerminalUrlFetchAdapter:
    adapter_id = "terminal_url_fetch_v1"
    adapter_version = "1"
    executor_kind = "openhands_local"

    def parse_tool_call(
        self, tool_name: str, arguments: dict[str, Any]
    ) -> dict[str, Any] | None:
        return (
            normalize_terminal_url_command(arguments)
            if tool_name == "terminal"
            else None
        )

    def canonicalize_arguments(self, parsed: dict[str, Any]) -> dict[str, Any]:
        return dict(parsed)

    def build_semantic_text(self, canonical: dict[str, Any]) -> None:
        return None

    def validate_result(self, observation: Any, execution_receipt: Any = None) -> bool:
        if not isinstance(observation, dict) or execution_receipt is None:
            return False
        content = observation.get("content")
        metadata = observation.get("metadata")
        return (
            observation.get("kind") == "TerminalObservation"
            and observation.get("is_error") is False
            and type(observation.get("exit_code")) is int
            and observation["exit_code"] == 0
            and observation.get("timeout") is False
            and isinstance(observation.get("command"), str)
            and normalize_terminal_url_command({"command": observation["command"]})
            is not None
            and isinstance(metadata, dict)
            and metadata.get("exit_code") == 0
            and isinstance(content, list)
            and bool(content)
            and all(
                isinstance(v, dict)
                and v.get("type") == "text"
                and isinstance(v.get("text"), str)
                for v in content
            )
        )

    def adapt_result(
        self, observation: dict[str, Any], output_budget: int | None = None
    ) -> dict[str, Any]:
        # A reused result is not an observation of the follower's shell state.
        result = {
            **observation,
            "metadata": {"exit_code": 0},
            "full_output_save_dir": None,
        }
        if (
            output_budget is not None
            and len(canonical_json(result).encode()) > output_budget
        ):
            raise ValueError("budget_exceeded")
        return result
