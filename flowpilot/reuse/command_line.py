"""Literal curl parser. Eligibility never asserts that a Shell is isolated."""

from __future__ import annotations

import ipaddress
import re
import shlex
from urllib.parse import urlsplit, urlunsplit

SAFE_FLAGS = frozenset(
    {
        "-4",
        "-6",
        "--compressed",
        "--fail",
        "--fail-with-body",
        "-s",
        "-S",
        "--silent",
        "--show-error",
    }
)


def normalize_url(value: str, *, public_only: bool = False) -> str | None:
    if not value or re.search(r"[\s\x00-\x1f\x7f\\]", value):
        return None
    if re.search(r"%(?![0-9a-fA-F]{2})", value):
        return None
    try:
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        if scheme not in {"http", "https"} or not parsed.hostname:
            return None
        if parsed.username is not None or parsed.password is not None:
            return None
        port = parsed.port
        host = parsed.hostname.encode("idna").decode("ascii").lower()
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if public_only:
            if address is not None and (not address.is_global or address.is_multicast):
                return None
            if host.rstrip(".") == "localhost" or host.endswith(".localhost"):
                return None
            # curl also accepts non-standard numeric IPv4 forms.
            if address is None and re.fullmatch(r"[0-9xXa-fA-F.]+", host):
                return None
        netloc = f"[{host}]" if ":" in host else host
        if port is not None and (scheme, port) not in {("http", 80), ("https", 443)}:
            netloc += f":{port}"
        # Preserve path bytes; the executor uses --path-as-is.
        return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, ""))
    except (ValueError, UnicodeError):
        return None


def normalize_curl_url_command(
    arguments: dict[str, object],
) -> dict[str, object] | None:
    if set(arguments) - {"command", "is_input", "reset", "timeout"}:
        return None
    if (
        arguments.get("is_input", False) is not False
        or arguments.get("reset", False) is not False
    ):
        return None
    command = arguments.get("command")
    if not isinstance(command, str) or re.search(
        r"[\x00-\x1f\x7f\\$\x60;|<>*{}~]", command
    ):
        return None
    if "&&" in command or re.search(r"\s&(?:\s|$)", command):
        return None
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    if not argv or argv[0] != "curl":
        return None
    flags: set[str] = set()
    urls: list[str] = []
    for item in argv[1:]:
        if item in SAFE_FLAGS:
            flags.add(item)
        elif item.startswith("-"):
            return None
        else:
            urls.append(item)
    if len(urls) != 1 or {"-4", "-6"} <= flags:
        return None
    url = normalize_url(urls[0], public_only=True)
    if url is None:
        return None
    return {
        "command_line_adapter": "curl_url_exact_v1",
        "url": url,
        "flags": sorted(flags),
    }
