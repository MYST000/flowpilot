"""Trusted-origin and canonical digest helpers for Tool reuse."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_result_digest(result: Any) -> str:
    payload = json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def canonical_input_digest(arguments: Any) -> str:
    payload = json.dumps(
        arguments,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_publication_digest(result: Any, declared: str | None) -> bool:
    return declared is not None and canonical_result_digest(result) == declared
