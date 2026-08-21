from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from typing import Protocol

_TOKEN_RE = re.compile(r"[\w]+|[./:@?&=+-]", re.UNICODE)
_CJK_RE = re.compile(r"[\u3400-\u9fff]")


class SemanticEmbedder(Protocol):
    """Versioned embedder used by both historical and in-flight indexes."""

    @property
    def index_id(self) -> str: ...

    def embed(self, text: str) -> tuple[float, ...]: ...


@dataclass(frozen=True, slots=True)
class HashingEmbedder:
    """Dependency-free deterministic embedder for local validation.

    Deployments should inject a calibrated production embedder. The index ID
    prevents vectors from different models or dimensions from being compared.
    """

    dimensions: int = 384

    def __post_init__(self) -> None:
        if self.dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")

    @property
    def index_id(self) -> str:
        return f"flowpilot-hashing-v1-{self.dimensions}"

    def embed(self, text: str) -> tuple[float, ...]:
        vector = [0.0] * self.dimensions
        tokens = _tokens(text)
        features = [
            *tokens,
            *(f"{a}::{b}" for a, b in zip(tokens, tokens[1:], strict=False)),
        ]
        for feature in features:
            digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
            number = int.from_bytes(digest, "big")
            index = number % self.dimensions
            sign = 1.0 if (number >> 63) == 0 else -1.0
            vector[index] += sign
        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0:
            return tuple(vector)
        return tuple(value / norm for value in vector)


def cosine_similarity(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    return sum(a * b for a, b in zip(left, right, strict=True))


def _tokens(text: str) -> list[str]:
    lowered = text.casefold()
    tokens = _TOKEN_RE.findall(lowered)
    cjk = _CJK_RE.findall(lowered)
    tokens.extend(cjk)
    tokens.extend(a + b for a, b in zip(cjk, cjk[1:], strict=False))
    return tokens
