from __future__ import annotations

import asyncio
import hashlib
import importlib
import math
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Protocol


class SemanticEmbedder(Protocol):
    @property
    def index_id(self) -> str: ...
    @property
    def dimension(self) -> int: ...
    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]: ...


@dataclass
class Qwen3Embedding:
    model_path: str = "/docker/data/HF_MODELS/Qwen3-Embedding-0.6B"
    dimension: int = 1024
    model_id: str = "qwen3-embedding-0.6b"
    instruction_version: str = "web-query-equivalence-v1"
    _model: Any = field(default=None, init=False, repr=False)
    _tokenizer: Any = field(default=None, init=False, repr=False)
    _pending: asyncio.Task | None = field(default=None, init=False, repr=False)
    _pending_texts: list[str] = field(default_factory=list, init=False, repr=False)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    @property
    def index_id(self) -> str:
        return (
            f"{self.model_id}:{self.dimension}:L2:{self.instruction_version}:"
            "last-token-v1:8192:local-v2"
        )

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        # Queue coroutines, not native jobs: cancellation removes a waiter while
        # an already running encoder remains the single owner of the model.
        while self._pending is not None and not self._pending.done():
            if texts == self._pending_texts:
                return await asyncio.shield(self._pending)
            await asyncio.wait((self._pending,))
        if self._pending is not None:
            self._pending.exception()  # consume a detached worker failure
        self._pending_texts = list(texts)
        self._pending = asyncio.create_task(asyncio.to_thread(self._encode, texts))
        return await asyncio.shield(self._pending)

    def _encode(self, texts: list[str]) -> list[tuple[float, ...]]:
        with self._lock:
            if self._model is None:
                transformers = importlib.import_module("transformers")
                self._tokenizer = transformers.AutoTokenizer.from_pretrained(
                    self.model_path,
                    local_files_only=True,
                    trust_remote_code=False,
                    padding_side="left",
                )
                model = transformers.AutoModel.from_pretrained(
                    self.model_path,
                    local_files_only=True,
                    trust_remote_code=False,
                ).eval()
                if model.config.hidden_size != self.dimension or self.dimension != 1024:
                    raise ValueError("Qwen3 embedding dimension mismatch")
                if model.config.model_type != "qwen3":
                    raise ValueError("Qwen3 model metadata mismatch")
                self._model = model
            torch = importlib.import_module("torch")
            inputs = [
                "Instruct: Represent this web search query for semantic equivalence."
                + "\nQuery:"
                + text
                for text in texts
            ]
            batch = self._tokenizer(
                inputs,
                padding=True,
                truncation=False,
                return_tensors="pt",
            )
            if batch["input_ids"].shape[1] > 8192:
                raise ValueError("semantic input exceeds versioned token budget")
            with torch.inference_mode():
                output = self._model(**batch).last_hidden_state[:, -1]
                values = torch.nn.functional.normalize(output, p=2, dim=1).tolist()
            return [tuple(float(v) for v in row) for row in values]


@dataclass(frozen=True)
class TestHashingEmbedder:
    """Explicit test injection only; never a production fallback."""

    __test__ = False
    dimension: int = 384

    @property
    def index_id(self) -> str:
        return f"test-hashing-v2:{self.dimension}"

    async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
        return [self._one(text) for text in texts]

    def _one(self, text: str) -> tuple[float, ...]:
        vector = [0.0] * self.dimension
        for token in re.findall(r"[\w]+", text.casefold()):
            value = int.from_bytes(
                hashlib.blake2b(token.encode(), digest_size=8).digest(), "big"
            )
            vector[value % self.dimension] += 1 if value >> 63 else -1
        norm = math.sqrt(sum(v * v for v in vector))
        return tuple(v / norm for v in vector) if norm else tuple(vector)


def cosine_similarity(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if not left or len(left) != len(right):
        return 0.0
    return max(-1.0, min(1.0, sum(a * b for a, b in zip(left, right, strict=True))))
