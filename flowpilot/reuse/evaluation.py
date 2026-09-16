"""Offline labelled query evaluation; never writes an origin or enables reuse."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any

from .contracts import digest
from .semantic import Qwen3Embedding, SemanticEmbedder, cosine_similarity


async def evaluate(
    samples: list[dict[str, Any]],
    embedder: SemanticEmbedder,
    *,
    k: int = 1,
    threshold: float = 0.97,
) -> dict[str, Any]:
    if not samples or k < 1:
        raise ValueError("nonempty labelled samples and positive k required")
    texts = list(
        dict.fromkeys(
            text
            for sample in samples
            for text in (
                sample["query"],
                *(item["query"] for item in sample["candidates"]),
            )
        )
    )
    started = time.perf_counter()
    vectors = dict(zip(texts, await embedder.embed(texts), strict=True))
    latency = time.perf_counter() - started
    recall = precision = reciprocal_rank = 0.0
    accepted = false_reuse = 0
    for sample in samples:
        candidates = sorted(
            (
                (
                    cosine_similarity(vectors[sample["query"]], vectors[item["query"]]),
                    number,
                    bool(item["equivalent"]),
                )
                for number, item in enumerate(sample["candidates"])
            ),
            key=lambda item: (-item[0], item[1]),
        )
        positives = sum(item[2] for item in candidates)
        top = candidates[:k]
        recall += sum(item[2] for item in top) / positives if positives else 0
        precision += sum(item[2] for item in top) / len(top) if top else 0
        reciprocal_rank += next(
            (1 / rank for rank, item in enumerate(candidates, 1) if item[2]), 0
        )
        if candidates and candidates[0][0] >= threshold:
            accepted += 1
            false_reuse += not candidates[0][2]
    return {
        "dataset_digest": digest(samples),
        "sample_count": len(samples),
        "index_id": embedder.index_id,
        "k": k,
        "threshold": threshold,
        "recall_at_k": recall / len(samples),
        "precision_at_k": precision / len(samples),
        "mrr": reciprocal_rank / len(samples),
        "accepted": accepted,
        "false_reuse_count": false_reuse,
        "false_reuse_rate": false_reuse / accepted if accepted else None,
        "embedding_batch_seconds": round(latency, 6),
        "production_gate_passed": False,
        "note": (
            "Offline labels only; no origin, freshness or upstream "
            "completeness is inferred."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--model-path", default=Qwen3Embedding.model_path)
    parser.add_argument("--k", type=int, default=1)
    parser.add_argument("--threshold", type=float, default=0.97)
    args = parser.parse_args()
    samples = json.loads(args.dataset.read_text())
    print(
        json.dumps(
            asyncio.run(
                evaluate(
                    samples,
                    Qwen3Embedding(model_path=args.model_path),
                    k=args.k,
                    threshold=args.threshold,
                )
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
