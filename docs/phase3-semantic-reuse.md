# Phase 3 Conservative Semantic Reuse

Phase 3 extends the Phase 1/2 reuse control plane without moving Tool execution
out of OpenHands. It is default-off at both ends: the FlowPilot registry entry
must enable semantic reuse and the OpenHands conversation must set
`semantic_reuse_enabled=True`. The client then uses
`flowpilot-phase3-reuse-v2`; Phase 1 clients remain exact-only.

## Matching Contract

Resolution order is fixed:

1. exact historical result;
2. semantic historical result;
3. exact in-flight binding;
4. semantic in-flight binding;
5. new local leader.

Before embedding comparison, FlowPilot filters candidates by canonical Tool
family, Tool and result schema versions, locale, language, region, safe-search
policy, time-sensitivity class, data-source constraints, the Tool's declared
reuse policy, and every argument outside `semantic_query_fields`. Query content
is not a private/public partition. Historical candidates must also be fresh.

Each Tool registry entry owns its threshold, candidate limit, query fields, and
allowed time-sensitivity classes. Queries containing common current/latest,
price, or weather terms are excluded from semantic reuse even when the caller
incorrectly labels them `standard`. Arguments containing credential-like fields
or text are also excluded. Exact behavior is unchanged.

Example registry entry:

```json
{
  "protocol_version": "flowpilot-phase3-reuse-v2",
  "tool_name": "web_search",
  "canonical_tool_family": "public_web_search",
  "tool_version": "1",
  "result_schema_version": "1",
  "semantic_reuse_enabled": true,
  "semantic_query_fields": ["query"],
  "semantic_similarity_threshold": 0.94,
  "semantic_candidate_limit": 100
}
```

The built-in versioned hashing embedder is dependency-free and deterministic;
it exists for local protocol and threshold tests, not as production-quality
semantic evidence. `create_app(..., semantic_embedder=...)` accepts a calibrated
implementation. An embedding index ID is stored with every vector, and vectors
from different implementations or configurations are never compared.

## Lifecycle And Failure

Historical lookup remains ahead of the in-flight table. Under the controller
lock, an in-flight semantic candidate is selected or a new leader is registered,
so concurrent misses cannot create two leaders through a check/register race.
Persisted candidates are treated as untrusted: descriptor and query digests,
hard-scope partition, schema, embedding metadata, freshness, result shape/size,
and sensitive fields are revalidated before a result can become an Observation.
Invalid rows are deleted and counted as corrupt rejections.
Leader failure or lease expiry releases followers to retry resolution. Follower
cancellation removes only that follower. Progress updates are monotonic and
idempotent; they never extend a lease, and estimates are informational rather
than authority to duplicate execution.

Phase 2 DCS receipts are exact-only (`exact_historical` or `exact_inflight`)
and remain bound to the current line's own Tool Call identity and arguments
digest. Semantic results are delivered through the ordinary reuse control
plane; deferred semantic continuation requires a separately versioned DCS
contract. Provider-visible provenance
contains only reuse type, match kind, observation time, and result schema
version. Similarity, threshold, match ID, source query digest, binding ID, and
scope stay in the control plane.

## Audit And Kill Switches

Every accepted semantic match gets a stable match ID and a metadata-only SQLite
audit row containing query digests, source ID, hard-scope digest, score,
threshold, source type, and timestamps. It does not store prompts or Tool
results. Retries of the same request/source tuple reuse the same match ID.
Agents or evaluators can submit idempotent false-reuse feedback to:

```text
POST /flowpilot/v1/reuse/semantic/false-reuse
```

`GET /flowpilot/v1/reuse` reports exact/semantic historical and in-flight
matches, hard-constraint/stale/threshold rejections, leader progress, and
false-reuse counts. `PUT /flowpilot/v1/reuse/semantic/policy` atomically updates
a versioned runtime kill switch for one Tool family. Disable a Tool by registry
at startup or use the runtime API for immediate rollback.

## Evidence Status

Implementation and local mock verification do not authorize production enablement.
Production remains NO-GO until E07/E08 provide representative labeled queries,
per-family frozen thresholds, an independent test set, zero hard-constraint
violations, acceptable false/stale reuse bounds, and evidence from the chosen
production embedding implementation. KV telemetry and Phase 4/5 scheduling are
still unsupported.

Phase 2 DCS remains exact-only. Phase 3 semantic results are returned through
the ordinary reuse control plane; extending deferred semantic continuation
requires a separately versioned DCS contract and OpenHands integration.
