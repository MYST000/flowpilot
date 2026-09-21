# FlowPilot

The active baseline is the Phase 0 OpenAI-compatible gateway and measurement
plane described in `design.md`. It immediately delivers intact LLM responses,
keeps a minimal five-state `LineTail`, records every GatewayCall attempt to an
explicit terminal outcome, and keeps all real Tool execution in OpenHands. The
non-blocking Phase 4 scheduling loop adds forecast metadata, factual Tool
readiness records, and ephemeral SLO/DAG projections without changing that
ownership boundary.

The repository also contains default-off Phase 1 exact Web Tool reuse, Phase 2
deferred context synchronization (DCS), and Phase 3 conservative semantic reuse
modules. Their presence does not expand Phase 0 behavior.

The `phase*` documents are historical and may describe superseded behavior.
For the current Tavily/URL reuse work, use
[the execution plan](docs/tavily-url-tool-reuse-plan.md) and
[the implementation/verification report](docs/tavily-url-tool-reuse-verification.md).
In particular, ordinary Terminal execution is not a trusted URL reuse executor;
new reuse requires a separate `reuse-v4.sqlite` database and the updated SDK.

FlowPilot never executes Tools. With later phases explicitly enabled, an
explicitly registered read-only Web Tool can become a leader, join an in-flight
leader, or consume an exact historical result. OpenHands still owns every real
execution and its authoritative history. DCS is default-off and Phase 2 is
exact-only: semantic results remain on the ordinary Phase 3 control plane until
a separately versioned DCS contract is introduced. Weighted request admission and capability-gated KV retention are default-off.
Tool cache capacity uses saved execution cost, observed reuse, and freshness.
See [the scheduling implementation](docs/scheduling-implementation.md).

## Run

```bash
cd workspace/flowpilot
uv sync --extra dev
FLOWPILOT_INGRESS_API_KEY=local-dev \
FLOWPILOT_UPSTREAMS=http://127.0.0.1:8001 \
FLOWPILOT_REUSE_ENABLED=true \
FLOWPILOT_REUSE_CACHE_PATH=data/flowpilot_exact_cache.sqlite \
FLOWPILOT_DCS_ENABLED=true \
FLOWPILOT_DCS_WAL_PATH=data/flowpilot_dcs.sqlite \
FLOWPILOT_DCS_ENCRYPTION_KEY='<Fernet key>' \
FLOWPILOT_WEB_TOOL_REGISTRY_JSON='[{"tool_name":"web_search","canonical_tool_family":"public_web_search","tool_version":"1","result_schema_version":"1"}]' \
uv run flowpilot
```

Generate a development key with `Fernet.generate_key()` and keep it outside the
repository. FlowPilot refuses to start DCS without this key. Request snapshots,
barriers, and pending provider messages are encrypted in WAL schema v3; metadata
and digests remain queryable for recovery.

Ingress authentication is deployment-wide. Set `FLOWPILOT_INGRESS_API_KEY` for
the single trusted ingress key; per-workflow API keys and client-supplied
isolation fields are not part of the v2 protocol.

The gateway exposes `/v1/chat/completions` and `/v1/responses`. Every request
must include the `X-FlowPilot-*` identity headers described in
`docs/phase0-protocol.md`; the API key and identity headers are removed before
forwarding to the inference instance. Responses preserve provider body bytes,
stream chunk order, repeated response headers, query strings, status codes, and
provider errors. `GET /flowpilot/v1/gateway-calls` exposes metadata-only,
attempt-preserving terminal audit records.

The Phase 1 control plane exposes `/flowpilot/v1/reuse/resolve` and versioned
binding result/failure/poll endpoints. Exact keys include canonical Tool family,
Tool and result schema versions, canonical arguments, locale, language, region,
safe-search policy, time-sensitivity class, and data-source constraints. For an
explicitly allowlisted reusable Tool family, query content is not split into
private and public partitions; non-allowlisted, stateful, mutating, or
login-bound Tools remain non-reusable.

Command-line web fetches can be enabled with an explicit registry adapter. For
example, a `terminal` registry entry may set
`"command_line_reuse":"curl_url_exact"`; only one HTTP(S) URL and a small
allow-list of body-preserving `curl` flags are accepted. Headers, cookies,
uploads, output files, shell operators, and all other terminal commands remain
local-only. Web or browser Tool entries should instead opt into Phase 3 with
`semantic_reuse_enabled=true`, a calibrated embedder, and a per-family threshold.

The Phase 2 control plane is documented in `docs/phase2-dcs.md`; Phase 3 matching,
audit, and production gates are documented in `docs/phase3-semantic-reuse.md`.
The current
local exit evidence is recorded in `docs/phase2-exit-evidence.md`. Its API covers
delegation, DCS-aware exact resolution, delta append, continuation construction,
sync chunks, idempotent ACK, and reconciliation. Provider messages and request
snapshots are stored only in the DCS WAL and returned to the authorized Agent;
metadata-only traces contain their sizes and digests, never their contents.

Phase 4 forecast, Tool resolution, and SLO projection behavior is documented in
`docs/phase4-forecast-slo.md`. Single-instance weighted admission (M5) and the new vLLM retention adapter
(M6) are implemented behind separate opt-in settings; `design.md` defines their
boundaries. Real GPU/CPU reuse evidence remains pending. The remaining
experiment matrix, execution procedures, evidence requirements, and
GPU/implementation gates are described by the current design; older experiment
plans are historical references.

## KV integration status

The legacy KV framework has been removed: there is no KV directory, external
restore queue, KV lease API, legacy action adapter, or KV affinity in the router.
Tool readiness (`T_need`) and SLO/DAG projections remain available. Projections
no longer expose `t_kv`, `t2`, `kv_restore_laxity_ms`, or `kv_telemetry`.

The retired `/flowpilot/v1/kv` routes (including capability/action routes),
`/flowpilot/v1/events/kv`, `/flowpilot/v1/scheduling/kv-action/{line_id}`, and
`/flowpilot/v1/scheduling/alignment` return 404. Remove `kv_telemetry_schema`,
`kv_endpoint`, `kv_api_key`, and `kv_timeout_seconds` from instance configuration;
startup rejects these retired keys. The old `flowpilot-vllm-kv-v2` protocol has
no compatibility adapter or migration path.

Health reports `kv_telemetry=unsupported` when retention is disabled or the
engine lacks KV control v1. The new adapter negotiates capabilities separately,
queries descriptors, and submits KEEP/OFFLOAD/DROP with versioned receipts.
vLLM owns finish GRACE and every restore/recompute decision after ordinary
inference submission. CPU-only requests are never gated on GPU readiness.

Enable one fixed instance with, for example:

```bash
export FLOWPILOT_ADMISSION_JSON='{"enabled":true,"limit":8,"weights":{"slo":0.55,"age":0.35,"progress":0.05,"release":0.03,"cost":0.02,"fairness":0}}'
export FLOWPILOT_RETENTION_JSON='{"enabled":true,"owner_scope":"flowpilot-local"}'
# If the engine requires authentication, configure its control-plane API key:
# export FLOWPILOT_UPSTREAM_CONTROL_API_KEY=...
```

The admission limit is configured gateway concurrency, not measured vLLM batch
capacity. `/health` supplies health observations; `/tokenize` supplies prompt
work estimates. Queue scores and KV receipt state are available at the authenticated
`GET /flowpilot/v1/scheduling/state` endpoint. Risk classes are removed; fairness
is disabled by default and waiting age continues to grow.

See [implementation and validation](docs/scheduling-implementation.md) for the
formulas, Tool protection rules, capability limitations, and GPU validation still
required. The [vLLM framework proposal](docs/vllm-kv-management-framework.md)
remains the backend contract reference.

## Tests

```bash
uv run pytest -q
uv run ruff check .
OPENHANDS_SUPPRESS_BANNER=1 uv run python examples/openhands_e2e.py
```

The trace baseline is recorded in the implementation audit: the
available root contains three `tool_calls.jsonl` files and 149 Tool records,
but no target Web Search calls. That is enough to test collection plumbing,
not to claim reuse quality or end-to-end optimization.

Historical results and pending DCS deltas survive restart in separate SQLite
stores. In-flight bindings and the frontier deliberately do not. The OpenHands
adapter prepares stable-ID event files, stores a metadata-only recovery manifest,
commits one authoritative conversation HEAD per DCS batch, and reconciles pending
WAL ranges after restart. Recovery verifies event IDs/content, appends only a
missing suffix, ACKs only after the complete history is authoritative, and clears
the manifest after final ACK. Legacy pending WAL without a recovery manifest
fails closed. The service remains single-worker because frontier and binding
coordination are process-local.

For a local transport smoke test, run the deterministic mock upstream in a
second terminal:

```bash
uv run uvicorn examples.mock_inference:app --port 9011
```

Then point `FLOWPILOT_UPSTREAMS` at `http://127.0.0.1:9011`. The mock is only a
protocol fixture; it is not evidence for routing quality or optimization.
The E2E command drives a real OpenHands Agent loop through an exact historical
hit, delegated continuation, terminal sync/ACK, and reconciliation. Its latency
numbers are local samples, not production performance evidence. KV telemetry is
reported as unsupported because the mock inference server exposes no real KV
facts.
