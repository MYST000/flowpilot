# FlowPilot

The active baseline is the Phase 0 OpenAI-compatible gateway and measurement
plane described in `design.md`. It immediately delivers intact LLM responses,
keeps a minimal five-state `LineTail`, records every GatewayCall attempt to an
explicit terminal outcome, and keeps all real Tool execution in OpenHands.

The repository also contains default-off Phase 1 exact Web Tool reuse, Phase 2
deferred context synchronization (DCS), and Phase 3 conservative semantic reuse
modules. Their presence does not expand Phase 0 behavior.

FlowPilot never executes Tools. With later phases explicitly enabled, an
explicitly registered read-only Web Tool can become a leader, join an in-flight
leader, or consume an exact historical result. OpenHands still owns every real
execution and its authoritative history. DCS is default-off and Phase 2 is
exact-only: semantic results remain on the ordinary Phase 3 control plane until
a separately versioned DCS contract is introduced. Tool/KV joint scheduling
remains unsupported.

## Run

```bash
cd workspace/flowpilot
uv sync --extra dev
FLOWPILOT_INGRESS_API_KEY=local-dev \
FLOWPILOT_UPSTREAMS=http://127.0.0.1:8001 \
FLOWPILOT_REUSE_ENABLED=true \
FLOWPILOT_REUSE_CACHE_PATH=data/flowpilot_exact_cache.sqlite \
FLOWPILOT_SEMANTIC_DISABLED_TENANTS='' \
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

For tenant-bound ingress isolation, set
`FLOWPILOT_TENANT_API_KEYS_JSON='{"key-a":"tenant-a"}'` instead of the global
ingress key. The two modes are mutually exclusive. Tenant-bound keys are
validated against LLM identity headers and every tenant-scoped control payload;
global snapshots are unavailable in tenant-bound mode.

The gateway exposes `/v1/chat/completions` and `/v1/responses`. Every request
must include the `X-FlowPilot-*` identity headers described in
`docs/phase0-protocol.md`; the API key and identity headers are removed before
forwarding to the inference instance. Responses preserve provider body bytes,
stream chunk order, repeated response headers, query strings, status codes, and
provider errors. `GET /flowpilot/v1/gateway-calls` exposes metadata-only,
attempt-preserving terminal audit records.

The Phase 1 control plane exposes `/flowpilot/v1/reuse/resolve` and versioned
binding result/failure/poll endpoints. Exact keys include canonical Tool family,
Tool and result schema versions, canonical arguments, tenant/auth scope, locale,
language, region, safe-search policy, time-sensitivity class, and data-source
constraints. Cross-tenant public reuse is disabled unless the registry entry
explicitly sets `allow_public_scope=true`.

The Phase 2 control plane is documented in `docs/phase2-dcs.md`; Phase 3 matching,
audit, and production gates are documented in `docs/phase3-semantic-reuse.md`.
The current
local exit evidence is recorded in `docs/phase2-exit-evidence.md`. Its API covers
delegation, DCS-aware exact resolution, delta append, continuation construction,
sync chunks, idempotent ACK, and reconciliation. Provider messages and request
snapshots are stored only in the DCS WAL and returned to the authorized Agent;
metadata-only traces contain their sizes and digests, never their contents.

The remaining experiment matrix, execution procedures, evidence requirements,
and GPU/implementation gates are tracked in `docs/experiment-todo.md`.

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
