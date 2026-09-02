# Phase 4 Forecast and SLO Loop

FlowPilot Phase 4 adds the externally-owned Tool predictor placeholder and the
fact-driven scheduling view. The predictor is a side channel: an accepted LLM
request is forwarded immediately, while `ForecastManager` starts a cancellable
forecast task with a versioned `ForecastRequest` envelope.

`ForecastResult` contains only Top-N Tool-family metadata, duration quantiles,
confidence, predictor version, and an expiry. FlowPilot rejects incompatible,
expired, over-TTL, low-confidence, malformed, late, or unavailable results.
Rejected results are recorded as metadata-only `forecast_discarded` events. A
valid result is retained briefly for optional Tool Cache metadata prewarm and as
a miss-duration prior. It never creates a DAG node, executes a Tool, or changes
OpenHands control flow. `NoOpForecastAdapter` and
`TraceReplayForecastAdapter` keep this boundary testable before a production
predictor exists.

Closed Tool Calls and reuse decisions are stored in `ToolResolutionStore`.
Actual Tool telemetry and cache/in-flight outcomes advance a versioned
`ToolResolutionRecord`; actual finish/size/latency values override forecast
priors. Records are deliberately separate from `LineTail`.

`ProjectionCalculator` creates an ephemeral `SchedulingProjection` from the
current frontier, resolution facts, dependency blocking count, deadline, and
waiting age. It computes DAG importance and SLO deadline urgency, carries a
factual `T_need` when available, and exposes `validate_current()` for
tail-version checks immediately before an action. Projections are never
persisted.

KV telemetry remains capability-gated. Standard OpenAI-compatible vLLM
instances return `kv_telemetry=unsupported`; only instances declaring
`flowpilot-vllm-kv-v2` contribute facts to `KVDirectory`. The directory can
return a metadata-only KEEP/OFFLOAD/RESTORE recommendation carrying the current
tail request/version. It does not execute KV actions, estimate bytes, or merge
Tool and KV capacities; full `max(T_need, T_KV)` alignment and restore queues
remain Phase 5.

## Endpoints

- `GET /flowpilot/v1/forecast` exposes active/retained forecast metadata.
- `GET /flowpilot/v1/tool-resolutions` and `POST /flowpilot/v1/tool-resolutions`
  expose the versioned resolution fact store.
- `GET /flowpilot/v1/scheduling/projections/{line_id}` computes an ephemeral
  SLO/DAG projection.
- `GET /flowpilot/v1/scheduling/kv-action/{line_id}` returns a capability-gated
  KV recommendation; it never performs the action.
- `GET /flowpilot/v1/kv` exposes only KV fact metadata and support status.

Forecast settings are configured with `FLOWPILOT_FORECAST_TIMEOUT_SECONDS`,
`FLOWPILOT_FORECAST_TTL_SECONDS`, `FLOWPILOT_FORECAST_MIN_CONFIDENCE`,
`FLOWPILOT_FORECAST_TOP_N`, and `FLOWPILOT_TOOL_CATALOG_VERSION`.
