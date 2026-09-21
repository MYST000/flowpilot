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

The legacy KV directory, adapters, leases, restore queue, and action
recommendations have been removed. Health reports `kv_telemetry=unsupported`.
Projections retain `T_need` and SLO/DAG fields, without `t_kv`, `t2`, restore
laxity, or a KV readiness gate. The replacement retention/prefix integration is
specified in `design.md` and `vllm-kv-management-framework.md`; restoration
belongs entirely to the inference engine after ordinary request submission.

## Endpoints

- `GET /flowpilot/v1/forecast` exposes active/retained forecast metadata.
- `GET /flowpilot/v1/tool-resolutions` and `POST /flowpilot/v1/tool-resolutions`
  expose the versioned resolution fact store.
- `GET /flowpilot/v1/scheduling/projections/{line_id}` computes an ephemeral
  SLO/DAG projection.

The former KV and alignment endpoints have been removed and return 404. See
[KV integration status](../README.md#kv-integration-status) for retired
configuration and protocol fields.

Forecast settings are configured with `FLOWPILOT_FORECAST_TIMEOUT_SECONDS`,
`FLOWPILOT_FORECAST_TTL_SECONDS`, `FLOWPILOT_FORECAST_MIN_CONFIDENCE`,
`FLOWPILOT_FORECAST_TOP_N`, and `FLOWPILOT_TOOL_CATALOG_VERSION`.
