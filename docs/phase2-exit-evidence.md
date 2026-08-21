# Phase 2 Exit Evidence

Validated on 2026-08-17 against the local OpenHands checkout and deterministic
mock inference service. These results establish local protocol correctness only;
they are not production performance or reliability evidence.

## Completion labels

- implementation complete: recoverable atomic OpenHands event batches, stable-ID
  suffix recovery, exact ACK, Responses multi-Tool DCS, and exact in-flight
  follower DCS are implemented.
- local verification complete: focused tests, full FlowPilot tests, Ruff,
  Pyright, compile/import checks, mock E2E, trace/privacy/WAL audits, and legacy
  database hashes pass.
- production evidence insufficient: no real inference deployment, rolling
  multi-process upgrade, network partition campaign, or target `web_search`
  calibration corpus was available.

## Correctness gates

- OpenHands FlowPilot tests: 29 passed. Fault injection covers recovery record
  persistence, partial event-file prepare, HEAD failure, ACK failure, stable-ID
  retry, manifest cleanup, and legacy WAL without a recovery record.
- FlowPilot tests: 54 passed.
- Ruff: both repositories pass.
- Pyright: both repositories report zero errors and warnings.
- Compile/import: both repositories pass.
- The adapter-disabled test confirms the existing OpenHands path is unchanged.

## Mock E2E evidence

The final run emitted 154 metadata-only trace records and 29 correlated LLM
requests. It exercised Chat Completions, Responses, exact historical reuse,
exact in-flight follower reuse, local Tool barriers, and terminal barriers.

- DCS lifecycle records: 27.
- Responses multi-Tool IDs, in provider order: `tool-call-responses-1`, then
  `tool-call-responses-2`.
- Sync batch sizes: 3, 3, and 5 provider messages/items for the historical
  Chat, local-barrier Chat, and multi-Tool Responses scenarios.
- Exact in-flight follower: one local leader execution; follower delta recorded
  `reuse_kinds=["exact_inflight"]` and did not execute the Tool locally.
- Barrier ordering: `context_sync_ack -> tool_start -> llm_request`.
- DCS WAL: SQLite integrity `ok`, four lines `acked`, zero pending messages.

The local single-sample comparison was:

| Path | JCT | Control trace events |
|---|---:|---:|
| Immediate-return exact historical reuse | 123.447 ms | 1 |
| Chat DCS exact historical reuse | 280.840 ms | 8 |
| Responses DCS, two Tool Calls | 288.846 ms | 9 |

The proxy latency micro-sample was 13.828 ms direct median versus 27.091 ms
proxy median, or 13.264 ms local mock overhead. Recovery unit execution was
below pytest's 5 ms duration display threshold. The DCS examples are slower and
have more control events in this one-hidden-round mock workload; no performance
benefit is claimed.

## Privacy and storage audit

- Trace search found no prompt markers, Tool result marker, API key, FlowPilot
  API-key header, or Authorization header.
- A strings scan of the encrypted DCS WAL found none of those values.
- Recovery manifests contain stable IDs, digests, ranges, policy/base metadata,
  and a ciphertext Event payload. Provider content is represented by redacted
  shape, byte count, and digest.
- Legacy Tool-Reuse databases were unchanged:
  - exact: `b3ae6e1bc7b0d134e7ea8ff2fdf8b566062523776351e462315e0e46baa21e0a`
  - semantic: `c33cf02c56886f7af4ccfe1b2244ad28c8b29f393ca97fc336d3f5ff425353f6`

## Phase 3 decision

NO-GO. The available experiment traces contain 149 Tool records and zero
structured `web_search` calls. They cannot calibrate per-tool semantic
thresholds, freshness behavior, false-reuse risk, or tenant/auth isolation.
Production crash/partition and rolling-upgrade evidence is also absent. Phase 3
semantic reuse must remain unimplemented until those evidence gaps are closed.
