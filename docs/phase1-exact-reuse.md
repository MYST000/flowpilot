# Phase 1 Exact Reuse

Phase 1 adds exact Web Tool reuse without moving Tool execution out of the local
Agent and without introducing deferred context synchronization.

```text
OpenHands ActionEvent
  -> resolve exact descriptor
     -> historical hit: validate and emit current Observation immediately
     -> in-flight hit: wait, validate, emit current Observation immediately
     -> leader: execute locally, emit START/terminal telemetry, publish result
     -> unavailable/invalid/expired: re-resolve or execute locally
```

`flowpilot-phase1-reuse-v2` is separate from the Phase 0 LLM transport
protocol. A resolve request contains the active tail identity, current Action and
Tool Call IDs, canonical JSON arguments, hard scope, and optional byte budget.
Only exact JSON descriptors match. Unknown, writable, disabled, or unregistered
Tools return `execute_locally`.

The registry is explicit. Each entry freezes the Tool family/version, result
schema version, TTL, maximum stored result size, read-only status, exact-reuse
switch, and reusable Tool policy. Name substring matching is never used for
reuse eligibility.

For terminal-like Tools, `command_line_reuse="curl_url_exact"` enables the
`curl_url_exact_v1` adapter. It accepts one credential-free HTTP(S) URL and
body-preserving flags only; shell composition, headers/cookies, uploads, output
redirection, and other commands resolve to `execute_locally`. The cache key
contains the normalized URL and allow-listed flags, so this adapter remains
exact reuse and does not infer semantic equivalence between pages.

The historical SQLite table stores the canonical descriptor, digests, sanitized
structured result, schema version, creation/freshness timestamps, and size. It
does not store prompts, credentials, authorization headers, leader identity, or
complete trace envelopes. Result adaptation keeps complete prefixes of a top-level
`items` array; an oversized non-structured result is rejected instead of being
cut at an arbitrary byte boundary.

In-flight bindings are process-local leases. Lookup and leader registration are
atomic under one controller lock with a second historical lookup inside the lock.
Leader completion validates ownership before optionally publishing history.
Followers retain their own Action/Tool Call identity and output budget. Leader
failure or expiry releases the descriptor so a follower can re-resolve; follower
cancellation never cancels the leader.

OpenHands enablement remains default-off and requires
`tool_concurrency_limit == 1`. Set `FlowPilotConfig.exact_reuse_enabled=True` and
provide `reusable_web_tools=(...)`; these names must agree with the server
registry. Cached Observation payloads are validated against the current Tool's
`observation_type`. Only `reuse_type`, `observed_at`, and `result_schema_version`
are appended to provider-visible provenance. Binding IDs, source scope, query
digests, and similarity fields remain control-plane-only.

Phase 1 explicitly does not support semantic lookup, DCS, pending context deltas,
delegation/ACK, internal continuation, multi-process bindings, or KV/Tool joint
scheduling. KV telemetry remains `unsupported` unless a real inference engine
reports measured state.
