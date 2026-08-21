# Phase 2 Deferred Context Synchronization

Phase 2 adds DCS only for Phase 1 exact historical hits and exact in-flight
followers. It does not enable semantic reuse. FlowPilot never executes a Tool;
a cache miss or non-reusable Tool creates a local execution barrier and OpenHands
executes it after context synchronization succeeds.

`flowpilot-phase2-dcs-v1` freezes these transitions:

```text
grant delegation -> OPEN
OPEN + exact result batch -> OPEN | SYNCING(capacity)
OPEN + continuation request -> scheduler-built non-streaming request snapshot
OPEN + local/final/TTL/lease/failure barrier -> SYNCING
SYNCING + matching ACK -> SYNCING(partial) | ACKED(final)
SYNCING + conflicting ACK -> DIVERGED
restart + matching Agent cursor -> IN_SYNC | SYNC_REQUIRED(recover pending batch)
ACKED + later Agent cursor/digest -> AGENT_AHEAD_REQUIRES_NEW_DELEGATION
OPEN/SYNCING + conflicting cursor/digest -> DIVERGED
```

A delegation is an atomically versioned, single-writer lease bound to one
`tenant_id/job_id/line_id/context_epoch`, base cursor/digest, exact Tool
allow-list, API kind, confirmed request snapshot, expiry, message/byte limits,
and continuation limit. Replacing a policy requires its current version and no
pending context. Expired leases cannot continue; pending data enters an early
sync barrier.

The SQLite WAL stores provider-valid messages in exact order. Each message has a
monotonic sequence and `sha256(previous_digest + "\n" + canonical_message)`.
Chat batches must contain one complete assistant message followed by matching
Tool messages in `tool_call_id` order. Responses batches use matching
`function_call` and `function_call_output` items. A batch is never split into
different histories.

Sync can be fragmented. An ACK must cover the earliest pending contiguous range
and match its terminal WAL digest. It also carries `new_context_cursor` and
`new_context_digest`, computed from OpenHands' authoritative history after the
events are persisted. The WAL `delta_digest` and Agent history digest are
different chains and are never substituted for one another. Repeating any of the
latest 128 matching ACK receipts for a line is idempotent; reusing a committed
range with different content, or sending a conflicting range, digest, cursor, or
resulting Agent digest, marks the line `DIVERGED` and revokes its writer lease.

The OpenHands adapter validates a complete provider-message chunk against locally
reconstructed Action/Observation/Message events. It prepares the stable-ID event
files under one EventLog lock, persists a recovery manifest, and advances the
authoritative `base_state.json` HEAD once for the complete batch. Prepared event
files remain unreachable after a crash until HEAD commit. Recovery verifies event
IDs and content digests, commits only a missing suffix, and ACKs only after the
complete batch is authoritative. An ACK failure retains the manifest; a matching
retry is idempotent and final ACK cleanup removes it.

Internal continuations mechanically clone the confirmed request snapshot and
append the WAL messages. System/developer content, Tool schemas, model and
sampling fields are not rewritten. Phase 2 currently rejects streaming internal
snapshots because provisional stream buffering/commit has not been implemented;
ordinary Phase 0 streaming remains unchanged when DCS is disabled.

The WAL persists pending deltas across restart and encrypts request snapshots,
barriers, and provider messages with the required
`FLOWPILOT_DCS_ENCRYPTION_KEY`. Receipt tokens are not stored; only their hashes
and bounded binding claims remain. Plaintext schema v1 databases are migrated to
encrypted schema v3 on open; schema v2 databases gain bounded ACK receipts. The
process-local frontier and in-flight binding registry do not. On reconnect the
Agent calls reconciliation with its authoritative epoch/cursor/digest. A
metadata-only recovery manifest restores the delegation reference, policy/base
metadata, WAL range, provider batch digest, and stable Event IDs. Provider bodies
are redacted in the recovery payload; complete Action/Observation bodies remain
only in OpenHands' normal event files. A legacy process with pending WAL but no
recovery manifest still fails closed instead of guessing history. Once a final ACK has
cleared every pending message and revoked the Scheduler writer, later local Tool
observations and ordinary Agent replies may legitimately advance the same epoch;
reconciliation reports
`agent_ahead_requires_new_delegation` instead of treating that Agent-owned
progress as a fork. A cursor/digest conflict while a writer or pending delta is
still active remains `CONTEXT_DIVERGED`; FlowPilot never guesses missing history.

The current OpenHands DCS path is deliberately narrow: non-streaming native Tool
calling, serial execution, explicit exact-reuse registration, and a Tool whose
local annotations set `readOnlyHint=True`. Router/subscription LLMs, images,
prompt caching, custom LiteLLM bodies, condensers, critics, hooks, security
analyzers, confirmation, and run budgets form immediate local barriers or disable
delegation. Semantic reuse, multi-worker continuation, provisional streaming,
and parallel local execution remain unsupported.

Trace records contain correlation IDs, sequence ranges, digests, sizes, decisions,
barriers, and outcomes. They exclude request snapshots, provider messages, Tool
arguments/results, prompts, credentials, and authorization headers. WAL access
therefore requires the same filesystem protection and retention policy as Agent
conversation state. Rotation and multi-worker coordination remain unsupported.
