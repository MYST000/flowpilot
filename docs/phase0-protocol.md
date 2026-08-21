# Phase 0 Protocol

The phase 0 boundary is intentionally small:

```text
trusted Local Agent
  -> FlowPilot OpenAI gateway
       -> one compatible LLM instance
       <- unchanged provider response/stream
  -> telemetry events for local Tool and KV observations
```

FlowPilot owns request routing, correlation and tail metadata. The Local Agent
owns conversation history, Action/Observation events, policy and every real
Tool execution. There is no history lookup, in-flight binding, DCS, replacement
or Tool executor in phase 0.

## LLM identity headers

Each `/v1/chat/completions` or `/v1/responses` request carries these headers:

| Header | Meaning |
| --- | --- |
| `X-FlowPilot-API-Key` | trusted ingress authentication; never forwarded |
| `X-FlowPilot-Protocol-Version` | `flowpilot-phase0-v1` |
| `X-FlowPilot-Tenant-ID` | trusted tenant boundary |
| `X-FlowPilot-Job-ID` | fairness and trace scope |
| `X-FlowPilot-Line-ID` | active execution line |
| `X-FlowPilot-Tail-Request-ID` | request represented by the current tail |
| `X-FlowPilot-LLM-Call-ID` | provider request correlation |
| `X-FlowPilot-Tail-Version` | expected current tail version; starts at `0` |
| `X-FlowPilot-Context-Epoch` | agent history epoch |
| `X-FlowPilot-Context-Sequence` | monotonic event sequence within the epoch |
| `X-FlowPilot-Context-Cursor` | last agent-confirmed context cursor |
| `X-FlowPilot-Context-Digest` | SHA-256 digest of that confirmed context |

The first successful request atomically replaces an `EMPTY` line tail and
returns version `1` in `X-FlowPilot-Tail-Version`. Connection failures and
provider error responses roll back the uncommitted replacement, so retrying the
same `llm_call_id` with the previous version remains valid. Subsequent successful
requests use the version returned by the previous response. A stale or concurrent
request receives 409; a late response cannot mutate the newer tail.

Within one context epoch, `context_sequence` may not decrease. Repeating a
sequence requires the same cursor and digest; conflicting metadata at the same
sequence is rejected. The cursor remains an opaque Agent event identifier and is
never ordered lexically. Phase 0 does not implement context synchronization,
exactly-once resume, epoch migration, or restart recovery; resume must register a
new line.

## Control events

The Local Agent registers jobs and lines, then may report:

- `POST /flowpilot/v1/events/tools`: `START` followed by exactly one `FINISH`,
  `FAIL`, or `CANCEL`. Every event includes a stable `event_id`, contiguous
  `sequence`, and `execution_attempt`. Duplicate event IDs are idempotent;
  conflicting terminal events are rejected. A call denied before execution uses
  one standalone `BLOCKED` event and never emits `START`. Payloads, rejection
  reason text, and credentials are not accepted or persisted.
- `POST /flowpilot/v1/events/kv`: session, instance, tier, bytes and measured
  restore cost.
- `PUT /flowpilot/v1/lines/{line_id}/dependencies`: atomically replaces the
  same-job prerequisite set with a monotonically increasing version. Unknown
  lines, duplicate IDs, stale versions and cycles are rejected.
- `POST /flowpilot/v1/lines/{line_id}/finish`: marks a ready line terminal and
  atomically releases current dependents. Further LLM requests for that line
  are rejected.

Trace records use `flowpilot-trace-v1`, JSONL, UUID event IDs and UTC timestamps.
LLM traces contain body digests and tool-call metadata, not prompts or result
payloads. This is a phase 0 privacy default; later experiments can add a
separately reviewed redacted capture mode.

The OpenHands adapter accepts the FlowPilot service root as `gateway_url` and
uses `<gateway_url>/v1` for the primary Agent LLM. Auxiliary LLMs keep their
existing route. Enablement registers one job/line and rejects
`tool_concurrency_limit != 1`; multiple Tool Calls in one assistant response are
still executed locally in original order with independent identities.

Trace writes are append-and-flush only. Write failure increments
`trace_write_failures` and degrades `/flowpilot/health`; rotation, disk-full
recovery, and restart continuity are unsupported in Phase 0. Standard OpenAI
endpoints do not expose real KV handles, tiers, bytes, or restore cost, so health
reports `kv_telemetry=unsupported` unless a future inference-specific integration
provides those measurements.
