# Phase 0 Protocol

`design.md` is authoritative. Phase 0 is an immediate-delivery,
OpenAI-compatible proxy and measurement boundary:

```text
OpenHands
  -> FlowPilot gateway
       -> compatible vLLM instance
       <- unchanged provider response or SSE stream
  <- response delivered immediately

OpenHands -> metadata-only Tool lifecycle telemetry -> FlowPilot
```

OpenHands owns the agent loop, authoritative conversation history,
Action/Observation identity, security policy, and every real Tool execution.
FlowPilot owns proxying, routing, correlation, the bounded line frontier,
versioned `DEPENDS_ON`, and metadata-only telemetry. vLLM owns inference and its
KV Cache.

Phase 0 does not perform history or semantic reuse, in-flight binding, Tool
replacement or suppression, DCS/internal continuation, context ACK, forecast
consumption, or Tool/KV readiness scheduling. A Tool Call is a response
attribute, not a DAG node.

## LLM Identity Headers

Every `/v1/chat/completions` and `/v1/responses` request carries:

| Header | Meaning |
| --- | --- |
| `X-FlowPilot-API-Key` | Trusted ingress authentication; never forwarded |
| `X-FlowPilot-Protocol-Version` | `flowpilot-phase0-v1` |
| `X-FlowPilot-Tenant-ID` | Tenant boundary |
| `X-FlowPilot-Job-ID` | Workflow identity |
| `X-FlowPilot-Line-ID` | Active execution line |
| `X-FlowPilot-Tail-Request-ID` | Request replacing the current tail |
| `X-FlowPilot-LLM-Call-ID` | Provider-call and retry correlation |
| `X-FlowPilot-Tail-Version` | Expected authoritative tail version |
| `X-FlowPilot-Context-Epoch` | OpenHands history epoch |
| `X-FlowPilot-Context-Sequence` | Monotonic evidence sequence |
| `X-FlowPilot-Context-Cursor` | Opaque OpenHands context cursor |
| `X-FlowPilot-Context-Digest` | SHA-256 digest for that context evidence |

FlowPilot forwards request bodies, query strings, provider headers, response
bodies, status codes, repeated headers, and SSE chunks without semantic changes.
It strips only its private ingress/correlation headers and hop-by-hop headers.
Complete multi-Tool responses and every `tool_call_id` remain intact.

## State Ownership

`LineTail` contains only:

```text
tenant_id, job_id, line_id
tail_request_id?
phase: EMPTY | ACTIVE | BLOCKED | READY | TERMINAL
context_epoch, base_context_cursor
delta_ref?, delegation_ref?
version
```

Request/response metadata, bounded context evidence, dependencies, line
metadata, and Tool lifecycle facts live in separate stores. Gateway streaming,
retry, cancellation, and terminal results live in a separate `GatewayCall`
state machine. These facts are projected into frontier API responses but are
not copied into `LineTail`.

The Phase 0 transitions are:

```text
LINE_REGISTER   -> EMPTY
LLM_REQUEST     -> ACTIVE and atomically increments/replaces the tail
LLM_RESPONSE    -> BLOCKED when Tool/dependency facts remain unresolved
LLM_RESPONSE    -> READY otherwise
TOOL/DEP update -> recompute BLOCKED or READY
LINE_FINISH     -> TERMINAL, then reclaim when safe
```

Because OpenHands owns the authoritative provider-valid history, a trusted next
request with an advanced context sequence also proves that the required local
Tool observations were incorporated. It may replace a Tool-blocked tail when no
`DEPENDS_ON` edge remains. This keeps telemetry failure observational rather
than control-flow-changing.

Connection, upstream, provider, malformed response/SSE, stream, and client
cancellation paths roll back an uncommitted tail replacement. A retry may use
the same `llm_call_id` and visible version. Each attempt remains separately
auditable through `GET /flowpilot/v1/gateway-calls`; no terminal path may leave
the line `ACTIVE` solely because proxy cleanup failed.

Within one context epoch, sequence cannot decrease. Repeating a sequence
requires the same cursor and digest. Phase 0 records continuity evidence only;
it does not claim context delivery, exactly-once resume, or restart recovery.

## Control Events

- `POST /flowpilot/v1/events/tools` records a stable `event_id`, monotonic
  `sequence`, and `execution_attempt`. `START` has one `FINISH`, `FAIL`, or
  `CANCEL`; a pre-execution denial uses one `BLOCKED`. Duplicate events are
  idempotent and conflicting terminals are rejected. Telemetry failure never
  changes the local Tool result.
- `PUT /flowpilot/v1/lines/{line_id}/dependencies` atomically replaces the
  same-job prerequisite set. Versions must increase, unknown lines and
  duplicates are rejected, cycles preserve the old graph, and a line with an
  unresolved prerequisite is immediately `BLOCKED` even when it has no tail
  request yet.
- `POST /flowpilot/v1/lines/{line_id}/finish` terminates a ready/empty line and
  releases current dependents.
- `POST /flowpilot/v1/events/kv` accepts facts only from an inference instance
  configured with `kv_telemetry_schema=flowpilot-vllm-kv-v1`. Standard vLLM
  instances return `{"status":"unsupported","kv_telemetry":"unsupported"}`
  and emit no `kv_state` trace.

Standard vLLM OpenAI-compatible serving requires no extension for Phase 0
proxying. It does not expose trusted per-session KV handles, tiers, bytes, or
restore costs. FlowPilot never estimates those values from token counts.

Trace records use `flowpilot-trace-v1`, JSONL, UUID event IDs, and UTC
timestamps. They contain digests, sizes, timing, status, and correlation, never
prompts, complete Tool inputs/results, authorization values, or credentials.
Write failure increments failure/drop counters and degrades health. Rotation,
disk-full recovery, and restart continuity remain unsupported.

Basic OpenHands integration uses the FlowPilot `/v1` base URL and static extra
headers; no OpenHands core change is required. Stable dynamic identities and
Tool-boundary telemetry may use the existing default-off adapter. Tools always
execute locally in provider order.
