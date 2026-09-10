from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, cast

import httpx

from flowpilot.protocol import (
    KVAction,
    KVActionKind,
    KVActionResult,
    KVLease,
    KVStateEvent,
    KVStateFact,
    KVTier,
    SchedulingProjection,
)

type KVScopeKey = tuple[str, str, str, str]
type KVHandleKey = tuple[str, str, str]


class KVUnsupported(RuntimeError):
    pass


class KVStale(ValueError):
    pass


class KVRejected(ValueError):
    pass


class KVAdapterError(RuntimeError):
    """Transport or capability failure from a remote KV extension."""


@dataclass(frozen=True, slots=True)
class KVFact:
    job_id: str
    line_id: str
    llm_call_id: str
    session_id: str
    instance_id: str
    engine_epoch: str
    kv_handle: str
    generation: int
    tier: KVTier
    bytes: int
    restore_cost_ms: float | None
    migration_cost_ms: float | None
    rematerialization_cost_ms: float | None
    observed_at: datetime
    sequence: int
    schema_version: str


@dataclass(frozen=True, slots=True)
class KVActionRecommendation:
    action: str
    session_id: str
    instance_id: str
    tail_request_id: str | None
    tail_version: int
    execute_after: datetime | None
    reason: str
    kv_handle: str | None = None
    engine_epoch: str | None = None
    generation: int | None = None


class VLLMKVAdapter(Protocol):
    schema_version: str

    async def acquire_lease(
        self, fact: KVStateFact, owner: str, ttl_seconds: float
    ) -> KVLease: ...
    async def renew_lease(self, lease: KVLease, ttl_seconds: float) -> KVLease: ...
    async def release_lease(self, lease: KVLease) -> None: ...
    async def execute(self, action: KVAction) -> KVActionResult: ...

    async def get_kv_state(
        self,
        session_id: str,
        *,
        job_id: str,
        line_id: str,
    ) -> KVStateFact | None: ...


class HTTPVLLMKVAdapter:
    """HTTP client for the feature-gated vLLM KV contract.

    The adapter starts unsupported and only enables actions after a successful
    capability negotiation.  A 404/501 or an incompatible schema is a normal
    downgrade; transport failures are surfaced as ``KVAdapterError`` so callers
    can fail closed without fabricating KV facts.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout_seconds: float = 5.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("KV adapter base_url must be HTTP(S)")
        if timeout_seconds <= 0:
            raise ValueError("KV adapter timeout must be positive")
        self.base_url = base_url.rstrip("/")
        if self.base_url.endswith("/v1"):
            self.base_url = self.base_url[:-3]
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.schema_version = "unsupported"
        self.engine_epoch: str | None = None
        self._client = client
        self._owns_client = client is None
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def negotiate(self) -> bool:
        """Probe capabilities and return whether v1 actions are available."""
        try:
            payload = await self._request("GET", "/flowpilot/v1/kv/capabilities")
        except KVAdapterError as exc:
            if str(exc).startswith("unsupported:"):
                self.schema_version = "unsupported"
                return False
            raise
        schema = payload.get("schema_version")
        enabled = payload.get("enabled", True)
        epoch = payload.get("engine_epoch")
        if (
            schema != "flowpilot-vllm-kv-v2"
            or not enabled
            or not isinstance(epoch, str)
        ):
            self.schema_version = "unsupported"
            self.engine_epoch = None
            return False
        if self.engine_epoch is not None and self.engine_epoch != epoch:
            prior = self.engine_epoch
            self.engine_epoch = epoch
            self.schema_version = "unsupported"
            raise KVStale(f"engine epoch changed from {prior} to {epoch}")
        self.schema_version = schema
        self.engine_epoch = epoch
        return True

    async def acquire_lease(
        self, fact: KVStateFact, owner: str, ttl_seconds: float
    ) -> KVLease:
        await self._ensure_supported()
        payload = await self._request(
            "POST",
            "/flowpilot/v1/kv/leases/acquire",
            {
                "fact": fact.model_dump(mode="json"),
                "owner": owner,
                "ttl_seconds": ttl_seconds,
            },
        )
        lease = KVLease.model_validate(payload)
        self._check_epoch(lease.engine_epoch)
        return lease

    async def renew_lease(self, lease: KVLease, ttl_seconds: float) -> KVLease:
        await self._ensure_supported()
        payload = await self._request(
            "POST",
            "/flowpilot/v1/kv/leases/renew",
            {"lease": lease.model_dump(mode="json"), "ttl_seconds": ttl_seconds},
        )
        renewed = KVLease.model_validate(payload)
        self._check_epoch(renewed.engine_epoch)
        return renewed

    async def release_lease(self, lease: KVLease) -> None:
        await self._ensure_supported()
        await self._request(
            "POST", "/flowpilot/v1/kv/leases/release", lease.model_dump(mode="json")
        )

    async def execute(self, action: KVAction) -> KVActionResult:
        await self._ensure_supported()
        payload = await self._request(
            "POST", "/flowpilot/v1/kv/actions", action.model_dump(mode="json")
        )
        fact_payload = payload.get("fact")
        fact = (
            KVStateFact.model_validate(fact_payload)
            if isinstance(fact_payload, dict)
            else None
        )
        raw_status = payload.get("status", "unsupported")
        status = cast(
            Literal["applied", "unsupported", "rejected"],
            raw_status
            if raw_status in {"applied", "unsupported", "rejected"}
            else "unsupported",
        )
        raw_schema = payload.get("schema_version", "flowpilot-vllm-kv-v2")
        if raw_schema != "flowpilot-vllm-kv-v2":
            self.schema_version = "unsupported"
            raise KVUnsupported("incompatible KV action result schema")
        result = KVActionResult(
            status=status,
            action_id=str(payload.get("action_id", action.action_id)),
            generation=(
                int(payload["generation"])
                if payload.get("generation") is not None
                else None
            ),
            fact=fact,
            reason=(
                str(payload["reason"]) if payload.get("reason") is not None else None
            ),
            schema_version="flowpilot-vllm-kv-v2",
        )
        if result.fact is not None:
            self._check_epoch(result.fact.engine_epoch)
        return result

    async def get_kv_state(
        self,
        session_id: str,
        *,
        job_id: str,
        line_id: str,
    ) -> KVStateFact | None:
        """Read one scoped fact; ambiguity never returns a guessed handle."""
        await self._ensure_supported()
        try:
            payload = await self._request(
                "GET",
                f"/flowpilot/v1/kv/state/{session_id}",
                params={"job_id": job_id, "line_id": line_id},
            )
        except KVAdapterError as exc:
            if str(exc).startswith("unsupported:"):
                return None
            raise
        if not payload:
            return None
        fact = KVStateFact.model_validate(payload)
        self._check_epoch(fact.engine_epoch)
        return fact

    async def offload_kv(self, action: KVAction) -> KVActionResult:
        if action.action is not KVActionKind.OFFLOAD:
            raise ValueError("offload_kv requires an OFFLOAD action")
        return await self.execute(action)

    async def restore_kv(self, action: KVAction) -> KVActionResult:
        if action.action is not KVActionKind.RESTORE:
            raise ValueError("restore_kv requires a RESTORE action")
        return await self.execute(action)

    async def drop_kv(self, action: KVAction) -> KVActionResult:
        if action.action is not KVActionKind.DROP:
            raise ValueError("drop_kv requires a DROP action")
        return await self.execute(action)

    async def _ensure_supported(self) -> None:
        if self.schema_version != "flowpilot-vllm-kv-v2":
            await self.negotiate()
        if self.schema_version != "flowpilot-vllm-kv-v2":
            raise KVUnsupported("kv_telemetry=unsupported")

    def _check_epoch(self, epoch: str) -> None:
        if self.engine_epoch is None:
            self.engine_epoch = epoch
            return
        if epoch != self.engine_epoch:
            prior = self.engine_epoch
            self.engine_epoch = epoch
            self.schema_version = "unsupported"
            raise KVStale(f"engine epoch changed from {prior} to {epoch}")

    async def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        client = self._client
        if client is None:
            client = self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds)
            )
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["X-FlowPilot-API-Key"] = self.api_key
        try:
            response = await client.request(
                method,
                f"{self.base_url}{path}",
                json=payload,
                params=params,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except httpx.HTTPError as exc:
            raise KVAdapterError(f"transport: {exc}") from exc
        if response.status_code in {404, 501}:
            raise KVAdapterError(f"unsupported:{response.status_code}")
        if response.status_code == 401 or response.status_code == 403:
            raise KVAdapterError("authentication failed")
        if response.status_code == 409:
            detail = response.text[:256]
            if any(
                word in detail.lower()
                for word in ("stale", "epoch", "generation", "fenc")
            ):
                raise KVStale(detail)
            raise KVRejected(detail)
        if response.status_code >= 400:
            raise KVAdapterError(f"upstream HTTP {response.status_code}")
        if response.status_code == 204:
            return {}
        try:
            value = response.json()
        except ValueError as exc:
            raise KVAdapterError("upstream returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise KVAdapterError("upstream returned a non-object payload")
        return value


class UnsupportedVLLMKVAdapter:
    schema_version = "unsupported"

    async def acquire_lease(
        self, fact: KVStateFact, owner: str, ttl_seconds: float
    ) -> KVLease:
        del fact, owner, ttl_seconds
        raise KVUnsupported("kv_telemetry=unsupported")

    async def renew_lease(self, lease: KVLease, ttl_seconds: float) -> KVLease:
        del lease, ttl_seconds
        raise KVUnsupported("kv_telemetry=unsupported")

    async def release_lease(self, lease: KVLease) -> None:
        del lease
        raise KVUnsupported("kv_telemetry=unsupported")

    async def execute(self, action: KVAction) -> KVActionResult:
        return KVActionResult(
            status="unsupported",
            action_id=action.action_id,
            generation=None,
            reason="kv_telemetry=unsupported",
        )

    async def get_kv_state(
        self,
        session_id: str,
        *,
        job_id: str,
        line_id: str,
    ) -> KVStateFact | None:
        del session_id, job_id, line_id
        return None


class MockVLLMKVAdapter:
    """Deterministic in-memory implementation of the v1 fencing contract.

    It supplies protocol evidence only; it is not evidence of vLLM/GPU KV
    migration.  Per-handle locks serialize the short compare-and-apply step.
    """

    schema_version = "flowpilot-vllm-kv-v2"

    def __init__(self) -> None:
        self._facts: dict[KVHandleKey, KVStateFact] = {}
        self._leases: dict[KVHandleKey, KVLease] = {}
        self._receipts: dict[str, tuple[KVAction, KVActionResult]] = {}
        self._fencing: dict[KVHandleKey, int] = {}
        self._locks: dict[KVHandleKey, asyncio.Lock] = {}

    async def seed(self, fact: KVStateFact) -> None:
        self._facts[(fact.instance_id, fact.engine_epoch, fact.kv_handle)] = fact

    async def get_kv_state(
        self,
        session_id: str,
        *,
        job_id: str,
        line_id: str,
    ) -> KVStateFact | None:
        matches = [
            fact
            for fact in self._facts.values()
            if fact.session_id == session_id
            and fact.job_id == job_id
            and fact.line_id == line_id
        ]
        return matches[0] if len(matches) == 1 else None

    async def acquire_lease(
        self, fact: KVStateFact, owner: str, ttl_seconds: float
    ) -> KVLease:
        if ttl_seconds <= 0:
            raise ValueError("lease TTL must be positive")
        key = (fact.instance_id, fact.engine_epoch, fact.kv_handle)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            current = self._facts.get(key)
            if current is None or current.generation != fact.generation:
                raise KVStale("stale KV handle or generation")
            token = self._fencing.get(key, 0) + 1
            self._fencing[key] = token
            lease = KVLease(
                lease_id=f"lease-{uuid.uuid4().hex}",
                job_id=fact.job_id,
                line_id=fact.line_id,
                instance_id=fact.instance_id,
                engine_epoch=fact.engine_epoch,
                session_id=fact.session_id,
                kv_handle=fact.kv_handle,
                generation=fact.generation,
                owner=owner,
                fencing_token=token,
                expires_at=datetime.now(UTC) + timedelta(seconds=ttl_seconds),
            )
            self._leases[key] = lease
            return lease

    async def renew_lease(self, lease: KVLease, ttl_seconds: float) -> KVLease:
        key = (lease.instance_id, lease.engine_epoch, lease.kv_handle)
        current = self._leases.get(key)
        if current != lease or lease.expires_at <= datetime.now(UTC):
            raise KVStale("lease is stale or expired")
        renewed = lease.model_copy(
            update={"expires_at": datetime.now(UTC) + timedelta(seconds=ttl_seconds)}
        )
        self._leases[key] = renewed
        return renewed

    async def release_lease(self, lease: KVLease) -> None:
        key = (lease.instance_id, lease.engine_epoch, lease.kv_handle)
        current = self._leases.get(key)
        if (
            current is not None
            and current.lease_id == lease.lease_id
            and current.fencing_token == lease.fencing_token
        ):
            self._leases.pop(key, None)

    async def execute(self, action: KVAction) -> KVActionResult:
        key = (action.instance_id, action.engine_epoch, action.kv_handle)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            lease = self._leases.get(key)
            now = datetime.now(UTC)
            if lease is None or lease.expires_at <= now:
                raise KVStale("lease expired or missing")
            if (lease.lease_id, lease.fencing_token) != (
                action.lease_id,
                action.fencing_token,
            ):
                raise KVStale("lease fencing token is stale")
            receipt = self._receipts.get(action.idempotency_key)
            if receipt is not None:
                prior_action, result = receipt
                if prior_action != action:
                    raise KVRejected("idempotency key conflicts with prior action")
                return result
            fact = self._facts.get(key)
            if fact is None or fact.generation != action.generation:
                raise KVStale("engine epoch, handle or generation is stale")
            tier = {
                KVActionKind.KEEP: fact.tier,
                KVActionKind.OFFLOAD: action.target_tier,
                KVActionKind.RESTORE: KVTier.GPU,
                KVActionKind.DROP: KVTier.DROPPED,
            }[action.action]
            if tier is None:
                raise KVRejected("OFFLOAD target tier is missing")
            next_fact = fact.model_copy(
                update={
                    "generation": fact.generation + 1,
                    "tier": tier,
                    "observed_at": now,
                    "sequence": fact.sequence + 1,
                }
            )
            self._facts[key] = next_fact
            result = KVActionResult(
                status="applied",
                action_id=action.action_id,
                generation=next_fact.generation,
                fact=next_fact,
            )
            self._receipts[action.idempotency_key] = (action, result)
            return result

    def restart(self, instance_id: str, engine_epoch: str) -> None:
        for key in tuple(self._facts):
            if key[0] == instance_id and key[1] != engine_epoch:
                self._facts.pop(key, None)
                self._leases.pop(key, None)


class KVDirectory:
    """Scoped directory for genuine v1 KV facts with epoch/sequence guards."""

    def __init__(
        self, *, fact_ttl_seconds: float = 60.0, max_entries: int = 50_000
    ) -> None:
        if fact_ttl_seconds <= 0 or max_entries <= 0:
            raise ValueError("KV fact TTL and capacity must be positive")
        self.fact_ttl_seconds = fact_ttl_seconds
        self.max_entries = max_entries
        self._facts: dict[KVScopeKey, KVFact] = {}
        self._handle_owner: dict[KVHandleKey, KVScopeKey] = {}
        self._engine_epochs: dict[str, tuple[str, datetime]] = {}
        self._lock = asyncio.Lock()

    async def record(
        self, event: KVStateEvent, *, supported: bool, llm_call_id: str = "unknown"
    ) -> KVFact | None:
        if (
            not supported
            or event.protocol_version != "flowpilot-vllm-kv-v2"
            or event.schema_version != "flowpilot-vllm-kv-v2"
            or not event.kv_handle
        ):
            return None
        if event.observed_at <= datetime.now(UTC) - timedelta(
            seconds=self.fact_ttl_seconds
        ):
            raise KVStale("KV fact expired before arrival")
        fact = KVFact(
            event.job_id,
            event.line_id,
            llm_call_id,
            event.session_id,
            event.instance_id,
            event.engine_epoch,
            event.kv_handle,
            event.generation,
            event.tier,
            event.bytes,
            event.restore_cost_ms,
            event.migration_cost_ms,
            event.rematerialization_cost_ms,
            event.observed_at,
            event.sequence,
            event.schema_version,
        )
        key = (
            event.job_id,
            event.line_id,
            event.instance_id,
            event.session_id,
        )
        handle_key = (event.instance_id, event.engine_epoch, event.kv_handle)
        async with self._lock:
            current_engine = self._engine_epochs.get(event.instance_id)
            if current_engine is None:
                self._engine_epochs[event.instance_id] = (
                    event.engine_epoch,
                    event.observed_at,
                )
            elif current_engine[0] != event.engine_epoch:
                if event.observed_at <= current_engine[1]:
                    raise KVStale("event belongs to an older engine epoch")
                for stale_key, stale_fact in tuple(self._facts.items()):
                    if stale_fact.instance_id == event.instance_id:
                        self._facts.pop(stale_key, None)
                        self._handle_owner.pop(
                            (
                                stale_fact.instance_id,
                                stale_fact.engine_epoch,
                                stale_fact.kv_handle,
                            ),
                            None,
                        )
                self._engine_epochs[event.instance_id] = (
                    event.engine_epoch,
                    event.observed_at,
                )
            prior = self._facts.get(key)
            if prior is not None:
                if event.engine_epoch == prior.engine_epoch:
                    if event.sequence < prior.sequence:
                        raise KVStale("out-of-order KV event")
                    if event.sequence == prior.sequence:
                        if fact == prior:
                            return prior
                        raise KVRejected("conflicting duplicate KV event")
                    if event.generation < prior.generation:
                        raise KVStale("KV generation regressed")
                else:
                    self._handle_owner.pop(
                        (prior.instance_id, prior.engine_epoch, prior.kv_handle), None
                    )
            owner = self._handle_owner.get(handle_key)
            if owner is not None and owner != key:
                raise KVRejected("KV handle collides across scoped sessions")
            self._facts[key] = fact
            self._handle_owner[handle_key] = key
            self._trim_locked()
        return fact

    async def record_fact(self, fact: KVStateFact) -> KVFact:
        event = KVStateEvent(
            protocol_version="flowpilot-vllm-kv-v2",
            schema_version=fact.schema_version,
            job_id=fact.job_id,
            line_id=fact.line_id,
            session_id=fact.session_id,
            instance_id=fact.instance_id,
            engine_epoch=fact.engine_epoch,
            kv_handle=fact.kv_handle,
            generation=fact.generation,
            tier=fact.tier,
            bytes=fact.bytes,
            restore_cost_ms=fact.restore_cost_ms,
            migration_cost_ms=fact.migration_cost_ms,
            rematerialization_cost_ms=fact.rematerialization_cost_ms,
            observed_at=fact.observed_at,
            sequence=fact.sequence,
        )
        recorded = await self.record(
            event, supported=True, llm_call_id=fact.llm_call_id
        )
        if recorded is None:
            raise KVUnsupported("kv_telemetry=unsupported")
        return recorded

    async def get(
        self,
        instance_id: str,
        session_id: str,
        *,
        job_id: str | None = None,
        line_id: str | None = None,
    ) -> KVFact | None:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            if job_id and line_id:
                return self._facts.get((job_id, line_id, instance_id, session_id))
            matches = [
                fact
                for key, fact in self._facts.items()
                if key[2:] == (instance_id, session_id)
            ]
            return matches[0] if len(matches) == 1 else None

    async def invalidate_engine(self, instance_id: str, engine_epoch: str) -> int:
        async with self._lock:
            keys = [
                key
                for key, fact in self._facts.items()
                if fact.instance_id == instance_id and fact.engine_epoch != engine_epoch
            ]
            for key in keys:
                fact = self._facts.pop(key)
                self._handle_owner.pop(
                    (fact.instance_id, fact.engine_epoch, fact.kv_handle), None
                )
            current = self._engine_epochs.get(instance_id)
            if current is None or current[0] != engine_epoch:
                self._engine_epochs[instance_id] = (engine_epoch, datetime.now(UTC))
            return len(keys)

    async def facts_for_line(self, job_id: str, line_id: str) -> tuple[KVFact, ...]:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            return tuple(
                fact
                for key, fact in self._facts.items()
                if key[:2] == (job_id, line_id)
            )

    async def recommend_action(
        self,
        projection: SchedulingProjection,
        *,
        instance_id: str,
        session_id: str,
        now: datetime | None = None,
    ) -> KVActionRecommendation:
        now = now or datetime.now(UTC)
        fact = await self.get(
            instance_id,
            session_id,
            job_id=projection.job_id,
            line_id=projection.line_id,
        )
        base = (
            session_id,
            instance_id,
            projection.tail_request_id,
            projection.tail_version,
        )
        if fact is None:
            return KVActionRecommendation(
                "unsupported", *base, None, "kv_telemetry=unsupported"
            )
        extra = (fact.kv_handle, fact.engine_epoch, fact.generation)
        if fact.tier is KVTier.GPU:
            return KVActionRecommendation(
                "keep", *base, None, "kv already resident on GPU", *extra
            )
        if projection.t_need is None or fact.restore_cost_ms is None:
            return KVActionRecommendation(
                "offload", *base, None, "no factual request-2 readiness time", *extra
            )
        start_at = projection.t_need - timedelta(milliseconds=fact.restore_cost_ms)
        return KVActionRecommendation(
            "restore",
            *base,
            max(start_at, now),
            "restore before factual T_need",
            *extra,
        )

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            self._expire_locked(datetime.now(UTC))
            return [
                item.__dict__
                if hasattr(item, "__dict__")
                else {
                    "job_id": item.job_id,
                    "line_id": item.line_id,
                    "llm_call_id": item.llm_call_id,
                    "session_id": item.session_id,
                    "instance_id": item.instance_id,
                    "engine_epoch": item.engine_epoch,
                    "kv_handle": item.kv_handle,
                    "generation": item.generation,
                    "tier": item.tier.value,
                    "bytes": item.bytes,
                    "restore_cost_ms": item.restore_cost_ms,
                    "migration_cost_ms": item.migration_cost_ms,
                    "rematerialization_cost_ms": item.rematerialization_cost_ms,
                    "observed_at": item.observed_at.isoformat(),
                    "sequence": item.sequence,
                    "schema_version": item.schema_version,
                }
                for item in self._facts.values()
            ]

    def _expire_locked(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.fact_ttl_seconds)
        for key, fact in tuple(self._facts.items()):
            if fact.observed_at <= cutoff:
                self._facts.pop(key, None)
                self._handle_owner.pop(
                    (fact.instance_id, fact.engine_epoch, fact.kv_handle), None
                )

    def _trim_locked(self) -> None:
        self._expire_locked(datetime.now(UTC))
        if len(self._facts) <= self.max_entries:
            return
        ordered = sorted(self._facts.items(), key=lambda item: item[1].observed_at)
        for key, fact in ordered[: len(self._facts) - self.max_entries]:
            self._facts.pop(key, None)
            self._handle_owner.pop(
                (fact.instance_id, fact.engine_epoch, fact.kv_handle), None
            )


def to_state_fact(fact: KVFact) -> KVStateFact:
    return KVStateFact(
        **{
            "job_id": fact.job_id,
            "line_id": fact.line_id,
            "llm_call_id": fact.llm_call_id,
            "instance_id": fact.instance_id,
            "engine_epoch": fact.engine_epoch,
            "session_id": fact.session_id,
            "kv_handle": fact.kv_handle,
            "generation": fact.generation,
            "tier": fact.tier,
            "bytes": fact.bytes,
            "restore_cost_ms": fact.restore_cost_ms,
            "migration_cost_ms": fact.migration_cost_ms,
            "rematerialization_cost_ms": fact.rematerialization_cost_ms,
            "observed_at": fact.observed_at,
            "sequence": fact.sequence,
        }
    )
