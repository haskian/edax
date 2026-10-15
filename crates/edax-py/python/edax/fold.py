"""Ordered delta folds with atomic state, deduplication, checkpoints and outbox intents."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar, cast

from . import _json, _native
from .errors import BadHandle

T = TypeVar("T")


@dataclass(frozen=True)
class FoldTarget:
    """Immutable generation contract; source identities require independent verification.

    ``starts`` is the complete partition/start vector. Partition additions use a new generation.
    ``source_history`` identifies an enforced no-truncation history or a verified history lineage.
    """

    generation: str
    source_identity: str
    source_history: str
    topic: str
    reducer_id: str
    envelope_id: str
    keying_id: str
    effect_namespace: str
    starts: Mapping[int, int]
    _contract: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        contract = _json.encode(
            {
                name: getattr(self, name)
                for name in (
                    "generation",
                    "source_identity",
                    "source_history",
                    "topic",
                    "reducer_id",
                    "envelope_id",
                    "keying_id",
                    "effect_namespace",
                    "starts",
                )
            }
        )
        object.__setattr__(self, "_contract", contract)
        _native.fold_keys(self.request(op="initialize"))

    def request(self, **operation: Any) -> str:
        return _json.encode({"contract": _json.decode(self._contract), "operation": operation})

    @property
    def manifest_key(self) -> str:
        return cast(str, _json.decode(_native.fold_keys(self.request(op="initialize")))[0])

    @property
    def prefix(self) -> str:
        return _native.ingest_prefix(self.generation)


@dataclass(frozen=True)
class SourceProof:
    """Verified cluster/topic incarnation and history, plus current retained/readable bounds."""

    identity: str
    history: str
    beginning: int
    end: int


@dataclass(frozen=True)
class FoldEvent:
    """All reducer-visible input. Offset and delivery attempt are not logical identity."""

    tenant: str | None
    aggregate: str
    event_id: str
    sequence: int
    event_type: str
    schema_version: str
    payload: Any
    metadata: Any


@dataclass(frozen=True)
class FoldIntent:
    """The destination must deduplicate the generated intent ID before applying its effect."""

    key: str
    destination: str
    payload: Any


@dataclass(frozen=True)
class FoldResult:
    """Replacement state, including None, and optional deterministic effects."""

    state: Any
    intents: Sequence[FoldIntent] = ()


@dataclass(frozen=True)
class FoldLease:
    checkpoint_key: str
    partition: int
    owner: str
    epoch: int
    next_offset: int
    expires_ms: int


@dataclass(frozen=True)
class FoldCutover:
    source_identity: str
    source_history: str
    topic: str
    cutoffs: Mapping[int, int]


@dataclass(frozen=True)
class FoldInput:
    offset: int
    event: FoldEvent


@dataclass(frozen=True)
class OutboxIntent:
    id: str
    destination: str
    payload: Any
    partition: int
    offset: int


Reducer = Callable[[Any, FoldEvent], FoldResult]
AssignmentGuard = Callable[[], bool]


class FoldSession(Protocol):
    async def execute(self, **operation: Any) -> Any: ...


class FoldStore:
    """Transaction interface shared by the memory reference and PostgreSQL adapter."""

    def __init__(self, target: FoldTarget) -> None:
        self.target = target

    async def _transaction(self, call: Callable[[FoldSession], Awaitable[T]]) -> T:
        raise NotImplementedError

    async def _execute(self, **operation: Any) -> Any:
        async def execute(session: FoldSession) -> Any:
            return await session.execute(**operation)

        return await self._transaction(execute)

    async def initialize(self) -> None:
        """Explicitly create an empty building generation. Never run against a partial restore."""
        await self._execute(op="initialize")

    async def acquire(
        self,
        partition: int,
        owner: str,
        proof: SourceProof,
        *,
        lease_ms: int = 30_000,
        is_current: AssignmentGuard = lambda: True,
    ) -> FoldLease:
        async def acquire(session: FoldSession) -> FoldLease:
            if not is_current():
                raise BadHandle("obsolete fold assignment")
            lease = FoldLease(
                **await session.execute(
                    op="acquire", partition=partition, owner=owner, lease_ms=lease_ms, proof=proof
                )
            )
            if not is_current():
                raise BadHandle("obsolete fold assignment")
            return lease

        return await self._transaction(acquire)

    async def renew(self, lease: FoldLease, lease_ms: int = 30_000) -> FoldLease:
        return FoldLease(**await self._execute(op="renew", lease=lease, lease_ms=lease_ms))

    async def refresh(
        self,
        lease: FoldLease,
        proof: SourceProof,
        *,
        lease_ms: int = 30_000,
        is_current: AssignmentGuard = lambda: True,
    ) -> FoldLease:
        """Reload authoritative progress and refence an expired but still-current assignment."""

        async def refresh(session: FoldSession) -> FoldLease:
            if not is_current():
                raise BadHandle("obsolete fold assignment")
            result = FoldLease(
                **await session.execute(op="refresh", lease=lease, lease_ms=lease_ms, proof=proof)
            )
            if not is_current():
                raise BadHandle("obsolete fold assignment")
            return result

        return await self._transaction(refresh)

    async def release(self, lease: FoldLease) -> None:
        await self._execute(op="release", lease=lease)

    async def recover(self, lease: FoldLease) -> FoldLease:
        """Reload the database cursor under the same fence after an uncertain commit."""
        return FoldLease(**await self._execute(op="load", lease=lease))

    async def commit(
        self,
        lease: FoldLease,
        inputs: Sequence[FoldInput],
        proof: SourceProof,
        reducer: Reducer,
        *,
        is_current: AssignmentGuard = lambda: True,
    ) -> FoldLease:
        # Snapshot before awaiting storage. A caller cannot mutate reducer input during lock waits.
        originals = _json.decode(_json.encode(inputs))

        async def apply(session: FoldSession) -> FoldLease:
            current = lease
            for item in originals:
                if not is_current():
                    raise BadHandle("obsolete fold assignment")
                operation = dict(lease=current, offset=item["offset"], event=item["event"], proof=proof)
                prepared = await session.execute(op="prepare", **operation)
                event = FoldEvent(**prepared["event"])
                reduced = reducer(prepared["state"], event) if prepared["novel"] else FoldResult(None)
                # Serialize now: reducer aliases cannot change a previous event's staged result.
                current = FoldLease(
                    **await session.execute(
                        op="apply", reduced=_json.decode(_json.encode(reduced)), **operation
                    )
                )
            if not is_current():
                raise BadHandle("obsolete fold assignment")
            return current

        return await self._transaction(apply)

    async def freeze(self) -> FoldCutover:
        """Drain under the generation lock, retire all owners, and capture committed cutoffs."""
        result = await self._execute(op="freeze")
        result["cutoffs"] = {int(p): int(o) for p, o in result["cutoffs"].items()}
        return FoldCutover(**result)

    async def activate(self, cutover: FoldCutover) -> None:
        """Enable effects at/above immutable cutoffs after every partition catches up."""
        await self._execute(op="activate", cutover=cutover)

    async def get(self, tenant: str | None, aggregate: str) -> dict[str, Any] | None:
        return cast(dict[str, Any] | None, await self._execute(op="get", tenant=tenant, aggregate=aggregate))

    async def pending(self, limit: int = 100) -> Sequence[OutboxIntent]:
        raise NotImplementedError

    async def published(self, intent_id: str) -> None:
        await self._execute(op="published", intent_id=intent_id)


def outbox_intent(value: Mapping[str, Any]) -> OutboxIntent:
    return OutboxIntent(**{k: value[k] for k in ("id", "destination", "payload", "partition", "offset")})


class _MemorySession:
    def __init__(self, target: FoldTarget, records: dict[str, Any], clock: Callable[[], int]) -> None:
        self.target, self.records, self.clock = target, records, clock

    async def execute(self, **operation: Any) -> Any:
        request = self.target.request(**operation)
        keys = _json.decode(_native.fold_keys(request))
        records = {k: self.records[k] for k in keys if k in self.records}
        plan = _json.decode(_native.fold_transition(request, _json.encode(records), self.clock()))
        self.records.update(plan["writes"])
        if plan["suppression"] is not None:
            manifest = self.records[self.target.manifest_key]
            for key, intent in tuple(self.records.items()):
                if intent.get("kind") == "intent" and not _native.fold_intent_eligible(
                    _json.encode(manifest), _json.encode(intent)
                ):
                    self.records[key] = {**intent, "suppressed": True}

        return plan["result"]


class MemoryFoldStore(FoldStore):
    """Atomic reference for tests; volatile, with a caller-injectable clock."""

    def __init__(self, target: FoldTarget, *, clock: Callable[[], int] | None = None) -> None:
        super().__init__(target)
        self._clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._records: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def _transaction(self, call: Callable[[FoldSession], Awaitable[T]]) -> T:
        async with self._lock:
            # Values only cross the kernel through JSON; writes replace whole immutable records.
            session = _MemorySession(self.target, dict(self._records), self._clock)
            result = await call(session)
            self._records = session.records
            return result

    async def pending(self, limit: int = 100) -> Sequence[OutboxIntent]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise BadHandle("outbox page limit must be positive")
        async with self._lock:
            manifest = self._records.get(self.target.manifest_key)
            if manifest is None:
                raise BadHandle("fold generation is not initialized")
            # Validate the immutable manifest even for an empty outbox.
            await _MemorySession(self.target, self._records, self._clock).execute(op="initialize")
            pending: list[OutboxIntent] = []
            for key in sorted(self._records):
                value = self._records[key]
                if (
                    value.get("kind") == "intent"
                    and not value["published"]
                    and not value["suppressed"]
                    and _native.fold_intent_eligible(_json.encode(manifest), _json.encode(value))
                ):
                    pending.append(outbox_intent(_json.decode(_json.encode(value))))
                    if len(pending) == limit:
                        break
            return pending


class FoldOutbox:
    """One bounded delivery page. A lost marker or restored backup can repeat an intent ID."""

    def __init__(
        self, store: FoldStore, deliver: Callable[[OutboxIntent], Awaitable[None]], *, timeout_s: float = 30
    ) -> None:
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise BadHandle("outbox timeout must be positive")
        self.store, self.deliver, self.timeout_s = store, deliver, timeout_s

    async def drain(self, limit: int = 100) -> int:
        count = 0
        for intent in await self.store.pending(limit):
            await asyncio.wait_for(self.deliver(intent), self.timeout_s)
            await self.store.published(intent.id)
            count += 1
        return count
