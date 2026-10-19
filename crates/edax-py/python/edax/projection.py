"""Read-model persistence, in two shapes.

:class:`ProjectionStore` saves a current document with an optimistic version and queries it directly. It
is a versioned document store, not a projector: it records no folded events, so a handler skips a
redelivered one with :func:`~edax.router.idempotent_by_id`. A :class:`ProjectionQuery` backend serves
:meth:`~ProjectionQuery.get_many` by pushing the kernel's filter, sort and pagination down to native SQL,
so two services page it identically. See ``docs/sdk/projections.md``.

:class:`ProjectionStateStore` is a projector's store. A :data:`Reducer` folds ordered delta events into
per-aggregate state, and each commit writes the state, event receipts, effect intents and source
checkpoint together. :class:`~edax.backend.kafka_projection.KafkaProjection` drives it. See
``docs/sdk/projection-processing.md``."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields
from typing import Any, Protocol, TypeVar, cast, runtime_checkable

from . import _json, _native
from ._owned import ThreadBound
from .errors import BadHandle, StoreError
from .outbox import check_page
from .query import Document, Page, Query
from .unit_of_work import TxT_contra

__all__ = [
    "EffectIntent",
    "Entity",
    "MemoryProjectionStateStore",
    "ProjectionCutover",
    "ProjectionEvent",
    "ProjectionInput",
    "ProjectionIntent",
    "ProjectionLease",
    "ProjectionQuery",
    "ProjectionResult",
    "ProjectionSession",
    "ProjectionStateStore",
    "ProjectionStore",
    "ProjectionTarget",
    "Reducer",
    "SourceProof",
    "check_document",
    "keyed_document",
]


def check_document(id: str, document: Any) -> None:
    """Refuse ``document`` as the stored document of projection ``id`` exactly as the kernel's stores do:
    one that is not a JSON object (the query reads each row's key from its ``"id"`` field, so a scalar or a
    list could be saved and never found), or one holding an integer wider than 64 bits (stored as the
    nearest float, it would come back changed). Raises the terminal :class:`~edax.errors.CodecError`. Call
    it in your :meth:`ProjectionStore.save` before the write. Pinned by
    ``testdata/golden/projection_document.json``."""
    _json.call(functools.partial(_native.check_projection_document, id), document)


def keyed_document(id: str, document: Any) -> dict[str, Any]:
    """``document``, the stored document of projection ``id``, as :meth:`ProjectionStore.get` and
    :meth:`ProjectionQuery.get_many` return it: with the row key as its ``"id"`` field, overwriting any the
    document carries. The query sorts its keyset tiebreak on that field and mints every cursor from it. A
    document :func:`check_document` refuses raises the same terminal :class:`~edax.errors.CodecError`. Call
    it in your ``get``, or inject the key in your ``get_many`` query. Pinned by
    ``testdata/golden/projection_document.json``."""
    keyed: Document = _json.decode(
        _json.call(functools.partial(_native.keyed_projection_document, id), document)
    )
    return keyed


@dataclass(frozen=True)
class Entity:
    """A read model at a point in time: the queryable ``document`` and the ``version`` it was loaded at
    (``0`` if never saved). A value: :meth:`ProjectionStore.save` persists it at ``version + 1`` and returns
    that new version, leaving this one as it was."""

    id: str
    version: int
    document: Document


class ProjectionStore(Protocol[TxT_contra]):
    """Stores read-model documents keyed by id, with optimistic concurrency: the read side's analogue of
    :class:`~edax.store.EventStore`. A store that also pages them by the kernel's query is a
    :class:`ProjectionQuery`. Pass the ``tx`` from :func:`~edax.unit_of_work.transaction` to commit a
    projection write with the events that produced it. What a backend must guarantee:

    - Nothing in the SDK calls this store, only your handlers, so implement what you use.
    - *Documents.* ``save`` refuses a document :func:`check_document` refuses, with
      :class:`~edax.errors.CodecError`, and writes nothing.
    - *Optimistic concurrency.* ``save`` writes at ``version + 1`` only if the stored version still equals
      ``entity.version`` (``0`` means not yet stored), and returns that new version; else it raises
      :class:`~edax.errors.ConcurrencyError` and writes nothing. Enforce it in the database.
    - *Enlistment.* ``get`` and ``save`` run inside ``tx``, or :func:`~edax.unit_of_work.current_tx`, else
      auto-commit.
    - *Reads.* ``get`` returns the current committed row plus the ``tx``'s own writes, or ``None``. Its
      document carries the row key as ``"id"``, overwriting any the saved document held
      (:func:`keyed_document`).
    - *Errors.* :class:`~edax.errors.ConcurrencyError` only for a version mismatch,
      :class:`~edax.errors.CodecError` for a refused document, else ``StoreError``.

    ``ProjectionStoreContract`` in :mod:`edax.testing` holds a backend to every clause.
    """

    async def get(self, id: str, tx: TxT_contra | None = None) -> Entity | None:
        """The current entity, or ``None`` if absent."""
        ...

    async def save(self, entity: Entity, tx: TxT_contra | None = None) -> int:
        """Persist at ``entity.version + 1`` if the stored version still equals ``entity.version``, and return
        that new version; else raise :class:`~edax.errors.ConcurrencyError`."""
        ...


@runtime_checkable
class ProjectionQuery(Protocol[TxT_contra]):
    """The query side of a :class:`ProjectionStore`, which a store implements beside it to page its
    documents by the kernel's query, so two services page them identically. What a backend must guarantee:

    - *Paging.* ``get_many`` pages identically to the kernel: the filter operators' semantics, the decimal
      collation, the fixed total order over JSON types with stable multi-key sort, keyset resume strictly
      after the cursor tuple, and a :class:`~edax.query.Page` whose ``next`` is minted with
      :func:`~edax.query.encode_cursor` from the last row when more rows follow. Read ``after`` with
      :func:`~edax.query.decode_cursor`, which refuses a cursor minted under another sort.
    - *Rows.* Each row is a document as ``get`` reads it, its row key as ``"id"`` (:func:`keyed_document`).
    - *Specs.* :meth:`~edax.query.Query.to_json` gives the kernel's spec. ``get_many`` refuses a spec
      :func:`~edax.query.check_query` refuses, with :class:`~edax.errors.CodecError`, and reads nothing.

    A backend that treats the spec as its own dialect is no ``ProjectionQuery``: its cursors are not
    portable across services. ``ProjectionStoreContract`` runs the paging clauses for a store that is one.
    """

    async def get_many(self, query: Query = Query(), *, tx: TxT_contra | None = None) -> Page:
        """The page ``query`` selects (filter -> keyset(``after``) -> sort -> offset -> limit), with the
        cursor that resumes after it."""
        ...


T = TypeVar("T")


@dataclass(frozen=True)
class ProjectionTarget:
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
    _manifest_key: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        contract = _json.encode({f.name: getattr(self, f.name) for f in fields(self) if f.init})
        object.__setattr__(self, "_contract", contract)
        # Validates the contract once; the manifest key comes first.
        keys = _json.decode(_native.projection_keys(self.request(op="initialize")))
        object.__setattr__(self, "_manifest_key", cast(str, keys[0]))

    def request(self, **operation: Any) -> str:
        """The kernel request for ``operation`` against this contract."""
        return _json.encode({"contract": _json.decode(self._contract), "operation": operation})

    def check_source(self, proof: SourceProof, cursor: int) -> None:
        """Refuse ``proof`` unless it proves this contract's source holds ``cursor``: the kernel's check."""
        _native.projection_check_source(self._contract, _json.encode(proof), cursor)

    def intent_key(self, intent_id: str) -> str:
        """The storage key of effect intent ``intent_id`` in this generation."""
        return _native.projection_intent_key(self._contract, intent_id)

    @property
    def manifest_key(self) -> str:
        """The generation's manifest key, which also names its locks."""
        return self._manifest_key


@dataclass(frozen=True)
class SourceProof:
    """Verified cluster/topic incarnation and history, plus current retained/readable bounds."""

    identity: str
    history: str
    beginning: int
    end: int


@dataclass(frozen=True)
class ProjectionEvent:
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
class ProjectionIntent:
    """An effect a reducer asks for: a stable ``key`` within its event, a destination and a payload.
    The store keeps it with the state; a :class:`~edax.outbox.Relay` delivers it as an
    :class:`EffectIntent`."""

    key: str
    destination: str
    payload: Any


@dataclass(frozen=True)
class ProjectionResult:
    """Replacement state, including None, and optional deterministic effects."""

    state: Any
    intents: Sequence[ProjectionIntent] = ()


@dataclass(frozen=True)
class ProjectionLease:
    """Database-authoritative ownership of one partition: the owner and its fencing ``epoch``, the next
    source offset to read, and the database-clock expiry in milliseconds."""

    checkpoint_key: str
    partition: int
    owner: str
    epoch: int
    next_offset: int
    expires_ms: int


@dataclass(frozen=True)
class ProjectionCutover:
    """A frozen generation's committed offsets per partition, bound to its source. A rebuilt generation
    activates with it, so each effect is delivered by exactly one generation."""

    source_identity: str
    source_history: str
    topic: str
    cutoffs: Mapping[int, int]


@dataclass(frozen=True)
class ProjectionInput:
    """One source record of a batch: its offset and its decoded event."""

    offset: int
    event: ProjectionEvent


@dataclass(frozen=True)
class EffectIntent:
    """A committed effect a reducer returned, as a :class:`~edax.outbox.Relay` drains it from a
    :class:`ProjectionStateStore`. ``id`` is stable across rebuilds, restores and redelivery, so the
    destination deduplicates by it. The relay orders intents by ``tenant`` and ``aggregate_id``;
    ``partition``, ``offset`` and ``ordinal`` name the source record and the intent's place in it."""

    id: str
    tenant: str | None
    aggregate_id: str
    destination: str
    payload: Any
    partition: int
    offset: int
    ordinal: int


def effect_intent(record: Mapping[str, Any]) -> EffectIntent:
    """The :class:`EffectIntent` a stored intent record holds."""
    return EffectIntent(
        id=record["id"],
        tenant=record["tenant"],
        aggregate_id=record["aggregate"],
        destination=record["destination"],
        payload=record["payload"],
        partition=record["partition"],
        offset=record["offset"],
        ordinal=record["ordinal"],
    )


Reducer = Callable[[Any, ProjectionEvent], ProjectionResult]
AssignmentGuard = Callable[[], bool]


class ProjectionSession:
    """One transaction's view of a generation, the seam a store adapter implements. Each record is read
    once, the kernel decides every operation against those reads, and every change is written once when
    the transaction ends. An adapter supplies :meth:`_lock`, :meth:`_read` and :meth:`_clock`."""

    def __init__(self, target: ProjectionTarget) -> None:
        self.target = target
        self._records: dict[str, Any] = {}
        self._held: Any = None
        self._now: int | None = None
        #: Records read as absent, to insert; a concurrent insert of one fails the transaction.
        self.inserts: dict[str, Any] = {}
        #: Records read as present, to overwrite under the locks this transaction holds.
        self.updates: dict[str, Any] = {}
        #: New effect intents.
        self.intents: dict[str, Any] = {}

    async def _lock(self, exclusive: bool, partition: int | None) -> None:
        raise NotImplementedError

    async def _read(self, keys: Sequence[str]) -> Mapping[str, Any]:
        raise NotImplementedError

    async def _clock(self) -> int:
        raise NotImplementedError

    async def prefetch(self, **operation: Any) -> None:
        """Take the locks and read the records ``operation`` needs, without running it, so a later
        operation of this transaction finds them read."""
        await self._prepare(self.target.request(**operation))

    async def _prepare(self, request: str) -> list[str]:
        reads = _json.decode(_native.projection_reads(request))
        lock = reads["lock"]
        if self._held is None:
            await self._lock(lock["exclusive"], lock["partition"])
            self._held = lock
        elif lock != self._held:
            raise BadHandle("one projection transaction holds one lock")
        keys: list[str] = reads["keys"]
        missing = [key for key in keys if key not in self._records]
        if missing:
            found = await self._read(missing)
            self._records.update({key: found.get(key) for key in missing})
        return keys

    async def execute(self, **operation: Any) -> Any:
        """Run one kernel operation inside this transaction and return its result."""
        request = self.target.request(**operation)
        keys = await self._prepare(request)
        if self._now is None:
            self._now = await self._clock()
        records = {key: self._records[key] for key in keys if self._records[key] is not None}
        plan = _json.decode(_native.projection_transition(request, _json.encode(records), self._now))
        for key, value in plan["inserts"].items():
            self._records[key] = self.inserts[key] = value
        for key, value in plan["updates"].items():
            self._records[key] = value
            (self.inserts if key in self.inserts else self.updates)[key] = value
        self.intents.update(plan["intents"])
        return plan["result"]


def _guard(is_current: AssignmentGuard) -> None:
    """Refuse work for an assignment the runner no longer holds."""
    if not is_current():
        raise BadHandle("obsolete projection assignment")


def _canonical(value: Any) -> Any:
    """``value`` as the kernel stores it, so a reducer sees the same state within a batch as after it."""
    return _json.decode(_native.canonical_json(_json.encode(value).encode()))


class ProjectionStateStore:
    """Transaction interface shared by the memory reference and the PostgreSQL adapter. A subclass runs
    :meth:`_transaction` over its own :class:`ProjectionSession` and serves the outbox read side."""

    def __init__(self, target: ProjectionTarget) -> None:
        self.target = target

    async def _transaction(self, call: Callable[[ProjectionSession], Awaitable[T]]) -> T:
        raise NotImplementedError

    async def _execute(self, **operation: Any) -> Any:
        async def execute(session: ProjectionSession) -> Any:
            return await session.execute(**operation)

        return await self._transaction(execute)

    async def initialize(self) -> None:
        """Explicitly create an empty building generation. Never run against a partial restore."""
        await self._execute(op="initialize")

    async def _fenced(self, is_current: AssignmentGuard, **operation: Any) -> ProjectionLease:
        """Run a lease operation in its own transaction, refused while the caller's assignment is not
        current, both before and after it."""

        async def fenced(session: ProjectionSession) -> ProjectionLease:
            _guard(is_current)
            lease = ProjectionLease(**await session.execute(**operation))
            _guard(is_current)
            return lease

        return await self._transaction(fenced)

    async def acquire(
        self,
        partition: int,
        owner: str,
        proof: SourceProof,
        *,
        lease_ms: int = 30_000,
        is_current: AssignmentGuard = lambda: True,
    ) -> ProjectionLease:
        """Take an unowned or expired partition for ``owner``. A live owner raises
        :class:`~edax.errors.InProgress`; retrying the same ``owner`` after a lost reply returns its lease."""
        return await self._fenced(
            is_current, op="acquire", partition=partition, owner=owner, lease_ms=lease_ms, proof=proof
        )

    async def refresh(
        self,
        lease: ProjectionLease,
        proof: SourceProof,
        *,
        lease_ms: int = 30_000,
        is_current: AssignmentGuard = lambda: True,
    ) -> ProjectionLease:
        """Reload authoritative progress and extend the lease, refencing an expired but current one."""
        return await self._fenced(is_current, op="refresh", lease=lease, lease_ms=lease_ms, proof=proof)

    async def release(self, lease: ProjectionLease) -> None:
        """Give up ``lease`` if it is still the stored owner and fence; otherwise do nothing."""
        await self._execute(op="release", lease=lease)

    async def recover(self, lease: ProjectionLease) -> ProjectionLease:
        """Reload the database cursor under the same fence after an uncertain commit."""
        return ProjectionLease(**await self._execute(op="load", lease=lease))

    async def commit(
        self,
        lease: ProjectionLease,
        inputs: Sequence[ProjectionInput],
        proof: SourceProof,
        reducer: Reducer,
        *,
        lease_ms: int = 30_000,
        is_current: AssignmentGuard = lambda: True,
    ) -> ProjectionLease:
        """Fold one batch in one transaction: extend the lease, run ``reducer`` once per novel event in
        source order, and commit state, receipts, intents and the next offset together."""
        # Snapshot before awaiting storage. A caller cannot mutate reducer input during lock waits.
        originals = _json.decode(_json.encode(inputs))

        async def apply(session: ProjectionSession) -> ProjectionLease:
            # One read for the whole batch: the batch's records with the extension's checkpoint.
            await session.prefetch(op="prepare_batch", lease=lease, inputs=originals, proof=proof)
            # Staging may outlive the lease; the extension shares this transaction's reads.
            current = await session.execute(op="refresh", lease=lease, lease_ms=lease_ms, proof=proof)
            if current["next_offset"] != lease.next_offset:
                # The stored cursor moved (a failover lost a commit, or another owner advanced it):
                # retry from it rather than skip or repeat records.
                raise StoreError("projection cursor moved; reload it and fetch again")
            prepared = await session.execute(op="prepare_batch", lease=current, inputs=originals, proof=proof)
            states: dict[tuple[str | None, str], Any] = {}
            reduced: list[Any] = []
            for item in prepared["items"]:
                _guard(is_current)
                if not item["novel"]:
                    reduced.append(None)
                    continue
                event = ProjectionEvent(**item["event"])
                key = (event.tenant, event.aggregate)
                # Serialize now: reducer aliases cannot change an earlier event's staged result.
                result = _canonical(reducer(states[key] if key in states else item["state"], event))
                states[key] = result["state"]
                reduced.append(result)
            committed = ProjectionLease(
                **await session.execute(
                    op="apply_batch", lease=current, inputs=originals, proof=proof, reduced=reduced
                )
            )
            _guard(is_current)
            return committed

        return await self._transaction(apply)

    async def freeze(self) -> ProjectionCutover:
        """Drain under the generation lock, retire all owners, and capture committed cutoffs."""
        result = await self._execute(op="freeze")
        result["cutoffs"] = {int(p): int(o) for p, o in result["cutoffs"].items()}
        return ProjectionCutover(**result)

    async def activate(self, cutover: ProjectionCutover) -> None:
        """Enable effects at/above immutable cutoffs after every partition catches up."""
        await self._execute(op="activate", cutover=cutover)

    async def get(self, tenant: str | None, aggregate: str) -> dict[str, Any] | None:
        """The stored state of one aggregate, ``{"sequence", "partition", "value"}``, or ``None``."""
        return cast(dict[str, Any] | None, await self._execute(op="get", tenant=tenant, aggregate=aggregate))

    async def unpublished(self, limit: int) -> list[EffectIntent]:
        """Committed effect intents the generation's activation allows, oldest offset first: the
        :class:`~edax.outbox.Outbox` read side. Pass the store to a :class:`~edax.outbox.Relay`."""
        raise NotImplementedError

    async def mark_published(self, ids: Sequence[str]) -> None:
        """Delete each delivered intent. An unknown or ineligible id is a no-op."""
        raise NotImplementedError

    async def prune(self, limit: int = 1000) -> int:
        """Delete up to ``limit`` intents a cutover made permanently ineligible; return how many."""
        raise NotImplementedError


class _MemorySession(ProjectionSession):
    def __init__(self, store: MemoryProjectionStateStore) -> None:
        super().__init__(store.target)
        self._store = store
        #: The version of every record read; the commit refuses if one has changed since.
        self.versions: dict[str, int] = {}

    async def _lock(self, exclusive: bool, partition: int | None) -> None:
        pass  # the commit checks every read instead, so no lock spans the reducer

    async def _read(self, keys: Sequence[str]) -> Mapping[str, Any]:
        read = _json.decode(self._store._database.get().read(_json.encode(keys)))
        self.versions.update(read["versions"])
        return cast(Mapping[str, Any], read["records"])

    async def _clock(self) -> int:
        return self._store._clock()


class MemoryProjectionStateStore(ProjectionStateStore):
    """Atomic reference for tests; volatile, with a caller-injectable clock. Transactions run without a
    lock: a commit whose reads another commit changed raises :class:`~edax.errors.ConcurrencyError`, a
    transient failure the runner retries, as a Postgres insert race does. Native state is bound to the
    constructing thread, like other memory backends."""

    def __init__(self, target: ProjectionTarget, *, clock: Callable[[], int] | None = None) -> None:
        super().__init__(target)
        self._clock = clock or (lambda: _native.current_timestamp() // 1_000_000)
        self._database = ThreadBound(_native.memory_projection_database(), "MemoryProjectionStateStore")

    async def _transaction(self, call: Callable[[ProjectionSession], Awaitable[T]]) -> T:
        session = _MemorySession(self)
        result = await call(session)
        # Only a finished transaction lands, as a database commit does.
        writes = {"inserts": session.inserts, "updates": session.updates, "intents": session.intents}
        self._database.get().commit(_json.encode(session.versions), _json.encode(writes))
        return result

    async def unpublished(self, limit: int) -> list[EffectIntent]:
        check_page(limit)
        pending = self._database.get().unpublished(self.target.manifest_key, self.target.generation, limit)
        return [effect_intent(intent) for intent in _json.decode(pending)]

    async def mark_published(self, ids: Sequence[str]) -> None:
        keys = [self.target.intent_key(intent_id) for intent_id in ids]
        self._database.get().mark_published(
            self.target.manifest_key, self.target.generation, _json.encode(keys)
        )

    async def prune(self, limit: int = 1000) -> int:
        check_page(limit)
        return self._database.get().prune(self.target.manifest_key, self.target.generation, limit)
