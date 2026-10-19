"""Atomic append and full-state ingestion contracts and an in-memory reference sink.

``commit`` owns a transaction; ``stage`` explicitly joins an existing one. Accepted state and
rejection records persist together; publication of
rejections is a separate, at-least-once operation: a :class:`~edax.outbox.Relay` drains a
:class:`RejectionOutbox`. A target generation fixes its source and mapper.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Any, Literal, Protocol, TypedDict, cast

from . import _native
from ._json import decode, encode
from ._owned import ThreadBound
from ._units import own_write, transaction_guard
from .erasure import ScopeCleanup
from .errors import BadHandle, CodecError
from .outbox import check_page
from .sealing import AtRest, KeyScope, ScopeGuard, Sealing, SealPart

__all__ = [
    "BatchInput",
    "BatchResult",
    "BatchSink",
    "StagedBatchResult",
    "TransactionalBatchSink",
    "IngestionPlan",
    "IngestionRow",
    "IngestionRejection",
    "plan_ingestion",
    "open_private",
    "prepare_ingestion",
    "IngestionTarget",
    "MappedRow",
    "MemoryIngestionSink",
    "Rejection",
    "RejectionOutbox",
]


@dataclass(frozen=True)
class MappedRow:
    """A mapper's row key and complete state, in two parts. ``private`` holds what belongs to the event's
    erasure subject: a sink seals it under the subject's key unless its target is
    ``at_rest="plaintext"``. ``public`` stays queryable plaintext. Both ``None`` retains a deletion
    marker."""

    key: str
    private: Any = None
    public: Any = None


@dataclass(frozen=True)
class BatchInput:
    """An original source record. Sinks validate every original before compaction."""

    source: str
    key: str
    stream: str
    event_id: str
    sequence: int | None
    content: Any
    private: Any
    public: Any
    raw: str
    raw_fingerprint: str
    failure: str | None = None
    #: The record's key was destroyed: the sink accepts it without a write.
    erased: bool = False
    #: The kernel's keyed fingerprint of a sealed record; ``None`` hashes the plaintext unkeyed.
    fingerprint: str | None = None
    #: The tenant and erasure subject the row key scopes: the write fence a sealed commit takes. The
    #: kernel reads them from the key, not from these fields.
    tenant: str | None = None
    subject: str | None = None


@dataclass(frozen=True)
class IngestionTarget:
    """A fixed projection generation, source incarnation, and mapper identity.

    Use a new generation when the mapper or source changes. Upserts require one aggregate stream
    per row key and complete state. Append keys are event identities within their tenant.

    ``at_rest`` is how the target stores a subject's ``private`` part. ``"sealed"`` keeps it under the
    subject's key, so destroying the key erases it, backups included; the sink needs ``sealing=``.
    ``"plaintext"`` keeps it readable: register the sink's ``cleanup()`` with the erasure coordinator,
    which edax does not check. A generation never changes it.
    """

    mode: Literal["append", "upsert"]
    generation: str
    source_incarnation: str
    mapper_id: str
    at_rest: AtRest = "sealed"

    def __post_init__(self) -> None:
        self.validate(())

    @property
    def manifest(self) -> str:
        """The immutable target contract encoded for the kernel and durable sink manifest."""
        return encode(self)

    @property
    def prefix(self) -> str:
        """The kernel's unambiguous prefix for this generation's keys."""
        return _native.generation_prefix(self.generation)

    def key(self, parts: list[Any]) -> str:
        """Build a scoped key from ordered components using the kernel's canonical encoding."""
        return _native.generation_key(self.generation, encode(parts))

    def validate(self, inputs: Sequence[BatchInput]) -> None:
        """Refuse records from another target, mode, or source incarnation."""
        _native.ingest_validate_inputs(self.manifest, encode(inputs))

    def check_binding(self, previous: Mapping[str, Any] | None) -> None:
        """Check the durable manifest before reading decisions. Persist ``manifest`` atomically on
        first use; serialize concurrent first binds and keep it after row retention/cleanup."""
        _native.ingest_bind_contract(encode(previous), self.manifest)

    def input(
        self,
        topic: str,
        partition: int,
        offset: int,
        tenant: str | None,
        row_key: str,
        stream: str,
        event_id: str,
        sequence: int | None,
        content: Any,
        private: Any,
        raw: str,
        failure: str | None = None,
        raw_fingerprint: str | None = None,
        *,
        public: Any = None,
        subject: str | None = None,
        erased: bool = False,
        fingerprint: str | None = None,
    ) -> BatchInput:
        """Build an original with scoped source and row keys, then validate its target contract.

        In append mode ``row_key`` must equal ``event_id``. ``raw`` is the lossless JSON source
        representation used by rejection publication. Content includes the full event envelope.
        ``raw_fingerprint`` identifies the original source record; a source whose ``raw`` is the
        whole record omits it, and the kernel fingerprints ``raw``. ``subject`` is the event's erasure
        subject: it scopes the row key, so an erasure cleanup finds every row of the subject.
        """
        original = BatchInput(
            source=self.source_key(topic, partition, offset),
            key=self.row_key(tenant, row_key, subject),
            stream=stream,
            event_id=event_id,
            sequence=sequence,
            content=content,
            private=private,
            public=public,
            raw=raw,
            raw_fingerprint=(
                _native.ingest_raw_fingerprint(raw) if raw_fingerprint is None else raw_fingerprint
            ),
            failure=failure,
            erased=erased,
            fingerprint=fingerprint,
            tenant=tenant,
            subject=subject,
        )
        self.validate((original,))
        return original

    def source_key(self, topic: str, partition: int, offset: int) -> str:
        """The key of one source record in this generation: the kernel's exact coordinates."""
        return _native.ingest_source_key(self.generation, self.source_incarnation, topic, partition, offset)

    def input_from_source(
        self,
        source_id: str,
        *,
        event_id: str,
        stream: str,
        row: MappedRow,
        content: Any,
        raw: str,
        tenant: str | None = None,
        subject: str | None = None,
        sequence: int | None = None,
        failure: str | None = None,
    ) -> BatchInput:
        """Build a direct original without transport coordinates or a wire envelope.

        Preserve ``source_id`` and ``raw`` across retries. The ID names the original/rejection;
        append replay uses ``event_id``, while upsert uses its row owner and ``sequence``.
        Supply authenticated tenant/subject metadata at the gateway. No ID creates replayability.
        """
        original = BatchInput(
            source=_native.ingest_record_source_key(
                self.generation, encode({"incarnation": self.source_incarnation, "source_id": source_id})
            ),
            key=self.row_key(tenant, row.key, subject),
            stream=stream,
            event_id=event_id,
            sequence=sequence,
            content=content,
            private=row.private,
            public=row.public,
            raw=raw,
            raw_fingerprint=_native.ingest_raw_fingerprint(raw),
            failure=failure,
            tenant=tenant,
            subject=subject,
        )
        self.validate((original,))
        return original

    def scope_range(self, scope: KeyScope) -> tuple[str, str]:
        """The key range ``[start, end)`` of ``scope``'s rows: a subject's, or a whole tenant's."""
        start, end = decode(_native.ingest_scope_range(self.manifest, scope.subject, scope.tenant))
        return start, end

    def row_key(self, tenant: str | None, row_key: str, subject: str | None = None) -> str:
        """The key of one target row in this generation, scoped by its tenant and erasure subject."""
        return _native.ingest_row_key(
            self.manifest, encode({"row_id": row_key, "tenant": tenant, "subject": subject})
        )


@dataclass(frozen=True)
class BatchResult:
    """An outcome per original plus committed rejection publication records.

    Verified replays may carry the current raw representation for bounded inline publication;
    the sink retains the first durable raw representation for independent outbox recovery.
    """

    outcomes: tuple[str, ...]
    rejections: dict[str, dict[str, Any]]

    @classmethod
    def from_json(cls, value: str) -> BatchResult:
        """Read the kernel result while keeping target writes private to the sink."""
        result = decode(value)
        return cls(tuple(result["outcomes"]), result["rejections"])


@dataclass(frozen=True)
class StagedBatchResult:
    """Provisional outcomes. Only a successful outer commit makes these durable.

    Do not acknowledge a source, publish a rejection, or report success from this result before
    that commit. Rollback discards both accepted state and rejection records.
    """

    outcomes: tuple[str, ...]
    rejections: dict[str, dict[str, Any]]


class BatchSink(Protocol):
    """An atomic ingestion sink with a durable, independently publishable rejection outbox."""

    @property
    def target(self) -> IngestionTarget: ...

    #: Seals and opens each subject's private part; a runner without its own ``sealing=`` uses it.
    sealing: Sealing | None

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult: ...

    async def pending(self, limit: int = 100) -> dict[str, dict[str, Any]]: ...

    async def published(self, confirmed: Mapping[str, str]) -> None: ...

    async def get(
        self, tenant: str | None, row_key: str, subject: str | None = None
    ) -> dict[str, Any] | None: ...


class TransactionalBatchSink(BatchSink, Protocol):
    """A sink that explicitly stages in an existing transaction. ``commit`` still owns its boundary."""

    async def stage(self, inputs: Sequence[BatchInput], *, tx: Any = None) -> StagedBatchResult: ...

    async def get(
        self, tenant: str | None, row_key: str, subject: str | None = None, *, tx: Any = None
    ) -> dict[str, Any] | None: ...


class IngestionRow(TypedDict):
    """Durable kernel metadata and full state. Retain metadata even for a deletion marker."""

    stream: str
    sequence: int | None
    fingerprint: str
    private: Any
    public: Any


class IngestionRejection(TypedDict):
    """A durable refusal. Preserve the first raw representation and publication flag on replay."""

    fingerprint: str
    error: str
    raw: str
    published: bool


@dataclass(frozen=True)
class IngestionPlan:
    """Kernel decisions for a custom sink, still provisional until atomic persistence.

    Read rows and rejections under serialization that lasts through applying this plan. Replace
    ``writes`` in full (including null parts); insert only absent ``rejections``. A raced absent
    key must abort and retry the whole transaction. Persist both sets atomically. ``json`` retains
    exact numeric tokens for stores that would round them through a host JSON decoder.
    """

    json: str

    @property
    def outcomes(self) -> tuple[str, ...]:
        return tuple(decode(self.json)["outcomes"])

    @property
    def writes(self) -> dict[str, IngestionRow]:
        return cast(dict[str, IngestionRow], decode(self.json)["writes"])

    @property
    def rejections(self) -> dict[str, IngestionRejection]:
        return cast(dict[str, IngestionRejection], decode(self.json)["rejections"])


def plan_ingestion(
    target: IngestionTarget,
    inputs: Sequence[BatchInput],
    stored: Mapping[str, IngestionRow],
    rejected: Mapping[str, IngestionRejection],
) -> IngestionPlan:
    """Validate prepared originals and plan append/upsert through the shared kernel.

    Use :func:`prepare_ingestion` before this call. ``stored`` contains current metadata for every
    input row; ``rejected`` contains durable decisions for every input source. A custom sink must
    check/bind the target manifest and serialize these reads with its writes. This function neither
    locks nor persists. Read rejections after acquiring row locks so a waited-for writer is visible.
    """
    encoded = encode(inputs)
    _native.ingest_check_sealed(target.manifest, encoded)
    return IngestionPlan(_native.ingest_plan(target.mode, encoded, encode(stored), encode(rejected)))


@contextlib.asynccontextmanager
async def prepare_ingestion(
    target: IngestionTarget,
    sealing: Sealing | None,
    inputs: Sequence[BatchInput],
    *,
    transactor: object | None = None,
    tx: object | None = None,
) -> AsyncIterator[tuple[BatchInput, ...]]:
    """Snapshot, fence, fingerprint and seal originals for a sink's atomic write.

    Hold this context through confirmed commit/rollback. A staged sink first validates its
    transaction handle, then supplies ``transactor`` and ``tx`` to retain fences until that
    transaction ends. Subject/tenant scopes come from validated kernel keys. Reuse an existing
    keyed fingerprint when a runner has already prepared the record.
    """
    if tx is not None and transactor is None:
        raise BadHandle("preparing ingestion with tx requires its transactor to retain scope fences")
    originals = deepcopy(tuple(inputs))
    scopes: list[tuple[str | None, str | None]] = [
        tuple(scope) for scope in decode(_native.ingest_write_scopes(target.manifest, encode(originals)))
    ]
    async with contextlib.AsyncExitStack() as guards:
        if sealing is not None:
            order = sorted(
                {
                    scope
                    for item, scope in zip(originals, scopes, strict=True)
                    if not item.erased and item.failure is None
                },
                key=lambda scope: (
                    scope[0] is not None,
                    scope[0] or "",
                    scope[1] is not None,
                    scope[1] or "",
                ),
            )
            for scope in order:
                guard = sealing.write_guard(*scope)
                if transactor is not None:
                    guard = transaction_guard(transactor, guard, tx)
                elif isinstance(guard, ScopeGuard):
                    guard = guard.for_owned_write()
                await guards.enter_async_context(guard)
            keys = sealing.for_batch(None)
            prepared = list(originals)
            for index, (item, scope) in enumerate(zip(originals, scopes, strict=True)):
                if (
                    scope[1] is not None
                    and not item.erased
                    and item.failure is None
                    and item.fingerprint is None
                ):
                    key = await keys.fingerprint_key(*scope)
                    state = encode({"content": item.content, "private": item.private, "public": item.public})
                    try:
                        fingerprint = _native.ingest_keyed_fingerprint(key, state)
                    except CodecError as error:
                        # Canonical payload failures are durable rejections, as in the source runner.
                        prepared[index] = replace(item, failure=f"{error.code}: invalid ingestion state")
                    else:
                        prepared[index] = replace(item, fingerprint=fingerprint)
            originals = tuple(prepared)
        originals = await seal_private(target, sealing, originals)
        _native.ingest_check_sealed(target.manifest, encode(originals))
        yield originals


async def seal_private(
    target: IngestionTarget, sealing: Sealing | None, inputs: Sequence[BatchInput]
) -> tuple[BatchInput, ...]:
    """``inputs`` as the target stores them: each private part the kernel's seal plan names, sealed with
    one key reservation per subject. The kernel's ``check_sealed`` then refuses anything left plaintext,
    so a sink without ``sealing`` fails before it writes."""
    originals = tuple(inputs)
    plan: list[tuple[int, SealPart]] = decode(_native.ingest_seal_plan(target.manifest, encode(originals)))
    if not plan or sealing is None:
        return originals
    sealed = await sealing.for_batch(None).seal_stored(
        [(part, originals[index].private) for index, part in plan]
    )
    result = list(originals)
    for (index, _), private in zip(plan, sealed, strict=True):
        result[index] = replace(originals[index], private=private)
    return tuple(result)


async def open_private(
    target: IngestionTarget, sealing: Sealing | None, key: str, row: dict[str, Any] | None
) -> dict[str, Any] | None:
    """``row``, as a sink stores it at ``key``, with the private part the kernel's open plan names opened."""
    if row is None:
        return None
    part: SealPart | None = decode(_native.ingest_open_plan(target.manifest, key, encode(row)))
    if part is None:
        return row
    if sealing is None:
        raise BadHandle("opening a sealed ingestion row needs the sink's sealing=")
    return {**row, "private": await sealing.open_stored(part, row["private"])}


class MemoryIngestionSink:
    """Atomic reference sink whose state lives as long as this object.

    Keep the instance across runner restarts to exercise replay. Native state is bound to the
    constructing thread, like other memory backends. Calls perform no asynchronous native work.
    ``sealing`` seals each subject's ``private`` part of a sealed target, and opens it in :meth:`get`.
    """

    def __init__(self, target: IngestionTarget, *, sealing: Sealing | None = None) -> None:
        self._target = target
        self.sealing = sealing
        self._database = ThreadBound(_native.memory_ingestion(), "MemoryIngestionSink")

    @property
    def target(self) -> IngestionTarget:
        return self._target

    def cleanup(self, *, key_namespace: str | None = None) -> ScopeCleanup:
        """The erasure participant that deletes a scope's rows: a subject's, or a whole tenant's. A
        ``plaintext`` target or subjectless state needs it. ``key_namespace`` defaults to the sink's
        sealing namespace and is required without sealing. Rejections retain the raw source record."""

        async def delete(scope: KeyScope) -> int:
            return self._database.get().delete_scope(*self.target.scope_range(scope))

        return ScopeCleanup(delete, sealing=self.sealing, key_namespace=key_namespace)

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
        async with prepare_ingestion(self.target, self.sealing, inputs) as originals, own_write():
            return BatchResult.from_json(self._database.get().commit(self.target.manifest, encode(originals)))

    async def pending(self, limit: int = 100) -> dict[str, dict[str, Any]]:
        check_page(limit)
        return cast(
            dict[str, dict[str, Any]], decode(self._database.get().pending(self.target.prefix, limit))
        )

    async def published(self, confirmed: Mapping[str, str]) -> None:
        self._database.get().published_many(encode(dict(confirmed)))

    async def get(
        self, tenant: str | None, row_key: str, subject: str | None = None
    ) -> dict[str, Any] | None:
        key = self.target.row_key(tenant, row_key, subject)
        row = cast(dict[str, Any] | None, decode(self._database.get().get(key)))
        return await open_private(self.target, self.sealing, key, row)


@dataclass(frozen=True)
class Rejection:
    """A committed rejection, as a :class:`~edax.outbox.Relay` drains it from a :class:`RejectionOutbox`.
    ``source`` is the immutable source coordinate destinations deduplicate by; ``error`` is the kernel's
    wire form; ``raw`` is the original record. Each rejection orders alone: it has no tenant or aggregate."""

    source: str
    fingerprint: str
    error: str
    raw: str
    tenant: None = None
    aggregate_id: None = None

    @property
    def id(self) -> str:
        """The relay's row id. It carries the fingerprint so marking stays fenced to this exact record."""
        return encode([self.source, self.fingerprint])

    @classmethod
    def saved(cls, source: str, record: Mapping[str, Any]) -> Rejection:
        """The rejection a sink stores at ``source``."""
        return cls(source, record["fingerprint"], record["error"], record["raw"])


class RejectionOutbox:
    """A :class:`BatchSink`'s committed rejections for :class:`~edax.outbox.Relay`. Drain it with
    ``Relay(RejectionOutbox(sink), publish)``. Delivery is at least once: a crash between delivery and
    marking repeats it. Unlike the general :class:`~edax.outbox.Outbox` unknown-id no-op, a decoded
    source/fingerprint pair raises :class:`~edax.errors.BadHandle` if the source is missing or its
    fingerprint changed. Malformed ids are ignored. Mark only ids returned by this adapter."""

    def __init__(self, sink: BatchSink) -> None:
        self.sink = sink

    async def unpublished(self, limit: int) -> list[Rejection]:
        pending = await self.sink.pending(limit)
        return [Rejection.saved(source, record) for source, record in pending.items()]

    async def mark_published(self, ids: Sequence[str]) -> None:
        confirmed: dict[str, str] = {}
        for row_id in ids:
            # Ignore malformed ids; decoded pairs retain the sink's strict publication checks.
            with contextlib.suppress(CodecError, TypeError, ValueError):
                source, fingerprint = decode(row_id)
                confirmed[source] = fingerprint
        await self.sink.published(confirmed)
