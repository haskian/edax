"""Atomic append and full-state ingestion contracts and an in-memory reference sink.

Sinks own their transaction. Accepted state and rejection records commit together; publication of
rejections is a separate, at-least-once operation: a :class:`~edax.outbox.Relay` drains a
:class:`RejectionOutbox`. A target generation fixes its source and mapper.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal, Protocol, cast

from . import _native
from ._json import decode, encode
from ._owned import ThreadBound
from ._units import own_write
from .erasure import ScopeCleanup
from .errors import BadHandle, CodecError
from .outbox import check_page
from .sealing import AtRest, KeyScope, Sealing, SealPart

__all__ = [
    "BatchInput",
    "BatchResult",
    "BatchSink",
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
        originals = await seal_private(self.target, self.sealing, inputs)
        async with own_write():
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
