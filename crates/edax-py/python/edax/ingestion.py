"""Atomic append and full-state ingestion contracts and an in-memory reference sink.

Sinks own their transaction. Accepted state and rejection records commit together; publication of
rejections is a separate, at-least-once operation. A target generation fixes its source and mapper.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

from . import _native
from ._json import decode, encode
from ._owned import ThreadBound
from .errors import StoreError

__all__ = [
    "BatchCommitError",
    "BatchInput",
    "BatchResult",
    "BatchSettings",
    "BatchSink",
    "IngestionTarget",
    "MappedRow",
    "MemoryIngestionSink",
    "RejectionPublisher",
]


@dataclass(frozen=True)
class MappedRow:
    """A mapper's row key and complete state. ``None`` retains a deletion marker."""

    key: str
    value: Any


@dataclass(frozen=True)
class BatchInput:
    """An original source record. Sinks validate every original before compaction."""

    source: str
    key: str
    stream: str
    event_id: str
    sequence: int | None
    content: Any
    value: Any
    raw: str
    failure: str | None = None
    raw_fingerprint: str | None = None


@dataclass(frozen=True)
class IngestionTarget:
    """A fixed projection generation, source incarnation, and mapper identity.

    Use a new generation when the mapper or source changes. Upserts require one aggregate stream
    per row key and complete state. Append keys are event identities within their tenant.
    """

    mode: Literal["append", "upsert"]
    generation: str
    source_incarnation: str
    mapper_id: str

    def __post_init__(self) -> None:
        self.validate(())

    @property
    def manifest(self) -> str:
        """The immutable target contract encoded for the kernel and durable sink manifest."""
        return encode(self)

    @property
    def prefix(self) -> str:
        """The kernel's unambiguous prefix for this generation's keys."""
        return _native.ingest_prefix(self.generation)

    def key(self, parts: list[Any]) -> str:
        """Build a scoped key from ordered components using the kernel's canonical encoding."""
        return _native.ingest_key(self.generation, encode(parts))

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
        value: Any,
        raw: str,
        failure: str | None = None,
        raw_fingerprint: str | None = None,
    ) -> BatchInput:
        """Build an original with scoped source and row keys, then validate its target contract.

        In append mode ``row_key`` must equal ``event_id``. ``raw`` is the lossless JSON source
        representation used by rejection publication. Content includes the full event envelope.
        """
        original = BatchInput(
            source=_native.ingest_source_key(
                self.generation, self.source_incarnation, topic, partition, offset
            ),
            key=_native.ingest_row_key(self.manifest, encode({"row_id": row_key, "tenant": tenant})),
            stream=stream,
            event_id=event_id,
            sequence=sequence,
            content=content,
            value=value,
            raw=raw,
            failure=failure,
            raw_fingerprint=raw_fingerprint,
        )
        self.validate((original,))
        return original


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


class BatchCommitError(StoreError):
    """A classified commit failure. ``kind`` is a kernel recovery instruction input.

    An uncertain commit must replay unchanged originals. Only a confirmed rolled-back,
    record-local failure permits isolation. The original failure remains available as ``cause``.
    """

    def __init__(self, kind: str, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.kind = kind
        self.cause = cause


class BatchSink(Protocol):
    """An atomic ingestion sink with a durable, independently publishable rejection outbox."""

    @property
    def target(self) -> IngestionTarget: ...

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult: ...

    async def pending(self, limit: int = 100) -> dict[str, dict[str, Any]]: ...

    async def published(self, source: str, fingerprint: str) -> None: ...

    async def get(self, tenant: str | None, row_key: str) -> dict[str, Any] | None: ...


class MemoryIngestionSink:
    """Atomic reference sink whose state lives as long as this object.

    Keep the instance across runner restarts to exercise replay. Native state is bound to the
    constructing thread, like other memory backends. Calls perform no asynchronous native work.
    """

    def __init__(self, target: IngestionTarget) -> None:
        self._target = target
        self._database = ThreadBound(_native.memory_ingestion(), "MemoryIngestionSink")

    @property
    def target(self) -> IngestionTarget:
        return self._target

    async def commit(self, inputs: Sequence[BatchInput]) -> BatchResult:
        originals = tuple(inputs)
        return BatchResult.from_json(self._database.get().commit(self.target.manifest, encode(originals)))

    async def pending(self, limit: int = 100) -> dict[str, dict[str, Any]]:
        return cast(
            dict[str, dict[str, Any]], decode(self._database.get().pending(self.target.prefix, limit))
        )

    async def published(self, source: str, fingerprint: str) -> None:
        self._database.get().published(source, fingerprint)

    async def get(self, tenant: str | None, row_key: str) -> dict[str, Any] | None:
        key = _native.ingest_row_key(self.target.manifest, encode({"row_id": row_key, "tenant": tenant}))
        return cast(dict[str, Any] | None, decode(self._database.get().get(key)))


_DEFAULTS: dict[str, int] = decode(_native.batch_config("{}"))


@dataclass(frozen=True)
class BatchSettings:
    """Validated runner limits. Defaults and constraints come from the event kernel.

    Retained bytes measure serialized data; broker prefetch, Python object overhead, and memory
    privately allocated by a mapper are outside that bound.
    """

    max_records: int = _DEFAULTS["max_records"]
    max_bytes: int = _DEFAULTS["max_bytes"]
    flush_ms: int = _DEFAULTS["flush_ms"]
    concurrency: int = _DEFAULTS["concurrency"]
    retained_records: int = _DEFAULTS["retained_records"]
    retained_bytes: int = _DEFAULTS["retained_bytes"]
    max_record_bytes: int = _DEFAULTS["max_record_bytes"]
    max_mapped_bytes: int = _DEFAULTS["max_mapped_bytes"]
    max_attempts: int = _DEFAULTS["max_attempts"]

    def __post_init__(self) -> None:
        _native.batch_config(self.json)

    @property
    def json(self) -> str:
        """The native policy configuration shared by admission, batching, and recovery."""
        return encode(self)


class RejectionPublisher:
    """Publish committed rejections and mark them only after confirmed delivery.

    A crash between delivery and marking may repeat publication. Destinations must deduplicate
    by the immutable source coordinate. A callback failure leaves the record pending.
    """

    def __init__(
        self,
        sink: BatchSink,
        publish: Callable[[str, dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.sink = sink
        self._publish = publish

    async def publish(self, source: str, row: dict[str, Any]) -> None:
        """Publish one rejection unless its durable record already confirms publication."""
        if row["published"]:
            return
        await self._publish(source, row)
        await self.sink.published(source, row["fingerprint"])

    async def drain(self, limit: int = 100) -> int:
        """Publish up to ``limit`` pending records; propagate failure and retain unpublished work."""
        pending = await self.sink.pending(limit)
        for source, row in pending.items():
            await self.publish(source, row)
        return len(pending)
