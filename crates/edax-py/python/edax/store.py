"""The event store seam: the durable log an :class:`EventStore` keeps, with its snapshots. The kernel owns
no I/O, so persistence is the SDK's job. No concrete store ships here. ``append`` stages the event outbox
rows in the caller's ``tx``, because a transactional outbox holds only if the event rows and the
to-publish rows commit together. Reading them back is another port, an :class:`~edax.outbox.Outbox` of
:class:`~edax.message.EventMessage`, which one object may implement beside this one."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from . import _native
from .message import EventDraft, EventMessage
from .unit_of_work import Transactor, TxT

__all__ = [
    "EventStore",
    "Snapshot",
    "check_sequence_batch",
]


@dataclass(frozen=True)
class Snapshot:
    """A marshaled aggregate at a given version, so a load can skip replaying the whole stream."""

    aggregate_id: str
    type_name: str
    version: int
    data: bytes


class EventStore(Transactor[TxT], Protocol[TxT]):
    """The durable log: per-aggregate events with optimistic concurrency and aggregate snapshots, under the
    unit of work it opens as a :class:`~edax.unit_of_work.Transactor`, and the outbox rows each ``append``
    stages in the same ``tx``, so state and to-publish rows land together. A
    :class:`~edax.outbox.Relay` reads them back through the :class:`~edax.outbox.Outbox` port, which a store
    may implement too; a store fed to change data capture need not.
    What a backend must guarantee; worked examples in ``docs/backends/README.md``.

    - *Stream key.* ``append``, ``load`` and ``load_snapshot`` name the stream by their ``aggregate_id``
      argument, and ``save_snapshot`` by the snapshot's ``aggregate_id``: never by a draft's own
      ``aggregate_id``. Under :attr:`~edax.tenancy.Tenancy.POOL` the repository passes the id composed with
      the tenant, while every draft keeps the bare id. A store keying its rows by the draft merges two
      tenants' same-named aggregates into one stream.
    - *Versions.* An aggregate's version is the ``sequence`` of its last stored event, ``0`` for an empty
      stream. ``Repository`` assigns ``sequence = expected_version + 1 .. + n``; a store never renumbers.
    - *Optimistic concurrency.* ``append`` succeeds only if the stored version equals ``expected_version``,
      else it raises :class:`~edax.errors.ConcurrencyError` and writes nothing. Enforce that with a unique
      constraint or a row lock in the database, never a read-then-write in Python.
    - *Dense batches.* A batch must carry ``expected_version + 1, + 2, ...`` in order, every draft
      sequenced; anything else raises :class:`~edax.errors.CodecError` and writes nothing. Call
      :func:`check_sequence_batch`, the kernel's rule, rather than restating it.
    - *Atomicity and durability.* Within one ``append`` the event rows, their outbox rows and any snapshot
      saved in the same ``tx`` commit together; a rollback removes all of them and a commit survives a crash.
    - *Recorded time.* ``append`` stamps every draft with the time the store records it, its own clock
      (a database's ``now()``), and returns the stored :class:`~edax.message.EventMessage` per draft, in
      order. ``load`` and the outbox's ``unpublished`` return the same ``recorded_at``. Under an open ``tx``
      the returned messages are not committed yet.
    - *Unique ids.* An event id is unique store-wide: an ``append`` carrying an id the store already
      holds raises :class:`~edax.errors.CodecError` and writes nothing, or marking one row published would
      prune the other unpublished. A primary key on the id gives it.
    - *Reads.* ``load`` returns every event with ``sequence > after``, ascending, gap-free, including this
      ``tx``'s own uncommitted appends; :class:`~edax.repository.Repository` checks the run with the kernel's
      dense-batch rule and fails the load on a gap or a repeat. ``load_snapshot`` returns the latest snapshot
      or ``None``. A snapshot may lag the stream but never lead it: ``save_snapshot`` refuses one past the
      stream's version as ``tx`` sees it with :class:`~edax.errors.CodecError`, and keeps the higher
      version when an older one arrives late.
    - *Outbox rows.* ``append`` stages one outbox row per stored message, that message itself, in the same
      ``tx``: committed with the events, rolled back with them. The :class:`~edax.outbox.Outbox` contract
      says how they are read back.
    - *Errors.* :class:`~edax.errors.ConcurrencyError` only for a version mismatch; any other failure raises
      :class:`~edax.errors.StoreError`, or :class:`~edax.errors.CodecError` for a marshal failure. Called
      from one asyncio loop; no thread safety required."""

    async def load(self, tx: TxT, aggregate_id: str, after: int = 0) -> list[EventMessage]:
        """Events with ``sequence > after``, ascending, gap-free, including ``tx``'s own appends."""
        ...

    async def load_snapshot(self, tx: TxT, aggregate_id: str) -> Snapshot | None:
        """The latest snapshot for the aggregate, or ``None``. May lag the stream; never lead it."""
        ...

    async def save_snapshot(self, tx: TxT, snapshot: Snapshot) -> None:
        """Store the aggregate's snapshot, committed with ``tx``: refused past the stream's version, and
        never replacing a newer one."""
        ...

    async def append(
        self, tx: TxT, aggregate_id: str, expected_version: int, drafts: Sequence[EventDraft]
    ) -> list[EventMessage]:
        """Compare-and-set append: if the stored version equals ``expected_version``, record ``drafts`` and
        their outbox rows atomically in ``tx`` and return the stored messages, each stamped with the
        store's ``recorded_at``; else raise :class:`~edax.errors.ConcurrencyError` and write nothing."""
        ...


def check_sequence_batch(expected_version: int, sequences: Sequence[int | None]) -> int:
    """The kernel's dense-batch rule an :meth:`EventStore.append` must enforce: every draft carries a
    sequence, and the batch runs ``expected_version + 1, + 2, ...`` in order. Returns the stream version
    after the batch, ``expected_version`` for an empty one. A gap, a repeat, a
    reordering or an unsequenced draft raises terminal :class:`~edax.errors.CodecError` naming the
    0-based batch index, because the same batch fails the same way on every retry. An empty batch passes.
    Versions and sequences are unsigned 64-bit, so a negative one is a :class:`~edax.errors.CodecError` too,
    and a batch that would run past ``2**64 - 1`` is refused rather than clamped.
    Pinned by ``testdata/golden/sequence_batch.json``."""
    return _native.check_sequence_batch(expected_version, list(sequences))
