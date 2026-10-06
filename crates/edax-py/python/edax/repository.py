"""Save/load orchestration: a :class:`Repository` rehydrates an aggregate from its snapshot and events,
and saves its pending events, a snapshot and the commands it sends in one unit of work."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import Generic

from . import _native
from ._lineage import stamp
from ._observer import wrap
from ._units import open_transaction
from ._wiring import wiring_u64
from .aggregate import A, AggregateCodec, EventCodec, stand_at
from .message import CommandMessage, EventMessage, Lineage
from .observe import Observer
from .outbox import CommandOutbox
from .store import EventStore, Snapshot, check_sequence_batch
from .tenancy import Tenancy, tenant_key
from .unit_of_work import TxT, current_tx

logger = logging.getLogger("edax.repository")

__all__ = ["Repository"]


class Repository(Generic[A, TxT]):
    """Loads aggregates (snapshot, then replay the rest) and saves them (append the pending events and a
    fresh snapshot in one transaction; the store also enqueues the outbox). ``load`` and ``save`` take an
    optional ``tx``; without one they join the unit of work the caller opened with
    :func:`~edax.unit_of_work.transaction`, if any, else run in their own, so the events commit with a
    projection or idempotency write in the same transaction and nothing ever nests.

    ``snapshot_every`` is how often a save writes a snapshot: each time the aggregate's version crosses a
    multiple of it, so ``1`` (the default) snapshots on every save and ``0`` never does; the kernel's
    rule, pinned by ``testdata/golden/snapshot_due.json``. A snapshot is a cache that doubles the writes
    of the save that takes it; a stream that is cheap to replay wants a larger interval. A value the
    kernel cannot carry (negative, past an unsigned 64-bit integer, not an ``int``) raises the terminal
    :class:`~edax.errors.BadHandle`.

    ``new_id`` names each event, and roots the flow of a save no message caused (default: the kernel's
    time-ordered :func:`~edax.guid`); override it for deterministic tests. The store, not the repository,
    stamps the time an event is recorded.

    ``observer`` receives one :meth:`~edax.observe.Observer.transaction` per unit of work the repository
    opens itself. A call that joined the caller's ``tx`` reports nothing; that caller's
    :func:`~edax.unit_of_work.transaction` does. ``None`` is the no-op observer.

    ``commands`` is the :class:`CommandOutbox` that ``save(..., commands=...)`` writes to, over the event
    store's database. Without one, a save that sends commands raises :class:`TypeError`.

    ``tenancy`` is how this repository separates tenants, fixed here rather than per call so no two
    calls of one service can key differently. The default :attr:`~edax.tenancy.Tenancy.SILO` takes no
    tenant and composes nothing. Under :attr:`~edax.tenancy.Tenancy.POOL` every stream and snapshot
    key is the aggregate id composed with its tenant, and a ``load`` that names none raises
    :class:`~edax.errors.CodecError` rather than reading the whole pool. Under
    :attr:`~edax.tenancy.Tenancy.BRIDGE` a tenant is required and keys stay bare, because this
    repository's store already holds one tenant. See ``docs/guides/multi-tenancy.md``."""

    def __init__(
        self,
        store: EventStore[TxT],
        events: EventCodec,
        aggregates: AggregateCodec[A],
        *,
        new_id: Callable[[], str] | None = None,
        observer: Observer | None = None,
        snapshot_every: int = 1,
        commands: CommandOutbox[TxT] | None = None,
        tenancy: Tenancy = Tenancy.SILO,
    ) -> None:
        self._snapshot_every = wiring_u64(snapshot_every, "snapshot_every")
        self._tenancy = tenancy
        self._store = store
        self._events = events
        self._aggregates = aggregates
        self._new_id = new_id or _native.guid
        self._observer = wrap(observer)
        self._commands = commands

    @asynccontextmanager
    async def _enlist(self, tx: TxT | None) -> AsyncIterator[TxT]:
        """Yield ``tx``, else the ambient :func:`current_tx` of the store's database, else a unit of work of
        our own."""
        if tx is None:
            tx = current_tx(self._store)
        if tx is not None:
            yield tx
        else:
            async with open_transaction(self._store, self._observer) as own:
                yield own

    async def load(self, aggregate_id: str, *, tenant: str | None = None, tx: TxT | None = None) -> A:
        """Rehydrate the aggregate: snapshot, then replay the events after it.

        ``tenant`` names whose aggregate this is. The key the store is called with follows from it and
        the repository's ``tenancy`` (:func:`~edax.tenancy.tenant_key`), and the loaded aggregate
        carries it, so the save that follows stamps and keys the same tenant.

        A snapshot is a cache of the event log, so one this codec cannot use (another aggregate type's
        under the same id, or one whose ``dump`` format ``load`` no longer reads) is ignored and the stream
        replays from version 0. The replayed run is held to the kernel's dense-batch rule, so a store that
        returns a gap or a repeat raises :class:`~edax.errors.CodecError` instead of folding the wrong
        state."""
        key = tenant_key(self._tenancy, tenant, aggregate_id)
        async with self._enlist(tx) as tx:
            snap = await self._store.load_snapshot(tx, key)
            cached = self._from_snapshot(aggregate_id, snap)
            base, agg = cached if cached is not None else (0, self._aggregates.load(aggregate_id, 0, None))
            stored = await self._store.load(tx, key, after=base)
            version = check_sequence_batch(base, [msg.sequence for msg in stored])
            for msg in stored:
                # transition(), not apply(): a replayed event must not be re-queued as pending
                agg.transition(self._events.unmarshal(msg))
            stand_at(agg, version, tenant=tenant)
            return agg

    def _from_snapshot(self, aggregate_id: str, snap: Snapshot | None) -> tuple[int, A] | None:
        """The aggregate rebuilt from ``snap`` and the version it stands at, or ``None`` to replay from 0."""
        if snap is None:
            return None
        if snap.type_name != self._aggregates.type:
            logger.warning(
                "edax repository: ignoring the %r snapshot of %r, which this %r codec does not read; "
                "replaying from version 0",
                snap.type_name,
                aggregate_id,
                self._aggregates.type,
            )
            return None
        try:
            return snap.version, self._aggregates.load(aggregate_id, snap.version, snap.data)
        except Exception:
            logger.warning(
                "edax repository: the snapshot of %r at version %d does not load; replaying from version 0",
                aggregate_id,
                snap.version,
                exc_info=True,
            )
            return None

    async def save(
        self,
        aggregate: A,
        *,
        lineage: Lineage | None = None,
        extensions: dict[str, str] | None = None,
        metadata: dict[str, str] | None = None,
        occurred_at: int | None = None,
        commands: Sequence[CommandMessage] | None = None,
        tx: TxT | None = None,
    ) -> list[EventMessage]:
        """Persist the pending events, and a snapshot when the version crosses a multiple of
        ``snapshot_every``; return the stored messages, for the :class:`~edax.outbox.Relay` or for an eager
        publish. Under a ``tx`` the caller commits, they are not committed until it does.

        ``commands`` go to the repository's :class:`CommandOutbox` in the same transaction as the events,
        so a command is sent only if the events it follows from landed. Build each with
        :func:`~edax.command.command_message`. A repository built without an outbox raises
        :class:`TypeError` before anything is written. Each command belongs to the aggregate's tenant, as
        its events do: one that names no tenant is stored with the aggregate's, and one that names another
        raises :class:`~edax.errors.CodecError` before anything is written.

        Every event of one save shares one ``lineage``: the one given, else the ambient
        :func:`~edax.causation.current_lineage` of the message being handled, else a new flow.
        ``extensions`` carry edax extension context, such as a trace parent, under the kernel's key rule;
        a key outside it raises :class:`~edax.errors.CodecError` before anything is written. ``metadata`` is
        the application's own. ``occurred_at`` is the valid time of the batch in unix nanoseconds, when the
        facts happened earlier than they are recorded (an import, an offline client); ``None`` means they
        happened when the store records them."""
        if commands is not None and self._commands is None:
            raise TypeError("save(commands=...) needs a Repository built with commands=CommandOutbox")
        # The tenant is the aggregate's, never this call's, so a save cannot name one the load did
        # not. A bare aggregate under a mode that requires a tenant fails here, before any write.
        tenant = aggregate.tenant
        key = tenant_key(self._tenancy, tenant, aggregate.id)
        payloads = [self._events.marshal(e) for e in aggregate.pending]
        # A value the binding cannot convert (a lone surrogate in an id, a non-str metadata value) fails
        # the same way on every retry, so it is the terminal CodecError, not a bare TypeError.
        flow, ext, meta = stamp(lineage, extensions, metadata, self._new_id)
        ids = [self._new_id() for _ in payloads]
        batch = _native.draft_batch(
            aggregate.id,
            aggregate.version.initial,
            self._snapshot_every,
            payloads,
            ids,
            flow,
            ext,
            meta,
            tenant,
            occurred_at,
        )
        owned = [_native.own_command(command, tenant) for command in commands or ()]
        # Dumped before the first write, like the events: a snapshot that cannot be written fails the save
        # outright, before it costs a staged append.
        snapshot = (
            Snapshot(key, self._aggregates.type, batch.version, self._aggregates.dump(aggregate))
            if batch.snapshot
            else None
        )
        async with self._enlist(tx) as tx:
            stored = await self._store.append(tx, key, aggregate.version.initial, batch.drafts)
            if owned and self._commands is not None:
                await self._commands.enqueue(tx, owned)
            if snapshot is not None:
                await self._store.save_snapshot(tx, snapshot)
        stand_at(aggregate, batch.version, tenant=tenant)
        return stored
