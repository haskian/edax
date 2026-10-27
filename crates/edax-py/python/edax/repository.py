"""Save/load orchestration: a :class:`Repository` rehydrates an aggregate from its snapshot and events,
and saves its pending events, a snapshot and the commands it sends in one unit of work."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import replace as replace_snapshot
from typing import Generic

from . import _native
from ._lineage import stamp
from ._observer import wrap
from ._units import open_transaction, transaction_guard
from ._values import replace
from ._wiring import wiring_u64
from .aggregate import A, AggregateCodec, EventCodec, stand_at
from .errors import BadHandle, CodecError
from .message import CommandMessage, EventDraft, EventMessage, Lineage, Payload
from .observe import Observer
from .outbox import CommandOutbox
from .sealing import SealedValue, Sealing
from .store import CommandFence, EventStore, Snapshot, StreamLock, check_sequence_batch
from .tenancy import Tenancy, tenant_key
from .unit_of_work import TxT, current_tx

logger = logging.getLogger("edax.repository")

__all__ = ["Repository"]


@dataclass
class _PreparedSave:
    repository: object
    signature: tuple[object, ...]
    drafts: list[EventDraft]
    commands: list[CommandMessage]
    snapshot: Snapshot | None


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

    ``command_fence`` (:class:`~edax.store.CommandFence`) keeps a save's commands fenced to the version they
    were decided at, and in order with the next save's. Under ``EVENT`` (the default) a save that enqueues
    commands must append an event: the event's ``(stream, sequence)`` key fails a rival save of the same
    version, and a save that sends commands without one raises :class:`~edax.errors.BadHandle`. Under
    ``LOCK`` every save takes its stream's lock first (:class:`~edax.store.StreamLock`), so a save may send
    commands alone, at one more round trip per save; a store with no ``lock_stream`` raises
    :class:`~edax.errors.BadHandle` here. Every writer of one stream must use the same fence. The kernel
    plans each save (``edax_event::plan_save``): whether it locks, and which event its commands wait behind.

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
        sealing: Sealing | None = None,
        command_fence: CommandFence = CommandFence.EVENT,
    ) -> None:
        try:
            self._command_fence = CommandFence(command_fence)
        except ValueError:
            names = [fence.value for fence in CommandFence]
            raise BadHandle(f"unknown command fence {command_fence!r}: use one of {names}") from None
        self._lock: StreamLock[TxT] | None = None
        if self._command_fence is CommandFence.LOCK:
            if not isinstance(store, StreamLock):
                raise BadHandle(
                    f"command_fence=LOCK needs a store with lock_stream, not {type(store).__name__}"
                )
            self._lock = store
        self._sealing = sealing
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

    def create(self, aggregate_id: str, *, tenant: str | None = None) -> A:
        """A new aggregate: empty at version 0, carrying ``tenant``. It persists nothing, and reads nothing;
        :meth:`save` writes it. ``tenant`` is checked as :meth:`load` checks it.

        Use it for an id this handler run minted. An id from outside, one in a command payload included,
        may already exist: its save raises the transient :class:`~edax.errors.ConcurrencyError`, so a retry
        re-runs the handler into the same conflict until its budget runs out. :meth:`load` such an id and
        check ``version.initial == 0`` instead."""
        tenant_key(self._tenancy, tenant, aggregate_id)
        agg = self._aggregates.load(aggregate_id, 0, None)
        stand_at(agg, 0, tenant=tenant)
        return agg

    async def load(
        self,
        aggregate_id: str,
        *,
        tenant: str | None = None,
        subject: str | None = None,
        tx: TxT | None = None,
    ) -> A:
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
            if snap is not None and snap.type_name != self._aggregates.type:
                snap = None
            if snap is not None and snap.encryption is not None and self._sealing is None:
                raise BadHandle("encrypted snapshots require repository sealing")
            if self._sealing is not None and snap is not None:
                if snap.encryption is not None or snap.tenant is not None:
                    _native.check_sealed_owner(True, tenant, snap.tenant)
                # Before the key provider and the plaintext policy, so another subject's snapshot is an
                # owner mismatch, not a key or policy refusal that tells the caller the stream exists.
                if subject is not None:
                    _native.check_sealed_owner(True, subject, snap.subject)
                if snap.encryption is None:
                    await self._sealing.check_legacy(tenant, snap.subject)
                else:
                    plaintext = await self._sealing.open_state(
                        "snapshot",
                        key,
                        snap.type_name,
                        snap.version,
                        SealedValue(encryption=snap.encryption, data=snap.data),
                        tenant=tenant,
                        subject=snap.subject,
                    )
                    snap = replace_snapshot(snap, data=plaintext)
            cached = self._from_snapshot(aggregate_id, snap)
            base, agg = cached if cached is not None else (0, self._aggregates.load(aggregate_id, 0, None))
            stored = await self._store.load(tx, key, after=base)
            version = check_sequence_batch(base, [msg.sequence for msg in stored])
            # Before the fold, so a replayed transition sees the tenant a live apply() sees.
            stand_at(agg, base, tenant=tenant)
            owner = snap.subject if cached is not None and snap is not None else subject
            committed = base > 0
            # Validate the complete ownership batch before application transitions run.
            for msg in stored:
                if self._sealing is not None:
                    _native.check_sealed_owner(True, tenant, msg.tenant)
                    if msg.aggregate_id != aggregate_id:
                        # A store that returns another aggregate's event under this key is corrupt, not
                        # another owner's stream, so it is no owner mismatch.
                        raise CodecError("stored event belongs to another aggregate")
                    _native.check_sealed_owner(committed, owner, msg.subject)
                    if subject is not None:
                        _native.check_sealed_owner(True, subject, msg.subject)
                if not committed:
                    owner = msg.subject
                committed = True
            payload: Payload | EventMessage
            for msg in stored:
                if msg.encryption is not None and self._sealing is None:
                    raise BadHandle("encrypted events require repository sealing")
                if self._sealing is not None:
                    data = await self._sealing.open_event(msg)
                    payload = Payload(name=msg.name, version=msg.version, data=data)
                else:
                    payload = msg
                agg.transition(self._events.unmarshal(payload))
            stand_at(agg, version, tenant=tenant)
            if self._sealing is not None and version > 0 and subject is not None:
                _native.check_sealed_owner(True, owner, subject)
            agg.subject = owner
            agg._owner = owner
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
                exc_info=self._sealing is None,
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
        subject = aggregate.subject
        if self._sealing is not None:
            _native.check_sealed_owner(aggregate.version.initial > 0, aggregate._owner, subject)
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
            subject,
        )
        owned = [_native.own_command(command, tenant) for command in commands or ()]
        # Dumped before the first write, like the events: a snapshot that cannot be written fails the save
        # outright, before it costs a staged append.
        snapshot = (
            Snapshot(
                key, self._aggregates.type, batch.version, self._aggregates.dump(aggregate), tenant, subject
            )
            if batch.snapshot
            else None
        )
        signature: tuple[object, ...] = (
            aggregate.version.initial,
            tenant,
            subject,
            tuple((p.name, p.version, p.data) for p in payloads),
            None if lineage is None else (lineage.correlation_id, lineage.causation_id),
            tuple(sorted((extensions or {}).items())),
            tuple(sorted((metadata or {}).items())),
            occurred_at,
            tuple(_native.encode_command("json", c) for c in commands or ())
            if self._sealing is not None
            else (),
            None if snapshot is None else snapshot.data,
        )
        previous = aggregate._prepared
        if previous is not None and (
            not isinstance(previous, _PreparedSave)
            or previous.repository is not self
            or previous.signature != signature
        ):
            raise BadHandle("prepared save inputs changed; reload to reconcile the original write")
        async with self._write_guard(tenant, subject, tx):
            drafts = batch.drafts
            if isinstance(previous, _PreparedSave):
                drafts, owned, snapshot = previous.drafts, previous.commands, previous.snapshot
                if not drafts:
                    raise BadHandle("uncertain command-only save requires reconciliation before retry")
            elif self._sealing is not None:
                drafts = [await self._sealing.seal_event(draft) for draft in drafts]
                for command in owned:
                    if command.subject is not None:
                        _native.check_sealed_owner(True, subject, command.subject)
                owned = [
                    await self._sealing.seal_command(replace(command, subject=subject)) for command in owned
                ]
                if snapshot is not None:
                    sealed = await self._sealing.seal_state(
                        "snapshot",
                        key,
                        snapshot.type_name,
                        snapshot.version,
                        snapshot.data,
                        tenant=tenant,
                        subject=subject,
                    )
                    snapshot = replace_snapshot(snapshot, data=sealed.data, encryption=sealed.encryption)
                aggregate._prepared = _PreparedSave(self, signature, drafts, owned, snapshot)
            # Planned for the drafts this save writes, before any write: a retried prepared save writes the
            # earlier ones.
            plan = _native.plan_save(self._command_fence.value, [draft.id for draft in drafts], len(owned))
            async with self._enlist(tx) as tx:
                if plan.lock_stream and self._lock is not None:
                    await self._lock.lock_stream(tx, key)
                existing = (
                    await self._store.load(tx, key, after=aggregate.version.initial)
                    if previous is not None
                    else []
                )
                reconciled = (
                    _native.reconcile_prepared_events(drafts, existing) if previous is not None else False
                )
                if reconciled:
                    stored = existing[: len(drafts)]
                else:
                    stored = await self._store.append(tx, key, aggregate.version.initial, drafts)
                    if owned and self._commands is not None:
                        await self._commands.enqueue(tx, owned, after_event=plan.after_event)
                    if snapshot is not None:
                        await self._store.save_snapshot(tx, snapshot)
            aggregate._prepared = None
            stand_at(aggregate, batch.version, tenant=tenant)
            aggregate._owner = subject
            return stored

    @asynccontextmanager
    async def _write_guard(
        self, tenant: str | None, subject: str | None, tx: TxT | None
    ) -> AsyncIterator[None]:
        if self._sealing is None:
            yield
        else:
            async with transaction_guard(self._store, self._sealing.write_guard(tenant, subject), tx):
                yield
