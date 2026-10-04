"""Command bus (CQRS write side): register handlers by command type, dispatch through a middleware chain.
The kernel owns the cross-language decisions: payload hashing, the idempotency verdict. See
``docs/sdk/commands.md``."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import typing
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, fields, is_dataclass
from time import perf_counter_ns
from typing import Any, Protocol, TypeVar, cast

import msgspec

from . import _edax, _json, errors
from .aggregate import CommandMessage, Lineage, _replace
from .causation import _stamp, caused_by
from .errors import (  # re-exported: the command errors are part of this module's API
    AlreadyRegistered,
    BadHandle,
    CachedFailure,
    CodecError,
    HandlerNotFound,
    InProgress,
    PayloadMismatch,
    is_terminal,
)
from .observe import ClaimVerdict, Observer, Settlement, _wrap
from .pacing import Pacing
from .store import EventCodec, Transactor, Tx, _in_unit_of_work, _transaction
from .tenancy import Tenancy

__all__ = [
    "AlreadyRegistered",
    "BadHandle",
    "CachedFailure",
    "Claimed",
    "CodecError",
    "Command",
    "CommandBus",
    "Handler",
    "HandlerNotFound",
    "IdempotencyStore",
    "InProgress",
    "Middleware",
    "PayloadMismatch",
    "Record",
    "Won",
    "claim_key",
    "command_message",
    "current_command",
    "decode_prior",
    "decode_record",
    "encode_record",
    "forget",
    "idempotent",
    "joins_transaction",
    "retry",
]


logger = logging.getLogger("edax.command")


class Command(Protocol):
    """A command: its instance fields are the payload, ``type`` names the handler.

    ``type`` becomes the message's ``name``, and the kernel hashes that name with the command's whole
    value, so a command decoded off a queue hashes as the one its constructor builds. Declare ``type`` as a
    class attribute: the hash takes it once as the name, not again as a field, as a Rust command's
    ``command_type()`` is. A ``version`` attribute, when the command has one, is the schema version its
    message carries; without one the version is 1."""

    # A read-only property, not `type: str`: a plain Protocol attribute demands a settable instance one,
    # which `type: ClassVar[str] = "user.create"` does not satisfy. A property accepts all three shapes.
    @property
    def type(self) -> str: ...


Handler = Callable[[Any], Awaitable[Any]]
Middleware = Callable[[Handler], Handler]
H = TypeVar("H", bound=Callable[..., Any])


def _compose(middleware: Sequence[Callable[[H], H]], handler: H) -> H:
    """Wrap ``handler`` in ``middleware``, the first one outermost: the one composition rule the command bus
    and the event router share."""
    for mw in reversed(middleware):
        handler = mw(handler)
    return handler


def _is_async(handler: Handler) -> bool:
    if inspect.iscoroutinefunction(handler):
        return True
    call = getattr(handler, "__call__", None)  # noqa: B004 - the bound method is what we inspect
    return call is not None and inspect.iscoroutinefunction(call)


# The message the bus is dispatching right now. Each dispatch sets a new value and resets it on exit, so a
# nested dispatch sees its own message and the outer one gets its own back.
_current: contextvars.ContextVar[CommandMessage | None] = contextvars.ContextVar("edax_command", default=None)


def current_command() -> CommandMessage | None:
    """The :class:`~edax.aggregate.CommandMessage` the bus is dispatching, for the handler's whole run, or
    ``None`` outside a dispatch. Middleware reads it too: :func:`idempotent` claims its
    ``idempotency_key``."""
    return _current.get()


def command_message(
    cmd: Command,
    *,
    codec: EventCodec | None = None,
    idempotency_key: str | None = None,
    aggregate_id: str | None = None,
    lineage: Lineage | None = None,
    extensions: dict[str, str] | None = None,
    metadata: dict[str, str] | None = None,
    tenant: str | None = None,
) -> CommandMessage:
    """The :class:`~edax.aggregate.CommandMessage` for ``cmd``: what :meth:`CommandBus.dispatch` runs, and
    what you publish to a broker or put in a command outbox.

    With ``codec``, ``name``, ``version`` and ``data`` are what ``codec.marshal(cmd)`` returns, and a name
    other than ``cmd.type`` raises :class:`~edax.errors.CodecError`. Without one, ``name`` is ``cmd.type``,
    ``version`` is ``cmd.version`` or 1, and ``data`` is the command's JSON. Build a message that another
    process runs with :meth:`CommandBus.dispatch_message` with the codec that consumer reads it with.
    Otherwise the two disagree on the version and the encoding: the consumer upcasts data that is already
    current, or cannot read it at all.

    ``id`` is a fresh :func:`~edax.guid`. ``lineage`` defaults to the ambient
    :func:`~edax.causation.current_lineage`, else a new flow. ``issued_at`` is the kernel clock.

    ``idempotency_key`` names the request: two messages with one key are one request, and a guard replays
    the second. ``None`` sends the message unguarded. ``tenant`` names whose command it is: a guard
    composes its ledger key from it, and a repository keys the aggregate by it, each under its own
    tenancy. ``aggregate_id`` orders the message on a broker; the
    message id orders it when there is none. A command that does not encode, a version outside an unsigned
    32-bit integer, or an extension key outside the kernel's rule raises the terminal
    :class:`~edax.errors.CodecError`."""
    name, version, data = _payload(cmd, codec)
    with errors.mapped():
        flow, ext, meta = _stamp(lineage, extensions, metadata)
        return CommandMessage(
            id=_edax.guid(),
            name=name,
            version=version,
            data=data,
            correlation_id=flow.correlation_id,
            causation_id=flow.causation_id,
            issued_at=_edax.current_timestamp(),
            extensions=ext,
            metadata=meta,
            idempotency_key=idempotency_key,
            aggregate_id=aggregate_id,
            tenant=tenant,
        )


def _payload(cmd: Command, codec: EventCodec | None) -> tuple[str, int, bytes]:
    """The ``name``, ``version`` and ``data`` of ``cmd``'s message: the codec's, else the JSON default."""
    if codec is not None:
        marshaled = codec.marshal(cmd)
        if marshaled.name != cmd.type:
            # dispatch() finds the handler by cmd.type and dispatch_message() by the message name, so the
            # two would run one command through different handlers.
            raise CodecError(
                f"the codec names a {cmd.type!r} command {marshaled.name!r}; a message name must be its type"
            )
        return marshaled.name, marshaled.version, marshaled.data
    version = getattr(cmd, "version", 1)
    # Only the type is checked here: the message constructor's own conversion refuses a version outside
    # an unsigned 32-bit integer, which `errors.mapped` turns into a CodecError.
    if isinstance(version, bool) or not isinstance(version, int):
        raise CodecError(f"{cmd.type} version must be an int schema version, got {type(version).__name__}")
    return cmd.type, version, _payload_json(cmd)


class CommandBus:
    """Dispatches commands to handlers by command type, through a middleware chain wrapped around each
    handler at register time; the first middleware is the outermost. A handler must be a coroutine
    function, and :meth:`register` raises :class:`TypeError` otherwise.

    Every dispatch runs one :class:`~edax.aggregate.CommandMessage`. The handler takes the command
    object, and reads the message with :func:`current_command`. It runs under the ambient lineage
    :func:`~edax.causation.propagate` of the message, so the events it saves are the command's children."""

    def __init__(self, *middleware: Middleware) -> None:
        self._handlers: dict[str, Handler] = {}
        self._middleware = middleware

    def register(self, command_type: str, handler: Handler) -> None:
        if command_type in self._handlers:
            raise AlreadyRegistered(command_type)
        # Reject at wiring time: a sync handler otherwise fails on its first dispatch, far from the mistake.
        # A command's result is what the caller awaits and what the guard caches, so the bus does not
        # adapt a sync handler the way the event router does: it would hide a blocking call in the loop.
        if not _is_async(handler):
            raise TypeError(
                f"command handler for {command_type!r} must be a coroutine function, "
                f"got {handler.__class__.__name__}"
            )
        self._handlers[command_type] = _compose(self._middleware, handler)

    def on(self, command_type: str) -> Callable[[Handler], Handler]:
        """Decorator form of :meth:`register`, as :meth:`~edax.router.EventRouter.on` is for events.
        Returns the bare handler, not the wrapped one."""

        def deco(fn: Handler) -> Handler:
            self.register(command_type, fn)
            return fn

        return deco

    async def dispatch(
        self,
        cmd: Command,
        *,
        idempotency_key: str | None = None,
        aggregate_id: str | None = None,
        tenant: str | None = None,
        lineage: Lineage | None = None,
        extensions: dict[str, str] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> Any:
        """Run ``cmd`` through its handler and return the handler's result. The message is
        :func:`command_message` of ``cmd`` and the keyword arguments, which take the same meaning there.
        Pass ``idempotency_key`` for a request a client may send twice; :func:`idempotent` claims it. An
        unregistered ``cmd.type`` raises :class:`~edax.errors.HandlerNotFound`."""
        handler = self._handler(cmd.type)
        msg = command_message(
            cmd,
            idempotency_key=idempotency_key,
            aggregate_id=aggregate_id,
            lineage=lineage,
            extensions=extensions,
            metadata=metadata,
            tenant=tenant,
        )
        return await _run(handler, msg, cmd)

    async def dispatch_message(self, msg: CommandMessage, codec: EventCodec) -> Any:
        """Run a received :class:`~edax.aggregate.CommandMessage`: off a broker, or from a command outbox.
        ``codec`` turns the message back into the command object by ``name`` and ``version``, as
        ``json_codec({"Ship": Ship})`` does, and the handler registered under ``msg.name`` runs it. The
        message keeps its id and lineage.

        A message with no ``idempotency_key`` is claimed under its ``id``, the kernel's ``received_key``. A
        broker delivers at least once, so the same message can arrive twice, and both copies carry one id;
        a guard then replays the second instead of running the handler again. An unregistered
        ``msg.name`` raises :class:`~edax.errors.HandlerNotFound`, and a message the codec cannot read
        raises the terminal :class:`~edax.errors.CodecError`."""
        handler = self._handler(msg.name)
        cmd = codec.unmarshal(msg)
        key = received_key(msg)
        if key != msg.idempotency_key:
            msg = _replace(msg, idempotency_key=key)
        return await _run(handler, msg, cmd)

    def _handler(self, name: str) -> Handler:
        handler = self._handlers.get(name)
        if handler is None:
            raise HandlerNotFound(name)
        return handler


async def _run(handler: Handler, msg: CommandMessage, cmd: Any) -> Any:
    """Run ``handler`` on ``cmd`` with ``msg`` as the current command and the lineage of its children."""
    token = _current.set(msg)
    try:
        with caused_by(msg):
            return await handler(cmd)
    finally:
        _current.reset(token)


def retry(attempts: int, *, pacing: Pacing | None = None) -> Middleware:
    """Retry the handler up to ``attempts`` times, waiting each step of ``pacing`` (default: the kernel's
    jittered backoff on ``asyncio.sleep``). A terminal failure (see :func:`~edax.errors.is_terminal`) is
    re-raised at once; everything else is retried.

    A retry must re-run a whole unit of work, never a handler inside one: a second attempt in the same
    transaction reads the first attempt's staged writes and applies them again, and the commit lands
    both. So ``retry`` refuses to run while any :func:`~edax.store.transaction` is open, with terminal
    :class:`~edax.errors.BadHandle`. Wire it outside a guard that owns the transaction,
    ``CommandBus(retry(n), idempotent(store, db, scope="api"))``, and open no transaction around the
    dispatch. See ``docs/sdk/commands.md``.

    ``attempts`` below 1 raises :class:`ValueError`: no attempt at all is not a retry policy."""
    if attempts < 1:
        raise ValueError(f"retry needs at least one attempt, got {attempts}")
    pace = pacing or Pacing()

    def mw(next_: Handler) -> Handler:
        async def handler(cmd: Command) -> Any:
            if _in_unit_of_work():
                raise BadHandle(
                    "retry was dispatched inside an open transaction: a second attempt would re-apply the "
                    "first attempt's staged writes, and one commit would land both. Put retry outside "
                    "idempotent(..., unit_of_work=...) and open no transaction around the dispatch"
                )
            for i in range(attempts - 1):
                try:
                    return await next_(cmd)
                except Exception as e:
                    if is_terminal(e):
                        raise
                await pace.wait(i)
            return await next_(cmd)  # last attempt: its failure is final

        return handler

    return mw


@dataclass
class Record:
    """Stored outcome of a dispatch, keyed by idempotency key. ``pending`` is ``True`` while a claim is held
    but the handler has not completed, so an in-flight claim is never replayed as a ``None`` success. A
    completed record with ``error=None`` is a success whose value is ``result``, even when that is ``None``.
    A failed record carries its message in ``error`` and its taxonomy code in ``error_code`` (``None`` for a
    record whose writer stored no code), so a replay in any SDK keeps the original code.
    Wire shape, reading order and replay verdict are the kernel's (``testdata/golden``)."""

    payload_hash: int
    result: Any = None
    error: str | None = None
    pending: bool = False
    error_code: str | None = None


@dataclass(frozen=True)
class Won:
    """What :meth:`IdempotencyStore.claim` returns to the caller that won: run the handler, then settle with
    ``handle``. The handle names this claim and only this one. Every in-tree store fences on it (Redis on the
    claim value, memory on a handle it mints per claim): it settles the key only while the key still holds
    this exact claim, so a claim that lapsed and was taken over is never settled by its old owner, whose
    ``complete`` raises :class:`~edax.errors.StoreError`. ``b""`` is only the default of a ``Won`` built by
    hand, for a store whose claims never lapse."""

    handle: bytes = b""


#: What :meth:`IdempotencyStore.claim` returns: :class:`Won`, or the prior :class:`Record` holding the key.
Claimed = Won | Record


class IdempotencyStore(Protocol):
    """Per-key outcome store behind :func:`idempotent`. The seam is an atomic claim.

    What a backend must guarantee; rationale and worked examples in ``docs/backends/README.md``.

    - *Atomic claim.* ``claim`` is an insert-if-absent, linearizable across every process sharing the store:
      one caller gets :class:`Won`, every other gets the existing :class:`Record`. Never a read then a write.
    - *Pending state.* The inserted record carries ``pending=True`` and the given ``payload_hash``, and must
      come back faithfully; the guard reads it to tell an in-flight claim from a completed ``None`` result.
    - *Enlistment.* Given ``tx``, or with :func:`~edax.store.current_tx` of its own transactor set, every
      operation joins that transaction, so a rollback removes the claim: effectively-once. Otherwise
      standalone: at-least-once.
    - *Declaration.* ``joins_transaction: ClassVar[bool]`` names which of the two you are; absent is
      ``False``. ``True`` obliges you to raise terminal :class:`~edax.errors.BadHandle` for a handle from
      another database, never to auto-commit. The guard hands such a store its transaction's handle
      explicitly, so a store over another database than ``unit_of_work`` fails at the claim.
    - *A loser inside a transaction* must block on the winner's row and then read it, or raise transient
      :class:`~edax.errors.StoreError`; it must never read a stale snapshot and report a win, which runs the
      handler twice. Bound the wait with a lock or statement timeout, also ``StoreError``. A backend that
      cannot block returns the holder's record as pending, reported as :class:`~edax.errors.InProgress`.
    - *Forget.* ``forget`` deletes a completed or failed record, so a command taken back from a dead-letter
      queue runs its handler again. It reads the record with the kernel: a pending claim raises
      :class:`~edax.errors.InProgress` and stays, because its run may still complete. An absent key is a
      no-op. It enlists as ``claim`` does, and deletes only the record it read, never a claim that took the
      key over since.
    - *Complete / release.* Both take the :attr:`Won.handle` the claim returned. ``complete`` overwrites the
      key with the terminal record, ``release`` deletes it. Both are idempotent; releasing an absent key is a
      no-op. A backend whose claims can lapse must fence both on the handle: write only while the key still
      holds that exact claim. A ``complete`` refused that way raises ``StoreError``, and the ``release`` the
      guard answers it with deletes nothing, because the key now holds another claim or a settled record.
    - *A key another open transaction holds.* A ``joins_transaction`` store refuses ``complete`` and
      ``release`` of a key another open transaction has written, with the transient
      :class:`~edax.errors.ConcurrencyError`, and writes nothing: that transaction's commit would overwrite
      the record, or the release would free its claim.
    - *Retention.* A record must outlive the longest window in which the same key can be redelivered.
    - *Record contents.* ``payload_hash`` is the kernel's unsigned 64-bit hash: store it losslessly, never a
      signed ``BIGINT``; one outside ``0 .. 2**64 - 1`` raises :class:`~edax.errors.CodecError`. ``result``
      must round-trip unchanged, within the codec's integer bounds (``docs/kernels/cqrs.md``).
    - *Unreadable records.* ``claim`` reads the record holding a key with :func:`decode_prior`, so bytes no
      reader can decode come back as the kernel's terminal ``codec`` failure, never as an exception and
      never as a free key. The guard replays it as :class:`~edax.errors.CachedFailure`, as every SDK does.
    - *Errors.* Backend failures raise :class:`~edax.errors.StoreError`, which the guard treats as transient.
    """

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        """Atomically insert a pending record. :class:`Won` means this caller won, else the existing
        record."""
        ...

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        """Overwrite ``key`` with the terminal ``record`` while it holds the claim ``handle`` names.
        Idempotent."""
        ...

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        """Delete ``key`` while it holds the claim ``handle`` names, so a transient failure can retry.
        Idempotent."""
        ...

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        """Delete the settled record ``key`` holds, so the next claim wins. The *Forget* clause above says
        what happens to a pending claim and to an absent key."""
        ...


def claim_key(
    scope: str, idempotency_key: str, *, tenancy: Tenancy = Tenancy.SILO, tenant: str | None = None
) -> str:
    """The ledger key the :func:`idempotent` guard named ``scope`` claims for ``idempotency_key``, in a
    deployment that separates tenants by ``tenancy``. Two scopes never share a ledger key. Under
    :attr:`~edax.tenancy.Tenancy.POOL` the key carries ``tenant``, so two tenants never share one either.
    A mode that requires a tenant and a call that names none raises :class:`~edax.errors.CodecError`.
    The composition is the kernel's, pinned by ``testdata/golden/claim_key.json``, so a guard in another
    binding sharing the ledger claims the same row."""
    with errors.mapped():
        return _edax.claim_key(tenancy.value, scope, idempotency_key, tenant)


def received_key(msg: CommandMessage) -> str:
    """The key a guard claims for a command received from a broker or an outbox: its ``idempotency_key``,
    or its ``id`` when it has none, so a redelivery of one message is caught. An empty key is a key. The
    rule is the kernel's, pinned by ``testdata/golden/command_received_key.json``."""
    return _edax.received_key(msg.id, msg.idempotency_key)


async def forget(
    store: IdempotencyStore,
    scope: str,
    idempotency_key: str,
    *,
    tenancy: Tenancy,
    tenant: str | None = None,
    tx: Tx | None = None,
) -> None:
    """Delete the record the :func:`idempotent` guard named ``scope`` keeps for ``idempotency_key``, so the
    next dispatch of that key runs its handler again. Use it to re-drive a dead-lettered command whose
    failure was cached. Pass the guard's ``tenancy`` and the command's ``tenant``: the record lives under
    the key :func:`claim_key` composes from them, and a mode that requires a tenant raises
    :class:`~edax.errors.CodecError` without one rather than delete nothing. ``tenancy`` has no default:
    a pooled caller that left it out would compose the silo key and delete nothing. The *Forget* clause of
    :class:`IdempotencyStore` says what happens to a claim still in flight and to a key with no record."""
    await store.forget(claim_key(scope, idempotency_key, tenancy=tenancy, tenant=tenant), tx)


def _payload_json(cmd: Command) -> bytes:
    """The whole command as one JSON object: the serialization step of the kernel's hash rule."""
    # Canonicalization is the kernel's job, so every binding agrees on the hash; the SDK's one encoder
    # turns the value into JSON, and its every failure is a terminal CodecError. Reading the fields can
    # fail too (a `__slots__` class has no `vars()`, a dataclass can hold itself): deterministic as well.
    try:
        data = _payload_data(cmd)
    except (TypeError, RecursionError) as e:
        raise CodecError(f"command payload is not JSON-encodable: {e}") from e
    return _json.encode_bytes(data)


def _payload_data(cmd: Command) -> Any:
    """The command's instance fields, whatever shape the command class takes."""
    # Three shapes: a dataclass's and a Struct's declared fields, a plain object's instance attributes.
    # The golden vector covers all three, so the fallback is contract, not convenience.
    if is_dataclass(cmd) or isinstance(cmd, msgspec.Struct):
        return _materialized(cmd)
    return _materialized(vars(cmd))


# Distinguishes a field with no value at all from one whose value is None.
_NO_VALUE = object()


def _materialized(value: Any) -> Any:
    """``value`` with every dataclass in it read through ``dataclasses.fields()``.

    ``msgspec.convert`` / ``decode`` leave a defaulted field out of the instance ``__dict__`` that msgspec's
    encoder reads, so one logical command would hash two ways. Reading each declared field with ``getattr``
    sees the class-level default. Returns ``value`` itself when nothing changed."""
    if is_dataclass(value) and not isinstance(value, type):
        data: dict[str, Any] = {}
        for f in fields(value):
            present = getattr(value, f.name, _NO_VALUE)
            # A field with no value (init=False, never assigned) is omitted, as msgspec omits it.
            if present is not _NO_VALUE:
                data[f.name] = _materialized(present)
        return data
    if isinstance(value, msgspec.Struct):
        changes = _changed({f.name: getattr(value, f.name) for f in msgspec.structs.fields(value)})
        return msgspec.structs.replace(value, **changes) if changes else value
    if isinstance(value, (list, tuple)):
        items = [_materialized(item) for item in value]
        return items if any(new is not old for new, old in zip(items, value, strict=True)) else value
    if isinstance(value, dict):
        changes = _changed(value)
        return {**value, **changes} if changes else value
    return value


def _changed(entries: dict[Any, Any]) -> dict[Any, Any]:
    changes: dict[Any, Any] = {}
    for key, old in entries.items():
        new = _materialized(old)
        if new is not old:
            changes[key] = new
    return changes


def _payload_hash(cmd: Command, name: str) -> int:
    """The kernel's idempotency hash of ``cmd``: the message ``name`` and the command's whole value,
    canonicalized, FNV-1a. Pinned by ``testdata/golden/command_hash.json``."""
    payload = _payload_json(cmd)
    with errors.mapped():  # the kernel's `codec:` refusal of the bytes is terminal
        return _edax.command_hash_json(name, payload)


def idempotent(
    store: IdempotencyStore,
    unit_of_work: Transactor | None = None,
    *,
    scope: str,
    result_type: Any = None,
    observer: Observer | None = None,
    tenancy: Tenancy = Tenancy.SILO,
) -> Middleware:
    """Deduplicate dispatches by the ``idempotency_key`` of the :func:`current_command`. A message with no
    key runs unguarded and writes nothing to the ledger. The first dispatch of a key wins the claim and runs
    the handler; a later or concurrent one loses and acts on the prior record: it replays a cached result,
    raises :class:`~edax.errors.CachedFailure` for a cached terminal error, or raises
    :class:`~edax.errors.InProgress` while the winner is in flight. A transient handler failure releases the
    claim so a retry can re-run. ``unit_of_work`` hands the guard the transaction, which is what lets a
    failed commit release the claim (see :func:`_guarded`). Place :func:`retry` outside a guard that owns
    the transaction: each attempt then re-claims in a fresh unit of work.

    ``scope`` names this guard, usually the service or the stage it guards: ``"api"`` at ingress,
    ``"billing-worker"`` on a consumer. The ledger key is the kernel's :func:`claim_key` of the scope, the
    message's ``idempotency_key`` and, under a pool, its ``tenant``, so two guards that share a ledger never
    replay each other's outcome for one key. The kernel hashes the message name with the value the handler
    receives, so one key reused by another command, or with another payload, raises
    :class:`~edax.errors.PayloadMismatch`. :func:`forget` deletes a record by the same scope, tenancy,
    tenant and key. See ``docs/sdk/commands.md`` and ``docs/sdk/idempotency.md``.

    A cached success crosses the store as JSON, so a replay hands back the handler's declared return type,
    not the plain JSON value: ``result_type``, else the return annotation of the handler this guard wraps
    directly (when it resolves at module scope), converted with msgspec. A replay that does not convert is
    an unreadable record: the terminal :class:`~edax.errors.CachedFailure` of code ``codec``, as in the
    Rust SDK, whose typed store reads it so. With neither, or with ``result_type=Any``, a replay is
    the plain JSON value, as with an ``object`` annotation. A ``result_type`` msgspec cannot convert to raises
    :class:`TypeError` at wiring, as a ``scope`` that is not a ``str`` does.

    ``tenancy`` is how the deployment this guard runs in separates tenants, fixed at wiring as it is on a
    :class:`~edax.store.Repository`. Under :attr:`~edax.tenancy.Tenancy.POOL` the ledger key is composed
    with the message's ``tenant`` (:func:`claim_key`), so one request id reused by two tenants claims two
    rows rather than replaying one tenant's result to the other. Under a pool or a bridge, a message that
    names no tenant raises :class:`~edax.errors.CodecError` instead of claiming a shared key.

    ``observer`` receives a :meth:`~edax.observe.Observer.transaction` per ``unit_of_work`` the guard opens,
    a :meth:`~edax.observe.Observer.claim` verdict for every lost claim and a
    :meth:`~edax.observe.Observer.settled` for every won one; ``None`` is the no-op observer."""
    if not isinstance(scope, str):
        raise TypeError(
            f"idempotent(scope=...) names the guard and must be a str, got {type(scope).__name__}"
        )
    observe = _wrap(observer)

    def mw(next_: Handler) -> Handler:
        replay_as = _replay_type(result_type, next_)

        async def handler(cmd: Command) -> Any:
            msg = current_command()
            if msg is None or msg.idempotency_key is None:
                return await next_(cmd)
            key = claim_key(scope, msg.idempotency_key, tenancy=tenancy, tenant=msg.tenant)
            guard = _Guard(store, msg.name, key, _payload_hash(cmd, msg.name), observe, replay_as)
            return await _guarded(guard, lambda: next_(cmd), unit_of_work)

        return handler

    return mw


def _replay_type(explicit: Any, handler: Handler) -> Any:
    """The type a cached success replays as: ``explicit``, else ``handler``'s resolvable return annotation,
    else ``None`` for the plain JSON value. A type msgspec has no conversion for is a wiring mistake."""
    chosen = explicit
    if chosen is None:
        try:
            chosen = typing.get_type_hints(handler).get("return")
        except Exception:  # noqa: BLE001 - an annotation that does not resolve here names no type to replay as
            return None
    if chosen is None or chosen is Any or chosen is object:
        return None
    if isinstance(msgspec.inspect.type_info(chosen), msgspec.inspect.CustomType):
        raise TypeError(
            f"a cached result cannot be replayed as {chosen!r}: msgspec has no conversion for it. "
            "Return a dataclass, a msgspec.Struct or a JSON type, or pass result_type=typing.Any"
        )
    return chosen


def joins_transaction(store: IdempotencyStore) -> bool:
    """Whether ``store`` enlists in the caller's unit of work, per its
    :attr:`IdempotencyStore.joins_transaction` declaration. Absent, the answer is ``False``, the safe side: a
    needless compensation costs one no-op delete, a skipped one leaves a ``done`` record for rolled-back
    work."""
    return bool(getattr(store, "joins_transaction", False))


@asynccontextmanager
async def _unit(unit_of_work: Transactor | None, observe: Observer) -> AsyncIterator[Tx | None]:
    if unit_of_work is None:
        yield None
    else:
        async with _transaction(unit_of_work, observe) as tx:
            yield tx


@dataclass(frozen=True)
class _Guard:
    """One guarded dispatch: the claim on ``key`` in ``store`` for ``payload_hash``, reported to ``observe``
    under ``name`` (the command type or event name), and the type a cached success replays as."""

    store: IdempotencyStore
    name: str
    key: str
    payload_hash: int
    observe: Observer
    replay_as: Any = None

    def replay(self, prior: Record) -> Any:
        """Lost the claim: act on the winner's record."""
        return _replay(self.name, self.key, self.payload_hash, prior, self.observe, self.replay_as)


async def _guarded(guard: _Guard, run: Callable[[], Awaitable[Any]], unit_of_work: Transactor | None) -> Any:
    """Run ``run`` once under ``guard``'s atomic claim and return its result, or the cached one, converted
    to ``replay_as`` when that is set.

    The one claim/complete/release orchestration behind :func:`idempotent` (commands) and
    :func:`~edax.router.idempotent_by_id` (events). Without ``unit_of_work`` the store enlists in the ambient
    :func:`~edax.store.current_tx` and the host owns the commit. With it, the guard owns the transaction and
    :func:`joins_transaction` picks the ordering. A shared ledger runs ``begin; claim; run; complete;
    commit``: one atomic commit, effectively-once, and a terminal failure is not cached because the record
    would roll back with the work. A ledger of its own runs ``claim; begin; run; commit; complete``: two
    commits, at-least-once within the store's retention, ``complete`` last so the worst case is a re-run and
    not a ``done`` record for work that never landed. Any failure up to and including the commit releases the
    claim. Failure windows: ``docs/sdk/idempotency.md``.

    ``observe`` is the factory's already-guarded observer: a lost claim reports its verdict through
    ``claim``, a won one reports how it ended through ``settled``, both under ``name``, the command type
    or event name the guarded handler serves."""
    if unit_of_work is not None and _in_unit_of_work():
        # The Transactor contract says the SDK never nests: a second unit of work would commit the claim
        # independently of the writes it guards. BadHandle, not StoreError, so the dispatcher dead-letters
        # the wiring mistake instead of redelivering it forever.
        raise BadHandle(
            "idempotency guard was given a unit_of_work while a transaction is already open: "
            "either pass unit_of_work or open the transaction around the dispatch, not both"
        )
    if unit_of_work is not None and joins_transaction(guard.store):
        return await _guarded_in_one_commit(guard, run, unit_of_work)
    return await _guarded_around_the_work(guard, run, unit_of_work)


async def _guarded_in_one_commit(
    guard: _Guard, run: Callable[[], Awaitable[Any]], unit_of_work: Transactor
) -> Any:
    """``begin; claim; run; complete; commit``. The ledger shares the transaction, so every failure path
    below is a rollback that drops the claim with the work: reported as ``released``. The commit is the
    write that ends the claim, so ``completed`` is reported only once it has landed."""
    started: int | None = None  # set once the claim is won: only a won claim is settled
    how: Settlement = "released"
    try:
        # The handle goes to the store explicitly. Left to `current_tx`, a store over another database
        # would find no ambient tx of its own and auto-commit the claim outside the guard's transaction,
        # so a rollback would leave the key pending; given this handle it refuses with BadHandle.
        async with _transaction(unit_of_work, guard.observe) as tx:
            claimed = await guard.store.claim(guard.key, guard.payload_hash, tx=tx)
            if not isinstance(claimed, Won):
                return guard.replay(claimed)
            started = perf_counter_ns()
            result = await run()
            record = Record(guard.payload_hash, result=result)
            await guard.store.complete(guard.key, claimed.handle, record, tx=tx)
        how = "completed"
        return result
    finally:
        if started is not None:
            guard.observe.settled(guard.name, how, perf_counter_ns() - started)


async def _guarded_around_the_work(
    guard: _Guard, run: Callable[[], Awaitable[Any]], unit_of_work: Transactor | None
) -> Any:
    """``claim; begin; run; commit; complete``. The claim lands on its own, so every failure after
    it must be compensated by hand. With ``unit_of_work`` unset the middle is just ``run()``."""
    # A claim that raises is deliberately not compensated: it returned no handle to release with, and a
    # claim whose response was lost is settled by the store's retention.
    store, key, payload_hash = guard.store, guard.key, guard.payload_hash
    claimed = await store.claim(key, payload_hash)
    if not isinstance(claimed, Won):
        return guard.replay(claimed)
    handle = claimed.handle

    # Won the claim. From here every exit must settle the record, including a failure of the commit
    # itself. That is why the transaction opens here and not around the caller.
    started = perf_counter_ns()
    # What a raise out of the completion below means: the store took neither the record nor the release.
    how: Settlement = "refused"
    try:
        try:
            async with _unit(unit_of_work, guard.observe):
                result = await run()
        except BaseException as e:
            # The kernel's settle table: a terminal failure is cached with its code; a transient one, or a
            # cancel, releases the claim so a retry or a redelivery can re-run. Shielded in every case: a
            # cancel landing during the settlement would otherwise abandon the claim until its retention
            # frees it.
            code = errors.code_of(e) if isinstance(e, Exception) else None
            record = None if code is None else Record(payload_hash, error=str(e), error_code=code)
            how = await asyncio.shield(_settle_failure(store, key, handle, record))
            raise
        refusal = await _complete_or_release(store, key, handle, Record(payload_hash, result=result))
        if refusal is not None:
            how = "released"
            raise refusal
        how = "completed"
        return result
    finally:
        guard.observe.settled(guard.name, how, perf_counter_ns() - started)


async def _settle_failure(
    store: IdempotencyStore, key: str, handle: bytes, record: Record | None
) -> Settlement:
    """Settle the claim after the handler failed: cache ``record``, or release when there is nothing to
    cache or the store would not take the record. Never raises; returns how the claim ended: ``cached``,
    ``released``, or ``refused`` when the store would not take the release. The handler's exception is
    the verdict the caller,
    :func:`retry` and the dispatcher act on, so a compensation the store refuses must not replace it.
    Inside the host's transaction that refusal is usually the abort the handler's own failed statement
    caused, and the host's rollback frees the claim; on a ledger of its own the claim stays pending until
    its retention frees it. Either way the claim is settled by something other than this call."""
    try:
        if record is None:
            await store.release(key, handle)
            return "released"
        refusal = await _complete_or_release(store, key, handle, record)
        if refusal is None:
            return "cached"
        logger.warning(
            "edax idempotency: caching the failure of %r was refused; the claim is released instead",
            key,
            exc_info=refusal,
        )
        return "released"
    except Exception:
        # Expected when the ledger shares a transaction the handler already aborted, so it is not news.
        expected = joins_transaction(store) and _in_unit_of_work()
        logger.log(
            logging.DEBUG if expected else logging.WARNING,
            "edax idempotency: compensation for %r was refused after a failed handler; "
            "the ambient rollback or the record's retention frees the claim",
            key,
            exc_info=True,
        )
        return "refused"


def _replay(name: str, key: str, payload_hash: int, prior: Record, observe: Observer, replay_as: Any) -> Any:
    """Lost the claim: the kernel classifies the winner's record and this maps the verdict onto the SDK's
    exceptions. The observer hears the kernel's verdict verbatim, so no SDK renames it. Never
    releases, because the key belongs to the winner."""
    verdict = _outcome(prior, payload_hash)
    replayed = prior.result
    if verdict == "succeeded" and replay_as is not None:
        try:
            replayed = msgspec.convert(prior.result, replay_as)
        except msgspec.ValidationError as e:
            # A cached result that does not read back as the result type is an unreadable record, as the
            # Rust SDK's typed decode finds it: the kernel's terminal codec failure, replayed as cached.
            cause = f"cached result for {key!r} does not convert to {replay_as!r}: {e}"
            prior = _record_of(_edax.unreadable_idempotency_record(prior.payload_hash, cause))
            verdict = "failed"
    observe.claim(name, verdict)
    if verdict == "payload_mismatch":
        raise PayloadMismatch(key)
    if verdict == "in_progress":
        raise InProgress(key)
    if verdict == "failed":
        # The kernel's replay text carries the original code (`codec: bad payload`), so it survives.
        message = _edax.cached_failure_message(prior.error or "", prior.error_code)
        raise CachedFailure(message, original_code=prior.error_code)
    return replayed  # "succeeded": the cached value, `None` included


async def _complete_or_release(
    store: IdempotencyStore, key: str, handle: bytes, record: Record
) -> BaseException | None:
    """Overwrite the pending claim with its terminal record; if that write fails, release the
    claim instead of leaving it pending for the store's whole retention. Both carry the claim's handle,
    so a fenced store deletes nothing when the write did land and only its reply was lost. Returns
    ``None`` once the record is written, or the error ``complete`` raised once the release has landed,
    for the caller to raise or log: the ledger then holds no record. A release that fails too raises."""
    try:
        await store.complete(key, handle, record)
    except BaseException as e:  # noqa: BLE001 - the record failed or was cancelled, not the handler; returned
        # Shielded for the same reason as the cancellation branch in `_guarded_around_the_work`:
        # a cancel landing mid-release leaves the claim pending for the store's whole retention.
        await asyncio.shield(store.release(key, handle))
        return e
    return None


def _outcome(prior: Record, current: int) -> ClaimVerdict:
    """The kernel's verdict on a prior record: ``payload_mismatch``, ``in_progress``, ``failed``
    or ``succeeded``. The result plays no part in the verdict, so only the classifying fields
    cross. A stored ``payload_hash`` outside ``0 .. 2**64 - 1`` (a lossy store) cannot cross, and
    :func:`~edax.errors.mapped` makes that the terminal :class:`~edax.errors.CodecError` it is."""
    with errors.mapped():
        verdict = _edax.idempotency_outcome(current, prior.payload_hash, prior.pending, prior.error)
    # The kernel's verdicts are the closed set `CLAIM_VERDICTS` names, pinned by `idempotency_record.json`.
    return cast(ClaimVerdict, verdict)


def encode_record(record: Record) -> str:
    """Serialize a :class:`Record` to the kernel's idempotency-record JSON. Public because it is a wire
    contract: every JSON-persisting :class:`IdempotencyStore` backend and every SDK read and write this exact
    shape, so one key stays readable across SDKs."""
    result_json = _json.encode(record.result)
    with errors.mapped():  # the kernel's `codec:` refusal, or a hash outside u64, is a terminal CodecError
        return _edax.encode_idempotency_record(
            record.payload_hash, record.pending, result_json, record.error, record.error_code
        )


def decode_record(raw: str) -> Record:
    """Parse the kernel's idempotency-record JSON. The kernel applies the reading order, so a
    record written by an older writer (no ``pending`` field) reads as completed, and a completed
    record with a ``null`` result reads as a success whose value is ``None``. A failure stored without a
    code the kernel knows reads with ``error_code=None``. An unreadable record raises the terminal
    :class:`~edax.errors.CodecError`; a store reads the record that holds a key with :func:`decode_prior`."""
    with errors.mapped():  # an unreadable record is the kernel's terminal `codec:` error
        return _record_of(_edax.decode_idempotency_record(raw))


def decode_prior(raw: str, claimant_hash: int) -> Record:
    """The record a losing claimant reads from the JSON holding its key: :func:`decode_record`, except that
    it never raises. A record no reader can decode is still a cached outcome, so the kernel reads it as a
    terminal ``codec`` failure, and the guard replays it as :class:`~edax.errors.CachedFailure` in every
    SDK: the handler never runs again for the key. The failure carries the stored ``payload_hash`` when
    that field still reads, so a reused key is still a :class:`~edax.errors.PayloadMismatch`, else
    ``claimant_hash``. Every :class:`IdempotencyStore` that persists the kernel's JSON reads with this."""
    with errors.mapped():  # a hash outside u64 cannot cross: the terminal CodecError
        return _record_of(_edax.decode_prior_idempotency_record(raw, claimant_hash))


def _record_of(decoded: _edax.IdempotencyRecord) -> Record:
    """A kernel-decoded record as the SDK's :class:`Record`."""
    if decoded.state == "pending":
        return Record(decoded.payload_hash, pending=True)
    if decoded.state == "failed":
        return Record(decoded.payload_hash, error=decoded.error, error_code=decoded.error_code or None)
    # The kernel wrote `result_json` from a value it parsed, so it is JSON.
    return Record(decoded.payload_hash, result=_json.decode(decoded.result_json))
