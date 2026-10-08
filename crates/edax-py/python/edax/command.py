"""Command bus (CQRS write side): register handlers by command type, dispatch through a middleware chain.
The kernel owns the cross-language decisions: payload hashing, the idempotency verdict. See
``docs/sdk/commands.md``."""

from __future__ import annotations

import contextvars
import functools
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, is_dataclass
from typing import Any, Protocol

import msgspec

from . import _json, _native, errors
from ._chain import Registry, require_async
from ._lineage import stamp
from ._observer import wrap
from ._units import in_unit_of_work
from ._wiring import is_int, wiring_str, wiring_u32
from .aggregate import CommandCodec, PayloadCodec
from .causation import caused_by
from .errors import (  # re-exported: the command errors are part of this module's API
    AlreadyRegistered,
    BadHandle,
    CachedFailure,
    CodecError,
    HandlerNotFound,
    InProgress,
    PayloadMismatch,
)
from .host_value import HOST_TYPES
from .idempotency import Guard, IdempotencyStore, claim_key, guarded
from .message import CommandMessage, Lineage
from .observe import Observer
from .pacing import Pacing
from .tenancy import Tenancy
from .unit_of_work import Transactor, TxT

__all__ = [
    "AlreadyRegistered",
    "BadHandle",
    "CachedFailure",
    "CodecError",
    "Command",
    "CommandBus",
    "Handler",
    "HandlerNotFound",
    "InProgress",
    "Middleware",
    "Next",
    "PayloadMismatch",
    "command_message",
    "current_command",
    "idempotent",
    "received_key",
    "retry",
]


class Command(Protocol):
    """A command: its instance fields are the payload, ``type`` names the handler.

    ``type`` becomes the message's ``name``, and the kernel hashes that name with the command's whole
    value, so a command decoded off a queue hashes as the one its constructor builds. Declare ``type`` as a
    class attribute: the hash takes it once as the name, not again as a field, as a Rust command's
    ``Command::TYPE`` is. A ``version`` attribute, when the command has one, is the schema version its
    message carries; without one the version is 1."""

    # A read-only property, not `type: str`: a plain Protocol attribute demands a settable instance one,
    # which `type: ClassVar[str] = "user.create"` does not satisfy. A property accepts all three shapes.
    @property
    def type(self) -> str: ...


#: A host's command handler: it takes the command object and returns its result.
Handler = Callable[[Any], Awaitable[Any]]
#: One step of a dispatch, as middleware sees it: the :class:`~edax.message.CommandMessage` being
#: dispatched, the unit of dispatch, and the command object decoded from it.
Next = Callable[[CommandMessage, Any], Awaitable[Any]]
#: Wraps the next step of a dispatch. It receives the message as an argument, so a guard reads the
#: idempotency key and the tenant off it and cannot miss it.
Middleware = Callable[[Next], Next]


@dataclass(frozen=True)
class _Dispatch:
    """The dispatch running now: its message, and whether it was received
    (:meth:`CommandBus.dispatch_message`) rather than built here (:meth:`CommandBus.dispatch`). A guard
    claims a received message with no key under its id, and runs one built here with no key unguarded."""

    message: CommandMessage
    received: bool


# The dispatch the bus is running right now. Each dispatch sets a new value and resets it on exit, so a
# nested dispatch sees its own and the outer one gets its own back.
_current: contextvars.ContextVar[_Dispatch | None] = contextvars.ContextVar("edax_command", default=None)


def current_command() -> CommandMessage | None:
    """The :class:`~edax.message.CommandMessage` the bus is dispatching, as it was built or received, for the
    handler's whole run, or ``None`` outside a dispatch: a read-only convenience for a handler, which
    receives only the command. Middleware receives the message as an argument instead (:data:`Next`)."""
    dispatch = _current.get()
    return None if dispatch is None else dispatch.message


def _guard_key(msg: CommandMessage) -> str | None:
    """The key a guard claims for ``msg``: :func:`received_key` when the running dispatch received it, else
    its own ``idempotency_key``, ``None`` running it unguarded."""
    dispatch = _current.get()
    if dispatch is not None and dispatch.received:
        return received_key(msg)
    return msg.idempotency_key


def _step(handler: Handler) -> Next:
    """``handler`` as the innermost step of a dispatch: it takes the command alone. Wrapped, so the guard
    reads the handler's return annotation off it (:func:`idempotent`'s ``result_type``)."""

    @functools.wraps(handler)
    async def step(msg: CommandMessage, cmd: Any) -> Any:
        return await handler(cmd)

    return step


def command_message(
    cmd: Command,
    *,
    codec: PayloadCodec | None = None,
    idempotency_key: str | None = None,
    aggregate_id: str | None = None,
    lineage: Lineage | None = None,
    extensions: dict[str, str] | None = None,
    metadata: dict[str, str] | None = None,
    tenant: str | None = None,
) -> CommandMessage:
    """The :class:`~edax.message.CommandMessage` for ``cmd``: what :meth:`CommandBus.dispatch` runs, and
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
    flow, ext, meta = stamp(lineage, extensions, metadata)
    # The constructor is no kernel function, so `edax._native` does not map it: a value it cannot convert
    # (a version past an unsigned 32-bit integer) is retyped here.
    with errors.mapped():
        return CommandMessage(
            id=_native.guid(),
            name=name,
            version=version,
            data=data,
            correlation_id=flow.correlation_id,
            causation_id=flow.causation_id,
            issued_at=_native.current_timestamp(),
            extensions=ext,
            metadata=meta,
            idempotency_key=idempotency_key,
            aggregate_id=aggregate_id,
            tenant=tenant,
        )


def _payload(cmd: Command, codec: PayloadCodec | None) -> tuple[str, int, bytes]:
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
    if not is_int(version):
        raise CodecError(f"{cmd.type} version must be an int schema version, got {type(version).__name__}")
    return cmd.type, version, _payload_json(cmd)


class CommandBus:
    """Dispatches commands to handlers by command type, through a middleware chain wrapped around each
    handler at register time; the first middleware is the outermost. A handler must be a coroutine
    function, and :meth:`register` raises :class:`TypeError` otherwise.

    Every dispatch runs one :class:`~edax.message.CommandMessage`, the unit of dispatch: each middleware
    receives it with the command (:data:`Next`). The handler takes the command object alone, and reads the
    message with :func:`current_command` if it needs it. It runs under the ambient lineage
    :func:`~edax.causation.propagate` of the message, so the events it saves are the command's children.

    ``codec`` is the bus's wire contract, the one map from a command's name to its class: the
    :class:`~edax.aggregate.CommandCodec` that :meth:`dispatch_message` decodes a received message with, and
    that :meth:`dispatch` builds its message with. A bus built with one refuses at :meth:`register` a command
    the codec does not name, so a missing entry fails at wiring rather than dead-letter every message. A bus
    that only dispatches in-process needs none."""

    def __init__(self, *middleware: Middleware, codec: CommandCodec | None = None) -> None:
        self._handlers: Registry[Next] = Registry(middleware)
        self._codec = codec

    @property
    def codec(self) -> CommandCodec | None:
        """The codec :meth:`dispatch_message` decodes with; ``None`` for a bus that dispatches in-process
        only."""
        return self._codec

    def register(self, command: type[Command], handler: Handler) -> None:
        """Install ``handler`` for commands of the class ``command``, under its ``type``: the name
        :meth:`dispatch` routes ``cmd.type`` by and :meth:`dispatch_message` routes a message's ``name`` by,
        written once, on the command. A class whose ``type`` is not a class-level ``str`` raises
        :class:`TypeError`, a type the bus's ``codec`` does not read as ``command`` the terminal
        :class:`~edax.errors.BadHandle`, and a second handler for one name
        :class:`~edax.errors.AlreadyRegistered`."""
        command_type = getattr(command, "type", None)
        if not isinstance(command_type, str):
            raise TypeError(
                f"{command!r} declares no command type: give the class a `type` str attribute, "
                '`type: ClassVar[str] = "user.create"`'
            )
        if self._codec is not None:
            decoded_as = self._codec.type_of(command_type)
            if decoded_as is None:
                raise BadHandle(
                    f"the bus's codec reads no {command_type!r} command: add {command.__name__} to it, or "
                    "every received message of it fails to decode"
                )
            # A stale class under the name would reach the handler from a received message, and `dispatch`
            # of this class would fail to marshal on every call.
            if decoded_as is not command:
                raise BadHandle(
                    f"the bus's codec reads {command_type!r} as {decoded_as.__name__}, not "
                    f"{command.__name__}: register the class the codec names"
                )
        # Reject at wiring time: a sync handler otherwise fails on its first dispatch, far from the mistake.
        require_async(handler, f"command handler for {command_type!r}")
        self._handlers.register(command_type, _step(handler))

    def on(self, command: type[Command]) -> Callable[[Handler], Handler]:
        """Decorator form of :meth:`register`, as :meth:`~edax.router.EventRouter.on` is for events.
        Returns the bare handler, not the wrapped one."""

        def deco(fn: Handler) -> Handler:
            self.register(command, fn)
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
        :func:`command_message` of ``cmd``, the bus's ``codec`` and the keyword arguments, which take the same
        meaning there. Pass ``idempotency_key`` for a request a client may send twice; :func:`idempotent`
        claims it. An unregistered ``cmd.type`` raises :class:`~edax.errors.HandlerNotFound`."""
        handler = self._handler(cmd.type)
        msg = command_message(
            cmd,
            codec=self._codec,
            idempotency_key=idempotency_key,
            aggregate_id=aggregate_id,
            lineage=lineage,
            extensions=extensions,
            metadata=metadata,
            tenant=tenant,
        )
        return await _run(handler, _Dispatch(msg, received=False), cmd)

    async def dispatch_message(self, msg: CommandMessage) -> Any:
        """Run a received :class:`~edax.message.CommandMessage`: off a broker, or from a command outbox. The
        bus's ``codec`` turns the message back into the command object by ``name`` and ``version``, as
        ``json_codec({"Ship": Ship})`` does, and the handler registered under ``msg.name`` runs it. The
        message runs as received: its id, lineage and key are what the sender set.

        A guard claims a message with no ``idempotency_key`` under its ``id``, the kernel's
        ``received_key``. A broker delivers at least once, so the same message can arrive twice, and both
        copies carry one id; a guard then replays the second instead of running the handler again. A bus
        built with no ``codec`` raises the fatal :class:`~edax.errors.BadHandle`, an unregistered
        ``msg.name`` :class:`~edax.errors.HandlerNotFound`, and a message the codec cannot read the terminal
        :class:`~edax.errors.CodecError`."""
        if self._codec is None:
            raise BadHandle(
                "dispatch_message decodes with the bus's codec: build it with CommandBus(codec=...)"
            )
        handler = self._handler(msg.name)
        cmd = self._codec.unmarshal(msg)
        return await _run(handler, _Dispatch(msg, received=True), cmd)

    def _handler(self, name: str) -> Next:
        handler = self._handlers.get(name)
        if handler is None:
            raise HandlerNotFound(name)
        return handler


async def _run(chain: Next, dispatch: _Dispatch, cmd: Any) -> Any:
    """Run ``chain`` on the dispatch's message and ``cmd``, with the message as the handler's current command
    and the lineage of its children."""
    msg = dispatch.message
    token = _current.set(dispatch)
    try:
        with caused_by(msg):
            return await chain(msg, cmd)
    finally:
        _current.reset(token)


def retry(attempts: int, *, pacing: Pacing | None = None) -> Middleware:
    """Retry the handler up to ``attempts`` times, waiting each step of ``pacing`` (default: the kernel's
    jittered backoff on ``asyncio.sleep``). A terminal failure (see :func:`~edax.errors.is_terminal`) is
    re-raised at once; everything else is retried.

    A retry must re-run a whole unit of work, never a handler inside one: a second attempt in the same
    transaction reads the first attempt's staged writes and applies them again, and the commit lands
    both. So ``retry`` refuses to run while any :func:`~edax.unit_of_work.transaction` is open, with terminal
    :class:`~edax.errors.BadHandle`. Wire it outside a guard that owns the transaction,
    ``CommandBus(retry(n), idempotent(store, db, scope="api"))``, and open no transaction around the
    dispatch. See ``docs/sdk/commands.md``.

    When to stop and how long to wait is the kernel's ``retry_step``, pinned by
    ``testdata/golden/retry_step.json``, so both SDKs retry alike: a terminal failure stops at once, the
    ``attempts``-th failure is final, and contention (:class:`~edax.errors.InProgress`) spends an attempt,
    because the caller is waiting.

    ``attempts`` the kernel refuses raises the terminal :class:`~edax.errors.BadHandle` here, at wiring
    time: ``0``, since no attempt at all is not a retry policy, and one that is not an unsigned 32-bit
    integer."""
    wiring_u32(attempts, "retry attempts")
    _native.retry_attempts(attempts)
    pace = pacing or Pacing()

    def mw(next_: Next) -> Next:
        async def step(msg: CommandMessage, cmd: Any) -> Any:
            if in_unit_of_work():
                raise BadHandle(
                    "retry was dispatched inside an open transaction: a second attempt would re-apply the "
                    "first attempt's staged writes, and one commit would land both. Put retry outside "
                    "idempotent(..., unit_of_work=...) and open no transaction around the dispatch"
                )
            failed = 0
            while True:
                try:
                    return await next_(msg, cmd)
                except Exception as e:
                    wait = _native.retry_step(attempts, failed, errors.code_of(e))
                    if wait is None:
                        raise
                await pace.wait(wait)
                failed += 1

        return step

    return mw


def received_key(msg: CommandMessage) -> str:
    """The key a guard claims for a command received from a broker or an outbox: its ``idempotency_key``,
    or its ``id`` when it has none, so a redelivery of one message is caught. An empty key is a key. The
    rule is the kernel's, pinned by ``testdata/golden/command_received_key.json``."""
    return _native.received_key(msg.id, msg.idempotency_key)


def _payload_json(cmd: Command) -> bytes:
    """The whole command as one JSON object: the serialization step of the kernel's hash rule."""
    # Canonicalization is the kernel's job, so every binding agrees on the hash; the SDK's one encoder
    # turns the value into JSON, and its every failure is a terminal CodecError. Reading the fields can
    # fail too (a `__slots__` class has no `vars()`): deterministic as well.
    try:
        data = _payload_data(cmd)
    except TypeError as e:
        raise CodecError(f"command payload is not JSON-encodable: {e}") from e
    return _json.encode_bytes(data)


def _payload_data(cmd: Command) -> Any:
    """The command's instance fields, whatever shape the command class takes."""
    # Three shapes: a dataclass's and a Struct's declared fields, a plain object's instance attributes.
    # The golden vector covers all three, so the fallback is contract, not convenience.
    if is_dataclass(cmd) or isinstance(cmd, msgspec.Struct):
        return cmd
    return vars(cmd)


def _payload_hash(cmd: Command, name: str) -> int:
    """The kernel's idempotency hash of ``cmd``: the message ``name`` and the command's whole value,
    canonicalized, FNV-1a. Pinned by ``testdata/golden/command_hash.json``."""
    payload = _payload_json(cmd)
    # The kernel's `codec:` refusal of the bytes is terminal.
    return _native.command_hash_json(name, payload)


def idempotent(
    store: IdempotencyStore[TxT],
    unit_of_work: Transactor[TxT] | None = None,
    *,
    scope: str,
    tenancy: Tenancy,
    result_type: Any = None,
    observer: Observer | None = None,
) -> Middleware:
    """Deduplicate dispatches by the ``idempotency_key`` of the message each one runs (:data:`Next`). A
    message :meth:`CommandBus.dispatch` built with no key runs unguarded and writes nothing to the ledger; one
    :meth:`CommandBus.dispatch_message` received with no key is claimed under its ``id``
    (:func:`received_key`). The first dispatch of a key wins
    the claim and runs the handler; a later or concurrent one loses and acts on the prior record: it
    replays a cached result, raises :class:`~edax.errors.CachedFailure` for a cached terminal error, or
    raises :class:`~edax.errors.InProgress` while the winner is in flight. A transient handler failure
    releases the claim so a retry can re-run. ``unit_of_work`` hands the guard the transaction, which is
    what lets a failed commit release the claim (see :func:`~edax.idempotency.guarded`). Place
    :func:`retry` outside a guard that owns the transaction: each attempt then re-claims in a fresh unit of
    work.

    ``scope`` names this guard, usually the service or the stage it guards: ``"api"`` at ingress,
    ``"billing-worker"`` on a consumer. The ledger key is the kernel's :func:`claim_key` of the scope, the
    message's ``idempotency_key`` and, under a pool, its ``tenant``. Two guards that share a ledger never
    replay each other's outcome for one key. The kernel hashes the message name with the value the handler
    receives, so one key reused by another command, or with another payload, raises
    :class:`~edax.errors.PayloadMismatch`. :func:`~edax.idempotency.forget` deletes a record by the same
    scope, tenancy, tenant and key. See ``docs/sdk/commands.md`` and ``docs/sdk/idempotency.md``.

    A cached success crosses the store as JSON, so a replay hands back the handler's declared return type,
    not the plain JSON value: ``result_type``, else the return annotation of the handler this guard wraps
    directly (when it resolves at module scope), converted with msgspec. A replay that does not convert is
    an unreadable record: the terminal :class:`~edax.errors.CachedFailure` of code ``codec``, as in the
    Rust SDK, whose typed store reads it so. With neither, or with ``result_type=Any``, a replay is
    the plain JSON value, as with an ``object`` annotation. A ``result_type`` msgspec cannot convert to raises
    :class:`TypeError` at wiring, and a ``scope`` that is not a ``str`` the terminal
    :class:`~edax.errors.BadHandle`.

    ``tenancy`` is how the deployment this guard runs in separates tenants, fixed at wiring as it is on a
    :class:`~edax.repository.Repository`. Under :attr:`~edax.tenancy.Tenancy.POOL` the ledger key is composed
    with the message's ``tenant`` (:func:`claim_key`), so one request id reused by two tenants claims two
    rows rather than replaying one tenant's result to the other. Under a pool or a bridge, a message that
    names no tenant raises :class:`~edax.errors.CodecError` instead of claiming a shared key. ``tenancy`` has
    no default, as on :func:`~edax.idempotency.forget`: a pooled deployment that missed one guard would claim
    outside its tenant.

    ``observer`` receives a :meth:`~edax.observe.Observer.transaction` per ``unit_of_work`` the guard opens,
    a :meth:`~edax.observe.Observer.claim` verdict for every lost claim and a
    :meth:`~edax.observe.Observer.settled` for every won one; ``None`` is the no-op observer."""
    wiring_str(scope, "idempotent(scope=...), which names the guard,")
    observe = wrap(observer)

    def mw(next_: Next) -> Next:
        replay_as = _replay_type(result_type, next_)

        async def step(msg: CommandMessage, cmd: Any) -> Any:
            request = _guard_key(msg)
            if request is None:
                return await next_(msg, cmd)
            key = claim_key(scope, request, tenancy=tenancy, tenant=msg.tenant)
            guard = Guard(store, msg.name, key, _payload_hash(cmd, msg.name), observe, replay_as)
            return await guarded(guard, lambda: next_(msg, cmd), unit_of_work)

        return step

    return mw


def _replay_type(explicit: Any, handler: Next) -> Any:
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
    info = msgspec.inspect.type_info(chosen)
    if isinstance(info, msgspec.inspect.CustomType) and info.cls not in HOST_TYPES:
        raise TypeError(
            f"a cached result cannot be replayed as {chosen!r}: msgspec has no conversion for it. "
            "Return a dataclass, a msgspec.Struct or a JSON type, or pass result_type=typing.Any"
        )
    return chosen
