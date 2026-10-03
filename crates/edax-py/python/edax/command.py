"""Command bus (CQRS write side): register handlers by command type, dispatch through a middleware chain.
The kernel owns the cross-language decisions: payload hashing, the idempotency verdict. See
``docs/sdk/commands.md``."""

from __future__ import annotations

import asyncio
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
from .store import Transactor, Tx, _in_unit_of_work, _transaction

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
    "decode_prior",
    "decode_record",
    "encode_record",
    "idempotent",
    "joins_transaction",
    "retry",
]


logger = logging.getLogger("edax.command")


class Command(Protocol):
    """A command: its instance fields are the payload, ``type`` names the handler.

    The kernel hashes the whole command value, the idempotency key field included, from the field values
    only, so a command decoded off a queue hashes as the one its constructor builds. Declare ``type`` as a
    class attribute so it stays out of the hash, as a Rust command's ``command_type()`` does."""

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


class CommandBus:
    """Dispatches commands to handlers by command type, through a middleware chain wrapped around each
    handler at register time; the first middleware is the outermost. A handler must be a coroutine
    function, and :meth:`register` raises :class:`TypeError` otherwise."""

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

    async def dispatch(self, cmd: Command) -> Any:
        handler = self._handlers.get(cmd.type)
        if handler is None:
            raise HandlerNotFound(cmd.type)
        return await handler(cmd)


def retry(attempts: int, *, pacing: Pacing | None = None) -> Middleware:
    """Retry the handler up to ``attempts`` times, waiting each step of ``pacing`` (default: the kernel's
    jittered backoff on ``asyncio.sleep``). A terminal failure (see :func:`~edax.errors.is_terminal`) is
    re-raised at once; everything else is retried.

    A retry must re-run a whole unit of work, never a handler inside one: a second attempt in the same
    transaction reads the first attempt's staged writes and applies them again, and the commit lands
    both. So ``retry`` refuses to run while any :func:`~edax.store.transaction` is open, with terminal
    :class:`~edax.errors.BadHandle`. Wire it outside a guard that owns the transaction,
    ``CommandBus(retry(n), idempotent(store, key, unit_of_work=db))``, and open no transaction around
    the dispatch. See ``docs/sdk/commands.md``.

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
    - *Enlistment.* Given ``tx``, or with :func:`~edax.store.current_tx` of its own transactor set, all three
      operations join that transaction, so a rollback removes the claim: effectively-once. Otherwise
      standalone: at-least-once.
    - *Declaration.* ``joins_transaction: ClassVar[bool]`` names which of the two you are; absent is
      ``False``. ``True`` obliges you to raise terminal :class:`~edax.errors.BadHandle` for a handle from
      another database, never to auto-commit. The guard hands such a store its transaction's handle
      explicitly, so a store over another database than ``unit_of_work`` fails at the claim.
    - *A loser inside a transaction* must block on the winner's row and then read it, or raise transient
      :class:`~edax.errors.StoreError`; it must never read a stale snapshot and report a win, which runs the
      handler twice. Bound the wait with a lock or statement timeout, also ``StoreError``. A backend that
      cannot block returns the holder's record as pending, reported as :class:`~edax.errors.InProgress`.
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


def _payload_hash(cmd: Command) -> int:
    """The kernel's idempotency hash of ``cmd``: whole value, canonicalized, FNV-1a."""
    payload = _payload_json(cmd)
    with errors.mapped():  # the kernel's `codec:` refusal of the bytes is terminal
        return _edax.payload_hash_json(payload)


# `key` takes `Any`, not `Command`, for the same reason `Handler` does: a key function is written against one
# concrete command and reads a field `Command` does not declare, so no realistic one would be assignable.
def idempotent(
    store: IdempotencyStore,
    key: Callable[[Any], str | None],
    unit_of_work: Transactor | None = None,
    *,
    result_type: Any = None,
    observer: Observer | None = None,
) -> Middleware:
    """Deduplicate dispatches by idempotency key. ``key`` returns the key for a command, or ``None`` to
    bypass. The kernel binds the payload hash to the key, so reuse with a different payload raises
    :class:`~edax.errors.PayloadMismatch`. The first dispatch of a key wins the claim and runs the handler; a
    later or concurrent one loses and acts on the prior record: it replays a cached result, raises
    :class:`~edax.errors.CachedFailure` for a cached terminal error, or raises
    :class:`~edax.errors.InProgress` while the winner is in flight. A transient handler failure releases the
    claim so a retry can re-run. ``unit_of_work`` hands the guard the transaction, which is what lets a
    failed commit release the claim (see :func:`_guarded`). Place :func:`retry` outside a guard that
    owns the transaction: each attempt then re-claims in a fresh unit of work.

    ``key`` owns its own namespace. The command *type* is deliberately outside the payload hash, so two
    different commands carrying the same business fields hash identically and one reused key replays the
    wrong command's cached result. Scope the key by command type, and by service where a ledger is shared.
    See ``docs/sdk/commands.md`` and ``docs/sdk/idempotency.md``.

    A cached success crosses the store as JSON, so a replay hands back the handler's declared return type,
    not the plain JSON value: ``result_type``, else the return annotation of the handler this guard wraps
    directly (when it resolves at module scope), converted with msgspec. A replay that does not convert is
    an unreadable record: the terminal :class:`~edax.errors.CachedFailure` of code ``codec``, as in the
    Rust SDK, whose typed store reads it so. With neither, or with ``result_type=Any``, a replay is
    the plain JSON value, as with an `object` annotation. A ``result_type`` msgspec cannot convert to raises
    :class:`TypeError` at wiring.

    ``observer`` receives a :meth:`~edax.observe.Observer.transaction` per ``unit_of_work`` the guard opens,
    a :meth:`~edax.observe.Observer.claim` verdict for every lost claim and a
    :meth:`~edax.observe.Observer.settled` for every won one; ``None`` is the no-op observer."""
    observe = _wrap(observer)

    def mw(next_: Handler) -> Handler:
        replay_as = _replay_type(result_type, next_)

        async def handler(cmd: Command) -> Any:
            k = key(cmd)
            if k is None:
                return await next_(cmd)
            guard = _Guard(store, cmd.type, k, _payload_hash(cmd), observe, replay_as)
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
    # itself — which is the whole reason the transaction is opened here and not around the caller.
    started = perf_counter_ns()
    # What a raise out of the completion below means: the store would not take the record.
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
        await _complete_or_release(store, key, handle, Record(payload_hash, result=result))
        how = "completed"
        return result
    finally:
        guard.observe.settled(guard.name, how, perf_counter_ns() - started)


async def _settle_failure(
    store: IdempotencyStore, key: str, handle: bytes, record: Record | None
) -> Settlement:
    """Settle the claim after the handler failed: cache ``record``, or release when there is nothing to
    cache. Never raises; returns how the claim ended: ``cached``, ``released``, or ``refused`` when the
    store would not take the compensation. The handler's exception is the verdict the caller,
    :func:`retry` and the dispatcher act on, so a compensation the store refuses must not replace it.
    Inside the host's transaction that refusal is usually the abort the handler's own failed statement
    caused, and the host's rollback frees the claim; on a ledger of its own the claim stays pending until
    its retention frees it. Either way the claim is settled by something other than this call."""
    try:
        if record is None:
            await store.release(key, handle)
            return "released"
        await _complete_or_release(store, key, handle, record)
        return "cached"
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


async def _complete_or_release(store: IdempotencyStore, key: str, handle: bytes, record: Record) -> None:
    """Overwrite the pending claim with its terminal record; if that write fails, release the
    claim instead of leaving it pending for the store's whole retention. Both carry the claim's handle,
    so a fenced store deletes nothing when the write did land and only its reply was lost."""
    try:
        await store.complete(key, handle, record)
    except BaseException:  # recording the outcome failed or was cancelled, not the handler
        # Shielded for the same reason as the cancellation branch in `_guarded_around_the_work`:
        # a cancel landing mid-release leaves the claim pending for the store's whole retention.
        await asyncio.shield(store.release(key, handle))
        raise


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
