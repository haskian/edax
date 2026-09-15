"""Command bus (CQRS write side): register handlers by command type, dispatch through a middleware chain.
The kernel owns the cross-language decisions: payload hashing, the idempotency verdict. See
``docs/sdk/commands.md``."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, fields, is_dataclass
from time import perf_counter_ns
from typing import Any, Callable, Optional, Protocol

import msgspec

from . import _edax, errors
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
from .observe import Observer, _wrap
from .store import Transactor, Tx, _transaction, current_tx

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

    def register(self, type: str, handler: Handler) -> None:
        if type in self._handlers:
            raise AlreadyRegistered(type)
        # Reject at wiring time: a sync handler otherwise fails on its first dispatch, far from the mistake.
        if not _is_async(handler):
            raise TypeError(
                f"command handler for {type!r} must be a coroutine function, got {handler.__class__.__name__}"
            )
        for mw in reversed(self._middleware):
            handler = mw(handler)
        self._handlers[type] = handler

    async def dispatch(self, cmd: Command) -> Any:
        handler = self._handlers.get(cmd.type)
        if handler is None:
            raise HandlerNotFound(cmd.type)
        return await handler(cmd)


def retry(attempts: int) -> Middleware:
    """Retry the handler up to ``attempts`` times on the kernel's backoff schedule. A terminal failure (see
    :func:`~edax.errors.is_terminal`) is re-raised at once; everything else is retried. Place inside
    :func:`idempotent` so retries share one claim, but outside a guard you passed ``unit_of_work`` to."""
    attempts = max(1, attempts)

    def mw(next_: Handler) -> Handler:
        async def handler(cmd: Command) -> Any:
            for i in range(attempts - 1):
                try:
                    return await next_(cmd)
                except Exception as e:
                    if is_terminal(e):
                        raise
                await asyncio.sleep(_edax.backoff_millis(i) / 1000)
            return await next_(cmd)  # last attempt: its failure is final

        return handler

    return mw


@dataclass
class Record:
    """Stored outcome of a dispatch, keyed by idempotency key. ``pending`` is ``True`` while a claim is held
    but the handler has not completed, so an in-flight claim is never replayed as a ``None`` success. A
    completed record with ``error=None`` is a success whose value is ``result``, even when that is ``None``.
    Wire shape, reading order and replay verdict are the kernel's (``testdata/golden``)."""

    payload_hash: int
    result: Any = None
    error: Optional[str] = None
    pending: bool = False


class IdempotencyStore(Protocol):
    """Per-key outcome store behind :func:`idempotent`. The seam is an atomic claim.

    What a backend must guarantee; rationale and worked examples in ``docs/backends/README.md``.

    - *Atomic claim.* ``claim`` is an insert-if-absent, linearizable across every process sharing the store:
      one caller gets ``None``, every other gets the existing :class:`Record`. Never a read then a write.
    - *Pending state.* The inserted record carries ``pending=True`` and the given ``payload_hash``, and must
      come back faithfully; the guard reads it to tell an in-flight claim from a completed ``None`` result.
    - *Enlistment.* Given ``tx``, or with :func:`~edax.store.current_tx` set, all three operations join that
      transaction, so a rollback removes the claim: effectively-once. Otherwise standalone: at-least-once.
    - *Declaration.* ``joins_transaction: ClassVar[bool]`` names which of the two you are; absent is
      ``False``. ``True`` obliges you to raise terminal :class:`~edax.errors.BadHandle` for a handle from
      another database, never to auto-commit.
    - *A loser inside a transaction* must block on the winner's row and then read it, or raise transient
      :class:`~edax.errors.StoreError`; it must never read a stale snapshot and report a win, which runs the
      handler twice. Bound the wait with a lock or statement timeout, also ``StoreError``. A backend that
      cannot block returns the holder's record as pending, reported as :class:`~edax.errors.InProgress`.
    - *Complete / release.* ``complete`` overwrites the key with the terminal record, ``release`` deletes it.
      Both are idempotent; releasing an absent key is a no-op. A fenced backend that refuses a ``complete``
      because the claim was taken over must keep its fence: the guard answers a refused ``complete`` with
      ``release``, and an unfenced ``release`` would then delete the claim that replaced this one.
    - *Retention.* A record must outlive the longest window in which the same key can be redelivered.
    - *Record contents.* ``payload_hash`` is the kernel's unsigned 64-bit hash: store it losslessly, never a
      signed ``BIGINT``; one outside ``0 .. 2**64 - 1`` raises :class:`~edax.errors.CodecError`. ``result``
      must round-trip unchanged, within the codec's integer bounds (``docs/kernels/cqrs.md``).
    - *Errors.* Backend failures raise :class:`~edax.errors.StoreError`, which the guard treats as transient.
    """

    async def claim(self, key: str, payload_hash: int, tx: Optional[Tx] = None) -> Optional[Record]:
        """Atomically insert a pending record. ``None`` means this caller won, else the existing record."""
        ...

    async def complete(self, key: str, record: Record, tx: Optional[Tx] = None) -> None:
        """Overwrite ``key`` with the terminal ``record``. Idempotent."""
        ...

    async def release(self, key: str, tx: Optional[Tx] = None) -> None:
        """Delete ``key`` so a transient failure can retry. Idempotent."""
        ...


def _payload_json(cmd: Command) -> bytes:
    """The whole command as one JSON object: the serialization step of the kernel's hash rule."""
    # Canonicalization is the kernel's job, so every binding agrees on the hash. msgspec, not json.dumps, so
    # UUID / datetime / Decimal encode deterministically. Every failure here is deterministic: all terminal.
    try:
        return msgspec.json.encode(_payload_data(cmd))
    except Exception as e:
        raise CodecError(f"command payload is not JSON-encodable: {e}") from e


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
        return items if any(new is not old for new, old in zip(items, value)) else value
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


@contextmanager
def _lossless_payload_hash(payload_hash: int) -> Iterator[None]:
    """Retype the kernel's range rejection of a stored ``payload_hash`` as a terminal error. A lossy store
    returns a negative hash, which the binding rejects with a bare ``OverflowError`` that
    :func:`~edax.errors.is_terminal` would read as transient. The defect is deterministic, so it is
    terminal."""
    try:
        yield
    except OverflowError as e:
        raise CodecError(
            f"stored payload_hash {payload_hash} is outside the kernel's unsigned 64-bit range; "
            "persist it losslessly (unsigned BIGINT or a string)"
        ) from e


def _payload_hash(cmd: Command) -> int:
    """The kernel's idempotency hash of ``cmd``: whole value, canonicalized, FNV-1a."""
    try:
        return _edax.payload_hash_json(_payload_json(cmd))
    except RuntimeError as e:  # the kernel could not canonicalize the bytes: deterministic
        raise CodecError(f"command payload canonicalization failed: {e}") from e


# `key` takes `Any`, not `Command`, for the same reason `Handler` does: a key function is written against one
# concrete command and reads a field `Command` does not declare, so no realistic one would be assignable.
def idempotent(
    store: IdempotencyStore,
    key: Callable[[Any], Optional[str]],
    unit_of_work: Optional[Transactor] = None,
    *,
    observer: Optional[Observer] = None,
) -> Middleware:
    """Deduplicate dispatches by idempotency key. ``key`` returns the key for a command, or ``None`` to
    bypass. The kernel binds the payload hash to the key, so reuse with a different payload raises
    :class:`~edax.errors.PayloadMismatch`. The first dispatch of a key wins the claim and runs the handler; a
    later or concurrent one loses and acts on the prior record: it replays a cached result, raises
    :class:`~edax.errors.CachedFailure` for a cached terminal error, or raises
    :class:`~edax.errors.InProgress` while the winner is in flight. A transient handler failure releases the
    claim so a retry can re-run. ``unit_of_work`` hands the guard the transaction, which is what lets a
    failed commit release the claim (see :func:`_guarded`); place this guard outside :func:`retry`, unless
    you pass it and then inside.

    ``key`` owns its own namespace. The command *type* is deliberately outside the payload hash, so two
    different commands carrying the same business fields hash identically and one reused key replays the
    wrong command's cached result. Scope the key by command type, and by service where a ledger is shared.
    See ``docs/sdk/commands.md`` and ``docs/sdk/idempotency.md``.

    ``observer`` receives a :meth:`~edax.observe.Observer.transaction` per ``unit_of_work`` the guard opens,
    a :meth:`~edax.observe.Observer.claim` verdict for every lost claim and a
    :meth:`~edax.observe.Observer.settled` for every won one; ``None`` is the no-op observer."""
    observe = _wrap(observer)

    def mw(next_: Handler) -> Handler:
        async def handler(cmd: Command) -> Any:
            k = key(cmd)
            if k is None:
                return await next_(cmd)
            return await _guarded(
                store, cmd.type, k, _payload_hash(cmd), lambda: next_(cmd), unit_of_work, observe
            )

        return handler

    return mw


def joins_transaction(store: IdempotencyStore) -> bool:
    """Whether ``store`` enlists in the caller's unit of work, per its
    :attr:`IdempotencyStore.joins_transaction` declaration. Absent, the answer is ``False``, the safe side: a
    needless compensation costs one no-op delete, a skipped one leaves a ``done`` record for rolled-back
    work."""
    return bool(getattr(store, "joins_transaction", False))


@asynccontextmanager
async def _unit(unit_of_work: Optional[Transactor], observe: Observer) -> AsyncIterator[Optional[Tx]]:
    if unit_of_work is None:
        yield None
    else:
        async with _transaction(unit_of_work, observe) as tx:
            yield tx


async def _guarded(
    store: IdempotencyStore,
    name: str,
    key: str,
    payload_hash: int,
    run: Callable[[], Awaitable[Any]],
    unit_of_work: Optional[Transactor],
    observe: Observer,
) -> Any:
    """Run ``run`` once under an atomic claim on ``key`` and return its result, or the cached one.

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
    if unit_of_work is not None and current_tx() is not None:
        # The Transactor contract says the SDK never nests: a second unit of work would commit the claim
        # independently of the writes it guards. BadHandle, not StoreError, so the dispatcher dead-letters
        # the wiring mistake instead of redelivering it forever.
        raise BadHandle(
            "idempotency guard was given a unit_of_work while a transaction is already open: "
            "either pass unit_of_work or open the transaction around the dispatch, not both"
        )
    if unit_of_work is not None and joins_transaction(store):
        return await _guarded_in_one_commit(store, name, key, payload_hash, run, unit_of_work, observe)
    return await _guarded_around_the_work(store, name, key, payload_hash, run, unit_of_work, observe)


async def _guarded_in_one_commit(
    store: IdempotencyStore,
    name: str,
    key: str,
    payload_hash: int,
    run: Callable[[], Awaitable[Any]],
    unit_of_work: Transactor,
    observe: Observer,
) -> Any:
    """``begin; claim; run; complete; commit``. The ledger shares the transaction, so every failure path
    below is a rollback that drops the claim with the work: reported as ``released``."""
    async with _transaction(unit_of_work, observe):
        prior = await store.claim(key, payload_hash)
        if prior is not None:
            return _replay(name, key, payload_hash, prior, observe)
        started = perf_counter_ns()
        how = "released"
        try:
            result = await run()
            await store.complete(key, Record(payload_hash, result=result))
            how = "completed"
            return result
        finally:
            observe.settled(name, how, perf_counter_ns() - started)


async def _guarded_around_the_work(
    store: IdempotencyStore,
    name: str,
    key: str,
    payload_hash: int,
    run: Callable[[], Awaitable[Any]],
    unit_of_work: Optional[Transactor],
    observe: Observer,
) -> Any:
    """``claim; begin; run; commit; complete``. The claim lands on its own, so every failure after
    it must be compensated by hand. With ``unit_of_work`` unset the middle is just ``run()``."""
    # A claim that raises is deliberately not compensated: the guard cannot tell a lost response from
    # reported contention, and releasing in the second case deletes the winner's live claim.
    prior = await store.claim(key, payload_hash)
    if prior is not None:
        return _replay(name, key, payload_hash, prior, observe)

    # Won the claim. From here every exit must settle the record, including a failure of the commit
    # itself — which is the whole reason the transaction is opened here and not around the caller.
    started = perf_counter_ns()
    how = "refused"  # what a raise out of the completion below means: the store would not take the record
    try:
        try:
            async with _unit(unit_of_work, observe):
                result = await run()
        except Exception as e:
            record = Record(payload_hash, error=str(e)) if is_terminal(e) else None
            how = await _settle_failure(store, key, record)
            raise
        except BaseException:
            # Cancelled: nobody will complete this claim, so release it or every later dispatch raises
            # InProgress. Shielded, or a second cancel would abandon the claim until its TTL expires.
            how = await asyncio.shield(_settle_failure(store, key, None))
            raise
        await _complete_or_release(store, key, Record(payload_hash, result=result))
        how = "completed"
        return result
    finally:
        observe.settled(name, how, perf_counter_ns() - started)


async def _settle_failure(store: IdempotencyStore, key: str, record: Optional[Record]) -> str:
    """Settle the claim after the handler failed: cache ``record``, or release when there is nothing to
    cache. Never raises; returns how the claim ended: ``cached``, ``released``, or ``refused`` when the
    store would not take the compensation. The handler's exception is the verdict the caller,
    :func:`retry` and the dispatcher act on, so a compensation the store refuses must not replace it.
    Inside the host's transaction that refusal is usually the abort the handler's own failed statement
    caused, and the host's rollback frees the claim; on a ledger of its own the claim stays pending until
    its retention frees it. Either way the claim is settled by something other than this call."""
    try:
        if record is None:
            await store.release(key)
            return "released"
        await _complete_or_release(store, key, record)
        return "cached"
    except Exception:
        # Expected when the ledger shares a transaction the handler already aborted, so it is not news.
        expected = joins_transaction(store) and current_tx() is not None
        logger.log(
            logging.DEBUG if expected else logging.WARNING,
            "edax idempotency: compensation for %r was refused after a failed handler; "
            "the ambient rollback or the record's retention frees the claim",
            key,
            exc_info=True,
        )
        return "refused"


def _replay(name: str, key: str, payload_hash: int, prior: Record, observe: Observer) -> Any:
    """Lost the claim: the kernel classifies the winner's record and this maps the verdict onto the SDK's
    exceptions. The observer hears the kernel's verdict verbatim, so no SDK renames it. Never
    releases, because the key belongs to the winner."""
    verdict = _outcome(prior, payload_hash)
    observe.claim(name, verdict)
    if verdict == "payload_mismatch":
        raise PayloadMismatch(key)
    if verdict == "in_progress":
        raise InProgress(key)
    if verdict == "failed":
        raise CachedFailure(prior.error)
    return prior.result  # "succeeded": the cached value, `None` included


async def _complete_or_release(store: IdempotencyStore, key: str, record: Record) -> None:
    """Overwrite the pending claim with its terminal record; if that write fails, release the
    claim instead of leaving it pending for the store's whole retention."""
    try:
        await store.complete(key, record)
    except BaseException:  # recording the outcome failed or was cancelled, not the handler
        # Shielded for the same reason as the cancellation branch in `_guarded_around_the_work`:
        # a cancel landing mid-release leaves the claim pending for the store's whole retention.
        await asyncio.shield(store.release(key))
        raise


def _outcome(prior: Record, current: int) -> str:
    """The kernel's verdict on a prior record: ``payload_mismatch``, ``in_progress``, ``failed``
    or ``succeeded``. The result plays no part in the verdict, so only the classifying fields
    cross."""
    with _lossless_payload_hash(prior.payload_hash):
        return _edax.idempotency_outcome(current, prior.payload_hash, prior.pending, prior.error)


def encode_record(record: Record) -> str:
    """Serialize a :class:`Record` to the kernel's idempotency-record JSON. Public because it is a wire
    contract: every JSON-persisting :class:`IdempotencyStore` backend and every SDK read and write this exact
    shape, so one key stays readable across SDKs."""
    try:
        result_json = msgspec.json.encode(record.result).decode()
    except Exception as e:
        raise CodecError(f"idempotency record is not JSON-encodable: {e}") from e
    with errors.mapped(), _lossless_payload_hash(record.payload_hash):
        # the kernel rejects only what it cannot encode: `codec:` -> CodecError
        return _edax.encode_idempotency_record(record.payload_hash, record.pending, result_json, record.error)


def decode_record(raw: str) -> Record:
    """Parse the kernel's idempotency-record JSON. The kernel applies the reading order, so a
    record written by an older writer (no ``pending`` field) reads as completed, and a completed
    record with a ``null`` result reads as a success whose value is ``None``."""
    try:
        decoded = _edax.decode_idempotency_record(raw)
    except RuntimeError as e:
        raise CodecError(f"idempotency record is unreadable: {e}") from e
    if decoded.state == "pending":
        return Record(decoded.payload_hash, pending=True)
    if decoded.state == "failed":
        return Record(decoded.payload_hash, error=decoded.error)
    try:
        result = msgspec.json.decode(decoded.result_json)
    except Exception as e:
        # The kernel validated its own fields, not the host's `result`: the same terminal fault.
        raise CodecError(f"idempotency record is unreadable: {e}") from e
    return Record(decoded.payload_hash, result=result)
