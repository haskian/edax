"""The idempotency guard engine both write paths share: the :class:`IdempotencyStore` seam, its record and
wire shape, and :func:`guarded`, the claim/complete/release orchestration behind
:func:`~edax.command.idempotent` (commands) and :func:`~edax.router.idempotent_by_id` (events). The kernel
owns every decision: the ledger key, the verdict on a prior record, and what a failure caches. See
``docs/sdk/idempotency.md``."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import perf_counter_ns
from typing import Any, Generic, Literal, Protocol, get_args

import msgspec

from . import _json, _names, _native, errors
from ._units import in_unit_of_work, open_transaction
from .errors import BadHandle, CachedFailure
from .observe import CLAIM_VERDICTS, ClaimVerdict, Observer, Settlement
from .tenancy import Tenancy
from .unit_of_work import Transactor, TxT, TxT_contra, current_tx

__all__ = [
    "Claimed",
    "Done",
    "Failed",
    "Guard",
    "IdempotencyStore",
    "Pending",
    "Record",
    "RecordState",
    "Won",
    "claim_key",
    "decode_claim",
    "decode_prior",
    "decode_record",
    "encode_record",
    "forget",
    "guarded",
    "joins_transaction",
]

logger = logging.getLogger("edax.idempotency")


@dataclass(frozen=True)
class Pending:
    """A claim is held and its handler has not completed, so an in-flight claim is never replayed as a
    ``None`` success."""


@dataclass(frozen=True)
class Done:
    """The handler completed: a replay returns ``result``, even when that is ``None``."""

    result: Any = None


@dataclass(frozen=True)
class Failed:
    """The handler failed terminally: a replay raises it. ``error`` is the message, ``error_code`` the
    taxonomy code (``None`` for a record whose writer stored none), so a replay in any SDK keeps the
    original code."""

    error: str
    error_code: str | None = None


#: Where a claimed key stands, as the kernel's ``State`` names it: one of three, never two at once.
RecordState = Pending | Done | Failed


@dataclass(frozen=True)
class Record:
    """Stored outcome of a dispatch, keyed by idempotency key: the ``payload_hash`` the key was claimed
    with, and its ``state``. A union, as the kernel's ``State`` is, so a record cannot be pending and
    failed at once. Build one with :meth:`pending`, :meth:`done` or :meth:`failed`; read the state with
    ``match`` or ``isinstance``. Wire shape, reading order and replay verdict are the kernel's
    (``testdata/golden``)."""

    payload_hash: int
    state: RecordState

    @classmethod
    def pending(cls, payload_hash: int) -> Record:
        """A claim whose handler has not completed."""
        return cls(payload_hash, Pending())

    @classmethod
    def done(cls, payload_hash: int, result: Any = None) -> Record:
        """A completed success carrying the handler's result."""
        return cls(payload_hash, Done(result))

    @classmethod
    def failed(cls, payload_hash: int, error: str, error_code: str | None = None) -> Record:
        """A completed terminal failure: its message and its code."""
        return cls(payload_hash, Failed(error, error_code))


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


class IdempotencyStore(Protocol[TxT_contra]):
    """Per-key outcome store behind :func:`~edax.command.idempotent`. The seam is an atomic claim.

    What a backend must guarantee; rationale and worked examples in ``docs/backends/README.md``.

    - *Atomic claim.* ``claim`` is an insert-if-absent, linearizable across every process sharing the store:
      one caller gets :class:`Won`, every other gets the existing :class:`Record`. Never a read then a write.
    - *Pending state.* The inserted record is :meth:`Record.pending` of the given ``payload_hash``, and must
      come back faithfully; the guard reads it to tell an in-flight claim from a completed ``None`` result.
    - *Enlistment.* Given ``tx``, or with :func:`~edax.unit_of_work.current_tx` of its own transactor set,
      every
      operation joins that transaction, so a rollback removes the claim: effectively-once. Otherwise
      standalone: at-least-once.
    - *Declaration.* ``joins_transaction: ClassVar[bool]`` names which of the two you are; absent is
      ``False``. ``True`` obliges you to raise terminal :class:`~edax.errors.BadHandle` for a handle from
      another database, never to auto-commit. The guard hands such a store its transaction's handle
      explicitly, so a store over another database than ``unit_of_work`` fails at the claim. Such a store
      names its transactor as its ``transactor`` attribute (the :class:`~edax.unit_of_work.Transactor`
      *Identity* clause): inside the host's transaction, the guard runs only when that transactor has the
      ambient tx, and raises :class:`~edax.errors.BadHandle` otherwise.
    - *A loser inside a transaction* must block on the winner's row and then read it, or raise transient
      :class:`~edax.errors.StoreError`; it must never read a stale snapshot and report a win, which runs the
      handler twice. Bound the wait with a lock or statement timeout, which raises ``StoreError`` or
      :class:`~edax.errors.InProgress`: the holder may still complete. A backend that cannot block returns
      the holder's record as pending, reported as ``InProgress``.
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

    async def claim(self, key: str, payload_hash: int, tx: TxT_contra | None = None) -> Claimed:
        """Atomically insert a pending record. :class:`Won` means this caller won, else the existing
        record."""
        ...

    async def complete(self, key: str, handle: bytes, record: Record, tx: TxT_contra | None = None) -> None:
        """Overwrite ``key`` with the terminal ``record`` while it holds the claim ``handle`` names.
        Idempotent."""
        ...

    async def release(self, key: str, handle: bytes, tx: TxT_contra | None = None) -> None:
        """Delete ``key`` while it holds the claim ``handle`` names, so a transient failure can retry.
        Idempotent."""
        ...

    async def forget(self, key: str, tx: TxT_contra | None = None) -> None:
        """Delete the settled record ``key`` holds, so the next claim wins. The *Forget* clause above says
        what happens to a pending claim and to an absent key."""
        ...


def claim_key(scope: str, idempotency_key: str, *, tenancy: Tenancy, tenant: str | None = None) -> str:
    """The ledger key the :func:`~edax.command.idempotent` guard named ``scope`` claims for
    ``idempotency_key``, in a deployment that separates tenants by ``tenancy``. Two scopes never share a
    ledger key. Under
    :attr:`~edax.tenancy.Tenancy.POOL` the key carries ``tenant``, so two tenants never share one either.
    Under a mode that requires a tenant, a call that names none raises :class:`~edax.errors.CodecError`.
    The composition is the kernel's, pinned by ``testdata/golden/claim_key.json``, so a guard in another
    binding sharing the ledger claims the same row."""
    return _native.claim_key(tenancy.value, scope, idempotency_key, tenant)


async def forget(
    store: IdempotencyStore[TxT],
    scope: str,
    idempotency_key: str,
    *,
    tenancy: Tenancy,
    tenant: str | None = None,
    tx: TxT | None = None,
) -> None:
    """Delete the record the :func:`~edax.command.idempotent` guard named ``scope`` keeps for
    ``idempotency_key``, so the next dispatch of that key runs its handler again. Use it to re-drive a
    dead-lettered command whose failure was cached. Pass the guard's ``tenancy`` and the command's ``tenant``:
    the record lives under the key :func:`claim_key` composes from them. A mode that requires a tenant raises
    :class:`~edax.errors.CodecError` without one, instead of deleting nothing. ``tenancy`` has no default:
    a pooled caller that left it out would compose the silo key and delete nothing. The *Forget* clause of
    :class:`IdempotencyStore` says what happens to a claim still in flight and to a key with no record."""
    await store.forget(claim_key(scope, idempotency_key, tenancy=tenancy, tenant=tenant), tx)


def joins_transaction(store: IdempotencyStore[Any]) -> bool:
    """Whether ``store`` enlists in the caller's unit of work, per its
    :attr:`IdempotencyStore.joins_transaction` declaration. Absent, the answer is ``False``, the safe side: a
    needless compensation costs one no-op delete, a skipped one leaves a ``done`` record for rolled-back
    work."""
    return bool(getattr(store, "joins_transaction", False))


def _joins_ambient(store: IdempotencyStore[Any]) -> bool:
    """Whether ``store`` joins a transaction open now: it declares :func:`joins_transaction` and its own
    database has the ambient :func:`~edax.unit_of_work.current_tx`."""
    return joins_transaction(store) and current_tx(store) is not None


@asynccontextmanager
async def _unit(unit_of_work: Transactor[TxT] | None, observe: Observer) -> AsyncIterator[TxT | None]:
    if unit_of_work is None:
        yield None
    else:
        async with open_transaction(unit_of_work, observe) as tx:
            yield tx


@dataclass(frozen=True)
class Guard(Generic[TxT]):
    """One guarded dispatch: the claim on ``key`` in ``store`` for ``payload_hash``, reported to ``observe``
    under ``name`` (the command type or event name), and the type a cached success replays as."""

    store: IdempotencyStore[TxT]
    name: str
    key: str
    payload_hash: int
    observe: Observer
    replay_as: Any = None

    def replay(self, prior: Record) -> Any:
        """Lost the claim: the kernel answers from the winner's record, its verdict and the error the loser
        fails with, and this raises that error or returns the cached result. The observer hears the kernel's
        verdict verbatim, so no SDK renames it. Never releases, because the key belongs to the winner."""
        verdict, error = _replay_answer(self.key, self.payload_hash, prior)
        replayed = prior.state.result if isinstance(prior.state, Done) else None
        if verdict == "succeeded" and self.replay_as is not None:
            try:
                replayed = msgspec.convert(replayed, self.replay_as, dec_hook=_json.decode_hook)
            except msgspec.ValidationError as e:
                # A cached result that does not read back as the result type is an unreadable record, as the
                # Rust SDK's typed decode finds it: the kernel's terminal codec failure, replayed as cached.
                cause = f"cached result for {self.key!r} does not convert to {self.replay_as!r}: {e}"
                prior = _record_of(_native.unreadable_idempotency_record(prior.payload_hash, cause))
                verdict, error = _replay_answer(self.key, self.payload_hash, prior)
        self.observe.claim(self.name, verdict)
        if error is None:
            return replayed  # the cached value, `None` included
        if isinstance(error, CachedFailure):
            # The kernel's replay text carries the original code (`codec: bad payload`); the record carries
            # it apart too, for a caller that branches on it.
            error.original_code = prior.state.error_code if isinstance(prior.state, Failed) else None
        raise error


async def guarded(
    guard: Guard[TxT], run: Callable[[], Awaitable[Any]], unit_of_work: Transactor[TxT] | None
) -> Any:
    """Run ``run`` once under ``guard``'s atomic claim and return its result, or the cached one, converted
    to ``replay_as`` when that is set.

    The one claim/complete/release orchestration behind :func:`~edax.command.idempotent` (commands) and
    :func:`~edax.router.idempotent_by_id` (events). Without ``unit_of_work`` there are two cases. With no
    transaction open, the guard runs ``claim; run; complete`` and the handler's writes commit on their own.
    Inside the host's :func:`~edax.unit_of_work.transaction`, the store must enlist in the ambient
    :func:`~edax.unit_of_work.current_tx` of its own database, and the host owns the one commit. Any other
    ledger raises :class:`~edax.errors.BadHandle`: it would cache a success before the host commits the work.
    With ``unit_of_work``, the guard owns the transaction and :func:`joins_transaction` picks the ordering. A
    shared ledger runs ``begin; claim; run; complete; commit``: one atomic commit, effectively-once, and a
    terminal failure is not cached because the record would roll back with the work. A ledger of its own
    runs ``claim; begin; run; commit; complete``: two commits, at-least-once within the store's retention,
    ``complete`` last so the worst case is a re-run and not a ``done`` record for work that never landed. Any
    failure up to and including the commit releases the claim. Failure windows: ``docs/sdk/idempotency.md``.

    ``observe`` is the factory's already-guarded observer: a lost claim reports its verdict through
    ``claim``, a won one reports how it ended through ``settled``, both under ``name``, the command type
    or event name the guarded handler serves."""
    if unit_of_work is not None and in_unit_of_work():
        # The Transactor contract says the SDK never nests: a second unit of work would commit the claim
        # independently of the writes it guards. BadHandle, not StoreError: the wiring mistake is fatal, so
        # a dispatcher stops on it and hands the message back instead of redelivering it forever.
        raise BadHandle(
            "idempotency guard was given a unit_of_work while a transaction is already open: "
            "either pass unit_of_work or open the transaction around the dispatch, not both"
        )
    if unit_of_work is None and in_unit_of_work() and not _joins_ambient(guard.store):
        # The ledger would complete before the host commits, and a host rollback would leave a cached
        # success over work that never landed. The flag alone is not proof: a joining store over another
        # database finds no ambient tx of its own and auto-commits.
        raise BadHandle(
            "idempotency guard runs inside a transaction its ledger does not join: the ledger would "
            "record success before the work commits. Pass unit_of_work to the guard instead of opening "
            "the transaction around the dispatch, or keep the ledger in the transaction's database"
        )
    if unit_of_work is not None and joins_transaction(guard.store):
        return await _guarded_in_one_commit(guard, run, unit_of_work)
    return await _guarded_around_the_work(guard, run, unit_of_work)


async def _guarded_in_one_commit(
    guard: Guard[TxT], run: Callable[[], Awaitable[Any]], unit_of_work: Transactor[TxT]
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
        async with open_transaction(unit_of_work, guard.observe) as tx:
            claimed = await guard.store.claim(guard.key, guard.payload_hash, tx=tx)
            if not isinstance(claimed, Won):
                return guard.replay(claimed)
            started = perf_counter_ns()
            result = await run()
            record = Record.done(guard.payload_hash, result)
            await guard.store.complete(guard.key, claimed.handle, record, tx=tx)
        how = "completed"
        return result
    finally:
        if started is not None:
            guard.observe.settled(guard.name, how, perf_counter_ns() - started)


async def _guarded_around_the_work(
    guard: Guard[TxT], run: Callable[[], Awaitable[Any]], unit_of_work: Transactor[TxT] | None
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
                done = Record.done(payload_hash, result)
                # Encoded before the commit: refused after it, the release would let a retry run work
                # that already landed. Refused here, the work rolls back and the codec failure is cached.
                encode_record(done)
        except BaseException as e:
            # The kernel's settle table: a terminal failure is cached with its code; a transient one, or a
            # cancel, releases the claim so a retry or a redelivery can re-run. Shielded in every case: a
            # cancel landing during the settlement would otherwise abandon the claim until its retention
            # frees it.
            how = await asyncio.shield(
                _settle_failure(store, key, handle, payload_hash, errors.code_of(e), str(e))
            )
            raise
        refusal = await _complete_or_release(store, key, handle, done)
        if refusal is not None:
            how = "released"
            raise refusal
        how = "completed"
        return result
    finally:
        guard.observe.settled(guard.name, how, perf_counter_ns() - started)


async def _settle_failure(
    store: IdempotencyStore[Any], key: str, handle: bytes, payload_hash: int, code: str | None, message: str
) -> Settlement:
    """Settle the claim after the handler failed with ``message`` and the taxonomy ``code``: cache the failed
    record the kernel's settle table answers, or release when it answers none or the store would not take the
    record. Never raises; returns how the claim ended: ``cached``,
    ``released``, or ``refused`` when the store would not take the release. The handler's exception is
    the verdict the caller, :func:`~edax.command.retry` and the dispatcher act on, so a compensation the store
    refuses must not replace it.
    Inside the host's transaction that refusal is usually the abort the handler's own failed statement
    caused, and the host's rollback frees the claim; on a ledger of its own the claim stays pending until
    its retention frees it. Either way the claim is settled by something other than this call."""
    try:
        cached = _native.idempotency_settle_failure(payload_hash, message, code)
        if cached is None:
            await store.release(key, handle)
            return "released"
        record = _record_of(cached)
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
        expected = joins_transaction(store) and in_unit_of_work()
        logger.log(
            logging.DEBUG if expected else logging.WARNING,
            "edax idempotency: compensation for %r was refused after a failed handler; "
            "the ambient rollback or the record's retention frees the claim",
            key,
            exc_info=True,
        )
        return "refused"


def _replay_answer(key: str, current: int, prior: Record) -> tuple[ClaimVerdict, BaseException | None]:
    """The kernel's answer to a claimant hashing to ``current`` that lost ``key`` to ``prior``: its verdict,
    and the error to raise, ``None`` for a success. The result plays no part in it, so only the classifying
    fields cross. A stored ``payload_hash`` outside ``0 .. 2**64 - 1`` (a lossy store) cannot cross: the
    terminal :class:`~edax.errors.CodecError`."""
    pending, failed = isinstance(prior.state, Pending), _failure(prior.state)
    answer = _native.idempotency_replay(current, key, prior.payload_hash, pending, *failed)
    verdict = _names.named(answer.outcome, CLAIM_VERDICTS, "claim verdict")
    return verdict, None if answer.error is None else errors.from_wire(answer.error)


async def _complete_or_release(
    store: IdempotencyStore[Any], key: str, handle: bytes, record: Record
) -> BaseException | None:
    """Overwrite the pending claim with its terminal record; if that write fails, release the
    claim instead of leaving it pending for the store's whole retention. Both carry the claim's handle,
    so a fenced store deletes nothing when the write did land and only its reply was lost. Returns
    ``None`` once the record is written. When ``complete`` raises and the release lands, returns that
    error for the caller to raise or log, and the ledger holds no record. A release that fails too raises."""
    try:
        await store.complete(key, handle, record)
    except BaseException as e:  # noqa: BLE001 - the record failed or was cancelled, not the handler; returned
        # Shielded for the same reason as the cancellation branch in `_guarded_around_the_work`:
        # a cancel landing mid-release leaves the claim pending for the store's whole retention.
        await asyncio.shield(store.release(key, handle))
        return e
    return None


def encode_record(record: Record) -> str:
    """Serialize a :class:`Record` to the kernel's idempotency-record JSON. Public because it is a wire
    contract: every JSON-persisting :class:`IdempotencyStore` backend and every SDK read and write this exact
    shape, so one key stays readable across SDKs."""
    state = record.state
    result_json = _json.encode(state.result if isinstance(state, Done) else None)
    # The kernel's `codec:` refusal, or a hash outside u64, is a terminal CodecError.
    return _native.encode_idempotency_record(
        record.payload_hash, isinstance(state, Pending), result_json, *_failure(state)
    )


def _failure(state: RecordState) -> tuple[str | None, str | None]:
    """The flat ``error`` and ``error_code`` the ABI takes for ``state``: both ``None`` unless it failed."""
    match state:
        case Failed(error=error, error_code=code):
            return error, code
        case Pending() | Done():
            return None, None


def decode_record(raw: str) -> Record:
    """Parse the kernel's idempotency-record JSON. The kernel applies the reading order, so a
    record written by an older writer (no ``pending`` field) reads as completed, and a completed
    record with a ``null`` result reads as a success whose value is ``None``. A failure stored without a
    code the kernel knows reads with ``error_code=None``. An unreadable record raises the terminal
    :class:`~edax.errors.CodecError`; a store reads the record that holds a key with :func:`decode_prior`."""
    # An unreadable record is the kernel's terminal `codec:` error.
    return _record_of(_native.decode_idempotency_record(raw))


def decode_prior(raw: str, claimant_hash: int) -> Record:
    """The record a losing claimant reads from the JSON holding its key: :func:`decode_record`, except that
    it never raises. A record no reader can decode is still a cached outcome, so the kernel reads it as a
    terminal ``codec`` failure, and the guard replays it as :class:`~edax.errors.CachedFailure` in every
    SDK: the handler never runs again for the key. The failure carries the stored ``payload_hash`` when
    that field still reads, so a reused key is still a :class:`~edax.errors.PayloadMismatch`, else
    ``claimant_hash``. Every :class:`IdempotencyStore` that persists the kernel's JSON reads with this."""
    # A hash outside u64 cannot cross: the terminal CodecError.
    return _record_of(_native.decode_prior_idempotency_record(raw, claimant_hash))


def decode_claim(claimed: _native.ClaimResult, payload_hash: int) -> Claimed:
    """What a native ledger's claim found, as the SDK reads it: :class:`Won` with the handle that settles the
    claim, or the prior record read with :func:`decode_prior`, so a record no reader can decode is replayed as
    cached, never re-run. The memory and the Redis ledger both answer with the kernel's one claim result."""
    if claimed.won:
        return Won(_names.given(claimed.handle, "won claim's handle"))
    return decode_prior(_names.given(claimed.prior, "prior record"), payload_hash)


#: The states a kernel-decoded record is in, as ``_native.IdempotencyRecord.state`` names them.
_RecordState = Literal["pending", "failed", "done"]
_RECORD_STATES: tuple[_RecordState, ...] = get_args(_RecordState)
_names.check(_RECORD_STATES, _native.idempotency_states(), "idempotency record states")
_names.check(CLAIM_VERDICTS, _native.idempotency_outcomes(), "claim verdicts")


def _record_of(decoded: _native.IdempotencyRecord) -> Record:
    """A kernel-decoded record as the SDK's :class:`Record`."""
    state = _names.named(decoded.state, _RECORD_STATES, "idempotency record state")
    match state:
        case "pending":
            return Record.pending(decoded.payload_hash)
        case "failed":
            return Record.failed(
                decoded.payload_hash, _names.given(decoded.error, "failure"), decoded.error_code
            )
        case "done":
            # The kernel wrote `result_json` from a value it parsed, so it is JSON.
            return Record.done(
                decoded.payload_hash, _json.decode(_names.given(decoded.result_json, "result"))
            )
        case _:
            _names.unreachable(state)
