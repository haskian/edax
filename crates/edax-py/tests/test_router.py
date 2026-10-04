"""EventRouter middleware chain and the consume-path ``idempotent_by_id`` wrapper.

The wrapper reuses the command bus's claim orchestration; what these tests pin is the event-side
semantics that differ from a command (a completed duplicate is skipped, not replayed) and that
the chain composes the wrapper once for every handler the router calls.
"""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import edax
import pytest
from conftest import build_message, delivered
from edax._async import maybe_await
from edax.backend.memory import MemoryBroker, MemoryIdempotencyStore
from edax.command import Claimed, Record, Won
from edax.router import EventHandler, EventMiddleware, idempotent_by_id
from edax.store import Tx, current_tx, transaction

DEPOSITED = (
    b'{"account_id":"acct-1","amount_cents":4200,"currency":"USD","occurred_at":"2024-05-01T12:00:00Z"}'
)


def deposited(data: bytes = DEPOSITED) -> edax.EventMessage:
    """One committed event; every case redelivers this exact message (or one sharing its id)."""
    return build_message(
        "acct-1", "Deposited", 3, data=data, metadata={"trace": "abc"}, recorded_at=1_714_564_800_000_000_000
    )


def tracing(name: str, trace: list[str]) -> EventMiddleware:
    """A middleware that records entering and leaving, so nesting order is observable."""

    def mw(next_: EventHandler) -> EventHandler:
        async def handler(env: edax.EventMessage) -> None:
            trace.append(f">{name}")
            await maybe_await(next_(env))
            trace.append(f"<{name}")

        return handler

    return mw


# =========================================================================
# EventRouter(*middleware): the chain is baked around every handler the router calls
# =========================================================================


@pytest.mark.parametrize(
    "branch, event_name, fallback, want",
    [
        ("first_middleware_is_outermost", "Deposited", True, [">a", ">b", "handler", "<b", "<a"]),
        ("fallback_runs_through_the_chain", "Closed", True, [">a", ">b", "fallback", "<b", "<a"]),
        ("ignored_event_runs_no_middleware", "Closed", False, []),
    ],
)
async def test_router_chain_branches(branch: str, event_name: str, fallback: bool, want: list[str]) -> None:
    trace: list[str] = []

    async def on_unhandled(env: edax.EventMessage) -> None:
        trace.append("fallback")

    router = edax.EventRouter(
        tracing("a", trace), tracing("b", trace), on_unhandled=on_unhandled if fallback else None
    )

    @router.on("Deposited")
    def handler(env: edax.EventMessage) -> None:  # sync handler: the chain still awaits it
        trace.append("handler")

    assert router.on("Other")(handler) is handler  # the decorator hands back the bare function
    await router.dispatch(build_message("acct-1", event_name, 3, data=DEPOSITED))
    assert trace == want


# =========================================================================
# idempotent_by_id: one branch per row of the event-side semantics table
# =========================================================================


class CountingStore(MemoryIdempotencyStore):
    def __init__(self) -> None:
        super().__init__()
        self.claims = 0
        self.releases = 0

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        self.claims += 1
        return await super().claim(key, payload_hash, tx)

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        self.releases += 1
        await super().release(key, handle, tx)


@dataclass
class IdCase:
    errors: list[BaseException | None]  # per handler run; None = return normally
    raises: list[type[BaseException] | None]  # per delivery
    calls: int  # handler runs after two deliveries
    claims: int
    releases: int
    held: bool = False  # another worker holds the claim before the first delivery
    redelivered: bytes = DEPOSITED  # the second delivery's body
    sync: bool = False  # the wrapped handler is a plain def, not a coroutine function


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            IdCase([None], [None, None], 1, 2, 0),
            id="first_delivery_runs_and_a_completed_duplicate_is_skipped",
        ),
        pytest.param(
            IdCase([], [edax.InProgress, edax.InProgress], 0, 2, 0, held=True),
            id="claim_held_by_another_worker_raises_in_progress",
        ),
        pytest.param(
            IdCase([RuntimeError("blip"), None], [RuntimeError, None], 2, 2, 1),
            id="transient_failure_releases_the_claim_so_the_redelivery_reruns",
        ),
        pytest.param(
            IdCase([edax.CodecError("bad bytes")], [edax.CodecError, edax.CachedFailure], 1, 2, 0),
            id="terminal_failure_is_cached_and_the_redelivery_replays_it",
        ),
        pytest.param(
            IdCase(
                [None],
                [None, edax.PayloadMismatch],
                1,
                2,
                0,
                redelivered=DEPOSITED.replace(b"4200", b"9900"),
            ),
            id="same_id_with_a_different_body_raises_payload_mismatch",
        ),
        pytest.param(
            IdCase([None], [None, None], 1, 2, 0, sync=True),
            id="sync_handler_runs_under_the_claim_and_its_duplicate_is_skipped",
        ),
        pytest.param(
            IdCase([edax.CodecError("bad bytes")], [edax.CodecError, edax.CachedFailure], 1, 2, 0, sync=True),
            id="sync_handler_raising_is_caught_by_the_guard_not_at_call_time",
        ),
    ],
)
async def test_idempotent_by_id_branches(case: IdCase) -> None:
    store = CountingStore()
    calls = 0

    def run(env: edax.EventMessage) -> None:
        nonlocal calls
        error = case.errors[calls]
        calls += 1
        if error is not None:
            raise error

    async def async_handler(env: edax.EventMessage) -> None:
        run(env)

    # A sync handler raises while the middleware is *building* the awaitable, so the guard has to
    # call it inside its own try or the claim is left pending and the failure never cached.
    guarded = idempotent_by_id(store)(run if case.sync else async_handler)
    if case.held:
        assert isinstance(await store.claim(deposited().id, edax.payload_hash(DEPOSITED)), Won)
        store.claims = 0

    for env, raises in zip([deposited(), deposited(case.redelivered)], case.raises, strict=True):
        if raises is None:
            assert await maybe_await(guarded(env)) is None
        else:
            with pytest.raises(raises):
                await maybe_await(guarded(env))
    assert calls == case.calls
    assert (store.claims, store.releases) == (case.claims, case.releases)


# =========================================================================
# the claim joins the ambient transaction: it commits with the handler's writes, or rolls back with them
# =========================================================================


@dataclass
class TxIdempotencyStore:
    """An ``IdempotencyStore`` that is its own ``Transactor``: writes made under a tx stage in
    that tx and land only on commit, so a rollback drops the claim like a database would."""

    # `None` is a released key, which `release` writes straight into `records` when no tx is open.
    records: dict[str, Record | None] = field(default_factory=dict)
    staged: dict[str, dict[str, Record | None]] = field(default_factory=dict)  # tx -> key -> record

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[str]:
        tx = f"tx-{len(self.staged) + 1}"
        self.staged[tx] = {}
        try:
            yield tx
        except BaseException:
            del self.staged[tx]  # rollback
            raise
        else:
            for key, record in self.staged.pop(tx).items():  # commit
                if record is None:
                    self.records.pop(key, None)
                else:
                    self.records[key] = record

    def _view(self, tx: Tx | None) -> dict[str, Record | None]:
        tx = tx if tx is not None else current_tx(self)
        return self.staged[tx] if tx is not None else self.records

    async def claim(self, key: str, payload_hash: int, tx: Tx | None = None) -> Claimed:
        view = self._view(tx)
        prior = view.get(key) if key in view else self.records.get(key)
        if prior is not None:
            return prior
        view[key] = Record(payload_hash, pending=True)
        return Won()

    async def complete(self, key: str, handle: bytes, record: Record, tx: Tx | None = None) -> None:
        self._view(tx)[key] = record

    async def release(self, key: str, handle: bytes, tx: Tx | None = None) -> None:
        self._view(tx)[key] = None

    async def forget(self, key: str, tx: Tx | None = None) -> None:
        self._view(tx)[key] = None


@pytest.mark.parametrize(
    "branch, body_raises, calls_after_redelivery",
    [
        ("commit_keeps_the_claim_so_the_redelivery_is_skipped", False, 1),
        ("rollback_drops_the_claim_so_the_redelivery_reruns", True, 2),
    ],
)
async def test_claim_enlists_in_the_ambient_tx(
    branch: str, body_raises: bool, calls_after_redelivery: int
) -> None:
    store = TxIdempotencyStore()
    calls = 0

    async def handler(env: edax.EventMessage) -> None:
        nonlocal calls
        calls += 1

    guarded = idempotent_by_id(store)(handler)
    env = deposited()
    try:
        async with transaction(store):
            await maybe_await(guarded(env))  # no tx threaded: the store reads current_tx(itself)
            assert env.id not in store.records  # staged, not landed
            if body_raises:
                raise RuntimeError("a later write in the same unit of work failed")
    except RuntimeError:
        assert body_raises
    assert (env.id in store.records) is not body_raises
    await maybe_await(guarded(env))  # redelivery outside any tx
    assert calls == calls_after_redelivery


# =========================================================================
# end to end: a redelivered event is skipped and acked, not nacked forever
# =========================================================================


def _record(label: str, log: list[str], inner: Callable[[], Any]) -> Callable[[], Any]:
    """Log a settlement and still perform it, so the broker sees the delivery settled."""

    async def settle() -> None:
        log.append(label)
        await maybe_await(inner())

    return settle


async def test_redelivery_through_the_dispatcher_is_skipped_and_acked() -> None:
    broker = MemoryBroker()
    env = deposited()
    await broker.publish(env)
    await broker.publish(env)  # the broker redelivers after a lost ack
    calls = 0
    settled: list[str] = []

    async def handler(env: edax.EventMessage) -> None:
        nonlocal calls
        calls += 1

    router = edax.EventRouter(idempotent_by_id(MemoryIdempotencyStore()))
    router.register("Deposited", handler)
    dispatcher = edax.KeyOrderedDispatcher(workers=1, handler=router.dispatch)
    dispatcher.start()
    for _ in range(2):
        delivery = delivered(await broker.poll())
        # Wrap, do not replace: the broker still has to be told, or its unsettled set never clears
        # and `empty()` below would be asserting nothing.
        delivery.on_ack, delivery.on_nack = (
            _record("ack", settled, delivery.on_ack),
            _record("nack", settled, delivery.on_nack),
        )
        await dispatcher.submit(delivery)
        await asyncio.wait_for(dispatcher._queues[0].join(), timeout=5)  # settled before the next poll
    await dispatcher.stop()

    assert calls == 1
    assert settled == ["ack", "ack"]  # the duplicate is acked, so the broker drops it
    assert broker.empty()


async def test_a_handler_runs_under_the_lineage_of_its_event() -> None:
    """What a handler saves continues the flow of the event it handles: the ambient lineage is the event's
    child, inside the middleware too, and nothing of it outlives the dispatch."""
    seen: list[edax.Lineage | None] = []

    def mw(next_: EventHandler) -> EventHandler:
        async def handler(msg: edax.EventMessage) -> None:
            seen.append(edax.current_lineage())
            await maybe_await(next_(msg))

        return handler

    router = edax.EventRouter(mw)

    async def on_deposited(msg: edax.EventMessage) -> None:
        seen.append(edax.current_lineage())

    router.register("Deposited", on_deposited)
    event = deposited()
    await router.dispatch(event)
    child = edax.Lineage(event.correlation_id, event.id)
    assert seen == [child, child]
    assert edax.current_lineage() is None
