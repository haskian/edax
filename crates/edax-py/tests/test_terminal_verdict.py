"""A terminal failure is never retried and never redelivered, on any path; a transient one always is.

One table: every way a handler can fail (a kernel refusal, a value the SDK cannot encode, a host's own
rejection, an edax error) crossed with every consumer that acts on the verdict (``retry``, the
idempotency guard, the dispatcher, and the relay that sends a saga's committed command). Before the error taxonomy had one
owner, several of these sources reached a consumer as a bare ``TypeError`` or an untagged
``RuntimeError``: transient, so ``retry`` burned its attempts, the guard released the claim and the
dispatcher nacked forever.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar

import edax
import pytest
from conftest import NO_WAIT, build_message
from edax.backend.memory import MemoryIdempotencyStore, MemorySagaStore, MemoryTransactor
from edax.filter import eq, matches


class DeclaredTerminal(Exception):
    """A host's own exception that declares itself terminal, with a code the kernel does not know."""

    terminal = True
    code = "order_already_shipped"


def _kernel_refusal() -> None:
    edax.check_sequence_batch(0, [2])  # a gap: the kernel's `codec:` error


def _unencodable_filter_value() -> None:
    matches(eq("placed_at", object()), {"placed_at": "2024-05-01T12:00:00Z"})


def _argument_the_binding_refuses() -> None:
    # A lone surrogate escape: the canonical form refuses it.
    with edax.errors.mapped():
        edax.payload_hash_json(b'{"note":"\\ud800"}')


def _raise(error: BaseException) -> Callable[[], None]:
    def fail() -> None:
        raise error

    return fail


class MalformedOrder(edax.CodecError):
    """A host subclass of a coded error: it inherits the code, and the verdict with it."""


def _claiming(error: edax.EdaxError, terminal: bool) -> edax.EdaxError:
    """An edax error whose instance attribute contradicts its code's verdict. A class cannot restate it, but an
    instance can, so every consumer must decide from the code and ignore the attribute."""
    error.terminal = terminal  # ty: ignore[invalid-attribute-access]
    return error


@dataclass
class Source:
    name: str
    fail: Callable[[], None]
    terminal: bool
    #: The code a terminal failure is cached under.
    code: str | None


SOURCES = [
    Source("kernel_refusal", _kernel_refusal, True, "codec"),
    Source("unencodable_filter_value", _unencodable_filter_value, True, "codec"),
    Source("argument_the_binding_refuses", _argument_the_binding_refuses, True, "codec"),
    Source("host_rejection", _raise(edax.Rejected("insufficient funds")), True, "rejected"),
    Source("host_exception_declaring_terminal", _raise(DeclaredTerminal("shipped")), True, "rejected"),
    Source("payload_mismatch", _raise(edax.PayloadMismatch("req-1")), True, "payload_mismatch"),
    Source("codec_subclass", _raise(MalformedOrder("no line items")), True, "codec"),
    Source(
        "codec_error_claiming_transient", _raise(_claiming(edax.CodecError("retry me"), False)), True, "codec"
    ),
    Source("store_hiccup", _raise(edax.StoreError("connection reset")), False, None),
    Source(
        "store_error_claiming_terminal",
        _raise(_claiming(edax.StoreError("down for good"), True)),
        False,
        None,
    ),
    Source("unclassified_host_error", _raise(ValueError("a host bug")), False, None),
]


@dataclass
class Withdraw:
    account_id: str
    amount_cents: int
    request_id: str
    type: ClassVar[str] = "Withdraw"


WITHDRAW = Withdraw("acct-1", 500, "req-1")


@dataclass
class Outcome:
    #: Handler runs, across the deliveries or dispatches the consumer made.
    runs: int
    #: How the last attempt surfaced: the exception type the caller saw, or a settlement.
    settled: list[str]


async def _through_retry(source: Source) -> Outcome:
    runs = 0

    async def handler(cmd: Withdraw) -> None:
        nonlocal runs
        runs += 1
        source.fail()

    bus = edax.CommandBus(edax.retry(3, pacing=NO_WAIT))
    bus.register("Withdraw", handler)
    with pytest.raises(Exception) as caught:
        await bus.dispatch(WITHDRAW)
    return Outcome(runs, [type(caught.value).__name__])


async def _through_the_guard(source: Source) -> Outcome:
    """Two dispatches of one command: a terminal failure is cached under its code and replays without a
    second run; a transient one is released and runs again."""
    runs = 0

    async def handler(cmd: Withdraw) -> None:
        nonlocal runs
        runs += 1
        source.fail()

    ledger = MemoryIdempotencyStore()
    bus = edax.CommandBus(edax.idempotent(ledger, scope="test", tenancy=edax.Tenancy.SILO))
    bus.register("Withdraw", handler)
    seen: list[str] = []
    for _ in range(2):
        with pytest.raises(Exception) as caught:
            await bus.dispatch(WITHDRAW, idempotency_key=WITHDRAW.request_id)
        seen.append(type(caught.value).__name__)
        if isinstance(caught.value, edax.CachedFailure):
            assert caught.value.original_code == source.code, "the replay keeps the original code"
    return Outcome(runs, seen)


async def _through_the_dispatcher(source: Source, handler_for: Callable[[], Any]) -> Outcome:
    """Up to three deliveries of one event, each nack redelivered at once: a terminal failure dies on the
    first attempt, a transient one is nacked until the ceiling dead-letters it."""
    settled: list[str] = []
    dead: list[int] = []
    handler, runs = handler_for()

    async def on_dead(delivery: edax.Delivery, error: BaseException) -> None:
        dead.append(1)

    dispatcher = edax.KeyOrderedDispatcher(1, handler, max_attempts=3, on_dead=on_dead, pacing=NO_WAIT)
    dispatcher.start()
    env = build_message("acct-1", "Withdrawn", 1)
    for _ in range(3):
        outcome: list[str] = []
        delivery = edax.Delivery(
            env, lambda out=outcome: out.append("ack"), lambda out=outcome: out.append("nack")
        )
        await dispatcher.submit(delivery)
        await dispatcher._queues[0].join()
        settled.append("dead" if dead and outcome == ["ack"] else outcome[0])
        if outcome == ["ack"]:
            break
    await dispatcher.stop()
    return Outcome(runs(), settled)


def _plain_handler(source: Source) -> Callable[[], Any]:
    def build() -> Any:
        count = 0

        async def handler(env: edax.EventMessage) -> None:
            nonlocal count
            count += 1
            source.fail()

        return handler, lambda: count

    return build


class _WithdrawSaga:
    """Every `Withdrawn` event asks for one more withdrawal, so the saga's step is a command."""

    name: ClassVar[str] = "withdraw"

    def instance_id(self, env: edax.EventMessage, /) -> str | None:
        return env.aggregate_id

    def handle(self, state: Any, env: edax.EventMessage, /) -> tuple[Any, list[Any]]:
        return "open", [WITHDRAW]


async def _through_the_saga_relay(source: Source) -> Outcome:
    """The saga commits its command to the outbox, and the relay sends it through a guarded bus. Up to
    three drains: a terminal failure is dead-lettered on the first, and the rows behind it go out; a
    transient one is handed back until the ceiling dead-letters it. The guard caches the terminal failure,
    so it is never re-run."""
    runs = 0

    async def command(cmd: Withdraw) -> None:
        nonlocal runs
        runs += 1
        source.fail()

    db = MemoryTransactor()
    bus = edax.CommandBus(
        edax.idempotent(MemoryIdempotencyStore(db), scope="test", tenancy=edax.Tenancy.SILO)
    )
    bus.register("Withdraw", command)
    store = MemorySagaStore(db)
    runner = edax.SagaRunner(_WithdrawSaga(), store, edax.json_codec({"Withdraw": Withdraw}))
    await runner.on_event(build_message("acct-1", "Withdrawn", 1))
    dead: list[str] = []

    async def on_dead(row: edax.CommandMessage, error: BaseException) -> None:
        dead.append(row.id)

    relay = runner.relay(bus, on_dead=on_dead, max_attempts=3, pacing=NO_WAIT)
    settled: list[str] = []
    for _ in range(3):
        try:
            await relay.drain()
        except Exception:  # noqa: BLE001 - the row was handed back; the source raises the subject
            settled.append("nack")
            continue
        settled.append("dead" if dead else "ack")
        break
    assert await store.unpublished() == [], "the row left the outbox, sent or dead-lettered"
    return Outcome(runs, settled)


CONSUMERS = ["retry", "guard", "dispatcher", "saga"]


async def _run(consumer: str, source: Source) -> Outcome:
    if consumer == "retry":
        return await _through_retry(source)
    if consumer == "guard":
        return await _through_the_guard(source)
    if consumer == "dispatcher":
        return await _through_the_dispatcher(source, _plain_handler(source))
    return await _through_the_saga_relay(source)


def _want(consumer: str, source: Source) -> Outcome:
    failure = type(_failure_of(source)).__name__
    if source.terminal:
        return {
            "retry": Outcome(1, [failure]),
            "guard": Outcome(1, [failure, "CachedFailure"]),
            "dispatcher": Outcome(1, ["dead"]),
            "saga": Outcome(1, ["dead"]),
        }[consumer]
    return {
        "retry": Outcome(3, [failure]),
        "guard": Outcome(2, [failure, failure]),
        "dispatcher": Outcome(3, ["nack", "nack", "dead"]),
        "saga": Outcome(3, ["nack", "nack", "dead"]),
    }[consumer]


def _failure_of(source: Source) -> BaseException:
    try:
        source.fail()
    except Exception as e:  # noqa: BLE001 - whatever the source raises is the subject
        return e
    raise AssertionError(f"{source.name} did not fail")


@pytest.mark.parametrize("consumer", CONSUMERS)
@pytest.mark.parametrize("source", SOURCES, ids=lambda s: s.name)
async def test_a_terminal_failure_is_never_retried_or_redelivered(consumer: str, source: Source) -> None:
    failure = _failure_of(source)
    assert edax.is_terminal(failure) is source.terminal, "the source raises what the row says"
    assert edax.errors.code_of(failure) == source.code
    assert await _run(consumer, source) == _want(consumer, source)


@pytest.mark.parametrize(
    ("parent", "restated"),
    [
        pytest.param(edax.StoreError, True, id="store_subclass_declaring_terminal"),
        pytest.param(edax.CodecError, False, id="codec_subclass_declaring_transient"),
    ],
)
def test_a_subclass_cannot_restate_its_codes_verdict(parent: type[edax.EdaxError], restated: bool) -> None:
    """The verdict is the code's: a subclass that restates it is refused when it is defined, so no consumer
    can read one verdict while another reads the code's."""
    with pytest.raises(TypeError, match="restates `terminal`"):
        type("Restated", (parent,), {"terminal": restated})


async def test_a_cancel_is_never_cached() -> None:
    """A cancel is not a verdict: the guard releases, so the next dispatch runs the handler again."""
    runs = 0
    gate = asyncio.Event()

    async def handler(cmd: Withdraw) -> None:
        nonlocal runs
        runs += 1
        if runs == 1:
            await gate.wait()

    ledger = MemoryIdempotencyStore()
    bus = edax.CommandBus(edax.idempotent(ledger, scope="test", tenancy=edax.Tenancy.SILO))
    bus.register("Withdraw", handler)
    task = asyncio.ensure_future(bus.dispatch(WITHDRAW, idempotency_key=WITHDRAW.request_id))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await bus.dispatch(WITHDRAW, idempotency_key=WITHDRAW.request_id)
    assert runs == 2
