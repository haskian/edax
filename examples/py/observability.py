"""Observability with no telemetry library: every `Observer` signal, handler timing, a trace link, and
the pull surface.

    CommandBus(timing, idempotent) ─▶ handler        timing() records (name, outcome, error_type, duration_ns)
                                                     idempotent reports claim(verdict) / settled(how, duration_ns)
    Consumer.run ─▶ KeyOrderedDispatcher ─▶ EventRouter(traced, timing, idempotent_by_id) ─▶ handler
                                                     delivery(outcome, attempt, duration_ns); settlement_failed;
                                                     the loop itself reports undecodable(message_id, error)
    transaction(store, observer=) ─▶ commit | rollback   reports transaction(outcome, duration_ns)
    Relay.drain ─▶ publish                           reports drain(published, duration_ns, oldest_timestamp); keeps `last`;
                                                     a drain that raises reports drain_failed(error) first
    dispatcher.stats() / idle()                      the gauges a health check reads on demand

Two seams, split by where a number is measurable. Handler latency and traces live in the middleware
that wraps the handler, because only there can a span be opened before the handler runs. Everything
that is known only after the fact — how a claim ended, how a delivery settled, what a drain did — is
reported to an `Observer`. Both are plain callbacks with plain values; the host decides what to
export. `Counting` below is the whole adapter for this run: it labels by outcome, verdict and
`error_type` only, never by an id, which is the rule any Prometheus or OpenTelemetry adapter must
follow too (`docs/sdk/observability.md`).

Run:  maturin develop -m crates/edax-py/Cargo.toml  &&  python examples/py/observability.py
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, ClassVar

import edax
from edax import Delivery, EventRouter, KeyOrderedDispatcher, Observer, Relay, timing, transaction
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore
from edax.broker import UNDECODABLE, UndecodableDelivery, _ConsumerLoop
from edax.command import CommandBus, idempotent
from edax.observe import error_type
from edax.router import EventHandler, idempotent_by_id

logger = logging.getLogger("example.observability")

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"  # what the producer saved


class Counting(Observer):
    """The whole metrics adapter for this run: every signal folded into one counter table, keyed by
    the outcome strings and `error_type` only. The ids go to the log line, never into a key."""

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.by_handler: Counter[str] = Counter()  # the same signals, keyed by the handler they came from
        self.process_ns: list[int] = []  # the `messaging.process.duration` samples

    def claim(self, name: str, verdict: str) -> None:
        self.counts[f"claim.{verdict}"] += 1
        self.by_handler[f"{name}.claim.{verdict}"] += 1

    def settled(self, name: str, how: str, duration_ns: int) -> None:
        self.counts[f"settled.{how}"] += 1
        self.by_handler[f"{name}.settled.{how}"] += 1

    def delivery(
        self, name: str, outcome: str, attempt: int, duration_ns: int, error_type: str | None
    ) -> None:
        self.counts[f"delivery.{outcome}" if error_type is None else f"delivery.{outcome}.{error_type}"] += 1
        self.by_handler[f"{name}.delivery.{outcome}"] += 1
        self.process_ns.append(duration_ns)

    def settlement_failed(self, name: str, event_id: str, error: BaseException) -> None:
        self.counts[f"settlement_failed.{error_type(error)}"] += 1
        self.by_handler[f"{name}.settlement_failed"] += 1
        logger.warning("ack/nack of %s failed: %s", event_id, error)  # the id belongs here, not on a label

    def transaction(self, outcome: str, duration_ns: int) -> None:
        self.counts[f"transaction.{outcome}"] += 1

    def drain(self, published: int, duration_ns: int, oldest_timestamp: int | None) -> None:
        self.counts["drain.batches"] += 1
        self.counts["drain.published"] += published

    def drain_failed(self, error: BaseException) -> None:
        self.counts[f"drain_failed.{error_type(error)}"] += 1

    def undecodable(self, message_id: str, error: BaseException) -> None:
        self.counts[f"undecodable.{error_type(error)}"] += 1
        logger.warning("poison message %s: %s", message_id, error)

    def named(self, prefix: str) -> dict[str, int]:
        return {key: n for key, n in self.counts.items() if key.startswith(prefix)}


class Latency:
    """A `timing` recorder: calls and total time per handler, split by outcome and `error_type`."""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self.total_ns: Counter[str] = Counter()

    def record(self, name: str, outcome: str, error_type: str | None, duration_ns: int) -> None:
        label = f"{name}.{outcome}" if error_type is None else f"{name}.{outcome}.{error_type}"
        self.calls[label] += 1
        self.total_ns[label] += duration_ns


class Spans:
    """A stdlib stand-in for a tracer: one row per event handled, linked to the `traceparent` the
    producer saved in `metadata`. A real tracer opens its span here, before the handler runs, so the
    handler's own instrumented calls nest under it; the `Observer` fires too late for that."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []  # (parent, event name, outcome)

    def traced(self, next_: EventHandler) -> EventHandler:
        async def handler(env: edax.EventEnvelope) -> None:
            parent = env.metadata.get("traceparent", "-")
            try:
                result = next_(env)  # sync or async, as any EventHandler may be
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                self.rows.append((parent, env.event.name, error_type(e)))
                raise
            self.rows.append((parent, env.event.name, "ok"))

        return handler


class Replaying(_ConsumerLoop):
    """A `Consumer` over a fixed list: what a broker adapter's `poll` yields, then end of stream."""

    def __init__(self, items: list[Delivery]) -> None:
        self._items: list[Delivery | None] = [*items, None]

    async def poll(self) -> Delivery | None:
        return self._items.pop(0)


@dataclass
class Deposit:
    account_id: str
    amount: int
    request_id: str  # the idempotency key; part of the hashed identity
    type: ClassVar[str] = "Deposit"


def request_id_of(cmd: Any) -> str:
    return cmd.request_id


def envelope(
    env_id: str,
    aggregate_id: str,
    name: str,
    *,
    timestamp: int = 0,
    metadata: dict[str, str] | None = None,
) -> edax.EventEnvelope:
    return edax.EventEnvelope(
        id=env_id,
        aggregate_id=aggregate_id,
        event=edax.Event(name, 1, b"{}"),
        namespace={},
        metadata=metadata or {},
        timestamp=timestamp,
        sequence=1,
    )


async def commands(observer: Counting, latency: Latency) -> None:
    """`timing` outermost so every dispatch is timed, the replayed ones included; the guard inside it
    reports every claim verdict and settlement. Command latency is measured here and nowhere else."""

    async def deposit(cmd: Deposit) -> int:
        if cmd.amount < 0:
            raise edax.CodecError("a negative deposit")  # terminal: the guard caches the failure
        if cmd.amount == 0:
            raise edax.StoreError("the ledger blinked")  # transient: the guard releases the claim
        return cmd.amount

    bus = CommandBus(
        timing(latency.record, lambda cmd: cmd.type),
        idempotent(MemoryIdempotencyStore(), key=request_id_of, observer=observer),
    )
    bus.register("Deposit", deposit)

    assert await bus.dispatch(Deposit("acct-1", 100, "req-1")) == 100  # won the claim: settled completed
    assert await bus.dispatch(Deposit("acct-1", 100, "req-1")) == 100  # lost it: the kernel says `succeeded`
    with contextlib.suppress(edax.PayloadMismatch):
        await bus.dispatch(Deposit("acct-1", 999, "req-1"))  # same key, other body: `payload_mismatch`
    with contextlib.suppress(edax.CodecError):
        await bus.dispatch(Deposit("acct-1", -1, "req-2"))  # terminal failure: settled cached
    with contextlib.suppress(edax.CachedFailure):
        await bus.dispatch(Deposit("acct-1", -1, "req-2"))  # replayed failure: the kernel says `failed`
    with contextlib.suppress(edax.StoreError):
        await bus.dispatch(Deposit("acct-1", 0, "req-3"))  # transient failure: settled released

    claims = observer.named("claim.")
    assert claims == {"claim.succeeded": 1, "claim.payload_mismatch": 1, "claim.failed": 1}, claims
    settled = observer.named("settled.")
    assert settled == {"settled.completed": 1, "settled.cached": 1, "settled.released": 1}, settled
    assert latency.calls["Deposit.ok"] == 2, latency.calls
    assert latency.calls["Deposit.error.codec"] == 1, latency.calls  # the kernel's code, not the class
    assert sum(latency.calls.values()) == 6, latency.calls
    print("commands:  3 claims lost, 3 settled (completed, cached, released); 6 dispatches timed")


async def consume(observer: Counting, latency: Latency, spans: Spans) -> None:
    """One delivery per settlement arm: ack, dead, nack, in_progress, unsettled; plus a poison message
    the consumer loop dead-letters itself, so no handler ever sees it. The gauges are read after the
    consumer queued everything and before the workers start, so the backlog is visible, and again
    after the drain."""
    ledger = MemoryIdempotencyStore()
    settled: list[str] = []
    dead: list[str] = []

    async def handle(env: edax.EventEnvelope) -> None:
        if env.event.name == "Poison":
            raise edax.CodecError("will never decode")  # terminal: dead-letters on the first attempt
        if env.event.name == "Flaky":
            raise edax.StoreError("the database blinked")  # transient: one backoff step, then nack

    async def sink(delivery: Delivery, error: BaseException) -> None:
        dead.append(f"{delivery.envelope.event.name} {delivery.envelope.id}:{error_type(error)}")

    router = EventRouter(
        spans.traced,  # outermost: the span must parent the guard's and the handler's work
        timing(latency.record, lambda env: env.event.name),
        idempotent_by_id(ledger, observer=observer),
    )
    for name in ("Deposited", "Poison", "Flaky", "Contended"):
        router.register(name, handle)

    def settle(how: str, env_id: str) -> Callable[[], None]:
        return lambda: settled.append(f"{how} {env_id}")

    def deliver(env_id: str, name: str, **metadata: str) -> Delivery:
        env = envelope(env_id, f"acct-{env_id}", name, metadata=metadata)
        return Delivery(env, settle("ack", env_id), settle("nack", env_id))

    def refuse() -> None:
        raise edax.StoreError("the commit was lost")  # an ack that raises: unsettled, the broker redelivers

    await ledger.claim("e4", edax.payload_hash(b"{}"))  # another worker still holds e4's claim
    items = [
        deliver("e1", "Deposited", traceparent=TRACEPARENT),
        deliver("e2", "Poison"),
        deliver("e3", "Flaky"),
        deliver("e4", "Contended"),
        replace(deliver("e5", "Deposited"), on_ack=refuse),
        UndecodableDelivery(
            b"not an envelope",
            "t/0/7",
            settle("ack", "t/0/7"),
            settle("nack", "t/0/7"),
            edax.CodecError("no envelope"),
        ),
    ]
    dispatcher = KeyOrderedDispatcher(workers=2, handler=router.dispatch, on_dead=sink, observer=observer)
    await Replaying(items).run(dispatcher)  # queues the five events; dead-letters the poison itself

    before = dispatcher.stats()
    print(f"queued:    depth={before.queue_depth} in_flight={before.in_flight} idle={dispatcher.idle()}")
    assert sum(before.queue_depth) == 5 and not dispatcher.idle(), before

    dispatcher.start()
    await dispatcher.stop()  # finishes the queued items, then returns

    after = dispatcher.stats()
    print(
        f"drained:   depth={after.queue_depth} in_flight={after.in_flight} attempts_tracked={after.attempts_tracked} "
        f"contended_tracked={after.contended_tracked} idle={dispatcher.idle()}"
    )
    assert dispatcher.idle() and after.in_flight == 0, after
    assert (after.attempts_tracked, after.contended_tracked) == (1, 1), (
        "nacked ids stay tracked until they come back"
    )
    assert sorted(settled) == ["ack e1", "ack e2", "ack t/0/7", "nack e3", "nack e4"], settled
    assert sorted(dead) == ["Poison e2:codec", f"{UNDECODABLE} t/0/7:codec"], dead
    deliveries = observer.named("delivery.")
    assert deliveries == {  # the error type rides along, so a dead by terminal error is told from a ceiling
        "delivery.ack": 1,
        "delivery.dead.codec": 1,
        "delivery.nack.store": 1,
        "delivery.in_progress.in_progress": 1,
        "delivery.unsettled": 1,
    }, deliveries
    assert observer.counts["claim.in_progress"] == 1, observer.counts
    assert observer.counts["settlement_failed.store"] == 1, observer.counts
    assert observer.by_handler["Poison.delivery.dead"] == 1, observer.by_handler
    assert observer.by_handler["Contended.claim.in_progress"] == 1, observer.by_handler
    assert observer.counts["undecodable.codec"] == 1, observer.counts
    assert (TRACEPARENT, "Deposited", "ok") in spans.rows, spans.rows  # the consume span links the producer's
    assert ("-", "Contended", "in_progress") in spans.rows, spans.rows
    slowest_ms = max(observer.process_ns) / 1e6
    assert slowest_ms < 50, "the reported duration is the handler alone, never the 100 ms backoff wait"
    print(
        f"delivery:  ack=1 dead=1 nack=1 in_progress=1 unsettled=1; 1 poison dead-lettered by the loop; "
        f"slowest handler {slowest_ms:.3f} ms, backoff excluded"
    )


async def outbox(observer: Counting) -> None:
    """A unit of work reports how it ended. A drain reports what it published and the oldest row's
    timestamp: against the reader's clock, the outbox lag signal. A drain that raises reports that
    too, so a stuck relay is a counter and not only a log line at the caller."""
    store = MemoryEventStore()
    async with transaction(store, observer=observer) as tx:
        await store.append(
            tx, "acct-1", 0, [envelope("e9", "acct-1", "Deposited", timestamp=edax.current_timestamp())]
        )
    with contextlib.suppress(RuntimeError):
        async with transaction(store, observer=observer):
            raise RuntimeError("abandoned")  # rolled back: reported as such

    def broker_down(env: edax.EventEnvelope) -> None:
        raise edax.StoreError("broker down")

    with contextlib.suppress(edax.StoreError):
        await Relay(store, publish=broker_down, observer=observer).drain()  # nothing marked: the row waits

    published: list[str] = []
    relay = Relay(store, publish=lambda env: published.append(env.id), observer=observer)
    await relay.drain()

    last = relay.last
    assert last is not None and last.published == 1 and published == ["e9"], last
    assert last.oldest_timestamp is not None, last
    lag_ms = (edax.current_timestamp() - last.oldest_timestamp) / 1e6  # the same kernel clock stamped it
    assert lag_ms >= 0, lag_ms
    assert observer.named("transaction.") == {"transaction.committed": 1, "transaction.rolled_back": 1}
    assert observer.counts["drain_failed.store"] == 1, observer.counts
    assert observer.counts["drain.published"] == 1, observer.counts
    print(
        f"relay:     1 drain failed (broker down), then published={last.published} "
        f"lag={lag_ms:.3f} ms in {last.duration_ns / 1e6:.3f} ms; transactions committed=1 rolled_back=1"
    )


async def main() -> None:
    observer = Counting()
    latency = Latency()
    spans = Spans()
    await commands(observer, latency)
    await consume(observer, latency, spans)
    await outbox(observer)

    print("\ncounters:")
    for key, n in sorted(observer.counts.items()):
        print(f"  {key:<30} {n}")
    print("by handler:")
    for key, n in sorted(observer.by_handler.items()):
        print(f"  {key:<30} {n}")
    print("handler latency:")
    for label, n in sorted(latency.calls.items()):
        print(f"  {label:<30} {n} call(s), {latency.total_ns[label] / n / 1e6:.3f} ms mean")
    print("spans:")
    for parent, name, outcome in sorted(spans.rows):
        print(f"  {name:<10} {outcome:<12} parent={parent}")

    print("\nok: every Observer signal counted; handler latency timed; spans linked; gauges read")


if __name__ == "__main__":
    asyncio.run(main())
