"""The broker contract: :class:`~edax.broker.Producer` and :class:`~edax.broker.Consumer` together."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from ...broker import Consumer
from ...delivery import Delivery
from ...message import CommandMessage, EventMessage
from .._concurrency import POLL_S, QUIET_S
from ..messages import command, event_of, message


class BrokerRig(Protocol):
    """What :class:`BrokerContract` drives: one broker, a producer into it, and one consumer per consumer
    group. Asking for the same group twice returns the same consumer. The producer takes events and
    commands, and the consumer reads both."""

    async def publish(self, msg: EventMessage | CommandMessage) -> None:
        """Publish ``msg`` and return once the broker holds it."""
        ...

    async def consumer(self, group: str) -> Consumer[EventMessage | CommandMessage]:
        """The consumer of consumer group ``group``; a group asked for the first time reads from the start."""
        ...

    async def commit(self, group: str) -> None:
        """Make what ``group`` settled durable now: a commit. A no-op for a broker that commits on ack."""
        ...

    async def crash(self, group: str) -> Consumer[EventMessage | CommandMessage]:
        """Kill ``group``'s consumer holding whatever it holds, with no commit, and return the consumer that
        replaces it. The replacement resumes from the group's last commit."""
        ...

    def drained(self, group: str) -> bool:
        """Whether ``group`` has committed past every message published."""
        ...


async def _poll(consumer: Consumer[EventMessage | CommandMessage], n: int) -> list[Delivery[Any]]:
    """The next ``n`` deliveries. Bounded: a broker that lost one fails the clause instead of hanging it."""
    out: list[Delivery[Any]] = []
    for _ in range(n):
        delivery = await asyncio.wait_for(consumer.poll(), POLL_S)
        assert delivery is not None, "the stream ended early"
        assert isinstance(delivery, Delivery), f"a published message came back undecodable: {delivery!r}"
        out.append(delivery)
    return out


async def _quiet(consumer: Consumer[EventMessage | CommandMessage]) -> None:
    """Assert nothing more is delivered now."""
    try:
        extra = await asyncio.wait_for(consumer.poll(), QUIET_S)
    except asyncio.TimeoutError:
        return
    raise AssertionError(f"delivered {extra!r} after everything was settled")


class BrokerContract:
    """The broker seam, :class:`~edax.broker.Producer` and :class:`~edax.broker.Consumer` together.

    Supply a ``broker_rig`` fixture returning a :class:`BrokerRig` over a fresh topic. The clauses are what
    the dispatcher and the handlers count on from a log-based broker such as Kafka: per-key order, a nack
    that redelivers without reordering the key, a crash that redelivers from the commit and never loses
    or skips a message, deliveries the broker retired (:attr:`~edax.delivery.Delivery.live`) that settle
    nothing, and consumer groups that each read every message."""

    async def test_publish_then_poll_keeps_each_keys_order(self, broker_rig: BrokerRig) -> None:
        """Per-key order, and every field as published: the tenant a pooled consumer keys by, the lineage
        and the context a handler reads."""
        msgs = [message(agg, seq) for seq in (1, 2, 3) for agg in ("a", "b")]
        for msg in msgs:
            await broker_rig.publish(msg)
        consumer = await broker_rig.consumer("g")
        published = {msg.id: msg for msg in msgs}
        seen: dict[str, list[int | None]] = {}
        for d in await _poll(consumer, len(msgs)):
            assert d.message == published[d.message.id], "delivered as published, the tenant included"
            seen.setdefault(event_of(d.message).aggregate_id, []).append(event_of(d.message).sequence)
            await d.ack()
        assert seen == {"a": [1, 2, 3], "b": [1, 2, 3]}
        await broker_rig.commit("g")
        assert broker_rig.drained("g")

    async def test_acks_out_of_order_settle_everything(self, broker_rig: BrokerRig) -> None:
        """The dispatcher acks in worker order, not in fetch order."""
        for msg in [message("a", 1), message("b", 1), message("a", 2)]:
            await broker_rig.publish(msg)
        consumer = await broker_rig.consumer("g")
        deliveries = await _poll(consumer, 3)
        for d in reversed(deliveries[1:]):
            await d.ack()
        await broker_rig.commit("g")
        assert not broker_rig.drained("g"), "committed past the delivery still in flight"
        await deliveries[0].ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")

    async def test_a_nack_redelivers_the_message(self, broker_rig: BrokerRig) -> None:
        await broker_rig.publish(message("a", 1))
        consumer = await broker_rig.consumer("g")
        (first,) = await _poll(consumer, 1)
        await first.nack()
        (again,) = await _poll(consumer, 1)
        assert again.message.id == first.message.id
        # The broker's own redelivery count where it keeps one, else 0: never lower than before.
        assert again.attempt >= first.attempt
        assert again.live()
        await again.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")

    async def test_a_nack_never_reorders_its_key(self, broker_rig: BrokerRig) -> None:
        """a-2 was handed out, even acked, before a-1 failed. After the nack the next delivery of key ``a``
        is a-1, and the a-2 copy handed out before the nack is retired: running it would apply a-2 ahead
        of a-1. a-2 comes back behind a-1, so a handler sees it again: at-least-once."""
        for seq in (1, 2):
            await broker_rig.publish(message("a", seq))
        consumer = await broker_rig.consumer("g")
        first, second = await _poll(consumer, 2)
        await second.ack()
        await first.nack()
        assert not second.live(), "the a-2 copy handed out before the nack is still live"
        again = await _poll(consumer, 2)
        assert [event_of(d.message).sequence for d in again] == [1, 2], "the key came back out of order"
        for d in again:
            assert d.live()
            await d.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")
        await _quiet(consumer)

    async def test_a_crash_redelivers_from_the_commit(self, broker_rig: BrokerRig) -> None:
        """The consumer dies holding two unsettled deliveries. Its replacement gets both again and nothing
        the group committed past, and a late ack from the dead consumer settles nothing the group sees: a
        zombie that finishes its handler after the crash must not commit past the redelivery."""
        for msg in [message("a", 1), message("b", 1), message("a", 2)]:
            await broker_rig.publish(msg)
        consumer = await broker_rig.consumer("g")
        held = await _poll(consumer, 3)
        await held[0].ack()
        await broker_rig.commit("g")
        replacement = await broker_rig.crash("g")
        for d in held[1:]:
            await d.ack()  # late: the dead consumer finished its handler after the crash
        await broker_rig.commit("g")
        assert not broker_rig.drained("g"), "a dead consumer's late ack settled a redelivered message"
        again = await _poll(replacement, 2)
        assert sorted(d.message.id for d in again) == sorted(d.message.id for d in held[1:])
        for d in again:
            await d.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")
        await _quiet(replacement)

    async def test_every_consumer_group_reads_every_message(self, broker_rig: BrokerRig) -> None:
        """A projection and a saga on one topic each get every message and settle on their own."""
        for seq in (1, 2):
            await broker_rig.publish(message("a", seq))
        projection = await broker_rig.consumer("projection")
        for d in await _poll(projection, 2):
            await d.ack()
        await broker_rig.commit("projection")
        assert broker_rig.drained("projection")
        assert not broker_rig.drained("saga"), "one group's acks settled another's"
        saga = await broker_rig.consumer("saga")
        assert [event_of(d.message).sequence for d in await _poll(saga, 2)] == [1, 2]

    async def test_a_command_is_delivered_whole_and_keyed_by_its_id(self, broker_rig: BrokerRig) -> None:
        """A broker carries commands as well as events. A command that names no aggregate is ordered by its
        own id, so two of them never wait on each other, and every field arrives as published, the tenant
        a pooled bus claims inside included."""
        commands = [command(None), command(None)]
        for sent in commands:
            await broker_rig.publish(sent)
        consumer = await broker_rig.consumer("g")
        got = await _poll(consumer, 2)
        assert sorted((d.message for d in got), key=lambda m: m.id) == sorted(commands, key=lambda m: m.id)
        for d in got:
            await d.ack()
        await broker_rig.commit("g")
        assert broker_rig.drained("g")
