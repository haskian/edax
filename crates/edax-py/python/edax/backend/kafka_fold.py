"""Kafka ordered folds using database-authoritative cursors and verified source continuity."""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import Awaitable, Collection, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast
from uuid import uuid4

from .. import _json, _native, errors
from ..errors import BadHandle, CodecError, StoreError, TransactionFailure
from ..fold import FoldEvent, FoldInput, FoldLease, FoldStore, Reducer, SourceProof
from ..ingestion import BatchSettings
from ..pacing import Pacing
from ._kafka import FetchedRecord, check_consumer, check_durations, check_listener

if TYPE_CHECKING:
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]


T = TypeVar("T")


class FoldConsumerClient(Protocol):
    """Only the fetch, assignment and seek operations needed by authoritative checkpoints."""

    async def getmany(
        self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None
    ) -> Mapping[TopicPartition, Sequence[FetchedRecord]]: ...
    def assignment(self) -> Collection[TopicPartition]: ...
    def seek(self, partition: TopicPartition, offset: int, /) -> Any: ...


class FoldSource(Protocol):
    """Host capability backed by supported identity and retained-boundary APIs.

    Verify cluster/topic incarnation and either detect truncation or enforce a no-rollback
    source history. ``end`` is the readable end (last stable offset for read_committed).
    A configured nickname is not evidence. Failure to prove continuity must raise.
    """

    async def verify(self, partition: TopicPartition, /) -> SourceProof: ...


class FoldDecoder(Protocol):
    def __call__(self, record: FetchedRecord, /) -> FoldEvent: ...


@dataclass
class _Assigned:
    generation: int
    owner: str = field(default_factory=lambda: uuid4().hex)
    failure: BaseException | None = None
    unresolved: asyncio.Task[Any] | None = None
    lease: FoldLease | None = None
    attempts: int = 0
    ready_at: float = 0
    seek_needed: bool = True


class KafkaFold:
    """Sequential bounded partition batches with a pure synchronous decoder/reducer.

    Subscribe with ``rebalance_listener`` before starting the caller-owned client. Disable
    automatic commit and use ``auto_offset_reset='none'``. Broker commits are unnecessary;
    every assignment seeks from the database. This runner never skips a poison delta.
    A slow generation is serialized by its store; batches rotate across ready partitions.
    """

    def __init__(
        self,
        client: FoldConsumerClient,
        store: FoldStore,
        source: FoldSource,
        decode: FoldDecoder,
        reducer: Reducer,
        *,
        settings: BatchSettings | None = None,
        lease_ms: int = 30_000,
        operation_timeout_s: float = 10,
        drain_timeout_s: float = 30,
        pacing: Pacing | None = None,
    ) -> None:
        check_consumer(cast(Any, client))
        reset = getattr(client, "_auto_offset_reset", None)
        if reset is not None and reset != "none":
            raise BadHandle("fold consumers require auto_offset_reset='none'")
        check_durations(
            publish_timeout_s=operation_timeout_s,
            commit_timeout_s=operation_timeout_s,
            drain_timeout_s=drain_timeout_s,
        )
        if (
            isinstance(lease_ms, bool)
            or not isinstance(lease_ms, int)
            or lease_ms <= operation_timeout_s * 1000
        ):
            raise BadHandle("fold lease must exceed the operation deadline")
        self.client, self.store, self.source = client, store, source
        self.decode, self.reducer = decode, reducer
        self.settings = settings or BatchSettings()
        self.lease_ms, self.timeout, self.drain_timeout = lease_ms, operation_timeout_s, drain_timeout_s
        self.pacing = pacing or Pacing()
        self._parts: dict[TopicPartition, _Assigned] = {}
        self._generation = 0
        self._stopped = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._started = False
        self._releases: set[asyncio.Task[None]] = set()
        self._operations: set[asyncio.Task[Any]] = set()
        self._fetches: dict[TopicPartition, asyncio.Task[Any]] = {}
        self.retained_records = 0
        self.retained_bytes = 0

    @property
    def partition_failures(self) -> dict[TopicPartition, BaseException]:
        """Stopped partitions require operator repair/reassignment; healthy partitions continue."""
        return {tp: part.failure for tp, part in self._parts.items() if part.failure is not None}

    def rebalance_listener(self) -> ConsumerRebalanceListener:
        from aiokafka import ConsumerRebalanceListener

        runner = self

        class Listener(ConsumerRebalanceListener):  # type: ignore[misc]
            async def on_partitions_revoked(self, revoked: Collection[TopicPartition]) -> None:
                # Retire immediately, before any wait on an unresolved I/O operation.
                retired = [runner._parts.pop(tp) for tp in revoked if tp in runner._parts]
                runner._generation += 1
                tasks = [runner._release(part.lease) for part in retired if part.lease is not None]
                if tasks:
                    await asyncio.wait(tasks, timeout=runner.drain_timeout)

            async def on_partitions_assigned(self, assigned: Collection[TopicPartition]) -> None:
                for tp in assigned:
                    runner._generation += 1
                    runner._parts[tp] = _Assigned(runner._generation)

        return Listener()

    def _release(self, lease: FoldLease) -> asyncio.Task[None]:
        task = asyncio.create_task(self.store.release(lease))
        self._releases.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            self._releases.discard(done)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finished)
        return task

    def _live(self, tp: TopicPartition, assigned: _Assigned) -> bool:
        return (
            not self._stopped.is_set() and self._parts.get(tp) is assigned and tp in self.client.assignment()
        )

    async def _operation(
        self, assigned: _Assigned, call: Awaitable[T], *, fetch: TopicPartition | None = None
    ) -> T:
        task = asyncio.ensure_future(call)
        self._operations.add(task)
        if fetch is not None:
            self._fetches[fetch] = task

        def finished(done: asyncio.Task[Any]) -> None:
            self._operations.discard(done)
            if fetch is not None and self._fetches.get(fetch) is done:
                self._fetches.pop(fetch)
            if assigned.unresolved is done:
                assigned.unresolved = None
            if not done.cancelled():
                error = done.exception()
                if error is not None:
                    _clear_failure_frames(error)

        task.add_done_callback(finished)
        try:
            done, _ = await asyncio.wait({task}, timeout=self.timeout)
            if done:
                return task.result()
            raise StoreError("fold operation exceeded its deadline; outcome requires recovery")
        except BaseException:
            assigned.seek_needed = True
            raise
        finally:
            if not task.done():
                assigned.unresolved = task
                task.cancel()

    async def _proof(
        self, tp: TopicPartition, assigned: _Assigned, lease: FoldLease | None = None
    ) -> SourceProof:
        proof = await self._operation(assigned, self.source.verify(tp))
        cursor = lease.next_offset if lease else proof.beginning
        _native.fold_check_source(self.store.target._contract, _json.encode(proof), cursor)
        return proof

    async def _partition(self, tp: TopicPartition, assigned: _Assigned) -> None:
        def current() -> bool:
            return self._live(tp, assigned) and assigned.unresolved is None

        previous_fetch = self._fetches.get(tp)
        if previous_fetch is not None and not previous_fetch.done():
            return
        if tp.topic != self.store.target.topic:
            raise BadHandle("fold assignment contains another topic")
        proof = await self._proof(tp, assigned)
        if not current():
            return
        if assigned.lease is None:
            lease = await self._operation(
                assigned,
                self.store.acquire(
                    tp.partition, assigned.owner, proof, lease_ms=self.lease_ms, is_current=current
                ),
            )
            assigned.lease = lease
        else:
            # Refresh reloads an uncertain commit and refences an expired current assignment.
            lease = await self._operation(
                assigned,
                self.store.refresh(assigned.lease, proof, lease_ms=self.lease_ms, is_current=current),
            )
            assigned.lease = lease
        if not current():
            self._release(lease)
            return
        await self._proof(tp, assigned, lease)
        if not current():
            return
        # Seeking happens only at a quiescent boundary with no retained previous batch.
        if assigned.seek_needed:
            self.client.seek(tp, lease.next_offset)
            assigned.seek_needed = False
        inputs: list[FoldInput] = []
        size = 0
        opened = asyncio.get_running_loop().time()
        try:
            while current():
                if (
                    inputs
                    and _native.batch_cut(
                        len(inputs),
                        size,
                        int((asyncio.get_running_loop().time() - opened) * 1000),
                        self.settings.json,
                    )
                ) or not _native.batch_admit(
                    self.retained_records,
                    self.retained_bytes,
                    1,
                    _native.batch_reserve(self.settings.json),
                    self.settings.json,
                ):
                    break
                fetched = await self._operation(
                    assigned,
                    self.client.getmany(
                        tp,
                        timeout_ms=min(self.settings.flush_ms, max(1, int(self.timeout * 500))),
                        max_records=1,
                    ),
                    fetch=tp,
                )
                if not current():
                    return
                records = fetched.get(tp, ())
                if any(key != tp and values for key, values in fetched.items()):
                    raise BadHandle("fold client violated the requested partition or fetch limit")
                if not records:
                    break
                if len(records) != 1:
                    raise BadHandle("fold client violated the requested partition or fetch limit")
                record = records[0]
                if record.topic != tp.topic or record.partition != tp.partition:
                    raise BadHandle("fold client returned a foreign record")
                raw_bytes = (
                    len(record.value or b"")
                    + len(record.key or b"")
                    + sum(len(k.encode()) + len(v or b"") for k, v in (record.headers or ()))
                )
                if raw_bytes > self.settings.max_record_bytes:
                    raise CodecError("fold record exceeds configured input limit")
                event = self.decode(record)
                item = FoldInput(record.offset, event)
                encoded = _json.encode(item)
                if len(encoded.encode()) > self.settings.max_mapped_bytes:
                    raise CodecError("fold canonical envelope exceeds configured input limit")
                # Own a snapshot before another callback can mutate its payload/metadata.
                decoded = _json.decode(encoded)
                inputs.append(FoldInput(decoded["offset"], FoldEvent(**decoded["event"])))
                added = len(encoded.encode()) + raw_bytes
                size += added
                self.retained_records += 1
                self.retained_bytes += added * 6
                del record, records, fetched, event, item, decoded
            if not current():
                return
            proof = await self._proof(tp, assigned, lease)
            if inputs and current():

                async def commit() -> FoldLease:
                    # Staging may outlive the lease; refresh and commit share one deadline.
                    refreshed = await self.store.refresh(
                        lease, proof, lease_ms=self.lease_ms, is_current=current
                    )
                    assigned.lease = refreshed
                    if not current():
                        self._release(refreshed)
                        return refreshed
                    return await self.store.commit(refreshed, inputs, proof, self.reducer, is_current=current)

                assigned.lease = await self._operation(assigned, commit())
            assigned.attempts = 0
        except BaseException:
            assigned.seek_needed = True
            raise
        finally:
            count, held_bytes = len(inputs), size * 6

            def released(_: object = None) -> None:
                inputs.clear()
                self.retained_records -= count
                self.retained_bytes -= held_bytes

            if assigned.unresolved is not None:
                assigned.unresolved.add_done_callback(released)
            else:
                released()

    async def run(self) -> None:
        if self._started:
            raise BadHandle("fold runner cannot be restarted")
        check_listener(cast(Any, self.client))
        self._started = True
        self._task = asyncio.current_task()
        try:
            while not self._stopped.is_set():
                ready = list(self._parts.items())
                progressed = False
                for tp, assigned in ready:
                    if (
                        assigned.unresolved is not None
                        or assigned.failure is not None
                        or not self._live(tp, assigned)
                        or assigned.ready_at > asyncio.get_running_loop().time()
                    ):
                        continue
                    progressed = True
                    try:
                        await self._partition(tp, assigned)
                    except Exception as error:  # noqa: BLE001 - classify host/driver failures and stop only their partition
                        _clear_failure_frames(error)
                        if not self._live(tp, assigned):
                            continue
                        cause = error.cause if isinstance(error, TransactionFailure) else error
                        if errors.code_of(cause) == "in_progress":
                            # Another fenced owner is still valid; waiting does not spend a DB-failure budget.
                            assigned.ready_at = asyncio.get_running_loop().time() + 0.1
                            continue
                        if errors.failure_kind(cause) != "transient":
                            assigned.failure = cause
                            if assigned.lease is not None:
                                self._release(assigned.lease)
                            continue
                        assigned.attempts += 1
                        action = _json.decode(
                            _native.batch_recover("retry", assigned.attempts, 1, self.settings.max_attempts)
                        )["action"]
                        if action != "retry":
                            assigned.failure = cause
                            if assigned.lease is not None:
                                self._release(assigned.lease)
                            continue
                        assigned.ready_at = asyncio.get_running_loop().time() + self.pacing.delay(
                            assigned.attempts - 1
                        )
                # Avoid busy polling empty partitions and wake immediately on stop.
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), 0.01 if progressed else 0.05)
        finally:
            self._stopped.set()
            # Lease expiry is the recovery fallback; release is a best-effort optimization.
            for assigned in self._parts.values():
                if assigned.lease is not None:
                    with suppress(Exception):
                        await asyncio.wait_for(self.store.release(assigned.lease), self.timeout)

    async def stop(self) -> None:
        """Stop admission; return after the drain deadline even if a client ignores cancellation."""
        self._stopped.set()
        task = self._task
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            await asyncio.wait({task}, timeout=self.drain_timeout)

    async def wait_closed(self) -> None:
        """Wait until operations actually release their data; may outlive the stop deadline."""
        # A caller's deadline cancels only this waiter, never the operations still owning data.
        if self._task is not None and self._task is not asyncio.current_task():
            await asyncio.shield(asyncio.gather(self._task, return_exceptions=True))
        if self._operations:
            await asyncio.shield(asyncio.gather(*self._operations, return_exceptions=True))
        if self._releases:
            await asyncio.shield(asyncio.gather(*self._releases, return_exceptions=True))
        await asyncio.sleep(0)  # complete data-credit release callbacks


def _clear_failure_frames(error: BaseException) -> None:
    failure: BaseException | None = error
    seen: set[int] = set()
    while failure is not None and id(failure) not in seen:
        seen.add(id(failure))
        traceback.clear_frames(failure.__traceback__)
        failure = failure.__cause__ or failure.__context__
