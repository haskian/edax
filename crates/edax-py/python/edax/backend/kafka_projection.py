"""Kafka projection runner with database-authoritative cursors and verified source continuity."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Collection, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast
from uuid import uuid4

from .. import _json, _native
from ..batch import BatchSettings, recover
from ..errors import BadHandle, CodecError, StoreError, TransactionFailure
from ..pacing import Pacing
from ..projection import (
    ProjectionEvent,
    ProjectionInput,
    ProjectionLease,
    ProjectionStateStore,
    Reducer,
    SourceProof,
)
from . import _kafka
from ._kafka import FetchedRecord, check_consumer, check_durations, check_listener

if TYPE_CHECKING:
    from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]


T = TypeVar("T")


class ProjectionConsumerClient(Protocol):
    """Only the fetch, assignment and seek operations needed by authoritative checkpoints."""

    async def getmany(
        self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None
    ) -> Mapping[TopicPartition, Sequence[FetchedRecord]]: ...
    def assignment(self) -> Collection[TopicPartition]: ...
    def seek(self, partition: TopicPartition, offset: int, /) -> Any: ...


class ProjectionSource(Protocol):
    """Host capability backed by supported identity and retained-boundary APIs.

    Verify cluster/topic incarnation and either detect truncation or enforce a no-rollback
    source history. ``end`` is the readable end (last stable offset for read_committed).
    A configured nickname is not evidence. Failure to prove continuity must raise.
    """

    async def verify(self, partition: TopicPartition, /) -> SourceProof: ...


class ProjectionDecoder(Protocol):
    def __call__(self, record: FetchedRecord, /) -> ProjectionEvent: ...


@dataclass
class _Assigned:
    owner: str = field(default_factory=lambda: uuid4().hex)
    failure: BaseException | None = None
    unresolved: asyncio.Task[Any] | None = None
    lease: ProjectionLease | None = None
    attempts: int = 0
    ready_at: float = 0
    #: Consecutive waits on another live owner; reset once this owner makes progress.
    contended: int = 0
    seek_needed: bool = True
    #: Loop time after which an idle partition extends its lease; half of it is still left.
    renew_at: float = 0


class KafkaProjection:
    """Sequential bounded partition batches with a pure synchronous decoder/reducer.

    Subscribe with ``rebalance_listener`` before starting the caller-owned client. Disable
    automatic commit and use ``auto_offset_reset='none'``. Broker commits are unnecessary;
    every assignment seeks from the database. This runner never skips a poison delta.
    A slow generation is serialized by its store; batches rotate across ready partitions.
    """

    def __init__(
        self,
        client: ProjectionConsumerClient,
        store: ProjectionStateStore,
        source: ProjectionSource,
        decode: ProjectionDecoder,
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
            raise BadHandle("projection consumers require auto_offset_reset='none'")
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
            raise BadHandle("projection lease must exceed the operation deadline")
        self.client, self.store, self.source = client, store, source
        self.decode, self.reducer = decode, reducer
        self.settings = settings or BatchSettings()
        self._reserve = _native.batch_reserve(self.settings.json)
        self.lease_ms, self.timeout, self.drain_timeout = lease_ms, operation_timeout_s, drain_timeout_s
        self.pacing = pacing or Pacing()
        self._parts: dict[TopicPartition, _Assigned] = {}
        self._stopped = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._started = False
        self._releases: set[asyncio.Task[None]] = set()
        self._operations: set[asyncio.Task[Any]] = set()
        self._fetches: dict[TopicPartition, asyncio.Task[Any]] = {}
        self._retained_records = 0
        self._retained_bytes = 0

    @property
    def retained_records(self) -> int:
        """Prepared inputs held in memory, including those an unresolved operation still owns."""
        return self._retained_records

    @property
    def retained_bytes(self) -> int:
        """The kernel's charge for those inputs, against ``BatchSettings.retained_bytes``."""
        return self._retained_bytes

    @property
    def partition_failures(self) -> dict[TopicPartition, BaseException]:
        """Stopped partitions require operator repair/reassignment; healthy partitions continue."""
        return {tp: part.failure for tp, part in self._parts.items() if part.failure is not None}

    def rebalance_listener(self) -> ConsumerRebalanceListener:
        """Install this listener before starting the client's subscription."""
        return _kafka.rebalance_listener(self._revoke, self._assign)

    async def _revoke(self, revoked: Collection[TopicPartition]) -> None:
        # Retire immediately, before any wait on an unresolved I/O operation.
        retired = [self._parts.pop(tp) for tp in revoked if tp in self._parts]
        tasks = [self._release(part.lease) for part in retired if part.lease is not None]
        if tasks:
            await asyncio.wait(tasks, timeout=self.drain_timeout)

    async def _assign(self, assigned: Collection[TopicPartition]) -> None:
        for tp in assigned:
            self._parts[tp] = _Assigned()

    def _release(self, lease: ProjectionLease) -> asyncio.Task[None]:
        task = asyncio.create_task(self.store.release(lease))
        self._releases.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            self._releases.discard(done)
            _kafka.retrieve_failure(done)

        task.add_done_callback(finished)
        return task

    def _retire(self, tp: TopicPartition, assigned: _Assigned, lease: ProjectionLease) -> None:
        """Release ``lease`` once its assignment is gone. A live assignment keeps it: an operation that only
        missed its deadline needs recovery, and releasing its lease would fence this runner out."""
        if not self._live(tp, assigned):
            assigned.lease = None
            self._release(lease)

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
                    _kafka.clear_failure_frames(error)

        task.add_done_callback(finished)
        try:
            done, _ = await asyncio.wait({task}, timeout=self.timeout)
            if done:
                return task.result()
            raise StoreError("projection operation exceeded its deadline; outcome requires recovery")
        except BaseException:
            assigned.seek_needed = True
            raise
        finally:
            if not task.done():
                assigned.unresolved = task
                task.cancel()

    async def _proof(
        self, tp: TopicPartition, assigned: _Assigned, lease: ProjectionLease | None = None
    ) -> SourceProof:
        proof = await self._operation(assigned, self.source.verify(tp))
        cursor = lease.next_offset if lease else proof.beginning
        self.store.target.check_source(proof, cursor)
        return proof

    async def _partition(self, tp: TopicPartition, assigned: _Assigned) -> None:
        def current() -> bool:
            return self._live(tp, assigned) and assigned.unresolved is None

        previous_fetch = self._fetches.get(tp)
        if previous_fetch is not None and not previous_fetch.done():
            return
        if tp.topic != self.store.target.topic:
            raise BadHandle("projection assignment contains another topic")
        loop = asyncio.get_running_loop()
        lease = assigned.lease
        # A healthy partition proves its source and touches the database only when it has a batch to
        # commit, or when half its lease has passed. Recovery and a new assignment do both at once.
        if lease is None or assigned.seek_needed or loop.time() >= assigned.renew_at:
            proof = await self._proof(tp, assigned)
            if not current():
                return
            if lease is None:
                call = self.store.acquire(
                    tp.partition, assigned.owner, proof, lease_ms=self.lease_ms, is_current=current
                )
            else:
                # Refresh reloads an uncertain commit and refences an expired current assignment.
                call = self.store.refresh(lease, proof, lease_ms=self.lease_ms, is_current=current)
            lease = assigned.lease = await self._operation(assigned, call)
            assigned.renew_at = loop.time() + self.lease_ms / 2000
            if not current():
                self._retire(tp, assigned, lease)
                return
            # Seeking happens only at a quiescent boundary with no retained previous batch.
            if assigned.seek_needed:
                self.client.seek(tp, lease.next_offset)
                assigned.seek_needed = False
        inputs: list[ProjectionInput] = []
        size = charged = 0
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
                    self._retained_records,
                    self._retained_bytes,
                    1,
                    self._reserve,
                    self.settings.json,
                ):
                    break
                fetched = await self._operation(
                    assigned,
                    self.client.getmany(
                        tp,
                        # An idle partition must not hold up the others: wait only once a batch has started.
                        timeout_ms=min(self.settings.flush_ms, max(1, int(self.timeout * 500)))
                        if inputs
                        else 0,
                        max_records=1,
                    ),
                    fetch=tp,
                )
                if not current():
                    return
                records = fetched.get(tp, ())
                if any(key != tp and values for key, values in fetched.items()):
                    raise BadHandle("projection client violated the requested partition or fetch limit")
                if not records:
                    break
                if len(records) != 1:
                    raise BadHandle("projection client violated the requested partition or fetch limit")
                record = records[0]
                if record.topic != tp.topic or record.partition != tp.partition:
                    raise BadHandle("projection client returned a foreign record")
                raw_bytes = _kafka.record_bytes(record)
                if raw_bytes > self.settings.max_record_bytes:
                    raise CodecError("projection record exceeds configured input limit")
                event = self.decode(record)
                item = ProjectionInput(record.offset, event)
                encoded = _json.encode(item)
                serialized = len(encoded.encode())
                if serialized > self.settings.max_mapped_bytes:
                    raise CodecError("projection canonical envelope exceeds configured input limit")
                # Own a snapshot before another callback can mutate its payload/metadata.
                decoded = _json.decode(encoded)
                inputs.append(ProjectionInput(decoded["offset"], ProjectionEvent(**decoded["event"])))
                charge = _native.batch_charge(serialized, self.settings.json)
                size += serialized
                charged += charge
                self._retained_records += 1
                self._retained_bytes += charge
                del record, records, fetched, event, item, decoded
            if not current():
                return
            if inputs and current():
                proof = await self._proof(tp, assigned, lease)
                if not current():
                    return
                # One transaction extends the lease and commits the batch; staging cannot spend it.
                assigned.lease = await self._operation(
                    assigned,
                    self.store.commit(
                        lease, inputs, proof, self.reducer, lease_ms=self.lease_ms, is_current=current
                    ),
                )
                assigned.renew_at = loop.time() + self.lease_ms / 2000
                if not current():
                    # Revoked during the commit: hand the lease the commit extended to the next owner.
                    self._retire(tp, assigned, assigned.lease)
                    return
            assigned.attempts = assigned.contended = 0
        except BaseException:
            assigned.seek_needed = True
            if assigned.lease is not None and assigned.unresolved is None:
                self._retire(tp, assigned, assigned.lease)
            raise
        finally:
            count, held_bytes = len(inputs), charged

            def released(_: object = None) -> None:
                inputs.clear()
                self._retained_records -= count
                self._retained_bytes -= held_bytes

            if assigned.unresolved is not None:
                assigned.unresolved.add_done_callback(released)
            else:
                released()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Process assigned partitions until ``stop`` is set or :meth:`stop` is called. Single use."""
        if self._started:
            raise BadHandle("projection runner cannot be restarted")
        check_listener(cast(Any, self.client))
        self._started = True
        self._task = asyncio.current_task()
        watcher = asyncio.create_task((stop or asyncio.Event()).wait())

        def stopped(done: asyncio.Task[Any]) -> None:
            if not done.cancelled():
                self._stopped.set()

        watcher.add_done_callback(stopped)
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
                        _kafka.clear_failure_frames(error)
                        if not self._live(tp, assigned):
                            continue
                        decision = recover(
                            error,
                            self.settings,
                            attempt=assigned.attempts + 1,
                            waits=assigned.contended + 1,
                            count=1,
                            isolable=False,
                        )
                        now = asyncio.get_running_loop().time()
                        if decision["action"] == "wait":
                            # Another fenced owner is still valid. Wait without spending the failure
                            # budget, longer each time, but never past one lease.
                            assigned.contended += 1
                            assigned.ready_at = now + min(
                                self.pacing.delay(decision["step"]), self.lease_ms / 1000
                            )
                            continue
                        if decision["action"] != "retry":
                            assigned.failure = error.cause if isinstance(error, TransactionFailure) else error
                            if assigned.lease is not None:
                                self._release(assigned.lease)
                            continue
                        assigned.attempts += 1
                        assigned.ready_at = now + self.pacing.delay(decision["step"])
                # Avoid busy polling empty partitions and wake immediately on stop.
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), 0.01 if progressed else 0.05)
        finally:
            watcher.cancel()
            self._stopped.set()
            # Lease expiry is the recovery fallback; release is a best-effort optimization. Snapshot the
            # partitions: a rebalance callback may change them while the releases run.
            releases = [self._release(a.lease) for a in list(self._parts.values()) if a.lease is not None]
            if releases:
                await asyncio.wait(releases, timeout=self.timeout)

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
