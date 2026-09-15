"""edax: polyglot event-sourcing toolkit (Python SDK over the shared kernel)."""

from . import (
    aggregate,
    broker,
    causation,
    codec,
    command,
    dispatcher,
    errors,
    filter,
    observe,
    projection,
    query,
    router,
    saga,
    store,
    upcast,
)
from ._edax import (
    backoff_millis,
    current_timestamp,
    guid,
    idempotency_decision,
    partition,
    payload_hash,
    payload_hash_json,
    rendezvous,
    # Staleness only: `should_apply(5, 5)` is True, because an ordinal cannot tell a redelivery from
    # a re-send. `idempotent_by_id` below is the dedup; this is the ordering check. See
    # `docs/kernels/event.md`. The compiled binding is generated and carries no docstring of its own.
    should_apply,
    upcast_plan,
)
from .aggregate import (
    Aggregate,
    Envelope,
    Event,
    EventEnvelope,
    KernelSerializer,
    Serializer,
    Version,
    kernel,
)

# The wrappers, not the raw kernel functions: one public name per causation rule.
from .causation import ensure_ids, originate, propagate
from .codec import json_codec, msgpack_codec
from .command import (
    CommandBus,
    IdempotencyStore,
    Record,
    idempotent,
    retry,
)
from .dispatcher import DeadLetterFn, Delivery, DispatcherStats, KeyOrderedDispatcher
from .errors import (
    AlreadyRegistered,
    BadHandle,
    CachedFailure,
    CodecError,
    ConcurrencyError,
    EdaxError,
    HandlerNotFound,
    InProgress,
    PayloadMismatch,
    StoreError,
    is_terminal,
)
from .observe import Observer, error_type, timing
from .projection import Entity, ProjectionStore
from .router import EventRouter, idempotent_by_id
from .saga import Completed, Saga, SagaInstance, SagaRunner, SagaStore
from .store import (
    AggregateCodec,
    DrainStats,
    EventCodec,
    EventStore,
    Relay,
    Repository,
    Snapshot,
    Transactor,
    Tx,
    current_tx,
    pump,
    transaction,
)
from .upcast import Upcaster

__all__ = [
    # submodules (every seam and wrapper module, so `from edax import *` is the whole surface)
    "aggregate",
    "broker",
    "causation",
    "codec",
    "command",
    "dispatcher",
    "errors",
    "filter",
    "observe",
    "projection",
    "query",
    "router",
    "saga",
    "store",
    "upcast",
    # kernel functions
    "backoff_millis",
    "current_timestamp",
    "guid",
    "idempotency_decision",
    "partition",
    "payload_hash",
    "payload_hash_json",
    "rendezvous",
    "should_apply",
    "upcast_plan",
    # causation (edax.causation wrappers over the kernel rules)
    "ensure_ids",
    "originate",
    "propagate",
    # models and serialization
    "Aggregate",
    "Envelope",
    "Event",
    "EventEnvelope",
    "KernelSerializer",
    "Serializer",
    "Version",
    "kernel",
    "json_codec",
    "msgpack_codec",
    "Upcaster",
    # errors
    "AlreadyRegistered",
    "BadHandle",
    "CachedFailure",
    "CodecError",
    "ConcurrencyError",
    "EdaxError",
    "HandlerNotFound",
    "InProgress",
    "PayloadMismatch",
    "StoreError",
    "is_terminal",
    # write side
    "CommandBus",
    "IdempotencyStore",
    "Record",
    "idempotent",
    "idempotent_by_id",
    "retry",
    # storage seam and orchestration
    "AggregateCodec",
    "DrainStats",
    "EventCodec",
    "EventStore",
    "Relay",
    "Repository",
    "Snapshot",
    "Transactor",
    "Tx",
    "current_tx",
    "pump",
    "transaction",
    # read side
    "Entity",
    "ProjectionStore",
    "EventRouter",
    "DeadLetterFn",
    "Delivery",
    "DispatcherStats",
    "KeyOrderedDispatcher",
    # observability (the seam, the timing middleware, the `error.type` rule)
    "Observer",
    "error_type",
    "timing",
    # sagas
    "Completed",
    "Saga",
    "SagaInstance",
    "SagaRunner",
    "SagaStore",
]
