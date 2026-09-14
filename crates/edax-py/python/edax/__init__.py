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
from .dispatcher import Delivery, KeyOrderedDispatcher
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
from .projection import Entity, ProjectionStore
from .router import EventRouter
from .saga import Completed, Saga, SagaInstance, SagaRunner, SagaStore
from .store import (
    AggregateCodec,
    EventCodec,
    EventStore,
    Relay,
    Repository,
    Snapshot,
    Transactor,
    current_tx,
    pump,
    transaction,
    with_tx,
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
    # storage seam and orchestration
    "AggregateCodec",
    "EventCodec",
    "EventStore",
    "Relay",
    "Repository",
    "Snapshot",
    "Transactor",
    "current_tx",
    "pump",
    "transaction",
    "with_tx",
    # read side
    "Entity",
    "ProjectionStore",
    "EventRouter",
    "Delivery",
    "KeyOrderedDispatcher",
    # sagas
    "Completed",
    "Saga",
    "SagaInstance",
    "SagaRunner",
    "SagaStore",
]
