"""edax: polyglot event-sourcing toolkit (Python SDK over the shared kernel)."""

from . import (
    aggregate,
    broker,
    causation,
    codec,
    command,
    dispatcher,
    errors,
    observe,
    pacing,
    projection,
    query,
    router,
    saga,
    store,
    tenancy,
    upcast,
)
from . import (
    filter as filter,
)
from ._edax import (
    # The watermark after an event applies: the higher sequence, so an unsequenced event never
    # erases it. Pair it with `is_stale`. The compiled binding carries no docstring of its own.
    advance,
    backoff_millis,
    # The exact text `payload_hash_json` hashes, for a host that checks bytes and not only hashes.
    canonical_json,
    current_timestamp,
    guid,
    # Ordering, not dedup: `is_stale(5, 5)` is False, because an ordinal cannot tell a redelivery
    # from a re-send. `idempotent_by_id` below is the dedup. See `docs/kernels/event.md`.
    is_stale,
    partition,
    payload_hash,
    payload_hash_json,
    upcast_plan,
    upcast_target,
)
from .aggregate import (
    Aggregate,
    CommandMessage,
    Event,
    EventDraft,
    EventMessage,
    KernelSerializer,
    Lineage,
    Serializer,
    Version,
)

# The wrappers, not the raw kernel functions: one public name per causation rule.
from .causation import caused_by, current_lineage, originate, propagate, use_lineage
from .codec import json_codec, msgpack_codec
from .command import (
    Claimed,
    CommandBus,
    IdempotencyStore,
    Record,
    Won,
    claim_key,
    command_message,
    current_command,
    forget,
    idempotent,
    received_key,
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
    Rejected,
    StoreError,
    is_terminal,
)
from .observe import Observer, error_type, timing
from .pacing import DEFAULT_BACKOFF, Backoff, Pacing
from .projection import Entity, ProjectionStore
from .query import Page
from .router import EventRouter, idempotent_by_id
from .saga import Completed, Saga, SagaInstance, SagaRunner, SagaStore
from .store import (
    AggregateCodec,
    CommandOutbox,
    DrainStats,
    EventCodec,
    EventStore,
    Outbox,
    Relay,
    Repository,
    Snapshot,
    Transactor,
    Tx,
    check_sequence_batch,
    current_tx,
    pump,
    transaction,
)
from .tenancy import TENANT_MAX, Tenancy, check_tenant, tenant_key
from .upcast import Upcaster

__all__ = [
    # submodules (every seam and wrapper module but `filter`, so `from edax import *` is the whole surface
    # without rebinding the builtin `filter`; reach the module as `edax.filter`)
    "aggregate",
    "broker",
    "causation",
    "codec",
    "command",
    "dispatcher",
    "errors",
    "observe",
    "pacing",
    "projection",
    "query",
    "router",
    "saga",
    "store",
    "tenancy",
    "upcast",
    # kernel functions
    "advance",
    "backoff_millis",
    "canonical_json",
    "current_timestamp",
    "guid",
    "is_stale",
    "partition",
    "payload_hash",
    "payload_hash_json",
    "upcast_plan",
    "upcast_target",
    # causation (edax.causation wrappers over the kernel rules, and the ambient lineage)
    "Lineage",
    "caused_by",
    "current_lineage",
    "originate",
    "propagate",
    "use_lineage",
    # models and serialization
    "Aggregate",
    "CommandMessage",
    "Event",
    "EventDraft",
    "EventMessage",
    "KernelSerializer",
    "Serializer",
    "Version",
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
    "Rejected",
    "StoreError",
    "is_terminal",
    # write side
    "Claimed",
    "CommandBus",
    "IdempotencyStore",
    "Record",
    "Won",
    "claim_key",
    "received_key",
    "command_message",
    "current_command",
    "forget",
    "idempotent",
    "idempotent_by_id",
    "retry",
    # tenancy: the mode a deployment keys by, and the kernel's key rule
    "TENANT_MAX",
    "Tenancy",
    "check_tenant",
    "tenant_key",
    # retry pacing (the kernel's jittered backoff, the sleep, the draw)
    "Backoff",
    "DEFAULT_BACKOFF",
    "Pacing",
    # storage seam and orchestration
    "AggregateCodec",
    "CommandOutbox",
    "DrainStats",
    "EventCodec",
    "EventStore",
    "Outbox",
    "Relay",
    "Repository",
    "Snapshot",
    "Transactor",
    "Tx",
    "check_sequence_batch",
    "current_tx",
    "pump",
    "transaction",
    # read side
    "Entity",
    "Page",
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
