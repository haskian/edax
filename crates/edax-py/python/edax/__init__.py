"""edax: polyglot event-sourcing toolkit (Python SDK over the shared kernel)."""

from . import (
    aggregate,
    broker,
    causation,
    codec,
    command,
    delivery,
    dispatcher,
    errors,
    idempotency,
    kernel,
    message,
    observe,
    outbox,
    pacing,
    projection,
    query,
    repository,
    router,
    saga,
    serializer,
    store,
    tenancy,
    unit_of_work,
    upcast,
)
from . import (
    filter as filter,
)
from .aggregate import Aggregate, AggregateCodec, CommandCodec, EventCodec, PayloadCodec, Version

# The wrappers, not the raw kernel functions: one public name per causation rule.
from .causation import caused_by, current_lineage, originate, propagate, use_lineage
from .codec import json_codec, msgpack_codec
from .command import CommandBus, command_message, current_command, idempotent, received_key, retry
from .delivery import DeadLetter, DeadLetterSink, Delivery, Undecodable
from .dispatcher import DispatcherStats, DispatcherStopped, KeyOrderedDispatcher
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
    is_fatal,
    is_terminal,
)
from .idempotency import Claimed, IdempotencyStore, Record, Won, claim_key, forget
from .kernel import (
    advance,
    canonical_json,
    current_timestamp,
    guid,
    is_stale,
    partition,
    payload_hash,
    payload_hash_json,
)
from .message import CommandMessage, EventDraft, EventMessage, Lineage, Payload
from .observe import Observer, error_type, timing
from .outbox import CommandOutbox, DrainStats, Outbox, Relay, pump
from .pacing import DEFAULT_BACKOFF, Backoff, Pacing
from .projection import Entity, ProjectionQuery, ProjectionStore
from .query import Page
from .repository import Repository
from .router import EventRouter, event_claim_key, forget_event, idempotent_by_id
from .saga import (
    SAGA_COMMAND_FAILED,
    Completed,
    CompletedInstance,
    RunningInstance,
    Saga,
    SagaCommandFailed,
    SagaInstance,
    SagaOutbox,
    SagaRunner,
    SagaStore,
)
from .serializer import KernelSerializer, Serializer
from .store import EventStore, Snapshot, check_sequence_batch
from .tenancy import TENANT_MAX, Tenancy, check_tenant, tenant_key
from .unit_of_work import Transactor, Tx, current_tx, transaction
from .upcast import Upcaster

__all__ = [
    # submodules (every seam and wrapper module but `filter`, so `from edax import *` is the whole surface
    # without rebinding the builtin `filter`; reach the module as `edax.filter`)
    "aggregate",
    "broker",
    "causation",
    "codec",
    "command",
    "delivery",
    "dispatcher",
    "errors",
    "idempotency",
    "kernel",
    "message",
    "observe",
    "outbox",
    "pacing",
    "projection",
    "query",
    "repository",
    "router",
    "saga",
    "serializer",
    "store",
    "tenancy",
    "unit_of_work",
    "upcast",
    # kernel functions (edax.kernel wrappers: a failure raises its typed edax error)
    "advance",
    "canonical_json",
    "current_timestamp",
    "guid",
    "is_stale",
    "partition",
    "payload_hash",
    "payload_hash_json",
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
    "EventDraft",
    "EventMessage",
    "KernelSerializer",
    "CommandCodec",
    "Payload",
    "PayloadCodec",
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
    "is_fatal",
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
    "event_claim_key",
    "forget_event",
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
    "ProjectionQuery",
    "ProjectionStore",
    "EventRouter",
    "DeadLetter",
    "DeadLetterSink",
    "Delivery",
    "DispatcherStats",
    "DispatcherStopped",
    "KeyOrderedDispatcher",
    "Undecodable",
    # observability (the seam, the timing middleware, the `error.type` rule)
    "Observer",
    "error_type",
    "timing",
    # sagas
    "SAGA_COMMAND_FAILED",
    "Completed",
    "CompletedInstance",
    "RunningInstance",
    "Saga",
    "SagaCommandFailed",
    "SagaInstance",
    "SagaOutbox",
    "SagaRunner",
    "SagaStore",
]
