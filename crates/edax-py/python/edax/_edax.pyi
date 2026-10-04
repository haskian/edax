# Committed type stub for the alef-generated `_edax` extension (PEP 561, beside `_edax.abi3.so`
# + `py.typed`). Gives the message records their field types; the native classes have none.
# Copy of `crates/edax-py/.alef/_edax.pyi` with the alef header stripped so alef cleanup leaves it.
# Never edit by hand: regenerate with `scripts/regenerate_binding.sh`.

class BackoffPolicy:
    base_millis: int
    cap_millis: int
    def __init__(self, base_millis: int, cap_millis: int) -> None: ...

class CommandMessage:
    id: str
    idempotency_key: str | None
    aggregate_id: str | None
    tenant: str | None
    name: str
    version: int
    data: bytes
    correlation_id: str
    causation_id: str
    issued_at: int
    extensions: dict[str, str]
    metadata: dict[str, str]
    def __init__(
        self,
        id: str,  # noqa: A002
        name: str,
        version: int,
        data: bytes,
        correlation_id: str,
        causation_id: str,
        issued_at: int,
        extensions: dict[str, str],
        metadata: dict[str, str],
        idempotency_key: str | None = None,
        aggregate_id: str | None = None,
        tenant: str | None = None,
    ) -> None: ...

class Event:
    name: str
    version: int
    data: bytes
    def __init__(self, name: str, version: int, data: bytes) -> None: ...

class EventDraft:
    id: str
    aggregate_id: str
    tenant: str | None
    sequence: int | None
    name: str
    version: int
    data: bytes
    correlation_id: str
    causation_id: str
    occurred_at: int | None
    extensions: dict[str, str]
    metadata: dict[str, str]
    def __init__(
        self,
        id: str,  # noqa: A002
        aggregate_id: str,
        name: str,
        version: int,
        data: bytes,
        correlation_id: str,
        causation_id: str,
        extensions: dict[str, str],
        metadata: dict[str, str],
        tenant: str | None = None,
        sequence: int | None = None,
        occurred_at: int | None = None,
    ) -> None: ...

class EventIdempotency:
    key: str
    payload_hash: int
    def __init__(self, key: str, payload_hash: int) -> None: ...

class EventMessage:
    id: str
    aggregate_id: str
    tenant: str | None
    sequence: int | None
    name: str
    version: int
    data: bytes
    correlation_id: str
    causation_id: str
    occurred_at: int | None
    recorded_at: int
    extensions: dict[str, str]
    metadata: dict[str, str]
    def __init__(
        self,
        id: str,  # noqa: A002
        aggregate_id: str,
        name: str,
        version: int,
        data: bytes,
        correlation_id: str,
        causation_id: str,
        recorded_at: int,
        extensions: dict[str, str],
        metadata: dict[str, str],
        tenant: str | None = None,
        sequence: int | None = None,
        occurred_at: int | None = None,
    ) -> None: ...

class IdempotencyRecord:
    payload_hash: int
    state: str
    error: str
    error_code: str
    result_json: str
    def __init__(
        self,
        payload_hash: int,
        state: str,
        error: str,
        error_code: str,
        result_json: str,
    ) -> None: ...

class KafkaDue:
    topic: str
    partition: int
    offset: int
    def __init__(self, topic: str, partition: int, offset: int) -> None: ...

class KafkaHeader:
    key: str
    value: bytes
    def __init__(self, key: str, value: bytes) -> None: ...

class KafkaRecord:
    key: bytes
    value: bytes
    headers: list[KafkaHeader]
    def __init__(self, key: bytes, value: bytes, headers: list[KafkaHeader]) -> None: ...

class KafkaRoute:
    registered: int | None
    unknown: str | None
    headerless: bool
    def __init__(
        self,
        headerless: bool,
        registered: int | None = None,
        unknown: str | None = None,
    ) -> None: ...

class Lineage:
    correlation_id: str
    causation_id: str
    def __init__(self, correlation_id: str, causation_id: str) -> None: ...

class MemoryClaim:
    won: bool
    handle: bytes
    prior: str
    def __init__(self, won: bool, handle: bytes, prior: str) -> None: ...

class NackDecision:
    seek: int | None
    def __init__(self, seek: int | None = None) -> None: ...

class PartitionStats:
    topic: str
    partition: int
    unsettled: int
    live: int
    max_delivered: int
    committed: int | None
    def __init__(
        self,
        topic: str,
        partition: int,
        unsettled: int,
        live: int,
        max_delivered: int,
        committed: int | None = None,
    ) -> None: ...

class PollResult:
    token: int
    partition: int
    offset: int
    event: EventMessage | None
    command: CommandMessage | None
    def __init__(
        self,
        token: int,
        partition: int,
        offset: int,
        event: EventMessage | None = None,
        command: CommandMessage | None = None,
    ) -> None: ...

class ProjectionRow:
    version: int
    document_json: str
    def __init__(self, version: int, document_json: str) -> None: ...

class RedisScripts:
    claim: str
    complete: str
    release: str
    extend: str
    forget: str
    def __init__(
        self,
        claim: str,
        complete: str,
        release: str,
        extend: str,
        forget: str,
    ) -> None: ...

class RetriesTracked:
    attempts: int
    contended: int
    def __init__(self, attempts: int, contended: int) -> None: ...

class RetryVerdict:
    dead: bool
    attempt: int
    step: int
    def __init__(self, dead: bool, attempt: int, step: int) -> None: ...

class SagaAdmit:
    action: str
    folded: str
    def __init__(self, action: str, folded: str) -> None: ...

class SagaRow:
    version: int
    completed: bool
    state_json: str
    folded: str
    def __init__(self, version: int, completed: bool, state_json: str, folded: str) -> None: ...

class SagaWrite:
    action: str
    expected: int
    version: int
    def __init__(self, action: str, expected: int, version: int) -> None: ...

class SnapshotResult:
    type_name: str
    version: int
    data: bytes
    def __init__(self, type_name: str, version: int, data: bytes) -> None: ...

class SplitError:
    code: str
    message: str
    terminal: bool
    def __init__(self, code: str, message: str, terminal: bool) -> None: ...

class TenancyRules:
    requires_tenant: bool
    composes_keys: bool
    def __init__(self, requires_tenant: bool, composes_keys: bool) -> None: ...

class MemoryBroker:
    def publish_event(self, message: EventMessage) -> None: ...
    def publish_command(self, message: CommandMessage) -> None: ...
    def try_poll(self, group: str) -> PollResult | None: ...
    def ack(self, group: str, token: int) -> bool: ...
    def nack(self, group: str, token: int) -> bool: ...
    def is_live(self, group: str, token: int) -> bool: ...
    def crash(self, group: str) -> None: ...
    def is_empty(self, group: str) -> bool: ...
class MemoryTransactor:
    def begin(self) -> int: ...
    def commit(self, tx: int) -> None: ...
    def rollback(self, tx: int) -> bool: ...
    def open_count(self) -> int: ...
    def load_events(self, tx: int, aggregate_id: str, after: int) -> list[EventMessage]: ...
    def append_events(
        self,
        tx: int,
        aggregate_id: str,
        expected_version: int,
        drafts: list[EventDraft],
        recorded_at: int,
    ) -> list[EventMessage]: ...
    def load_snapshot(self, tx: int, aggregate_id: str) -> SnapshotResult | None: ...
    def save_snapshot(
        self,
        tx: int,
        aggregate_id: str,
        type_name: str,
        version: int,
        data: bytes,
    ) -> None: ...
    def unpublished(self, limit: int) -> list[EventMessage]: ...
    def unpublished_count(self) -> int: ...
    def mark_published(self, ids: list[str]) -> None: ...
    def claim(self, tx: int, key: str, payload_hash: int) -> MemoryClaim: ...
    def complete(self, tx: int, key: str, handle: bytes, record_json: str) -> None: ...
    def release(self, tx: int, key: str, handle: bytes) -> None: ...
    def forget(self, tx: int, key: str) -> None: ...
    def expire_claim(self, key: str) -> bool: ...
    def get_projection(
        self,
        tx: int,
        id: str,  # noqa: A002
    ) -> ProjectionRow | None: ...
    def save_projection(
        self,
        tx: int,
        id: str,  # noqa: A002
        version: int,
        document_json: str,
    ) -> int: ...
    def query_projections(self, tx: int, query_json: str) -> str: ...
    def load_saga(
        self,
        tx: int,
        id: str,  # noqa: A002
    ) -> SagaRow | None: ...
    def save_saga(
        self,
        tx: int,
        id: str,  # noqa: A002
        version: int,
        state_json: str,
        folded: str,
        commands: list[CommandMessage],
    ) -> int: ...
    def complete_saga(
        self,
        tx: int,
        id: str,  # noqa: A002
        version: int,
        commands: list[CommandMessage],
    ) -> int: ...
    def unpublished_saga_commands(self, limit: int) -> list[CommandMessage]: ...
    def mark_saga_commands_published(self, ids: list[str]) -> None: ...
    def enqueue_commands(self, tx: int, commands: list[CommandMessage]) -> None: ...
    def unpublished_commands(self, limit: int) -> list[CommandMessage]: ...
    def unpublished_command_count(self) -> int: ...
    def mark_commands_published(self, ids: list[str]) -> None: ...
class OffsetTracker:
    def book(self, topic: str, partition: int, offset: int) -> int: ...
    def is_live(self, token: int) -> bool: ...
    def ack(self, token: int) -> bool: ...
    def due(self) -> list[KafkaDue]: ...
    def committed(self, topic: str, partition: int, offset: int) -> None: ...
    def nack(self, token: int) -> NackDecision: ...
    def reset(self, topic: str, partition: int) -> None: ...
    def stats(self) -> list[PartitionStats]: ...
class Retries:
    def attempt(
        self,
        id: str,  # noqa: A002
        broker_attempt: int,
    ) -> int: ...
    def succeeded(
        self,
        id: str,  # noqa: A002
    ) -> None: ...
    def failed(
        self,
        id: str,  # noqa: A002
        broker_attempt: int,
        code: str | None = None,
    ) -> RetryVerdict: ...
    def dead_lettered(
        self,
        id: str,  # noqa: A002
    ) -> None: ...
    def sink_failed(
        self,
        id: str,  # noqa: A002
        attempt: int,
    ) -> int: ...
    def tracked(self) -> RetriesTracked: ...

def advance(stored: int | None = None, incoming: int | None = None) -> int | None: ...
def backoff_millis(base_millis: int, cap_millis: int, attempt: int, entropy: int) -> int: ...
def cached_failure_message(error: str, error_code: str | None = None) -> str: ...
def canonical_json(payload: bytes) -> str: ...
def check_extensions(extensions: dict[str, str]) -> None: ...
def check_sequence_batch(expected: int, sequences: list[int | None]) -> None: ...
def check_tenant(tenant: str) -> None: ...
def claim_key(scope: str, idempotency_key: str) -> str: ...
def command_hash_json(name: str, payload: bytes) -> int: ...
def current_timestamp() -> int: ...
def decode_command(
    format: str,  # noqa: A002
    wire: bytes,
) -> CommandMessage: ...

def decode_cursor(cursor: str, sort: str) -> str: ...
def decode_event(
    format: str,  # noqa: A002
    wire: bytes,
) -> EventMessage: ...

def decode_idempotency_record(record_json: str) -> IdempotencyRecord: ...
def decode_prior_idempotency_record(record_json: str, claimant_hash: int) -> IdempotencyRecord: ...
def default_backoff() -> BackoffPolicy: ...
def effective_sort(sort: str) -> str: ...
def encode_command(
    format: str,  # noqa: A002
    message: CommandMessage,
) -> bytes: ...

def encode_cursor(doc: str, sort: str) -> str: ...
def encode_event(
    format: str,  # noqa: A002
    message: EventMessage,
) -> bytes: ...

def encode_idempotency_record(
    payload_hash: int,
    pending: bool,
    result_json: str,
    error: str | None = None,
    error_code: str | None = None,
) -> str: ...

def error_code_is_terminal(code: str) -> bool: ...
def error_codes() -> list[str]: ...
def error_verdict(wire: str) -> bool: ...
def eval_filter(
    filter: str,  # noqa: A002
    doc: str,
) -> bool: ...

def event_idempotency(message: EventMessage) -> EventIdempotency: ...
def guid() -> str: ...
def idempotency_outcome(
    current: int,
    payload_hash: int,
    pending: bool,
    error: str | None = None,
) -> str: ...

def idempotency_settle(code: str | None = None) -> str | None: ...
def is_stale(stored: int | None = None, incoming: int | None = None) -> bool: ...
def kafka_check_media_types(registered: list[str]) -> None: ...
def kafka_content_type_header() -> str: ...
def kafka_dead_letter_record(
    record: KafkaRecord,
    topic: str,
    partition: int,
    offset: int,
    error: str,
) -> KafkaRecord: ...

def kafka_partition_for(key: bytes, partitions: int) -> int: ...
def kafka_poison_id(topic: str, partition: int, offset: int) -> str: ...
def kafka_record(key: str, value: bytes, content_type: str) -> KafkaRecord: ...
def kafka_route(
    headers: list[KafkaHeader],
    registered: list[str],
    default_kind: str,
) -> KafkaRoute: ...

def media_type(
    format: str,  # noqa: A002
    kind: str,
) -> str: ...

def memory_broker(partitions: int) -> MemoryBroker: ...
def memory_transactor() -> MemoryTransactor: ...
def no_tx() -> int: ...
def offset_tracker() -> OffsetTracker: ...
def ordering_key(
    id: str,  # noqa: A002
    aggregate_id: str | None = None,
) -> str: ...

def originate(
    fresh_id: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> Lineage: ...

def partition(aggregate_id: str, n: int) -> int: ...
def payload_hash(payload: bytes) -> int: ...
def payload_hash_json(payload: bytes) -> int: ...
def propagate(parent_id: str, parent_correlation_id: str) -> Lineage: ...
def query(query: str, docs: str) -> str: ...
def received_key(
    id: str,  # noqa: A002
    idempotency_key: str | None = None,
) -> str: ...

def record_event(draft: EventDraft, recorded_at: int) -> EventMessage: ...
def redis_check_claim(claim: bytes) -> None: ...
def redis_check_config(
    prefix: str,
    ttl_ms: int,
    claim_ttl_ms: int,
    op_timeout_ms: int,
    heartbeat: bool,
) -> None: ...

def redis_claim_outcome(claim: bytes, reply: bytes | None = None) -> str | None: ...
def redis_forget_outcome(key: str, held: bytes | None = None) -> bool: ...
def redis_forget_reads() -> int: ...
def redis_heartbeats_per_ttl() -> int: ...
def redis_ledger_key(prefix: str, key: str) -> bytes: ...
def redis_new_claim(payload_hash: int) -> bytes: ...
def redis_reply_error(reply: str) -> str: ...
def redis_scripts() -> RedisScripts: ...
def redis_settled_value(claim: bytes, record_json: str) -> bytes: ...
def retries(max_attempts: int, sink: bool) -> Retries: ...
def saga_admit(completed: bool, folded: str, message: EventMessage) -> SagaAdmit: ...
def saga_command(
    saga: str,
    instance_id: str,
    version: int,
    index: int,
    command: Event,
    cause_id: str,
    cause_correlation_id: str,
    issued_at: int,
    tenant: str | None = None,
) -> CommandMessage: ...

def saga_command_failed(
    command: CommandMessage,
    error: str,
    recorded_at: int,
    code: str | None = None,
) -> EventMessage: ...

def saga_command_failed_name() -> str: ...
def saga_command_id(instance_id: str, version: int, index: int) -> str: ...
def saga_write(completes: bool, has_commands: bool, version: int | None = None) -> SagaWrite: ...
def snapshot_due(every: int, initial: int, current: int) -> bool: ...
def split_error(wire: str) -> SplitError: ...
def tenancy_rules(mode: str) -> TenancyRules: ...
def tenant_key(mode: str, key: str, tenant: str | None = None) -> str: ...
def unreadable_idempotency_record(payload_hash: int, cause: str) -> IdempotencyRecord: ...
def upcast_plan(from_version: int, to: int, available: list[int] | None = None) -> list[int]: ...
