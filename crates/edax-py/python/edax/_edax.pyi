# Committed type stub for the alef-generated `_edax` extension (PEP 561, beside `_edax.abi3.so`
# + `py.typed`). Gives the message records their field types; the native classes have none.
# Copy of `crates/edax-py/.alef/_edax.pyi` with the alef header stripped so alef cleanup leaves it.
# Never edit by hand: regenerate with `mise run bindings:generate`.

class BackoffPolicy:
    base_millis: int
    cap_millis: int
    def __init__(self, base_millis: int, cap_millis: int) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> BackoffPolicy: ...

class ClaimResult:
    won: bool
    handle: bytes | None
    prior: str | None
    def __init__(
        self,
        won: bool,
        handle: bytes | None = None,
        prior: str | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> ClaimResult: ...

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
    @staticmethod
    def from_json(json_str: str) -> CommandMessage: ...

class DraftBatch:
    drafts: list[EventDraft]
    version: int
    snapshot: bool
    def __init__(self, drafts: list[EventDraft], version: int, snapshot: bool) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> DraftBatch: ...

class DrainReply:
    value: DrainRowFailed | None
    def __init__(self, value: DrainRowFailed | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> DrainReply: ...

class DrainRow:
    id: str
    tenant: str | None
    aggregate_id: str | None
    def __init__(
        self,
        id: str,  # noqa: A002
        tenant: str | None = None,
        aggregate_id: str | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> DrainRow: ...

class DrainRowFailed:
    action: str
    attempt: int | None
    def __init__(self, action: str, attempt: int | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> DrainRowFailed: ...

class Drained:
    settled: list[str]
    published: int
    dead_lettered: int
    wait_step: int | None
    raise_id: str | None
    def __init__(
        self,
        settled: list[str],
        published: int,
        dead_lettered: int,
        wait_step: int | None = None,
        raise_id: str | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> Drained: ...

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
    @staticmethod
    def from_json(json_str: str) -> EventDraft: ...

class EventIdempotency:
    key: str
    payload_hash: int
    def __init__(self, key: str, payload_hash: int) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> EventIdempotency: ...

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
    @staticmethod
    def from_json(json_str: str) -> EventMessage: ...

class IdempotencyRecord:
    payload_hash: int
    state: str
    error: str | None
    error_code: str | None
    result_json: str | None
    def __init__(
        self,
        payload_hash: int,
        state: str,
        error: str | None = None,
        error_code: str | None = None,
        result_json: str | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> IdempotencyRecord: ...

class IdempotencyReplay:
    outcome: str
    error: str | None
    def __init__(self, outcome: str, error: str | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> IdempotencyReplay: ...

class KafkaDue:
    topic: str
    partition: int
    offset: int
    def __init__(self, topic: str, partition: int, offset: int) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> KafkaDue: ...

class KafkaHeader:
    key: str
    value: bytes
    def __init__(self, key: str, value: bytes) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> KafkaHeader: ...

class KafkaRecord:
    key: bytes
    value: bytes
    headers: list[KafkaHeader]
    def __init__(self, key: bytes, value: bytes, headers: list[KafkaHeader]) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> KafkaRecord: ...

class KafkaRevoked:
    value: KafkaDue | None
    def __init__(self, value: KafkaDue | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> KafkaRevoked: ...

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
    @staticmethod
    def from_json(json_str: str) -> KafkaRoute: ...

class Lineage:
    correlation_id: str
    causation_id: str
    def __init__(self, correlation_id: str, causation_id: str) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> Lineage: ...

class NackDecision:
    seek: int | None
    def __init__(self, seek: int | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> NackDecision: ...

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
    @staticmethod
    def from_json(json_str: str) -> PartitionStats: ...

class Payload:
    name: str
    version: int
    data: bytes
    def __init__(self, name: str, version: int, data: bytes) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> Payload: ...

class PollLookup:
    value: PollResult | None
    def __init__(self, value: PollResult | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> PollLookup: ...

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
    @staticmethod
    def from_json(json_str: str) -> PollResult: ...

class ProjectionLookup:
    value: ProjectionRow | None
    def __init__(self, value: ProjectionRow | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> ProjectionLookup: ...

class ProjectionRow:
    version: int
    document_json: str
    def __init__(self, version: int, document_json: str) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> ProjectionRow: ...

class RedisDefaults:
    ttl_ms: int
    claim_ttl_ms: int
    op_timeout_ms: int
    def __init__(self, ttl_ms: int, claim_ttl_ms: int, op_timeout_ms: int) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> RedisDefaults: ...

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
    @staticmethod
    def from_json(json_str: str) -> RedisScripts: ...

class RetriesTracked:
    attempts: int
    contended: int
    def __init__(self, attempts: int, contended: int) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> RetriesTracked: ...

class RetryVerdict:
    action: str
    attempt: int | None
    step: int | None
    failure: str
    def __init__(
        self,
        action: str,
        failure: str,
        attempt: int | None = None,
        step: int | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> RetryVerdict: ...

class SagaAdmit:
    action: str
    folded: str | None
    def __init__(self, action: str, folded: str | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SagaAdmit: ...

class SagaLookup:
    value: SagaRow | None
    def __init__(self, value: SagaRow | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SagaLookup: ...

class SagaRow:
    version: int
    completed: bool
    state_json: str
    folded: str
    def __init__(self, version: int, completed: bool, state_json: str, folded: str) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SagaRow: ...

class SagaWrite:
    action: str
    expected: int | None
    version: int | None
    def __init__(
        self,
        action: str,
        expected: int | None = None,
        version: int | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SagaWrite: ...

class SnapshotLookup:
    value: SnapshotResult | None
    def __init__(self, value: SnapshotResult | None = None) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SnapshotLookup: ...

class SnapshotResult:
    type_name: str
    version: int
    data: bytes
    def __init__(self, type_name: str, version: int, data: bytes) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SnapshotResult: ...

class SplitError:
    code: str
    message: str
    terminal: bool
    def __init__(self, code: str, message: str, terminal: bool) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> SplitError: ...

class TenancyRules:
    requires_tenant: bool
    composes_keys: bool
    def __init__(self, requires_tenant: bool, composes_keys: bool) -> None: ...
    @staticmethod
    def from_json(json_str: str) -> TenancyRules: ...

class Drain:
    def begin(self, rows: list[DrainRow]) -> None: ...
    def admits(
        self,
        id: str,  # noqa: A002
    ) -> bool: ...
    def published(
        self,
        id: str,  # noqa: A002
    ) -> None: ...
    def failed(
        self,
        id: str,  # noqa: A002
        code: str | None = None,
    ) -> DrainRowFailed: ...
    def sunk(
        self,
        id: str,  # noqa: A002
        attempt: int,
        taken: bool,
        code: str | None = None,
    ) -> DrainRowFailed | None: ...
    def finish(self) -> Drained: ...
    def store_failed(self) -> int: ...
    def store_answered(self) -> None: ...
    def tracked(self) -> RetriesTracked: ...
    def sunk_result(
        self,
        id: str,  # noqa: A002
        attempt: int,
        taken: bool,
        code: str | None = None,
    ) -> DrainReply: ...
class MemoryBroker:
    def publish_event(self, message: EventMessage) -> None: ...
    def publish_command(self, message: CommandMessage) -> None: ...
    def try_poll(self, group: str) -> PollResult | None: ...
    def ack(self, group: str, token: int) -> bool: ...
    def nack(self, group: str, token: int) -> bool: ...
    def is_live(self, group: str, token: int) -> bool: ...
    def crash(self, group: str) -> None: ...
    def is_empty(self, group: str) -> bool: ...
    def try_poll_result(self, group: str) -> PollLookup: ...
class MemoryCommandOutbox:
    def enqueue(self, tx: int, commands: list[CommandMessage]) -> None: ...
    def unpublished(self, limit: int) -> list[CommandMessage]: ...
    def unpublished_count(self) -> int: ...
    def mark_published(self, ids: list[str]) -> None: ...
class MemoryEventStore:
    def load(self, tx: int, aggregate_id: str, after: int) -> list[EventMessage]: ...
    def append(
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
    def load_snapshot_result(self, tx: int, aggregate_id: str) -> SnapshotLookup: ...
class MemoryIdempotencyStore:
    def claim(self, tx: int, key: str, payload_hash: int) -> ClaimResult: ...
    def complete(self, tx: int, key: str, handle: bytes, record_json: str) -> None: ...
    def release(self, tx: int, key: str, handle: bytes) -> None: ...
    def forget(self, tx: int, key: str) -> None: ...
    def expire(self, key: str) -> bool: ...
    def put_raw(self, key: str, record_json: str) -> None: ...
class MemoryProjectionStore:
    def get(
        self,
        tx: int,
        id: str,  # noqa: A002
    ) -> ProjectionRow | None: ...
    def save(
        self,
        tx: int,
        id: str,  # noqa: A002
        version: int,
        document_json: str,
    ) -> int: ...
    def get_many(self, tx: int, query_json: str) -> str: ...
    def get_result(
        self,
        tx: int,
        id: str,  # noqa: A002
    ) -> ProjectionLookup: ...
class MemorySagaStore:
    def load(
        self,
        tx: int,
        saga: str,
        id: str,  # noqa: A002
    ) -> SagaRow | None: ...
    def save(
        self,
        tx: int,
        saga: str,
        id: str,  # noqa: A002
        version: int,
        state_json: str,
        folded: str,
        commands: list[CommandMessage],
    ) -> int: ...
    def complete(
        self,
        tx: int,
        saga: str,
        id: str,  # noqa: A002
        version: int,
        commands: list[CommandMessage],
    ) -> int: ...
    def unpublished(self, saga: str, limit: int) -> list[CommandMessage]: ...
    def unpublished_count(self, saga: str) -> int: ...
    def mark_published(self, saga: str, ids: list[str]) -> None: ...
    def load_result(
        self,
        tx: int,
        saga: str,
        id: str,  # noqa: A002
    ) -> SagaLookup: ...
class MemoryTransactor:
    def begin(self) -> int: ...
    def commit(self, tx: int) -> None: ...
    def rollback(self, tx: int) -> bool: ...
    def open_count(self) -> int: ...
    def event_store(self) -> MemoryEventStore: ...
    def idempotency_store(self) -> MemoryIdempotencyStore: ...
    def projection_store(self) -> MemoryProjectionStore: ...
    def saga_store(self) -> MemorySagaStore: ...
    def command_outbox(self) -> MemoryCommandOutbox: ...
class OffsetTracker:
    def book(self, topic: str, partition: int, offset: int) -> int: ...
    def is_live(self, token: int) -> bool: ...
    def ack(self, token: int) -> bool: ...
    def due(self) -> list[KafkaDue]: ...
    def committed(self, topic: str, partition: int, offset: int) -> None: ...
    def nack(self, token: int) -> NackDecision: ...
    def revoke(self, topic: str, partition: int) -> KafkaDue | None: ...
    def reset(self, topic: str, partition: int) -> None: ...
    def stats(self) -> list[PartitionStats]: ...
    def revoke_result(self, topic: str, partition: int) -> KafkaRevoked: ...
class Retries:
    def attempt(
        self,
        id: str,  # noqa: A002
        broker_attempt: int,
        tenant: str | None = None,
    ) -> int: ...
    def succeeded(
        self,
        id: str,  # noqa: A002
        tenant: str | None = None,
    ) -> None: ...
    def failed(
        self,
        id: str,  # noqa: A002
        broker_attempt: int,
        code: str | None = None,
        tenant: str | None = None,
    ) -> RetryVerdict: ...
    def sunk(
        self,
        id: str,  # noqa: A002
        attempt: int,
        taken: bool,
        code: str | None = None,
        tenant: str | None = None,
    ) -> int | None: ...
    def tracked(self) -> RetriesTracked: ...
class Streak:
    def failed(self) -> int: ...
    def answered(self) -> None: ...

def advance(
    stored_present: bool,
    stored: int,
    incoming_present: bool,
    incoming: int,
) -> int | None: ...

def backoff_millis(base_millis: int, cap_millis: int, attempt: int, entropy: int) -> int: ...
def canonical_decimal(text: str) -> str: ...
def canonical_json(payload: bytes) -> str: ...
def canonical_timestamp(text: str) -> str: ...
def check_extensions(extensions: dict[str, str]) -> None: ...
def check_projection_document(
    id: str,  # noqa: A002
    document_json: str,
) -> None: ...

def check_query(query: str) -> None: ...
def check_sequence_batch(expected: int, sequences: list[int | None]) -> int: ...
def check_tenant(tenant: str) -> None: ...
def claim_key(mode: str, scope: str, idempotency_key: str, tenant: str | None = None) -> str: ...
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
def draft_batch(
    aggregate_id: str,
    initial: int,
    snapshot_every: int,
    payloads: list[Payload],
    ids: list[str],
    lineage: Lineage,
    extensions: dict[str, str],
    metadata: dict[str, str],
    tenant: str | None = None,
    occurred_at: int | None = None,
) -> DraftBatch: ...

def drain(max_attempts: int, sink: bool) -> Drain: ...
def drain_actions() -> list[str]: ...
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

def error_codes() -> list[str]: ...
def error_verdict(code: str | None = None) -> bool: ...
def eval_filter(
    filter: str,  # noqa: A002
    doc: str,
) -> bool: ...

def event_claim_key(mode: str, scope: str, event_id: str, tenant: str | None = None) -> str: ...
def event_idempotency(message: EventMessage, mode: str, scope: str) -> EventIdempotency: ...
def guid() -> str: ...
def idempotency_outcomes() -> list[str]: ...
def idempotency_replay(
    current: int,
    key: str,
    payload_hash: int,
    pending: bool,
    error: str | None = None,
    error_code: str | None = None,
) -> IdempotencyReplay: ...

def idempotency_settle_failure(
    payload_hash: int,
    message: str,
    code: str | None = None,
) -> IdempotencyRecord | None: ...

def idempotency_states() -> list[str]: ...
def is_stale(stored_present: bool, stored: int, incoming_present: bool, incoming: int) -> bool: ...
def kafka_acks_warning(acks: int) -> str | None: ...
def kafka_broker_error(error_code: int, message: str) -> str: ...
def kafka_check_config(
    publish_timeout_ms: int | None = None,
    commit_interval_ms: int | None = None,
    commit_timeout_ms: int | None = None,
    drain_timeout_ms: int | None = None,
) -> None: ...

def kafka_check_consumer(auto_commit: bool, has_group: bool) -> None: ...
def kafka_check_dead_sink(has_dead_sink: bool) -> None: ...
def kafka_check_media_types(registered: list[str]) -> None: ...
def kafka_command_record(
    tenancy: str,
    message: CommandMessage,
    value: bytes,
    content_type: str,
    key: str | None = None,
) -> KafkaRecord: ...

def kafka_content_type_header() -> str: ...
def kafka_dead_letter_record(
    record: KafkaRecord,
    topic: str,
    partition: int,
    offset: int,
    message: str,
    code: str | None = None,
) -> KafkaRecord: ...

def kafka_dead_letter_stub(dead: KafkaRecord) -> KafkaRecord: ...
def kafka_event_record(
    tenancy: str,
    message: EventMessage,
    value: bytes,
    content_type: str,
    key: str | None = None,
) -> KafkaRecord: ...

def kafka_partition_for(key: bytes, partitions: int) -> int: ...
def kafka_poison_id(topic: str, partition: int, offset: int) -> str: ...
def kafka_route(
    headers: list[KafkaHeader],
    registered: list[str],
    default_kind: str,
) -> KafkaRoute: ...

def keyed_projection_document(
    id: str,  # noqa: A002
    document_json: str,
) -> str: ...

def media_type(
    format: str,  # noqa: A002
    kind: str,
) -> str: ...

def memory_broker(partitions: int, tenancy: str) -> MemoryBroker: ...
def memory_transactor() -> MemoryTransactor: ...
def message_kinds() -> list[str]: ...
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

def own_command(command: CommandMessage, tenant: str | None = None) -> CommandMessage: ...
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

def redis_claim_outcome(claim: bytes, reply: bytes | None = None) -> ClaimResult: ...
def redis_claim_outcome_present(claim: bytes, present: bool, reply: bytes) -> ClaimResult: ...
def redis_complete_outcome(key: str, written: bool) -> None: ...
def redis_defaults() -> RedisDefaults: ...
def redis_forget_exhausted(key: str) -> str: ...
def redis_forget_outcome(key: str, held: bytes | None = None) -> bool: ...
def redis_forget_outcome_present(key: str, present: bool, held: bytes) -> bool: ...
def redis_forget_reads() -> int: ...
def redis_heartbeat_due_ms(sent_at_ms: int, claim_ttl_ms: int, beat: int) -> int: ...
def redis_ledger_key(prefix: str, key: str) -> bytes: ...
def redis_new_claim(payload_hash: int) -> bytes: ...
def redis_reply_error(reply: str) -> str: ...
def redis_scripts() -> RedisScripts: ...
def redis_settled_value(claim: bytes, record_json: str) -> bytes: ...
def retries(max_attempts: int, sink: bool) -> Retries: ...
def retry_actions() -> list[str]: ...
def retry_attempts(attempts: int) -> None: ...
def retry_failure(code: str | None = None) -> str: ...
def retry_failures() -> list[str]: ...
def retry_step(attempts: int, failed: int, code: str | None = None) -> int | None: ...
def saga_admit(
    saga: str,
    completed: bool,
    folded: str,
    message: EventMessage,
    version: int | None = None,
) -> SagaAdmit: ...

def saga_admit_actions() -> list[str]: ...
def saga_command(
    saga: str,
    instance_id: str,
    version: int,
    index: int,
    payload: Payload,
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
def saga_command_id(saga: str, instance_id: str, version: int, index: int) -> str: ...
def saga_write(
    completes: bool,
    has_commands: bool,
    completed: bool,
    version: int | None = None,
) -> SagaWrite: ...

def saga_write_actions() -> list[str]: ...
def snapshot_due(every: int, initial: int, current: int) -> bool: ...
def split_error(wire: str) -> SplitError: ...
def streak() -> Streak: ...
def tenancy_modes() -> list[str]: ...
def tenancy_rules(mode: str) -> TenancyRules: ...
def tenant_key(mode: str, key: str, tenant: str | None = None) -> str: ...
def tenant_max() -> int: ...
def unreadable_idempotency_record(payload_hash: int, cause: str) -> IdempotencyRecord: ...
def upcast_plan(from_version: int, to: int, available: list[int] | None = None) -> list[int]: ...
def upcast_plan_present(
    from_version: int,
    to: int,
    present: bool,
    available: list[int],
) -> list[int]: ...

def upcast_target(available: list[int] | None = None) -> int: ...
def upcast_target_present(present: bool, available: list[int]) -> int: ...
def wire_formats() -> list[str]: ...
