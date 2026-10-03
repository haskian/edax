//! Flat, plain-Rust API surface over the edax kernels and adapters.
//!
//! This crate holds no logic of its own: every function forwards to a kernel (`edax-core` /
//! `edax-event` / `edax-cqrs` / `edax-kafka` / `edax-redis`) or to a backend (`edax-memory`),
//! reshaping only at the edges so the surface is trivial to bind. alef extracts these signatures and
//! emits the native binding from them, so the shapes here are chosen to map cleanly:
//!
//! - A fallible call returns `Result<_, String>`, and the generator raises on `Err`. The text is
//!   always a kernel [`edax_core::Error`]'s wire form `"<code>: <message>"`, so the code comes from
//!   the kernel and never from a choice made here.
//! - Bytes are `Vec<u8>` / `&[u8]`.
//! - Dictionaries are `HashMap<String, String>`, converted to the kernel's ordered `BTreeMap` where
//!   deterministic encoding requires it.
//! - An aggregate result is a plain struct rather than a tuple.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use edax_core::ErrorCode;

// The kernel's nested envelope is referred to fully-qualified (`edax_event::EventEnvelope`); the names
// `Event` / `EventEnvelope` below are this crate's own binding-facing records, shaped to mirror it.

// ---- routing / sequencing ------------------------------------------------

/// The local worker, of `n`, that runs `aggregate_id`'s events: local fan-out only, never broker
/// placement (that is [`kafka_partition_for`]). See [`edax_event::partition()`].
pub fn partition(aggregate_id: &str, n: u32) -> u32 {
    edax_event::partition(aggregate_id, n)
}

/// Whether an event with sequence `incoming` is older than the watermark `stored`, so an ordered
/// consumer skips it. Ordering, not dedup: an equal sequence is not stale. See
/// [`edax_event::is_stale`] and `docs/kernels/event.md`.
pub fn is_stale(stored: Option<u64>, incoming: Option<u64>) -> bool {
    edax_event::is_stale(stored, incoming)
}

/// The watermark after an event with sequence `incoming` applied over `stored`: the higher of the
/// two, so an unsequenced event never erases it. See [`edax_event::advance`].
pub fn advance(stored: Option<u64>, incoming: Option<u64>) -> Option<u64> {
    edax_event::advance(stored, incoming)
}

/// Check an append batch against the dense-sequence rule: every envelope carries a sequence and the
/// batch runs `expected + 1, expected + 2, ...` in order. `Err` names the 0-based batch index and is a
/// `codec:` (terminal) error. An empty batch passes: it is a version check only.
pub fn check_sequence_batch(expected: u64, sequences: Vec<Option<u64>>) -> Result<(), String> {
    edax_event::sequence::check_batch(expected, &sequences).map_err(wire)
}

/// Whether a save from version `initial` to `current` writes a snapshot under an interval of `every`
/// versions (`0`: never). See [`edax_event::snapshot_due`].
pub fn snapshot_due(every: u64, initial: u64, current: u64) -> bool {
    edax_event::snapshot_due(every, initial, current)
}

/// A backoff policy, in milliseconds. See [`edax_core::Backoff`].
pub struct BackoffPolicy {
    /// The ceiling of the first retry's wait; each later attempt doubles it.
    pub base_millis: u64,
    /// The bound on any single wait.
    pub cap_millis: u64,
}

/// The toolkit's default backoff policy, so a host never restates its numbers.
pub fn default_backoff() -> BackoffPolicy {
    let policy = edax_core::Backoff::DEFAULT;
    BackoffPolicy { base_millis: policy.base_millis, cap_millis: policy.cap_millis }
}

/// The wait in milliseconds before retry `attempt` (0-based) under the policy `(base_millis,
/// cap_millis)`: full jitter, `entropy` (a random `u64` the host draws per wait) reduced into
/// `0 ..= ceiling`. Flat arguments rather than a [`BackoffPolicy`], so a host calls it without
/// building a native object per wait. See [`edax_core::backoff_millis`].
pub fn backoff_millis(base_millis: u64, cap_millis: u64, attempt: u64, entropy: u64) -> u64 {
    edax_core::backoff_millis(edax_core::Backoff { base_millis, cap_millis }, attempt, entropy)
}

/// A fresh time-ordered globally-unique id (UUID v7), as a string. Non-deterministic.
pub fn guid() -> String {
    edax_core::guid()
}

/// Current Unix time in nanoseconds, strictly increasing per process. Non-deterministic.
pub fn current_timestamp() -> i64 {
    edax_core::current_timestamp()
}

// ---- error mapping -------------------------------------------------------

/// A kernel failure as it crosses: the kernel's own wire form `"<code>: <message>"`, nothing added.
/// Every kernel function fails with [`edax_core::Error`], so the code is always the kernel's and a
/// call site here cannot pick a wrong one, or none.
fn wire(error: edax_core::Error) -> String {
    error.to_string()
}

/// Every code of the error taxonomy, in the kernel's order. A host builds its exception table from
/// this list rather than restating it, so a code the kernel adds is a code the host must map.
pub fn error_codes() -> Vec<String> {
    ErrorCode::ALL.iter().map(|code| code.as_str().to_owned()).collect()
}

/// Whether `code` is terminal: it fails the same way on every retry, so an idempotency guard caches
/// it instead of releasing the claim. A code the kernel does not know is a `codec:` (terminal) error:
/// the same name fails the same way every time. For the verdict on a whole wire string, known code
/// or not, see [`error_verdict`].
pub fn error_code_is_terminal(code: &str) -> Result<bool, String> {
    code.parse::<ErrorCode>().map(ErrorCode::is_terminal).map_err(wire)
}

/// The verdict for a wire failure: its code's, or the kernel's default for a string with no code it
/// knows. A host reads its default for an unclassified failure from here instead of choosing one.
/// See [`edax_core::verdict_of_wire`].
pub fn error_verdict(wire: &str) -> bool {
    edax_core::verdict_of_wire(wire)
}

/// A wire failure taken apart. A struct rather than a tuple: the generator sanitizes a tuple return
/// to `Vec<String>` and loses the field names.
pub struct SplitError {
    /// The taxonomy code: `store` when the wire form carries no code the kernel knows.
    pub code: String,
    /// The message after the separator, or the whole wire string when it carries no known code.
    pub message: String,
    /// The kernel's verdict for this failure.
    pub terminal: bool,
}

/// Split a wire failure `"<code>: <message>"` into its code, its message and its verdict.
///
/// An unknown code, or a string with no separator, is a `store` failure carrying the whole string
/// unchanged, so a host still logs the original text and decides nothing itself. The separator and
/// that default are the kernel's ([`edax_core::Error::from_wire`]); a host that split the string
/// itself would own a second copy of them. `testdata/golden/error_wire.json` pins every case.
pub fn split_error(wire: &str) -> SplitError {
    let error = edax_core::Error::from_wire(wire);
    SplitError { code: error.code.as_str().to_owned(), terminal: error.is_terminal(), message: error.message }
}

// ---- filter / query ------------------------------------------------------

/// Evaluate a filter (JSON) against a document (JSON). The match decision — including the two-valued
/// null/missing-path rules — is the kernel's. (The boundary is JSON text: alef has no object-passing
/// for arbitrary values, so structured crossing isn't available here.)
pub fn eval_filter(filter: &str, doc: &str) -> Result<bool, String> {
    edax_cqrs::eval_json(filter.as_bytes(), doc.as_bytes()).map_err(wire)
}

/// Apply a read query (JSON string) to a JSON array of documents; returns the page as JSON text
/// `{"rows": [...], "next": cursor|null}`, `next` minted under the query's own sort. See
/// [`edax_cqrs::query()`].
pub fn query(query: &str, docs: &str) -> Result<String, String> {
    edax_cqrs::query_json(query.as_bytes(), docs.as_bytes()).map_err(wire)
}

/// Mint the keyset cursor that resumes after a document (JSON string) under sort keys (JSON string),
/// for a backend that pages natively. A row without a non-null `id` is a `codec:` error.
pub fn encode_cursor(doc: &str, sort: &str) -> Result<String, String> {
    edax_cqrs::encode_cursor_json(doc.as_bytes(), sort.as_bytes()).map_err(wire)
}

/// Decode a keyset cursor into the value tuple (JSON array string) a backend binds into its own
/// keyset `WHERE` clause, one value per key of the effective sort (sort keys as a JSON string). A
/// cursor minted under another sort is a `codec:` error.
pub fn decode_cursor(cursor: &str, sort: &str) -> Result<String, String> {
    edax_cqrs::decode_cursor_json(cursor.as_bytes(), sort.as_bytes()).map_err(wire)
}

/// The sort keys a page is actually ordered by, with the `id` tiebreak appended (JSON in, JSON out).
///
/// A backend ordering by the declared sort alone disagrees with the kernel at every tie.
pub fn effective_sort(sort: &str) -> Result<String, String> {
    edax_cqrs::effective_sort_json(sort.as_bytes()).map_err(wire)
}

// ---- upcast / causation --------------------------------------------------

/// Ordered upcast steps from `from_version` to `to` given the available step versions, or `None` for
/// an event name with no transforms (every step is the identity, so the plan is empty).
///
/// A gap in the steps, or a downward plan (a payload from a newer writer), is the kernel's `codec`
/// error: the event cannot be unmarshaled to the target version, and no extra attempt changes that.
/// A codec asks on every unmarshal. See [`edax_event::upcast_plan`].
pub fn upcast_plan(from_version: i32, to: i32, available: Option<Vec<i32>>) -> Result<Vec<i32>, String> {
    edax_event::upcast_plan(from_version, to, available.as_deref()).map_err(wire)
}

/// Causation/correlation metadata for a child event given its parent's metadata and event id.
pub fn propagate(parent_metadata: HashMap<String, String>, parent_event_id: &str) -> HashMap<String, String> {
    let parent: BTreeMap<String, String> = parent_metadata.into_iter().collect();
    edax_event::propagate(&parent, parent_event_id).into_iter().collect()
}

/// The metadata key whose value is constant across one logical flow, `"correlation_id"`. A call
/// because the generator binds functions, not constants. See [`edax_event::CORRELATION_ID`].
pub fn correlation_id_key() -> String {
    edax_event::CORRELATION_ID.to_owned()
}

/// The metadata key whose value is the id of the event that directly caused this one,
/// `"causation_id"`. See [`edax_event::CAUSATION_ID`].
pub fn causation_id_key() -> String {
    edax_event::CAUSATION_ID.to_owned()
}

/// Causation/correlation metadata for an originating (root) event, given its metadata and one
/// freshly minted id.
///
/// The host mints `fresh_id` (with [`guid`]) because minting is non-deterministic; the kernel keeps
/// only the decision of which key gets defaulted to what, so a golden vector pins it. Returns just
/// the two chain keys for the host to merge.
pub fn originate(metadata: HashMap<String, String>, fresh_id: &str) -> HashMap<String, String> {
    let given: BTreeMap<String, String> = metadata.into_iter().collect();
    edax_event::originate(&given, fresh_id).into_iter().collect()
}

// ---- idempotency ---------------------------------------------------------

/// Stable 64-bit hash of arbitrary bytes (FNV-1a). For commands, prefer [`payload_hash_json`], which
/// canonicalizes first so every binding agrees.
pub fn payload_hash(payload: &[u8]) -> u64 {
    edax_cqrs::payload_hash(payload)
}

/// Canonicalize a UTF-8 JSON command payload (RFC 8785) and hash it. Canonicalization lives in the
/// kernel, so a reused idempotency key binds to the same hash in every binding — don't canonicalize
/// on the host side. An integer literal past 2^53 is a `codec` error.
pub fn payload_hash_json(payload: &[u8]) -> Result<u64, String> {
    let canonical = edax_cqrs::canonical_json(payload).map_err(wire)?;
    Ok(edax_cqrs::payload_hash(&canonical))
}

/// The kernel's canonical form of a UTF-8 JSON payload: the exact text [`payload_hash_json`] hashes.
/// For a host that must check bytes, not only hashes. See [`edax_cqrs::canonical_json`].
pub fn canonical_json(payload: &[u8]) -> Result<String, String> {
    let canonical = edax_cqrs::canonical_json(payload).map_err(wire)?;
    // The canonical writer emits UTF-8 only, so the lossy path never replaces anything.
    Ok(String::from_utf8_lossy(&canonical).into_owned())
}

/// What an event's idempotency guard claims: the key and the payload hash. See
/// [`edax_event::event_idempotency`].
pub struct EventIdempotency {
    /// The envelope id.
    pub key: String,
    /// FNV-1a over the raw `event.data` bytes.
    pub payload_hash: u64,
}

/// The key and hash an event's idempotency guard claims for `envelope`: the kernel's rule, pinned by
/// `testdata/golden/event_idempotency.json`, so two SDKs consuming one stream bind alike.
pub fn event_idempotency(envelope: EventEnvelope) -> EventIdempotency {
    let env = envelope_from_record(envelope);
    let rule = edax_event::event_idempotency(&env);
    EventIdempotency { key: rule.key.to_owned(), payload_hash: rule.payload_hash }
}

// ---- idempotency record --------------------------------------------------

/// A stored idempotency record, decoded by the kernel. `state` is `"pending"`, `"failed"` or
/// `"done"`; `error` and `error_code` are set only for `"failed"` and `result_json` (the cached
/// result as JSON text) only for `"done"` — the others are empty, and so is `error_code` for a
/// failure stored without a code the kernel knows. Flat strings rather than options so the shape
/// binds without an optional-field conversion.
pub struct IdempotencyRecord {
    pub payload_hash: u64,
    pub state: String,
    pub error: String,
    pub error_code: String,
    pub result_json: String,
}

/// The canonical JSON record for a claim, a cached success, or a cached failure. The kernel applies
/// its reading order to the flat inputs (`pending` wins, then `error`, then `result_json`), so a
/// host that models the record as separate fields cannot write an ambiguous one. `result_json` is
/// the host's result as JSON text; it is read only when the record is neither pending nor failed.
/// `error_code` is the failure's taxonomy code, read only beside `error`; an unknown one is a
/// `codec:` error. `error` is `None` for a record that did not fail; any `Some`, the empty message
/// included, is a failure. See [`edax_cqrs::encode_record_fields`].
pub fn encode_idempotency_record(
    payload_hash: u64,
    pending: bool,
    result_json: &str,
    error: Option<String>,
    error_code: Option<String>,
) -> Result<String, String> {
    edax_cqrs::encode_record_fields(payload_hash, pending, result_json, error, error_code.as_deref()).map_err(wire)
}

/// Decode a stored idempotency record with the kernel's reading order. A record that is not a JSON
/// object, lacks `payload_hash`, or carries a non-integer hash is a `codec:` error. A store reads the
/// record holding a key with [`decode_prior_idempotency_record`] instead.
pub fn decode_idempotency_record(record_json: &str) -> Result<IdempotencyRecord, String> {
    let record = edax_cqrs::decode_record::<serde_json::Value>(record_json.as_bytes()).map_err(wire)?;
    Ok(flat_record(record))
}

/// The record a losing claimant reads from the JSON holding its key. Never fails: a record no reader
/// can decode is a terminal `codec` failure, carrying the stored hash when it still reads, else
/// `claimant_hash`. See [`edax_cqrs::decode_prior`].
pub fn decode_prior_idempotency_record(record_json: &str, claimant_hash: u64) -> IdempotencyRecord {
    flat_record(edax_cqrs::decode_prior(record_json.as_bytes(), claimant_hash))
}

/// The record that stands in for a cached result the host cannot read back as its result type: a
/// terminal `codec` failure under `payload_hash` whose message names `cause`. See
/// [`edax_cqrs::Record::unreadable`].
pub fn unreadable_idempotency_record(payload_hash: u64, cause: &str) -> IdempotencyRecord {
    flat_record(edax_cqrs::Record::unreadable(payload_hash, &edax_core::Error::codec(cause)))
}

/// A decoded record as the flat fields a binding holds.
fn flat_record(record: edax_cqrs::Record<serde_json::Value>) -> IdempotencyRecord {
    let state = record.state.as_str().to_owned();
    let (error, error_code, result_json) = match record.state {
        edax_cqrs::State::Pending => (String::new(), String::new(), String::new()),
        edax_cqrs::State::Failed { code, message } => {
            (message, code.map(|c| c.as_str().to_owned()).unwrap_or_default(), String::new())
        }
        edax_cqrs::State::Done(result) => (String::new(), String::new(), result.to_string()),
    };
    IdempotencyRecord { payload_hash: record.payload_hash, state, error, error_code, result_json }
}

/// The message a replay of a cached failure carries: the original failure's wire form, so its code
/// survives (`"codec: bad payload"`), or the bare message when it was stored without a code the
/// kernel knows. The replay itself is always the terminal `cached_failure`. See
/// [`edax_cqrs::cached_failure`].
pub fn cached_failure_message(error: &str, error_code: Option<String>) -> String {
    edax_cqrs::cached_failure_of(error, error_code.as_deref()).message
}

/// The failure half of the guard's settle table: the code a won claim caches a failure under, or
/// `None` to release the claim instead. `code` is the failure's taxonomy code, `None` for a failure
/// no code classifies; an unknown code is a `codec:` error. See [`edax_cqrs::settle`].
pub fn idempotency_settle(code: Option<String>) -> Result<Option<String>, String> {
    let code = code.map(|c| c.parse::<edax_core::ErrorCode>()).transpose().map_err(wire)?;
    Ok(match edax_cqrs::settle(code) {
        edax_cqrs::Settle::Cache(code) => Some(code.as_str().to_owned()),
        edax_cqrs::Settle::Release => None,
    })
}

/// What a claimant that lost the claim does with the winner's record: `"payload_mismatch"`,
/// `"in_progress"`, `"failed"` (replay the cached error) or `"succeeded"` (replay the cached result).
/// Takes the record's classifying fields rather than its JSON so a host that already decoded the
/// record does not re-encode it; the result plays no part in the verdict. `error` is `None` for a record
/// that did not fail; any `Some`, the empty message included, is a failure.
pub fn idempotency_outcome(current: u64, payload_hash: u64, pending: bool, error: Option<String>) -> String {
    let prior = edax_cqrs::Record { payload_hash, state: edax_cqrs::State::from_wire(pending, error, None, ()) };
    edax_cqrs::outcome(&prior, current).as_str().to_owned()
}

// ---- envelope codec ------------------------------------------------------

/// A marshaled domain event: a name, a version, and the caller-serialized payload `data`. The nested
/// half of [`EventEnvelope`], mirroring the kernel's `edax_event::Event`.
pub struct Event {
    pub name: String,
    pub version: i32,
    pub data: Vec<u8>,
}

/// The binding's event record — the same nested shape as the kernel's `edax_event::EventEnvelope`
/// (`event` is a nested [`Event`]). It is the currency for an envelope across the boundary: returned by
/// [`decode_envelope`] and accepted/returned by the in-memory event store, so events cross as a
/// structured object (bytes stay bytes, dictionaries stay dictionaries) with no JSON or wire encoding.
/// `envelope_from_record` / `record_from_envelope` map it to and from the kernel envelope (only the
/// dictionaries change container: a host `HashMap` becomes the kernel's order-deterministic `BTreeMap`).
pub struct EventEnvelope {
    pub id: String,
    pub aggregate_id: String,
    pub event: Event,
    pub sequence: Option<u64>,
    pub namespace: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub timestamp: i64,
}

/// Binding event -> the kernel's.
fn kernel_event(e: Event) -> edax_event::Event {
    edax_event::Event { name: e.name, version: e.version, data: e.data }
}

/// The kernel's event -> a binding event.
fn record_event(e: edax_event::Event) -> Event {
    Event { name: e.name, version: e.version, data: e.data }
}

/// Binding record -> the kernel's nested envelope (HashMap -> the kernel's ordered BTreeMap).
fn envelope_from_record(r: EventEnvelope) -> edax_event::EventEnvelope {
    edax_event::EventEnvelope {
        id: r.id,
        aggregate_id: r.aggregate_id,
        event: kernel_event(r.event),
        sequence: r.sequence,
        namespace: r.namespace.into_iter().collect(),
        metadata: r.metadata.into_iter().collect(),
        timestamp: r.timestamp,
    }
}

/// The kernel's nested envelope -> a binding record.
fn record_from_envelope(env: edax_event::EventEnvelope) -> EventEnvelope {
    EventEnvelope {
        id: env.id,
        aggregate_id: env.aggregate_id,
        event: record_event(env.event),
        sequence: env.sequence,
        namespace: env.namespace.into_iter().collect(),
        metadata: env.metadata.into_iter().collect(),
        timestamp: env.timestamp,
    }
}

fn resolve(format: &str) -> Result<edax_event::Format, String> {
    edax_event::Format::from_name(format).map_err(wire)
}

/// An envelope -> wire bytes in `format` (`"json"` or `"msgpack"`). Takes the binding's own
/// [`EventEnvelope`], as [`kafka_record`] does, so a host hands over the object it already holds.
pub fn encode_envelope(format: &str, envelope: EventEnvelope) -> Result<Vec<u8>, String> {
    Ok(resolve(format)?.encode(&envelope_from_record(envelope)))
}

/// Wire bytes in `format` -> a decoded [`EventEnvelope`] record (nested `event`).
pub fn decode_envelope(format: &str, wire: &[u8]) -> Result<EventEnvelope, String> {
    let env = resolve(format)?.decode(wire).map_err(self::wire)?;
    Ok(record_from_envelope(env))
}

/// The media type naming `format` (`"json"` or `"msgpack"`) on the wire: the string a transport that
/// carries a type — a Kafka `content-type` header, an HTTP request — puts there. It names the codec
/// and not the transport, so it lives here and not with the Kafka record. See
/// [`edax_event::Format::media_type`].
pub fn media_type(format: &str) -> Result<String, String> {
    Ok(resolve(format)?.media_type().to_owned())
}

// ---- consumer retry policy ------------------------------------------------

/// What [`Retries::failed`] tells the host to do: `dead` (hand the message to the sink, then ack;
/// `attempt` is the attempt that killed it) or a nack after backoff step `step`. See [`edax_event::Verdict`].
pub struct RetryVerdict {
    pub dead: bool,
    pub attempt: u32,
    pub step: u64,
}

fn retry_verdict(verdict: edax_event::Verdict) -> RetryVerdict {
    match verdict {
        edax_event::Verdict::Nack { step } => RetryVerdict { dead: false, attempt: 0, step },
        edax_event::Verdict::Dead { attempt } => RetryVerdict { dead: true, attempt, step: 0 },
    }
}

/// How many messages a [`Retries`] tracks. See [`edax_event::Tracked`].
pub struct RetriesTracked {
    pub attempts: u64,
    pub contended: u64,
}

/// The consumer's retry and dead-letter policy, keyed by message id. Every decision is data the host
/// performs: the handler, the sink, the wait and the ack or nack stay the host's. Construct it with
/// [`retries`]. See [`edax_event::Retries`].
pub struct Retries {
    inner: Arc<Mutex<edax_event::Retries>>,
}

impl Retries {
    /// The attempt a run of `id` is: `max(broker_attempt, recorded) + 1`.
    pub fn attempt(&self, id: &str, broker_attempt: u32) -> u32 {
        edax_core::lock(&self.inner).attempt(id, broker_attempt)
    }

    /// `id` succeeded: forget its counts.
    pub fn succeeded(&self, id: &str) {
        edax_core::lock(&self.inner).succeeded(id);
    }

    /// `id` failed with a failure carrying the taxonomy code `code`, `None` for one no code classifies;
    /// the kernel reads the kind off the code ([`edax_event::Failure::of`]). An unknown code is a
    /// `codec:` error.
    pub fn failed(&self, id: &str, broker_attempt: u32, code: Option<String>) -> Result<RetryVerdict, String> {
        let code = code.map(|c| c.parse::<edax_core::ErrorCode>()).transpose().map_err(wire)?;
        let failure = edax_event::Failure::of(code);
        Ok(retry_verdict(edax_core::lock(&self.inner).failed(id, broker_attempt, failure)))
    }

    /// The sink took the dead message: forget its counts.
    pub fn dead_lettered(&self, id: &str) {
        edax_core::lock(&self.inner).dead_lettered(id);
    }

    /// The sink refused the dead message at `attempt`: keep it, and nack after the returned step.
    pub fn sink_failed(&self, id: &str, attempt: u32) -> u64 {
        edax_core::lock(&self.inner).sink_failed(id, attempt)
    }

    /// How many messages the policy tracks.
    pub fn tracked(&self) -> RetriesTracked {
        let tracked = edax_core::lock(&self.inner).tracked();
        RetriesTracked {
            attempts: u64::try_from(tracked.attempts).unwrap_or(u64::MAX),
            contended: u64::try_from(tracked.contended).unwrap_or(u64::MAX),
        }
    }
}

/// A retry policy that dead-letters a transient failure at its `max_attempts`-th attempt (`0`: only
/// a terminal one dies). `sink` is whether the host has a dead-letter sink: without one nothing dies.
pub fn retries(max_attempts: u32, sink: bool) -> Retries {
    Retries { inner: Arc::new(Mutex::new(edax_event::Retries::new(max_attempts, sink))) }
}

// ---- saga step -------------------------------------------------------------

/// The write a saga step makes: `action` is `"nothing"`, `"save"` or `"complete"`, and `expected` the
/// version a save or complete is conditional on (`0` otherwise). See [`edax_event::SagaWrite`].
pub struct SagaWrite {
    pub action: String,
    pub expected: u64,
}

/// The write for a step whose fold `completes` the instance or not, with or without commands, over a
/// running instance loaded at `version` (`None` when nothing was loaded). A completed instance never
/// gets here: [`saga_admit`] skips it. See [`edax_event::saga_write`].
// `version` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn saga_write(completes: bool, has_commands: bool, version: Option<u64>) -> SagaWrite {
    let loaded = version.map_or(edax_event::Loaded::Absent, |version| edax_event::Loaded::Active { version });
    let (action, expected) = match edax_event::saga_write(loaded, completes, has_commands) {
        edax_event::SagaWrite::Nothing => ("nothing", 0),
        edax_event::SagaWrite::Save { expected } => ("save", expected),
        edax_event::SagaWrite::Complete { expected } => ("complete", expected),
    };
    SagaWrite { action: action.to_owned(), expected }
}

/// Whether an instance that saved the marks `folded` (empty for a new instance), or is `completed`
/// (a tombstone), folds `envelope`: `None` when it does not (a tombstone, or a redelivery of an event
/// it folded: skip it, write nothing), else the marks to save with the step. Marks this function did
/// not write are a `codec:` error. See [`edax_event::saga_admit`].
pub fn saga_admit(completed: bool, folded: &str, envelope: EventEnvelope) -> Result<Option<String>, String> {
    let env = envelope_from_record(envelope);
    Ok(match edax_event::saga_admit(completed, folded, &env).map_err(wire)? {
        edax_event::SagaAdmit::Completed | edax_event::SagaAdmit::Repeat => None,
        edax_event::SagaAdmit::Fold { folded } => Some(folded),
    })
}

/// The id of the `index`-th command a saga step committed at `version`. See
/// [`edax_event::saga_command_id`].
pub fn saga_command_id(correlation_id: &str, version: u64, index: u64) -> String {
    edax_event::saga_command_id(correlation_id, version, index)
}

/// One command in a saga's outbox, crossing structurally like [`EventEnvelope`]: the command is the
/// host codec's nested [`Event`]. See [`edax_event::SagaCommand`].
pub struct SagaCommand {
    pub id: String,
    pub correlation_id: String,
    pub command: Event,
    pub timestamp: i64,
}

fn saga_command_from_record(r: SagaCommand) -> edax_event::SagaCommand {
    edax_event::SagaCommand {
        id: r.id,
        correlation_id: r.correlation_id,
        command: kernel_event(r.command),
        timestamp: r.timestamp,
    }
}

fn record_from_saga_command(c: edax_event::SagaCommand) -> SagaCommand {
    SagaCommand {
        id: c.id,
        correlation_id: c.correlation_id,
        command: record_event(c.command),
        timestamp: c.timestamp,
    }
}

// ---- in-memory stores behind one transaction boundary ---------------------

/// A stored snapshot as the event store holds it.
pub struct SnapshotResult {
    pub type_name: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// What [`MemoryTransactor::claim`] found: `won` with the `handle` that settles the claim (and an empty
/// `prior`), or the `prior` record JSON holding the key (and an empty `handle`). One struct rather than
/// a union, which the boundary cannot carry.
pub struct MemoryClaim {
    pub won: bool,
    pub handle: Vec<u8>,
    pub prior: String,
}

/// A stored read-model document and its version.
pub struct ProjectionRow {
    pub version: u64,
    /// The document as JSON text: always an object.
    pub document_json: String,
}

/// A stored saga instance and its version: running with its state and the kernel's `folded` marks, or
/// `completed` (a tombstone, whose `state_json` is `null` and whose `folded` is empty).
pub struct SagaRow {
    pub version: u64,
    pub completed: bool,
    pub state_json: String,
    pub folded: String,
}

/// JSON text -> a value, refusing text that is not JSON as the `codec:` error it is.
fn parse_json<T: serde::de::DeserializeOwned>(what: &str, text: &str) -> Result<T, String> {
    serde_json::from_str(text).map_err(|e| wire(edax_core::Error::codec(format_args!("{what}: {e}"))))
}

/// A value -> JSON text. Serializing a `serde_json` value cannot fail; the arm keeps the call total.
fn to_json(value: &impl serde::Serialize) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| wire(edax_core::Error::codec(e)))
}

/// A host's `u64` row limit as the store's `usize`, saturating on a target narrower than 64 bits.
fn limit_of(limit: u64) -> usize {
    usize::try_from(limit).unwrap_or(usize::MAX)
}

/// The token that names no transaction: a write under it lands at once and a read under it sees
/// committed state only. The host passes it for a call outside any unit of work.
pub const NO_TX: i64 = edax_memory::NO_TX;

/// [`NO_TX`] as a call, because the generator binds functions and not constants. A host imports this
/// instead of restating the value, so the token stays defined in one place.
pub fn no_tx() -> i64 {
    NO_TX
}

/// The in-memory backend for development and tests: one database holding every seam — the event
/// store (per-aggregate log with optimistic concurrency, one snapshot per aggregate, a transactional
/// outbox), the idempotency store (atomic claim / complete / release over JSON records), the
/// projection store (versioned documents, querying deferred to the kernel) and the saga store
/// (versioned per-correlation state) — behind one transaction boundary. Construct it with
/// [`memory_transactor`].
///
/// The transaction crosses as a token, like the broker's delivery token: [`begin`](Self::begin)
/// hands one out, every store method takes one, [`commit`](Self::commit) applies that transaction's
/// writes on every seam atomically and [`rollback`](Self::rollback) drops them. Reads under a token
/// see the transaction's own staged writes; other tokens and [`NO_TX`] see nothing of it before the
/// commit. The journal itself lives in `edax-memory`, so the host SDK keeps no transaction state
/// of its own: it holds the token across calls and hands it back. The seams are flattened onto this
/// one object because the host's four store classes are views over one database, and one object is
/// the shape the binding can share between them.
///
/// Events cross as structured [`EventEnvelope`] records (no JSON or wire encoding at the boundary);
/// snapshots cross as their raw `data` bytes; idempotency records, documents, saga state and query
/// specs cross as JSON text. `edax-memory` is typed (`u64` versions, JSON values, the kernel's query);
/// the text is this boundary's shape, so the parsing happens here.
pub struct MemoryTransactor {
    inner: Arc<edax_memory::MemoryTransactor>,
}

// Every method is synchronous, the store calls included: the work is in-memory under one mutex, so
// there is nothing to await. An `async fn` here would make the generator spawn each call on a runtime
// thread, where it starts at once and a cancelled await cannot stop it. The host's async store views
// await nothing of the native call, so a cancellation lands before or after it, never inside.
impl MemoryTransactor {
    /// Open a transaction and return its token. Never [`NO_TX`].
    pub fn begin(&self) -> i64 {
        self.inner.begin()
    }

    /// Apply the transaction's writes on every seam atomically and close it. A token that names no
    /// open transaction is refused with a `bad_handle:` (terminal) error.
    pub fn commit(&self, tx: i64) -> Result<(), String> {
        self.inner.commit(tx).map_err(wire)
    }

    /// Drop the transaction's writes and close it. `false` and nothing changes if the token names no
    /// open transaction: there was nothing to roll back.
    pub fn rollback(&self, tx: i64) -> bool {
        self.inner.rollback(tx)
    }

    /// How many transactions are open on this database. A leak diagnostic: a token that is never
    /// committed or rolled back conflicts every key it staged for the process lifetime. `u64` rather
    /// than `usize` because the boundary crosses fixed-width integers only.
    pub fn open_count(&self) -> u64 {
        u64::try_from(self.inner.open_count()).unwrap_or(u64::MAX)
    }

    // -- event store --

    /// Events for `aggregate_id` whose sequence is greater than `after`, as `tx` sees them.
    pub fn load_events(&self, tx: i64, aggregate_id: &str, after: u64) -> Result<Vec<EventEnvelope>, String> {
        let events = self.inner.event_store().load(tx, aggregate_id, after).map_err(wire)?;
        Ok(events.into_iter().map(record_from_envelope).collect())
    }

    /// Append `events` under `tx` and enqueue the same onto the outbox; the stream's last sequence as
    /// `tx` sees it must equal `expected_version`, and the batch must carry sequences
    /// `expected_version + 1, + 2, ...`.
    pub fn append_events(
        &self,
        tx: i64,
        aggregate_id: &str,
        expected_version: u64,
        events: Vec<EventEnvelope>,
    ) -> Result<(), String> {
        let envs = events.into_iter().map(envelope_from_record).collect();
        self.inner.event_store().append(tx, aggregate_id, expected_version, envs).map_err(wire)
    }

    /// The snapshot for `aggregate_id` as `tx` sees it, or `None`.
    pub fn load_snapshot(&self, tx: i64, aggregate_id: &str) -> Result<Option<SnapshotResult>, String> {
        let snapshot = self.inner.event_store().load_snapshot(tx, aggregate_id).map_err(wire)?;
        Ok(snapshot.map(|s| SnapshotResult { type_name: s.type_name, version: s.version, data: s.data }))
    }

    /// Store a snapshot for `aggregate_id` under `tx`.
    pub fn save_snapshot(
        &self,
        tx: i64,
        aggregate_id: &str,
        type_name: &str,
        version: u64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.inner.event_store().save_snapshot(tx, aggregate_id, type_name, version, data).map_err(wire)
    }

    /// Committed outbox events not yet published, capped at `limit`. The relay reads outside any
    /// transaction, so there is no token.
    pub fn unpublished(&self, limit: u64) -> Vec<EventEnvelope> {
        self.inner.event_store().unpublished(limit_of(limit)).into_iter().map(record_from_envelope).collect()
    }

    /// How many committed outbox rows are waiting, without paging them.
    pub fn unpublished_count(&self) -> u64 {
        u64::try_from(self.inner.event_store().unpublished_count()).unwrap_or(u64::MAX)
    }

    /// Mark outbox rows published by envelope id.
    pub fn mark_published(&self, ids: Vec<String>) {
        self.inner.event_store().mark_published(ids);
    }

    // -- idempotency store --

    /// Atomically claim `key` under `tx` with a pending record: `won` with the handle that settles the
    /// claim, or the prior record JSON if the key was already present — the host decodes it to decide
    /// replay, payload mismatch, or that the winner has not completed yet. A key another open
    /// transaction holds reads as its pending record.
    pub fn claim(&self, tx: i64, key: &str, payload_hash: u64) -> Result<MemoryClaim, String> {
        Ok(match self.inner.idempotency_store().claim(tx, key, payload_hash).map_err(wire)? {
            edax_memory::Claimed::Won(handle) => MemoryClaim { won: true, handle, prior: String::new() },
            // The store holds only text a `complete` wrote from a `&str`, so the lossy path never
            // replaces anything.
            edax_memory::Claimed::Prior(bytes) => {
                MemoryClaim { won: false, handle: Vec::new(), prior: String::from_utf8_lossy(&bytes).into_owned() }
            }
        })
    }

    /// Write the terminal record JSON for `key` under `tx`, replacing the claim `handle` names. A key
    /// that no longer holds that claim is refused with a `store:` error and nothing is written.
    pub fn complete(&self, tx: i64, key: &str, handle: Vec<u8>, record_json: &str) -> Result<(), String> {
        self.inner.idempotency_store().complete(tx, key, &handle, record_json.as_bytes()).map_err(wire)
    }

    /// Drop the claim `handle` names on `key` under `tx` (e.g. after a transient failure that should be
    /// retried). A key that no longer holds that claim is left alone.
    pub fn release(&self, tx: i64, key: &str, handle: Vec<u8>) -> Result<(), String> {
        self.inner.idempotency_store().release(tx, key, &handle).map_err(wire)
    }

    /// Lapse whatever `key` holds in committed state, as a lease or a retention TTL running out would:
    /// a test hook for rehearsing a takeover. `false` when nothing was held.
    pub fn expire_claim(&self, key: &str) -> bool {
        self.inner.idempotency_store().expire(key)
    }

    // -- projection store --

    /// The stored document for `id` and its version as `tx` sees it, or `None`.
    pub fn get_projection(&self, tx: i64, id: &str) -> Result<Option<ProjectionRow>, String> {
        let row = self.inner.projection_store().get(tx, id).map_err(wire)?;
        row.map(|row| Ok(ProjectionRow { version: row.version, document_json: to_json(&row.value)? })).transpose()
    }

    /// Persist `document_json` under `tx` at `version + 1` if the stored version still equals
    /// `version`; returns the new version. A document that is not a JSON object is a `codec:` error:
    /// the query injects the row key as its `"id"`, so a scalar or an array could be saved and never
    /// queried.
    pub fn save_projection(&self, tx: i64, id: &str, version: u64, document_json: &str) -> Result<u64, String> {
        let document: edax_memory::Document = parse_json("a projection document is a JSON object", document_json)?;
        self.inner.projection_store().save(tx, id, version, document).map_err(wire)
    }

    /// Run a read query (a JSON spec) over all documents as `tx` sees them; returns the kernel's page
    /// as JSON, `{"rows": [...], "next": cursor|null}`. A malformed spec is a `codec:` error.
    pub fn query_projections(&self, tx: i64, query_json: &str) -> Result<String, String> {
        // The kernel's own reader, so a spec the kernel refuses (an integer past 64 bits) is refused here.
        let query = edax_cqrs::query_spec(query_json.as_bytes()).map_err(wire)?;
        to_json(&self.inner.projection_store().get_many(tx, &query).map_err(wire)?)
    }

    // -- saga store --

    /// The stored instance for `id` as `tx` sees it, or `None`. A completed instance is a tombstone:
    /// `completed` is true and `state_json` is `null`.
    pub fn load_saga(&self, tx: i64, id: &str) -> Result<Option<SagaRow>, String> {
        let row = self.inner.saga_store().load(tx, id).map_err(wire)?;
        row.map(|row| {
            let (completed, state_json, folded) = match row.value {
                edax_memory::SagaRow::Active { state, folded } => (false, to_json(&state)?, folded),
                edax_memory::SagaRow::Completed => (true, "null".to_owned(), String::new()),
            };
            Ok(SagaRow { version: row.version, completed, state_json, folded })
        })
        .transpose()
    }

    /// Persist `state_json`, the `folded` marks ([`saga_admit`]) and the step's `commands` under `tx` at
    /// `version + 1` if the stored version still equals `version` (an absent instance is at version 0);
    /// returns the new version. A completed instance takes no write. State that is not JSON is a
    /// `codec:` error.
    pub fn save_saga(
        &self,
        tx: i64,
        id: &str,
        version: u64,
        state_json: &str,
        folded: &str,
        commands: Vec<SagaCommand>,
    ) -> Result<u64, String> {
        let state: serde_json::Value = parse_json("saga state", state_json)?;
        let commands = commands.into_iter().map(saga_command_from_record).collect();
        self.inner.saga_store().save(tx, id, version, state, folded.to_owned(), commands).map_err(wire)
    }

    /// Complete the instance under `tx`: write its tombstone and the step's `commands` at
    /// `version + 1` under the same rules as [`save_saga`](Self::save_saga).
    pub fn complete_saga(&self, tx: i64, id: &str, version: u64, commands: Vec<SagaCommand>) -> Result<u64, String> {
        let commands = commands.into_iter().map(saga_command_from_record).collect();
        self.inner.saga_store().complete(tx, id, version, commands).map_err(wire)
    }

    /// Committed saga outbox commands not yet sent, oldest commit first, capped at `limit`. The
    /// relay reads outside any transaction, so there is no token.
    pub fn unpublished_saga_commands(&self, limit: u64) -> Vec<SagaCommand> {
        self.inner.saga_store().unpublished(limit_of(limit)).into_iter().map(record_from_saga_command).collect()
    }

    /// Mark saga outbox commands sent by id. Idempotent; an unknown id is a no-op.
    pub fn mark_saga_commands_published(&self, ids: Vec<String>) {
        self.inner.saga_store().mark_published(ids);
    }
}

/// A fresh in-memory database with every seam behind one transaction boundary.
pub fn memory_transactor() -> MemoryTransactor {
    MemoryTransactor { inner: Arc::new(edax_memory::MemoryTransactor::new()) }
}

// ---- in-memory broker --------------------------------------------------

/// One message a non-blocking poll fetched.
pub struct PollResult {
    /// The delivery token that settles this delivery through [`MemoryBroker::ack`] / [`MemoryBroker::nack`].
    pub token: i64,
    /// The partition the message sits in.
    pub partition: i32,
    /// Its offset in that partition.
    pub offset: i64,
    pub envelope: EventEnvelope,
}

/// In-memory broker for development and tests: a partitioned log read by consumer groups, settled
/// through the Kafka kernel's offset tracker, so a nack rewinds and a crash redelivers the way Kafka
/// does. Every method returns at once — the waiting for a message is the host's, so a poll never
/// blocks the host's event loop: the host loops over [`try_poll`](MemoryBroker::try_poll) and parks on
/// its own signal between empty polls. Settlement is by token, not by callback (a closure cannot cross
/// the binding): the host builds its own delivery whose ack / nack / liveness capture the token.
/// Construct it with [`memory_broker`]. See [`edax_memory::MemoryBroker`].
pub struct MemoryBroker {
    inner: Arc<edax_memory::MemoryBroker>,
}

// Synchronous, like the stores: the host's `empty()` is a synchronous predicate, and the peek is
// non-blocking by contract, so there is nothing for the host to await.
impl MemoryBroker {
    /// Append `envelope` to its key's partition; every group reads it.
    pub fn publish(&self, envelope: EventEnvelope) {
        self.inner.publish(envelope_from_record(envelope));
    }

    /// Fetch the next message for `group` as an unsettled delivery, or `None` at once when the group
    /// has fetched everything. A group's first poll starts at the beginning.
    pub fn try_poll(&self, group: &str) -> Result<Option<PollResult>, String> {
        let polled = self.inner.try_poll(group).map_err(wire)?;
        Ok(polled.map(|d| PollResult {
            token: d.token,
            partition: d.partition,
            offset: d.offset,
            envelope: record_from_envelope(d.envelope),
        }))
    }

    /// Settle `token` of `group` as handled. `false` if the token is not live (already settled,
    /// never issued, or retired by a nack or a crash); nothing changes then.
    pub fn ack(&self, group: &str, token: i64) -> bool {
        self.inner.ack(group, token)
    }

    /// Settle `token` of `group` as failed: its partition rewinds to the lowest unsettled offset and
    /// every delivery of it already handed out is retired. `false` if the token is not live.
    pub fn nack(&self, group: &str, token: i64) -> bool {
        self.inner.nack(group, token)
    }

    /// Whether `token` of `group` still settles its delivery; a dispatcher skips one that does not.
    pub fn is_live(&self, group: &str, token: i64) -> bool {
        self.inner.is_live(group, token)
    }

    /// Simulate the death of `group`'s consumer, or a rebalance: every delivery it holds is retired,
    /// and every message above its committed watermark is fetched again.
    pub fn crash(&self, group: &str) {
        self.inner.crash(group);
    }

    /// True only when `group` has committed past every message in the log.
    pub fn is_empty(&self, group: &str) -> bool {
        self.inner.is_empty(group)
    }
}

/// A fresh in-memory broker with `partitions` partitions; a key lands on Kafka's partition for it.
/// `0` is a `codec:` error.
pub fn memory_broker(partitions: u32) -> Result<MemoryBroker, String> {
    Ok(MemoryBroker { inner: Arc::new(edax_memory::MemoryBroker::with_partitions(partitions).map_err(wire)?) })
}

// ---- kafka ---------------------------------------------------------------

/// One Kafka record header. The value crosses as bytes and not text: a foreign header on the same
/// topic may carry anything, and the boundary hands it back unchanged rather than lose it. See
/// [`edax_kafka::KafkaHeader`].
pub struct KafkaHeader {
    /// The header name, as the client hands it over.
    pub key: String,
    /// The header value, uninterpreted.
    pub value: Vec<u8>,
}

/// One Kafka record, minus the client's own coordinates (topic, partition, offset), which stay with
/// the host's IO shell. See [`edax_kafka::KafkaRecord`].
pub struct KafkaRecord {
    /// The partitioning key: the aggregate id, or the host's override. UTF-8 bytes.
    pub key: Vec<u8>,
    /// The encoded envelope.
    pub value: Vec<u8>,
    /// The contract headers, in the order the kernel writes them.
    pub headers: Vec<KafkaHeader>,
}

/// Binding headers -> the kernel's.
fn kernel_headers(headers: Vec<KafkaHeader>) -> Vec<edax_kafka::KafkaHeader> {
    headers
        .into_iter()
        .map(|header| edax_kafka::KafkaHeader { key: header.key, value: header.value })
        .collect()
}

/// The kernel's record -> a binding record, headers included.
fn record_from_kafka_record(record: edax_kafka::KafkaRecord) -> KafkaRecord {
    KafkaRecord {
        key: record.key,
        value: record.value,
        headers: record
            .headers
            .into_iter()
            .map(|header| KafkaHeader { key: header.key, value: header.value })
            .collect(),
    }
}

/// Build the record for `envelope` from value bytes the host already encoded; `content_type` names
/// the format of `value` and `key` overrides the partitioning key. See [`edax_kafka::record()`].
// `key` is optional, so it trails `content_type` — alef moves optional parameters to the end of the
// generated signature, and the surface order must match for the generated call to line up.
pub fn kafka_record(
    envelope: EventEnvelope,
    value: Vec<u8>,
    content_type: &str,
    key: Option<String>,
) -> KafkaRecord {
    let env = envelope_from_record(envelope);
    record_from_kafka_record(edax_kafka::record(&env, key.as_deref(), value, content_type))
}

/// Where a fetched record goes by its `content-type`: flattened from [`edax_kafka::Route`]. Neither
/// field set means the default serializer; `registered` names the index of the media type in the
/// list the host passed; `unknown` carries a readable media type nobody registered, the empty one
/// included, which is poison.
pub struct KafkaRoute {
    /// The index of the matching media type in the host's registered list.
    pub registered: Option<u32>,
    /// The unregistered media type the record names.
    pub unknown: Option<String>,
}

/// Route a fetched record by `headers` against the media types the host registered, in its order.
/// A null header value must cross as empty bytes, which routes as unknown. See [`edax_kafka::route`].
pub fn kafka_route(headers: Vec<KafkaHeader>, registered: Vec<String>) -> KafkaRoute {
    let headers = kernel_headers(headers);
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    match edax_kafka::route(&headers, &registered) {
        edax_kafka::Route::Default => KafkaRoute { registered: None, unknown: None },
        edax_kafka::Route::Registered(index) => {
            // An index into a list the host passed across the binding fits a `u32` by construction.
            KafkaRoute { registered: Some(u32::try_from(index).unwrap_or(u32::MAX)), unknown: None }
        }
        edax_kafka::Route::Unknown(media_type) => KafkaRoute { registered: None, unknown: Some(media_type) },
    }
}

/// Refuse a serializer registry that names one media type twice, as an `already_registered:` error.
/// A host calls it once, on the list it later passes to [`kafka_route`]. See
/// [`edax_kafka::check_media_types`].
pub fn kafka_check_media_types(registered: Vec<String>) -> Result<(), String> {
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    edax_kafka::check_media_types(&registered).map_err(wire)
}

/// The record a dead-letter sink publishes for `record`, fetched from `topic`/`partition`/`offset`:
/// key, value and headers verbatim, plus the four `edax-dlq-*` headers. See
/// [`edax_kafka::dead_letter_record`].
pub fn kafka_dead_letter_record(record: KafkaRecord, topic: &str, partition: i32, offset: i64, error: &str) -> KafkaRecord {
    let kernel = edax_kafka::KafkaRecord { key: record.key, value: record.value, headers: kernel_headers(record.headers) };
    record_from_kafka_record(edax_kafka::dead_letter_record(&kernel, topic, partition, offset, error))
}

/// The header that names a record value's media type, `"content-type"`: the one contract header, so a
/// record middleware that rewrites the value rewrites this header by the kernel's name. See
/// [`edax_kafka::CONTENT_TYPE`].
pub fn kafka_content_type_header() -> String {
    edax_kafka::CONTENT_TYPE.to_owned()
}

/// The partition a record keyed `key` lands on in a topic of `partitions` partitions: Java's default
/// partitioner, `(murmur2(key) & 0x7fffffff) % partitions`, so an edax producer places a key where a
/// Java producer, Kafka Connect or aiokafka would. Pass the result as the producer's partition
/// instead of leaving placement to the client library. `0` partitions is a `codec:` error. See
/// [`edax_kafka::partition_for`].
pub fn kafka_partition_for(key: &[u8], partitions: u32) -> Result<u32, String> {
    edax_kafka::partition_for(key, partitions).map_err(wire)
}

/// The identity of a message that did not decode: `"{topic}/{partition}/{offset}"`. See
/// [`edax_kafka::poison_id`].
pub fn kafka_poison_id(topic: &str, partition: i32, offset: i64) -> String {
    edax_kafka::poison_id(topic, partition, offset)
}

/// What [`OffsetTracker::nack`] tells the host to do. See [`edax_kafka::NackDecision`].
pub struct NackDecision {
    /// The offset to seek the partition back to — the lowest still in flight — or `None` when the
    /// token is stale or unknown and there is nothing to do.
    pub seek: Option<i64>,
}

/// One partition's watermark that is due for a commit. See [`edax_kafka::Due`].
pub struct KafkaDue {
    pub topic: String,
    pub partition: i32,
    /// The watermark to commit.
    pub offset: i64,
}

/// One partition's bookkeeping, for a health check, a gauge or a rebalance drain.
pub struct PartitionStats {
    /// The topic this partition belongs to.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
    /// Offsets delivered but not yet settled, retired ones included.
    pub unsettled: u64,
    /// Unsettled offsets whose token is still live: work a handler may still settle. A rebalance
    /// drain waits for this to reach zero.
    pub live: u64,
    /// The highest offset booked.
    pub max_delivered: i64,
    /// The last watermark the host reported committed, or `None` before the first commit.
    pub committed: Option<i64>,
}

/// The consumer's offset bookkeeping: one commit watermark per partition, settled by token. Every
/// decision is data the host performs, never a callback across the boundary. Construct it with
/// [`offset_tracker`]. See [`edax_kafka::OffsetTracker`].
pub struct OffsetTracker {
    inner: Arc<edax_kafka::OffsetTracker>,
}

// Synchronous, like [`MemoryBroker`]: the bookkeeping is in-memory and the IO — the commit and the
// seek — is the host's, so there is nothing here for the host to await.
impl OffsetTracker {
    /// Book a fetched offset as in flight and return the token that settles it.
    ///
    /// Call this before decoding: a payload the host cannot decode still has to be settled, and an
    /// unbooked offset is either committed past by the next ack or replayed into the same failure
    /// forever. Booking an offset already in flight takes a fresh token and retires the old one, so a
    /// redelivered copy settles once.
    /// A negative offset is a `codec:` error.
    pub fn book(&self, topic: &str, partition: i32, offset: i64) -> Result<i64, String> {
        self.inner.book(topic, partition, offset).map_err(wire)
    }

    /// Whether `token` still settles its delivery; a dispatcher skips a delivery whose token a nack
    /// or a reset retired.
    pub fn is_live(&self, token: i64) -> bool {
        self.inner.is_live(token)
    }

    /// Settle `token` as handled: `true` when it settled, `false` when it was stale or unknown. What to
    /// commit is [`due`](Self::due)'s answer.
    pub fn ack(&self, token: i64) -> bool {
        match self.inner.ack(token) {
            edax_kafka::AckDecision::Settled => true,
            edax_kafka::AckDecision::Stale => false,
        }
    }

    /// Every partition whose watermark moved past its last commit, ordered by topic then partition:
    /// what the host commits in one call, then reports back with [`committed`](Self::committed).
    pub fn due(&self) -> Vec<KafkaDue> {
        self.inner
            .due()
            .into_iter()
            .map(|due| KafkaDue { topic: due.topic, partition: due.partition, offset: due.offset })
            .collect()
    }

    /// Record that the client committed `offset` on this partition.
    pub fn committed(&self, topic: &str, partition: i32, offset: i64) {
        self.inner.committed(topic, partition, offset);
    }

    /// Settle `token` as failed: rewind its partition, retire every token of it, and report the
    /// offset the host should seek the partition to.
    pub fn nack(&self, token: i64) -> NackDecision {
        match self.inner.nack(token) {
            edax_kafka::NackDecision::Seek { offset, .. } => NackDecision { seek: Some(offset) },
            edax_kafka::NackDecision::Stale => NackDecision { seek: None },
        }
    }

    /// Drop a revoked partition's bookkeeping and retire its tokens. A partition that is not
    /// assigned is a no-op, so a rebalance listener calls this for every revoked partition blindly.
    pub fn reset(&self, topic: &str, partition: i32) {
        self.inner.reset(topic, partition);
    }

    /// A snapshot of every assigned partition, ordered by topic then partition.
    pub fn stats(&self) -> Vec<PartitionStats> {
        self.inner
            .stats()
            .into_iter()
            .map(|row| PartitionStats {
                topic: row.topic,
                partition: row.partition,
                unsettled: row.unsettled,
                live: row.live,
                max_delivered: row.max_delivered,
                committed: row.committed,
            })
            .collect()
    }
}

/// A fresh offset tracker with nothing booked. One tracker serves every partition a consumer is
/// assigned.
pub fn offset_tracker() -> OffsetTracker {
    OffsetTracker { inner: Arc::new(edax_kafka::OffsetTracker::new()) }
}


// ---- redis ---------------------------------------------------------------

/// A fresh claim's value for `payload_hash` under a new owner token, minted by the kernel and never
/// by the host. The value is also the claim's handle: pass it to the complete, release and extend
/// scripts. Non-deterministic. See [`edax_redis::new_claim`].
pub fn redis_new_claim(payload_hash: u64) -> Result<Vec<u8>, String> {
    edax_redis::new_claim(payload_hash).map_err(wire)
}

/// The value that settles the claim `claim`, over `record_json` as the host encoded it (with
/// [`encode_idempotency_record`]). A handle the kernel never minted is a `codec:` error. See
/// [`edax_redis::settled_value`].
pub fn redis_settled_value(claim: &[u8], record_json: &str) -> Result<Vec<u8>, String> {
    edax_redis::settled_value(claim, record_json).map_err(wire)
}

/// Refuse a claim handle the kernel never minted, as a `codec:` error. See [`edax_redis::check_claim`].
pub fn redis_check_claim(claim: &[u8]) -> Result<(), String> {
    edax_redis::check_claim(claim).map_err(wire)
}

/// The Redis key for an idempotency key: `{prefix}{key}` as bytes. An empty prefix is a
/// `bad_handle:` error. See [`edax_redis::ledger_key`].
pub fn redis_ledger_key(prefix: &str, key: &str) -> Result<Vec<u8>, String> {
    edax_redis::ledger_key(prefix, key).map_err(wire)
}

/// Refuse the settings a ledger store is built with, as a `bad_handle:` error naming the first one
/// refused. See [`edax_redis::check_config`].
pub fn redis_check_config(
    prefix: &str,
    ttl_ms: i64,
    claim_ttl_ms: i64,
    op_timeout_ms: i64,
    heartbeat: bool,
) -> Result<(), String> {
    edax_redis::check_config(prefix, ttl_ms, claim_ttl_ms, op_timeout_ms, heartbeat).map_err(wire)
}

/// How many times per claim TTL a heartbeat renews a held claim. A call because the generator binds
/// functions, not constants. See [`edax_redis::HEARTBEATS_PER_TTL`].
pub fn redis_heartbeats_per_ttl() -> i64 {
    edax_redis::HEARTBEATS_PER_TTL
}

/// What the claim script's `reply` to the caller's `claim` means: `None` when this caller holds the
/// claim (the key was free, or the reply is the caller's own claim, re-sent after a lost reply), else
/// the record JSON that holds the key, for [`decode_prior_idempotency_record`]. A value no layout reads
/// comes back as the kernel's unreadable record. `claim` comes first because the reply is optional. See
/// [`edax_redis::claim_outcome`].
pub fn redis_claim_outcome(claim: &[u8], reply: Option<Vec<u8>>) -> Result<Option<String>, String> {
    match edax_redis::claim_outcome(reply.as_deref(), claim).map_err(wire)? {
        edax_redis::ClaimOutcome::Won => Ok(None),
        edax_redis::ClaimOutcome::Prior(record) => Ok(Some(record)),
    }
}

/// The kernel error for a Redis error reply, prefix included, in its wire form
/// `"<code>: <message>"`. See [`edax_redis::reply_error`].
pub fn redis_reply_error(reply: &str) -> String {
    wire(edax_redis::reply_error(reply))
}

/// The four kernel scripts, byte for byte, so every SDK's `EVALSHA` names one digest. A struct
/// rather than four calls, like [`SplitError`]. See [`edax_redis::CLAIM_SCRIPT`] and its siblings.
pub struct RedisScripts {
    pub claim: String,
    pub complete: String,
    pub release: String,
    pub extend: String,
}

/// The four kernel scripts. See [`RedisScripts`].
pub fn redis_scripts() -> RedisScripts {
    RedisScripts {
        claim: edax_redis::CLAIM_SCRIPT.to_owned(),
        complete: edax_redis::COMPLETE_SCRIPT.to_owned(),
        release: edax_redis::RELEASE_SCRIPT.to_owned(),
        extend: edax_redis::EXTEND_SCRIPT.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// One realistic envelope record; every case overrides only the field under test.
    fn envelope(id: &str, sequence: Option<u64>) -> EventEnvelope {
        EventEnvelope {
            id: id.to_owned(),
            aggregate_id: "invoice-7".to_owned(),
            event: Event {
                name: "InvoiceIssued".to_owned(),
                version: 1,
                data: br#"{"invoice_id":"invoice-7","total":5000,"currency":"EUR"}"#.to_vec(),
            },
            sequence,
            namespace: HashMap::from([("tenant".to_owned(), "acme".to_owned())]),
            metadata: HashMap::from([
                ("correlation_id".to_owned(), "corr-1".to_owned()),
                ("causation_id".to_owned(), "evt-0".to_owned()),
            ]),
            timestamp: 1_700_000_000_000_000_000,
        }
    }

    /// Field-by-field envelope equality. The record is a binding type with no `PartialEq` (alef owns
    /// the derives on the generated twin), and a whole-record assert is what proves nothing is dropped
    /// at the boundary.
    fn assert_same_envelope(got: &EventEnvelope, want: &EventEnvelope, case: &str) {
        assert_eq!((&got.id, &got.aggregate_id, got.sequence, got.timestamp), (&want.id, &want.aggregate_id, want.sequence, want.timestamp), "{case}");
        assert_eq!((&got.event.name, got.event.version, &got.event.data), (&want.event.name, want.event.version, &want.event.data), "{case}");
        assert_eq!((&got.namespace, &got.metadata), (&want.namespace, &want.metadata), "{case}");
    }

    // ---- routing / sequencing ----------------------------------------------

    #[test]
    fn routing_and_sequencing_forward_their_arguments_in_order() {
        // A transposed argument is the one way a forwarding surface can drift from its kernel, so
        // every case is asymmetric: swapping the two arguments changes the answer.
        assert_eq!(partition("invoice-7", 256), edax_event::partition("invoice-7", 256));
        assert!(partition("invoice-7", 8) < 8);
        assert_eq!(partition("invoice-7", 0), 0, "n is clamped to at least one partition");

        // (stored, incoming): each swap changes the answer.
        assert!(!is_stale(Some(4), Some(5)), "newer than stored -> apply");
        assert!(is_stale(Some(5), Some(4)), "older than stored -> skip");
        assert!(!is_stale(None, Some(4)), "nothing stored -> apply");
        assert!(!is_stale(Some(5), None), "unsequenced -> apply");
        assert_eq!(advance(Some(4), Some(5)), Some(5));
        assert_eq!(advance(Some(5), Some(4)), Some(5), "a stale sequence never lowers the mark");
        assert_eq!(advance(Some(5), None), Some(5), "an unsequenced event never erases the mark");
        assert_eq!(advance(None, Some(4)), Some(4));
        assert_eq!((correlation_id_key(), causation_id_key()), ("correlation_id".to_owned(), "causation_id".to_owned()));

        // `(base, cap, attempt, entropy)` in that order: each swap changes the answer.
        let policy = default_backoff();
        assert_eq!((policy.base_millis, policy.cap_millis), (100, 30_000), "the kernel's default");
        assert_eq!(backoff_millis(100, 30_000, 0, 100), 100, "entropy at the first ceiling");
        assert_eq!(backoff_millis(100, 30_000, 1, 150), 150, "the second ceiling is 200");
        assert_eq!(backoff_millis(100, 30_000, 1_000, u64::MAX), u64::MAX % 30_001, "capped, no overflow");
        assert_eq!(backoff_millis(100, 50, 0, 77), 77 % 51, "the cap bounds the base");
    }

    #[test]
    fn error_taxonomy_is_the_kernels() {
        // The list and every verdict are the kernel's own, in its order: a binding that builds its
        // exception table from these two calls cannot restate the taxonomy. One row per code.
        let codes = error_codes();
        assert_eq!(codes, ErrorCode::ALL.iter().map(|c| c.as_str().to_owned()).collect::<Vec<_>>());
        for code in ErrorCode::ALL {
            assert_eq!(error_code_is_terminal(code.as_str()), Ok(code.is_terminal()), "{code}");
        }
        // An unknown code is refused with the kernel's terminal `codec:` error: the same name fails the
        // same way every time, so a host must not read it as a transient store failure.
        assert_eq!(error_code_is_terminal("timeout"), Err("codec: unknown error code: timeout".to_owned()));
        assert_eq!(error_code_is_terminal(""), Err("codec: unknown error code: ".to_owned()));
        // The default for a failure with no code the kernel knows is the kernel's too.
        assert!(!error_verdict("timeout: deadline exceeded"), "unclassified is transient");
        assert!(error_verdict("payload_mismatch: acct-1-3"));
    }

    #[test]
    fn split_error_branches() {
        struct Case {
            name: &'static str,
            wire: &'static str,
            want: (&'static str, &'static str, bool),
        }
        let cases = [
            Case { name: "known_code", wire: "codec: bad json", want: ("codec", "bad json", true) },
            Case {
                name: "separator_inside_the_message",
                wire: "store: redis: connection reset",
                want: ("store", "redis: connection reset", false),
            },
            Case { name: "empty_message", wire: "concurrency: ", want: ("concurrency", "", false) },
            // No code the kernel knows: a store failure carrying the whole string, so the class and the
            // verdict are the kernel's default rather than the host's.
            Case { name: "unknown_code", wire: "timeout: deadline exceeded", want: ("store", "timeout: deadline exceeded", false) },
            Case { name: "no_separator", wire: "connection reset", want: ("store", "connection reset", false) },
            Case { name: "empty", wire: "", want: ("store", "", false) },
        ];
        for c in cases {
            let got = split_error(c.wire);
            assert_eq!((got.code.as_str(), got.message.as_str(), got.terminal), c.want, "{}", c.name);
        }
    }

    #[test]
    fn clock_readers_are_unique_and_ordered() {
        assert_ne!(guid(), guid(), "ids are unique");
        assert!(current_timestamp() < current_timestamp(), "timestamps strictly increase");
    }

    #[test]
    fn propagate_maps_metadata_both_ways() {
        // Parent with a chain: the child inherits the correlation id and points causation at the parent.
        let parent = HashMap::from([
            ("correlation_id".to_owned(), "corr-1".to_owned()),
            ("tenant".to_owned(), "acme".to_owned()),
        ]);
        let child = propagate(parent, "evt-1");
        assert_eq!(
            child,
            HashMap::from([
                ("correlation_id".to_owned(), "corr-1".to_owned()),
                ("causation_id".to_owned(), "evt-1".to_owned()),
            ]),
            "only the two chain keys cross back, unrelated parent metadata is dropped"
        );

        // Root event: its own id seeds the chain.
        let root = propagate(HashMap::new(), "evt-root");
        assert_eq!(root["correlation_id"], "evt-root");
        assert_eq!(root["causation_id"], "evt-root");
    }

    #[test]
    fn originate_maps_metadata_both_ways() {
        // Nothing given: the host's fresh id seeds both keys, and unrelated metadata is dropped.
        let seeded = originate(HashMap::from([("tenant".to_owned(), "acme".to_owned())]), "fresh-1");
        assert_eq!(
            seeded,
            HashMap::from([
                ("correlation_id".to_owned(), "fresh-1".to_owned()),
                ("causation_id".to_owned(), "fresh-1".to_owned()),
            ]),
            "only the two chain keys cross back"
        );

        // Supplied ids survive the round trip, so the fresh id goes unused.
        let given = originate(
            HashMap::from([
                ("correlation_id".to_owned(), "corr-1".to_owned()),
                ("causation_id".to_owned(), "evt-0".to_owned()),
            ]),
            "fresh-1",
        );
        assert_eq!(given["correlation_id"], "corr-1");
        assert_eq!(given["causation_id"], "evt-0");
    }

    // ---- filter / query ----------------------------------------------------

    #[test]
    fn filter_and_query_defer_to_the_kernel() {
        let doc = r#"{"id":"user-1","email":"ada@example.com","age":36,"tags":["eu","paid"]}"#;
        assert!(eval_filter(r#"{"compare":{"path":"age","op":"gte","value":36}}"#, doc).unwrap());
        assert!(!eval_filter(r#"{"compare":{"path":"age","op":"gt","value":36}}"#, doc).unwrap());
        assert!(!eval_filter(r#"{"compare":{"path":"deleted_at","op":"ne","value":"x"}}"#, doc).unwrap(),
            "a missing path makes the leaf false — the kernel's two-valued rule");
        assert!(eval_filter(r#"{"compare":{"path":"deleted_at","op":"ne","value":null}}"#, doc).unwrap_err().starts_with("codec: a null comparand"),
            "a null comparand is refused, with the kernel's pointer to `exists`");

        let docs = r#"[{"id":"user-1","age":36},{"id":"user-2","age":29},{"id":"user-3","age":41}]"#;
        let page: Value = serde_json::from_str(&query(r#"{"sort":[{"path":"age"}],"limit":2}"#, docs).unwrap()).unwrap();
        let rows = |page: &Value| page["rows"].as_array().unwrap().iter().map(|d| d["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
        assert_eq!(rows(&page), ["user-2", "user-1"]);

        // The page's own `next` continues it with no gap or repeat, and is the cursor a backend mints.
        let cursor = page["next"].as_str().unwrap();
        assert_eq!(encode_cursor(r#"{"id":"user-1","age":36}"#, r#"[{"path":"age"}]"#).unwrap(), cursor);
        assert_eq!(decode_cursor(cursor, r#"[{"path":"age"}]"#).unwrap(), r#"[36,"user-1"]"#);
        let spec = json!({"sort": [{"path": "age"}], "limit": 2, "after": cursor}).to_string();
        let next: Value = serde_json::from_str(&query(&spec, docs).unwrap()).unwrap();
        assert_eq!(rows(&next), ["user-3"]);
        assert!(next["next"].is_null(), "nothing follows the last page");
    }

    // ---- upcast ------------------------------------------------------------

    #[test]
    fn upcast_plan_branches() {
        assert_eq!(upcast_plan(1, 3, Some(vec![1, 2, 9])).unwrap(), vec![1, 2], "extra steps are ignored");
        assert_eq!(upcast_plan(3, 3, Some(vec![])).unwrap(), Vec::<i32>::new(), "already current");
        assert_eq!(upcast_plan(1, 3, None).unwrap(), Vec::<i32>::new(), "no transforms: identity");
        // Both failures are deterministic, so both must carry the terminal `codec` code.
        assert_eq!(
            upcast_plan(1, 3, Some(vec![1])).unwrap_err(),
            "codec: missing upcast step 2 (no transform for v2 -> v3)"
        );
        for available in [Some(vec![1, 2]), None] {
            assert_eq!(
                upcast_plan(3, 1, available).unwrap_err(),
                "codec: cannot upcast downward: from 3 > to 1 (the payload comes from a newer writer)"
            );
        }
    }

    #[test]
    fn retries_cross_their_decisions_structurally() {
        let code = |c: &str| Some(c.to_owned());
        let policy = retries(2, true);
        let nack = policy.failed("evt-1", 0, code("store")).unwrap();
        assert_eq!((nack.dead, nack.step), (false, 0));
        let dead = policy.failed("evt-1", 0, None).unwrap();
        assert_eq!((dead.dead, dead.attempt), (true, 2), "an unclassified failure is transient and counts");
        assert_eq!(policy.sink_failed("evt-1", 2), 1);
        assert_eq!(policy.attempt("evt-1", 0), 3);
        policy.dead_lettered("evt-1");
        let contended = policy.failed("evt-2", 0, code("in_progress")).unwrap();
        assert_eq!((contended.dead, contended.step), (false, 0));
        let tracked = policy.tracked();
        assert_eq!((tracked.attempts, tracked.contended), (0, 1));
        policy.succeeded("evt-2");
        assert_eq!(policy.tracked().contended, 0);
        assert_eq!(policy.failed("evt-1", 0, code("poison")).err().as_deref(), Some("codec: unknown error code: poison"));
        let terminal = retries(2, false).failed("evt-3", 0, code("codec")).unwrap();
        assert_eq!((terminal.dead, terminal.step), (false, 0), "no sink: a terminal failure nacks");
    }

    #[test]
    fn saga_admit_crosses_the_marks_as_text() {
        let env = |id: &str| envelope(id, Some(1));
        let folded = saga_admit(false, "", env("evt-1")).unwrap().expect("a new instance folds");
        assert_eq!(saga_admit(false, &folded, env("evt-1")).unwrap(), None, "a redelivery is skipped");
        assert_eq!(saga_admit(true, &folded, env("evt-2")).unwrap(), None, "a tombstone folds nothing");
        assert!(saga_admit(false, &folded, env("evt-2")).unwrap().is_some(), "the next event folds");
        assert!(saga_admit(false, "[1]", env("evt-1")).unwrap_err().starts_with("codec: saga folded marks"));
    }

    #[test]
    fn idempotency_settle_crosses_the_code_to_cache() {
        assert_eq!(idempotency_settle(None), Ok(None));
        assert_eq!(idempotency_settle(Some("codec".to_owned())), Ok(Some("codec".to_owned())));
        assert_eq!(idempotency_settle(Some("store".to_owned())), Ok(None));
        assert_eq!(idempotency_settle(Some("timeout".to_owned())).err().as_deref(), Some("codec: unknown error code: timeout"));
    }

    #[test]
    fn saga_write_crosses_every_action() {
        let got = |version, completes, cmds| {
            let write = saga_write(completes, cmds, version);
            (write.action, write.expected)
        };
        assert_eq!(got(None, true, false), ("nothing".to_owned(), 0));
        assert_eq!(got(None, false, false), ("save".to_owned(), 0));
        assert_eq!(got(Some(3), false, false), ("save".to_owned(), 3));
        assert_eq!(got(Some(3), true, true), ("complete".to_owned(), 3));
        assert_eq!(saga_command_id("order-7", 2, 1), "order-7:2:1");
    }

    // ---- idempotency -------------------------------------------------------

    #[test]
    fn payload_hash_binds_the_canonical_form() {
        assert_eq!(payload_hash(b"user.create"), edax_core::fnv1a64(b"user.create"));

        // Key order, whitespace and number spelling do not change the hash. Canonicalization is the
        // kernel's, so every binding agrees — golden/canonical_json.json pins the bytes.
        let a = payload_hash_json(br#"{"email":"ada@example.com","age":36}"#).unwrap();
        let b = payload_hash_json(br#"{ "age": 36.0, "email": "ada@example.com" }"#).unwrap();
        assert_eq!(a, b);
        assert_eq!(canonical_json(br#"{ "n": 2.0, "a": "\u00e9" }"#).as_deref(), Ok(r#"{"a":"é","n":2}"#));
        assert_eq!(payload_hash_json(canonical_json(b"[2.0]").unwrap().as_bytes()), payload_hash_json(b"[2]"));
        assert!(a > (1u64 << 53), "hashes exceed float precision, so they must cross as integers");

        // The event guard's key and hash: the envelope id, and the raw bytes of the body.
        let rule = event_idempotency(envelope("evt-1", Some(1)));
        assert_eq!(rule.key, "evt-1");
        assert_eq!(rule.payload_hash, payload_hash(&envelope("evt-1", Some(1)).event.data));
    }

    // ---- idempotency record ------------------------------------------------

    /// A real 64-bit payload hash, past 2^53 so a lossy (float) reader would be caught.
    const HASH: u64 = 11_234_062_831_410_934_249;
    /// One cached handler result; every case overrides only the field under test.
    const RESULT: &str = r#"{"invoice_id":"invoice-7","total":5000}"#;

    #[test]
    fn idempotency_record_branches() {
        struct Case {
            name: &'static str,
            pending: bool,
            result_json: &'static str,
            error: Option<&'static str>,
            error_code: Option<&'static str>,
            /// The canonical record bytes — the cross-language contract, pinned by
            /// `testdata/golden/idempotency_record.json`.
            encoded: &'static str,
            state: &'static str,
            /// What `decode_idempotency_record` reads back into the flat `(error, error_code, result_json)`.
            decoded: (&'static str, &'static str, &'static str),
            /// The verdict a losing claimant reaches from the record.
            outcome: &'static str,
        }
        let cases = [
            Case {
                name: "pending_claim",
                pending: true,
                result_json: RESULT,
                error: None,
                error_code: None,
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":true}"#,
                state: "pending",
                decoded: ("", "", ""),
                outcome: "in_progress",
            },
            Case {
                name: "cached_success",
                pending: false,
                result_json: RESULT,
                error: None,
                error_code: None,
                encoded: concat!(
                    r#"{"payload_hash":11234062831410934249,"result":{"invoice_id":"invoice-7","#,
                    r#""total":5000},"error":null,"error_code":null,"pending":false}"#
                ),
                state: "done",
                decoded: ("", "", RESULT),
                outcome: "succeeded",
            },
            Case {
                // A completed `null` is a success whose value is null, not "no result" and not an
                // in-flight claim.
                name: "cached_success_with_a_null_result",
                pending: false,
                result_json: "null",
                error: None,
                error_code: None,
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":false}"#,
                state: "done",
                decoded: ("", "", "null"),
                outcome: "succeeded",
            },
            Case {
                name: "cached_failure",
                pending: false,
                result_json: RESULT,
                error: Some("bad payload"),
                error_code: Some("codec"),
                encoded: concat!(
                    r#"{"payload_hash":11234062831410934249,"result":null,"#,
                    r#""error":"bad payload","error_code":"codec","pending":false}"#
                ),
                state: "failed",
                decoded: ("bad payload", "codec", ""),
                outcome: "failed",
            },
            Case {
                // Reading order, first rule: pending wins, so a stale error cannot mask an
                // in-flight claim into a terminal one.
                name: "pending_wins_over_an_error",
                pending: true,
                result_json: RESULT,
                error: Some("boom"),
                error_code: Some("rejected"),
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"error_code":null,"pending":true}"#,
                state: "pending",
                decoded: ("", "", ""),
                outcome: "in_progress",
            },
            Case {
                // Second rule: error wins over a result, so a half-written record never replays as
                // a success.
                name: "error_wins_over_a_result",
                pending: false,
                result_json: RESULT,
                error: Some("boom"),
                error_code: Some("rejected"),
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","error_code":"rejected","pending":false}"#,
                state: "failed",
                decoded: ("boom", "rejected", ""),
                outcome: "failed",
            },
        ];

        for case in cases {
            let owned_error = || case.error.map(str::to_owned);
            let encoded =
                encode_idempotency_record(HASH, case.pending, case.result_json, owned_error(), case.error_code.map(str::to_owned));
            assert_eq!(encoded.as_deref(), Ok(case.encoded), "{}", case.name);

            let record = decode_idempotency_record(case.encoded).expect(case.name);
            assert_eq!(record.payload_hash, HASH, "{} hash", case.name);
            assert_eq!(record.state, case.state, "{} state", case.name);
            assert_eq!(
                (record.error.as_str(), record.error_code.as_str(), record.result_json.as_str()),
                case.decoded,
                "{} fields",
                case.name
            );

            // The verdict reads only the classifying fields, and the hash is checked before them:
            // a key reused with another command mismatches whatever state the winner is in.
            let verdict = idempotency_outcome(HASH, record.payload_hash, case.pending, owned_error());
            assert_eq!(verdict, case.outcome, "{} outcome", case.name);
            let reused = idempotency_outcome(HASH ^ 1, record.payload_hash, case.pending, owned_error());
            assert_eq!(reused, "payload_mismatch", "{} reused key", case.name);
        }
    }

    #[test]
    fn a_cached_failure_replays_with_its_original_code() {
        for (name, error, code, want) in [
            ("known_code_is_kept_in_the_wire_form", "bad payload", Some("codec"), "codec: bad payload"),
            ("no_code_replays_the_message", "boom", None, "boom"),
            ("unknown_code_reads_as_no_code", "boom", Some("quota_exceeded"), "boom"),
        ] {
            assert_eq!(cached_failure_message(error, code.map(str::to_owned)), want, "{name}");
        }
    }

    #[test]
    fn a_prior_record_never_fails_to_decode() {
        let flat = |r: IdempotencyRecord| (r.payload_hash, r.state, r.error, r.error_code, r.result_json);
        let readable = r#"{"payload_hash":5,"result":{"n":1},"error":null,"pending":false}"#;
        assert_eq!(flat(decode_prior_idempotency_record(readable, 9)), flat(decode_idempotency_record(readable).unwrap()));
        let unreadable = |hash: u64, error: &str| (hash, "failed".to_owned(), error.to_owned(), "codec".to_owned(), String::new());
        assert_eq!(
            flat(decode_prior_idempotency_record("{", 9)),
            unreadable(9, "unreadable idempotency record: EOF while parsing an object at line 1 column 1"),
            "garbage carries the claimant's hash"
        );
        assert_eq!(
            flat(decode_prior_idempotency_record(r#"{"payload_hash":5,"error":7}"#, 9)),
            unreadable(5, "unreadable idempotency record: invalid type: integer `7`, expected a string"),
            "a readable hash is kept"
        );
        assert_eq!(
            flat(unreadable_idempotency_record(5, "does not convert to Invoice")),
            unreadable(5, "unreadable idempotency record: does not convert to Invoice"),
        );
    }

    #[test]
    fn a_record_from_an_older_writer_reads_by_the_kernels_defaults() {
        // Postel's law on the way in: a writer from before `pending` existed, and one that omitted
        // an absent result, both read as a completed success rather than an endless in-flight claim.
        for (name, stored, state) in [
            ("missing_pending_reads_as_completed", r#"{"payload_hash":11234062831410934249,"result":42}"#, "done"),
            ("missing_result_reads_as_null", r#"{"payload_hash":11234062831410934249,"pending":false}"#, "done"),
            (
                "missing_pending_with_an_error_reads_as_failed",
                r#"{"payload_hash":11234062831410934249,"error":"boom"}"#,
                "failed",
            ),
        ] {
            let record = decode_idempotency_record(stored).expect(name);
            assert_eq!((record.payload_hash, record.state.as_str()), (HASH, state), "{name}");
        }
    }

    // ---- envelope codec ----------------------------------------------------

    #[test]
    fn envelope_round_trips_through_every_format() {
        for format in ["json", "msgpack"] {
            for record in [envelope("evt-1", Some(7)), envelope("evt-1", None)] {
                let sequence = record.sequence;
                let wire = encode_envelope(format, envelope("evt-1", sequence)).unwrap();
                let back = decode_envelope(format, &wire).unwrap();
                assert_same_envelope(&back, &record, &format!("{format} sequence={sequence:?}"));
            }
            // The name the host selects by resolves to the media type a transport puts on the wire.
            assert_eq!(media_type(format).as_deref(), Ok(format!("application/vnd.edax.envelope+{format}").as_str()));
        }
    }

    // ---- error contract ----------------------------------------------------

    #[test]
    fn kernel_failures_carry_the_codec_code() {
        // Every deterministic marshaling failure must name its cause *and* carry the taxonomy code, or
        // the host falls back to a transient store error and retries a failure that always repeats.
        let valid_doc = r#"{"id":"user-1","age":36}"#;
        let cases: [(&str, Result<(), String>, &str); 17] = [
            ("filter_is_not_json", eval_filter("{not json}", valid_doc).map(|_| ()), "key must be a string at line 1 column 2"),
            ("filter_is_not_a_filter", eval_filter(r#"{"nope":1}"#, valid_doc).map(|_| ()), "unknown variant"),
            ("document_is_not_json", eval_filter(r#"{"exists":{"path":"id","exists":true}}"#, "nope").map(|_| ()), "expected ident"),
            ("query_spec_is_not_json", query("{not json}", "[]").map(|_| ()), "key must be a string at line 1 column 2"),
            ("cursor_is_not_base64", query(r#"{"sort":[{"path":"age"}],"after":"!!!"}"#, "[]").map(|_| ()), "not base64url"),
            ("cursor_document_is_not_json", encode_cursor("nope", "[]").map(|_| ()), "expected ident"),
            ("command_payload_is_not_json", payload_hash_json(b"{not json}").map(|_| ()), "key must be a string at line 1 column 2"),
            ("wire_bytes_are_not_the_format", decode_envelope("json", b"not an envelope").map(|_| ()), "decode"),
            (
                "cached_result_is_not_json",
                encode_idempotency_record(HASH, false, "{not json}", None, None).map(|_| ()),
                "key must be a string",
            ),
            (
                "cached_error_code_is_unknown",
                encode_idempotency_record(HASH, false, "null", Some("boom".to_owned()), Some("timeout".to_owned())).map(|_| ()),
                "unknown error code: timeout",
            ),
            // A batch refusal and an unknown code carry the code too, or a host reads them as transient.
            ("sequence_batch_has_a_gap", check_sequence_batch(1, vec![Some(2), Some(4)]), "envelope 1 has sequence 4, expected 3"),
            ("error_code_is_unknown", error_code_is_terminal("timeout").map(|_| ()), "unknown error code: timeout"),
            (
                "record_is_not_a_json_object",
                decode_idempotency_record("[11234062831410934249]").map(|_| ()),
                "idempotency record is not a JSON object",
            ),
            (
                "record_has_no_payload_hash",
                decode_idempotency_record(r#"{"pending":true}"#).map(|_| ()),
                "missing field `payload_hash`",
            ),
            (
                // A JSON reader that rounds the hash through a float writes it back as a string or a
                // decimal; either is terminal, not a store hiccup to retry.
                "record_hash_is_not_an_integer",
                decode_idempotency_record(r#"{"payload_hash":"11234062831410934249"}"#).map(|_| ()),
                "invalid type: string",
            ),
            (
                // A format name no kernel knows repeats identically on every retry, so it carries
                // the terminal code rather than a transient one the host would retry.
                "media_type_format_is_not_a_format",
                media_type("protobuf").map(|_| ()),
                "unknown format: protobuf",
            ),
            (
                // A Redis kernel failure reaches the host under the same terminal code.
                "redis_claim_handle_is_not_a_claim",
                redis_settled_value(b"1deadbeef", "{}").map(|_| ()),
                "claim handle is not a claim this kernel minted",
            ),
        ];
        for (name, got, cause) in cases {
            let message = got.expect_err(name);
            assert!(message.starts_with("codec: "), "{name}: {message}");
            assert!(message.contains(cause), "{name}: {message}");
        }

        // An unknown format is rejected on both directions of the codec, and names the format.
        for message in [
            encode_envelope("protobuf", envelope("evt-1", Some(1))).unwrap_err(),
            decode_envelope("protobuf", b"{}").map(|_| ()).unwrap_err(),
        ] {
            assert_eq!(message, "codec: unknown format: protobuf");
        }
    }

    // ---- in-memory stores --------------------------------------------------

    #[test]
    fn event_store_crosses_envelopes_structurally() {
        let store = memory_transactor();
        store.append_events(NO_TX, "invoice-7", 0, vec![envelope("evt-1", Some(1))]).unwrap();

        // Every field survives the boundary — no JSON or wire encoding in between.
        let page = store.load_events(NO_TX, "invoice-7", 0).unwrap();
        assert_eq!(page.len(), 1);
        assert_same_envelope(&page[0], &envelope("evt-1", Some(1)), "loaded envelope");
        assert!(store.load_events(NO_TX, "invoice-404", 0).unwrap().is_empty());

        // The adapter's own error code is forwarded untouched, not re-tagged as a codec failure.
        let conflict = store.append_events(NO_TX, "invoice-7", 0, vec![envelope("evt-2", Some(2))]).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        let unsequenced = store.append_events(NO_TX, "invoice-7", 1, vec![envelope("evt-2", None)]).unwrap_err();
        assert_eq!(unsequenced, "codec: envelope 0 has no sequence");

        // Outbox: append enqueued the event; marking it published drains it.
        assert_eq!((store.unpublished(10).len(), store.unpublished_count()), (1, 1));
        store.mark_published(vec!["evt-1".to_owned()]);
        assert!(store.unpublished(10).is_empty());
        assert_eq!(store.unpublished_count(), 0);

        // A missing snapshot crosses as `None`, not as an error.
        assert!(store.load_snapshot(NO_TX, "invoice-7").unwrap().is_none());

        store.save_snapshot(NO_TX, "invoice-7", "Invoice", 1, br#"{"total":5000}"#.to_vec()).unwrap();
        let present = store.load_snapshot(NO_TX, "invoice-7").unwrap().expect("saved");
        assert_eq!((present.type_name.as_str(), present.version, present.data), ("Invoice", 1, br#"{"total":5000}"#.to_vec()));
        // A snapshot leading the stream is the kernel's terminal refusal, forwarded untouched.
        let leading = store.save_snapshot(NO_TX, "invoice-7", "Invoice", 2, b"{}".to_vec()).unwrap_err();
        assert_eq!(leading, "codec: snapshot of invoice-7 at version 2 leads its stream at version 1");
    }

    #[test]
    fn idempotency_store_claims_and_records() {
        let store = memory_transactor();
        let hash = payload_hash_json(br#"{"email":"ada@example.com"}"#).unwrap();

        let won = store.claim(NO_TX, "cmd-key-1", hash).unwrap();
        assert!(won.won && !won.handle.is_empty() && won.prior.is_empty(), "first claim wins, with a handle");

        let lost = store.claim(NO_TX, "cmd-key-1", hash).unwrap();
        assert!(!lost.won && lost.handle.is_empty());
        let prior: Value = serde_json::from_str(&lost.prior).unwrap();
        assert_eq!(prior, json!({"payload_hash": hash, "result": null, "error": null, "error_code": null, "pending": true}));

        // The fence crosses: a handle that names no claim settles nothing, the live one does.
        let done = r#"{"payload_hash":0,"result":{"invoice_id":"invoice-7"},"error":null,"pending":false}"#;
        let refused = store.complete(NO_TX, "cmd-key-1", b"stale".to_vec(), done).unwrap_err();
        assert!(refused.starts_with("store: idempotency key cmd-key-1 no longer holds"), "{refused}");
        store.complete(NO_TX, "cmd-key-1", won.handle.clone(), done).unwrap();
        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash).unwrap().prior, done);

        // A release after the complete keeps the record; an expiry frees the key.
        store.release(NO_TX, "cmd-key-1", won.handle).unwrap();
        assert_eq!(store.claim(NO_TX, "cmd-key-1", hash).unwrap().prior, done);
        assert!(store.expire_claim("cmd-key-1"));
        assert!(!store.expire_claim("cmd-key-404"), "nothing to lapse");
        let again = store.claim(NO_TX, "cmd-key-1", hash).unwrap();
        assert!(again.won, "an expired key is claimable again");
        store.release(NO_TX, "cmd-key-1", again.handle).unwrap();
        assert!(store.claim(NO_TX, "cmd-key-1", hash).unwrap().won, "a released key is claimable again");
    }

    #[test]
    fn projection_store_versions_and_queries() {
        let store = memory_transactor();
        assert!(store.get_projection(NO_TX, "user-1").unwrap().is_none());

        assert_eq!(store.save_projection(NO_TX, "user-1", 0, r#"{"email":"ada@example.com","age":36}"#).unwrap(), 1);
        let row = store.get_projection(NO_TX, "user-1").unwrap().expect("saved");
        assert_eq!(row.version, 1);
        assert_eq!(serde_json::from_str::<Value>(&row.document_json).unwrap(), json!({"email": "ada@example.com", "age": 36}));

        // The adapter's codes cross untouched: a stale version is a conflict, a bad document is codec.
        let conflict = store.save_projection(NO_TX, "user-1", 0, r#"{"email":"ada@example.com","age":37}"#).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        for (name, document, cause) in [
            ("not_json", r#"{"email":"ada@example.com","#, "EOF while parsing"),
            ("not_an_object", r#"["ada@example.com"]"#, "invalid type: sequence, expected a map"),
            ("a_scalar", "36", "invalid type: integer `36`, expected a map"),
        ] {
            let refused = store.save_projection(NO_TX, "user-2", 0, document).unwrap_err();
            assert!(refused.starts_with("codec: a projection document is a JSON object: "), "{name}: {refused}");
            assert!(refused.contains(cause), "{name}: {refused}");
        }

        store.save_projection(NO_TX, "user-2", 0, r#"{"email":"grace@example.com","age":29}"#).unwrap();
        let page: Value =
            serde_json::from_str(&store.query_projections(NO_TX, r#"{"sort":[{"path":"age"}]}"#).unwrap()).unwrap();
        let ids: Vec<&str> = page["rows"].as_array().unwrap().iter().map(|d| d["id"].as_str().unwrap()).collect();
        assert_eq!((ids, &page["next"]), (vec!["user-2", "user-1"], &Value::Null));
        assert!(store.query_projections(NO_TX, "{not a query}").unwrap_err().starts_with("codec: "));
        let wide = store.query_projections(NO_TX, r#"{"filter":{"compare":{"path":"age","op":"eq","value":1180591620717411303424}}}"#);
        assert!(wide.is_err_and(|e| e.starts_with("codec: ")), "an integer past 64 bits is the kernel's refusal");
        let cursor = store.query_projections(NO_TX, r#"{"sort":[{"path":"age"}],"after":"!!!"}"#).unwrap_err();
        assert!(cursor.starts_with("codec: "), "the kernel's cursor refusal crosses untouched: {cursor}");
    }

    #[test]
    fn saga_store_versions_and_completes() {
        let store = memory_transactor();
        assert!(store.load_saga(NO_TX, "order-1").unwrap().is_none());

        let folded = r#"{"order-1":{"sequence":1,"id":"evt-1"}}"#;
        assert_eq!(store.save_saga(NO_TX, "order-1", 0, r#"{"status":"awaiting_payment"}"#, folded, vec![]).unwrap(), 1);
        let row = store.load_saga(NO_TX, "order-1").unwrap().expect("saved");
        assert_eq!((row.version, row.completed, row.folded.as_str()), (1, false, folded), "the marks cross as they were saved");
        assert_eq!(serde_json::from_str::<Value>(&row.state_json).unwrap(), json!({"status": "awaiting_payment"}));

        // The adapter's codes cross untouched: a stale version is a conflict, bad state JSON is codec.
        let conflict = store.save_saga(NO_TX, "order-1", 0, r#"{"status":"shipped"}"#, "", vec![]).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        assert!(store.save_saga(NO_TX, "order-1", 1, "{not json", "", vec![]).unwrap_err().starts_with("codec: saga state: "));
        let stale_complete = store.complete_saga(NO_TX, "order-1", 0, vec![]).unwrap_err();
        assert_eq!(stale_complete, "concurrency: expected 0, stored 1");

        // The commands cross structurally with the tombstone and come back from the outbox.
        let fulfill = SagaCommand {
            id: saga_command_id("order-1", 2, 0),
            correlation_id: "order-1".to_owned(),
            command: Event { name: "Fulfill".to_owned(), version: 1, data: br#"{"order_id":"order-1"}"#.to_vec() },
            timestamp: 7,
        };
        assert_eq!(store.complete_saga(NO_TX, "order-1", 1, vec![fulfill]).unwrap(), 2);
        let done = store.load_saga(NO_TX, "order-1").unwrap().expect("a tombstone");
        assert_eq!((done.version, done.completed, done.state_json.as_str(), done.folded.as_str()), (2, true, "null", ""));
        let unsent = store.unpublished_saga_commands(10);
        assert_eq!(unsent.len(), 1);
        let row = &unsent[0];
        assert_eq!((row.id.as_str(), row.correlation_id.as_str(), row.timestamp), ("order-1:2:0", "order-1", 7));
        assert_eq!((row.command.name.as_str(), row.command.version, row.command.data.as_slice()), ("Fulfill", 1, &br#"{"order_id":"order-1"}"#[..]));
        store.mark_saga_commands_published(vec!["order-1:2:0".to_owned()]);
        assert!(store.unpublished_saga_commands(10).is_empty());
        let resurrect = store.save_saga(NO_TX, "order-1", 0, r#"{"status":"open"}"#, "", vec![]).unwrap_err();
        assert!(resurrect.starts_with("concurrency: saga order-1 completed"), "{resurrect}");
        assert_eq!(limit_of(u64::MAX), usize::MAX, "a limit past the target's width saturates");
    }

    #[test]
    fn transaction_token_crosses_the_boundary_and_settles_every_seam() {
        // The host holds only the token between calls: the journal is the adapters'. One write on
        // every seam under it, then the two outcomes.
        struct Case {
            name: &'static str,
            commit: bool,
            /// `(events, outbox, snapshot, projection, claim taken, saga)` visible under NO_TX afterwards.
            want: (usize, usize, bool, bool, bool, bool),
        }
        let cases = [
            Case { name: "commit lands every seam", commit: true, want: (1, 1, true, true, true, true) },
            Case { name: "rollback leaves nothing on any seam", commit: false, want: (0, 0, false, false, false, false) },
        ];
        for c in cases {
            let store = memory_transactor();
            let tx = store.begin();
            assert_ne!(tx, NO_TX, "{}", c.name);
            store.append_events(tx, "invoice-7", 0, vec![envelope("evt-1", Some(1))]).unwrap();
            store.save_snapshot(tx, "invoice-7", "Invoice", 1, b"snap".to_vec()).unwrap();
            store.save_projection(tx, "user-1", 0, r#"{"age":36}"#).unwrap();
            assert!(store.claim(tx, "req-A", 7).unwrap().won, "{}", c.name);
            store.save_saga(tx, "order-1", 0, r#"{"status":"open"}"#, "", vec![]).unwrap();

            // Read-your-writes under the token; nothing visible under NO_TX yet (the outbox never
            // shows a staged append).
            assert_eq!(store.load_events(tx, "invoice-7", 0).unwrap().len(), 1, "{}", c.name);
            assert!(store.load_events(NO_TX, "invoice-7", 0).unwrap().is_empty(), "{}", c.name);
            assert!(store.unpublished(10).is_empty(), "{}", c.name);
            assert!(store.get_projection(NO_TX, "user-1").unwrap().is_none(), "{}", c.name);

            if c.commit {
                store.commit(tx).unwrap();
            } else {
                assert!(store.rollback(tx), "{}", c.name);
            }
            let visible = (
                store.load_events(NO_TX, "invoice-7", 0).unwrap().len(),
                store.unpublished(10).len(),
                store.load_snapshot(NO_TX, "invoice-7").unwrap().is_some(),
                store.get_projection(NO_TX, "user-1").unwrap().is_some(),
                !store.claim(NO_TX, "req-A", 7).unwrap().won,
                store.load_saga(NO_TX, "order-1").unwrap().is_some(),
            );
            assert_eq!(visible, c.want, "{}", c.name);

            // A spent token is refused everywhere with the terminal code, and a rollback of it is a no-op.
            let refused = store.load_events(tx, "invoice-7", 0).map(|p| p.len()).unwrap_err();
            assert!(refused.starts_with("bad_handle: unknown transaction"), "{}: {refused}", c.name);
            assert_eq!(store.commit(tx).unwrap_err(), refused, "{}", c.name);
            assert!(!store.rollback(tx), "{}", c.name);
        }
    }

    #[test]
    fn broker_polls_and_settles_by_token() {
        assert!(memory_broker(0).is_err_and(|e| e.starts_with("codec:")), "zero partitions is refused");
        let broker = memory_broker(1).unwrap();
        assert!(broker.is_empty("g"));

        // An empty poll is `None`, never an error.
        assert!(broker.try_poll("g").unwrap().is_none());

        broker.publish(envelope("evt-1", Some(1)));
        broker.publish(envelope("evt-1", Some(1))); // at-least-once: the same envelope republished
        assert!(!broker.is_empty("g"));

        let first = broker.try_poll("g").unwrap().expect("published");
        assert_eq!((first.partition, first.offset), (0, 0), "the coordinates cross");
        assert_same_envelope(&first.envelope, &envelope("evt-1", Some(1)), "polled envelope");
        let second = broker.try_poll("g").unwrap().expect("published twice");
        assert_eq!(second.offset, 1);
        assert_ne!(first.token, second.token, "one id in flight twice is two deliveries");

        // A nack rewinds: both copies are retired, the stale tokens settle nothing, and both come back.
        assert!(broker.is_live("g", second.token));
        assert!(broker.nack("g", first.token));
        assert!(!broker.is_live("g", second.token), "the rewind retired the later delivery");
        assert!(!broker.ack("g", first.token));
        let again = broker.try_poll("g").unwrap().expect("rewound");
        assert!(again.token != first.token && again.offset == 0);

        // Another group reads every message on its own; a crash redelivers from the commit.
        let other = broker.try_poll("h").unwrap().expect("another group reads everything");
        assert_eq!(other.offset, 0);
        broker.crash("h");
        assert!(!broker.is_live("h", other.token));
        assert_eq!(broker.try_poll("h").unwrap().map(|d| d.offset), Some(0), "nothing was committed, so 0 comes back");

        assert!(broker.ack("g", again.token));
        assert!(!broker.ack("g", again.token), "a second settlement is refused");
        let last = broker.try_poll("g").unwrap().expect("offset 1");
        assert!(!broker.is_empty("g"), "offset 1 is still outstanding");
        assert!(broker.ack("g", last.token));
        assert!(broker.is_empty("g"));
        assert!(!broker.is_empty("h"));
    }

    // ---- kafka -------------------------------------------------------------

    /// Copy headers for a second call: the binding record carries no derives (alef owns them on the
    /// generated twin), and every header-taking call consumes its `Vec`.
    fn copy_headers(headers: &[KafkaHeader]) -> Vec<KafkaHeader> {
        headers.iter().map(|header| KafkaHeader { key: header.key.clone(), value: header.value.clone() }).collect()
    }

    /// What the boundary adds to the record kernel: the envelope and the record cross as structs,
    /// so every field survives both conversions. The key rule and the header set are the kernel's,
    /// pinned by `testdata/golden/kafka_record.json`.
    #[test]
    fn kafka_record_crosses_the_record_structurally() {
        let record = kafka_record(envelope("evt-1", Some(7)), b"<invoice/>".to_vec(), "application/xml", None);
        assert_eq!(record.key.as_slice(), b"invoice-7".as_slice(), "the kernel's key rule, on the host's record");
        assert_eq!(record.value.as_slice(), b"<invoice/>".as_slice(), "the host's own bytes, untouched");
        let headers: Vec<(&str, &[u8])> =
            record.headers.iter().map(|h| (h.key.as_str(), h.value.as_slice())).collect();
        assert_eq!(
            headers,
            [(kafka_content_type_header().as_str(), b"application/xml".as_slice())],
            "a host serializer keeps the one contract header, under the kernel's name"
        );

        // `key` trails the required parameters because alef moves an optional one to the end; a
        // surface that listed it earlier would bind the host's override to the wrong argument.
        let key = Some("eu-west/invoice-7".to_owned());
        let overridden = kafka_record(envelope("evt-1", Some(7)), Vec::new(), "application/xml", key);
        assert_eq!(overridden.key, b"eu-west/invoice-7");

        // The route crosses flattened: the default, an index into the host's list, or the unknown
        // media type — the empty one kept apart from no header at all.
        let route = |headers: Vec<KafkaHeader>| {
            let route = kafka_route(headers, vec!["application/json".to_owned(), "application/xml".to_owned()]);
            (route.registered, route.unknown)
        };
        assert_eq!(route(copy_headers(&record.headers)), (Some(1), None));
        assert_eq!(route(Vec::new()), (None, None), "no headers at all: the default");
        let empty = vec![KafkaHeader { key: kafka_content_type_header(), value: Vec::new() }];
        assert_eq!(route(empty), (None, Some(String::new())), "an empty value is unknown, not the default");

        // A dead letter keeps the record and appends its source.
        let dead = kafka_dead_letter_record(
            KafkaRecord { key: record.key.clone(), value: record.value.clone(), headers: copy_headers(&record.headers) },
            "orders",
            3,
            42,
            "codec: bad",
        );
        assert_eq!((dead.key.as_slice(), dead.value.as_slice()), (record.key.as_slice(), record.value.as_slice()));
        assert_eq!(dead.headers.len(), record.headers.len() + 4);
        let source: Vec<(&str, &[u8])> = dead.headers[record.headers.len()..].iter().map(|h| (h.key.as_str(), h.value.as_slice())).collect();
        assert_eq!(
            source,
            [("edax-dlq-topic", &b"orders"[..]), ("edax-dlq-partition", b"3"), ("edax-dlq-offset", b"42"), ("edax-dlq-error", b"codec: bad")]
        );

        // Topic, partition and offset together are the identity of a message that has no envelope.
        assert_eq!(kafka_poison_id("orders", 3, 42), "orders/3/42");

        // Placement: `(key, partitions)` in that order, Java's rule, and no partitions is refused.
        assert_eq!(kafka_partition_for(b"21", 12), edax_kafka::partition_for(b"21", 12).map_err(wire));
        assert_eq!(kafka_partition_for(b"21", 1), Ok(0));
        assert_eq!(kafka_partition_for(b"21", 0), Err("codec: a topic has at least one partition, got 0".to_owned()));
    }

    /// What the boundary adds to the offset kernel: the decisions and the two pages cross field for
    /// field. The watermark rules themselves are the kernel's, pinned by
    /// `testdata/golden/kafka_offsets.json`.
    #[test]
    fn offset_tracker_crosses_its_decisions_structurally() {
        let tracker = offset_tracker();
        assert!(tracker.stats().is_empty(), "nothing is assigned before the first booking");
        assert!(tracker.book("orders", 0, -1).is_err_and(|e| e.starts_with("codec:")), "a negative offset is refused");

        let five = tracker.book("orders", 0, 5).unwrap();
        let six = tracker.book("orders", 0, 6).unwrap();
        assert_ne!(five, six, "one token per delivery");
        assert_ne!(five, edax_core::NO_TOKEN, "a live token never aliases the placeholder");
        assert!(tracker.is_live(six));

        assert!(tracker.ack(six), "settled");
        assert!(!tracker.ack(six), "stale: the token already settled");
        assert!(tracker.due().is_empty(), "5 is in flight: the watermark has not moved past the start");
        let seven = tracker.book("orders", 0, 7).unwrap();
        let nack = tracker.nack(five);
        assert_eq!(nack.seek, Some(5));
        assert!(!tracker.is_live(seven), "the rewind retired the later token");
        assert_eq!(tracker.nack(edax_core::NO_TOKEN).seek, None, "a stale nack seeks nowhere");

        let again = tracker.book("orders", 0, 5).unwrap();
        assert!(tracker.ack(again));
        let due = tracker.due();
        let rows: Vec<(&str, i32, i64)> = due.iter().map(|d| (d.topic.as_str(), d.partition, d.offset)).collect();
        assert_eq!(rows, [("orders", 0, 7)], "7 is the lowest still in flight");
        tracker.committed("orders", 0, 7);
        assert!(tracker.due().is_empty(), "committed: nothing due");

        let page = tracker.stats();
        type Row<'a> = (&'a str, i32, u64, u64, i64, Option<i64>);
        let rows: Vec<Row> = page
            .iter()
            .map(|row| (row.topic.as_str(), row.partition, row.unsettled, row.live, row.max_delivered, row.committed))
            .collect();
        assert_eq!(rows, [("orders", 0, 1, 0, 7, Some(7))], "7 is unsettled and retired");

        tracker.reset("orders", 0);
        assert!(tracker.stats().is_empty(), "a revoked partition leaves no row behind");
    }

    // ---- redis -------------------------------------------------------------

    /// What the boundary adds to the ledger kernel: the claim outcome crosses as an optional record.
    /// The value layout is the kernel's, pinned by `testdata/golden/redis_value.json`.
    #[test]
    fn redis_value_crosses_as_owner_and_record() {
        let claim = redis_new_claim(HASH).unwrap();
        assert_ne!(claim, redis_new_claim(HASH).unwrap(), "each claim mints its own owner");
        assert_eq!(redis_check_claim(&claim), Ok(()));

        // A settled record is passed through untouched, so a result never round-trips through a
        // second serializer on its way to the ledger.
        let done = encode_idempotency_record(HASH, false, RESULT, None, None).unwrap();
        let settled = redis_settled_value(&claim, &done).unwrap();
        assert_eq!(&settled[..33], &claim[..33], "the claim's own owner token");

        assert_eq!(redis_claim_outcome(&claim, None), Ok(None));
        assert_eq!(redis_claim_outcome(&claim, Some(claim.clone())), Ok(None));
        assert_eq!(redis_claim_outcome(&claim, Some(settled)), Ok(Some(done)));
        let pending = redis_claim_outcome(&redis_new_claim(HASH).unwrap(), Some(claim)).unwrap().expect("another claim");
        assert_eq!(decode_idempotency_record(&pending).unwrap().state, "pending");
    }

    /// The settings and the replies cross with the kernel's codes: a refused setting or command is a
    /// terminal `bad_handle:`, a passing server state a transient `store:`.
    #[test]
    fn redis_refusals_cross_with_the_kernels_codes() {
        assert_eq!(redis_ledger_key("idem:", "k"), Ok(b"idem:k".to_vec()));
        assert_eq!(redis_check_config("idem:", 1, 3, 1, true), Ok(()));
        assert_eq!(redis_heartbeats_per_ttl(), 3);
        for refused in [
            redis_ledger_key("", "k").map(|_| ()),
            redis_check_config("", 1, 1, 1, false),
            redis_check_config("idem:", 0, 1, 1, false),
            redis_check_claim(b""),
        ] {
            let wire = refused.unwrap_err();
            assert!(wire.starts_with("bad_handle: ") || wire.starts_with("codec: "), "{wire}");
        }
        assert!(redis_reply_error("ERR invalid expire time").starts_with("bad_handle: "));
        assert!(redis_reply_error("READONLY replica").starts_with("store: "));
        let scripts = redis_scripts();
        assert_eq!(
            [scripts.claim, scripts.complete, scripts.release, scripts.extend],
            [edax_redis::CLAIM_SCRIPT, edax_redis::COMPLETE_SCRIPT, edax_redis::RELEASE_SCRIPT, edax_redis::EXTEND_SCRIPT]
        );
    }
}
