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

// The kernel's messages are referred to fully-qualified (`edax_event::EventMessage`); the names
// `Event`, `EventDraft`, `EventMessage`, `CommandMessage` and `Lineage` below are this crate's own
// binding-facing records, shaped to mirror them.

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

/// Check an append batch against the dense-sequence rule: every event carries a sequence and the
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
pub fn upcast_plan(from_version: u32, to: u32, available: Option<Vec<u32>>) -> Result<Vec<u32>, String> {
    edax_event::upcast_plan(from_version, to, available.as_deref()).map_err(wire)
}

/// The schema version a codec writes for one event name: one past the highest of its `available`
/// steps, or the first version (1) for `None` or none. A hole in the steps, or a step at the `u32`
/// ceiling, is the kernel's `codec` error. A codec asks once per name when it is built. See
/// [`edax_event::upcast_target`].
pub fn upcast_target(available: Option<Vec<u32>>) -> Result<u32, String> {
    edax_event::upcast_target(available.as_deref()).map_err(wire)
}

/// The two ids that place a message in its flow. See [`edax_event::Lineage`].
pub struct Lineage {
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
}

fn record_lineage(lineage: edax_event::Lineage) -> Lineage {
    Lineage { correlation_id: lineage.correlation_id, causation_id: lineage.causation_id }
}

/// The lineage of a child of the message `parent_id`: its causation is the parent's id, and it
/// inherits `parent_correlation_id`. See [`edax_event::propagate`].
pub fn propagate(parent_id: &str, parent_correlation_id: &str) -> Lineage {
    record_lineage(edax_event::propagate(parent_id, parent_correlation_id))
}

/// The lineage of a message with no parent message, given the ids the caller already holds and one
/// freshly minted id. Fills only what is missing: `correlation_id` defaults to `fresh_id`, and
/// `causation_id` to the resulting correlation id. An empty string is a supplied id.
///
/// The host mints `fresh_id` (with [`guid`]) because minting is non-deterministic. The kernel keeps
/// the defaulting rule, so a golden vector pins it. See [`edax_event::originate`].
// The two ids are optional, so they trail: alef moves optional parameters to the end of the signature.
pub fn originate(fresh_id: &str, correlation_id: Option<String>, causation_id: Option<String>) -> Lineage {
    record_lineage(edax_event::originate(correlation_id.as_deref(), causation_id.as_deref(), fresh_id))
}

/// The ordering key of a message: `aggregate_id`, or the message's own `id` when it targets no
/// aggregate. An event always has an aggregate, so its key is its aggregate. See
/// [`edax_event::ordering_key`].
// `aggregate_id` is optional, so it trails `id`: alef moves optional parameters to the end.
pub fn ordering_key(id: &str, aggregate_id: Option<String>) -> String {
    edax_event::ordering_key(aggregate_id.as_deref(), id).to_owned()
}

/// The key a guard claims for a received command: its `idempotency_key`, or its own `id` when it
/// carries none, so a redelivery of the same message still deduplicates. See
/// [`edax_event::received_key`].
// `idempotency_key` is optional, so it trails `id`: alef moves optional parameters to the end.
pub fn received_key(id: &str, idempotency_key: Option<String>) -> String {
    edax_event::received_key(idempotency_key.as_deref(), id).to_owned()
}

/// Refuse an extension key that breaks the rule, as a `codec:` error naming it. A key is 1 to 64
/// bytes of `[a-z0-9._-]` and starts with `[a-z0-9]`. See [`edax_event::check_extensions`].
pub fn check_extensions(extensions: HashMap<String, String>) -> Result<(), String> {
    edax_event::check_extensions(&ordered(extensions)).map_err(wire)
}

/// Refuse a tenant id that breaks the rule, as a `codec:` error. A tenant id is 1 to 64 bytes; the
/// charset is open, because `tenant_key` is injective over any bytes. See [`edax_core::check_tenant`].
pub fn check_tenant(tenant: &str) -> Result<(), String> {
    edax_core::check_tenant(tenant).map_err(wire)
}

/// The key a seam is called with under the tenancy `mode` (`"silo"`, `"pool"` or `"bridge"`): the
/// `key` itself, or `key` composed with its tenant. A mode that requires a tenant and a call that
/// names none is a `codec:` error, so a pool can never read an uncomposed key. Pinned by
/// `testdata/golden/tenancy.json`. See [`edax_core::tenant_key`].
pub fn tenant_key(mode: &str, key: &str, tenant: Option<String>) -> Result<String, String> {
    let mode = edax_core::Tenancy::from_name(mode).map_err(wire)?;
    edax_core::tenant_key(mode, tenant.as_deref(), key).map_err(wire)
}

/// Whether the tenancy `mode` requires every message and keyed call to name a tenant, and whether it
/// composes that tenant into the key. One call, so a binding reads both rules from the kernel rather
/// than restating them. See [`edax_core::Tenancy`].
pub fn tenancy_rules(mode: &str) -> Result<TenancyRules, String> {
    let mode = edax_core::Tenancy::from_name(mode).map_err(wire)?;
    Ok(TenancyRules { requires_tenant: mode.requires_tenant(), composes_keys: mode.composes_keys() })
}

/// What a tenancy mode demands, as [`tenancy_rules`] reports it.
pub struct TenancyRules {
    /// Every message and keyed call must name a tenant.
    pub requires_tenant: bool,
    /// The tenant is composed into every key a seam is called with.
    pub composes_keys: bool,
}

// ---- idempotency ---------------------------------------------------------

/// Stable 64-bit hash of arbitrary bytes (FNV-1a). A command's guard hashes with
/// [`command_hash_json`] instead, which binds the name and canonicalizes first.
pub fn payload_hash(payload: &[u8]) -> u64 {
    edax_cqrs::payload_hash(payload)
}

/// Canonicalize a UTF-8 JSON payload (RFC 8785) and hash it. Canonicalization lives in the kernel,
/// so every binding gets the same hash; don't canonicalize on the host side. A command's guard uses
/// [`command_hash_json`], which also binds the command name. An integer literal past 2^53 is a
/// `codec` error.
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

/// The hash a command's idempotency guard binds to its key: FNV-1a over the RFC 8785 form of
/// `{"name": name, "payload": payload}`. `payload` is the command value as UTF-8 JSON, in the shape
/// the handler receives (after upcast). The kernel canonicalizes, so every binding agrees. An integer
/// literal past 2^53 is a `codec` error. See [`edax_cqrs::command_hash_json`].
pub fn command_hash_json(name: &str, payload: &[u8]) -> Result<u64, String> {
    edax_cqrs::command_hash_json(name, payload).map_err(wire)
}

/// The ledger key a command's idempotency guard claims for `idempotency_key` under the guard's
/// `scope`: `"edax:cmd:{len(scope)}:{scope}:{idempotency_key}"`. Two guards with different scopes
/// never share a key. See [`edax_cqrs::claim_key`].
pub fn claim_key(scope: &str, idempotency_key: &str) -> String {
    edax_cqrs::claim_key(scope, idempotency_key)
}

/// What an event's idempotency guard claims: the key and the payload hash. See
/// [`edax_event::event_idempotency`].
pub struct EventIdempotency {
    /// The event id.
    pub key: String,
    /// FNV-1a over the raw `data` bytes.
    pub payload_hash: u64,
}

/// The key and hash an event's idempotency guard claims for `message`: the kernel's rule, pinned by
/// `testdata/golden/event_idempotency.json`, so two SDKs consuming one stream bind alike.
pub fn event_idempotency(message: EventMessage) -> EventIdempotency {
    let msg = kernel_event_message(message);
    let rule = edax_event::event_idempotency(&msg);
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

// ---- messages and their codec ----------------------------------------------

/// A marshaled payload: a name, a schema version, and the host-serialized `data`. What a host codec
/// produces from a domain value; a saga hands its commands over in this shape. See
/// [`edax_event::Event`].
pub struct Event {
    pub name: String,
    pub version: u32,
    pub data: Vec<u8>,
}

/// An event as a repository hands it to the store: every field of [`EventMessage`] but
/// `recorded_at`, which the store assigns. See [`edax_event::EventDraft`].
pub struct EventDraft {
    /// Globally unique event id.
    pub id: String,
    /// The aggregate this event belongs to.
    pub aggregate_id: String,
    /// The tenant this message belongs to, `None` outside a multi-tenant deployment. Every key a seam
    /// derives from this message is composed from it; see `tenant_key`.
    pub tenant: Option<String>,
    /// The per-aggregate ordinal, `None` for an event outside an aggregate stream.
    pub sequence: Option<u64>,
    /// The event type name.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one.
    pub causation_id: String,
    /// When the fact happened, if the host knows it apart from when it was recorded.
    pub occurred_at: Option<i64>,
    /// Extension context, under the key rule of [`check_extensions`].
    pub extensions: HashMap<String, String>,
    /// Application context, opaque to edax.
    pub metadata: HashMap<String, String>,
}

/// An event as stored, published and consumed. It crosses as a structured object: bytes stay
/// bytes, dictionaries stay dictionaries, with no JSON or wire encoding in between. See
/// [`edax_event::EventMessage`].
pub struct EventMessage {
    /// Globally unique event id: the causation id of anything this event causes.
    pub id: String,
    /// The aggregate this event belongs to: its ordering key.
    pub aggregate_id: String,
    /// The tenant this message belongs to, `None` outside a multi-tenant deployment. Every key a seam
    /// derives from this message is composed from it; see `tenant_key`.
    pub tenant: Option<String>,
    /// The per-aggregate ordinal, `None` for an event outside an aggregate stream.
    pub sequence: Option<u64>,
    /// The event type name a consumer routes and unmarshals by.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
    /// When the fact happened, set by the host. `None` means it happened when recorded.
    pub occurred_at: Option<i64>,
    /// Unix nanoseconds on the store's clock when the store appended the event.
    pub recorded_at: i64,
    /// Extension context, under the key rule of [`check_extensions`].
    pub extensions: HashMap<String, String>,
    /// Application context, opaque to edax.
    pub metadata: HashMap<String, String>,
}

/// A command as dispatched, queued and consumed. See [`edax_event::CommandMessage`].
pub struct CommandMessage {
    /// This message instance. A redelivery of the same message keeps it.
    pub id: String,
    /// The client's intent: the key an idempotency guard claims. `None`: no claim is made.
    pub idempotency_key: Option<String>,
    /// The aggregate the command targets, if any: its ordering key.
    pub aggregate_id: Option<String>,
    /// The tenant this message belongs to, `None` outside a multi-tenant deployment. Every key a seam
    /// derives from this message is composed from it; see `tenant_key`.
    pub tenant: Option<String>,
    /// The command type name a handler is registered under.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
    /// Unix nanoseconds on the issuer's clock when the command was built.
    pub issued_at: i64,
    /// Extension context, under the key rule of [`check_extensions`].
    pub extensions: HashMap<String, String>,
    /// Application context, opaque to edax.
    pub metadata: HashMap<String, String>,
}

/// A host dictionary as the kernel's key-ordered map, so encoded bytes are deterministic.
fn ordered(map: HashMap<String, String>) -> BTreeMap<String, String> {
    map.into_iter().collect()
}

/// The kernel's key-ordered map as a host dictionary.
fn unordered(map: BTreeMap<String, String>) -> HashMap<String, String> {
    map.into_iter().collect()
}

fn kernel_event(e: Event) -> edax_event::Event {
    edax_event::Event { name: e.name, version: e.version, data: e.data }
}

fn kernel_event_draft(d: EventDraft) -> edax_event::EventDraft {
    edax_event::EventDraft {
        id: d.id,
        aggregate_id: d.aggregate_id,
        tenant: d.tenant,
        sequence: d.sequence,
        name: d.name,
        version: d.version,
        data: d.data,
        correlation_id: d.correlation_id,
        causation_id: d.causation_id,
        occurred_at: d.occurred_at,
        extensions: ordered(d.extensions),
        metadata: ordered(d.metadata),
    }
}

fn kernel_event_message(m: EventMessage) -> edax_event::EventMessage {
    edax_event::EventMessage {
        id: m.id,
        aggregate_id: m.aggregate_id,
        tenant: m.tenant,
        sequence: m.sequence,
        name: m.name,
        version: m.version,
        data: m.data,
        correlation_id: m.correlation_id,
        causation_id: m.causation_id,
        occurred_at: m.occurred_at,
        recorded_at: m.recorded_at,
        extensions: ordered(m.extensions),
        metadata: ordered(m.metadata),
    }
}

fn record_event_message(m: edax_event::EventMessage) -> EventMessage {
    EventMessage {
        id: m.id,
        aggregate_id: m.aggregate_id,
        tenant: m.tenant,
        sequence: m.sequence,
        name: m.name,
        version: m.version,
        data: m.data,
        correlation_id: m.correlation_id,
        causation_id: m.causation_id,
        occurred_at: m.occurred_at,
        recorded_at: m.recorded_at,
        extensions: unordered(m.extensions),
        metadata: unordered(m.metadata),
    }
}

fn kernel_command_message(m: CommandMessage) -> edax_event::CommandMessage {
    edax_event::CommandMessage {
        id: m.id,
        idempotency_key: m.idempotency_key,
        aggregate_id: m.aggregate_id,
        tenant: m.tenant,
        name: m.name,
        version: m.version,
        data: m.data,
        correlation_id: m.correlation_id,
        causation_id: m.causation_id,
        issued_at: m.issued_at,
        extensions: ordered(m.extensions),
        metadata: ordered(m.metadata),
    }
}

fn record_command_message(m: edax_event::CommandMessage) -> CommandMessage {
    CommandMessage {
        id: m.id,
        idempotency_key: m.idempotency_key,
        aggregate_id: m.aggregate_id,
        tenant: m.tenant,
        name: m.name,
        version: m.version,
        data: m.data,
        correlation_id: m.correlation_id,
        causation_id: m.causation_id,
        issued_at: m.issued_at,
        extensions: unordered(m.extensions),
        metadata: unordered(m.metadata),
    }
}

/// The stored message for `draft`: every field as drafted, stamped `recorded_at` on the store's clock.
/// See [`edax_event::EventDraft::record`].
pub fn record_event(draft: EventDraft, recorded_at: i64) -> EventMessage {
    record_event_message(kernel_event_draft(draft).record(recorded_at))
}

fn resolve(format: &str) -> Result<edax_event::Format, String> {
    edax_event::Format::from_name(format).map_err(wire)
}

/// An event -> wire bytes in `format` (`"json"` or `"msgpack"`). A bad extension key is a `codec:`
/// error, so a producer never mints a message every consumer would refuse.
pub fn encode_event(format: &str, message: EventMessage) -> Result<Vec<u8>, String> {
    resolve(format)?.encode(&kernel_event_message(message)).map_err(wire)
}

/// Wire bytes in `format` -> an event. Bytes that are not an event in that format are a `codec:`
/// error.
pub fn decode_event(format: &str, wire: &[u8]) -> Result<EventMessage, String> {
    let message = resolve(format)?.decode::<edax_event::EventMessage>(wire).map_err(self::wire)?;
    Ok(record_event_message(message))
}

/// A command -> wire bytes in `format`, as [`encode_event`] does an event.
pub fn encode_command(format: &str, message: CommandMessage) -> Result<Vec<u8>, String> {
    resolve(format)?.encode(&kernel_command_message(message)).map_err(wire)
}

/// Wire bytes in `format` -> a command, as [`decode_event`] does an event.
pub fn decode_command(format: &str, wire: &[u8]) -> Result<CommandMessage, String> {
    let message = resolve(format)?.decode::<edax_event::CommandMessage>(wire).map_err(self::wire)?;
    Ok(record_command_message(message))
}

/// The media type that names `format` (`"json"` or `"msgpack"`) and `kind` (`"event"` or
/// `"command"`) on the wire, `application/vnd.edax.<kind>+<format>`: the string a transport that
/// carries a type puts there, such as a Kafka `content-type` header. It names the codec and not the
/// transport, so it lives here and not with the Kafka record. An unknown format or kind is a `codec:`
/// error. See [`edax_event::Format::media_type`].
pub fn media_type(format: &str, kind: &str) -> Result<String, String> {
    let kind = edax_event::Kind::from_name(kind).map_err(wire)?;
    Ok(resolve(format)?.media_type(kind).to_owned())
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

/// The write a saga step makes: `action` is `"nothing"`, `"save"` or `"complete"`, `expected` the
/// version a save or complete is conditional on, and `version` the one it commits, which the step's
/// command ids carry (both `0` for nothing). See [`edax_event::SagaWrite`].
pub struct SagaWrite {
    pub action: String,
    pub expected: u64,
    pub version: u64,
}

/// The write for a step whose fold `completes` the instance or not, with or without commands, over a
/// running instance loaded at `version` (`None` when nothing was loaded). A completed instance never
/// gets here: [`saga_admit`] skips it. See [`edax_event::saga_write`].
// `version` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn saga_write(completes: bool, has_commands: bool, version: Option<u64>) -> Result<SagaWrite, String> {
    let loaded = version.map_or(edax_event::Loaded::Absent, |version| edax_event::Loaded::Active { version });
    let (action, expected, version) = match edax_event::saga_write(loaded, completes, has_commands).map_err(wire)? {
        edax_event::SagaWrite::Nothing => ("nothing", 0, 0),
        edax_event::SagaWrite::Save { expected, version } => ("save", expected, version),
        edax_event::SagaWrite::Complete { expected, version } => ("complete", expected, version),
    };
    Ok(SagaWrite { action: action.to_owned(), expected, version })
}

/// Whether an instance that saved the marks `folded` (empty for a new instance), or is `completed`
/// (a tombstone), folds `message`: `None` when it does not (a tombstone, or a redelivery of an event
/// it folded: skip it, write nothing), else the marks to save with the step. Marks this function did
/// not write are a `codec:` error. See [`edax_event::saga_admit`].
pub fn saga_admit(completed: bool, folded: &str, message: EventMessage) -> Result<Option<String>, String> {
    let msg = kernel_event_message(message);
    Ok(match edax_event::saga_admit(completed, folded, &msg).map_err(wire)? {
        edax_event::SagaAdmit::Completed | edax_event::SagaAdmit::Repeat => None,
        edax_event::SagaAdmit::Fold { folded } => Some(folded),
    })
}

/// The id of the `index`-th command a saga step committed at `version`:
/// `"{instance_id}:{version}:{index}"`. See [`edax_event::saga_command_id`].
pub fn saga_command_id(instance_id: &str, version: u64, index: u64) -> String {
    edax_event::saga_command_id(instance_id, version, index)
}

/// The `index`-th command the instance `instance_id` of the saga named `saga` decided at the step that
/// committed `version`, folding the event `cause_id` of the flow `cause_correlation_id`: the row its
/// outbox holds and the message its relay sends. `command` is the host codec's output. Its `id` is
/// [`saga_command_id`], its `idempotency_key` that id under the saga's name, its aggregate the
/// instance, its tenant the folded event's, and its lineage the folded event's child. Where one store
/// holds every tenant, the instance id is the host's own composition, so nothing here composes again.
/// Pinned by `testdata/golden/saga.json`. See
/// [`edax_event::saga_command`].
#[allow(clippy::too_many_arguments)] // the kernel's own parameter list, one for one
pub fn saga_command(
    saga: &str,
    instance_id: &str,
    version: u64,
    index: u64,
    command: Event,
    cause_id: &str,
    cause_correlation_id: &str,
    issued_at: i64,
    tenant: Option<String>,
) -> CommandMessage {
    record_command_message(edax_event::saga_command(
        saga,
        instance_id,
        version,
        index,
        tenant.as_deref(),
        kernel_event(command),
        cause_id,
        cause_correlation_id,
        issued_at,
    ))
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
/// Events cross as structured [`EventMessage`] records and saga commands as [`CommandMessage`] records
/// (no JSON or wire encoding at the boundary);
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
    pub fn load_events(&self, tx: i64, aggregate_id: &str, after: u64) -> Result<Vec<EventMessage>, String> {
        let events = self.inner.event_store().load(tx, aggregate_id, after).map_err(wire)?;
        Ok(events.into_iter().map(record_event_message).collect())
    }

    /// Append `drafts` under `tx`, each stamped with `recorded_at`, and enqueue the same onto the
    /// outbox. Returns the stored events. The stream's last sequence as `tx` sees it must equal
    /// `expected_version`, and the batch must carry sequences `expected_version + 1, + 2, ...`.
    ///
    /// `recorded_at` comes from the host because a clock cannot cross the boundary: the host's store
    /// view reads its own injectable clock. Under an open `tx`, the returned events are not committed.
    pub fn append_events(
        &self,
        tx: i64,
        aggregate_id: &str,
        expected_version: u64,
        drafts: Vec<EventDraft>,
        recorded_at: i64,
    ) -> Result<Vec<EventMessage>, String> {
        let drafts = drafts.into_iter().map(kernel_event_draft).collect();
        let events =
            self.inner.event_store().append(tx, aggregate_id, expected_version, drafts, recorded_at).map_err(wire)?;
        Ok(events.into_iter().map(record_event_message).collect())
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
    pub fn unpublished(&self, limit: u64) -> Vec<EventMessage> {
        self.inner.event_store().unpublished(limit_of(limit)).into_iter().map(record_event_message).collect()
    }

    /// How many committed outbox rows are waiting, without paging them.
    pub fn unpublished_count(&self) -> u64 {
        u64::try_from(self.inner.event_store().unpublished_count()).unwrap_or(u64::MAX)
    }

    /// Mark outbox rows published by event id.
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

    /// Delete the settled record `key` holds under `tx`, so the next claim wins and a re-driven message
    /// runs its handler again. A pending claim is refused with an `in_progress:` error, because its
    /// run may still complete. A completed or failed record is deleted, and so is a record no reader
    /// decodes. An absent key is a no-op. The decision is the kernel's ([`edax_cqrs::forget`]).
    pub fn forget(&self, tx: i64, key: &str) -> Result<(), String> {
        self.inner.idempotency_store().forget(tx, key).map_err(wire)
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
        commands: Vec<CommandMessage>,
    ) -> Result<u64, String> {
        let state: serde_json::Value = parse_json("saga state", state_json)?;
        let commands = commands.into_iter().map(kernel_command_message).collect();
        self.inner.saga_store().save(tx, id, version, state, folded.to_owned(), commands).map_err(wire)
    }

    /// Complete the instance under `tx`: write its tombstone and the step's `commands` at
    /// `version + 1` under the same rules as [`save_saga`](Self::save_saga).
    pub fn complete_saga(&self, tx: i64, id: &str, version: u64, commands: Vec<CommandMessage>) -> Result<u64, String> {
        let commands = commands.into_iter().map(kernel_command_message).collect();
        self.inner.saga_store().complete(tx, id, version, commands).map_err(wire)
    }

    /// Committed saga outbox commands not yet sent, oldest commit first, capped at `limit`. The
    /// relay reads outside any transaction, so there is no token.
    pub fn unpublished_saga_commands(&self, limit: u64) -> Vec<CommandMessage> {
        self.inner.saga_store().unpublished(limit_of(limit)).into_iter().map(record_command_message).collect()
    }

    /// Mark saga outbox commands sent by id. Idempotent; an unknown id is a no-op.
    pub fn mark_saga_commands_published(&self, ids: Vec<String>) {
        self.inner.saga_store().mark_published(ids);
    }

    // -- command outbox --

    /// Stage `commands` in the command outbox under `tx`, to commit with the transaction's events. An
    /// id already waiting, or repeated in the batch, is a `codec:` error and stages nothing.
    pub fn enqueue_commands(&self, tx: i64, commands: Vec<CommandMessage>) -> Result<(), String> {
        let commands = commands.into_iter().map(kernel_command_message).collect();
        self.inner.command_outbox().enqueue(tx, commands).map_err(wire)
    }

    /// Committed outbox commands not yet sent, oldest commit first, capped at `limit`.
    pub fn unpublished_commands(&self, limit: u64) -> Vec<CommandMessage> {
        self.inner.command_outbox().unpublished(limit_of(limit)).into_iter().map(record_command_message).collect()
    }

    /// How many committed outbox commands wait, without paging them.
    pub fn unpublished_command_count(&self) -> u64 {
        u64::try_from(self.inner.command_outbox().unpublished_count()).unwrap_or(u64::MAX)
    }

    /// Mark outbox commands sent by id. Idempotent; an unknown id is a no-op.
    pub fn mark_commands_published(&self, ids: Vec<String>) {
        self.inner.command_outbox().mark_published(ids);
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
    /// The event delivered, or `None` when the message is a command.
    pub event: Option<EventMessage>,
    /// The command delivered, or `None` when the message is an event.
    pub command: Option<CommandMessage>,
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
    /// Append `message` to the partition of its ordering key, its aggregate. Every group reads it.
    pub fn publish_event(&self, message: EventMessage) {
        self.inner.publish_event(kernel_event_message(message));
    }

    /// Append `message` to the partition of its ordering key: its target aggregate, or its own id when
    /// it has none. Every group reads it.
    pub fn publish_command(&self, message: CommandMessage) {
        self.inner.publish_command(kernel_command_message(message));
    }

    /// Fetch the next message for `group` as an unsettled delivery, or `None` at once when the group
    /// has fetched everything. A group's first poll starts at the beginning.
    pub fn try_poll(&self, group: &str) -> Result<Option<PollResult>, String> {
        let polled = self.inner.try_poll(group).map_err(wire)?;
        Ok(polled.map(|d| {
            // Flattened into two optional fields: the shape every binding can cross.
            let (event, command) = match d.message {
                edax_memory::Message::Event(event) => (Some(record_event_message(event)), None),
                edax_memory::Message::Command(command) => (None, Some(record_command_message(command))),
            };
            PollResult { token: d.token, partition: d.partition, offset: d.offset, event, command }
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
    /// The partitioning key: the message's ordering key, or the host's override. UTF-8 bytes.
    pub key: Vec<u8>,
    /// The encoded message.
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

/// Build the record for value bytes the host already encoded, under `key`. `content_type` names the
/// format and kind of `value`. The key is the host's choice made: its override, or the message's
/// [`ordering_key`]. See [`edax_kafka::keyed_record`].
pub fn kafka_record(key: &str, value: Vec<u8>, content_type: &str) -> KafkaRecord {
    record_from_kafka_record(edax_kafka::keyed_record(key, value, content_type))
}

/// Where a fetched record goes by its `content-type`: flattened from [`edax_kafka::Route`]. No field
/// set means the default serializer; `registered` names the index of the media type in the list the
/// host passed; `unknown` carries a readable media type nobody registered, the empty one included;
/// `headerless` is a record with no readable content type under a command default. The last two are
/// poison.
pub struct KafkaRoute {
    /// The index of the matching media type in the host's registered list.
    pub registered: Option<u32>,
    /// The unregistered media type the record names.
    pub unknown: Option<String>,
    /// No readable content type, and the default serializer reads commands.
    pub headerless: bool,
}

/// Route a fetched record by `headers` against the media types the host registered, in its order.
/// `default_kind` is the kind its default serializer reads (`"event"` or `"command"`); anything else
/// is a `codec:` error. A null header value must cross as empty bytes, which routes as unknown. See
/// [`edax_kafka::route`].
pub fn kafka_route(headers: Vec<KafkaHeader>, registered: Vec<String>, default_kind: &str) -> Result<KafkaRoute, String> {
    let default = edax_event::Kind::from_name(default_kind).map_err(wire)?;
    let headers = kernel_headers(headers);
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    let none = KafkaRoute { registered: None, unknown: None, headerless: false };
    Ok(match edax_kafka::route(&headers, &registered, default) {
        edax_kafka::Route::Default => none,
        edax_kafka::Route::Headerless => KafkaRoute { headerless: true, ..none },
        edax_kafka::Route::Registered(index) => {
            // An index into a list the host passed across the binding fits a `u32` by construction.
            KafkaRoute { registered: Some(u32::try_from(index).unwrap_or(u32::MAX)), ..none }
        }
        edax_kafka::Route::Unknown(media_type) => KafkaRoute { unknown: Some(media_type), ..none },
    })
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

/// How many times a forget reads a key that changed under it before it gives up as transient. A call
/// because the generator binds functions, not constants. See [`edax_redis::FORGET_READS`].
pub fn redis_forget_reads() -> u32 {
    edax_redis::FORGET_READS
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

/// Whether forgetting `key`, which held `held` when it was read, deletes it: `true` means run the
/// forget script with `held` as its argument, `false` means the key was free and nothing is sent. A
/// pending claim is refused as `in_progress:`, naming `key`. See [`edax_redis::forget_outcome`].
pub fn redis_forget_outcome(key: &str, held: Option<Vec<u8>>) -> Result<bool, String> {
    Ok(match edax_redis::forget_outcome(key, held.as_deref()).map_err(wire)? {
        edax_redis::ForgetOutcome::Nothing => false,
        edax_redis::ForgetOutcome::Delete => true,
    })
}

/// The five kernel scripts, byte for byte, so every SDK's `EVALSHA` names one digest. A struct
/// rather than five calls, like [`SplitError`]. See [`edax_redis::CLAIM_SCRIPT`] and its siblings.
pub struct RedisScripts {
    pub claim: String,
    pub complete: String,
    pub release: String,
    pub extend: String,
    pub forget: String,
}

/// The five kernel scripts. See [`RedisScripts`].
pub fn redis_scripts() -> RedisScripts {
    RedisScripts {
        claim: edax_redis::CLAIM_SCRIPT.to_owned(),
        complete: edax_redis::COMPLETE_SCRIPT.to_owned(),
        release: edax_redis::RELEASE_SCRIPT.to_owned(),
        extend: edax_redis::EXTEND_SCRIPT.to_owned(),
        forget: edax_redis::FORGET_SCRIPT.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// The store's clock reading every fixture is recorded at.
    const RECORDED_AT: i64 = 1_700_000_000_000_000_000;

    /// One realistic event draft; every case overrides only the field under test.
    fn draft(id: &str, sequence: Option<u64>) -> EventDraft {
        EventDraft {
            tenant: None,
            id: id.to_owned(),
            aggregate_id: "invoice-7".to_owned(),
            sequence,
            name: "InvoiceIssued".to_owned(),
            version: 1,
            data: br#"{"invoice_id":"invoice-7","total":5000,"currency":"EUR"}"#.to_vec(),
            correlation_id: "corr-1".to_owned(),
            causation_id: "cmd-0".to_owned(),
            occurred_at: Some(1_699_999_999_000_000_000),
            extensions: HashMap::from([("traceparent".to_owned(), "00-4bf92f3577b34da6-00f067aa0ba902b7-01".to_owned())]),
            metadata: HashMap::from([("tenant".to_owned(), "acme".to_owned())]),
        }
    }

    /// The draft above as the store records it at [`RECORDED_AT`].
    fn event_message(id: &str, sequence: Option<u64>) -> EventMessage {
        record_event_message(kernel_event_draft(draft(id, sequence)).record(RECORDED_AT))
    }

    /// One realistic command message; every case overrides only the field under test.
    fn command_message(aggregate_id: Option<&str>) -> CommandMessage {
        CommandMessage {
            tenant: None,
            id: "cmd-1".to_owned(),
            idempotency_key: Some("req-42".to_owned()),
            aggregate_id: aggregate_id.map(str::to_owned),
            name: "IssueInvoice".to_owned(),
            version: 2,
            data: br#"{"invoice_id":"invoice-7","total":5000}"#.to_vec(),
            correlation_id: "corr-1".to_owned(),
            causation_id: "corr-1".to_owned(),
            issued_at: 1_700_000_000_000_000_001,
            extensions: HashMap::from([("http.origin_ip".to_owned(), "203.0.113.7".to_owned())]),
            metadata: HashMap::from([("tenant".to_owned(), "acme".to_owned())]),
        }
    }

    /// Field-by-field event equality. The record is a binding type with no `PartialEq` (alef owns the
    /// derives on the generated twin), and a whole-record assert is what proves nothing is dropped at
    /// the boundary.
    fn assert_same_event(got: &EventMessage, want: &EventMessage, case: &str) {
        assert_eq!((&got.id, &got.aggregate_id, got.sequence), (&want.id, &want.aggregate_id, want.sequence), "{case}");
        assert_eq!((&got.name, got.version, &got.data), (&want.name, want.version, &want.data), "{case}");
        assert_eq!((&got.correlation_id, &got.causation_id), (&want.correlation_id, &want.causation_id), "{case}");
        assert_eq!((got.occurred_at, got.recorded_at), (want.occurred_at, want.recorded_at), "{case}");
        assert_eq!((&got.extensions, &got.metadata), (&want.extensions, &want.metadata), "{case}");
    }

    /// Field-by-field command equality, for the reason [`assert_same_event`] gives.
    fn assert_same_command(got: &CommandMessage, want: &CommandMessage, case: &str) {
        assert_eq!((&got.id, &got.idempotency_key, &got.aggregate_id), (&want.id, &want.idempotency_key, &want.aggregate_id), "{case}");
        assert_eq!((&got.name, got.version, &got.data), (&want.name, want.version, &want.data), "{case}");
        assert_eq!((&got.correlation_id, &got.causation_id, got.issued_at), (&want.correlation_id, &want.causation_id, want.issued_at), "{case}");
        assert_eq!((&got.extensions, &got.metadata), (&want.extensions, &want.metadata), "{case}");
    }

    // ---- routing / sequencing ----------------------------------------------

    #[test]
    fn routing_and_sequencing_forward_their_arguments_in_order() {
        // A transposed argument is the one way a forwarding surface can drift from its kernel, so
        // every case is asymmetric: swapping the two arguments changes the answer.
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
    fn lineage_crosses_in_argument_order() {
        // (parent_id, parent_correlation_id): a swap would point causation at the flow.
        let child = propagate("evt-1", "corr-1");
        assert_eq!((child.correlation_id.as_str(), child.causation_id.as_str()), ("corr-1", "evt-1"));

        // (fresh_id, correlation_id, causation_id): with both ids given, a swap of the two shows. The
        // kernel's golden pins every branch.
        let root = originate("fresh-1", Some("corr-1".to_owned()), Some("evt-0".to_owned()));
        assert_eq!((root.correlation_id.as_str(), root.causation_id.as_str()), ("corr-1", "evt-0"));
    }

    #[test]
    fn extension_keys_follow_the_kernels_rule() {
        let check = |key: &str| check_extensions(HashMap::from([(key.to_owned(), "v".to_owned())]));
        assert_eq!(check("http.origin_ip"), Ok(()));
        assert_eq!(check_extensions(HashMap::new()), Ok(()));
        let refused = check("Traceparent").unwrap_err();
        assert!(refused.starts_with("codec: extension key \"Traceparent\""), "{refused}");
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
        assert_eq!(upcast_plan(3, 3, Some(vec![])).unwrap(), Vec::<u32>::new(), "already current");
        assert_eq!(upcast_plan(1, 3, None).unwrap(), Vec::<u32>::new(), "no transforms: identity");
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
    fn upcast_target_branches() {
        assert_eq!(upcast_target(None).unwrap(), 1, "no upcaster: the first version");
        assert_eq!(upcast_target(Some(vec![2, 1])).unwrap(), 3, "one past the highest step");
        // A wiring mistake fails the same way every time, so it carries the terminal `codec` code.
        assert_eq!(
            upcast_target(Some(vec![1, 3])).unwrap_err(),
            "codec: upcast steps skip v2: a payload stored at v2 could never be raised"
        );
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
        let env = |id: &str| event_message(id, Some(1));
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
            let write = saga_write(completes, cmds, version).unwrap();
            (write.action, write.expected, write.version)
        };
        assert_eq!(got(None, true, false), ("nothing".to_owned(), 0, 0));
        assert_eq!(got(None, false, false), ("save".to_owned(), 0, 1));
        assert_eq!(got(Some(3), false, false), ("save".to_owned(), 3, 4));
        assert_eq!(got(Some(3), true, true), ("complete".to_owned(), 3, 4));
        assert!(saga_write(false, true, Some(u64::MAX)).is_err_and(|e| e.starts_with("codec: saga instance at version")));
        assert_eq!(saga_command_id("order-7", 2, 1), "order-7:2:1");
    }

    #[test]
    fn saga_command_crosses_the_outbox_row() {
        let command = Event { name: "Fulfill".to_owned(), version: 3, data: br#"{"order_id":"order-7"}"#.to_vec() };
        let cause = event_message("evt-1", Some(1));
        let row = saga_command("shipping", "order-7", 2, 1, command, &cause.id, &cause.correlation_id, 7, None);
        let want = CommandMessage {
            tenant: None,
            id: "order-7:2:1".to_owned(),
            idempotency_key: Some("8:shipping:order-7:2:1".to_owned()),
            aggregate_id: Some("order-7".to_owned()),
            name: "Fulfill".to_owned(),
            version: 3,
            data: br#"{"order_id":"order-7"}"#.to_vec(),
            correlation_id: "corr-1".to_owned(),
            causation_id: "evt-1".to_owned(),
            issued_at: 7,
            extensions: HashMap::new(),
            metadata: HashMap::new(),
        };
        assert_same_command(&row, &want, "the cause's child, keyed and ordered by the instance");
    }

    // ---- idempotency -------------------------------------------------------

    #[test]
    fn idempotency_inputs_cross_in_argument_order() {
        // The event guard's key and hash cross as two fields: the event id, and the hash of the body.
        let rule = event_idempotency(event_message("evt-1", Some(1)));
        assert_eq!(rule.key, "evt-1");
        assert_eq!(rule.payload_hash, payload_hash(&event_message("evt-1", Some(1)).data));

        // (name, payload): the name reaches the hash, so another name over one payload is another hash.
        let payload = br#"{"invoice_id":"invoice-7","total":5000}"#;
        assert_ne!(command_hash_json("IssueInvoice", payload), command_hash_json("VoidInvoice", payload));
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

    // ---- message codec -----------------------------------------------------

    #[test]
    fn messages_round_trip_through_every_format() {
        for format in ["json", "msgpack"] {
            for sequence in [Some(7), None] {
                let wire = encode_event(format, event_message("evt-1", sequence)).unwrap();
                let back = decode_event(format, &wire).unwrap();
                assert_same_event(&back, &event_message("evt-1", sequence), &format!("{format} sequence={sequence:?}"));
            }
            for aggregate_id in [Some("invoice-7"), None] {
                let wire = encode_command(format, command_message(aggregate_id)).unwrap();
                let back = decode_command(format, &wire).unwrap();
                assert_same_command(&back, &command_message(aggregate_id), &format!("{format} aggregate={aggregate_id:?}"));
            }
            // (format, kind) in that order.
            for kind in ["event", "command"] {
                let minted = media_type(format, kind).unwrap();
                assert_eq!(minted, format!("application/vnd.edax.{kind}+{format}"));
            }
        }
    }

    // ---- error contract ----------------------------------------------------

    #[test]
    fn kernel_failures_carry_the_codec_code() {
        // Every deterministic marshaling failure must name its cause *and* carry the taxonomy code, or
        // the host falls back to a transient store error and retries a failure that always repeats.
        let valid_doc = r#"{"id":"user-1","age":36}"#;
        let cases: [(&str, Result<(), String>, &str); 22] = [
            ("filter_is_not_json", eval_filter("{not json}", valid_doc).map(|_| ()), "key must be a string at line 1 column 2"),
            ("filter_is_not_a_filter", eval_filter(r#"{"nope":1}"#, valid_doc).map(|_| ()), "unknown variant"),
            ("document_is_not_json", eval_filter(r#"{"exists":{"path":"id","exists":true}}"#, "nope").map(|_| ()), "expected ident"),
            ("query_spec_is_not_json", query("{not json}", "[]").map(|_| ()), "key must be a string at line 1 column 2"),
            ("cursor_is_not_base64", query(r#"{"sort":[{"path":"age"}],"after":"!!!"}"#, "[]").map(|_| ()), "not base64url"),
            ("cursor_document_is_not_json", encode_cursor("nope", "[]").map(|_| ()), "expected ident"),
            ("command_payload_is_not_json", payload_hash_json(b"{not json}").map(|_| ()), "key must be a string at line 1 column 2"),
            ("wire_bytes_are_not_the_format", decode_event("json", b"not an event").map(|_| ()), "decode"),
            ("command_bytes_are_not_the_format", decode_command("msgpack", b"not a command").map(|_| ()), "decode"),
            (
                "command_name_payload_is_not_json",
                command_hash_json("IssueInvoice", b"{not json}").map(|_| ()),
                "not one JSON value",
            ),
            (
                // A producer never mints a message every consumer would refuse.
                "event_extension_key_is_refused",
                encode_event("json", EventMessage { extensions: HashMap::from([("X-Trace".to_owned(), String::new())]), ..event_message("evt-1", Some(1)) })
                    .map(|_| ()),
                "extension key \"X-Trace\"",
            ),
            (
                "command_extension_key_is_refused",
                encode_command("msgpack", CommandMessage { extensions: HashMap::from([(String::new(), String::new())]), ..command_message(None) })
                    .map(|_| ()),
                "extension key \"\"",
            ),
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
            ("sequence_batch_has_a_gap", check_sequence_batch(1, vec![Some(2), Some(4)]), "event 1 has sequence 4, expected 3"),
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
                media_type("protobuf", "event").map(|_| ()),
                "unknown format: protobuf",
            ),
            ("media_type_kind_is_not_a_kind", media_type("json", "query").map(|_| ()), "unknown message kind: query"),
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
            encode_event("protobuf", event_message("evt-1", Some(1))).unwrap_err(),
            decode_event("protobuf", b"{}").map(|_| ()).unwrap_err(),
            encode_command("protobuf", command_message(None)).unwrap_err(),
            decode_command("protobuf", b"{}").map(|_| ()).unwrap_err(),
        ] {
            assert_eq!(message, "codec: unknown format: protobuf");
        }
    }

    // ---- in-memory stores --------------------------------------------------

    #[test]
    fn event_store_crosses_events_structurally() {
        let store = memory_transactor();
        // The store stamps the host's clock reading on every draft and hands back what it stored.
        let stored = store.append_events(NO_TX, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT).unwrap();
        assert_eq!(stored.len(), 1);
        assert_same_event(&stored[0], &event_message("evt-1", Some(1)), "appended event");
        assert!(store.append_events(NO_TX, "invoice-7", 1, vec![], RECORDED_AT).unwrap().is_empty(), "a version check");

        // Every field survives the boundary — no JSON or wire encoding in between.
        let page = store.load_events(NO_TX, "invoice-7", 0).unwrap();
        assert_eq!(page.len(), 1);
        assert_same_event(&page[0], &event_message("evt-1", Some(1)), "loaded event");
        assert!(store.load_events(NO_TX, "invoice-404", 0).unwrap().is_empty());

        // The adapter's own error code is forwarded untouched, not re-tagged as a codec failure.
        let conflict = store.append_events(NO_TX, "invoice-7", 0, vec![draft("evt-2", Some(2))], RECORDED_AT).map(|_| ()).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        let unsequenced = store.append_events(NO_TX, "invoice-7", 1, vec![draft("evt-2", None)], RECORDED_AT).map(|_| ()).unwrap_err();
        assert_eq!(unsequenced, "codec: event 0 has no sequence");

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
    fn forget_crosses_the_kernels_decision() {
        let store = memory_transactor();
        let done = r#"{"payload_hash":7,"result":null,"error":null,"pending":false}"#;

        // A pending claim may still complete, so it is refused with the kernel's code.
        let claim = store.claim(NO_TX, "req-A", 7).unwrap();
        let refused = store.forget(NO_TX, "req-A").unwrap_err();
        assert!(refused.starts_with("in_progress: "), "{refused}");

        // A settled record goes, so the next claim wins and a re-driven message runs again.
        store.complete(NO_TX, "req-A", claim.handle, done).unwrap();
        assert_eq!(store.forget(NO_TX, "req-A"), Ok(()));
        assert!(store.claim(NO_TX, "req-A", 7).unwrap().won, "a forgotten key is claimable again");
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
        let fulfill = || {
            let command = Event { name: "Fulfill".to_owned(), version: 1, data: br#"{"order_id":"order-1"}"#.to_vec() };
            let cause = event_message("evt-1", Some(1));
            saga_command("shipping", "order-1", 2, 0, command, &cause.id, &cause.correlation_id, 7, None)
        };
        assert_eq!(store.complete_saga(NO_TX, "order-1", 1, vec![fulfill()]).unwrap(), 2);
        let done = store.load_saga(NO_TX, "order-1").unwrap().expect("a tombstone");
        assert_eq!((done.version, done.completed, done.state_json.as_str(), done.folded.as_str()), (2, true, "null", ""));
        let unsent = store.unpublished_saga_commands(10);
        assert_eq!(unsent.len(), 1);
        assert_same_command(&unsent[0], &fulfill(), "the outbox row");
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
            store.append_events(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT).unwrap();
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

        broker.publish_event(event_message("evt-1", Some(1)));
        broker.publish_event(event_message("evt-1", Some(1))); // at-least-once: the same event republished
        assert!(!broker.is_empty("g"));

        let first = broker.try_poll("g").unwrap().expect("published");
        assert_eq!((first.partition, first.offset), (0, 0), "the coordinates cross");
        assert_same_event(first.event.as_ref().expect("an event"), &event_message("evt-1", Some(1)), "polled event");
        assert!(first.command.is_none(), "an event delivery carries no command");
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

        // A command crosses the same log, in the other field.
        broker.publish_command(command_message(Some("invoice-7")));
        let command = broker.try_poll("g").unwrap().expect("a command");
        assert!(command.event.is_none(), "a command delivery carries no event");
        assert_same_command(command.command.as_ref().expect("a command"), &command_message(Some("invoice-7")), "polled command");
    }

    // ---- kafka -------------------------------------------------------------

    /// Copy headers for a second call: the binding record carries no derives (alef owns them on the
    /// generated twin), and every header-taking call consumes its `Vec`.
    fn copy_headers(headers: &[KafkaHeader]) -> Vec<KafkaHeader> {
        headers.iter().map(|header| KafkaHeader { key: header.key.clone(), value: header.value.clone() }).collect()
    }

    /// What the boundary adds to the record kernel: the record crosses as a struct, so every field
    /// survives the conversion. The header set is the kernel's, pinned by
    /// `testdata/golden/kafka_record.json`.
    #[test]
    fn kafka_record_crosses_the_record_structurally() {
        let record = kafka_record("invoice-7", b"<invoice/>".to_vec(), "application/xml");
        assert_eq!(record.key.as_slice(), b"invoice-7".as_slice(), "the host's key, untouched");
        assert_eq!(record.value.as_slice(), b"<invoice/>".as_slice(), "the host's own bytes, untouched");
        let headers: Vec<(&str, &[u8])> =
            record.headers.iter().map(|h| (h.key.as_str(), h.value.as_slice())).collect();
        assert_eq!(
            headers,
            [(kafka_content_type_header().as_str(), b"application/xml".as_slice())],
            "a host serializer keeps the one contract header, under the kernel's name"
        );

        // The route crosses flattened: the default, an index into the host's list, or the unknown
        // media type — the empty one kept apart from no header at all.
        let route = |headers: Vec<KafkaHeader>| {
            let route = kafka_route(headers, vec!["application/json".to_owned(), "application/xml".to_owned()], "event").unwrap();
            (route.registered, route.unknown)
        };
        assert_eq!(route(copy_headers(&record.headers)), (Some(1), None));
        assert_eq!(route(Vec::new()), (None, None), "no headers at all: the default");
        let empty = vec![KafkaHeader { key: kafka_content_type_header(), value: Vec::new() }];
        assert_eq!(route(empty), (None, Some(String::new())), "an empty value is unknown, not the default");
        let headerless = kafka_route(Vec::new(), Vec::new(), "command").unwrap();
        assert!(headerless.headerless, "no header under a command default is poison");
        assert!(kafka_route(Vec::new(), Vec::new(), "query").is_err(), "an unknown kind is refused");

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

        // Topic, partition and offset together are the identity of a message that did not decode.
        assert_eq!(kafka_poison_id("orders", 3, 42), "orders/3/42");

        // Placement: `(key, partitions)` in that order, Java's rule, and no partitions is refused.
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
        assert_eq!(redis_forget_reads(), 3);
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
