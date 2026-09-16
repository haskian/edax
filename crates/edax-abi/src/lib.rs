//! Flat, plain-Rust API surface over the edax kernels and adapters.
//!
//! This crate holds no logic of its own: every function forwards to a kernel
//! (`edax-core` / `edax-event` / `edax-cqrs` / `edax-kafka` / `edax-redis`) or to a backend
//! (`edax-memory`), reshaping only at the edges so the surface is trivial to bind. alef extracts
//! these signatures and emits the native binding from them, so the shapes here
//! are chosen to map cleanly: fallible calls return `Result<_, String>` (the
//! generator raises on `Err`), bytes are `Vec<u8>` / `&[u8]`, dictionaries are
//! `HashMap<String, String>` (converted to the kernel's ordered `BTreeMap` where
//! deterministic encoding requires it), and an aggregate result is a plain struct
//! rather than a tuple.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use edax_core::ErrorCode;

// The kernel's nested envelope is referred to fully-qualified (`edax_event::EventEnvelope`); the names
// `Event` / `EventEnvelope` below are this crate's own binding-facing records, shaped to mirror it.

// ---- routing / sequencing ------------------------------------------------

/// Partition index for `aggregate_id` over `n` partitions.
pub fn partition(aggregate_id: &str, n: u32) -> u32 {
    edax_event::partition(aggregate_id, n)
}

/// Rendezvous-hashed owner index for `aggregate_id` over `n` candidates.
pub fn rendezvous(aggregate_id: &str, n: u32) -> u32 {
    edax_event::rendezvous(aggregate_id, n)
}

/// Whether an incoming sequence should be applied given the stored one.
///
/// Staleness only, not dedup: the comparison is `stored <= incoming`, so a redelivery of the
/// sequence already applied returns `true`. Guard the handler with `idempotent_by_id`, or a ledger
/// keyed on the event id, when its effect is not itself idempotent. See
/// [`edax_event::should_apply`] and `docs/kernels/event.md`.
pub fn should_apply(stored: Option<u64>, incoming: Option<u64>) -> bool {
    edax_event::should_apply(stored, incoming)
}

/// Check an append batch against the dense-sequence rule: every envelope carries a sequence and the
/// batch runs `expected + 1, expected + 2, ...` in order. `Err` names the 0-based batch index and is
/// deterministic, so a host tags it terminal. An empty batch passes: it is a version check only.
pub fn check_sequence_batch(expected: i64, sequences: Vec<Option<u64>>) -> Result<(), String> {
    edax_event::sequence::check_batch(expected, &sequences)
}

/// Backoff delay in milliseconds for a retry `attempt`.
pub fn backoff_millis(attempt: u32) -> u64 {
    edax_core::backoff_millis(attempt)
}

/// A fresh time-ordered globally-unique id (UUID v7), as a string. Non-deterministic.
pub fn guid() -> String {
    edax_core::guid()
}

/// Current Unix time in nanoseconds, strictly increasing per process. Non-deterministic.
pub fn current_timestamp() -> u64 {
    edax_core::current_timestamp()
}

// ---- error mapping -------------------------------------------------------

/// Tag a failure with the `codec` code of the boundary's error taxonomy.
///
/// Errors cross as `"<code>: <message>"` and the host maps the code to its own exception type. The
/// kernels are language-neutral and emit a bare message, so the binding — the layer that owns
/// error-to-exception mapping — attaches the code. `codec` is the deterministic marshaling failure:
/// malformed JSON, an unknown wire format, undecodable bytes. Without a code a host falls back to a
/// *transient* store error and retries a failure that repeats identically every time.
///
/// Errors forwarded from `edax-memory` already carry their own code (`concurrency` / `store` /
/// `codec` / `bad_handle`) and must not be tagged again: they cross through [`adapter_error`].
fn codec_error(cause: impl std::fmt::Display) -> String {
    ErrorCode::Codec.tag(cause)
}

/// A backend failure as the wire form. The kernel's typed error renders its own `"<code>: <message>"`
/// through the kernel's tag, so its `Display` is the whole conversion — nothing is added here.
fn adapter_error(cause: edax_core::AdapterError) -> String {
    cause.to_string()
}

/// Every code of the error taxonomy, in the kernel's order. A host builds its exception table from
/// this list rather than restating it, so a code the kernel adds is a code the host must map.
pub fn error_codes() -> Vec<String> {
    ErrorCode::ALL.iter().map(|code| code.as_str().to_owned()).collect()
}

/// Whether `code` is terminal: it fails the same way on every retry, so an idempotency guard caches
/// it instead of releasing the claim. `Err` for a code the kernel does not know — the host keeps its
/// own default for those; the kernel does not guess.
pub fn error_code_is_terminal(code: &str) -> Result<bool, String> {
    code.parse::<ErrorCode>().map(ErrorCode::is_terminal)
}

/// A wire failure taken apart. A struct rather than a tuple: the generator sanitizes a tuple return
/// to `Vec<String>` and loses the field names.
pub struct SplitError {
    /// The taxonomy code, or empty when the wire form carries no code the kernel knows.
    pub code: String,
    /// The message after the separator, or the whole wire string when `code` is empty.
    pub message: String,
}

/// Split a wire failure `"<code>: <message>"` into its code and its message.
///
/// An unknown code, or a string with no separator, yields an empty code and the whole string
/// unchanged, so a host applies its own default for those and still logs the original text. The
/// separator is the kernel's; a host that split the string itself would own a second copy of it.
pub fn split_error(wire: &str) -> SplitError {
    match ErrorCode::split(wire) {
        Some((code, message)) => SplitError { code: code.as_str().to_owned(), message: message.to_owned() },
        None => SplitError { code: String::new(), message: wire.to_owned() },
    }
}

// ---- filter / query ------------------------------------------------------

/// Evaluate a filter (JSON) against a document (JSON). The match decision — including the two-valued
/// null/missing-path rules — is the kernel's. (The boundary is JSON text: alef has no object-passing
/// for arbitrary values, so structured crossing isn't available here.)
pub fn eval_filter(filter: &str, doc: &str) -> Result<bool, String> {
    edax_core::eval_json(filter.as_bytes(), doc.as_bytes()).map_err(codec_error)
}

/// Apply a read query (JSON string) to a JSON array of documents; returns the page as a JSON string.
pub fn query(query: &str, docs: &str) -> Result<String, String> {
    let out = edax_event::query::query_json(query.as_bytes(), docs.as_bytes()).map_err(codec_error)?;
    String::from_utf8(out).map_err(codec_error)
}

/// Encode a keyset cursor (base64 string) from a document (JSON string) and sort keys (JSON string).
pub fn encode_cursor(doc: &str, sort: &str) -> Result<String, String> {
    let out = edax_event::query::encode_cursor_json(doc.as_bytes(), sort.as_bytes()).map_err(codec_error)?;
    String::from_utf8(out).map_err(codec_error)
}

/// Decode keyset cursor (base64 string) value tuple (JSON array string) sort keys (JSON string).
///
/// The inverse of [`encode_cursor`]: a backend turns `Query::after` into the bound values of its own
/// `WHERE` clause without re-deriving the wire format. Aligned to the tiebroken sort, so a short
/// tuple pads with null and a long one truncates, matching what the reference applier compares.
pub fn decode_cursor(cursor: &str, sort: &str) -> Result<String, String> {
    let out = edax_event::query::decode_cursor_json(cursor.as_bytes(), sort.as_bytes()).map_err(codec_error)?;
    String::from_utf8(out).map_err(codec_error)
}

/// Sort keys a page ordered by (JSON string), `id` tiebreak appended sort keys (JSON string).
///
/// A backend ordering by the declared sort alone disagrees with the kernel at every tie.
pub fn effective_sort(sort: &str) -> Result<String, String> {
    let out = edax_event::query::effective_sort_json(sort.as_bytes()).map_err(codec_error)?;
    String::from_utf8(out).map_err(codec_error)
}

// ---- upcast / causation --------------------------------------------------

/// Ordered upcast steps from `from_version` to `to` given the available step versions.
///
/// A gap in the steps, or a downward plan, is `codec`: the event cannot be unmarshaled to the target
/// version, and no extra attempt changes that. Untagged it would read as a transient store error and
/// be retried forever.
pub fn upcast_plan(from_version: i32, to: i32, available: Vec<i32>) -> Result<Vec<i32>, String> {
    edax_event::upcast_plan(from_version, to, &available).map_err(codec_error)
}

/// Causation/correlation metadata for a child event given its parent's metadata and event id.
pub fn propagate(parent_metadata: HashMap<String, String>, parent_event_id: &str) -> HashMap<String, String> {
    let parent: BTreeMap<String, String> = parent_metadata.into_iter().collect();
    edax_event::propagate(&parent, parent_event_id).into_iter().collect()
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

/// Canonicalize a UTF-8 JSON command payload and hash it. Canonicalization (key order, number/string
/// form) lives in the kernel, so a reused idempotency key binds to the same hash in every binding —
/// don't canonicalize on the host side.
pub fn payload_hash_json(payload: &[u8]) -> Result<u64, String> {
    let canonical = edax_cqrs::canonical_json(payload).map_err(codec_error)?;
    Ok(edax_cqrs::payload_hash(&canonical))
}

/// Idempotency decision: `"proceed"`, `"replay"`, or `"payload_mismatch"`. `prior` is the stored
/// payload hash for the key (None if unseen); `current` is this command's hash.
// `prior` is optional, so it trails `current` — alef moves optional parameters to the end of the
// generated signature, and the surface order must match for the generated call to line up.
pub fn idempotency_decision(current: u64, prior: Option<u64>) -> Result<String, String> {
    // `Decision::as_str` is the single source of truth for the string form and is exhaustive in
    // `edax-cqrs`, so a future `Decision` variant is a compile error there (no local re-match to drift).
    Ok(edax_cqrs::decide(prior, current).as_str().to_owned())
}

// ---- idempotency record --------------------------------------------------

/// A stored idempotency record, decoded by the kernel. `state` is `"pending"`, `"failed"` or
/// `"done"`; `error` is set only for `"failed"` and `result_json` (the cached result as JSON text)
/// only for `"done"` — the others are empty. Flat strings rather than options so the shape binds
/// without an optional-field conversion.
pub struct IdempotencyRecord {
    pub payload_hash: u64,
    pub state: String,
    pub error: String,
    pub result_json: String,
}

/// The canonical JSON record for a claim, a cached success, or a cached failure. The kernel applies
/// its reading order to the flat inputs (`pending` wins, then `error`, then `result_json`), so a
/// host that models the record as four fields cannot write an ambiguous one. `result_json` is the
/// host's result as JSON text; it is read only when the record is neither pending nor failed.
pub fn encode_idempotency_record(
    payload_hash: u64,
    pending: bool,
    result_json: &str,
    error: Option<String>,
) -> Result<String, String> {
    let record = match edax_cqrs::State::from_wire(pending, error, ()) {
        edax_cqrs::State::Pending => edax_cqrs::Record::<serde_json::Value>::pending(payload_hash),
        edax_cqrs::State::Failed(error) => edax_cqrs::Record::failed(payload_hash, error),
        edax_cqrs::State::Done(()) => {
            let result: serde_json::Value = serde_json::from_str(result_json).map_err(codec_error)?;
            edax_cqrs::Record::done(payload_hash, result)
        }
    };
    let bytes = edax_cqrs::encode_record(&record).map_err(codec_error)?;
    String::from_utf8(bytes).map_err(codec_error)
}

/// Decode a stored idempotency record with the kernel's reading order. A record that is not a JSON
/// object, lacks `payload_hash`, or carries a non-integer hash is a `codec:` error.
pub fn decode_idempotency_record(record_json: &str) -> Result<IdempotencyRecord, String> {
    let record = edax_cqrs::decode_record::<serde_json::Value>(record_json.as_bytes()).map_err(codec_error)?;
    let state = record.state.as_str().to_owned();
    let (error, result_json) = match record.state {
        edax_cqrs::State::Pending => (String::new(), String::new()),
        edax_cqrs::State::Failed(error) => (error, String::new()),
        edax_cqrs::State::Done(result) => (String::new(), result.to_string()),
    };
    Ok(IdempotencyRecord { payload_hash: record.payload_hash, state, error, result_json })
}

/// What a claimant that lost the claim does with the winner's record: `"payload_mismatch"`,
/// `"in_progress"`, `"failed"` (replay the cached error) or `"succeeded"` (replay the cached result).
/// Takes the record's classifying fields rather than its JSON so a host that already decoded the
/// record does not re-encode it; the result plays no part in the verdict.
pub fn idempotency_outcome(
    current: u64,
    payload_hash: u64,
    pending: bool,
    error: Option<String>,
) -> Result<String, String> {
    let prior = edax_cqrs::Record { payload_hash, state: edax_cqrs::State::from_wire(pending, error, ()) };
    Ok(edax_cqrs::outcome(&prior, current).as_str().to_owned())
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
/// [`envelope_from_record`] / [`record_from_envelope`] map it to and from the kernel envelope (only the
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

/// Binding record -> the kernel's nested envelope (HashMap -> the kernel's ordered BTreeMap).
fn envelope_from_record(r: EventEnvelope) -> edax_event::EventEnvelope {
    edax_event::EventEnvelope {
        id: r.id,
        aggregate_id: r.aggregate_id,
        event: edax_event::Event { name: r.event.name, version: r.event.version, data: r.event.data },
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
        event: Event { name: env.event.name, version: env.event.version, data: env.event.data },
        sequence: env.sequence,
        namespace: env.namespace.into_iter().collect(),
        metadata: env.metadata.into_iter().collect(),
        timestamp: env.timestamp,
    }
}

fn resolve(format: &str) -> Result<edax_event::Format, String> {
    edax_event::Format::from_name(format).map_err(codec_error)
}

/// Envelope fields -> wire bytes in `format` (`"json"` or `"msgpack"`). Takes the fields flat (not an
/// [`EventEnvelope`]) so any host model backend can be encoded by reading its fields, without first
/// building a binding record.
#[allow(clippy::too_many_arguments)]
pub fn encode_envelope(
    format: &str,
    id: String,
    aggregate_id: String,
    event_name: String,
    event_version: i32,
    event_data: Vec<u8>,
    namespace: HashMap<String, String>,
    metadata: HashMap<String, String>,
    timestamp: i64,
    // Trails the required parameters: alef moves optional parameters to the end of the generated
    // signature, and the surface order must match for the generated call to line up.
    sequence: Option<u64>,
) -> Result<Vec<u8>, String> {
    let env = envelope_from_record(EventEnvelope {
        id,
        aggregate_id,
        event: Event { name: event_name, version: event_version, data: event_data },
        sequence,
        namespace,
        metadata,
        timestamp,
    });
    Ok(resolve(format)?.encode(&env))
}

/// Wire bytes in `format` -> a decoded [`EventEnvelope`] record (nested `event`).
pub fn decode_envelope(format: &str, wire: &[u8]) -> Result<EventEnvelope, String> {
    let env = resolve(format)?.decode(wire).map_err(codec_error)?;
    Ok(record_from_envelope(env))
}

// ---- in-memory stores behind one transaction boundary ---------------------

/// A loaded page of envelopes. A single struct wrapping the `Vec` so an `async` method can return it:
/// the binding generator infers the element conversion through this pinned wrapper, where a bare
/// `Vec<EventEnvelope>` return from an `async` method would not.
pub struct EnvelopePage {
    pub items: Vec<EventEnvelope>,
}

/// The result of a snapshot load: `present` is false when there is no snapshot (the other fields are
/// then empty). A struct rather than an `Option<SnapshotRecord>` for the same reason as
/// [`EnvelopePage`] — an `async` method returning `Option<Struct>` cannot infer its conversion.
pub struct SnapshotResult {
    pub present: bool,
    pub type_name: String,
    pub version: i64,
    pub data: Vec<u8>,
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
/// specs cross as JSON text.
pub struct MemoryTransactor {
    inner: Arc<edax_memory::MemoryTransactor>,
}

// Store methods are async so the host awaits every store the same way regardless of backend; the
// work is in-memory and synchronous, so each just returns a ready future. Collection/optional
// returns are wrapped in [`EnvelopePage`] / [`SnapshotResult`] so the generator can infer their
// conversions. The boundary calls (`begin` / `commit` / `rollback`) are synchronous, like the
// broker's: the host's context manager runs them on entry and exit, and there is nothing to await.
impl MemoryTransactor {
    /// Open a transaction and return its token. Never [`NO_TX`].
    pub fn begin(&self) -> i64 {
        self.inner.begin()
    }

    /// Apply the transaction's writes on every seam atomically and close it. A token that names no
    /// open transaction is refused with a `bad_handle:` (terminal) error.
    pub fn commit(&self, tx: i64) -> Result<(), String> {
        self.inner.commit(tx).map_err(adapter_error)
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
    pub async fn load_events(&self, tx: i64, aggregate_id: &str, after: i64) -> Result<EnvelopePage, String> {
        let events = self.inner.event_store().load(tx, aggregate_id, after).map_err(adapter_error)?;
        Ok(EnvelopePage { items: events.into_iter().map(record_from_envelope).collect() })
    }

    /// Append `events` under `tx` and enqueue the same onto the outbox; the stream's last sequence as
    /// `tx` sees it must equal `expected_version`, and the batch must carry sequences
    /// `expected_version + 1, + 2, ...`.
    pub async fn append_events(
        &self,
        tx: i64,
        aggregate_id: &str,
        expected_version: i64,
        events: Vec<EventEnvelope>,
    ) -> Result<(), String> {
        let envs = events.into_iter().map(envelope_from_record).collect();
        self.inner.event_store().append(tx, aggregate_id, expected_version, envs).map_err(adapter_error)
    }

    /// The snapshot for `aggregate_id` as `tx` sees it (`present` is false when there is none).
    pub async fn load_snapshot(&self, tx: i64, aggregate_id: &str) -> Result<SnapshotResult, String> {
        Ok(match self.inner.event_store().load_snapshot(tx, aggregate_id).map_err(adapter_error)? {
            Some(s) => {
                SnapshotResult { present: true, type_name: s.type_name, version: s.version, data: s.data }
            }
            None => SnapshotResult { present: false, type_name: String::new(), version: 0, data: Vec::new() },
        })
    }

    /// Store a snapshot for `aggregate_id` under `tx`.
    pub async fn save_snapshot(
        &self,
        tx: i64,
        aggregate_id: &str,
        type_name: &str,
        version: i64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.inner.event_store().save_snapshot(tx, aggregate_id, type_name, version, data).map_err(adapter_error)
    }

    /// Committed outbox events not yet published, capped at `limit`. The relay reads outside any
    /// transaction, so there is no token.
    pub async fn unpublished(&self, limit: i64) -> Result<EnvelopePage, String> {
        Ok(EnvelopePage { items: self.inner.event_store().unpublished(limit).into_iter().map(record_from_envelope).collect() })
    }

    /// Mark outbox rows published by envelope id.
    pub async fn mark_published(&self, ids: Vec<String>) -> Result<(), String> {
        self.inner.event_store().mark_published(ids);
        Ok(())
    }

    // -- idempotency store --

    /// Atomically claim `key` under `tx` with a pending record. Returns `None` if this caller won the
    /// claim, or the prior record JSON if the key was already present — the host decodes it to decide
    /// replay, payload mismatch, or that the winner has not completed yet. A key another open
    /// transaction holds reads as its pending record.
    pub async fn claim(&self, tx: i64, key: &str, payload_hash: u64) -> Result<Option<String>, String> {
        match self.inner.idempotency_store().claim(tx, key, payload_hash).map_err(adapter_error)? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(codec_error)?)),
            None => Ok(None),
        }
    }

    /// Write the terminal record JSON for `key` under `tx`, replacing the pending claim.
    pub async fn complete(&self, tx: i64, key: &str, record_json: &str) -> Result<(), String> {
        self.inner.idempotency_store().complete(tx, key, record_json.as_bytes()).map_err(adapter_error)
    }

    /// Drop the key under `tx`, releasing a claim (e.g. after a transient failure that should be
    /// retried).
    pub async fn release(&self, tx: i64, key: &str) -> Result<(), String> {
        self.inner.idempotency_store().release(tx, key).map_err(adapter_error)
    }

    // -- projection store --

    /// The stored entity for `id` as `tx` sees it, as JSON `{"id","version","document"}`, or `None`.
    pub async fn get_projection(&self, tx: i64, id: &str) -> Result<Option<String>, String> {
        self.inner.projection_store().get(tx, id).map_err(adapter_error)
    }

    /// Persist `document` under `tx` at `version + 1` if the stored version still equals `version`;
    /// returns the new version.
    pub async fn save_projection(&self, tx: i64, id: &str, version: i64, document_json: &str) -> Result<i64, String> {
        self.inner.projection_store().save(tx, id, version, document_json).map_err(adapter_error)
    }

    /// Run a read query (a JSON spec) over all documents as `tx` sees them; returns the page as a
    /// JSON array.
    pub async fn query_projections(&self, tx: i64, query_json: &str) -> Result<String, String> {
        self.inner.projection_store().get_many(tx, query_json).map_err(adapter_error)
    }

    // -- saga store --

    /// The stored instance for `id` as `tx` sees it, as JSON `{"id","version","state"}`, or `None`.
    pub async fn load_saga(&self, tx: i64, id: &str) -> Result<Option<String>, String> {
        self.inner.saga_store().load(tx, id).map_err(adapter_error)
    }

    /// Persist `state` under `tx` at `version + 1` if the stored version still equals `version` (an
    /// absent instance is at version 0); returns the new version.
    pub async fn save_saga(&self, tx: i64, id: &str, version: i64, state_json: &str) -> Result<i64, String> {
        self.inner.saga_store().save(tx, id, version, state_json).map_err(adapter_error)
    }

    /// Remove the instance at `id` under `tx` if the stored version still equals `version`; an absent
    /// instance is a no-op.
    pub async fn delete_saga(&self, tx: i64, id: &str, version: i64) -> Result<(), String> {
        self.inner.saga_store().delete(tx, id, version).map_err(adapter_error)
    }
}

/// A fresh in-memory database with every seam behind one transaction boundary.
pub fn memory_transactor() -> MemoryTransactor {
    MemoryTransactor { inner: Arc::new(edax_memory::MemoryTransactor::new()) }
}

// ---- in-memory broker --------------------------------------------------

/// The result of a non-blocking poll: `present` is false when the queue was empty (the other fields
/// are then the placeholder token and an empty envelope). A struct rather than an `Option<Struct>` for
/// the same reason as [`SnapshotResult`].
pub struct PollResult {
    pub present: bool,
    /// The delivery token that settles this delivery through [`MemoryBroker::ack`] / [`MemoryBroker::nack`].
    pub token: i64,
    pub envelope: EventEnvelope,
}

/// In-memory broker for development and tests: one FIFO queue plus the accounting of polled deliveries
/// not yet settled. Every method returns at once — the waiting for a message is the host's, so a poll
/// never blocks the host's event loop: the host loops over [`try_poll`](MemoryBroker::try_poll) and
/// parks on its own signal between empty polls. Settlement is by token, not by callback (a closure
/// cannot cross the binding): the host builds its own delivery whose ack / nack capture the token.
/// Construct it with [`memory_broker`].
pub struct MemoryBroker {
    inner: Arc<edax_memory::MemoryBroker>,
}

// Synchronous, unlike the stores: the host's `empty()` is a synchronous predicate, and the peek is
// non-blocking by contract, so there is nothing for the host to await.
impl MemoryBroker {
    /// Queue `envelope` at the tail.
    pub fn publish(&self, envelope: EventEnvelope) {
        self.inner.publish(envelope_from_record(envelope));
    }

    /// Take the head of the queue as an unsettled delivery, or a `present: false` result at once when
    /// the queue is empty.
    pub fn try_poll(&self) -> PollResult {
        match self.inner.try_poll() {
            Some(d) => PollResult { present: true, token: d.token, envelope: record_from_envelope(d.envelope) },
            None => PollResult { present: false, token: edax_memory::NO_DELIVERY, envelope: empty_envelope() },
        }
    }

    /// Settle the delivery `token` as handled. `false` if the token is not outstanding (already
    /// settled, or never issued); nothing changes then.
    pub fn ack(&self, token: i64) -> bool {
        self.inner.ack(token)
    }

    /// Settle the delivery `token` as failed: its envelope is requeued at the tail. `false` if the
    /// token is not outstanding; nothing is requeued then.
    pub fn nack(&self, token: i64) -> bool {
        self.inner.nack(token)
    }

    /// True only when nothing is queued *and* every polled delivery has been settled.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// The envelope a `present: false` [`PollResult`] carries: every field empty.
fn empty_envelope() -> EventEnvelope {
    EventEnvelope {
        id: String::new(),
        aggregate_id: String::new(),
        event: Event { name: String::new(), version: 0, data: Vec::new() },
        sequence: None,
        namespace: HashMap::new(),
        metadata: HashMap::new(),
        timestamp: 0,
    }
}

/// A fresh in-memory broker.
pub fn memory_broker() -> MemoryBroker {
    MemoryBroker { inner: Arc::new(edax_memory::MemoryBroker::new()) }
}

// ---- kafka ---------------------------------------------------------------

/// One Kafka record header, mirroring the kernel's `edax_kafka::KafkaHeader`. The value crosses as
/// bytes and not text: a foreign header on the same topic may carry anything, and the boundary hands
/// it back unchanged rather than lose it.
pub struct KafkaHeader {
    /// The header name, as the client hands it over.
    pub key: String,
    /// The header value, uninterpreted.
    pub value: Vec<u8>,
}

/// One Kafka record — what a producer sends, or what a consumer fetched — minus the client's own
/// coordinates (topic, partition, offset), which stay with the host's IO shell. [`kafka_record`]
/// builds one from the host's value bytes and [`encode_kafka_record`] from the kernel codec; the host
/// builds one itself to hand a fetched record to [`kafka_content_type`] / [`decode_kafka_record`].
pub struct KafkaRecord {
    /// The partitioning key: the aggregate id, or the host's override. UTF-8 bytes.
    pub key: Vec<u8>,
    /// The encoded envelope.
    pub value: Vec<u8>,
    /// The contract headers, in the order the kernel writes them.
    pub headers: Vec<KafkaHeader>,
}

/// Binding headers -> the kernel's, for a media-type lookup or a decode.
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

/// The media type naming `format` (`"json"` or `"msgpack"`) on the wire, for a record's
/// `content-type` header. A format the kernel does not mint a media type for is a `codec:` error and
/// never a fallback, so a record cannot carry a header that names the wrong codec.
pub fn kafka_media_type(format: &str) -> Result<String, String> {
    edax_kafka::media_type(resolve(format)?).map(|media_type| media_type.to_owned()).map_err(codec_error)
}

/// Build the record for `envelope` from value bytes the host already encoded, so a host serializer
/// keeps the kernel's key rule and header set whatever the bytes are; `content_type` names the format
/// of `value`. `key` overrides the partitioning key; `None` uses the aggregate id, which is what keeps
/// one aggregate's events on one partition and therefore in order.
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

/// The media type `headers` name for the value, or `""` when they name no readable one.
///
/// An empty string rather than an `Option`: a media type is never empty, and the host branches on
/// "is there one" either way. The first `content-type` wins, matched case-sensitively — the kernel's
/// rule, so two SDKs reading one topic pick the same header.
pub fn kafka_content_type(headers: Vec<KafkaHeader>) -> String {
    let headers = kernel_headers(headers);
    edax_kafka::content_type(&headers).unwrap_or_default().to_owned()
}

/// Build the record for `envelope` with the kernel codec end to end: the value encoded in `format`
/// and the matching media type in the header.
pub fn encode_kafka_record(
    format: &str,
    envelope: EventEnvelope,
    key: Option<String>,
) -> Result<KafkaRecord, String> {
    let env = envelope_from_record(envelope);
    let record = edax_kafka::encode_record(resolve(format)?, &env, key.as_deref()).map_err(codec_error)?;
    Ok(record_from_kafka_record(record))
}

/// Decode a fetched record back to its envelope.
///
/// The record's own `content-type` decides the format; a media type the kernel does not mint is a
/// `codec:` (terminal) error and never a silent fallback, so a producer rollout the consumer cannot
/// read becomes one poison message instead of a stream of decode failures. A record naming no
/// readable media type falls back to `default_format`, so a header-less record still decodes.
pub fn decode_kafka_record(
    default_format: &str,
    value: &[u8],
    headers: Vec<KafkaHeader>,
) -> Result<EventEnvelope, String> {
    let default = resolve(default_format)?;
    let headers = kernel_headers(headers);
    let env = edax_kafka::decode_record(default, value, &headers).map_err(codec_error)?;
    Ok(record_from_envelope(env))
}

/// The identity of a message that did not decode: `"{topic}/{partition}/{offset}"`. A poison message
/// has no envelope and so no id, and the offset alone repeats on every partition — a dead-letter
/// queue keyed on the offset would keep one partition's poison and reject another's.
pub fn kafka_poison_id(topic: &str, partition: i32, offset: i64) -> String {
    edax_kafka::poison_id(topic, partition, offset)
}

/// What [`OffsetTracker::ack`] tells the host to do: commit `offset` as the partition's watermark, or
/// nothing at all.
pub struct AckDecision {
    /// True when the watermark is higher than the last one the host reported committed.
    pub commit: bool,
    /// The watermark to commit, or `-1` when `commit` is false because the token is stale or unknown.
    pub offset: i64,
}

/// What [`OffsetTracker::nack`] tells the host to do: seek the partition back to `offset` so the
/// delivery comes again, or nothing at all.
pub struct NackDecision {
    /// True when the partition should be sought back.
    pub seek: bool,
    /// The offset to seek to — the lowest still in flight — or `-1` when `seek` is false.
    pub offset: i64,
}

/// One partition's bookkeeping, for a health check or a gauge.
pub struct PartitionStats {
    /// The topic this partition belongs to.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
    /// Offsets delivered but not yet settled.
    pub unsettled: u64,
    /// The highest offset booked, or `-1` before the first booking.
    pub max_delivered: i64,
    /// The last watermark the host reported committed, or `-1` before the first commit.
    pub committed: i64,
}

/// A snapshot of every assigned partition. A single struct wrapping the `Vec` for the same reason as
/// [`EnvelopePage`]: the generator infers the element conversion through a pinned wrapper.
pub struct PartitionStatsPage {
    /// One row per assigned partition, ordered by topic then partition.
    pub items: Vec<PartitionStats>,
}

/// The consumer's offset bookkeeping: one commit watermark per partition, settled by token.
///
/// A dispatcher acks in worker order, not in fetch order, so a bare commit would skip past an offset
/// still in flight and lose it on restart. [`ack`](Self::ack) instead names a watermark — the lowest
/// offset still in flight, or one past the highest delivered when none remain — and the host performs
/// the commit, then reports it back with [`committed`](Self::committed), so a client commit that
/// failed is not recorded as one. Settlement is by token, like the broker's deliveries: a token names
/// one delivery for the tracker's whole life, so an ack from before a rebalance settles nothing.
/// Construct it with [`offset_tracker`].
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
    pub fn book(&self, topic: &str, partition: i32, offset: i64) -> i64 {
        self.inner.book(topic, partition, offset)
    }

    /// Settle `token` as handled and report the watermark the host should commit. A stale or unknown
    /// token commits nothing.
    pub fn ack(&self, token: i64) -> AckDecision {
        let decision = self.inner.ack(token);
        AckDecision { commit: decision.commit, offset: decision.offset }
    }

    /// Record that the client committed `offset` on this partition. Max-merge, so two commits that
    /// returned out of order leave the higher watermark recorded; a no-op for a partition never
    /// booked or since reset.
    pub fn committed(&self, topic: &str, partition: i32, offset: i64) {
        self.inner.committed(topic, partition, offset);
    }

    /// Settle `token` as failed and report the offset the host should seek the partition to. The
    /// offset stays in flight until its redelivered copy settles, so a later ack cannot commit past
    /// it. A stale or unknown token seeks nothing.
    pub fn nack(&self, token: i64) -> NackDecision {
        let decision = self.inner.nack(token);
        NackDecision { seek: decision.seek, offset: decision.offset }
    }

    /// Drop a revoked partition's bookkeeping and retire its tokens; the new owner redelivers its
    /// unsettled offsets from the last committed watermark. A partition that is not assigned is a
    /// no-op, not an error, so a rebalance listener calls this for every revoked partition blindly.
    pub fn reset(&self, topic: &str, partition: i32) {
        self.inner.reset(topic, partition);
    }

    /// A snapshot of every assigned partition, ordered by topic then partition.
    pub fn stats(&self) -> PartitionStatsPage {
        let items = self
            .inner
            .stats()
            .into_iter()
            .map(|row| PartitionStats {
                topic: row.topic,
                partition: row.partition,
                unsettled: row.unsettled,
                max_delivered: row.max_delivered,
                committed: row.committed,
            })
            .collect();
        PartitionStatsPage { items }
    }
}

/// A fresh offset tracker with nothing booked. One tracker serves every partition a consumer is
/// assigned.
pub fn offset_tracker() -> OffsetTracker {
    OffsetTracker { inner: Arc::new(edax_kafka::OffsetTracker::new()) }
}

/// The token value no delivery ever carries, so a host's "no delivery" placeholder cannot alias a
/// live one. It is the token a `present: false` [`PollResult`] carries and the one no
/// [`OffsetTracker`] booking is issued. A call, because the generator binds functions and not
/// constants; a test pins the broker's kernel and the Kafka kernel to this one value.
pub fn no_delivery() -> i64 {
    edax_kafka::NO_DELIVERY
}

// ---- redis ---------------------------------------------------------------

/// A stored Redis ledger value taken apart: the fence token that owns the claim, and the idempotency
/// record behind it. A struct rather than a tuple, like [`SplitError`]: the generator sanitizes a
/// tuple return to `Vec<String>` and loses the field names.
pub struct RedisValue {
    /// The owner token the value carries, whatever its width — read back liberally so a ledger an
    /// older store wrote stays readable.
    pub owner: String,
    /// The idempotency record JSON behind the token, for [`decode_idempotency_record`].
    pub record: String,
}

/// A fresh fence token for a claim: a UUID v7 in its 32-character lowercase hex form.
///
/// Minted by the kernel and not by the host, so every SDK's token has the width the fence scripts
/// compare. Non-deterministic.
pub fn redis_owner() -> String {
    edax_redis::new_owner()
}

/// The stored value for a fresh claim: `owner` followed by the kernel's **pending** record for
/// `payload_hash`, under one key so one TTL governs both halves. Whoever loses the race reads the
/// winner's pending record and learns the key is in flight rather than unseen. A token of the wrong
/// width is a `codec:` error: it could only fence a prefix of someone else's claim.
pub fn redis_claim_value(owner: &str, payload_hash: u64) -> Result<String, String> {
    edax_redis::claim_value(owner, payload_hash).map_err(adapter_error)
}

/// The stored value for a settled claim: `owner` followed by `record_json` as the host encoded it
/// (with [`encode_idempotency_record`]), passed through untouched so a result never round-trips
/// through a second serializer.
pub fn redis_record_value(owner: &str, record_json: &str) -> Result<String, String> {
    edax_redis::record_value(owner, record_json).map_err(adapter_error)
}

/// Take a stored value apart into its owner token and its record.
///
/// Strict about the frame — a value that is not UTF-8, or too short to hold a token, is a `codec:`
/// error because no claimant could ever settle it — and liberal about the token itself.
pub fn redis_split_value(raw: &[u8]) -> Result<RedisValue, String> {
    let (owner, record) = edax_redis::split_value(raw).map_err(adapter_error)?;
    Ok(RedisValue { owner, record })
}

/// The Lua that sets a key's value only while it still carries the caller's owner token, refreshing
/// the TTL: `KEYS[1]` is the key, `ARGV` is the token, the new value and the TTL in milliseconds.
/// Returns `1` when the write landed, `0` when the key is absent or another claimant holds it. The
/// body is the kernel's byte for byte, so two SDKs registering it share one `EVALSHA` digest.
pub fn redis_fenced_set_script() -> String {
    edax_redis::FENCED_SET_SCRIPT.to_owned()
}

/// The Lua that deletes a key only while it still carries the caller's owner token: `KEYS[1]` is the
/// key, `ARGV[1]` is the token. Returns `1` when the delete landed, `0` when the key is absent or
/// another claimant holds it. Byte for byte the kernel's, like [`redis_fenced_set_script`].
pub fn redis_fenced_del_script() -> String {
    edax_redis::FENCED_DEL_SCRIPT.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    /// Drive a binding future to completion. Every `async` method on this surface is ready on the
    /// first poll — the work is in-memory and synchronous, and the host's runtime integration is
    /// alef's concern — so the tests need no runtime.
    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("a binding future yielded; it must not await"),
        }
    }

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
        assert!(rendezvous("invoice-7", 8) < 8);
        assert_eq!(rendezvous("invoice-7", 0), 0);

        assert!(should_apply(Some(4), Some(5)), "newer than stored -> apply");
        assert!(!should_apply(Some(5), Some(4)), "older than stored -> drop");
        assert!(should_apply(None, Some(4)), "nothing stored -> apply");
        assert!(should_apply(Some(5), None), "unsequenced -> apply");

        assert_eq!(backoff_millis(0), 100);
        assert_eq!(backoff_millis(1_000), 30_000, "capped, no overflow");
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
        // An unknown code is refused, not guessed: the host owns its default for those.
        assert_eq!(error_code_is_terminal("timeout"), Err("unknown error code: timeout".to_owned()));
        assert_eq!(error_code_is_terminal(""), Err("unknown error code: ".to_owned()));
    }

    #[test]
    fn split_error_branches() {
        struct Case {
            name: &'static str,
            wire: &'static str,
            want: (&'static str, &'static str),
        }
        let cases = [
            Case { name: "known_code", wire: "codec: bad json", want: ("codec", "bad json") },
            Case {
                name: "separator_inside_the_message",
                wire: "store: redis: connection reset",
                want: ("store", "redis: connection reset"),
            },
            Case { name: "empty_message", wire: "concurrency: ", want: ("concurrency", "") },
            // No code the kernel knows: the host keeps the whole string and applies its own default.
            Case { name: "unknown_code", wire: "timeout: deadline exceeded", want: ("", "timeout: deadline exceeded") },
            Case { name: "no_separator", wire: "connection reset", want: ("", "connection reset") },
            Case { name: "empty", wire: "", want: ("", "") },
        ];
        for c in cases {
            let got = split_error(c.wire);
            assert_eq!((got.code.as_str(), got.message.as_str()), c.want, "{}", c.name);
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
        assert!(!eval_filter(r#"{"compare":{"path":"deleted_at","op":"ne","value":null}}"#, doc).unwrap(),
            "a missing path makes the leaf false — the kernel's two-valued rule");

        let docs = r#"[{"id":"user-1","age":36},{"id":"user-2","age":29},{"id":"user-3","age":41}]"#;
        let page: Vec<Value> = serde_json::from_str(&query(r#"{"sort":[{"path":"age"}],"limit":2}"#, docs).unwrap()).unwrap();
        assert_eq!(page.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(), ["user-2", "user-1"]);

        // A cursor minted from the last row of a page continues it with no gap or repeat.
        let cursor = encode_cursor(&page.last().unwrap().to_string(), r#"[{"path":"age"}]"#).unwrap();
        let spec = json!({"sort": [{"path": "age"}], "limit": 2, "after": cursor}).to_string();
        let next: Vec<Value> = serde_json::from_str(&query(&spec, docs).unwrap()).unwrap();
        assert_eq!(next.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(), ["user-3"]);
    }

    // ---- upcast ------------------------------------------------------------

    #[test]
    fn upcast_plan_branches() {
        assert_eq!(upcast_plan(1, 3, vec![1, 2, 9]).unwrap(), vec![1, 2], "extra steps are ignored");
        assert_eq!(upcast_plan(3, 3, vec![]).unwrap(), Vec::<i32>::new(), "already current");
        // Both failures are deterministic, so both must carry the terminal `codec` code.
        assert_eq!(
            upcast_plan(1, 3, vec![1]).unwrap_err(),
            "codec: missing upcast step 2 (no transform for v2 -> v3)"
        );
        assert_eq!(
            upcast_plan(3, 1, vec![1, 2]).unwrap_err(),
            "codec: cannot upcast downward: from 3 > to 1"
        );
    }

    // ---- idempotency -------------------------------------------------------

    #[test]
    fn payload_hash_binds_the_canonical_form() {
        assert_eq!(payload_hash(b"user.create"), edax_core::fnv1a64(b"user.create"));

        // Key order and whitespace do not change the hash; the number form does. Canonicalization is
        // the kernel's, so every binding agrees — golden/canonical_payload_hash.json pins the values.
        let a = payload_hash_json(br#"{"email":"ada@example.com","age":36}"#).unwrap();
        let b = payload_hash_json(br#"{ "age": 36, "email": "ada@example.com" }"#).unwrap();
        assert_eq!(a, b);
        assert_ne!(payload_hash_json(br#"{"n":1}"#).unwrap(), payload_hash_json(br#"{"n":1.0}"#).unwrap());
        assert!(a > (1u64 << 53), "hashes exceed float precision, so they must cross as integers");
    }

    #[test]
    fn idempotency_decision_branches() {
        let current = payload_hash_json(br#"{"email":"ada@example.com"}"#).unwrap();
        let other = payload_hash_json(br#"{"email":"grace@example.com"}"#).unwrap();
        for (name, prior, expected) in [
            ("unseen_key_proceeds", None, "proceed"),
            ("same_payload_replays", Some(current), "replay"),
            ("different_payload_mismatches", Some(other), "payload_mismatch"),
            ("stored_zero_is_a_real_prior", Some(0), "payload_mismatch"),
        ] {
            assert_eq!(idempotency_decision(current, prior).unwrap(), expected, "{name}");
        }
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
            /// The canonical record bytes — the cross-language contract, pinned by
            /// `testdata/golden/idempotency_record.json`.
            encoded: &'static str,
            state: &'static str,
            /// What `decode_idempotency_record` reads back into the flat `(error, result_json)`.
            decoded: (&'static str, &'static str),
            /// The verdict a losing claimant reaches from the record.
            outcome: &'static str,
        }
        let cases = [
            Case {
                name: "pending_claim",
                pending: true,
                result_json: RESULT,
                error: None,
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":true}"#,
                state: "pending",
                decoded: ("", ""),
                outcome: "in_progress",
            },
            Case {
                name: "cached_success",
                pending: false,
                result_json: RESULT,
                error: None,
                encoded: concat!(
                    r#"{"payload_hash":11234062831410934249,"result":{"invoice_id":"invoice-7","#,
                    r#""total":5000},"error":null,"pending":false}"#
                ),
                state: "done",
                decoded: ("", RESULT),
                outcome: "succeeded",
            },
            Case {
                // A completed `null` is a success whose value is null, not "no result" — the case
                // that used to replay as an in-flight claim.
                name: "cached_success_with_a_null_result",
                pending: false,
                result_json: "null",
                error: None,
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":false}"#,
                state: "done",
                decoded: ("", "null"),
                outcome: "succeeded",
            },
            Case {
                name: "cached_failure",
                pending: false,
                result_json: RESULT,
                error: Some("codec: bad payload"),
                encoded: concat!(
                    r#"{"payload_hash":11234062831410934249,"result":null,"#,
                    r#""error":"codec: bad payload","pending":false}"#
                ),
                state: "failed",
                decoded: ("codec: bad payload", ""),
                outcome: "failed",
            },
            Case {
                // Reading order, first rule: pending wins, so a stale error cannot mask an
                // in-flight claim into a terminal one.
                name: "pending_wins_over_an_error",
                pending: true,
                result_json: RESULT,
                error: Some("boom"),
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":true}"#,
                state: "pending",
                decoded: ("", ""),
                outcome: "in_progress",
            },
            Case {
                // Second rule: error wins over a result, so a half-written record never replays as
                // a success.
                name: "error_wins_over_a_result",
                pending: false,
                result_json: RESULT,
                error: Some("boom"),
                encoded: r#"{"payload_hash":11234062831410934249,"result":null,"error":"boom","pending":false}"#,
                state: "failed",
                decoded: ("boom", ""),
                outcome: "failed",
            },
        ];

        for case in cases {
            let owned_error = || case.error.map(str::to_owned);
            let encoded = encode_idempotency_record(HASH, case.pending, case.result_json, owned_error());
            assert_eq!(encoded.as_deref(), Ok(case.encoded), "{}", case.name);

            let record = decode_idempotency_record(case.encoded).expect(case.name);
            assert_eq!(record.payload_hash, HASH, "{} hash", case.name);
            assert_eq!(record.state, case.state, "{} state", case.name);
            assert_eq!((record.error.as_str(), record.result_json.as_str()), case.decoded, "{} fields", case.name);

            // The verdict reads only the classifying fields, and the hash is checked before them:
            // a key reused with another command mismatches whatever state the winner is in.
            let verdict = idempotency_outcome(HASH, record.payload_hash, case.pending, owned_error());
            assert_eq!(verdict.as_deref(), Ok(case.outcome), "{} outcome", case.name);
            let reused = idempotency_outcome(HASH ^ 1, record.payload_hash, case.pending, owned_error());
            assert_eq!(reused.as_deref(), Ok("payload_mismatch"), "{} reused key", case.name);
        }
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
                let wire = encode_envelope(
                    format,
                    record.id.clone(),
                    record.aggregate_id.clone(),
                    record.event.name.clone(),
                    record.event.version,
                    record.event.data.clone(),
                    record.namespace.clone(),
                    record.metadata.clone(),
                    record.timestamp,
                    record.sequence,
                )
                .unwrap();
                let back = decode_envelope(format, &wire).unwrap();
                assert_same_envelope(&back, &record, &format!("{format} sequence={:?}", record.sequence));
            }
        }
    }

    // ---- error contract ----------------------------------------------------

    #[test]
    fn kernel_failures_carry_the_codec_code() {
        // Every deterministic marshaling failure must name its cause *and* carry the taxonomy code, or
        // the host falls back to a transient store error and retries a failure that always repeats.
        let valid_doc = r#"{"id":"user-1","age":36}"#;
        let cases: [(&str, Result<(), String>, &str); 17] = [
            ("filter_is_not_json", eval_filter("{not json}", valid_doc).map(|_| ()), "key must be a string"),
            ("filter_is_not_a_filter", eval_filter(r#"{"nope":1}"#, valid_doc).map(|_| ()), "unknown variant"),
            ("document_is_not_json", eval_filter(r#"{"exists":{"path":"id","exists":true}}"#, "nope").map(|_| ()), "expected ident"),
            ("query_spec_is_not_json", query("{not json}", "[]").map(|_| ()), "key must be a string"),
            ("cursor_is_not_base64", query(r#"{"sort":[{"path":"age"}],"after":"!!!"}"#, "[]").map(|_| ()), "Invalid symbol"),
            ("cursor_document_is_not_json", encode_cursor("nope", "[]").map(|_| ()), "expected ident"),
            ("command_payload_is_not_json", payload_hash_json(b"{not json}").map(|_| ()), "key must be a string"),
            ("wire_bytes_are_not_the_format", decode_envelope("json", b"not an envelope").map(|_| ()), "decode"),
            (
                "cached_result_is_not_json",
                encode_idempotency_record(HASH, false, "{not json}", None).map(|_| ()),
                "key must be a string",
            ),
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
                // The four Kafka failures. A media type no producer should ever write, a header no
                // consumer can read, and bytes that are not the format the header names: all three
                // repeat identically on redelivery, so all three carry the terminal code.
                "kafka_format_is_not_a_format",
                kafka_media_type("protobuf").map(|_| ()),
                "unknown format: protobuf",
            ),
            (
                "kafka_encode_format_is_not_a_format",
                encode_kafka_record("protobuf", envelope("evt-1", Some(7)), None).map(|_| ()),
                "unknown format: protobuf",
            ),
            (
                "kafka_content_type_is_not_a_media_type",
                decode_kafka_record("json", b"{}", vec![KafkaHeader {
                    key: "content-type".to_owned(),
                    value: b"application/xml".to_vec(),
                }])
                .map(|_| ()),
                "unknown format: application/xml",
            ),
            (
                "kafka_value_is_not_the_named_format",
                decode_kafka_record("json", b"not an envelope", Vec::new()).map(|_| ()),
                "decode",
            ),
            (
                // The Redis failures cross through `adapter_error`, not `codec_error` — the kernel
                // tags them itself — and must still reach the host under the same terminal code.
                "redis_owner_token_is_the_wrong_width",
                redis_record_value("deadbeef", "{}").map(|_| ()),
                "owner token is 8 bytes, expected 32",
            ),
        ];
        for (name, got, cause) in cases {
            let message = got.expect_err(name);
            assert!(message.starts_with("codec: "), "{name}: {message}");
            assert!(message.contains(cause), "{name}: {message}");
        }

        // An unknown format is rejected on both directions of the codec, and names the format.
        for message in [
            encode_envelope("protobuf", String::new(), String::new(), String::new(), 1, Vec::new(), HashMap::new(), HashMap::new(), 0, None).unwrap_err(),
            decode_envelope("protobuf", b"{}").map(|_| ()).unwrap_err(),
        ] {
            assert_eq!(message, "codec: unknown format: protobuf");
        }
    }

    // ---- in-memory stores --------------------------------------------------

    #[test]
    fn event_store_crosses_envelopes_structurally() {
        let store = memory_transactor();
        block_on(store.append_events(NO_TX, "invoice-7", 0, vec![envelope("evt-1", Some(1))])).unwrap();

        // Every field survives the boundary — no JSON or wire encoding in between.
        let page = block_on(store.load_events(NO_TX, "invoice-7", 0)).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_same_envelope(&page.items[0], &envelope("evt-1", Some(1)), "loaded envelope");
        assert!(block_on(store.load_events(NO_TX, "invoice-404", 0)).unwrap().items.is_empty());

        // The adapter's own error code is forwarded untouched, not re-tagged as a codec failure.
        let conflict = block_on(store.append_events(NO_TX, "invoice-7", 0, vec![envelope("evt-2", Some(2))])).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        let unsequenced = block_on(store.append_events(NO_TX, "invoice-7", 1, vec![envelope("evt-2", None)])).unwrap_err();
        assert_eq!(unsequenced, "codec: envelope 0 has no sequence");

        // Outbox: append enqueued the event; marking it published drains it.
        assert_eq!(block_on(store.unpublished(10)).unwrap().items.len(), 1);
        block_on(store.mark_published(vec!["evt-1".to_owned()])).unwrap();
        assert!(block_on(store.unpublished(10)).unwrap().items.is_empty());

        // A missing snapshot crosses as `present: false` with empty fields, not as an error.
        let absent = block_on(store.load_snapshot(NO_TX, "invoice-7")).unwrap();
        assert!(!absent.present);
        assert_eq!((absent.type_name.as_str(), absent.version, absent.data.len()), ("", 0, 0));

        block_on(store.save_snapshot(NO_TX, "invoice-7", "Invoice", 2, br#"{"total":5000}"#.to_vec())).unwrap();
        let present = block_on(store.load_snapshot(NO_TX, "invoice-7")).unwrap();
        assert!(present.present);
        assert_eq!((present.type_name.as_str(), present.version, present.data), ("Invoice", 2, br#"{"total":5000}"#.to_vec()));
    }

    #[test]
    fn idempotency_store_claims_and_records() {
        let store = memory_transactor();
        let hash = payload_hash_json(br#"{"email":"ada@example.com"}"#).unwrap();

        assert_eq!(block_on(store.claim(NO_TX, "cmd-key-1", hash)).unwrap(), None, "first claim wins");

        let prior: Value = serde_json::from_str(&block_on(store.claim(NO_TX, "cmd-key-1", hash)).unwrap().unwrap()).unwrap();
        assert_eq!(prior, json!({"payload_hash": hash, "result": null, "error": null, "pending": true}));

        let done = r#"{"payload_hash":0,"result":{"invoice_id":"invoice-7"},"error":null,"pending":false}"#;
        block_on(store.complete(NO_TX, "cmd-key-1", done)).unwrap();
        assert_eq!(block_on(store.claim(NO_TX, "cmd-key-1", hash)).unwrap().as_deref(), Some(done));

        block_on(store.release(NO_TX, "cmd-key-1")).unwrap();
        assert_eq!(block_on(store.claim(NO_TX, "cmd-key-1", hash)).unwrap(), None, "released key is claimable again");
    }

    #[test]
    fn projection_store_versions_and_queries() {
        let store = memory_transactor();
        assert_eq!(block_on(store.get_projection(NO_TX, "user-1")).unwrap(), None);

        assert_eq!(block_on(store.save_projection(NO_TX, "user-1", 0, r#"{"email":"ada@example.com","age":36}"#)).unwrap(), 1);
        let entity: Value = serde_json::from_str(&block_on(store.get_projection(NO_TX, "user-1")).unwrap().unwrap()).unwrap();
        assert_eq!(entity, json!({"id": "user-1", "version": 1, "document": {"email": "ada@example.com", "age": 36}}));

        // The adapter's codes cross untouched: a stale version is a conflict, a bad document is codec.
        let conflict = block_on(store.save_projection(NO_TX, "user-1", 0, r#"{"email":"ada@example.com","age":37}"#)).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        assert!(block_on(store.save_projection(NO_TX, "user-2", 0, "[]")).unwrap_err().starts_with("codec: "));

        block_on(store.save_projection(NO_TX, "user-2", 0, r#"{"email":"grace@example.com","age":29}"#)).unwrap();
        let page: Vec<Value> =
            serde_json::from_str(&block_on(store.query_projections(NO_TX, r#"{"sort":[{"path":"age"}]}"#)).unwrap()).unwrap();
        assert_eq!(page.iter().map(|d| d["id"].as_str().unwrap()).collect::<Vec<_>>(), ["user-2", "user-1"]);
        assert!(block_on(store.query_projections(NO_TX, "{not a query}")).unwrap_err().starts_with("codec: "));
    }

    #[test]
    fn saga_store_versions_and_deletes() {
        let store = memory_transactor();
        assert_eq!(block_on(store.load_saga(NO_TX, "order-1")).unwrap(), None);

        assert_eq!(block_on(store.save_saga(NO_TX, "order-1", 0, r#"{"status":"awaiting_payment"}"#)).unwrap(), 1);
        let instance: Value = serde_json::from_str(&block_on(store.load_saga(NO_TX, "order-1")).unwrap().unwrap()).unwrap();
        assert_eq!(instance, json!({"id": "order-1", "version": 1, "state": {"status": "awaiting_payment"}}));

        // The adapter's codes cross untouched: a stale version is a conflict, bad state JSON is codec.
        let conflict = block_on(store.save_saga(NO_TX, "order-1", 0, r#"{"status":"shipped"}"#)).unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        assert!(block_on(store.save_saga(NO_TX, "order-1", 1, "{not json")).unwrap_err().starts_with("codec: "));
        let stale_delete = block_on(store.delete_saga(NO_TX, "order-1", 0)).unwrap_err();
        assert_eq!(stale_delete, "concurrency: expected 0, stored 1");

        block_on(store.delete_saga(NO_TX, "order-1", 1)).unwrap();
        assert_eq!(block_on(store.load_saga(NO_TX, "order-1")).unwrap(), None);
        block_on(store.delete_saga(NO_TX, "order-1", 1)).unwrap(); // absent: idempotent, not an error
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
            block_on(store.append_events(tx, "invoice-7", 0, vec![envelope("evt-1", Some(1))])).unwrap();
            block_on(store.save_snapshot(tx, "invoice-7", "Invoice", 1, b"snap".to_vec())).unwrap();
            block_on(store.save_projection(tx, "user-1", 0, r#"{"age":36}"#)).unwrap();
            assert_eq!(block_on(store.claim(tx, "req-A", 7)).unwrap(), None, "{}", c.name);
            block_on(store.save_saga(tx, "order-1", 0, r#"{"status":"open"}"#)).unwrap();

            // Read-your-writes under the token; nothing visible under NO_TX yet (the outbox never
            // shows a staged append).
            assert_eq!(block_on(store.load_events(tx, "invoice-7", 0)).unwrap().items.len(), 1, "{}", c.name);
            assert!(block_on(store.load_events(NO_TX, "invoice-7", 0)).unwrap().items.is_empty(), "{}", c.name);
            assert!(block_on(store.unpublished(10)).unwrap().items.is_empty(), "{}", c.name);
            assert_eq!(block_on(store.get_projection(NO_TX, "user-1")).unwrap(), None, "{}", c.name);

            if c.commit {
                store.commit(tx).unwrap();
            } else {
                assert!(store.rollback(tx), "{}", c.name);
            }
            let visible = (
                block_on(store.load_events(NO_TX, "invoice-7", 0)).unwrap().items.len(),
                block_on(store.unpublished(10)).unwrap().items.len(),
                block_on(store.load_snapshot(NO_TX, "invoice-7")).unwrap().present,
                block_on(store.get_projection(NO_TX, "user-1")).unwrap().is_some(),
                block_on(store.claim(NO_TX, "req-A", 7)).unwrap().is_some(),
                block_on(store.load_saga(NO_TX, "order-1")).unwrap().is_some(),
            );
            assert_eq!(visible, c.want, "{}", c.name);

            // A spent token is refused everywhere with the terminal code, and a rollback of it is a no-op.
            let refused = block_on(store.load_events(tx, "invoice-7", 0)).map(|p| p.items.len()).unwrap_err();
            assert!(refused.starts_with("bad_handle: unknown transaction"), "{}: {refused}", c.name);
            assert_eq!(store.commit(tx).unwrap_err(), refused, "{}", c.name);
            assert!(!store.rollback(tx), "{}", c.name);
        }
    }

    #[test]
    fn broker_polls_and_settles_by_token() {
        let broker = memory_broker();
        assert!(broker.is_empty());

        // An empty poll is a present-less placeholder, never an error and never a live token.
        let none = broker.try_poll();
        assert!(!none.present);
        assert_eq!((none.token, none.envelope.id.as_str()), (edax_memory::NO_DELIVERY, ""));

        broker.publish(envelope("evt-1", Some(1)));
        broker.publish(envelope("evt-1", Some(1))); // at-least-once: the same envelope republished
        assert!(!broker.is_empty());

        let first = broker.try_poll();
        assert!(first.present);
        assert_same_envelope(&first.envelope, &envelope("evt-1", Some(1)), "polled envelope");
        let second = broker.try_poll();
        assert!(second.present);
        assert_ne!(first.token, second.token, "one id in flight twice is two deliveries");

        // nack requeues under a fresh token; the stale token settles nothing afterwards.
        assert!(broker.nack(first.token));
        assert!(!broker.ack(first.token));
        let again = broker.try_poll();
        assert!(again.present && again.token != first.token);

        assert!(broker.ack(second.token));
        assert!(!broker.ack(second.token), "a second settlement is refused");
        assert!(!broker.is_empty(), "the redelivery is still outstanding");
        assert!(broker.ack(again.token));
        assert!(broker.is_empty());
    }

    // ---- kafka -------------------------------------------------------------

    /// Copy headers for a second call: the binding record carries no derives (alef owns them on the
    /// generated twin), and every header-taking call consumes its `Vec`.
    fn copy_headers(headers: &[KafkaHeader]) -> Vec<KafkaHeader> {
        headers.iter().map(|header| KafkaHeader { key: header.key.clone(), value: header.value.clone() }).collect()
    }

    /// One header, for the cases that build a fetched record by hand.
    fn header(key: &str, value: &[u8]) -> KafkaHeader {
        KafkaHeader { key: key.to_owned(), value: value.to_vec() }
    }

    #[test]
    fn kafka_record_binds_key_headers_and_round_trips() {
        struct Case {
            name: &'static str,
            format: &'static str,
            /// The host's key override, or `None` for the kernel's default key rule.
            key: Option<&'static str>,
            want_key: &'static str,
            want_media_type: &'static str,
            /// A consumer default that disagrees with the record, so only the header can decode it.
            other_format: &'static str,
        }
        let cases = [
            Case {
                name: "json_defaults_the_key_to_the_aggregate_id",
                format: "json",
                key: None,
                want_key: "invoice-7",
                want_media_type: "application/vnd.edax.envelope+json",
                other_format: "msgpack",
            },
            Case {
                name: "msgpack_takes_the_hosts_key_override",
                format: "msgpack",
                key: Some("eu-west/invoice-7"),
                want_key: "eu-west/invoice-7",
                want_media_type: "application/vnd.edax.envelope+msgpack",
                other_format: "json",
            },
        ];
        for case in cases {
            assert_eq!(kafka_media_type(case.format).as_deref(), Ok(case.want_media_type), "{}", case.name);

            let record = encode_kafka_record(case.format, envelope("evt-1", Some(7)), case.key.map(str::to_owned))
                .expect(case.name);
            assert_eq!(record.key, case.want_key.as_bytes(), "{} key", case.name);

            // The four contract headers, in the kernel's order: a consumer routing on the event name
            // or planning an upcast never has to decode the value first.
            let headers: Vec<(&str, &[u8])> =
                record.headers.iter().map(|h| (h.key.as_str(), h.value.as_slice())).collect();
            assert_eq!(
                headers,
                [
                    ("content-type", case.want_media_type.as_bytes()),
                    ("edax-id", b"evt-1".as_slice()),
                    ("edax-event", b"InvoiceIssued".as_slice()),
                    ("edax-event-version", b"1".as_slice()),
                ],
                "{} headers",
                case.name
            );
            assert_eq!(kafka_content_type(copy_headers(&record.headers)), case.want_media_type, "{}", case.name);

            // The header names the format, so a consumer whose default disagrees still decodes it...
            let via_header =
                decode_kafka_record(case.other_format, &record.value, copy_headers(&record.headers)).expect(case.name);
            assert_same_envelope(&via_header, &envelope("evt-1", Some(7)), case.name);
            // ...and a record that arrives with no headers at all falls back to that default.
            let via_default = decode_kafka_record(case.format, &record.value, Vec::new()).expect(case.name);
            assert_same_envelope(&via_default, &envelope("evt-1", Some(7)), case.name);
        }

        // The primitive path: the host's bytes and the host's media type, with the kernel's key rule
        // and header set still applied.
        let host = kafka_record(envelope("evt-1", Some(7)), b"<invoice/>".to_vec(), "application/xml", None);
        assert_eq!((host.key.as_slice(), host.value.as_slice()), (b"invoice-7".as_slice(), b"<invoice/>".as_slice()));
        assert_eq!(kafka_content_type(copy_headers(&host.headers)), "application/xml");
        assert_eq!(host.headers.len(), 4, "a host serializer keeps the contract headers");

        // No readable media type crosses as an empty string, not an error and not a guess.
        assert_eq!(kafka_content_type(Vec::new()), "", "no headers at all");
        assert_eq!(kafka_content_type(vec![header("edax-id", b"evt-1")]), "", "no content-type header");
        assert_eq!(kafka_content_type(vec![header("Content-Type", b"application/json")]), "", "case-sensitive");
        assert_eq!(kafka_content_type(vec![header("content-type", &[0xff, 0xfe])]), "", "not UTF-8");

        // A media type the kernel does not mint is one poison message, never a silent fallback to the
        // consumer's default.
        let unknown = decode_kafka_record("json", b"{}", vec![header("content-type", b"application/xml")])
            .map(|_| ())
            .unwrap_err();
        assert_eq!(unknown, "codec: unknown format: application/xml");

        // Topic, partition and offset together are the identity of a message that has no envelope.
        assert_eq!(kafka_poison_id("orders", 3, 42), "orders/3/42");
        assert_ne!(kafka_poison_id("orders", 3, 42), kafka_poison_id("orders", 4, 42), "the offset alone repeats");
    }

    #[test]
    fn offset_tracker_settles_by_token() {
        let tracker = offset_tracker();
        assert!(tracker.stats().items.is_empty(), "nothing is assigned before the first booking");
        assert_eq!(no_delivery(), edax_memory::NO_DELIVERY, "one placeholder token value across both kernels");

        // Two offsets in flight; the higher settles first, so the commit stops at the lower one.
        let five = tracker.book("orders", 0, 5);
        let six = tracker.book("orders", 0, 6);
        assert_ne!(five, six, "one token per delivery");
        assert_ne!(five, no_delivery(), "a live token never aliases the placeholder");

        let partial = tracker.ack(six);
        assert_eq!((partial.commit, partial.offset), (true, 5), "the watermark is the lowest still in flight");
        tracker.committed("orders", 0, 5);

        let drained = tracker.ack(five);
        assert_eq!((drained.commit, drained.offset), (true, 7), "nothing in flight: one past the highest delivered");
        tracker.committed("orders", 0, 7);

        // A token settles once: the second ack belongs to a delivery already settled.
        let stale = tracker.ack(five);
        assert_eq!((stale.commit, stale.offset), (false, -1), "a stale token commits nothing");

        // A nack seeks back to the lowest in flight — not to its own offset, which would strand a
        // lower one — and leaves the offset unsettled so no later ack commits past it.
        let seven = tracker.book("orders", 0, 7);
        let eight = tracker.book("orders", 0, 8);
        let seek = tracker.nack(eight);
        assert_eq!((seek.seek, seek.offset), (true, 7));
        let unknown = tracker.nack(no_delivery());
        assert_eq!((unknown.seek, unknown.offset), (false, -1), "an unknown token seeks nothing");

        // One partition's progress never settles another's, and the rows are ordered.
        tracker.book("orders", 1, 0);
        let page = tracker.stats();
        let rows: Vec<(&str, i32, u64, i64, i64)> = page
            .items
            .iter()
            .map(|row| (row.topic.as_str(), row.partition, row.unsettled, row.max_delivered, row.committed))
            .collect();
        assert_eq!(rows, [("orders", 0, 2, 8, 7), ("orders", 1, 1, 0, -1)]);

        // A revoked partition's bookkeeping goes, and its tokens with it: the new owner redelivers
        // from the last committed watermark.
        tracker.reset("orders", 0);
        let after_reset = tracker.ack(seven);
        assert_eq!((after_reset.commit, after_reset.offset), (false, -1), "a reset partition's token settles nothing");
        assert_eq!(tracker.stats().items.len(), 1, "only the partition still assigned is reported");
        tracker.reset("orders", 9); // never assigned: a no-op, so a rebalance listener resets blindly
        tracker.committed("orders", 9, 4); // same, so a commit for a revoked partition records nothing
        assert_eq!(tracker.stats().items.len(), 1);
    }

    // ---- redis -------------------------------------------------------------

    #[test]
    fn redis_ledger_binds_the_kernel() {
        // The fence compares the first 32 bytes of the value, so the token is fixed-width lowercase
        // hex and minted by the kernel, never by the host.
        let owner = redis_owner();
        assert_eq!(owner.len(), 32);
        assert!(owner.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "{owner}");
        assert_ne!(owner, redis_owner(), "two claimants are distinguishable");

        // A claim stores the kernel's pending record behind the token, under one key and one TTL.
        let claim = redis_claim_value(&owner, HASH).unwrap();
        let claimed = redis_split_value(claim.as_bytes()).unwrap();
        assert_eq!(claimed.owner, owner);
        assert_eq!(
            claimed.record,
            r#"{"payload_hash":11234062831410934249,"result":null,"error":null,"pending":true}"#,
            "the record half is edax-cqrs's own encoder, not a hand-formatted copy"
        );
        assert_eq!(decode_idempotency_record(&claimed.record).unwrap().state, "pending");

        // A settled record is passed through untouched, so a result never round-trips through a
        // second serializer on its way to the ledger.
        let done = encode_idempotency_record(HASH, false, RESULT, None).unwrap();
        let settled = redis_record_value(&owner, &done).unwrap();
        let read_back = redis_split_value(settled.as_bytes()).unwrap();
        assert_eq!((read_back.owner.as_str(), read_back.record.as_str()), (owner.as_str(), done.as_str()));

        // A value nobody could ever fence is terminal, not a store hiccup to retry.
        for (name, got) in [
            ("value_is_shorter_than_a_token", redis_split_value(b"deadbeef")),
            ("value_is_not_utf8", redis_split_value(&[0xff; 48])),
        ] {
            let message = got.map(|_| ()).expect_err(name);
            assert!(message.starts_with("codec: "), "{name}: {message}");
        }

        // Both Lua bodies cross verbatim, so two SDKs registering them land on one EVALSHA digest.
        assert_eq!(redis_fenced_set_script(), edax_redis::FENCED_SET_SCRIPT);
        assert_eq!(redis_fenced_del_script(), edax_redis::FENCED_DEL_SCRIPT);
        for script in [redis_fenced_set_script(), redis_fenced_del_script()] {
            assert!(script.contains("GETRANGE"), "the fence reads the token's width: {script}");
        }
    }
}
