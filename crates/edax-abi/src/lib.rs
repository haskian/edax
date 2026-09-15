//! Flat, plain-Rust API surface over the edax kernels and adapters.
//!
//! This crate holds no logic of its own: every function forwards to a kernel
//! (`edax-core` / `edax-event` / `edax-cqrs`) or to an adapter (`edax-adapters`),
//! reshaping only at the edges so the surface is trivial to bind. alef extracts
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
/// Errors forwarded from `edax-adapters` already carry their own code (`concurrency` / `store` /
/// `codec` / `bad_handle`) and must not be tagged again: they cross through [`adapter_error`].
fn codec_error(cause: impl std::fmt::Display) -> String {
    ErrorCode::Codec.tag(cause)
}

/// An adapter failure as the wire form. The adapters' typed error renders its own `"<code>: <message>"`
/// through the kernel's tag, so its `Display` is the whole conversion — nothing is added here.
fn adapter_error(cause: edax_adapters::AdapterError) -> String {
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
pub const NO_TX: i64 = edax_adapters::NO_TX;

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
/// commit. The journal itself lives in `edax-adapters`, so the host SDK keeps no transaction state
/// of its own: it holds the token across calls and hands it back. The seams are flattened onto this
/// one object because the host's four store classes are views over one database, and one object is
/// the shape the binding can share between them.
///
/// Events cross as structured [`EventEnvelope`] records (no JSON or wire encoding at the boundary);
/// snapshots cross as their raw `data` bytes; idempotency records, documents, saga state and query
/// specs cross as JSON text.
pub struct MemoryTransactor {
    inner: Arc<edax_adapters::MemoryTransactor>,
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
    MemoryTransactor { inner: Arc::new(edax_adapters::MemoryTransactor::new()) }
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
    inner: Arc<edax_adapters::MemoryBroker>,
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
            None => PollResult { present: false, token: edax_adapters::NO_DELIVERY, envelope: empty_envelope() },
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
    MemoryBroker { inner: Arc::new(edax_adapters::MemoryBroker::new()) }
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
        let cases: [(&str, Result<(), String>, &str); 12] = [
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
        assert_eq!((none.token, none.envelope.id.as_str()), (edax_adapters::NO_DELIVERY, ""));

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
}
