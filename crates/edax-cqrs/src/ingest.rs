//! Decisions for append-only events and complete, sequence-ordered state ingestion.
//!
//! The caller holds deterministic locks for all target and rejection keys and commits the
//! resulting writes and rejections together. `source` identifies a record in an immutable
//! source incarnation; `key` already includes the target generation and tenant.

use std::collections::BTreeMap;

use edax_core::{Error, ErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::namespace::{self, key, prefix, AtRest};
use crate::{canonical_json, holds_nul, is_digest_hex, keyed_digest, payload_hash};

edax_core::closed_names! {
    /// The interpretation of rows in one ingestion target.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Mode {
        /// Retain each event identity once.
        Append = "append",
        /// Replace the complete state at a sequence-ordered row identity.
        Upsert = "upsert",
    }
}

edax_core::closed_names! {
    /// The durable decision for an original input.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Outcome {
        /// Applied, or already represented by committed state.
        Accepted = "accepted",
        /// Refused; its durable rejection must complete publication.
        Rejected = "rejected",
    }
}

/// Exact source coordinates. Kafka offsets are decimal strings in canonical JSON keys,
/// so their entire signed 64-bit range survives the JSON safe-integer boundary.
pub fn source_key(target: &str, incarnation: &str, topic: &str, partition: i32, offset: i64) -> Result<String, Error> {
    if incarnation.is_empty() || topic.is_empty() || partition < 0 || offset < 0 {
        return Err(Error::bad_handle("invalid ingestion source coordinate"));
    }
    let parts = serde_json::json!(["source", incarnation, topic, partition, offset.to_string()]);
    key(target, &parts.to_string())
}

/// A transport-independent original identity. The caller preserves this ID across retries;
/// it is not an ordering sequence or a promise that the source can replay lost deliveries.
pub fn record_source_key(target: &str, incarnation: &str, source_id: &str) -> Result<String, Error> {
    if incarnation.is_empty() || source_id.is_empty() || incarnation.contains('\0') || source_id.contains('\0') {
        return Err(Error::bad_handle("invalid ingestion source identity"));
    }
    key(
        target,
        &serde_json::json!(["source-id", incarnation, source_id]).to_string(),
    )
}

fn valid_source(contract: &Contract, source: &[Value]) -> bool {
    if source.get(1).and_then(Value::as_str) != Some(&contract.source_incarnation)
        || source.get(2).and_then(Value::as_str).is_none_or(str::is_empty)
    {
        return false;
    }
    match source.first().and_then(Value::as_str) {
        Some("source-id") => {
            source.len() == 3 && source.get(2).and_then(Value::as_str).is_some_and(|v| !v.contains('\0'))
        }
        Some("source") => {
            source.len() == 5
                && source
                    .get(3)
                    .and_then(Value::as_u64)
                    .is_some_and(|v| v <= i32::MAX as u64)
                && source
                    .get(4)
                    .and_then(Value::as_str)
                    .is_some_and(|v| v.parse::<i64>().is_ok_and(|n| n >= 0 && n.to_string() == v))
        }
        _ => false,
    }
}

/// Scope an append event identity or upsert row identity to its generation, tenant and erasure
/// subject. The subject sits before the row identity, so one key range holds every row of a subject.
pub fn row_key(
    target: &str,
    mode: &str,
    tenant: Option<&str>,
    subject: Option<&str>,
    row_id: &str,
) -> Result<String, Error> {
    let mode = Mode::by_name(mode).ok_or_else(|| Error::bad_handle("invalid ingestion row identity"))?;
    if row_id.is_empty() {
        return Err(Error::codec("invalid ingestion row identity"));
    }
    namespace::check_tenant(tenant)?;
    subject.map(namespace::check_subject).transpose()?;
    key(
        target,
        &serde_json::json!([mode.as_str(), tenant, subject, row_id]).to_string(),
    )
}

/// The half-open key range `[start, end)` holding every row of one tenant and subject in a target:
/// what an erasure cleanup deletes. Without a subject, every row of the tenant, unsealed rows included,
/// as a tenant erasure deletes them.
pub fn scope_range(
    target: &str,
    mode: &str,
    tenant: Option<&str>,
    subject: Option<&str>,
) -> Result<(String, String), Error> {
    let mode = Mode::by_name(mode).ok_or_else(|| Error::bad_handle("invalid ingestion row identity"))?;
    namespace::check_tenant(tenant)?;
    match subject {
        Some(subject) => {
            namespace::check_subject(subject)?;
            namespace::range(target, &serde_json::json!([mode.as_str(), tenant, subject]))
        }
        None => namespace::prefix_range(target, &serde_json::json!([mode.as_str(), tenant])),
    }
}

/// Immutable interpretation of one target generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    /// Append events or replace complete state.
    pub mode: String,
    /// Unique physical target namespace.
    pub generation: String,
    /// Verified source identity, changed when the source is replaced.
    pub source_incarnation: String,
    /// Immutable mapping and keying implementation identity.
    pub mapper_id: String,
    /// How the target stores a subject's private part: [`AtRest`] by name.
    pub at_rest: String,
}

/// A restored/reused target cannot silently change how records are interpreted.
pub fn bind_contract(previous: Option<&Contract>, expected: &Contract) -> Result<(), Error> {
    prefix(&expected.generation)?;
    if Mode::by_name(&expected.mode).is_none()
        || AtRest::by_name(&expected.at_rest).is_none()
        || expected.source_incarnation.is_empty()
        || expected.mapper_id.is_empty()
        || expected.source_incarnation.contains('\0')
        || expected.mapper_id.contains('\0')
        || previous.is_some_and(|prior| prior != expected)
    {
        return Err(Error::bad_handle("invalid or changed ingestion target contract"));
    }
    Ok(())
}

/// The parts of `value`, a canonical key of the target's generation.
fn identity(contract: &Contract, value: &str) -> Result<Vec<Value>, Error> {
    let suffix = value
        .strip_prefix(&prefix(&contract.generation)?)
        .ok_or_else(|| Error::bad_handle("input belongs to another ingestion target"))?;
    let parts: Vec<Value> = serde_json::from_str(suffix).map_err(Error::codec)?;
    if key(&contract.generation, suffix)? != value {
        return Err(Error::bad_handle("ingestion identity is not canonical"));
    }
    Ok(parts)
}

/// Prevent a caller from escaping the target manifest through hand-built input keys.
pub fn validate_inputs(contract: &Contract, inputs: &[Input]) -> Result<(), Error> {
    bind_contract(None, contract)?;
    for input in inputs {
        let source = identity(contract, &input.source)?;
        let row = identity(contract, &input.key)?;
        if input.stream.is_empty() || input.event_id.is_empty() {
            return Err(Error::codec("invalid ingestion event or aggregate identity"));
        }
        if !valid_source(contract, &source)
            || row.len() != 4
            || row.first().and_then(Value::as_str) != Some(&contract.mode)
            || row.get(1).is_none_or(|v| !v.is_null() && !v.is_string())
            || row
                .get(2)
                .is_none_or(|v| !v.is_null() && v.as_str().is_none_or(str::is_empty))
            || row.get(3).and_then(Value::as_str).is_none_or(str::is_empty)
            || (contract.mode == Mode::Append.as_str() && row.get(3).and_then(Value::as_str) != Some(&input.event_id))
        {
            return Err(Error::bad_handle("invalid ingestion source or row identity"));
        }
        namespace::check_tenant(row.get(1).and_then(Value::as_str))?;
        row.get(2)
            .and_then(Value::as_str)
            .map(namespace::check_subject)
            .transpose()?;
        if input.raw.contains('\0') {
            return Err(Error::codec("ingestion raw representation holds a NUL character"));
        }
        if !is_digest_hex(&input.raw_fingerprint) {
            return Err(Error::bad_handle(
                "ingestion raw fingerprint must be a SHA-256 digest in lowercase hex",
            ));
        }
        if input.fingerprint.as_deref().is_some_and(|f| !is_digest_hex(f)) {
            return Err(Error::bad_handle(
                "ingestion keyed fingerprint must be an HMAC-SHA-256 digest in lowercase hex",
            ));
        }
    }
    Ok(())
}

/// The tenant and subject whose key seals `input`'s private part on this target, or none: a plaintext
/// target, an erased or refused input, or a row whose key names no subject.
fn sealed_scope(contract: &Contract, input: &Input) -> Result<Option<(Option<String>, String)>, Error> {
    if contract.at_rest == AtRest::Plaintext.as_str() || input.erased || input.failure.is_some() {
        return Ok(None);
    }
    row_scope(contract, &input.key)
}

/// A tenant and optional erasure subject, including the subjectless tenant scope.
pub type WriteScope = (Option<String>, Option<String>);

/// The scope of each original, in input order, from validated keys rather than host metadata.
pub fn write_scopes(contract: &Contract, inputs: &[Input]) -> Result<Vec<WriteScope>, Error> {
    validate_inputs(contract, inputs)?;
    inputs
        .iter()
        .map(|input| {
            let row = identity(contract, &input.key)?;
            Ok((
                row.get(1).and_then(Value::as_str).map(str::to_owned),
                row.get(2).and_then(Value::as_str).map(str::to_owned),
            ))
        })
        .collect()
}

/// The tenant and subject a row key names, or none when it names no subject.
fn row_scope(contract: &Contract, key: &str) -> Result<Option<(Option<String>, String)>, Error> {
    let row = identity(contract, key)?;
    Ok(row.get(2).and_then(Value::as_str).map(|subject| {
        (
            row.get(1).and_then(Value::as_str).map(str::to_owned),
            subject.to_owned(),
        )
    }))
}

/// The sealed part of the row at `key`: its sequence, 0 in append mode, is the bound version.
fn part(key: &str, sequence: Option<u64>, (tenant, subject): (Option<String>, String)) -> namespace::Part {
    namespace::Part {
        kind: "ingestion".into(),
        identity: key.to_owned(),
        version: sequence.unwrap_or(0),
        tenant,
        subject,
    }
}

/// Refuse what a sealed target would store as plaintext, before the sink writes: every written input
/// whose row key names a subject must carry a keyed fingerprint (a hash of ciphertext would never match
/// a replay), and its `private` must be absent or sealed under the key's tenant and subject. A plaintext
/// target passes; its erasure cleanup deletes the subject's rows instead.
pub fn check_sealed(contract: &Contract, inputs: &[Input]) -> Result<(), Error> {
    validate_inputs(contract, inputs)?;
    for input in inputs {
        let Some((tenant, subject)) = sealed_scope(contract, input)? else {
            continue;
        };
        if input.fingerprint.is_none() {
            return Err(Error::bad_handle("a sealed ingestion row needs a keyed fingerprint"));
        }
        namespace::check_sealed(input.private.as_ref(), tenant.as_deref(), &subject)?;
    }
    Ok(())
}

/// What a sink seals before it writes, as `(input index, part)`: the present private part of each input
/// [`check_sealed`] requires sealed.
pub fn seal_plan(contract: &Contract, inputs: &[Input]) -> Result<Vec<(usize, namespace::Part)>, Error> {
    validate_inputs(contract, inputs)?;
    let mut parts = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        if input.private.as_ref().is_none_or(Value::is_null) {
            continue;
        }
        if let Some(scope) = sealed_scope(contract, input)? {
            parts.push((index, part(&input.key, input.sequence, scope)));
        }
    }
    Ok(parts)
}

/// What a sink opens in the row it read at `key`, or none: a plaintext target, a row whose key names no
/// subject, or a row without a private part.
pub fn open_plan(contract: &Contract, key: &str, stored: &Stored) -> Result<Option<namespace::Part>, Error> {
    bind_contract(None, contract)?;
    if contract.at_rest == AtRest::Plaintext.as_str() || stored.private.as_ref().is_none_or(Value::is_null) {
        return Ok(None);
    }
    Ok(row_scope(contract, key)?.map(|scope| part(key, stored.sequence, scope)))
}

/// One original source record, before compaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Input<T = Value> {
    /// Immutable source coordinate, including source incarnation.
    pub source: String,
    /// Fully scoped target key.
    pub key: String,
    /// Aggregate stream that permanently owns an upsert key.
    pub stream: String,
    /// Stable event identity; the append key must include it.
    pub event_id: String,
    /// Aggregate sequence. Upserts require a PostgreSQL bigint-compatible value.
    pub sequence: Option<u64>,
    /// Canonical event content used to detect conflicting replays.
    pub content: T,
    /// The mapped state's private part: sealed at rest when the host configures sealing.
    pub private: Option<T>,
    /// The mapped state's public part, stored as plaintext. Both parts absent: a retained deletion
    /// marker.
    pub public: Option<T>,
    /// The record's key was destroyed: accept it without a write, so an erased subject's event never
    /// recreates its data.
    #[serde(default)]
    pub erased: bool,
    /// [`keyed_fingerprint`] of a sealed record. Without it the plan hashes `content`, `private` and
    /// `public` unkeyed, which suits plaintext only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// Lossless source representation retained by a rejection.
    pub raw: String,
    /// SHA-256 of the original source record, 64 lowercase hex digits, independent of a retained
    /// full record or stub. Kafka's raw encoding supplies it; [`raw_fingerprint()`] computes it for any
    /// other source.
    pub raw_fingerprint: String,
    /// An already classified decode or mapping rejection in kernel error wire form.
    pub failure: Option<String>,
}

/// Parse original payload tokens before JSON can round an integer wider than 64 bits.
/// Invalid payloads become per-record rejections; source/sequence fields keep their full width.
pub fn inputs_json(text: &str) -> Result<Vec<Input>, Error> {
    use crate::json_numbers::{check_integers, Width};
    use serde_json::value::RawValue;

    let inputs: Vec<Input<&RawValue>> = serde_json::from_str(text).map_err(Error::codec)?;
    inputs
        .into_iter()
        .map(|input| {
            let parse = |raw: &RawValue| -> Result<Value, Error> {
                // Preserve integers the Value model represents exactly. The planner applies
                // canonical-number validation after sequence and durable rejection checks.
                check_integers(raw.get().as_bytes(), Width::Bits64)?;
                serde_json::from_str(raw.get()).map_err(Error::codec)
            };
            let payload = parse(input.content).and_then(|content| {
                let private = input.private.map(parse).transpose()?;
                let public = input.public.map(parse).transpose()?;
                Ok((content, private, public))
            });
            let (content, private, public, failure) = match payload {
                Ok((content, private, public)) => (content, private, public, input.failure),
                Err(error) => (
                    Value::Null,
                    None,
                    None,
                    input.failure.or_else(|| Some(error.to_string())),
                ),
            };
            Ok(Input {
                source: input.source,
                key: input.key,
                stream: input.stream,
                event_id: input.event_id,
                sequence: input.sequence,
                content,
                private,
                public,
                erased: input.erased,
                fingerprint: input.fingerprint,
                raw: input.raw,
                raw_fingerprint: input.raw_fingerprint,
                failure,
            })
        })
        .collect()
}

/// The committed value and its permanent owner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stored {
    /// Permanent aggregate stream owner, including for deletion markers.
    pub stream: String,
    /// Last accepted aggregate sequence; append mode does not require one.
    pub sequence: Option<u64>,
    /// Hash of canonical event content and mapped state: keyed for a sealed record.
    pub fingerprint: String,
    /// Private part of the state, sealed when the host configures sealing.
    pub private: Option<Value>,
    /// Public part of the state. Both parts absent: a retained deletion marker.
    pub public: Option<Value>,
}

/// Durable refusal, independent of later target state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Rejection {
    /// Hash of the original source representation.
    pub fingerprint: String,
    /// Original error in kernel wire form.
    pub error: String,
    /// Original source representation for the dead-letter publisher.
    pub raw: String,
    /// Whether publication has been confirmed; delivery itself remains at least once.
    pub published: bool,
}

/// Atomic changes plus an outcome for every original input, in source order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// `accepted` includes exact and stale replays; `rejected` requires dead-letter completion.
    pub outcomes: Vec<String>,
    /// Changed target rows only. Deletion markers are rows, not removals.
    pub writes: BTreeMap<String, Stored>,
    /// New rejections and persisted rejections touched by this call. Persist only absent keys:
    /// a fingerprint-verified replay may carry its current bounded raw representation.
    pub rejections: BTreeMap<String, Rejection>,
}

fn canonical_state(content: &Value, private: Option<&Value>, public: Option<&Value>) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(&(content, private, public)).map_err(Error::codec)?;
    canonical_json(&bytes)
}

/// The fingerprint of an input: the host's keyed fingerprint when it has one, else an unkeyed hash.
/// Canonicalizing still refuses a number no store can hold exactly.
fn fingerprint(input: &Input) -> Result<String, Error> {
    let canonical = canonical_state(&input.content, input.private.as_ref(), input.public.as_ref())?;
    Ok(input
        .fingerprint
        .clone()
        .unwrap_or_else(|| payload_hash(&canonical).to_string()))
}

/// The fingerprint of a sealed record: HMAC-SHA-256 under the subject's fingerprint key over the
/// canonical `content` and the plaintext `private` and `public` parts, before `private` is sealed.
pub fn keyed_fingerprint(
    key: &[u8],
    content: &Value,
    private: Option<&Value>,
    public: Option<&Value>,
) -> Result<String, Error> {
    keyed_digest(
        key,
        b"edax.ingestion.state/v1\0",
        &canonical_state(content, private, public)?,
    )
}

/// The raw fingerprint of a source record whose `raw` representation is the whole record.
pub fn raw_fingerprint(raw: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::new()
            .chain_update(b"edax.ingestion.raw/v1\0")
            .chain_update(raw)
            .finalize()
    )
}

fn reject(plan: &mut Plan, input: &Input, error: String) {
    plan.rejections.insert(
        input.source.clone(),
        Rejection {
            fingerprint: input.raw_fingerprint.clone(),
            error,
            raw: input.raw.clone(),
            published: false,
        },
    );
    plan.outcomes.push(Outcome::Rejected.as_str().into());
}

/// Plan one append or full-state upsert transaction against locked committed state.
///
/// Persisted rejects and invalid inputs cannot propose ownership or participate in compaction.
/// Upserts compare stale inputs against committed state, retain the first value at each newer
/// sequence, and write only the greatest accepted sequence. No historical fingerprints survive
/// beyond the current state. Unknown modes and reused source coordinates fail the whole call.
pub fn plan(
    mode: &str,
    inputs: &[Input],
    stored: &BTreeMap<String, Stored>,
    rejected: &BTreeMap<String, Rejection>,
) -> Result<Plan, Error> {
    let mode = Mode::by_name(mode).ok_or_else(|| Error::bad_handle("ingestion mode must be append or upsert"))?;
    let mut sources = BTreeMap::<&str, &Input>::new();
    for input in inputs {
        if sources
            .insert(&input.source, input)
            .is_some_and(|prior| prior.raw_fingerprint != input.raw_fingerprint)
            || rejected
                .get(&input.source)
                .is_some_and(|prior| prior.fingerprint != input.raw_fingerprint)
        {
            return Err(Error::bad_handle(
                "ingestion source coordinate reused with different raw content",
            ));
        }
    }
    let mut result = Plan {
        outcomes: Vec::with_capacity(inputs.len()),
        writes: BTreeMap::new(),
        rejections: BTreeMap::new(),
    };
    let mut owners = BTreeMap::<String, String>::new();
    let mut versions = BTreeMap::<(String, u64), String>::new();
    for input in inputs {
        if let Some(prior) = rejected
            .get(&input.source)
            .or_else(|| result.rejections.get(&input.source))
        {
            let prior = Rejection {
                fingerprint: prior.fingerprint.clone(),
                error: prior.error.clone(),
                // A store need not read back a durable rejection's raw: the verified replay carries it.
                raw: if rejected.contains_key(&input.source) {
                    input.raw.clone()
                } else {
                    prior.raw.clone()
                },
                published: prior.published,
            };
            result.rejections.insert(input.source.clone(), prior);
            result.outcomes.push(Outcome::Rejected.as_str().into());
            continue;
        }
        if input.erased {
            result.outcomes.push(Outcome::Accepted.as_str().into());
            continue;
        }
        if let Some(failure) = &input.failure {
            // Diagnostics are durable jsonb too. Never let untrusted exception text make
            // a rejected original impossible to persist or exhaust the batch reservation.
            let failure = if failure.contains('\0') || failure.len() > 256 {
                Error::new(
                    Error::from_wire(failure).code,
                    "ingestion preparation refused this original",
                )
                .to_string()
            } else {
                failure.clone()
            };
            reject(&mut result, input, failure);
            continue;
        }
        // Postgres cannot store NUL in jsonb: refuse the record here, as every store would.
        if holds_nul(&input.content)
            || input.private.as_ref().is_some_and(holds_nul)
            || input.public.as_ref().is_some_and(holds_nul)
            || input.stream.contains('\0')
            || input.event_id.contains('\0')
        {
            reject(
                &mut result,
                input,
                Error::codec("an ingested record holds a NUL character, which Postgres cannot store").to_string(),
            );
            continue;
        }
        if mode == Mode::Upsert && input.sequence.is_none_or(|sequence| sequence > i64::MAX as u64) {
            reject(
                &mut result,
                input,
                Error::codec("upsert requires a sequence in 0..=i64::MAX").to_string(),
            );
            continue;
        }
        let fingerprint = match fingerprint(input) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                reject(&mut result, input, error.to_string());
                continue;
            }
        };
        if mode == Mode::Append {
            if let Some(prior) = result.writes.get(&input.key).or_else(|| stored.get(&input.key)) {
                if prior.fingerprint != fingerprint {
                    reject(
                        &mut result,
                        input,
                        Error::new(ErrorCode::Rejected, "append event identity has different content").to_string(),
                    );
                    continue;
                }
            } else {
                result.writes.insert(
                    input.key.clone(),
                    Stored {
                        stream: input.stream.clone(),
                        sequence: input.sequence,
                        fingerprint,
                        private: input.private.clone(),
                        public: input.public.clone(),
                    },
                );
            }
            result.outcomes.push(Outcome::Accepted.as_str().into());
            continue;
        }
        let owner = stored
            .get(&input.key)
            .map(|row| &row.stream)
            .or_else(|| owners.get(&input.key));
        if owner.is_some_and(|owner| owner != &input.stream) {
            reject(
                &mut result,
                input,
                Error::new(ErrorCode::Rejected, "upsert key belongs to another aggregate stream").to_string(),
            );
            continue;
        }
        // Presence was checked above; keep the fallible boundary explicit rather than panic.
        let sequence = input
            .sequence
            .ok_or_else(|| Error::bad_handle("validated sequence missing"))?;
        if let Some(current) = stored.get(&input.key) {
            let current_sequence = current
                .sequence
                .ok_or_else(|| Error::bad_handle("stored upsert sequence missing"))?;
            if sequence < current_sequence {
                result.outcomes.push(Outcome::Accepted.as_str().into());
                continue;
            }
            if sequence == current_sequence {
                if fingerprint != current.fingerprint {
                    reject(
                        &mut result,
                        input,
                        Error::new(
                            ErrorCode::Rejected,
                            "committed aggregate sequence has different content",
                        )
                        .to_string(),
                    );
                } else {
                    result.outcomes.push(Outcome::Accepted.as_str().into());
                }
                continue;
            }
        }
        let version_key = (input.key.clone(), sequence);
        if versions.get(&version_key).is_some_and(|prior| prior != &fingerprint) {
            reject(
                &mut result,
                input,
                Error::new(
                    ErrorCode::Rejected,
                    "aggregate sequence has different content in this batch",
                )
                .to_string(),
            );
            continue;
        }
        owners.entry(input.key.clone()).or_insert_with(|| input.stream.clone());
        versions.insert(version_key, fingerprint.clone());
        if result
            .writes
            .get(&input.key)
            .is_none_or(|prior| prior.sequence < Some(sequence))
        {
            result.writes.insert(
                input.key.clone(),
                Stored {
                    stream: input.stream.clone(),
                    sequence: Some(sequence),
                    fingerprint,
                    private: input.private.clone(),
                    public: input.public.clone(),
                },
            );
        }
        result.outcomes.push(Outcome::Accepted.as_str().into());
    }
    Ok(result)
}
