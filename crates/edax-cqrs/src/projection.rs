//! Ordered projection state: a reducer folds delta events into per-aggregate state.
//!
//! A store runs every operation in one transaction under the lock [`lock`] names: the generation lock,
//! shared for partition work and exclusive for generation-wide changes, plus the partition's own lock.
//! Every transition is pure. A batch runs in two steps inside one transaction: `PrepareBatch` proves
//! which events are novel, the host calls its reducer once per novel event, and `ApplyBatch` checks the
//! results and returns every write. The host reads each record once and writes each change once.

use crate::{canonical_json, holds_nul, namespace, payload_hash};
use edax_core::{Error, ErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Parse a command without rounding reducer-visible integer literals. Protocol counters
/// retain their i64 domain; only application payloads use the canonical numeric domain.
pub fn request_json(text: &str) -> Result<Request, Error> {
    use crate::json_numbers::{check_integers, Width};
    use serde_json::value::RawValue;

    fn fields(text: &str) -> Result<BTreeMap<String, &RawValue>, Error> {
        serde_json::from_str(text).map_err(Error::codec)
    }
    fn items(text: &str) -> Result<Vec<&RawValue>, Error> {
        serde_json::from_str(text).map_err(Error::codec)
    }
    let request = fields(text)?;
    if let Some(operation) = request.get("operation") {
        let operation = fields(operation.get())?;
        if let Some(inputs) = operation.get("inputs") {
            for input in items(inputs.get())? {
                if let Some(event) = fields(input.get())?.get("event") {
                    let event = fields(event.get())?;
                    for name in ["payload", "metadata"] {
                        if let Some(value) = event.get(name) {
                            check_integers(value.get().as_bytes(), Width::Double)?;
                        }
                    }
                }
            }
        }
        if let Some(reduced) = operation.get("reduced") {
            check_integers(reduced.get().as_bytes(), Width::Double)?;
        }
    }
    // Keep structural error messages independent of host JSON whitespace.
    let value: Value = serde_json::from_str(text).map_err(Error::codec)?;
    serde_json::from_value(value).map_err(Error::codec)
}

/// Immutable interpretation and complete partition set of a generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    /// Isolated physical generation namespace.
    pub generation: String,
    /// Verified cluster and topic incarnation.
    pub source_identity: String,
    /// Verified history lineage or enforced no-truncation contract.
    pub source_history: String,
    /// Source topic name.
    pub topic: String,
    /// Immutable reducer implementation identity.
    pub reducer_id: String,
    /// Immutable canonical-envelope version.
    pub envelope_id: String,
    /// Immutable aggregate keying contract.
    pub keying_id: String,
    /// Stable logical effect namespace shared across rebuilds.
    pub effect_namespace: String,
    #[serde(deserialize_with = "partition_map")]
    /// Complete explicit partition start vector.
    pub starts: BTreeMap<i32, i64>,
}

/// Evidence obtained from a trusted source capability, not inferred from event sequences.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Proof {
    /// Verified physical source identity.
    pub identity: String,
    /// Verified source history lineage.
    pub history: String,
    /// Earliest retained offset.
    pub beginning: i64,
    /// Exclusive readable end offset.
    pub end: i64,
}

/// Reducer-visible canonical envelope. Transport coordinates are intentionally separate.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Tenant namespace, or absence for a single tenant.
    pub tenant: Option<String>,
    /// Complete aggregate stream identity.
    pub aggregate: String,
    /// Stable logical event identity.
    pub event_id: String,
    /// Aggregate sequence, starting at one.
    pub sequence: i64,
    /// Canonical event type.
    pub event_type: String,
    /// Canonical event schema version.
    pub schema_version: String,
    /// Canonical effect or event payload.
    pub payload: Value,
    /// All reducer-visible event metadata.
    pub metadata: Value,
}

/// One source record of a batch: its offset and its canonical event.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    /// Source offset; gaps between offsets are permitted.
    pub offset: i64,
    /// Canonical reducer input.
    pub event: Event,
}

/// One deterministic external effect.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    /// Reducer-defined stable effect key within the event.
    pub key: String,
    /// Logical effect destination.
    pub destination: String,
    /// Canonical effect or event payload.
    pub payload: Value,
}

/// Pure reducer output.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reduced {
    /// Replacement aggregate state, including JSON null.
    pub state: Value,
    /// Deterministic external effects.
    pub intents: Vec<Intent>,
}

/// Database-authoritative partition ownership and progress.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    /// Exact generation/source/topic/partition checkpoint identity.
    pub checkpoint_key: String,
    /// Registered source partition.
    pub partition: i32,
    /// Unique acquisition owner.
    pub owner: String,
    /// Monotonically increasing ownership fence.
    pub epoch: i64,
    /// Authoritative next source offset.
    pub next_offset: i64,
    /// Database-clock lease expiry in milliseconds.
    pub expires_ms: i64,
}

/// Frozen source boundary, carried intact to the rebuilt generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cutover {
    /// Verified cluster and topic incarnation.
    pub source_identity: String,
    /// Verified history lineage or enforced no-truncation contract.
    pub source_history: String,
    /// Source topic name.
    pub topic: String,
    #[serde(deserialize_with = "partition_map")]
    /// Immutable inclusive effect activation offsets.
    pub cutoffs: BTreeMap<i32, i64>,
}

/// Closed projection transition protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    /// Create an empty building generation or validate an existing one.
    Initialize,
    /// Acquire an unowned or expired checkpoint.
    Acquire {
        /// Registered source partition.
        partition: i32,
        /// Unique acquisition owner.
        owner: String,
        /// Positive lease duration in milliseconds.
        lease_ms: i64,
        /// Verified source identity and retained boundaries.
        proof: Proof,
    },
    /// Reload and extend a still-current assignment, fencing an expired lease anew.
    Refresh {
        /// Previously acquired ownership identity.
        lease: Lease,
        /// Positive lease duration.
        lease_ms: i64,
        /// Fresh verified source boundaries.
        proof: Proof,
    },
    /// Release only the matching ownership fence.
    Release {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
    },
    /// Reload the authoritative cursor after an uncertain commit.
    Load {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
    },
    /// Decide which inputs are novel, before the host calls its reducer. Writes nothing.
    PrepareBatch {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
        /// Source records in offset order.
        inputs: Vec<Input>,
        /// Verified source identity and retained boundaries.
        proof: Proof,
    },
    /// Atomically advance state, both receipts, effects and cursor for a whole batch.
    ApplyBatch {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
        /// The inputs `PrepareBatch` decided over.
        inputs: Vec<Input>,
        /// Verified source identity and retained boundaries.
        proof: Proof,
        /// One reducer result per input: present for a novel input, absent for a replay.
        reduced: Vec<Option<Reduced>>,
    },
    /// Stop all writes and capture a committed cutoff vector.
    Freeze,
    /// Enable effects using a complete immutable cutoff vector.
    Activate {
        /// Source-bound frozen partition vector.
        cutover: Cutover,
    },
    /// Read one generation-scoped aggregate state.
    Get {
        /// Tenant namespace, or absence for a single tenant.
        tenant: Option<String>,
        /// Complete aggregate stream identity.
        aggregate: String,
    },
}

/// A generation-bound projection command.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Expected immutable generation contract.
    pub contract: Contract,
    /// Requested projection transition.
    pub operation: Operation,
}

/// The locks a store takes, in this order, before it reads any record of an operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Lock {
    /// Take the generation lock exclusively: initialization, freeze and activation see no other writer.
    pub exclusive: bool,
    /// Also take this partition's lock: one writer per partition's checkpoint and aggregates.
    pub partition: Option<i32>,
}

/// Atomic projection writes and the resulting host value.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Plan {
    /// Records the operation read as absent. A store inserts them and fails on a concurrent insert,
    /// so two partitions racing on one event identity cannot both win.
    pub inserts: BTreeMap<String, Value>,
    /// Records the operation read as present, under a lock that excludes every other writer.
    pub updates: BTreeMap<String, Value>,
    /// New effect intents, stored apart from state so delivery never rewrites state rows.
    pub intents: BTreeMap<String, Value>,
    /// Operation result returned only after commit.
    pub result: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    contract: Contract,
    active: bool,
    frozen: bool,
    cutoffs: BTreeMap<i32, i64>,
    upper: BTreeMap<i32, i64>,
}

/// A stored state's position, read without copying its value.
#[derive(Deserialize)]
struct StateHead {
    sequence: i64,
    partition: i32,
}

/// A record as this transaction sees it: staged by an earlier input of the batch, else as read.
fn staged<'a>(
    writes: &'a BTreeMap<String, Value>,
    records: &'a BTreeMap<String, Value>,
    key: &str,
) -> Option<&'a Value> {
    writes.get(key).or_else(|| records.get(key))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct State {
    sequence: i64,
    partition: i32,
    value: Value,
}

fn required(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.contains('\0') {
        return Err(Error::bad_handle("empty or NUL projection identity"));
    }
    Ok(())
}

impl Contract {
    /// Validate all immutable identities and registered starts.
    pub fn validate(&self) -> Result<(), Error> {
        for value in [
            &self.generation,
            &self.source_identity,
            &self.source_history,
            &self.topic,
            &self.reducer_id,
            &self.envelope_id,
            &self.keying_id,
            &self.effect_namespace,
        ] {
            required(value)?;
        }
        if self.starts.is_empty() || self.starts.iter().any(|(p, o)| *p < 0 || *o < 0) {
            return Err(Error::bad_handle(
                "projection requires explicit nonnegative partition starts",
            ));
        }
        Ok(())
    }
    /// Build a generation-scoped canonical storage key.
    pub fn key(&self, parts: Value) -> Result<String, Error> {
        namespace::key(&self.generation, &parts.to_string())
    }
    /// Generation manifest and generation lock identity.
    pub fn manifest_key(&self) -> Result<String, Error> {
        self.key(json!(["projection", "manifest"]))
    }
    fn checkpoint_key(&self, partition: i32) -> Result<String, Error> {
        if !self.starts.contains_key(&partition) {
            return Err(Error::bad_handle("unregistered projection partition"));
        }
        self.key(json!([
            "projection",
            "checkpoint",
            self.source_identity,
            self.topic,
            partition
        ]))
    }
    fn state_key(&self, tenant: &Option<String>, aggregate: &str) -> Result<String, Error> {
        required(aggregate)?;
        self.key(json!(["projection", "state", tenant, aggregate]))
    }
    fn event_keys(&self, event: &Event) -> Result<[String; 3], Error> {
        required(&event.event_id)?;
        required(&event.event_type)?;
        required(&event.schema_version)?;
        if event.sequence < 1 {
            return Err(Error::codec("projection sequence must start at one"));
        }
        if event.tenant.as_deref().is_some_and(|tenant| tenant.contains('\0')) {
            return Err(Error::codec("projection tenant holds a NUL character"));
        }
        Ok([
            self.state_key(&event.tenant, &event.aggregate)?,
            self.key(json!(["projection", "event", event.tenant, event.event_id]))?,
            self.key(json!([
                "projection",
                "sequence",
                event.tenant,
                event.aggregate,
                event.sequence.to_string()
            ]))?,
        ])
    }
    /// Fail closed on identity/history changes or missing required input.
    pub fn continuity(&self, proof: &Proof, cursor: i64) -> Result<(), Error> {
        if proof.identity != self.source_identity
            || proof.history != self.source_history
            || proof.beginning < 0
            || proof.end < proof.beginning
            || cursor < proof.beginning
            || cursor > proof.end
        {
            return Err(Error::bad_handle(
                "projection source continuity is unproven or required history is missing",
            ));
        }
        Ok(())
    }
    /// The storage key of the effect intent `id`, as delivery marks it.
    pub fn intent_key(&self, id: &str) -> Result<String, Error> {
        required(id)?;
        self.key(json!(["projection", "intent", id]))
    }
}

/// Keys of the state records an operation reads, the manifest first. Stores read them after taking
/// [`lock`].
pub fn keys(request: &Request) -> Result<Vec<String>, Error> {
    let c = &request.contract;
    c.validate()?;
    let mut keys = vec![c.manifest_key()?];
    match &request.operation {
        Operation::Initialize | Operation::Freeze | Operation::Activate { .. } => {
            for p in c.starts.keys() {
                keys.push(c.checkpoint_key(*p)?);
            }
        }
        Operation::Acquire { partition, .. } => keys.push(c.checkpoint_key(*partition)?),
        Operation::Refresh { lease, .. } | Operation::Release { lease } | Operation::Load { lease } => {
            keys.push(c.checkpoint_key(lease.partition)?);
        }
        Operation::PrepareBatch { lease, inputs, .. } | Operation::ApplyBatch { lease, inputs, .. } => {
            keys.push(c.checkpoint_key(lease.partition)?);
            for input in inputs {
                keys.extend(c.event_keys(&input.event)?);
            }
        }
        Operation::Get { tenant, aggregate } => keys.push(c.state_key(tenant, aggregate)?),
    }
    // The manifest stays first; a batch names each record once.
    let mut seen = std::collections::BTreeSet::new();
    keys.retain(|key| seen.insert(key.clone()));
    Ok(keys)
}

/// The locks an operation runs under. Partition work shares the generation lock, so partitions commit
/// in parallel; initialization, freeze and activation take it alone.
pub fn lock(request: &Request) -> Result<Lock, Error> {
    let partition = match &request.operation {
        Operation::Initialize | Operation::Freeze | Operation::Activate { .. } => {
            return Ok(Lock {
                exclusive: true,
                partition: None,
            });
        }
        Operation::Get { .. } => None,
        Operation::Acquire { partition, .. } => Some(*partition),
        Operation::Refresh { lease, .. }
        | Operation::Release { lease }
        | Operation::Load { lease }
        | Operation::PrepareBatch { lease, .. }
        | Operation::ApplyBatch { lease, .. } => Some(lease.partition),
    };
    if let Some(p) = partition {
        request.contract.checkpoint_key(p)?;
    }
    Ok(Lock {
        exclusive: false,
        partition,
    })
}

fn read<T: serde::de::DeserializeOwned>(records: &BTreeMap<String, Value>, key: &str) -> Result<T, Error> {
    serde_json::from_value(
        records
            .get(key)
            .cloned()
            .ok_or_else(|| Error::bad_handle("missing projection storage record"))?,
    )
    .map_err(Error::codec)
}

fn lease_check(c: &Contract, records: &BTreeMap<String, Value>, lease: &Lease, now: i64) -> Result<Lease, Error> {
    let stored: Lease = read(records, &c.checkpoint_key(lease.partition)?)?;
    if lease.checkpoint_key != c.checkpoint_key(lease.partition)?
        || stored.checkpoint_key != lease.checkpoint_key
        || stored.partition != lease.partition
        || stored.owner != lease.owner
        || stored.epoch != lease.epoch
        || stored.next_offset != lease.next_offset
        || stored.expires_ms <= now
        || stored.owner.is_empty()
    {
        return Err(Error::bad_handle("stale projection owner, cursor or expired lease"));
    }
    Ok(stored)
}

fn expiry(now: i64, duration: i64) -> Result<i64, Error> {
    if now < 0 || duration <= 0 {
        return Err(Error::bad_handle("invalid projection lease duration or clock"));
    }
    now.checked_add(duration)
        .ok_or_else(|| Error::bad_handle("projection lease clock overflow"))
}

fn next_epoch(checkpoint: &Lease) -> Result<i64, Error> {
    checkpoint
        .epoch
        .checked_add(1)
        .ok_or_else(|| Error::bad_handle("projection epoch exhausted"))
}

/// Stable across generation rebuilds, restore, delivery attempts and source offsets.
pub fn intent_id(c: &Contract, event: &Event, intent: &Intent) -> Result<String, Error> {
    required(&intent.key)?;
    required(&intent.destination)?;
    namespace::key(
        &c.effect_namespace,
        &json!([event.tenant, event.aggregate, event.event_id, intent.key]).to_string(),
    )
}

/// What a batch decided: the reducer input per input, the writes and the advanced lease.
struct Batch {
    items: Vec<Value>,
    writes: BTreeMap<String, Value>,
    intents: BTreeMap<String, Value>,
    lease: Lease,
}

/// Run a batch over `records`, in source order. Without `reduced` it only decides novelty: each novel
/// event still stages its receipts and sequence, so a later copy in the same batch reads
/// as a replay and a later delta of the same aggregate needs the next sequence.
#[allow(clippy::too_many_arguments)]
fn run_batch(
    c: &Contract,
    manifest: &Manifest,
    records: &BTreeMap<String, Value>,
    lease: &Lease,
    inputs: &[Input],
    proof: &Proof,
    reduced: Option<&[Option<Reduced>]>,
    now: i64,
) -> Result<Batch, Error> {
    if manifest.frozen {
        return Err(Error::bad_handle("projection generation is frozen"));
    }
    if reduced.is_some_and(|r| r.len() != inputs.len()) {
        return Err(Error::bad_handle("projection batch needs one reducer result per input"));
    }
    // The state each novel event's reducer starts from is sent once per aggregate.
    let mut sent_state = std::collections::BTreeSet::new();
    let mut batch = Batch {
        items: Vec::with_capacity(inputs.len()),
        writes: BTreeMap::new(),
        intents: BTreeMap::new(),
        lease: lease_check(c, records, lease, now)?,
    };
    for (index, Input { offset, event }) in inputs.iter().enumerate() {
        let checkpoint = &mut batch.lease;
        c.continuity(proof, checkpoint.next_offset)?;
        if *offset < checkpoint.next_offset || *offset >= proof.end {
            return Err(Error::bad_handle("projection record outside proven source interval"));
        }
        let [state_key, event_key, sequence_key] = c.event_keys(event)?;
        let mut canonical_event = json!(event);
        *canonical_event
            .get_mut("sequence")
            .ok_or_else(|| Error::codec("missing event sequence"))? = json!(event.sequence.to_string());
        let canonical = canonical_json(&serde_json::to_vec(&canonical_event).map_err(Error::codec)?)?;
        // Normalized values must remain valid reducer inputs after storage and replay.
        // A float such as 1e18 otherwise becomes an unsafe integer on the next JSON crossing.
        canonical_json(&canonical)?;
        let mut normalized_event: Value = serde_json::from_slice(&canonical).map_err(Error::codec)?;
        *normalized_event
            .get_mut("sequence")
            .ok_or_else(|| Error::codec("missing sequence"))? = json!(event.sequence);
        // The receipt binds a hash of the canonical event: the same conflict check, without keeping
        // a second copy of every payload.
        let receipt = json!({"fingerprint": payload_hash(&canonical).to_string(), "partition": checkpoint.partition});
        let by_event = staged(&batch.writes, records, &event_key);
        let by_sequence = staged(&batch.writes, records, &sequence_key);
        let duplicate = by_event.is_some() || by_sequence.is_some();
        // A receipt that differs blames the message; a missing partner receipt blames the store.
        if [by_event, by_sequence]
            .into_iter()
            .flatten()
            .any(|saved| *saved != receipt)
        {
            return Err(Error::codec("conflicting projection event identity"));
        }
        if by_event.is_some() != by_sequence.is_some() {
            return Err(Error::bad_handle("incomplete projection deduplication indexes"));
        }
        let state = staged(&batch.writes, records, &state_key)
            .map(StateHead::deserialize)
            .transpose()
            .map_err(Error::codec)?;
        if state.as_ref().is_some_and(|s| s.partition != checkpoint.partition) {
            return Err(Error::codec("aggregate moved to another source partition"));
        }
        if !duplicate && state.as_ref().is_some_and(|s| s.sequence >= event.sequence) {
            return Err(Error::bad_handle(
                "projection state is ahead of its deduplication records",
            ));
        }
        if !duplicate && state.as_ref().map_or(Some(1), |s| s.sequence.checked_add(1)) != Some(event.sequence) {
            return Err(Error::codec("projection aggregate sequence gap"));
        }
        if duplicate && state.as_ref().is_none_or(|s| s.sequence < event.sequence) {
            return Err(Error::bad_handle("projection state and deduplication restore disagree"));
        }
        if reduced.is_none() {
            // The reducer starts from the state the batch read; the host carries it between deltas.
            let first = !duplicate && sent_state.insert(state_key.clone());
            let before = first
                .then(|| records.get(&state_key).and_then(|v| v.get("value")))
                .flatten();
            batch
                .items
                .push(json!({"novel": !duplicate, "state": before, "event": normalized_event}));
        }
        if !duplicate {
            let result = match reduced.and_then(|r| r.get(index)) {
                None => None,
                Some(None) => return Err(Error::bad_handle("projection reducer result missing for a novel event")),
                Some(Some(result)) => Some(checked(result)?),
            };
            let value = result.as_ref().map_or(Value::Null, |r| r.state.clone());
            let state = json!(State {
                sequence: event.sequence,
                partition: checkpoint.partition,
                value
            });
            for (key, value) in [
                (state_key, state),
                (event_key, receipt.clone()),
                (sequence_key, receipt),
            ] {
                batch.writes.insert(key, value);
            }
            for (ordinal, intent) in result.iter().flat_map(|r| r.intents.iter()).enumerate() {
                let id = intent_id(c, event, intent)?;
                let key = c.intent_key(&id)?;
                let record = json!({
                    "id": id,
                    "generation": c.generation,
                    "tenant": event.tenant,
                    "aggregate": event.aggregate,
                    "destination": intent.destination,
                    "payload": intent.payload,
                    "partition": checkpoint.partition,
                    "offset": offset,
                    "ordinal": ordinal
                });
                if batch.intents.insert(key, record).is_some() {
                    return Err(Error::codec("duplicate projection intent key"));
                }
            }
        } else if reduced.and_then(|r| r.get(index)).is_some_and(Option::is_some) {
            return Err(Error::bad_handle(
                "projection reducer result given for a replayed event",
            ));
        }
        checkpoint.next_offset = offset
            .checked_add(1)
            .ok_or_else(|| Error::bad_handle("projection offset exhausted"))?;
        let key = c.checkpoint_key(checkpoint.partition)?;
        batch.writes.insert(key, json!(checkpoint));
    }
    Ok(batch)
}

/// A reducer result in canonical form, refused as `codec` when no store could keep it.
fn checked(reduced: &Reduced) -> Result<Reduced, Error> {
    let bytes = canonical_json(&serde_json::to_vec(reduced).map_err(Error::codec)?)?;
    canonical_json(&bytes)?;
    let reduced: Reduced = serde_json::from_slice(&bytes).map_err(Error::codec)?;
    if holds_nul(&reduced.state) || reduced.intents.iter().any(|intent| holds_nul(&intent.payload)) {
        return Err(Error::codec(
            "projection reducer output holds a NUL character, which Postgres cannot store",
        ));
    }
    Ok(reduced)
}

/// Apply one operation. The caller persists every write atomically, or none of them.
pub fn transition(request: &Request, records: &BTreeMap<String, Value>, now: i64) -> Result<Plan, Error> {
    keys(request)?;
    let c = &request.contract;
    let manifest_key = c.manifest_key()?;
    let mut writes = BTreeMap::new();
    let mut plan = Plan::default();
    if let Operation::Initialize = request.operation {
        if records.contains_key(&manifest_key) {
            let saved: Manifest = read(records, &manifest_key)?;
            if saved.contract != *c {
                return Err(Error::bad_handle("changed projection contract"));
            }
            for p in c.starts.keys() {
                let _: Lease = read(records, &c.checkpoint_key(*p)?)?;
            }
            return Ok(plan);
        }
        if records.keys().any(|key| key != &manifest_key) {
            return Err(Error::bad_handle("partial projection restore"));
        }
        let manifest = Manifest {
            contract: c.clone(),
            active: false,
            frozen: false,
            cutoffs: BTreeMap::new(),
            upper: BTreeMap::new(),
        };
        plan.inserts.insert(manifest_key, json!(manifest));
        for (partition, start) in &c.starts {
            let checkpoint = Lease {
                checkpoint_key: c.checkpoint_key(*partition)?,
                partition: *partition,
                owner: String::new(),
                epoch: 0,
                next_offset: *start,
                expires_ms: 0,
            };
            plan.inserts
                .insert(checkpoint.checkpoint_key.clone(), json!(checkpoint));
        }
        return Ok(plan);
    }
    let mut manifest: Manifest = read(records, &manifest_key)?;
    if manifest.contract != *c {
        return Err(Error::bad_handle("changed projection contract"));
    }
    match &request.operation {
        Operation::Initialize => return Err(Error::bad_handle("unexpected projection initialization")),
        Operation::Acquire {
            partition,
            owner,
            lease_ms,
            proof,
        } => {
            required(owner)?;
            if manifest.frozen {
                return Err(Error::bad_handle("projection generation is frozen"));
            }
            let key = c.checkpoint_key(*partition)?;
            let mut checkpoint: Lease = read(records, &key)?;
            let expires_ms = expiry(now, *lease_ms)?;
            // A live owner may have committed past this caller's proof: wait for it, check no proof.
            if checkpoint.expires_ms > now && !checkpoint.owner.is_empty() {
                if checkpoint.owner == *owner {
                    // The same unique acquisition retried after a lost response.
                    plan.result = json!(checkpoint);
                    return Ok(plan);
                }
                return Err(Error::new(ErrorCode::InProgress, "projection partition already owned"));
            }
            c.continuity(proof, checkpoint.next_offset)?;
            checkpoint.epoch = next_epoch(&checkpoint)?;
            checkpoint.owner.clone_from(owner);
            checkpoint.expires_ms = expires_ms;
            plan.result = json!(checkpoint);
            writes.insert(key, json!(checkpoint));
        }
        Operation::Refresh { lease, lease_ms, proof } => {
            required(&lease.owner)?;
            if manifest.frozen {
                return Err(Error::bad_handle("projection generation is frozen"));
            }
            let key = c.checkpoint_key(lease.partition)?;
            let mut checkpoint: Lease = read(records, &key)?;
            if lease.checkpoint_key != key
                || checkpoint.checkpoint_key != key
                || checkpoint.owner != lease.owner
                || checkpoint.epoch < lease.epoch
            {
                return Err(Error::bad_handle("stale projection ownership"));
            }
            c.continuity(proof, checkpoint.next_offset)?;
            if checkpoint.expires_ms <= now {
                checkpoint.epoch = next_epoch(&checkpoint)?;
            }
            checkpoint.expires_ms = expiry(now, *lease_ms)?;
            plan.result = json!(checkpoint);
            writes.insert(key, json!(checkpoint));
        }
        Operation::Load { lease } => {
            let stored: Lease = read(records, &c.checkpoint_key(lease.partition)?)?;
            let mut expected = lease.clone();
            expected.next_offset = stored.next_offset;
            plan.result = json!(lease_check(c, records, &expected, now)?);
        }
        Operation::Release { lease } => {
            let key = c.checkpoint_key(lease.partition)?;
            let mut checkpoint: Lease = read(records, &key)?;
            if lease.checkpoint_key == key
                && checkpoint.checkpoint_key == key
                && !lease.owner.is_empty()
                && checkpoint.owner == lease.owner
                && checkpoint.epoch == lease.epoch
            {
                checkpoint.owner.clear();
                checkpoint.expires_ms = 0;
                writes.insert(key, json!(checkpoint));
            }
        }
        Operation::PrepareBatch { lease, inputs, proof } => {
            let batch = run_batch(c, &manifest, records, lease, inputs, proof, None, now)?;
            plan.result = json!({"items": batch.items});
        }
        Operation::ApplyBatch {
            lease,
            inputs,
            proof,
            reduced,
        } => {
            let batch = run_batch(c, &manifest, records, lease, inputs, proof, Some(reduced), now)?;
            plan.result = json!(batch.lease);
            plan.intents = batch.intents;
            writes = batch.writes;
        }
        Operation::Freeze => {
            if !manifest.active {
                return Err(Error::bad_handle(
                    "cannot hand over effects from an inactive projection generation",
                ));
            }
            if !manifest.frozen {
                for p in c.starts.keys() {
                    let key = c.checkpoint_key(*p)?;
                    let mut checkpoint: Lease = read(records, &key)?;
                    manifest.upper.insert(*p, checkpoint.next_offset);
                    checkpoint.owner.clear();
                    checkpoint.expires_ms = 0;
                    writes.insert(key, json!(checkpoint));
                }
                manifest.frozen = true;
                writes.insert(manifest_key, json!(manifest));
            }
            plan.result = json!(Cutover {
                source_identity: c.source_identity.clone(),
                source_history: c.source_history.clone(),
                topic: c.topic.clone(),
                cutoffs: manifest.upper
            });
        }
        Operation::Activate { cutover } => {
            if cutover.source_identity != c.source_identity
                || cutover.source_history != c.source_history
                || cutover.topic != c.topic
            {
                return Err(Error::bad_handle(
                    "projection cutover belongs to another source history",
                ));
            }
            let cutoffs = &cutover.cutoffs;
            if manifest.frozen
                || cutoffs.keys().ne(c.starts.keys())
                || cutoffs
                    .iter()
                    .any(|(p, o)| c.starts.get(p).is_none_or(|start| o < start))
            {
                return Err(Error::bad_handle("invalid projection activation vector"));
            }
            if manifest.active && manifest.cutoffs != *cutoffs {
                return Err(Error::bad_handle("projection activation is immutable"));
            }
            for (p, cutoff) in cutoffs {
                let checkpoint: Lease = read(records, &c.checkpoint_key(*p)?)?;
                if checkpoint.next_offset < *cutoff {
                    return Err(Error::bad_handle("projection rebuild has not reached cutoff"));
                }
            }
            // Eligibility is read against these immutable cutoffs, so no historical intent is rewritten.
            manifest.active = true;
            manifest.cutoffs.clone_from(cutoffs);
            writes.insert(manifest_key, json!(manifest));
        }
        Operation::Get { tenant, aggregate } => {
            plan.result = records
                .get(&c.state_key(tenant, aggregate)?)
                .cloned()
                .unwrap_or(Value::Null);
        }
    }
    for (key, value) in writes {
        if records.contains_key(&key) {
            plan.updates.insert(key, value);
        } else {
            plan.inserts.insert(key, value);
        }
    }
    Ok(plan)
}

/// Whether an intent may be delivered under a generation's manifest: at or above its partition's
/// activation cutoff, and below its frozen upper bound. Every store reads it at delivery time.
pub fn eligible(manifest: &Value, intent: &Value) -> Result<bool, Error> {
    let m: Manifest = serde_json::from_value(manifest.clone()).map_err(Error::codec)?;
    let partition = intent
        .get("partition")
        .and_then(Value::as_i64)
        .and_then(|p| i32::try_from(p).ok())
        .ok_or_else(|| Error::codec("invalid intent partition"))?;
    let offset = intent
        .get("offset")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::codec("invalid intent offset"))?;
    Ok(m.active
        && m.cutoffs.get(&partition).is_some_and(|start| offset >= *start)
        && (!m.frozen || m.upper.get(&partition).is_some_and(|end| offset < *end)))
}

fn partition_map<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<BTreeMap<i32, i64>, D::Error> {
    let raw = BTreeMap::<String, i64>::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(key, value)| {
            let partition = key.parse::<i32>().map_err(serde::de::Error::custom)?;
            if partition < 0 || partition.to_string() != key {
                return Err(serde::de::Error::custom("invalid partition key"));
            }
            Ok((partition, value))
        })
        .collect()
}
