//! Ordered delta folds. The host holds a generation lock through the database commit.
//! Every transition is pure; reducers run only after `prepare` proves a novel event.

use crate::{canonical_json, ingest};
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
    let request = fields(text)?;
    if let Some(operation) = request.get("operation") {
        let operation = fields(operation.get())?;
        if let Some(event) = operation.get("event") {
            let event = fields(event.get())?;
            for name in ["payload", "metadata"] {
                if let Some(value) = event.get(name) {
                    check_integers(value.get().as_bytes(), Width::Double)?;
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

/// Closed fold transition protocol.
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
    /// Extend a current ownership lease.
    Renew {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
        /// Positive lease duration in milliseconds.
        lease_ms: i64,
    },
    /// Reload and refresh a still-current assignment, fencing an expired lease anew.
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
    /// Validate the event before invoking its reducer.
    Prepare {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
        /// Input source offset; gaps are permitted.
        offset: i64,
        /// Canonical reducer input.
        event: Event,
        /// Verified source identity and retained boundaries.
        proof: Proof,
    },
    /// Atomically advance state, both receipts, effects and cursor.
    Apply {
        /// Previously loaded ownership fence and cursor.
        lease: Lease,
        /// Input source offset; gaps are permitted.
        offset: i64,
        /// Canonical reducer input.
        event: Event,
        /// Verified source identity and retained boundaries.
        proof: Proof,
        /// Pure reducer result for a novel event.
        reduced: Reduced,
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
    /// Mark confirmed at-least-once external delivery.
    Published {
        /// Stable deterministic effect identity.
        intent_id: String,
    },
}

/// A generation-bound fold command.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Expected immutable generation contract.
    pub contract: Contract,
    /// Requested fold transition.
    pub operation: Operation,
}

/// Atomic fold writes and the resulting host value.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Plan {
    /// Activation cutoffs whose older intent rows become permanently suppressed.
    pub suppression: Option<BTreeMap<i32, i64>>,
    /// Whole records to persist atomically.
    pub writes: BTreeMap<String, Value>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct State {
    sequence: i64,
    partition: i32,
    value: Value,
}

fn required(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.contains('\0') {
        return Err(Error::bad_handle("empty or NUL fold identity"));
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
            return Err(Error::bad_handle("fold requires explicit nonnegative partition starts"));
        }
        Ok(())
    }
    /// Build a generation-scoped canonical storage key.
    pub fn key(&self, parts: Value) -> Result<String, Error> {
        ingest::key(&self.generation, &parts.to_string())
    }
    /// Generation manifest and transaction lock identity.
    pub fn manifest_key(&self) -> Result<String, Error> {
        self.key(json!(["fold", "manifest"]))
    }
    fn checkpoint_key(&self, partition: i32) -> Result<String, Error> {
        if !self.starts.contains_key(&partition) {
            return Err(Error::bad_handle("unregistered fold partition"));
        }
        self.key(json!([
            "fold",
            "checkpoint",
            self.source_identity,
            self.topic,
            partition
        ]))
    }
    fn state_key(&self, tenant: &Option<String>, aggregate: &str) -> Result<String, Error> {
        required(aggregate)?;
        self.key(json!(["fold", "state", tenant, aggregate]))
    }
    fn event_keys(&self, event: &Event) -> Result<[String; 3], Error> {
        required(&event.event_id)?;
        required(&event.event_type)?;
        required(&event.schema_version)?;
        if event.sequence < 1 {
            return Err(Error::codec("fold sequence must start at one"));
        }
        Ok([
            self.state_key(&event.tenant, &event.aggregate)?,
            self.key(json!(["fold", "event", event.tenant, event.event_id]))?,
            self.key(json!([
                "fold",
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
                "fold source continuity is unproven or required history is missing",
            ));
        }
        Ok(())
    }
    fn intent_key(&self, id: &str) -> Result<String, Error> {
        self.key(json!(["fold", "intent", id]))
    }
}

/// Keys needed by a transition. Stores read these under the generation lock.
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
        Operation::Renew { lease, .. }
        | Operation::Refresh { lease, .. }
        | Operation::Release { lease }
        | Operation::Load { lease } => keys.push(c.checkpoint_key(lease.partition)?),
        Operation::Prepare { lease, event, .. } | Operation::Apply { lease, event, .. } => {
            keys.push(c.checkpoint_key(lease.partition)?);
            keys.extend(c.event_keys(event)?);
        }
        Operation::Get { tenant, aggregate } => keys.push(c.state_key(tenant, aggregate)?),
        Operation::Published { intent_id } => keys.push(c.intent_key(intent_id)?),
    }
    Ok(keys)
}

fn read<T: serde::de::DeserializeOwned>(records: &BTreeMap<String, Value>, key: &str) -> Result<T, Error> {
    serde_json::from_value(
        records
            .get(key)
            .cloned()
            .ok_or_else(|| Error::bad_handle("missing fold storage record"))?,
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
        return Err(Error::bad_handle("stale fold owner, cursor or expired lease"));
    }
    Ok(stored)
}
fn expiry(now: i64, duration: i64) -> Result<i64, Error> {
    if now < 0 || duration <= 0 {
        return Err(Error::bad_handle("invalid fold lease duration or clock"));
    }
    now.checked_add(duration)
        .ok_or_else(|| Error::bad_handle("fold lease clock overflow"))
}

/// Stable across generation rebuilds, restore, delivery attempts and source offsets.
pub fn intent_id(c: &Contract, event: &Event, intent: &Intent) -> Result<String, Error> {
    required(&intent.key)?;
    required(&intent.destination)?;
    ingest::key(
        &c.effect_namespace,
        &json!([event.tenant, event.aggregate, event.event_id, intent.key]).to_string(),
    )
}

/// Apply one operation. The caller persists every write atomically, or none of them.
pub fn transition(request: &Request, records: &BTreeMap<String, Value>, now: i64) -> Result<Plan, Error> {
    keys(request)?;
    let c = &request.contract;
    let manifest_key = c.manifest_key()?;
    let mut plan = Plan::default();
    if let Operation::Initialize = request.operation {
        if records.contains_key(&manifest_key) {
            let saved: Manifest = read(records, &manifest_key)?;
            if saved.contract != *c {
                return Err(Error::bad_handle("changed fold contract"));
            }
            for p in c.starts.keys() {
                let _: Lease = read(records, &c.checkpoint_key(*p)?)?;
            }
            return Ok(plan);
        }
        if records.keys().any(|key| key != &manifest_key) {
            return Err(Error::bad_handle("partial fold restore"));
        }
        plan.writes.insert(
            manifest_key,
            json!(Manifest {
                contract: c.clone(),
                active: false,
                frozen: false,
                cutoffs: BTreeMap::new(),
                upper: BTreeMap::new()
            }),
        );
        for (partition, start) in &c.starts {
            plan.writes.insert(
                c.checkpoint_key(*partition)?,
                json!(Lease {
                    checkpoint_key: c.checkpoint_key(*partition)?,
                    partition: *partition,
                    owner: String::new(),
                    epoch: 0,
                    next_offset: *start,
                    expires_ms: 0
                }),
            );
        }
        return Ok(plan);
    }
    let mut manifest: Manifest = read(records, &manifest_key)?;
    if manifest.contract != *c {
        return Err(Error::bad_handle("changed fold contract"));
    }
    match &request.operation {
        Operation::Initialize => return Err(Error::bad_handle("unexpected fold initialization")),
        Operation::Acquire {
            partition,
            owner,
            lease_ms,
            proof,
        } => {
            required(owner)?;
            if manifest.frozen {
                return Err(Error::bad_handle("fold generation is frozen"));
            }
            let key = c.checkpoint_key(*partition)?;
            let mut checkpoint: Lease = read(records, &key)?;
            c.continuity(proof, checkpoint.next_offset)?;
            if checkpoint.expires_ms > now && !checkpoint.owner.is_empty() {
                if checkpoint.owner == *owner {
                    // The same unique acquisition retried after a lost response.
                    plan.result = json!(checkpoint);
                    return Ok(plan);
                }
                return Err(Error::new(ErrorCode::InProgress, "fold partition already owned"));
            }
            checkpoint.epoch = checkpoint
                .epoch
                .checked_add(1)
                .ok_or_else(|| Error::bad_handle("fold epoch exhausted"))?;
            checkpoint.owner.clone_from(owner);
            checkpoint.expires_ms = expiry(now, *lease_ms)?;
            plan.result = json!(checkpoint);
            plan.writes.insert(key, json!(checkpoint));
        }
        Operation::Renew { lease, lease_ms } => {
            if manifest.frozen {
                return Err(Error::bad_handle("fold generation is frozen"));
            }
            let mut checkpoint = lease_check(c, records, lease, now)?;
            checkpoint.expires_ms = expiry(now, *lease_ms)?;
            plan.result = json!(checkpoint);
            plan.writes
                .insert(c.checkpoint_key(lease.partition)?, json!(checkpoint));
        }
        Operation::Refresh { lease, lease_ms, proof } => {
            if manifest.frozen {
                return Err(Error::bad_handle("fold generation is frozen"));
            }
            let key = c.checkpoint_key(lease.partition)?;
            let mut checkpoint: Lease = read(records, &key)?;
            if lease.checkpoint_key != key
                || checkpoint.checkpoint_key != key
                || checkpoint.owner != lease.owner
                || checkpoint.epoch < lease.epoch
            {
                return Err(Error::bad_handle("stale fold ownership"));
            }
            c.continuity(proof, checkpoint.next_offset)?;
            if checkpoint.expires_ms <= now {
                checkpoint.epoch = checkpoint
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| Error::bad_handle("fold epoch exhausted"))?;
            }
            checkpoint.expires_ms = expiry(now, *lease_ms)?;
            plan.result = json!(checkpoint);
            plan.writes.insert(key, json!(checkpoint));
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
                && checkpoint.owner == lease.owner
                && checkpoint.epoch == lease.epoch
            {
                checkpoint.owner.clear();
                checkpoint.expires_ms = 0;
                plan.writes.insert(key, json!(checkpoint));
            }
        }
        Operation::Prepare {
            lease,
            offset,
            event,
            proof,
        }
        | Operation::Apply {
            lease,
            offset,
            event,
            proof,
            ..
        } => {
            if manifest.frozen {
                return Err(Error::bad_handle("fold generation is frozen"));
            }
            let mut checkpoint = lease_check(c, records, lease, now)?;
            c.continuity(proof, checkpoint.next_offset)?;
            if *offset < checkpoint.next_offset || *offset >= proof.end {
                return Err(Error::bad_handle("fold record outside proven source interval"));
            }
            let [state_key, event_key, sequence_key] = c.event_keys(event)?;
            let mut canonical_event = json!(event);
            *canonical_event
                .get_mut("sequence")
                .ok_or_else(|| Error::codec("missing event sequence"))? = json!(event.sequence.to_string());
            let fingerprint = String::from_utf8(canonical_json(
                &serde_json::to_vec(&canonical_event).map_err(Error::codec)?,
            )?)
            .map_err(Error::codec)?;
            // Normalized values must remain valid reducer inputs after storage and replay.
            // A float such as 1e18 otherwise becomes an unsafe integer on the next JSON crossing.
            canonical_json(fingerprint.as_bytes())?;
            let mut normalized_event: Value = serde_json::from_str(&fingerprint).map_err(Error::codec)?;
            *normalized_event
                .get_mut("sequence")
                .ok_or_else(|| Error::codec("missing sequence"))? = json!(event.sequence);
            let receipt = json!({"fingerprint":fingerprint,"partition":lease.partition});
            let by_event = records.get(&event_key);
            let by_sequence = records.get(&sequence_key);
            let duplicate = by_event.is_some() || by_sequence.is_some();
            if duplicate && (by_event != Some(&receipt) || by_sequence != Some(&receipt)) {
                return Err(Error::codec(
                    "conflicting fold identity or incomplete deduplication indexes",
                ));
            }
            let state: Option<State> = records
                .get(&state_key)
                .map(|v| serde_json::from_value(v.clone()).map_err(Error::codec))
                .transpose()?;
            if state.as_ref().is_some_and(|s| s.partition != lease.partition) {
                return Err(Error::codec("aggregate moved to another source partition"));
            }
            if !duplicate && state.as_ref().map_or(Some(1), |s| s.sequence.checked_add(1)) != Some(event.sequence) {
                return Err(Error::codec("fold aggregate sequence gap"));
            }
            if duplicate && state.as_ref().is_none_or(|s| s.sequence < event.sequence) {
                return Err(Error::bad_handle("fold state and deduplication restore disagree"));
            }
            plan.result = json!({"novel":!duplicate,"state":state.as_ref().map(|s| &s.value),"event":normalized_event,"lease":checkpoint});
            if let Operation::Apply { reduced, .. } = &request.operation {
                if !duplicate {
                    let bytes = canonical_json(&serde_json::to_vec(reduced).map_err(Error::codec)?)?;
                    canonical_json(&bytes)?;
                    let reduced: Reduced = serde_json::from_slice(&bytes).map_err(Error::codec)?;
                    plan.writes.insert(
                        state_key,
                        json!(State {
                            sequence: event.sequence,
                            partition: lease.partition,
                            value: reduced.state.clone()
                        }),
                    );
                    plan.writes.insert(event_key, receipt.clone());
                    plan.writes.insert(sequence_key, receipt);
                    for intent in &reduced.intents {
                        let id = intent_id(c, event, intent)?;
                        let key = c.intent_key(&id)?;
                        if plan.writes.contains_key(&key) {
                            return Err(Error::codec("duplicate fold intent key"));
                        }
                        plan.writes.insert(key, json!({"kind":"intent", "id":id,"destination":intent.destination,"payload":intent.payload,"partition":lease.partition,"offset":offset,"published":false,"suppressed":manifest.active && manifest.cutoffs.get(&lease.partition).is_some_and(|cutoff| offset < cutoff)}));
                    }
                }
                checkpoint.next_offset = offset
                    .checked_add(1)
                    .ok_or_else(|| Error::bad_handle("fold offset exhausted"))?;
                plan.result = json!(checkpoint);
                plan.writes
                    .insert(c.checkpoint_key(lease.partition)?, json!(checkpoint));
            }
        }
        Operation::Freeze => {
            if !manifest.active {
                return Err(Error::bad_handle(
                    "cannot hand over effects from an inactive fold generation",
                ));
            }
            if !manifest.frozen {
                for p in c.starts.keys() {
                    let key = c.checkpoint_key(*p)?;
                    let mut checkpoint: Lease = read(records, &key)?;
                    manifest.upper.insert(*p, checkpoint.next_offset);
                    checkpoint.owner.clear();
                    checkpoint.expires_ms = 0;
                    plan.writes.insert(key, json!(checkpoint));
                }
                manifest.frozen = true;
                plan.writes.insert(manifest_key, json!(manifest));
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
                return Err(Error::bad_handle("fold cutover belongs to another source history"));
            }
            let cutoffs = &cutover.cutoffs;
            if manifest.frozen
                || cutoffs.keys().ne(c.starts.keys())
                || cutoffs
                    .iter()
                    .any(|(p, o)| c.starts.get(p).is_none_or(|start| o < start))
            {
                return Err(Error::bad_handle("invalid fold activation vector"));
            }
            if manifest.active && manifest.cutoffs != *cutoffs {
                return Err(Error::bad_handle("fold activation is immutable"));
            }
            for (p, cutoff) in cutoffs {
                let checkpoint: Lease = read(records, &c.checkpoint_key(*p)?)?;
                if checkpoint.next_offset < *cutoff {
                    return Err(Error::bad_handle("fold rebuild has not reached cutoff"));
                }
            }
            plan.suppression = Some(cutoffs.clone());
            manifest.active = true;
            manifest.cutoffs.clone_from(cutoffs);
            plan.writes.insert(manifest_key, json!(manifest));
        }
        Operation::Get { tenant, aggregate } => {
            plan.result = records
                .get(&c.state_key(tenant, aggregate)?)
                .cloned()
                .unwrap_or(Value::Null);
        }
        Operation::Published { intent_id } => {
            let key = c.intent_key(intent_id)?;
            let mut intent: Value = read(records, &key)?;
            if !eligible(&json!(manifest), &intent)? {
                return Err(Error::bad_handle("fold intent is ineligible"));
            }
            *intent
                .get_mut("published")
                .ok_or_else(|| Error::codec("missing publication marker"))? = json!(true);
            plan.writes.insert(key, intent);
        }
    }
    Ok(plan)
}

/// Dispatch gating also applies to restored publication markers and later replay inserts.
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
