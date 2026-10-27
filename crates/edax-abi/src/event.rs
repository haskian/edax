//! The `edax-event` surface: routing, sequencing, upcast, lineage, the event guard's key, and the
//! message records with their codec.
//!
//! The kernel's messages are referred to fully-qualified (`edax_event::EventMessage`); the names
//! `Payload`, `EventDraft`, `EventMessage`, `CommandMessage` and `Lineage` below are this crate's own
//! binding-facing records, shaped to mirror them.

use std::collections::HashMap;

use crate::{names, ordered, tenancy_mode, to_wire, unordered};

/// Both conversions between a binding record and the kernel type it mirrors, field for field: each
/// `field` moves as it is, and each `map` changes type, to the kernel's key-ordered map and back. Both
/// struct literals stay exhaustive, so a field either side lacks is a compile error.
macro_rules! mirror {
    ($record:ident <-> $kernel:path { $($field:ident),* $(; maps: $($map:ident),*)? }) => {
        impl From<$record> for $kernel {
            fn from(m: $record) -> Self {
                Self { $($field: m.$field,)* $($($map: ordered(m.$map),)*)? }
            }
        }

        impl From<$kernel> for $record {
            fn from(m: $kernel) -> Self {
                Self { $($field: m.$field,)* $($($map: unordered(m.$map),)*)? }
            }
        }
    };
}

/// Protected records validate the cipher name while crossing into the typed kernel.
macro_rules! mirror_protected {
    ($record:ident <-> $kernel:path { $($field:ident),* }) => {
        impl TryFrom<$record> for $kernel {
            type Error = edax_core::Error;

            fn try_from(message: $record) -> Result<Self, Self::Error> {
                Ok(Self {
                    $($field: message.$field,)*
                    subject: message.subject,
                    encryption: message.encryption.map(edax_seal::EncryptionInfo::try_from).transpose()?,
                    extensions: ordered(message.extensions),
                    metadata: ordered(message.metadata),
                })
            }
        }

        impl From<$kernel> for $record {
            fn from(message: $kernel) -> Self {
                Self {
                    $($field: message.$field,)*
                    subject: message.subject,
                    encryption: message.encryption.map(crate::EncryptionInfo::from),
                    extensions: unordered(message.extensions),
                    metadata: unordered(message.metadata),
                }
            }
        }
    };
}

/// The local worker, of `n`, that runs `aggregate_id`'s events: local fan-out only, never broker
/// placement (that is [`kafka_partition_for`](crate::kafka_partition_for)). See [`edax_event::partition()`].
pub fn partition(aggregate_id: &str, n: u32) -> u32 {
    edax_event::partition(aggregate_id, n)
}

/// Whether an event with sequence `incoming` is older than the watermark `stored`, so an ordered
/// consumer skips it. Ordering, not dedup: an equal sequence is not stale. See
/// [`edax_event::is_stale`] and `docs/kernels/event.md`.
///
/// Each sequence crosses as a presence bit and a value, read only when present. The C interface
/// encodes an absent `Option<u64>` as `u64::MAX`, which is a valid sequence: `staleness.json` pins it.
pub fn is_stale(stored_present: bool, stored: u64, incoming_present: bool, incoming: u64) -> bool {
    edax_event::is_stale(stored_present.then_some(stored), incoming_present.then_some(incoming))
}

/// The watermark after an event with sequence `incoming` applied over `stored`: the higher of the
/// two, so an unsequenced event never erases it. See [`edax_event::advance`]. The sequences cross as
/// in [`is_stale`].
pub fn advance(stored_present: bool, stored: u64, incoming_present: bool, incoming: u64) -> Option<u64> {
    edax_event::advance(stored_present.then_some(stored), incoming_present.then_some(incoming))
}

/// Check an append batch against the dense-sequence rule: every event carries a sequence and the
/// batch runs `expected + 1, expected + 2, ...` in order. Returns the stream version after the batch.
/// `Err` names the 0-based batch index and is a `codec:` (terminal) error. An empty batch passes: it is
/// a version check only.
pub fn check_sequence_batch(expected: u64, sequences: Vec<Option<u64>>) -> Result<u64, String> {
    edax_event::sequence::check_batch(expected, &sequences).map_err(to_wire)
}

/// Every command fence's name (`"event"`, `"lock"`), in the kernel's order. A host checks its own list of
/// fences against this one. See [`edax_event::CommandFence`].
pub fn command_fences() -> Vec<String> {
    crate::names(edax_event::CommandFence::NAMES)
}

/// What a save does beside its append. See [`edax_event::SavePlan`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SavePlan {
    pub lock_stream: bool,
    pub after_event: Option<String>,
}

/// The plan for a save that appends the events `event_ids`, in order, and enqueues `commands` commands,
/// under the command fence named `fence`. A fence no variant names, or a save the fence does not fence, is
/// a `bad_handle:` error. See [`edax_event::plan_save`].
pub fn plan_save(fence: &str, event_ids: Vec<String>, commands: u64) -> Result<SavePlan, String> {
    let fence = edax_event::CommandFence::from_name(fence).map_err(to_wire)?;
    let plan = edax_event::plan_save(fence, &event_ids, crate::host_count(commands)).map_err(to_wire)?;
    Ok(SavePlan {
        lock_stream: plan.lock_stream,
        after_event: plan.after_event,
    })
}

/// The batch a save appends: its drafts, the aggregate's version after it, and whether the save writes a
/// snapshot at that version. See [`edax_event::DraftBatch`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DraftBatch {
    #[serde(default)]
    pub drafts: Vec<EventDraft>,
    pub version: u64,
    pub snapshot: bool,
}

/// The drafts a save of the aggregate `aggregate_id` at version `initial` appends for `payloads`, each
/// named by the id in `ids` at its index, sequenced `initial + 1, ...` and stamped with the batch's
/// `lineage`, `extensions`, `metadata`, `tenant` and `occurred_at`. `snapshot_every` is the snapshot
/// interval. A count of `ids` that is not the count of `payloads` is a `bad_handle:` error; a bad
/// extension key or tenant, or a batch past sequence `u64::MAX`, a `codec:` error. See
/// [`edax_event::draft_batch`].
// `tenant` and `occurred_at` are optional, so they trail: alef moves optional parameters to the end.
#[allow(clippy::too_many_arguments)] // the kernel's parameters with its `SaveContext` spread flat
pub fn draft_batch(
    aggregate_id: &str,
    initial: u64,
    snapshot_every: u64,
    payloads: Vec<Payload>,
    ids: Vec<String>,
    lineage: Lineage,
    extensions: HashMap<String, String>,
    metadata: HashMap<String, String>,
    tenant: Option<String>,
    occurred_at: Option<i64>,
    subject: Option<String>,
) -> Result<DraftBatch, String> {
    let lineage = edax_event::Lineage::from(lineage);
    let (extensions, metadata) = (ordered(extensions), ordered(metadata));
    let context = edax_event::SaveContext {
        tenant: tenant.as_deref(),
        lineage: &lineage,
        extensions: &extensions,
        metadata: &metadata,
        occurred_at,
        subject: subject.as_deref(),
    };
    let payloads = payloads.into_iter().map(edax_event::Payload::from).collect();
    let batch =
        edax_event::draft_batch(aggregate_id, initial, snapshot_every, payloads, ids, &context).map_err(to_wire)?;
    Ok(DraftBatch {
        drafts: batch.drafts.into_iter().map(EventDraft::from).collect(),
        version: batch.version,
        snapshot: batch.snapshot,
    })
}

/// Whether a save from version `initial` to `current` writes a snapshot under an interval of `every`
/// versions (`0`: never). See [`edax_event::snapshot_due`].
pub fn snapshot_due(every: u64, initial: u64, current: u64) -> bool {
    edax_event::snapshot_due(every, initial, current)
}

/// Ordered upcast steps from `from_version` to `to` given the available step versions, or `None` for
/// an event name with no transforms (every step is the identity, so the plan is empty).
///
/// A gap in the steps, or a downward plan (a payload from a newer writer), is the kernel's `codec`
/// error: the event cannot be unmarshaled to the target version, and no extra attempt changes that.
/// A codec asks on every unmarshal. See [`edax_event::upcast_plan`].
pub fn upcast_plan(from_version: u32, to: u32, available: Option<Vec<u32>>) -> Result<Vec<u32>, String> {
    edax_event::upcast_plan(from_version, to, available.as_deref()).map_err(to_wire)
}

/// The schema version a codec writes for one event name: one past the highest of its `available`
/// steps, or the first version (1) for `None` or none. A hole in the steps, or a step at the `u32`
/// ceiling, is the kernel's `codec` error. A codec asks once per name when it is built. See
/// [`edax_event::upcast_target`].
pub fn upcast_target(available: Option<Vec<u32>>) -> Result<u32, String> {
    edax_event::upcast_target(available.as_deref()).map_err(to_wire)
}

/// Preserve absent versus empty steps across generators that JSON-encode optional vectors.
pub fn upcast_plan_present(from_version: u32, to: u32, present: bool, available: Vec<u32>) -> Result<Vec<u32>, String> {
    upcast_plan(from_version, to, present.then_some(available))
}

/// The explicit presence bit keeps an absent upcaster distinct from an empty one.
pub fn upcast_target_present(present: bool, available: Vec<u32>) -> Result<u32, String> {
    upcast_target(present.then_some(available))
}

/// The two ids that place a message in its flow. See [`edax_event::Lineage`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Lineage {
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
}

mirror!(Lineage <-> edax_event::Lineage { correlation_id, causation_id });

/// The lineage of a child of the message `parent_id`: its causation is the parent's id, and it
/// inherits `parent_correlation_id`. See [`edax_event::propagate`].
pub fn propagate(parent_id: &str, parent_correlation_id: &str) -> Lineage {
    Lineage::from(edax_event::propagate(parent_id, parent_correlation_id))
}

/// The lineage of a message with no parent message, given the ids the caller already holds and one
/// freshly minted id. Fills only what is missing: `correlation_id` defaults to `fresh_id`, and
/// `causation_id` to the resulting correlation id. An empty string is a supplied id.
///
/// The host mints `fresh_id` (with [`guid`](crate::guid)) because minting is non-deterministic. The kernel keeps
/// the defaulting rule, so a golden vector pins it. See [`edax_event::originate`].
// The two ids are optional, so they trail: alef moves optional parameters to the end of the signature.
pub fn originate(fresh_id: &str, correlation_id: Option<String>, causation_id: Option<String>) -> Lineage {
    Lineage::from(edax_event::originate(
        correlation_id.as_deref(),
        causation_id.as_deref(),
        fresh_id,
    ))
}

/// The ordering key of a message: `aggregate_id`, or the message's own `id` when it targets no
/// aggregate. An event always has an aggregate, so its key is its aggregate. See
/// [`edax_event::ordering_key`].
// `aggregate_id` is optional, so it trails `id`: alef moves optional parameters to the end.
pub fn ordering_key(id: &str, aggregate_id: Option<String>) -> String {
    edax_event::ordering_key(aggregate_id.as_deref(), id).to_owned()
}

/// Refuse an extension key that breaks the rule, as a `codec:` error naming it. A key is 1 to 64
/// bytes of `[a-z0-9._-]` and starts with `[a-z0-9]`. See [`edax_event::check_extensions`].
pub fn check_extensions(extensions: HashMap<String, String>) -> Result<(), String> {
    edax_event::check_extensions(&ordered(extensions)).map_err(to_wire)
}

/// What an event's idempotency guard claims: the key and the payload hash. See
/// [`edax_event::event_idempotency`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct EventIdempotency {
    /// The event id under the guard's scope, composed with the event's tenant under a pool.
    pub key: String,
    /// FNV-1a over the raw `data` bytes.
    pub payload_hash: u64,
}

/// The key and hash the event idempotency guard named `scope` claims for `message`, in a deployment
/// of tenancy `mode`: the kernel's rule, pinned by `testdata/golden/event_idempotency.json`, so two
/// SDKs consuming one stream bind alike. A mode that requires a tenant and a message that names none
/// is a `codec:` error.
pub fn event_idempotency(message: EventMessage, mode: &str, scope: &str) -> Result<EventIdempotency, String> {
    let mode = tenancy_mode(mode)?;
    let rule = edax_event::event_idempotency(
        &edax_event::EventMessage::try_from(message).map_err(to_wire)?,
        mode,
        scope,
    )
    .map_err(to_wire)?;
    Ok(EventIdempotency {
        key: rule.key,
        payload_hash: rule.payload_hash,
    })
}

/// The ledger key the event idempotency guard named `scope` claims for the event `event_id`, in a
/// deployment of tenancy `mode`: the `key` [`event_idempotency`] returns, from the id alone, so an
/// operator can forget a dead-lettered event's record. A mode that requires a tenant and a call that
/// names none is a `codec:` error. See [`edax_event::event_claim_key`].
// `tenant` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn event_claim_key(mode: &str, scope: &str, event_id: &str, tenant: Option<String>) -> Result<String, String> {
    let mode = tenancy_mode(mode)?;
    edax_event::event_claim_key(mode, tenant.as_deref(), scope, event_id).map_err(to_wire)
}

/// A marshaled payload: a name, a schema version, and the host-serialized `data`, an event's or a
/// command's. What a host codec produces from a domain value; a saga hands its commands over in this
/// shape. See [`edax_event::Payload`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Payload {
    pub name: String,
    pub version: u32,
    #[serde(default)]
    pub data: Vec<u8>,
}

/// An event as a repository hands it to the store: every field of [`EventMessage`] but
/// `recorded_at`, which the store assigns. See [`edax_event::EventDraft`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct EventDraft {
    /// Globally unique event id.
    pub id: String,
    /// The aggregate this event belongs to.
    pub aggregate_id: String,
    /// The tenant this message belongs to, `None` outside a multi-tenant deployment. Every key a seam
    /// derives from this message is composed from it; see `tenant_key`.
    pub tenant: Option<String>,
    /// Immutable erasure owner for a protected aggregate.
    #[serde(default)]
    pub subject: Option<String>,
    /// Typed encryption framing; absent for historical plaintext records.
    #[serde(default)]
    pub encryption: Option<crate::EncryptionInfo>,
    /// The per-aggregate ordinal, `None` for an event outside an aggregate stream.
    pub sequence: Option<u64>,
    /// The event type name.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    #[serde(default)]
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one.
    pub causation_id: String,
    /// When the fact happened, if the host knows it apart from when it was recorded.
    pub occurred_at: Option<i64>,
    /// Extension context, under the key rule of [`check_extensions`].
    #[serde(default)]
    pub extensions: HashMap<String, String>,
    /// Application context, opaque to edax.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// An event as stored, published and consumed. Python receives a native record. Go uses the
/// generated C interface and serde conversion. The SDK exposes byte slices and maps in both. See
/// [`edax_event::EventMessage`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct EventMessage {
    /// Globally unique event id: the causation id of anything this event causes.
    pub id: String,
    /// The aggregate this event belongs to: its ordering key.
    pub aggregate_id: String,
    /// The tenant this message belongs to, `None` outside a multi-tenant deployment. Every key a seam
    /// derives from this message is composed from it; see `tenant_key`.
    pub tenant: Option<String>,
    /// Immutable erasure owner for a protected aggregate.
    #[serde(default)]
    pub subject: Option<String>,
    /// Typed encryption framing; absent for historical plaintext records.
    #[serde(default)]
    pub encryption: Option<crate::EncryptionInfo>,
    /// The per-aggregate ordinal, `None` for an event outside an aggregate stream.
    pub sequence: Option<u64>,
    /// The event type name a consumer routes and unmarshals by.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    #[serde(default)]
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
    #[serde(default)]
    pub extensions: HashMap<String, String>,
    /// Application context, opaque to edax.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// A command as dispatched, queued and consumed. See [`edax_event::CommandMessage`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    /// Erasure owner of the protected command payload.
    #[serde(default)]
    pub subject: Option<String>,
    /// Typed encryption framing; absent for historical plaintext records.
    #[serde(default)]
    pub encryption: Option<crate::EncryptionInfo>,
    /// The command type name a handler is registered under.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    #[serde(default)]
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
    /// Unix nanoseconds on the issuer's clock when the command was built.
    pub issued_at: i64,
    /// Extension context, under the key rule of [`check_extensions`].
    #[serde(default)]
    pub extensions: HashMap<String, String>,
    /// Application context, opaque to edax.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl From<Payload> for edax_event::Payload {
    fn from(p: Payload) -> Self {
        Self {
            name: p.name,
            version: p.version,
            data: p.data,
        }
    }
}

mirror_protected!(EventDraft <-> edax_event::EventDraft {
    id, aggregate_id, tenant, sequence, name, version, data, correlation_id, causation_id, occurred_at
});

mirror_protected!(EventMessage <-> edax_event::EventMessage {
    id, aggregate_id, tenant, sequence, name, version, data, correlation_id, causation_id, occurred_at, recorded_at
});

mirror_protected!(CommandMessage <-> edax_event::CommandMessage {
    id, idempotency_key, aggregate_id, tenant, name, version, data, correlation_id, causation_id, issued_at
});

/// The stored message for `draft`: every field as drafted, stamped `recorded_at` on the store's clock.
/// See [`edax_event::EventDraft::record`].
pub fn record_event(draft: EventDraft, recorded_at: i64) -> Result<EventMessage, String> {
    Ok(EventMessage::from(
        edax_event::EventDraft::try_from(draft)
            .map_err(to_wire)?
            .record(recorded_at),
    ))
}

fn format_of(format: &str) -> Result<edax_event::Format, String> {
    edax_event::Format::from_name(format).map_err(to_wire)
}

/// An event -> wire bytes in `format` (`"json"` or `"msgpack"`). A bad extension key is a `codec:`
/// error, so a producer never mints a message every consumer would refuse.
pub fn encode_event(format: &str, message: EventMessage) -> Result<Vec<u8>, String> {
    format_of(format)?
        .encode(&edax_event::EventMessage::try_from(message).map_err(to_wire)?)
        .map_err(to_wire)
}

/// Wire bytes in `format` -> an event. Bytes that are not an event in that format are a `codec:`
/// error.
pub fn decode_event(format: &str, wire: &[u8]) -> Result<EventMessage, String> {
    let message = format_of(format)?
        .decode::<edax_event::EventMessage>(wire)
        .map_err(to_wire)?;
    Ok(EventMessage::from(message))
}

/// A command -> wire bytes in `format`, as [`encode_event`] does an event.
pub fn encode_command(format: &str, message: CommandMessage) -> Result<Vec<u8>, String> {
    format_of(format)?
        .encode(&edax_event::CommandMessage::try_from(message).map_err(to_wire)?)
        .map_err(to_wire)
}

/// Wire bytes in `format` -> a command, as [`decode_event`] does an event.
pub fn decode_command(format: &str, wire: &[u8]) -> Result<CommandMessage, String> {
    let message = format_of(format)?
        .decode::<edax_event::CommandMessage>(wire)
        .map_err(to_wire)?;
    Ok(CommandMessage::from(message))
}

/// The media type that names `format` (`"json"` or `"msgpack"`) and `kind` (`"event"` or
/// `"command"`) on the wire, `application/vnd.edax.<kind>+<format>`: the string a transport that
/// carries a type puts there, such as a Kafka `content-type` header. It names the codec and not the
/// transport, so it lives here and not with the Kafka record. An unknown format or kind is a `codec:`
/// error. See [`edax_event::Format::media_type`].
pub fn media_type(format: &str, kind: &str) -> Result<String, String> {
    let kind = edax_event::Kind::from_name(kind).map_err(to_wire)?;
    Ok(format_of(format)?.media_type(kind).to_owned())
}

/// Every message kind's name (`"event"`, `"command"`), in the kernel's order. A host checks its own list
/// of kinds against this one. See [`edax_event::Kind`].
pub fn message_kinds() -> Vec<String> {
    names(edax_event::Kind::NAMES)
}

/// Every wire format's name (`"json"`, `"msgpack"`), in the kernel's order. A host checks its own list of
/// formats against this one. See [`edax_event::Format`].
pub fn wire_formats() -> Vec<String> {
    names(edax_event::Format::NAMES)
}

/// `command` as a save of an aggregate of `tenant` sends it: stamped with `tenant` when it names none,
/// as it is when it names the same one, and a `codec:` error when it names another. See
/// [`edax_event::own_command`].
// `tenant` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn own_command(command: CommandMessage, tenant: Option<String>) -> Result<CommandMessage, String> {
    edax_event::own_command(
        edax_event::CommandMessage::try_from(command).map_err(to_wire)?,
        tenant.as_deref(),
    )
    .map(CommandMessage::from)
    .map_err(to_wire)
}

/// Every microbatch recovery action, in the kernel's order.
pub fn batch_action_names() -> Vec<String> {
    names(edax_event::batch::Action::NAMES)
}

/// Validate and resolve the microbatch limits, including omitted defaults.
pub fn batch_config(config: &str) -> Result<String, String> {
    let config: edax_event::batch::Config = crate::read_json(config)?;
    edax_event::batch::check(&config).map_err(crate::to_wire)?;
    crate::write_json(&config)
}

/// Serialization headroom for fetching and preparing one original.
pub fn batch_reserve(config: &str) -> Result<u64, String> {
    edax_event::batch::reserve(&crate::read_json(config)?)
        .map(crate::count)
        .map_err(crate::to_wire)
}

/// Retained bytes for one prepared input. See [`edax_event::batch::charge`].
pub fn batch_charge(serialized_bytes: u64, config: &str) -> Result<u64, String> {
    let config = crate::read_json(config)?;
    edax_event::batch::charge(crate::host_count(serialized_bytes), &config)
        .map(crate::count)
        .map_err(to_wire)
}

/// Whether a batch has reached its record, byte, or age flush threshold.
pub fn batch_cut(records: u64, bytes: u64, age_ms: u64, config: &str) -> Result<bool, String> {
    let config = crate::read_json(config)?;
    Ok(edax_event::batch::cut(
        crate::host_count(records),
        crate::host_count(bytes),
        age_ms,
        &config,
    ))
}

/// Whether the requested retained data fits all admission caps.
pub fn batch_admit(used_records: u64, used_bytes: u64, records: u64, bytes: u64, config: &str) -> Result<bool, String> {
    let config = crate::read_json(config)?;
    Ok(edax_event::batch::admit(
        crate::host_count(used_records),
        crate::host_count(used_bytes),
        crate::host_count(records),
        crate::host_count(bytes),
        &config,
    ))
}

/// The recovery after a failed microbatch attempt with error `code`. See [`edax_event::batch::recover`].
pub fn batch_recover(
    attempt: u32,
    waits: u32,
    count: u64,
    max_attempts: u32,
    rolled_back: bool,
    isolable: bool,
    code: Option<String>,
) -> Result<String, String> {
    let attempt = edax_event::batch::Attempt {
        attempt,
        waits,
        count: crate::host_count(count),
        max_attempts,
        rolled_back,
        isolable,
    };
    crate::write_json(&edax_event::batch::recover(code.as_deref(), attempt).map_err(to_wire)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;

    #[test]
    fn routing_and_sequencing_forward_their_arguments_in_order() {
        // A transposed argument is the one way a forwarding surface can drift from its kernel, so
        // every case is asymmetric: swapping the two arguments changes the answer.
        assert!(partition("invoice-7", 8) < 8);
        assert_eq!(partition("invoice-7", 0), 0, "n is clamped to at least one partition");

        // (stored, incoming), each as its presence bit and value: each swap changes the answer.
        let stale = |stored: Option<u64>, incoming: Option<u64>| {
            is_stale(
                stored.is_some(),
                stored.unwrap_or(0),
                incoming.is_some(),
                incoming.unwrap_or(0),
            )
        };
        let advanced = |stored: Option<u64>, incoming: Option<u64>| {
            advance(
                stored.is_some(),
                stored.unwrap_or(0),
                incoming.is_some(),
                incoming.unwrap_or(0),
            )
        };
        assert!(!stale(Some(4), Some(5)), "newer than stored -> apply");
        assert!(stale(Some(5), Some(4)), "older than stored -> skip");
        assert!(!stale(None, Some(4)), "nothing stored -> apply");
        assert!(!stale(Some(5), None), "unsequenced -> apply");
        assert!(
            stale(Some(u64::MAX), Some(u64::MAX - 1)),
            "the ceiling is a sequence, not absence"
        );
        assert_eq!(advanced(Some(4), Some(5)), Some(5));
        assert_eq!(
            advanced(Some(5), Some(4)),
            Some(5),
            "a stale sequence never lowers the mark"
        );
        assert_eq!(
            advanced(Some(5), None),
            Some(5),
            "an unsequenced event never erases the mark"
        );
        assert_eq!(advanced(None, Some(4)), Some(4));
        assert_eq!(advanced(Some(u64::MAX), None), Some(u64::MAX));
        assert!(!is_stale(false, 9, true, 4), "an absent value is never read");
    }

    #[test]
    fn lineage_crosses_in_argument_order() {
        // (parent_id, parent_correlation_id): a swap would point causation at the flow.
        let child = propagate("evt-1", "corr-1");
        assert_eq!(
            (child.correlation_id.as_str(), child.causation_id.as_str()),
            ("corr-1", "evt-1")
        );

        // (fresh_id, correlation_id, causation_id): with both ids given, a swap of the two shows. The
        // kernel's golden pins every branch.
        let root = originate("fresh-1", Some("corr-1".to_owned()), Some("evt-0".to_owned()));
        assert_eq!(
            (root.correlation_id.as_str(), root.causation_id.as_str()),
            ("corr-1", "evt-0")
        );
    }

    #[test]
    fn extension_keys_follow_the_kernels_rule() {
        let check = |key: &str| check_extensions(HashMap::from([(key.to_owned(), "v".to_owned())]));
        assert_eq!(check("http.origin_ip"), Ok(()));
        assert_eq!(check_extensions(HashMap::new()), Ok(()));
        let refused = check("Traceparent").unwrap_err();
        assert!(refused.starts_with("codec: extension key \"Traceparent\""), "{refused}");
    }

    #[test]
    fn upcast_plan_branches() {
        assert_eq!(
            upcast_plan(1, 3, Some(vec![1, 2, 9])).unwrap(),
            vec![1, 2],
            "extra steps are ignored"
        );
        assert_eq!(
            upcast_plan(3, 3, Some(vec![])).unwrap(),
            Vec::<u32>::new(),
            "already current"
        );
        assert_eq!(
            upcast_plan(1, 3, None).unwrap(),
            Vec::<u32>::new(),
            "no transforms: identity"
        );
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
    fn explicit_upcast_presence_preserves_absent_and_empty() {
        for available in [None, Some(vec![]), Some(vec![1, 2]), Some(vec![1, 3])] {
            let present = available.is_some();
            let steps = available.clone().unwrap_or_default();
            assert_eq!(
                upcast_plan_present(1, 3, present, steps.clone()),
                upcast_plan(1, 3, available.clone())
            );
            assert_eq!(upcast_target_present(present, steps), upcast_target(available));
        }
    }

    #[test]
    fn own_command_crosses_the_tenant() {
        let owned = own_command(command_message(Some("acct-1")), Some("acme".to_owned())).unwrap();
        assert_eq!(owned.tenant.as_deref(), Some("acme"));
        let foreign = own_command(
            CommandMessage {
                tenant: Some("globex".to_owned()),
                ..command_message(Some("acct-1"))
            },
            Some("acme".to_owned()),
        );
        assert!(foreign
            .err()
            .is_some_and(|e| e.starts_with("codec: command cmd-1 names tenant")));
    }

    #[test]
    fn messages_round_trip_through_every_format() {
        // The tenant crosses every record conversion: the draft's, the event's both ways, the command's
        // both ways. `event` takes the draft path, and each case checks the tenant against the literal,
        // so a conversion that drops it fails here.
        let event = |sequence: Option<u64>, tenant: Option<&str>| {
            record_event(
                EventDraft {
                    tenant: tenant.map(str::to_owned),
                    ..draft("evt-1", sequence)
                },
                RECORDED_AT,
            )
            .unwrap()
        };
        let command = |aggregate_id: Option<&str>, tenant: Option<&str>| CommandMessage {
            tenant: tenant.map(str::to_owned),
            ..command_message(aggregate_id)
        };
        for format in ["json", "msgpack"] {
            for (sequence, tenant) in [(Some(7), None), (None, None), (Some(7), Some("acme"))] {
                let wire = encode_event(format, event(sequence, tenant)).unwrap();
                let back = decode_event(format, &wire).unwrap();
                let case = format!("{format} sequence={sequence:?} tenant={tenant:?}");
                assert_eq!(back.tenant.as_deref(), tenant, "{case}");
                assert_same_event(back, event(sequence, tenant), &case);
            }
            for (aggregate_id, tenant) in [
                (Some("invoice-7"), None),
                (None, None),
                (Some("invoice-7"), Some("acme")),
            ] {
                let wire = encode_command(format, command(aggregate_id, tenant)).unwrap();
                let back = decode_command(format, &wire).unwrap();
                let case = format!("{format} aggregate={aggregate_id:?} tenant={tenant:?}");
                assert_same_command(back, command(aggregate_id, tenant), &case);
            }
            // (format, kind) in that order.
            for kind in ["event", "command"] {
                let minted = media_type(format, kind).unwrap();
                assert_eq!(minted, format!("application/vnd.edax.{kind}+{format}"));
            }
        }
    }
}

fn erasure_record(record: &str) -> Result<edax_event::erasure::ErasureOperation, String> {
    edax_event::erasure::decode_operation(record).map_err(to_wire)
}

/// Construct the operation that must be persisted before revocation.
pub fn erasure_create(
    scope: crate::KeyScope,
    operation_id: &str,
    request_id: &str,
    participants: Vec<String>,
) -> Result<String, String> {
    let op = edax_event::erasure::ErasureOperation::new(
        operation_id.to_owned(),
        request_id.to_owned(),
        scope.try_into().map_err(to_wire)?,
        participants.into_iter().collect(),
    )
    .map_err(to_wire)?;
    edax_event::erasure::encode_operation(&op).map_err(to_wire)
}
/// Validate and normalize an operation restored by a store.
pub fn erasure_validate(record: &str) -> Result<String, String> {
    edax_event::erasure::encode_operation(&erasure_record(record)?).map_err(to_wire)
}
/// Confirm a workflow effect or one participant's cleanup.
pub fn erasure_apply(record: &str, action: &str, participant: Option<String>) -> Result<String, String> {
    let op = edax_event::erasure::apply(erasure_record(record)?, action, participant.as_deref()).map_err(to_wire)?;
    edax_event::erasure::encode_operation(&op).map_err(to_wire)
}
/// Whether every required cleanup participant acknowledged this notification.
pub fn erasure_can_destroy(record: &str) -> Result<bool, String> {
    Ok(erasure_record(record)?.can_destroy())
}
/// Whether the operation's scope is revoked: a cleanup sweep deletes for it.
pub fn erasure_scope_revoked(record: &str) -> Result<bool, String> {
    Ok(erasure_record(record)?.scope_revoked())
}
/// Validate request idempotency and its frozen participants.
pub fn erasure_check_request(existing: &str, candidate: &str) -> Result<(), String> {
    edax_event::erasure::check_request(&erasure_record(existing)?, &erasure_record(candidate)?).map_err(to_wire)
}
/// The stable-ID control notification generated by a durable transition.
pub fn erasure_notification(previous: &str, next: &str) -> Result<Option<String>, String> {
    edax_event::erasure::notification(&erasure_record(previous)?, &erasure_record(next)?).map_err(to_wire)
}
/// Interpret a provider's authoritative status after a requested lifecycle effect.
pub fn erasure_provider_confirmed(action: &str, status: &str) -> Result<bool, String> {
    edax_event::erasure::provider_confirmed(action, status).map_err(to_wire)
}

/// Every durable erasure stage.
pub fn erasure_stages() -> Vec<String> {
    names(edax_event::erasure::ErasureStage::NAMES)
}

/// Every erasure workflow action.
pub fn erasure_actions() -> Vec<String> {
    names(edax_event::erasure::ErasureAction::NAMES)
}

/// Validate coordinator participants before any revocation effect.
pub fn erasure_validate_participants(required: Vec<String>, registered: Vec<String>) -> Result<(), String> {
    edax_event::erasure::validate_participants(&required.into_iter().collect(), &registered.into_iter().collect())
        .map_err(to_wire)
}

/// Reconcile an uncertain commit without reusing a message ID for new ciphertext.
pub fn reconcile_prepared_events(drafts: Vec<EventDraft>, stored: Vec<EventMessage>) -> Result<bool, String> {
    let drafts = drafts
        .into_iter()
        .map(edax_event::EventDraft::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_wire)?;
    let stored = stored
        .into_iter()
        .map(edax_event::EventMessage::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_wire)?;
    edax_event::sealing::reconcile_prepared_events(&drafts, &stored).map_err(to_wire)
}

/// Typed protected application-state envelope, retained opaquely by storage adapters.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProtectedState {
    pub tenant: Option<String>,
    pub subject: Option<String>,
    pub completed: bool,
    pub encryption: crate::EncryptionInfo,
    pub data: Vec<u8>,
}
/// Presence envelope preserves optional native handle ownership in generated bindings.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProtectedStateResult {
    pub value: Option<ProtectedState>,
}
/// Encode the versioned protected state representation.
pub fn encode_protected_state(state: ProtectedState) -> Result<String, String> {
    edax_event::sealing::encode_protected_state(&edax_event::sealing::ProtectedState::new(
        state.tenant,
        state.subject,
        state.completed,
        state.encryption.try_into().map_err(to_wire)?,
        state.data,
    ))
    .map_err(to_wire)
}
/// Distinguish protected state from explicitly supported historical plaintext.
pub fn decode_protected_state(state_json: &str) -> Result<ProtectedStateResult, String> {
    let value = edax_event::sealing::decode_protected_state(state_json)
        .map_err(to_wire)?
        .map(|s| ProtectedState {
            tenant: s.tenant,
            subject: s.subject,
            completed: s.completed,
            encryption: s.encryption.into(),
            data: s.data,
        });
    Ok(ProtectedStateResult { value })
}
