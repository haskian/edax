//! Core domain types — the language-agnostic shapes every binding agrees on.
//!
//! `EventEnvelope` is (de)serialized to a selectable wire format via [`crate::codec`]. The byte
//! field `data` carries the host's already-serialized domain payload; the codec encodes it as
//! base64 in human-readable formats and as native bytes in binary formats.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Multi-tenancy / routing keys carried alongside an event. A `BTreeMap` fixes the key order, so the
/// encoded bytes are deterministic and the golden vectors hold.
pub type Namespace = BTreeMap<String, String>;

/// Free-form context propagated with an event (trace ids, tenant, user, ...).
// String values only: a host encodes a structured value itself (as JSON, say). Widening to bytes
// would cost a wire-format bump, so it waits for a real need.
pub type Metadata = BTreeMap<String, String>;

/// An event whose domain payload the host has already serialized to bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The event type name a consumer routes and unmarshals by.
    pub name: String,
    /// The payload schema version, the input to [`crate::upcast`] planning.
    pub version: i32,
    /// The host-serialized domain payload. The lib treats it as opaque bytes — base64 in
    /// human-readable wire formats, native bytes in binary ones.
    #[serde(with = "crate::codec::bytes_field")]
    pub data: Vec<u8>,
}

/// The full, persisted/transported event record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Globally unique event id, the causation id of anything this event produces.
    pub id: String,
    /// The aggregate this event belongs to: the ordering and partitioning key.
    pub aggregate_id: String,
    /// The event itself, payload included. A map on the wire, like the envelope.
    #[serde(deserialize_with = "crate::codec::map_only")]
    pub event: Event,
    /// Per-aggregate ordinal. `None` for events published without an aggregate sequence.
    pub sequence: Option<u64>,
    /// Tenancy and routing keys the store and the broker read.
    pub namespace: Namespace,
    /// Free-form context propagated with the event; opaque to the kernel.
    pub metadata: Metadata,
    /// Unix nanoseconds. Injectable so golden vectors are reproducible.
    pub timestamp: i64,
}
