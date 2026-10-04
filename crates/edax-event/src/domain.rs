//! Core domain types: the language-agnostic shapes every binding agrees on.
//!
//! Two messages cross a process boundary, [`EventMessage`] and [`CommandMessage`], each encoded by
//! [`crate::codec`]. Both are flat: identity, payload, lineage, time, then two string maps. The byte
//! field `data` carries the host's already-serialized payload; the codec writes it as base64 in
//! human-readable formats and as native bytes in binary ones.
//!
//! Three tiers of fields, so every key has one owner:
//!
//! - a **typed field** is one an edax kernel or SDK decision reads;
//! - [`Extensions`] belong to edax extensions (trace context, request origin), under the key rule in
//!   [`check_extensions`];
//! - [`Metadata`] belongs to the application and is opaque to edax.
//!
//! Broker facts (offset, partition, broker time) never enter a message: a transport reports them
//! beside the message it delivers.

use edax_core::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Application-owned context carried with a message. A `BTreeMap` fixes the key order, so the encoded
/// bytes are deterministic and the golden vectors hold.
// String values only: a host encodes a structured value itself (as JSON, say). Widening to bytes
// would cost a wire-format bump, so it waits for a real need.
pub type Metadata = BTreeMap<String, String>;

/// Context an edax extension writes and reads, such as `traceparent` or `http.origin_ip`. Keys follow
/// [`check_extensions`]; values are opaque strings.
pub type Extensions = BTreeMap<String, String>;

/// The kind of a message. Exhaustive on purpose: a host must decide about every kind, so a new one is
/// a compile error in every SDK rather than a wildcard's silent default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// An [`EventMessage`]: a fact that happened.
    Event,
    /// A [`CommandMessage`]: a request for something to happen.
    Command,
}

impl Kind {
    /// The name a binding selects this kind by: `"event"` or `"command"`.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Event => "event",
            Kind::Command => "command",
        }
    }

    /// The inverse of [`name`](Kind::name). Anything else is a `codec` error.
    pub fn from_name(name: &str) -> Result<Kind, Error> {
        match name {
            "event" => Ok(Kind::Event),
            "command" => Ok(Kind::Command),
            other => Err(Error::codec(format_args!("unknown message kind: {other}"))),
        }
    }
}

/// A marshaled payload: what a host codec produces from a domain value and reads back. Not a wire
/// type of its own; an aggregate holds its pending events in this shape until a save stamps them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The type name a consumer routes and unmarshals by.
    pub name: String,
    /// The payload schema version, the input to [`crate::upcast`] planning.
    pub version: u32,
    /// The host-serialized payload, opaque to the kernel.
    #[serde(with = "crate::codec::bytes_field")]
    pub data: Vec<u8>,
}

/// An event as a repository hands it to the store: every field but the one the store assigns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDraft {
    /// Globally unique event id.
    pub id: String,
    /// The aggregate this event belongs to.
    pub aggregate_id: String,
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
    /// Extension context.
    pub extensions: Extensions,
    /// Application context.
    pub metadata: Metadata,
}

impl EventDraft {
    /// The stored message: this draft as the store recorded it at `recorded_at`.
    pub fn record(self, recorded_at: i64) -> EventMessage {
        EventMessage {
            id: self.id,
            aggregate_id: self.aggregate_id,
            sequence: self.sequence,
            name: self.name,
            version: self.version,
            data: self.data,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            occurred_at: self.occurred_at,
            recorded_at,
            extensions: self.extensions,
            metadata: self.metadata,
        }
    }
}

/// An event as stored, published and consumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMessage {
    /// Globally unique event id: the causation id of anything this event causes.
    pub id: String,
    /// The aggregate this event belongs to: its ordering key.
    pub aggregate_id: String,
    /// The per-aggregate ordinal, `None` for an event outside an aggregate stream. A missing field
    /// reads as `None`.
    #[serde(default)]
    pub sequence: Option<u64>,
    /// The event type name a consumer routes and unmarshals by.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    #[serde(with = "crate::codec::bytes_field")]
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
    /// Valid time: when the fact happened, set by the host. `None` means it happened when recorded.
    /// A missing field reads as `None`.
    #[serde(default)]
    pub occurred_at: Option<i64>,
    /// Transaction time: unix nanoseconds on the store's clock when the store appended the event.
    pub recorded_at: i64,
    /// Extension context.
    pub extensions: Extensions,
    /// Application context.
    pub metadata: Metadata,
}

impl EventMessage {
    /// The payload part, moved out of the message, for a codec that unmarshals by name and version.
    pub fn into_payload(self) -> Event {
        Event { name: self.name, version: self.version, data: self.data }
    }
}

/// A command as dispatched, queued and consumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandMessage {
    /// This message instance: the causation id of what it causes, and the key a retry policy counts
    /// attempts by. A redelivery of the same message keeps it.
    pub id: String,
    /// The client's intent, the key an idempotency guard claims. `None`: no claim is made. A missing
    /// field reads as `None`.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// The aggregate the command targets, if any: its ordering key. A missing field reads as `None`.
    #[serde(default)]
    pub aggregate_id: Option<String>,
    /// The command type name a handler is registered under.
    pub name: String,
    /// The payload schema version.
    pub version: u32,
    /// The host-serialized payload.
    #[serde(with = "crate::codec::bytes_field")]
    pub data: Vec<u8>,
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
    /// Unix nanoseconds on the issuer's clock when the command was built.
    pub issued_at: i64,
    /// Extension context.
    pub extensions: Extensions,
    /// Application context.
    pub metadata: Metadata,
}

impl CommandMessage {
    /// The key a guard claims for this command once it is received. See [`received_key`].
    pub fn received_key(&self) -> &str {
        received_key(self.idempotency_key.as_deref(), &self.id)
    }

    /// The payload part, for a codec that unmarshals by name and version.
    pub fn payload(&self) -> Event {
        Event { name: self.name.clone(), version: self.version, data: self.data.clone() }
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::EventMessage {}
    impl Sealed for super::CommandMessage {}
}

/// What the codec and a transport read off either kind of message, so each writes one function for
/// both. Sealed: the kinds are [`Kind`]'s, and a host adds none.
pub trait Message: sealed::Sealed {
    /// The kind this message is.
    const KIND: Kind;

    /// The extension context, which the codec holds to [`check_extensions`].
    fn extensions(&self) -> &Extensions;

    /// The key that orders this message against others. See [`ordering_key`].
    fn ordering_key(&self) -> &str;
}

impl Message for EventMessage {
    const KIND: Kind = Kind::Event;

    fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// An event's aggregate: an event always has one.
    fn ordering_key(&self) -> &str {
        &self.aggregate_id
    }
}

impl Message for CommandMessage {
    const KIND: Kind = Kind::Command;

    fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    fn ordering_key(&self) -> &str {
        ordering_key(self.aggregate_id.as_deref(), &self.id)
    }
}

/// The ordering key of a message: its target aggregate, or its own id when it has none, so a message
/// without a target spreads by id instead of piling every such message onto one empty key. The Kafka
/// record key, the memory broker's partition and a key-ordered dispatcher's worker all start here.
/// Pinned by `testdata/golden/ordering_key.json`.
pub fn ordering_key<'a>(aggregate_id: Option<&'a str>, id: &'a str) -> &'a str {
    aggregate_id.unwrap_or(id)
}

/// The idempotency key a received command is claimed under: its own `idempotency_key`, or its `id`
/// when it carries none. A broker redelivers the same message, and both copies carry one id, so with
/// no client key the message's own id still deduplicates a redelivery. An empty key is still a key.
/// Pinned by `testdata/golden/command_received_key.json`.
pub fn received_key<'a>(idempotency_key: Option<&'a str>, id: &'a str) -> &'a str {
    idempotency_key.unwrap_or(id)
}

/// The longest [`Extensions`] key.
pub const EXTENSION_KEY_MAX: usize = 64;

/// Whether every key in `extensions` follows the rule: 1 to [`EXTENSION_KEY_MAX`] bytes of lowercase
/// ASCII letters, digits, `.`, `_` and `-`, starting with a letter or digit. A dot namespaces keys per
/// extension (`http.origin_ip`). Lowercase keeps a key safe as a header name on every transport.
///
/// A bad key is a `codec` error naming it. The codec checks on encode and decode, so a producer never
/// mints a message every consumer would refuse. Pinned by `testdata/golden/extensions.json`.
pub fn check_extensions(extensions: &Extensions) -> Result<(), Error> {
    for key in extensions.keys() {
        let bytes = key.as_bytes();
        let first_ok = bytes.first().is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        let rest_ok =
            bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'));
        if !first_ok || !rest_ok || bytes.len() > EXTENSION_KEY_MAX {
            return Err(Error::codec(format_args!(
                "extension key {key:?} must be 1 to {EXTENSION_KEY_MAX} of [a-z0-9._-], starting with [a-z0-9]"
            )));
        }
    }
    Ok(())
}
