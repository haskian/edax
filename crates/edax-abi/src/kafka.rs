//! The `edax-kafka` surface: the record, its route, the dead letter, placement, client checks and
//! the offset tracker.

use std::sync::Arc;

use crate::{error_code, tenancy_mode, to_wire, CommandMessage, EventMessage};

/// One Kafka record header. The value crosses as bytes and not text: a foreign header on the same
/// topic may carry anything, and the boundary hands it back unchanged rather than lose it. See
/// [`edax_kafka::KafkaHeader`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaHeader {
    /// The header name, as the client hands it over.
    pub key: String,
    /// The header value, uninterpreted.
    #[serde(default)]
    pub value: Vec<u8>,
}

/// One Kafka record, minus the client's own coordinates (topic, partition, offset), which stay with
/// the host's IO shell. See [`edax_kafka::KafkaRecord`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaRecord {
    /// The partitioning key: the message's ordering key, or the host's override. UTF-8 bytes.
    #[serde(default)]
    pub key: Vec<u8>,
    /// The encoded message.
    #[serde(default)]
    pub value: Vec<u8>,
    /// The contract headers, in the order the kernel writes them.
    #[serde(default)]
    pub headers: Vec<KafkaHeader>,
}

impl From<KafkaHeader> for edax_kafka::KafkaHeader {
    fn from(header: KafkaHeader) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }
}

impl From<edax_kafka::KafkaHeader> for KafkaHeader {
    fn from(header: edax_kafka::KafkaHeader) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }
}

impl From<KafkaRecord> for edax_kafka::KafkaRecord {
    fn from(record: KafkaRecord) -> Self {
        let headers = record.headers.into_iter().map(edax_kafka::KafkaHeader::from).collect();
        Self {
            key: record.key,
            value: record.value,
            headers,
        }
    }
}

impl From<edax_kafka::KafkaRecord> for KafkaRecord {
    fn from(record: edax_kafka::KafkaRecord) -> Self {
        let headers = record.headers.into_iter().map(KafkaHeader::from).collect();
        Self {
            key: record.key,
            value: record.value,
            headers,
        }
    }
}

/// Build the record for an event whose value bytes the host already encoded. `content_type` names the
/// format and kind of `value`. The key is `key`, the host's override, or else the kernel's: the
/// event's aggregate composed with its tenant where the `tenancy` mode (`"silo"`, `"pool"` or
/// `"bridge"`) composes keys. A mode that requires a tenant and an event that names none is a
/// `codec:` error. See [`edax_kafka::record()`].
// `key` is optional, so it trails: alef moves optional parameters to the end.
pub fn kafka_event_record(
    tenancy: &str,
    message: EventMessage,
    value: Vec<u8>,
    content_type: &str,
    key: Option<String>,
) -> Result<KafkaRecord, String> {
    let tenancy = tenancy_mode(tenancy)?;
    let message = edax_event::EventMessage::try_from(message).map_err(to_wire)?;
    edax_kafka::record(&message, tenancy, key.as_deref(), value, content_type)
        .map(KafkaRecord::from)
        .map_err(to_wire)
}

/// [`kafka_event_record`] for a command: its key is the target aggregate, or else its own id, under
/// the same tenancy rule. See [`edax_kafka::record()`].
pub fn kafka_command_record(
    tenancy: &str,
    message: CommandMessage,
    value: Vec<u8>,
    content_type: &str,
    key: Option<String>,
) -> Result<KafkaRecord, String> {
    let tenancy = tenancy_mode(tenancy)?;
    let message = edax_event::CommandMessage::try_from(message).map_err(to_wire)?;
    edax_kafka::record(&message, tenancy, key.as_deref(), value, content_type)
        .map(KafkaRecord::from)
        .map_err(to_wire)
}

/// Where a fetched record goes by its `content-type`: flattened from [`edax_kafka::Route`]. No field
/// set means the default serializer; `registered` names the index of the media type in the list the
/// host passed; `unknown` carries a readable media type nobody registered, the empty one included;
/// `headerless` is a record with no readable content type under a command default. The last two are
/// poison.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
pub fn kafka_route(
    headers: Vec<KafkaHeader>,
    registered: Vec<String>,
    default_kind: &str,
) -> Result<KafkaRoute, String> {
    let default = edax_event::Kind::from_name(default_kind).map_err(to_wire)?;
    let headers: Vec<edax_kafka::KafkaHeader> = headers.into_iter().map(edax_kafka::KafkaHeader::from).collect();
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    let none = KafkaRoute {
        registered: None,
        unknown: None,
        headerless: false,
    };
    Ok(match edax_kafka::route(&headers, &registered, default) {
        edax_kafka::Route::Default => none,
        edax_kafka::Route::Headerless => KafkaRoute {
            headerless: true,
            ..none
        },
        edax_kafka::Route::Registered(index) => {
            // An index into a list the host passed across the binding fits a `u32` by construction.
            KafkaRoute {
                registered: Some(u32::try_from(index).unwrap_or(u32::MAX)),
                ..none
            }
        }
        edax_kafka::Route::Unknown(media_type) => KafkaRoute {
            unknown: Some(media_type),
            ..none
        },
    })
}

/// Refuse a serializer registry that names one media type twice, as an `already_registered:` error.
/// A host calls it once, on the list it later passes to [`kafka_route`]. See
/// [`edax_kafka::check_media_types`].
pub fn kafka_check_media_types(registered: Vec<String>) -> Result<(), String> {
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    edax_kafka::check_media_types(&registered).map_err(to_wire)
}

/// The record a dead-letter sink publishes for `record`, fetched from `topic`/`partition`/`offset`:
/// key, value and headers verbatim except prior error headers, plus four `edax-dlq-*` headers.
/// The error header contains only `code`, or `unclassified` when absent. The compatibility
/// `message` argument is never persisted. An unknown code is a `codec:` error.
/// `code` trails because alef moves optional
/// parameters to the end. See [`edax_kafka::dead_letter_record`].
pub fn kafka_dead_letter_record(
    record: KafkaRecord,
    topic: &str,
    partition: i32,
    offset: i64,
    message: &str,
    code: Option<String>,
) -> Result<KafkaRecord, String> {
    let code = error_code(code)?;
    let kernel = edax_kafka::KafkaRecord::from(record);
    Ok(KafkaRecord::from(edax_kafka::dead_letter_record(
        &kernel, topic, partition, offset, code, message,
    )))
}

/// The dead-letter record `dead` with its value dropped and an `edax-dlq-dropped` header naming how many
/// bytes went, for a dead-letter topic that refused `dead` itself. See [`edax_kafka::dead_letter_stub`].
pub fn kafka_dead_letter_stub(dead: KafkaRecord) -> KafkaRecord {
    KafkaRecord::from(edax_kafka::dead_letter_stub(&edax_kafka::KafkaRecord::from(dead)))
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
    edax_kafka::partition_for(key, partitions).map_err(to_wire)
}

/// The identity of a message that did not decode: `"{topic}/{partition}/{offset}"`. See
/// [`edax_kafka::poison_id`].
pub fn kafka_poison_id(topic: &str, partition: i32, offset: i64) -> String {
    edax_kafka::poison_id(topic, partition, offset)
}

/// Refuse the durations a Kafka shell is built with, in milliseconds, as a `bad_handle:` error naming
/// the first one refused; `None` for a setting this call site does not take. See
/// [`edax_kafka::check_config`].
pub fn kafka_check_config(
    publish_timeout_ms: Option<i64>,
    commit_interval_ms: Option<i64>,
    commit_timeout_ms: Option<i64>,
    drain_timeout_ms: Option<i64>,
) -> Result<(), String> {
    edax_kafka::check_config(
        publish_timeout_ms,
        commit_interval_ms,
        commit_timeout_ms,
        drain_timeout_ms,
    )
    .map_err(to_wire)
}

/// Refuse a consumer client that auto-commits or has no group, as a `bad_handle:` error. A shell passes
/// what it read from its client library, and the safe value for a fact it cannot read. See
/// [`edax_kafka::check_consumer`].
pub fn kafka_check_consumer(auto_commit: bool, has_group: bool) -> Result<(), String> {
    edax_kafka::check_consumer(auto_commit, has_group).map_err(to_wire)
}

/// Refuse a consumer that runs with no dead-letter sink, as a `bad_handle:` error: it rewinds on a
/// nack, so a message that fails for good stalls its partition. See [`edax_kafka::check_dead_sink`].
pub fn kafka_check_dead_sink(has_dead_sink: bool) -> Result<(), String> {
    edax_kafka::check_dead_sink(has_dead_sink).map_err(to_wire)
}

/// The warning a producer client built with `acks` earns, or `None` when it acks on every in-sync
/// replica (`-1`, all). See [`edax_kafka::acks_warning`].
pub fn kafka_acks_warning(acks: i64) -> Option<String> {
    edax_kafka::acks_warning(acks)
}

/// The kernel error for a Kafka broker error, by its protocol error code, in its wire form
/// `"<code>: <message>"`. See [`edax_kafka::broker_error`].
pub fn kafka_broker_error(error_code: i64, message: &str) -> String {
    to_wire(edax_kafka::broker_error(error_code, message))
}

/// What [`OffsetTracker::nack`] tells the host to do. See [`edax_kafka::NackDecision`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct NackDecision {
    /// The offset to seek the partition back to — the lowest still in flight — or `None` when the
    /// token is stale or unknown and there is nothing to do.
    pub seek: Option<i64>,
}

/// One partition's watermark that is due for a commit. See [`edax_kafka::Due`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaDue {
    pub topic: String,
    pub partition: i32,
    /// The watermark to commit.
    pub offset: i64,
    /// The partition generation that produced this watermark.
    #[serde(default)]
    pub generation: i64,
}

impl From<edax_kafka::Due> for KafkaDue {
    fn from(due: edax_kafka::Due) -> Self {
        Self {
            topic: due.topic,
            partition: due.partition,
            offset: due.offset,
            generation: due.generation,
        }
    }
}

/// One partition's bookkeeping, for a health check or a gauge.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionStats {
    /// The topic this partition belongs to.
    pub topic: String,
    /// The partition number.
    pub partition: i32,
    /// Offsets delivered but not yet settled, retired ones included.
    pub unsettled: u64,
    /// Unsettled offsets whose token is still live: work a handler may still settle. A retired
    /// offset waits for a fetch instead.
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
        self.inner.book(topic, partition, offset).map_err(to_wire)
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
        self.inner.due().into_iter().map(KafkaDue::from).collect()
    }

    /// Record that the client committed `offset` on this partition.
    pub fn committed(&self, topic: &str, partition: i32, offset: i64) {
        self.inner.committed(topic, partition, offset);
    }

    /// Report a commit only to the generation that issued its watermark.
    pub fn committed_generation(&self, topic: &str, partition: i32, offset: i64, generation: i64) {
        self.inner.committed_generation(topic, partition, offset, generation);
    }

    /// Settle `token` as failed: rewind its partition, retire every token of it, and report the
    /// offset the host should seek the partition to.
    pub fn nack(&self, token: i64) -> NackDecision {
        match self.inner.nack(token) {
            edax_kafka::NackDecision::Seek { offset, .. } => NackDecision { seek: Some(offset) },
            edax_kafka::NackDecision::Stale => NackDecision { seek: None },
        }
    }

    /// Give up a revoked partition in one step: its last watermark still due, for the host to commit,
    /// and its bookkeeping dropped with every token retired. `None` when nothing of it is due, a
    /// partition never booked included, so a rebalance listener calls this for every revoked
    /// partition blindly. See [`edax_kafka::OffsetTracker::revoke`].
    pub fn revoke(&self, topic: &str, partition: i32) -> Option<KafkaDue> {
        self.inner.revoke(topic, partition).map(KafkaDue::from)
    }

    /// Drop a partition's bookkeeping and retire its tokens, committing nothing: for a partition just
    /// assigned, in case a revoke was never announced. A partition that is not tracked is a no-op.
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
    OffsetTracker {
        inner: Arc::new(edax_kafka::OffsetTracker::new()),
    }
}

/// Restore an ingestion rejection with its immutable source identity and provenance headers.
pub fn kafka_ingestion_rejection_record(raw: &str, source: &str) -> Result<KafkaRecord, String> {
    let raw: edax_kafka::record::IngestionRaw = crate::read_json(raw)?;
    edax_kafka::record::ingestion_rejection_record(&raw, source)
        .map(KafkaRecord::from)
        .map_err(to_wire)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use crate::*;

    /// Copy headers for a second call: the binding record carries no derives (alef owns them on the
    /// generated twin), and every header-taking call consumes its `Vec`.
    fn copy_headers(headers: &[KafkaHeader]) -> Vec<KafkaHeader> {
        headers
            .iter()
            .map(|header| KafkaHeader {
                key: header.key.clone(),
                value: header.value.clone(),
            })
            .collect()
    }

    /// What the boundary adds to the record kernel: the record crosses as a struct, so every field
    /// survives the conversion. The header set is the kernel's, pinned by
    /// `testdata/golden/kafka_record.json`.
    #[test]
    fn kafka_client_decisions_cross_in_wire_form() {
        // The rules themselves are pinned by `kafka_config.json` and `kafka_error.json`.
        assert_eq!(kafka_check_config(None, Some(1), Some(1), None), Ok(()));
        assert!(kafka_check_config(None, Some(0), None, None)
            .is_err_and(|e| e.starts_with("bad_handle: commit_interval_ms")));
        assert!(
            kafka_broker_error(29, "denied").starts_with("bad_handle: "),
            "a refused client"
        );
        assert!(
            kafka_broker_error(10, "too large").starts_with("codec: "),
            "a refused record"
        );
        assert!(
            kafka_broker_error(7, "timed out").starts_with("store: "),
            "a passing broker state"
        );
    }

    #[test]
    fn kafka_record_crosses_the_record_structurally() {
        let tenanted = || EventMessage {
            tenant: Some("acme".to_owned()),
            ..event_message("evt-1", Some(1))
        };
        let record = kafka_event_record("silo", tenanted(), b"<invoice/>".to_vec(), "application/xml", None).unwrap();
        assert_eq!(
            record.key.as_slice(),
            b"agg-1".as_slice(),
            "the aggregate, bare outside a pool"
        );
        assert_eq!(
            record.value.as_slice(),
            b"<invoice/>".as_slice(),
            "the host's own bytes, untouched"
        );
        let headers: Vec<(&str, &[u8])> = record
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_slice()))
            .collect();
        assert_eq!(
            headers,
            [(kafka_content_type_header().as_str(), b"application/xml".as_slice())],
            "a host serializer keeps the one contract header, under the kernel's name"
        );

        // The route crosses flattened: the default, an index into the host's list, or the unknown
        // media type — the empty one kept apart from no header at all.
        let route = |headers: Vec<KafkaHeader>| {
            let route = kafka_route(
                headers,
                vec!["application/json".to_owned(), "application/xml".to_owned()],
                "event",
            )
            .unwrap();
            (route.registered, route.unknown)
        };
        assert_eq!(route(copy_headers(&record.headers)), (Some(1), None));
        assert_eq!(route(Vec::new()), (None, None), "no headers at all: the default");
        let empty = vec![KafkaHeader {
            key: kafka_content_type_header(),
            value: Vec::new(),
        }];
        assert_eq!(
            route(empty),
            (None, Some(String::new())),
            "an empty value is unknown, not the default"
        );
        let headerless = kafka_route(Vec::new(), Vec::new(), "command").unwrap();
        assert!(headerless.headerless, "no header under a command default is poison");
        assert!(
            kafka_route(Vec::new(), Vec::new(), "query").is_err(),
            "an unknown kind is refused"
        );

        // The key crosses as the kernel decides it: `(tenancy, message, ..., key)`, each argument
        // reaching its own place. The rule itself is pinned by `kafka_record.json`.
        let command = || CommandMessage {
            tenant: Some("acme".to_owned()),
            ..command_message(None)
        };
        let keys = [
            (
                "pool_composes_the_aggregate",
                kafka_event_record("pool", tenanted(), Vec::new(), "x", None),
                Ok("4:acme:agg-1"),
            ),
            (
                "pool_composes_a_commands_own_id",
                kafka_command_record("pool", command(), Vec::new(), "x", None),
                Ok("4:acme:cmd-1"),
            ),
            (
                "override_wins_outright",
                kafka_event_record("pool", tenanted(), Vec::new(), "x", Some("k".to_owned())),
                Ok("k"),
            ),
            (
                "pool_without_a_tenant_is_refused",
                kafka_event_record("pool", event_message("evt-1", Some(1)), Vec::new(), "x", None),
                Err("codec: tenancy pool requires a tenant, and none was given"),
            ),
            (
                "bridge_without_a_tenant_is_refused",
                kafka_command_record("bridge", command_message(None), Vec::new(), "x", None),
                Err("codec: tenancy bridge requires a tenant, and none was given"),
            ),
            (
                "unknown_mode_is_refused",
                kafka_event_record("shard", tenanted(), Vec::new(), "x", None),
                Err("codec: unknown tenancy mode: shard"),
            ),
        ];
        for (name, got, want) in keys {
            let got = got.map(|record| String::from_utf8(record.key).unwrap());
            assert_eq!(got, want.map(str::to_owned).map_err(str::to_owned), "{name}");
        }
        assert_eq!(tenant_max(), 64, "the kernel's limit, read rather than restated");

        // A dead letter keeps the record and appends its source.
        let dead = kafka_dead_letter_record(
            KafkaRecord {
                key: record.key.clone(),
                value: record.value.clone(),
                headers: copy_headers(&record.headers),
            },
            "orders",
            3,
            42,
            "bad",
            Some("codec".to_owned()),
        )
        .unwrap();
        assert_eq!(
            (dead.key.as_slice(), dead.value.as_slice()),
            (record.key.as_slice(), record.value.as_slice())
        );
        assert_eq!(dead.headers.len(), record.headers.len() + 4);
        let source: Vec<(&str, &[u8])> = dead.headers[record.headers.len()..]
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_slice()))
            .collect();
        assert_eq!(
            source,
            [
                ("edax-dlq-topic", &b"orders"[..]),
                ("edax-dlq-partition", b"3"),
                ("edax-dlq-offset", b"42"),
                ("edax-dlq-error", b"codec")
            ]
        );
        let refused = kafka_dead_letter_record(
            KafkaRecord {
                key: Vec::new(),
                value: Vec::new(),
                headers: Vec::new(),
            },
            "t",
            0,
            0,
            "x",
            Some("oops".to_owned()),
        );
        assert_eq!(
            refused.map(|_| ()),
            Err("codec: unknown error code: oops".to_owned()),
            "a code the taxonomy does not know"
        );

        // Topic, partition and offset together are the identity of a message that did not decode.
        assert_eq!(kafka_poison_id("orders", 3, 42), "orders/3/42");

        // Placement: `(key, partitions)` in that order, Java's rule, and no partitions is refused.
        assert_eq!(kafka_partition_for(b"21", 1), Ok(0));
        assert_eq!(
            kafka_partition_for(b"21", 0),
            Err("codec: a topic has at least one partition, got 0".to_owned())
        );
    }

    /// What the boundary adds to the offset kernel: the decisions and the two pages cross field for
    /// field. The watermark rules themselves are the kernel's, pinned by
    /// `testdata/golden/kafka_offsets.json`.
    #[test]
    fn offset_tracker_crosses_its_decisions_structurally() {
        let tracker = offset_tracker();
        assert!(
            tracker.stats().is_empty(),
            "nothing is assigned before the first booking"
        );
        assert!(
            tracker.book("orders", 0, -1).is_err_and(|e| e.starts_with("codec:")),
            "a negative offset is refused"
        );

        let five = tracker.book("orders", 0, 5).unwrap();
        let six = tracker.book("orders", 0, 6).unwrap();
        assert_ne!(five, six, "one token per delivery");
        assert_ne!(five, edax_core::NO_TOKEN, "a live token never aliases the placeholder");
        assert!(tracker.is_live(six));

        assert!(tracker.ack(six), "settled");
        assert!(!tracker.ack(six), "stale: the token already settled");
        assert!(
            tracker.due().is_empty(),
            "5 is in flight: the watermark has not moved past the start"
        );
        let seven = tracker.book("orders", 0, 7).unwrap();
        let nack = tracker.nack(five);
        assert_eq!(nack.seek, Some(5));
        assert!(!tracker.is_live(seven), "the rewind retired the later token");
        assert_eq!(
            tracker.nack(edax_core::NO_TOKEN).seek,
            None,
            "a stale nack seeks nowhere"
        );

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
            .map(|row| {
                (
                    row.topic.as_str(),
                    row.partition,
                    row.unsettled,
                    row.live,
                    row.max_delivered,
                    row.committed,
                )
            })
            .collect();
        assert_eq!(rows, [("orders", 0, 1, 0, 7, Some(7))], "7 is unsettled and retired");

        tracker.reset("orders", 0);
        assert!(tracker.stats().is_empty(), "a revoked partition leaves no row behind");
    }
}

/// A revoked watermark with an owned outer record for Go's generated return conversion.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaRevoked {
    pub value: Option<KafkaDue>,
}

impl OffsetTracker {
    /// The ownership-safe C result shape of [`Self::revoke`].
    pub fn revoke_result(&self, topic: &str, partition: i32) -> KafkaRevoked {
        KafkaRevoked {
            value: self.revoke(topic, partition),
        }
    }
}

/// The bytes a fetched record counts against `max_record_bytes`. See [`edax_kafka::record::record_bytes`].
pub fn kafka_record_bytes(record: KafkaRecord, topic: &str) -> u64 {
    edax_kafka::record::record_bytes(&record.into(), topic) as u64
}

/// Kernel-owned durable source encoding, including a stable digest and oversize stub policy.
pub fn kafka_ingestion_raw(record: KafkaRecord, source: &str, limit: u64) -> Result<String, String> {
    #[derive(serde::Deserialize)]
    struct Source {
        topic: String,
        partition: i32,
        offset: i64,
        timestamp: Option<i64>,
        timestamp_type: Option<i32>,
    }
    let source: Source = crate::read_json(source)?;
    let raw = edax_kafka::record::ingestion_raw(
        &record.into(),
        &source.topic,
        source.partition,
        source.offset,
        source.timestamp,
        source.timestamp_type,
        limit as usize,
    )
    .map_err(to_wire)?;
    crate::write_json(&raw)
}

/// Decode the bytes of a persisted ingestion original (empty for a stub).
pub fn kafka_ingestion_record(raw: &str) -> Result<KafkaRecord, String> {
    let raw: edax_kafka::record::IngestionRaw = crate::read_json(raw)?;
    raw.record().map(KafkaRecord::from).map_err(to_wire)
}
