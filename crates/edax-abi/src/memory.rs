//! The `edax-memory` backend: every store behind one transaction boundary, and the broker.

use std::sync::Arc;

use crate::{claim_result, count};
use crate::{tenancy_mode, to_wire, ClaimResult, CommandMessage, EventDraft, EventMessage};

/// A stored snapshot as the event store holds it.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotResult {
    pub tenant: Option<String>,
    pub subject: Option<String>,
    pub encryption: Option<crate::EncryptionInfo>,
    pub type_name: String,
    pub version: u64,
    #[serde(default)]
    pub data: Vec<u8>,
}

/// A stored read-model document and its version.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProjectionRow {
    pub version: u64,
    /// The document as JSON text: always an object.
    pub document_json: String,
}

/// A stored saga instance and its version: running with its state and the kernel's `folded` marks, or
/// `completed` (a tombstone, whose `state_json` is `null` and whose `folded` is empty).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SagaRow {
    pub version: u64,
    pub completed: bool,
    pub state_json: String,
    pub folded: String,
}

/// A host's `u64` row limit as the store's `usize`, saturating on a target narrower than 64 bits.
fn limit_of(limit: u64) -> usize {
    usize::try_from(limit).unwrap_or(usize::MAX)
}

/// The token that names no transaction, [`edax_memory::Tx::auto_commit`], as the number the host holds:
/// a write under it lands at once and a read under it sees committed state only. The host passes it
/// for a call outside any unit of work. A call, because the generator binds functions and not
/// constants, so the token stays defined in one place.
pub fn no_tx() -> i64 {
    edax_memory::Tx::auto_commit().raw()
}

/// The host's token as the store's typed one. A host holds a number, never a Rust value, so every
/// store method crosses the token as `i64` and converts it here.
fn tx_of(raw: i64) -> edax_memory::Tx {
    edax_memory::Tx::from_raw(raw)
}

/// The in-memory backend for development and tests: one database holding every seam behind one
/// transaction boundary. Construct it with [`memory_transactor`], and reach each seam through its own
/// handle: [`event_store`](Self::event_store), [`idempotency_store`](Self::idempotency_store),
/// [`projection_store`](Self::projection_store), [`saga_store`](Self::saga_store) and
/// [`command_outbox`](Self::command_outbox). Every handle of one transactor reads and writes the same
/// database. See [`edax_memory::MemoryTransactor`].
///
/// The transaction crosses as a token, like the broker's delivery token: [`begin`](Self::begin)
/// hands one out, every store method takes one, [`commit`](Self::commit) applies that transaction's
/// writes on every seam atomically and [`rollback`](Self::rollback) drops them. Reads under a token
/// see the transaction's own staged writes; other tokens and [`no_tx`] see nothing of it before the
/// commit. The journal itself lives in `edax-memory`, so the host SDK keeps no transaction state
/// of its own: it holds the token across calls and hands it back.
///
/// Events cross as [`EventMessage`] records and commands as [`CommandMessage`] records. Python uses
/// native objects; Go uses the generated C interface with serde conversion. Snapshots carry bytes; idempotency
/// records, documents, saga state and query specs cross as JSON text. `edax-memory` is typed (`u64`
/// versions, JSON values, the kernel's query); the text is this boundary's shape, so the parsing
/// happens here.
pub struct MemoryTransactor {
    inner: Arc<edax_memory::MemoryTransactor>,
}

// Every method of every handle here is synchronous, the store calls included: the work is in-memory
// under one mutex, so there is nothing to await. An `async fn` here would make the generator spawn each
// call on a runtime thread, where it starts at once and a cancelled await cannot stop it. The host's
// async store views await nothing of the native call, so a cancellation lands before or after it,
// never inside.
impl MemoryTransactor {
    /// Open a transaction and return its token. Never [`no_tx`].
    pub fn begin(&self) -> i64 {
        self.inner.begin().raw()
    }

    /// Apply the transaction's writes on every seam atomically and close it. A token that names no
    /// open transaction is refused with a `bad_handle:` (terminal) error.
    pub fn commit(&self, tx: i64) -> Result<(), String> {
        self.inner.commit(tx_of(tx)).map_err(to_wire)
    }

    /// Drop the transaction's writes and close it. `false` and nothing changes if the token names no
    /// open transaction: there was nothing to roll back.
    pub fn rollback(&self, tx: i64) -> bool {
        self.inner.rollback(tx_of(tx))
    }

    /// How many transactions are open on this database. A leak diagnostic: a token that is never
    /// committed or rolled back conflicts every key it staged for the process lifetime. `u64` rather
    /// than `usize` because the boundary crosses fixed-width integers only.
    pub fn open_count(&self) -> u64 {
        count(self.inner.open_count())
    }

    /// The event store of this database: events, snapshots and the event outbox.
    pub fn event_store(&self) -> MemoryEventStore {
        MemoryEventStore {
            inner: self.inner.event_store(),
        }
    }

    /// The idempotency store of this database: the atomic claim over JSON records.
    pub fn idempotency_store(&self) -> MemoryIdempotencyStore {
        MemoryIdempotencyStore {
            inner: self.inner.idempotency_store(),
        }
    }

    /// The projection store of this database: versioned documents, queried by the kernel.
    pub fn projection_store(&self) -> MemoryProjectionStore {
        MemoryProjectionStore {
            inner: self.inner.projection_store(),
        }
    }

    /// The saga store of this database: versioned instances and each saga's command outbox.
    pub fn saga_store(&self) -> MemorySagaStore {
        MemorySagaStore {
            inner: self.inner.saga_store(),
        }
    }

    /// The command outbox of this database, which commits with the transaction's events.
    pub fn command_outbox(&self) -> MemoryCommandOutbox {
        MemoryCommandOutbox {
            inner: self.inner.command_outbox(),
        }
    }
}

/// The event store seam of a [`MemoryTransactor`]: a per-aggregate log with optimistic concurrency,
/// one snapshot per aggregate, and a transactional outbox. See [`edax_memory::MemoryEventStore`].
pub struct MemoryEventStore {
    inner: edax_memory::MemoryEventStore,
}

impl MemoryEventStore {
    /// Events for `aggregate_id` whose sequence is greater than `after`, as `tx` sees them.
    pub fn load(&self, tx: i64, aggregate_id: &str, after: u64) -> Result<Vec<EventMessage>, String> {
        let events = self.inner.load(tx_of(tx), aggregate_id, after).map_err(to_wire)?;
        Ok(events.into_iter().map(EventMessage::from).collect())
    }

    /// Append `drafts` under `tx`, each stamped with `recorded_at`, and enqueue the same onto the
    /// outbox. Returns the stored events. The stream's last sequence as `tx` sees it must equal
    /// `expected_version`, and the batch must carry sequences `expected_version + 1, + 2, ...`.
    ///
    /// `recorded_at` comes from the host because a clock cannot cross the boundary: the host's store
    /// view reads its own injectable clock. Under an open `tx`, the returned events are not committed.
    pub fn append(
        &self,
        tx: i64,
        aggregate_id: &str,
        expected_version: u64,
        drafts: Vec<EventDraft>,
        recorded_at: i64,
    ) -> Result<Vec<EventMessage>, String> {
        let drafts = drafts
            .into_iter()
            .map(edax_event::EventDraft::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_wire)?;
        let events = self
            .inner
            .append(tx_of(tx), aggregate_id, expected_version, drafts, recorded_at)
            .map_err(to_wire)?;
        Ok(events.into_iter().map(EventMessage::from).collect())
    }

    /// The snapshot for `aggregate_id` as `tx` sees it, or `None`.
    pub fn load_snapshot(&self, tx: i64, aggregate_id: &str) -> Result<Option<SnapshotResult>, String> {
        let snapshot = self.inner.load_snapshot(tx_of(tx), aggregate_id).map_err(to_wire)?;
        Ok(snapshot.map(|s| SnapshotResult {
            tenant: s.tenant,
            subject: s.subject,
            encryption: s.encryption.map(Into::into),
            type_name: s.type_name,
            version: s.version,
            data: s.data,
        }))
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
        self.inner
            .save_snapshot(tx_of(tx), aggregate_id, type_name, version, data)
            .map_err(to_wire)
    }

    /// Store the complete protected snapshot envelope.
    pub fn save_snapshot_record(&self, tx: i64, aggregate_id: &str, snapshot: SnapshotResult) -> Result<(), String> {
        self.inner
            .save_snapshot_record(
                tx_of(tx),
                aggregate_id,
                edax_memory::Snapshot {
                    type_name: snapshot.type_name,
                    version: snapshot.version,
                    data: snapshot.data,
                    tenant: snapshot.tenant,
                    subject: snapshot.subject,
                    encryption: snapshot
                        .encryption
                        .map(TryInto::try_into)
                        .transpose()
                        .map_err(to_wire)?,
                },
            )
            .map_err(to_wire)
    }

    /// Committed outbox events not yet published, capped at `limit`. The relay reads outside any
    /// transaction, so there is no token.
    pub fn unpublished(&self, limit: u64) -> Vec<EventMessage> {
        self.inner
            .unpublished(limit_of(limit))
            .into_iter()
            .map(EventMessage::from)
            .collect()
    }

    /// How many committed outbox events wait, without paging them.
    pub fn unpublished_count(&self) -> u64 {
        count(self.inner.unpublished_count())
    }

    /// Mark outbox events published by id. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, ids: Vec<String>) {
        self.inner.mark_published(&ids);
    }
}

/// The idempotency store seam of a [`MemoryTransactor`]: an atomic claim, complete and release over
/// JSON records, fenced on the claim's handle. See [`edax_memory::MemoryIdempotencyStore`].
pub struct MemoryIdempotencyStore {
    inner: edax_memory::MemoryIdempotencyStore,
}

impl MemoryIdempotencyStore {
    /// Atomically claim `key` under `tx` with a pending record: `won` with the handle that settles the
    /// claim, or the prior record JSON if the key was already present — the host decodes it to decide
    /// replay, payload mismatch, or that the winner has not completed yet. A key another open
    /// transaction holds reads as its pending record.
    pub fn claim(&self, tx: i64, key: &str, payload_hash: u64) -> Result<ClaimResult, String> {
        self.inner
            .claim(tx_of(tx), key, payload_hash)
            .map(claim_result)
            .map_err(to_wire)
    }

    /// Write the terminal record JSON for `key` under `tx`, replacing the claim `handle` names. A key
    /// that no longer holds that claim is refused with a `store:` error and nothing is written.
    pub fn complete(&self, tx: i64, key: &str, handle: Vec<u8>, record_json: &str) -> Result<(), String> {
        self.inner
            .complete(tx_of(tx), key, &handle, record_json.as_bytes())
            .map_err(to_wire)
    }

    /// Drop the claim `handle` names on `key` under `tx` (e.g. after a transient failure that should be
    /// retried). A key that no longer holds that claim is left alone.
    pub fn release(&self, tx: i64, key: &str, handle: Vec<u8>) -> Result<(), String> {
        self.inner.release(tx_of(tx), key, &handle).map_err(to_wire)
    }

    /// Delete the settled record `key` holds under `tx`, so the next claim wins and a re-driven message
    /// runs its handler again. A pending claim is refused with an `in_progress:` error, because its
    /// run may still complete. A completed or failed record is deleted, and so is a record no reader
    /// decodes. An absent key is a no-op. The decision is the kernel's ([`edax_cqrs::forget`]).
    pub fn forget(&self, tx: i64, key: &str) -> Result<(), String> {
        self.inner.forget(tx_of(tx), key).map_err(to_wire)
    }

    /// Lapse whatever `key` holds in committed state, as a lease or a retention TTL running out would:
    /// a test hook for rehearsing a takeover. `false` when nothing was held.
    pub fn expire(&self, key: &str) -> bool {
        self.inner.expire(key)
    }

    /// Overwrite `key` with `record_json` in committed state, readable or not: a test hook that rehearses
    /// a record another writer left behind. A key another open transaction holds is refused with a
    /// `concurrency:` error and kept. See [`edax_memory::MemoryIdempotencyStore::put_raw`].
    pub fn put_raw(&self, key: &str, record_json: &str) -> Result<(), String> {
        self.inner.put_raw(key, record_json.as_bytes()).map_err(to_wire)
    }
}

/// The projection store seam of a [`MemoryTransactor`]: versioned read-model documents, queried by the
/// kernel. See [`edax_memory::MemoryProjectionStore`].
pub struct MemoryProjectionStore {
    inner: edax_memory::MemoryProjectionStore,
}

impl MemoryProjectionStore {
    /// The stored document for `id` in read model `projection` and its version as `tx` sees it, or `None`; the document carries its
    /// row key as `"id"`, as [`keyed_projection_document`](crate::keyed_projection_document) reads it.
    pub fn get(&self, tx: i64, projection: &str, id: &str) -> Result<Option<ProjectionRow>, String> {
        let row = self.inner.get(tx_of(tx), projection, id).map_err(to_wire)?;
        Ok(row.map(|row| ProjectionRow {
            version: row.version,
            document_json: serde_json::Value::Object(row.value).to_string(),
        }))
    }

    /// Persist `document_json` under `tx` at `version + 1` if the stored version still equals
    /// `version`; returns the new version. A document the kernel's [`edax_cqrs::document_json`] refuses
    /// is a `codec:` error: one that is not a JSON object, which the query could never key, or one
    /// holding an integer wider than 64 bits, which would be stored as the nearest float.
    pub fn save(&self, tx: i64, projection: &str, id: &str, version: u64, document_json: &str) -> Result<u64, String> {
        let document = edax_cqrs::document_json(id, document_json.as_bytes()).map_err(to_wire)?;
        self.inner
            .save(tx_of(tx), projection, id, version, document)
            .map_err(to_wire)
    }

    /// Run a read query (a JSON spec) over the documents of read model `projection` as `tx` sees them; returns the kernel's page
    /// as JSON, `{"rows": [...], "next": cursor|null}`. A malformed spec is a `codec:` error.
    pub fn get_many(&self, tx: i64, projection: &str, query_json: &str) -> Result<String, String> {
        // The kernel's own reader, so a spec the kernel refuses (an integer past 64 bits) is refused here.
        let query = edax_cqrs::query_spec(query_json.as_bytes()).map_err(to_wire)?;
        self.inner
            .get_many(tx_of(tx), projection, &query)
            .and_then(|page| page.to_json())
            .map_err(to_wire)
    }
}

/// The saga store seam of a [`MemoryTransactor`]: versioned instances under optimistic concurrency,
/// their tombstones, and each saga's command outbox. See [`edax_memory::MemorySagaStore`].
pub struct MemorySagaStore {
    inner: edax_memory::MemorySagaStore,
}

impl MemorySagaStore {
    /// Instance `id` of `saga` as `tx` sees it, or `None`. A completed instance is a tombstone:
    /// `completed` is true and `state_json` is `null`.
    pub fn load(&self, tx: i64, saga: &str, id: &str) -> Result<Option<SagaRow>, String> {
        let row = self.inner.load(tx_of(tx), saga, id).map_err(to_wire)?;
        Ok(row.map(|row| {
            let (completed, state_json, folded) = match row.value {
                edax_memory::SagaRow::Active { state, folded } => (false, state.to_string(), folded),
                edax_memory::SagaRow::Completed => (true, "null".to_owned(), String::new()),
            };
            SagaRow {
                version: row.version,
                completed,
                state_json,
                folded,
            }
        }))
    }

    /// Persist `state_json`, the `folded` marks ([`saga_admit`](crate::saga_admit)) and the step's `commands` for instance
    /// `id` of `saga` under `tx` at `version + 1` if the stored version still equals `version` (an
    /// absent instance is at version 0); returns the new version. A completed instance takes no write.
    /// State that is not JSON, or holds an integer wider than 64 bits, is a `codec:` error
    /// ([`edax_cqrs::read_json`]).
    #[allow(
        clippy::too_many_arguments,
        reason = "the store's own arguments: the row's key, its version and its contents"
    )]
    pub fn save(
        &self,
        tx: i64,
        saga: &str,
        id: &str,
        version: u64,
        state_json: &str,
        folded: &str,
        commands: Vec<CommandMessage>,
    ) -> Result<u64, String> {
        let state: serde_json::Value = edax_cqrs::read_json("saga state", state_json.as_bytes()).map_err(to_wire)?;
        let commands = commands
            .into_iter()
            .map(edax_event::CommandMessage::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_wire)?;
        self.inner
            .save(tx_of(tx), saga, id, version, state, folded.to_owned(), commands)
            .map_err(to_wire)
    }

    /// Complete instance `id` of `saga` under `tx`: write its tombstone and the step's `commands` at
    /// `version + 1` under the same rules as [`save`](Self::save).
    pub fn complete(
        &self,
        tx: i64,
        saga: &str,
        id: &str,
        version: u64,
        commands: Vec<CommandMessage>,
    ) -> Result<u64, String> {
        let commands = commands
            .into_iter()
            .map(edax_event::CommandMessage::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_wire)?;
        self.inner
            .complete(tx_of(tx), saga, id, version, commands)
            .map_err(to_wire)
    }

    /// `saga`'s committed outbox commands not yet sent, oldest commit first, capped at `limit`. The
    /// relay reads outside any transaction, so there is no token.
    pub fn unpublished(&self, saga: &str, limit: u64) -> Vec<CommandMessage> {
        self.inner
            .unpublished(saga, limit_of(limit))
            .into_iter()
            .map(CommandMessage::from)
            .collect()
    }

    /// How many of `saga`'s committed outbox commands wait, without paging them.
    pub fn unpublished_count(&self, saga: &str) -> u64 {
        count(self.inner.unpublished_count(saga))
    }

    /// Mark `saga`'s outbox commands sent by id. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, saga: &str, ids: Vec<String>) {
        self.inner.mark_published(saga, &ids);
    }
}

/// The command outbox seam of a [`MemoryTransactor`]: the commands a save sends, committed with its
/// events. See [`edax_memory::MemoryCommandOutbox`].
pub struct MemoryCommandOutbox {
    inner: edax_memory::MemoryCommandOutbox,
}

impl MemoryCommandOutbox {
    /// Stage `commands` in the outbox under `tx`, to commit with the transaction's events. An id already
    /// waiting, or repeated in the batch, is a `codec:` error and stages nothing.
    pub fn enqueue(&self, tx: i64, commands: Vec<CommandMessage>) -> Result<(), String> {
        let commands = commands
            .into_iter()
            .map(edax_event::CommandMessage::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_wire)?;
        self.inner.enqueue(tx_of(tx), commands).map_err(to_wire)
    }

    /// Committed outbox commands not yet sent, oldest commit first, capped at `limit`. The relay reads
    /// outside any transaction, so there is no token.
    pub fn unpublished(&self, limit: u64) -> Vec<CommandMessage> {
        self.inner
            .unpublished(limit_of(limit))
            .into_iter()
            .map(CommandMessage::from)
            .collect()
    }

    /// How many committed outbox commands wait, without paging them.
    pub fn unpublished_count(&self) -> u64 {
        count(self.inner.unpublished_count())
    }

    /// Mark outbox commands sent by id. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, ids: Vec<String>) {
        self.inner.mark_published(&ids);
    }
}

/// A fresh in-memory database with every seam behind one transaction boundary.
pub fn memory_transactor() -> MemoryTransactor {
    MemoryTransactor {
        inner: Arc::new(edax_memory::MemoryTransactor::new()),
    }
}

/// One message a non-blocking poll fetched.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    /// Append `message` to the partition of its key: its aggregate, scoped by its tenant where the
    /// broker's tenancy composes keys. Every group reads it. A tenancy that requires a tenant and a
    /// message that names none is a `codec:` error, and nothing is appended.
    pub fn publish_event(&self, message: EventMessage) -> Result<(), String> {
        self.inner
            .publish_event(edax_event::EventMessage::try_from(message).map_err(to_wire)?)
            .map_err(to_wire)
    }

    /// Append `message` to the partition of its key: its target aggregate, or its own id when it has
    /// none, scoped as [`MemoryBroker::publish_event`] scopes it. Every group reads it. Fails as that does.
    pub fn publish_command(&self, message: CommandMessage) -> Result<(), String> {
        self.inner
            .publish_command(edax_event::CommandMessage::try_from(message).map_err(to_wire)?)
            .map_err(to_wire)
    }

    /// Fetch the next message for `group` as an unsettled delivery, or `None` at once when the group
    /// has fetched everything. A group's first poll starts at the beginning.
    pub fn try_poll(&self, group: &str) -> Result<Option<PollResult>, String> {
        let polled = self.inner.try_poll(group).map_err(to_wire)?;
        Ok(polled.map(|d| {
            // Flattened into two optional fields: the shape every binding can cross.
            let (event, command) = match d.message {
                edax_memory::LogMessage::Event(event) => (Some(EventMessage::from(event)), None),
                edax_memory::LogMessage::Command(command) => (None, Some(CommandMessage::from(command))),
            };
            PollResult {
                token: d.token,
                partition: d.partition,
                offset: d.offset,
                event,
                command,
            }
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

/// A fresh in-memory broker with `partitions` partitions under the tenancy mode `tenancy`: a message
/// lands on the partition Kafka gives its key. `0` partitions or an unknown mode is a `codec:` error.
/// See [`edax_memory::MemoryBroker::with_partitions`].
pub fn memory_broker(partitions: u32, tenancy: &str) -> Result<MemoryBroker, String> {
    let tenancy = tenancy_mode(tenancy)?;
    Ok(MemoryBroker {
        inner: Arc::new(edax_memory::MemoryBroker::with_partitions(partitions, tenancy).map_err(to_wire)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use crate::*;
    use serde_json::{json, Value};

    #[test]
    fn event_store_crosses_events_structurally() {
        let store = memory_transactor();
        // The store stamps the host's clock reading on every draft and hands back what it stored.
        let mut stored = store
            .event_store()
            .append(no_tx(), "agg-1", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_same_event(stored.remove(0), event_message("evt-1", Some(1)), "appended event");
        assert!(
            store
                .event_store()
                .append(no_tx(), "agg-1", 1, vec![], RECORDED_AT)
                .unwrap()
                .is_empty(),
            "a version check"
        );

        // Every field survives the boundary — no JSON or wire encoding in between.
        let mut page = store.event_store().load(no_tx(), "agg-1", 0).unwrap();
        assert_eq!(page.len(), 1);
        assert_same_event(page.remove(0), event_message("evt-1", Some(1)), "loaded event");
        assert!(store.event_store().load(no_tx(), "invoice-404", 0).unwrap().is_empty());

        // The adapter's own error code is forwarded untouched, not re-tagged as a codec failure.
        let conflict = store
            .event_store()
            .append(no_tx(), "agg-1", 0, vec![draft("evt-2", Some(2))], RECORDED_AT)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        let unsequenced = store
            .event_store()
            .append(no_tx(), "agg-1", 1, vec![draft("evt-2", None)], RECORDED_AT)
            .map(|_| ())
            .unwrap_err();
        assert_eq!(unsequenced, "codec: event 0 has no sequence");

        // Outbox: append enqueued the event; marking it published drains it.
        assert_eq!(
            (
                store.event_store().unpublished(10).len(),
                store.event_store().unpublished_count()
            ),
            (1, 1)
        );
        store.event_store().mark_published(vec!["evt-1".to_owned()]);
        assert!(store.event_store().unpublished(10).is_empty());
        assert_eq!(store.event_store().unpublished_count(), 0);

        // A missing snapshot crosses as `None`, not as an error.
        assert!(store.event_store().load_snapshot(no_tx(), "agg-1").unwrap().is_none());

        store
            .event_store()
            .save_snapshot(no_tx(), "agg-1", "Invoice", 1, br#"{"total":5000}"#.to_vec())
            .unwrap();
        let present = store
            .event_store()
            .load_snapshot(no_tx(), "agg-1")
            .unwrap()
            .expect("saved");
        assert_eq!(
            (present.type_name.as_str(), present.version, present.data),
            ("Invoice", 1, br#"{"total":5000}"#.to_vec())
        );
        // A snapshot leading the stream is the kernel's terminal refusal, forwarded untouched.
        let leading = store
            .event_store()
            .save_snapshot(no_tx(), "agg-1", "Invoice", 2, b"{}".to_vec())
            .unwrap_err();
        assert_eq!(
            leading,
            "codec: snapshot of agg-1 at version 2 leads its stream at version 1"
        );
    }

    #[test]
    fn idempotency_store_claims_and_records() {
        let store = memory_transactor();
        let hash = payload_hash_json(br#"{"email":"ada@example.com"}"#).unwrap();

        let won = store.idempotency_store().claim(no_tx(), "cmd-key-1", hash).unwrap();
        let handle = won.handle.expect("first claim wins, with a handle");
        assert!(
            won.won && !handle.is_empty() && won.prior.is_none(),
            "first claim wins, with a handle and no prior"
        );

        let lost = store.idempotency_store().claim(no_tx(), "cmd-key-1", hash).unwrap();
        assert!(!lost.won && lost.handle.is_none());
        let prior: Value = serde_json::from_str(&lost.prior.expect("a lost claim reads the prior record")).unwrap();
        assert_eq!(
            prior,
            json!({"payload_hash": hash, "result": null, "error": null, "error_code": null, "pending": true})
        );

        // The fence crosses: a handle that names no claim settles nothing, the live one does.
        let done = r#"{"payload_hash":0,"result":{"invoice_id":"invoice-7"},"error":null,"pending":false}"#;
        let refused = store
            .idempotency_store()
            .complete(no_tx(), "cmd-key-1", b"stale".to_vec(), done)
            .unwrap_err();
        assert!(
            refused.starts_with("store: idempotency key cmd-key-1 no longer holds"),
            "{refused}"
        );
        store
            .idempotency_store()
            .complete(no_tx(), "cmd-key-1", handle.clone(), done)
            .unwrap();
        assert_eq!(
            store
                .idempotency_store()
                .claim(no_tx(), "cmd-key-1", hash)
                .unwrap()
                .prior
                .as_deref(),
            Some(done)
        );

        // A release after the complete keeps the record; an expiry frees the key.
        store.idempotency_store().release(no_tx(), "cmd-key-1", handle).unwrap();
        assert_eq!(
            store
                .idempotency_store()
                .claim(no_tx(), "cmd-key-1", hash)
                .unwrap()
                .prior
                .as_deref(),
            Some(done)
        );
        assert!(store.idempotency_store().expire("cmd-key-1"));
        assert!(!store.idempotency_store().expire("cmd-key-404"), "nothing to lapse");
        let again = store.idempotency_store().claim(no_tx(), "cmd-key-1", hash).unwrap();
        assert!(again.won, "an expired key is claimable again");
        store
            .idempotency_store()
            .release(no_tx(), "cmd-key-1", again.handle.expect("won"))
            .unwrap();
        assert!(
            store.idempotency_store().claim(no_tx(), "cmd-key-1", hash).unwrap().won,
            "a released key is claimable again"
        );
    }

    #[test]
    fn forget_crosses_the_kernels_decision() {
        let store = memory_transactor();
        let done = r#"{"payload_hash":7,"result":null,"error":null,"pending":false}"#;

        // A pending claim may still complete, so it is refused with the kernel's code.
        let claim = store.idempotency_store().claim(no_tx(), "req-A", 7).unwrap();
        let refused = store.idempotency_store().forget(no_tx(), "req-A").unwrap_err();
        assert!(refused.starts_with("in_progress: "), "{refused}");

        // A settled record goes, so the next claim wins and a re-driven message runs again.
        store
            .idempotency_store()
            .complete(no_tx(), "req-A", claim.handle.expect("won"), done)
            .unwrap();
        assert_eq!(store.idempotency_store().forget(no_tx(), "req-A"), Ok(()));
        assert!(
            store.idempotency_store().claim(no_tx(), "req-A", 7).unwrap().won,
            "a forgotten key is claimable again"
        );
    }

    #[test]
    fn projection_store_versions_and_queries() {
        let store = memory_transactor();
        assert!(store.projection_store().get(no_tx(), "p", "user-1").unwrap().is_none());

        assert_eq!(
            store
                .projection_store()
                .save(no_tx(), "p", "user-1", 0, r#"{"email":"ada@example.com","age":36}"#)
                .unwrap(),
            1
        );
        let row = store
            .projection_store()
            .get(no_tx(), "p", "user-1")
            .unwrap()
            .expect("saved");
        assert_eq!(row.version, 1);
        let keyed = json!({"id": "user-1", "email": "ada@example.com", "age": 36});
        assert_eq!(
            serde_json::from_str::<Value>(&row.document_json).unwrap(),
            keyed,
            "read with its row key"
        );

        // The adapter's codes cross untouched: a stale version is a conflict, a bad document is codec.
        let conflict = store
            .projection_store()
            .save(no_tx(), "p", "user-1", 0, r#"{"email":"ada@example.com","age":37}"#)
            .unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        // The kernel's document rule decides, as `check_projection_document` reports it to a host's store.
        for (name, document) in [
            ("not_json", r#"{"email":"ada@example.com","#),
            ("not_an_object", r#"["ada@example.com"]"#),
            ("a_scalar", "36"),
        ] {
            let refused = store
                .projection_store()
                .save(no_tx(), "p", "user-2", 0, document)
                .unwrap_err();
            assert_eq!(Err(refused), check_projection_document("user-2", document), "{name}");
        }
        assert_eq!(check_projection_document("user-2", r#"{"age":29}"#), Ok(()));

        store
            .projection_store()
            .save(no_tx(), "p", "user-2", 0, r#"{"email":"grace@example.com","age":29}"#)
            .unwrap();
        let page: Value = serde_json::from_str(
            &store
                .projection_store()
                .get_many(no_tx(), "p", r#"{"sort":[{"path":"age"}]}"#)
                .unwrap(),
        )
        .unwrap();
        let ids: Vec<&str> = page["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!((ids, &page["next"]), (vec!["user-2", "user-1"], &Value::Null));
        assert!(store
            .projection_store()
            .get_many(no_tx(), "p", "{not a query}")
            .unwrap_err()
            .starts_with("codec: "));
        let wide = store.projection_store().get_many(
            no_tx(),
            "p",
            r#"{"filter":{"compare":{"path":"age","op":"eq","value":1180591620717411303424}}}"#,
        );
        assert!(
            wide.is_err_and(|e| e.starts_with("codec: ")),
            "an integer past 64 bits is the kernel's refusal"
        );
        let cursor = store
            .projection_store()
            .get_many(no_tx(), "p", r#"{"sort":[{"path":"age"}],"after":"!!!"}"#)
            .unwrap_err();
        assert!(
            cursor.starts_with("codec: "),
            "the kernel's cursor refusal crosses untouched: {cursor}"
        );
    }

    #[test]
    fn saga_store_versions_and_completes() {
        let store = memory_transactor();
        assert!(store
            .saga_store()
            .load(no_tx(), "shipping", "order-1")
            .unwrap()
            .is_none());

        let folded = r#"{"order-1":{"sequence":1,"id":"evt-1","unsequenced":"evt-u"}}"#;
        assert_eq!(
            store
                .saga_store()
                .save(
                    no_tx(),
                    "shipping",
                    "order-1",
                    0,
                    r#"{"status":"awaiting_payment"}"#,
                    folded,
                    vec![]
                )
                .unwrap(),
            1
        );
        let row = store
            .saga_store()
            .load(no_tx(), "shipping", "order-1")
            .unwrap()
            .expect("saved");
        assert_eq!(
            (row.version, row.completed, row.folded.as_str()),
            (1, false, folded),
            "the marks cross as they were saved"
        );
        assert_eq!(
            serde_json::from_str::<Value>(&row.state_json).unwrap(),
            json!({"status": "awaiting_payment"})
        );

        // The adapter's codes cross untouched: a stale version is a conflict, bad state JSON is codec.
        let conflict = store
            .saga_store()
            .save(no_tx(), "shipping", "order-1", 0, r#"{"status":"shipped"}"#, "", vec![])
            .unwrap_err();
        assert_eq!(conflict, "concurrency: expected 0, stored 1");
        assert!(store
            .saga_store()
            .save(no_tx(), "shipping", "order-1", 1, "{not json", "", vec![])
            .unwrap_err()
            .starts_with("codec: saga state is not JSON: "));
        let wide = store
            .saga_store()
            .save(
                no_tx(),
                "shipping",
                "order-1",
                1,
                r#"{"total":18446744073709551616}"#,
                "",
                vec![],
            )
            .unwrap_err();
        assert_eq!(
            wide, "codec: integer at byte 9 is outside the 64-bit range",
            "a state the store would round is refused"
        );
        let stale_complete = store
            .saga_store()
            .complete(no_tx(), "shipping", "order-1", 0, vec![])
            .unwrap_err();
        assert_eq!(stale_complete, "concurrency: expected 0, stored 1");

        // The commands cross structurally with the tombstone and come back from the outbox.
        let fulfill = || {
            let command = Payload {
                name: "Fulfill".to_owned(),
                version: 1,
                data: br#"{"order_id":"order-1"}"#.to_vec(),
            };
            let cause = event_message("evt-1", Some(1));
            saga_command(
                "shipping",
                "order-1",
                2,
                0,
                command,
                &cause.id,
                &cause.correlation_id,
                7,
                None,
                None,
            )
        };
        assert_eq!(
            store
                .saga_store()
                .complete(no_tx(), "shipping", "order-1", 1, vec![fulfill()])
                .unwrap(),
            2
        );
        let done = store
            .saga_store()
            .load(no_tx(), "shipping", "order-1")
            .unwrap()
            .expect("a tombstone");
        assert_eq!(
            (
                done.version,
                done.completed,
                done.state_json.as_str(),
                done.folded.as_str()
            ),
            (2, true, "null", "")
        );
        let mut unsent = store.saga_store().unpublished("shipping", 10);
        assert_eq!(unsent.len(), 1);
        assert_same_command(unsent.remove(0), fulfill(), "the outbox row");
        store
            .saga_store()
            .mark_published("billing", vec!["8:shipping:order-1:2:0".to_owned()]);
        assert_eq!(
            store.saga_store().unpublished_count("shipping"),
            1,
            "another saga's mark prunes nothing here"
        );
        store
            .saga_store()
            .mark_published("shipping", vec!["8:shipping:order-1:2:0".to_owned()]);
        assert_eq!(store.saga_store().unpublished_count("shipping"), 0);
        assert!(store.saga_store().unpublished("shipping", 10).is_empty());
        let resurrect = store
            .saga_store()
            .save(no_tx(), "shipping", "order-1", 0, r#"{"status":"open"}"#, "", vec![])
            .unwrap_err();
        assert!(
            resurrect.starts_with("concurrency: saga shipping instance order-1 completed"),
            "{resurrect}"
        );
        assert_eq!(
            limit_of(u64::MAX),
            usize::MAX,
            "a limit past the target's width saturates"
        );
    }

    #[test]
    fn transaction_token_crosses_the_boundary_and_settles_every_seam() {
        // The host holds only the token between calls: the journal is the adapters'. One write on
        // every seam under it, then the two outcomes.
        struct Case {
            name: &'static str,
            commit: bool,
            /// `(events, outbox, snapshot, projection, claim taken, saga)` visible under no_tx() afterwards.
            want: (usize, usize, bool, bool, bool, bool),
        }
        let cases = [
            Case {
                name: "commit lands every seam",
                commit: true,
                want: (1, 1, true, true, true, true),
            },
            Case {
                name: "rollback leaves nothing on any seam",
                commit: false,
                want: (0, 0, false, false, false, false),
            },
        ];
        for c in cases {
            let store = memory_transactor();
            let tx = store.begin();
            assert_ne!(tx, no_tx(), "{}", c.name);
            store
                .event_store()
                .append(tx, "agg-1", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
                .unwrap();
            store
                .event_store()
                .save_snapshot(tx, "agg-1", "Invoice", 1, b"snap".to_vec())
                .unwrap();
            store
                .projection_store()
                .save(tx, "p", "user-1", 0, r#"{"age":36}"#)
                .unwrap();
            assert!(
                store.idempotency_store().claim(tx, "req-A", 7).unwrap().won,
                "{}",
                c.name
            );
            store
                .saga_store()
                .save(tx, "shipping", "order-1", 0, r#"{"status":"open"}"#, "", vec![])
                .unwrap();

            // Read-your-writes under the token; nothing visible under no_tx() yet (the outbox never
            // shows a staged append).
            assert_eq!(store.event_store().load(tx, "agg-1", 0).unwrap().len(), 1, "{}", c.name);
            assert!(
                store.event_store().load(no_tx(), "agg-1", 0).unwrap().is_empty(),
                "{}",
                c.name
            );
            assert!(store.event_store().unpublished(10).is_empty(), "{}", c.name);
            assert!(
                store.projection_store().get(no_tx(), "p", "user-1").unwrap().is_none(),
                "{}",
                c.name
            );

            if c.commit {
                store.commit(tx).unwrap();
            } else {
                assert!(store.rollback(tx), "{}", c.name);
            }
            let visible = (
                store.event_store().load(no_tx(), "agg-1", 0).unwrap().len(),
                store.event_store().unpublished(10).len(),
                store.event_store().load_snapshot(no_tx(), "agg-1").unwrap().is_some(),
                store.projection_store().get(no_tx(), "p", "user-1").unwrap().is_some(),
                !store.idempotency_store().claim(no_tx(), "req-A", 7).unwrap().won,
                store
                    .saga_store()
                    .load(no_tx(), "shipping", "order-1")
                    .unwrap()
                    .is_some(),
            );
            assert_eq!(visible, c.want, "{}", c.name);

            // A spent token is refused everywhere with the terminal code, and a rollback of it is a no-op.
            let refused = store.event_store().load(tx, "agg-1", 0).map(|p| p.len()).unwrap_err();
            assert!(
                refused.starts_with("bad_handle: unknown transaction"),
                "{}: {refused}",
                c.name
            );
            assert_eq!(store.commit(tx).unwrap_err(), refused, "{}", c.name);
            assert!(!store.rollback(tx), "{}", c.name);
        }
    }

    #[test]
    fn broker_polls_and_settles_by_token() {
        assert!(
            memory_broker(0, "silo").is_err_and(|e| e.starts_with("codec:")),
            "zero partitions is refused"
        );
        assert!(
            memory_broker(1, "shared").is_err_and(|e| e.starts_with("codec:")),
            "an unknown tenancy is refused"
        );
        let pooled = memory_broker(1, "pool").unwrap();
        assert!(
            pooled
                .publish_event(event_message("evt-1", Some(1)))
                .is_err_and(|e| e.starts_with("codec:")),
            "a pool needs a tenant"
        );
        assert!(pooled.is_empty("g"), "and the refused event is nowhere");
        let broker = memory_broker(1, "silo").unwrap();
        assert!(broker.is_empty("g"));

        // An empty poll is `None`, never an error.
        assert!(broker.try_poll("g").unwrap().is_none());

        broker.publish_event(event_message("evt-1", Some(1))).unwrap();
        broker.publish_event(event_message("evt-1", Some(1))).unwrap(); // at-least-once: the same event republished
        assert!(!broker.is_empty("g"));

        let mut first = broker.try_poll("g").unwrap().expect("published");
        assert_eq!((first.partition, first.offset), (0, 0), "the coordinates cross");
        assert_same_event(
            first.event.take().expect("an event"),
            event_message("evt-1", Some(1)),
            "polled event",
        );
        assert!(first.command.is_none(), "an event delivery carries no command");
        let second = broker.try_poll("g").unwrap().expect("published twice");
        assert_eq!(second.offset, 1);
        assert_ne!(first.token, second.token, "one id in flight twice is two deliveries");

        // A nack rewinds: both copies are retired, the stale tokens settle nothing, and both come back.
        assert!(broker.is_live("g", second.token));
        assert!(broker.nack("g", first.token));
        assert!(
            !broker.is_live("g", second.token),
            "the rewind retired the later delivery"
        );
        assert!(!broker.ack("g", first.token));
        let again = broker.try_poll("g").unwrap().expect("rewound");
        assert!(again.token != first.token && again.offset == 0);

        // Another group reads every message on its own; a crash redelivers from the commit.
        let other = broker.try_poll("h").unwrap().expect("another group reads everything");
        assert_eq!(other.offset, 0);
        broker.crash("h");
        assert!(!broker.is_live("h", other.token));
        assert_eq!(
            broker.try_poll("h").unwrap().map(|d| d.offset),
            Some(0),
            "nothing was committed, so 0 comes back"
        );

        assert!(broker.ack("g", again.token));
        assert!(!broker.ack("g", again.token), "a second settlement is refused");
        let last = broker.try_poll("g").unwrap().expect("offset 1");
        assert!(!broker.is_empty("g"), "offset 1 is still outstanding");
        assert!(broker.ack("g", last.token));
        assert!(broker.is_empty("g"));
        assert!(!broker.is_empty("h"));

        // A command crosses the same log, in the other field.
        broker.publish_command(command_message(Some("invoice-7"))).unwrap();
        let mut command = broker.try_poll("g").unwrap().expect("a command");
        assert!(command.event.is_none(), "a command delivery carries no event");
        assert_same_command(
            command.command.take().expect("a command"),
            command_message(Some("invoice-7")),
            "polled command",
        );
    }
}

// Alef 0.106.1's Go wrappers do not free optional record handles returned by methods.
// These non-optional envelopes use its regular owned-return path. Python keeps the
// original methods; the Go facade unwraps `value` after the generated call frees the envelope.

/// A snapshot lookup whose outer record always has an owned native handle.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotLookup {
    pub value: Option<SnapshotResult>,
}

/// A projection lookup whose outer record always has an owned native handle.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProjectionLookup {
    pub value: Option<ProjectionRow>,
}

/// A saga lookup whose outer record always has an owned native handle.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SagaLookup {
    pub value: Option<SagaRow>,
}

/// A broker poll whose outer record always has an owned native handle.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PollLookup {
    pub value: Option<PollResult>,
}

impl MemoryEventStore {
    /// The ownership-safe C result shape of [`Self::load_snapshot`].
    pub fn load_snapshot_result(&self, tx: i64, aggregate_id: &str) -> Result<SnapshotLookup, String> {
        self.load_snapshot(tx, aggregate_id)
            .map(|value| SnapshotLookup { value })
    }
}
impl MemoryProjectionStore {
    /// The ownership-safe C result shape of [`Self::get`].
    pub fn get_result(&self, tx: i64, projection: &str, id: &str) -> Result<ProjectionLookup, String> {
        self.get(tx, projection, id).map(|value| ProjectionLookup { value })
    }
}
impl MemorySagaStore {
    /// The ownership-safe C result shape of [`Self::load`].
    pub fn load_result(&self, tx: i64, saga: &str, id: &str) -> Result<SagaLookup, String> {
        self.load(tx, saga, id).map(|value| SagaLookup { value })
    }
}
impl MemoryBroker {
    /// The ownership-safe C result shape of [`Self::try_poll`].
    pub fn try_poll_result(&self, group: &str) -> Result<PollLookup, String> {
        self.try_poll(group).map(|value| PollLookup { value })
    }
}
/// An atomic in-memory ingestion database. Persist this handle across runner restarts in tests.
pub struct MemoryIngestion {
    inner: edax_memory::MemoryIngest,
}

/// Build an independent in-memory ingestion database.
pub fn memory_ingestion() -> MemoryIngestion {
    MemoryIngestion {
        inner: edax_memory::MemoryIngest::default(),
    }
}

impl MemoryIngestion {
    /// Commit original records and durable rejection decisions together.
    pub fn commit(&self, manifest: &str, inputs: &str) -> Result<String, String> {
        let contract = crate::read_json::<edax_cqrs::ingest::Contract>(manifest)?;
        let inputs = edax_cqrs::ingest::inputs_json(inputs).map_err(crate::to_wire)?;
        crate::write_json(&self.inner.commit(&contract, &inputs).map_err(crate::to_wire)?)
    }

    /// Read a committed row as JSON, or JSON null.
    pub fn get(&self, key: &str) -> Result<String, String> {
        crate::write_json(&self.inner.get(key))
    }

    /// Read pending rejections as a JSON map, scoped to a target prefix.
    pub fn pending(&self, prefix: &str, limit: u32) -> Result<String, String> {
        crate::write_json(&self.inner.pending(prefix, limit as usize))
    }

    /// Delete every row in the erasure key range `[start, end)`; return how many.
    pub fn delete_scope(&self, start: &str, end: &str) -> u64 {
        count(self.inner.delete_scope(start, end))
    }

    /// Confirm publication of a rejection whose original fingerprint still matches.
    pub fn published(&self, source: &str, fingerprint: &str) -> Result<(), String> {
        self.inner.published(source, fingerprint).map_err(crate::to_wire)
    }

    /// Confirm a JSON map of source fingerprints atomically; invalid entries mark nothing.
    pub fn published_many(&self, confirmed: &str) -> Result<(), String> {
        self.inner
            .published_many(&crate::read_json(confirmed)?)
            .map_err(crate::to_wire)
    }
}

/// An atomic in-memory projection state database: records and effect intents. Keep this handle for
/// as long as the state should live.
pub struct MemoryProjectionDatabase {
    inner: edax_memory::MemoryProjectionState,
}

/// Build an independent in-memory projection state database.
pub fn memory_projection_database() -> MemoryProjectionDatabase {
    MemoryProjectionDatabase {
        inner: edax_memory::MemoryProjectionState::default(),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionWrites {
    inserts: std::collections::BTreeMap<String, serde_json::Value>,
    updates: std::collections::BTreeMap<String, serde_json::Value>,
    intents: std::collections::BTreeMap<String, serde_json::Value>,
}

impl MemoryProjectionDatabase {
    /// Read the JSON list `keys`: `{"records": {key: value}, "versions": {key: version}}`, an absent key
    /// at version zero with no record.
    pub fn read(&self, keys: &str) -> Result<String, String> {
        let read = self.inner.read(&crate::read_json::<Vec<String>>(keys)?);
        crate::write_json(&serde_json::json!({"records": read.records, "versions": read.versions}))
    }

    /// Commit `writes`, `{"inserts", "updates", "intents"}`, if every record in `versions` is unchanged.
    pub fn commit(&self, versions: &str, writes: &str) -> Result<(), String> {
        let writes: ProjectionWrites = crate::read_json(writes)?;
        let writes = edax_memory::Writes {
            inserts: writes.inserts,
            updates: writes.updates,
            intents: writes.intents,
        };
        self.inner
            .commit(&crate::read_json(versions)?, &writes)
            .map_err(to_wire)
    }

    /// Up to `limit` deliverable intents of `generation`, as a JSON list in delivery order.
    pub fn unpublished(&self, manifest_key: &str, generation: &str, limit: u32) -> Result<String, String> {
        crate::write_json(
            &self
                .inner
                .unpublished(manifest_key, generation, limit as usize)
                .map_err(to_wire)?,
        )
    }

    /// Delete each still-eligible intent the JSON list `keys` names; return how many.
    pub fn mark_published(&self, manifest_key: &str, generation: &str, keys: &str) -> Result<u64, String> {
        let keys: Vec<String> = crate::read_json(keys)?;
        Ok(count(
            self.inner
                .mark_published(manifest_key, generation, &keys)
                .map_err(to_wire)?,
        ))
    }

    /// Delete every record in the erasure key range `[start, end)`; return how many.
    pub fn delete_scope(&self, start: &str, end: &str) -> u64 {
        count(self.inner.delete_scope(start, end))
    }

    /// Delete up to `limit` intents a cutover made permanently ineligible; return how many.
    pub fn prune(&self, manifest_key: &str, generation: &str, limit: u32) -> Result<u64, String> {
        Ok(count(
            self.inner
                .prune(manifest_key, generation, limit as usize)
                .map_err(to_wire)?,
        ))
    }
}
