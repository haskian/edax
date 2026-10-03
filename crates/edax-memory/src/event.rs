//! The event store view: the per-aggregate log, its snapshots and the transactional outbox.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error};

use crate::tx::{held_by_other, sequence_of, store_view, Committed, MemoryState, MemoryTransactor, WriteSet};

/// Refuse a batch carrying an envelope id the store already holds, another open transaction or this
/// one has staged, or the batch repeats. Deterministic, so `codec`.
fn refuse_known_ids(
    committed: &Committed,
    others: &std::collections::HashMap<i64, WriteSet>,
    own: &WriteSet,
    events: &[edax_event::EventEnvelope],
) -> Result<(), Error> {
    let staged = |set: &WriteSet, id: &str| set.streams.values().flatten().any(|e| e.id == id);
    let mut batch: HashSet<&str> = HashSet::new();
    for env in events {
        let known = committed.event_ids.contains(&env.id)
            || staged(own, &env.id)
            || others.values().any(|set| staged(set, &env.id))
            || !batch.insert(env.id.as_str());
        if known {
            return Err(Error::codec(format_args!("envelope id {} is already stored", env.id)));
        }
    }
    Ok(())
}

/// A stored aggregate snapshot: the host-marshaled state plus its type and version.
#[derive(Clone)]
pub struct Snapshot {
    pub type_name: String,
    pub version: u64,
    pub data: Vec<u8>,
}

/// In-memory event store for development and tests: a per-aggregate event log with optimistic
/// concurrency on the sequence, one snapshot per aggregate, and a transactional outbox that holds
/// only unpublished rows. Events are held as the kernel's own structured
/// [`EventEnvelope`](edax_event::EventEnvelope) — no serialization at the binding. Appends and
/// snapshots under an open token are staged until the transactor commits; the outbox is read by the
/// relay outside any transaction and holds committed rows only. Not for production: a durable store
/// appends events and enqueues the outbox in one database transaction.
#[derive(Clone)]
pub struct MemoryEventStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryEventStore, event_store);

impl MemoryEventStore {
    /// Envelopes for `aggregate_id` whose sequence is greater than `after`, including the ones `tx`
    /// itself appended. `Err` only if `tx` names no open transaction.
    pub fn load(&self, tx: i64, aggregate_id: &str, after: u64) -> Result<Vec<edax_event::EventEnvelope>, Error> {
        lock(&self.state).read(tx, |committed, own| {
            let committed_stream = committed.events.get(aggregate_id).into_iter().flatten();
            let staged_tail = own.and_then(|set| set.streams.get(aggregate_id)).into_iter().flatten();
            committed_stream.chain(staged_tail).filter(|e| sequence_of(e) > after).cloned().collect()
        })
    }

    /// Append `events` and enqueue the same onto the outbox, atomically. The stream's last sequence
    /// as `tx` sees it must equal `expected_version`, else a conflict error is returned; the batch
    /// must carry sequences `expected_version + 1, + 2, ...` in order, and every envelope id must be
    /// new to the whole store (stored, staged by any open transaction, or earlier in the batch), else
    /// a codec error is returned and nothing is written. A stream another open transaction has
    /// appended to is held until that transaction ends: the conflict reports the version it would
    /// commit.
    ///
    /// The id rule is what makes [`mark_published`](Self::mark_published) by id safe: with two rows
    /// under one id, publishing the first would prune the second unpublished. A durable store gets it
    /// from a primary key on the id.
    ///
    /// An empty batch is a version check, not a repeatable read: it stages no tail, so it holds
    /// nothing, and another transaction may append to the stream before this token commits. A caller
    /// that needs the version to stay put must stage a write under the token.
    pub fn append(
        &self,
        tx: i64,
        aggregate_id: &str,
        expected_version: u64,
        events: Vec<edax_event::EventEnvelope>,
    ) -> Result<(), Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            // The stale-writer conflict is checked first: it is the documented reload-and-retry signal,
            // and a stale caller's sequences are wrong by construction.
            // A held stream reports the committed version: the holder's own may yet roll back.
            let stored = committed.events.get(aggregate_id).and_then(|s| s.last()).map_or(0, sequence_of);
            if held_by_other(others, |set| set.stream_version(aggregate_id)).is_some() {
                return Err(Error::concurrency(expected_version, stored));
            }
            let have = own.stream_version(aggregate_id).unwrap_or(stored);
            if have != expected_version {
                return Err(Error::concurrency(expected_version, have));
            }
            // The dense-batch rule is the kernel's, so every store enforces the same one; the vector
            // `testdata/golden/sequence_batch.json` pins it. Whole-batch rejection, nothing written.
            //
            // Tagged `codec:` (terminal), not `store:`: the same batch fails identically every time, so a
            // transient tag would have a host retry its whole budget and then redeliver it forever. The
            // taxonomy's codes encode *transience*, not failure kind, and `codec:` is the
            // deterministic-terminal slot.
            let sequences: Vec<Option<u64>> = events.iter().map(|e| e.sequence).collect();
            edax_event::sequence::check_batch(expected_version, &sequences)?;
            refuse_known_ids(committed, others, own, &events)?;
            // An empty batch stages no tail, so it holds nothing: it was a version check only.
            if events.is_empty() {
                return Ok(());
            }
            own.streams.entry(aggregate_id.to_owned()).or_default().extend(events.iter().cloned());
            own.outbox.extend(events);
            Ok(())
        })
    }

    /// The snapshot for `aggregate_id` as `tx` sees it, or `None`. `Err` only if `tx` names no open
    /// transaction.
    pub fn load_snapshot(&self, tx: i64, aggregate_id: &str) -> Result<Option<Snapshot>, Error> {
        lock(&self.state).read(tx, |committed, own| {
            own.and_then(|set| set.snapshots.get(aggregate_id))
                .or_else(|| committed.snapshots.get(aggregate_id))
                .cloned()
        })
    }

    /// Store a snapshot for `aggregate_id` under `tx`. A snapshot may lag the stream but never lead
    /// it: one past the stream's last sequence as `tx` sees it is a codec error and stores nothing,
    /// because the repository would load it, find no events after it, and conflict on every append.
    /// A snapshot older than the one `tx` sees is dropped and the newer one kept, so a slow writer
    /// never rolls the cache back; two transactions that both save keep the higher version whichever
    /// commits last. `Err` also if `tx` names no open transaction.
    pub fn save_snapshot(&self, tx: i64, aggregate_id: &str, type_name: &str, version: u64, data: Vec<u8>) -> Result<(), Error> {
        let snapshot = Snapshot { type_name: type_name.to_owned(), version, data };
        lock(&self.state).write(tx, |committed, _, own| {
            let stream = own
                .stream_version(aggregate_id)
                .or_else(|| committed.events.get(aggregate_id).and_then(|s| s.last()).map(sequence_of))
                .unwrap_or(0);
            if version > stream {
                return Err(Error::codec(format_args!(
                    "snapshot of {aggregate_id} at version {version} leads its stream at version {stream}"
                )));
            }
            let current = own.snapshots.get(aggregate_id).or_else(|| committed.snapshots.get(aggregate_id));
            if current.is_some_and(|stored| stored.version > version) {
                return Ok(());
            }
            own.snapshots.insert(aggregate_id.to_owned(), snapshot);
            Ok(())
        })
    }

    /// Committed outbox events not yet published, capped at `limit`. The outbox holds only
    /// unpublished rows (published ones are pruned by [`mark_published`](Self::mark_published)), so
    /// this is a bounded prefix scan rather than a filter over an ever-growing log. The relay reads
    /// outside any transaction, so there is no token: a staged append is not a row to publish.
    pub fn unpublished(&self, limit: usize) -> Vec<edax_event::EventEnvelope> {
        lock(&self.state).committed.outbox.iter().take(limit).cloned().collect()
    }

    /// How many committed outbox rows are waiting: the backlog depth a relay's health check reads,
    /// without paging the rows.
    pub fn unpublished_count(&self) -> usize {
        lock(&self.state).committed.outbox.len()
    }

    /// Mark outbox rows published by envelope id — pruning them from the outbox so it stays bounded.
    pub fn mark_published(&self, ids: Vec<String>) {
        let set: HashSet<String> = ids.into_iter().collect();
        lock(&self.state).committed.outbox.retain(|e| !set.contains(&e.id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{envelope, ids, wire};
    use crate::tx::NO_TX;
    /// An event store holding one committed event at sequence 1 (also sitting in the outbox).
    fn seeded_event_store() -> MemoryEventStore {
        let store = MemoryEventStore::new();
        store.append(NO_TX, "invoice-7", 0, vec![envelope("evt-1", Some(1))]).expect("seed appends");
        store
    }

    #[test]
    fn append_branches() {
        struct Case {
            name: &'static str,
            expected_version: u64,
            events: Vec<edax_event::EventEnvelope>,
            error_code: Option<&'static str>,
            stored: Vec<&'static str>,
            outbox: Vec<&'static str>,
        }
        let cases = [
            Case {
                name: "matching_version_appends_and_enqueues",
                expected_version: 1,
                events: vec![envelope("evt-2", Some(2))],
                error_code: None,
                stored: vec!["evt-1", "evt-2"],
                outbox: vec!["evt-1", "evt-2"],
            },
            Case {
                name: "empty_batch_is_a_noop",
                expected_version: 1,
                events: vec![],
                error_code: None,
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                name: "stale_version_conflicts_and_writes_nothing",
                expected_version: 0,
                events: vec![envelope("evt-2", Some(2))],
                error_code: Some("concurrency: expected 0, stored 1"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                name: "sequence_less_envelope_is_rejected",
                expected_version: 1,
                events: vec![envelope("evt-2", None)],
                error_code: Some("codec: envelope 0 has no sequence"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // The whole batch is rejected on the first sequence-less envelope, not partially applied.
                name: "sequence_less_envelope_rejects_the_whole_batch",
                expected_version: 1,
                events: vec![envelope("evt-2", Some(2)), envelope("evt-3", None)],
                error_code: Some("codec: envelope 1 has no sequence"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // A gap would pass the last-sequence version check yet break the gap-free `load`.
                name: "gapped_sequence_is_rejected",
                expected_version: 1,
                events: vec![envelope("evt-2", Some(3))],
                error_code: Some("codec: envelope 0 has sequence 3, expected 2"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // Reordered within the batch: the last element is right, the first is not.
                name: "reordered_batch_is_rejected",
                expected_version: 1,
                events: vec![envelope("evt-3", Some(3)), envelope("evt-2", Some(2))],
                error_code: Some("codec: envelope 0 has sequence 3, expected 2"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // Sequences are `u64` end to end, so the error names the value the caller sent.
                name: "sequence_past_i64_max_is_rejected_by_its_real_value",
                expected_version: 1,
                events: vec![envelope("evt-2", Some(u64::MAX))],
                error_code: Some("codec: envelope 0 has sequence 18446744073709551615, expected 2"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // A stale writer sees the conflict, not a sequence complaint: reload-and-retry first.
                name: "stale_version_conflicts_before_the_sequence_check",
                expected_version: 0,
                events: vec![envelope("evt-2", None)],
                error_code: Some("concurrency: expected 0, stored 1"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
        ];

        for case in cases {
            let store = seeded_event_store();
            let got = store.append(NO_TX, "invoice-7", case.expected_version, case.events);
            match case.error_code {
                Some(message) => assert_eq!(wire(got), Err(message.to_owned()), "{}", case.name),
                None => assert_eq!(got, Ok(()), "{}", case.name),
            }
            assert_eq!(ids(&store.load(NO_TX, "invoice-7", 0).unwrap()), case.stored, "{} stored", case.name);
            assert_eq!(ids(&store.unpublished(100)), case.outbox, "{} outbox", case.name);
        }
    }

    #[test]
    fn sequence_of_branches() {
        for (name, sequence, want) in [
            ("unsequenced_reads_as_zero", None, 0),
            ("in_range_sequence_is_itself", Some(7), 7),
            // Past `i64::MAX` the sequence is itself: no cast clamps or wraps it.
            ("past_i64_max_is_exact", Some(u64::MAX), u64::MAX),
        ] {
            assert_eq!(sequence_of(&envelope("evt-1", sequence)), want, "{name}");
        }
    }

    #[test]
    fn load_filters_by_sequence_and_aggregate() {
        let store = seeded_event_store();
        store.append(NO_TX, "invoice-7", 1, vec![envelope("evt-2", Some(2))]).expect("appends");

        assert_eq!(ids(&store.load(NO_TX, "invoice-7", 0).unwrap()), ["evt-1", "evt-2"]);
        assert_eq!(ids(&store.load(NO_TX, "invoice-7", 1).unwrap()), ["evt-2"], "after is exclusive");
        assert!(store.load(NO_TX, "invoice-7", 2).unwrap().is_empty(), "nothing past the last sequence");
        assert!(store.load(NO_TX, "invoice-404", 0).unwrap().is_empty(), "unknown aggregate reads empty");

        // The stored envelope survives the round trip field for field, not just by id.
        let loaded = store.load(NO_TX, "invoice-7", 1).unwrap().remove(0);
        assert_eq!(loaded, envelope("evt-2", Some(2)));
    }

    #[test]
    fn unpublished_limit_branches() {
        let store = seeded_event_store();
        store.append(NO_TX, "invoice-7", 1, vec![envelope("evt-2", Some(2)), envelope("evt-3", Some(3))]).expect("appends");

        for (name, limit, expected) in [
            ("zero_limit_reads_nothing", 0usize, vec![]),
            ("limit_below_size_reads_a_prefix", 2, vec!["evt-1", "evt-2"]),
            ("limit_above_size_reads_everything", 10, vec!["evt-1", "evt-2", "evt-3"]),
        ] {
            assert_eq!(ids(&store.unpublished(limit)), expected, "{name}");
        }

        assert_eq!(store.unpublished_count(), 3, "the count is the whole backlog, whatever the page");

        // mark_published prunes only the named rows; an unknown id is a no-op.
        store.mark_published(vec!["evt-1".to_owned(), "evt-404".to_owned()]);
        assert_eq!(ids(&store.unpublished(10)), ["evt-2", "evt-3"]);
        store.mark_published(vec!["evt-404".to_owned()]);
        assert_eq!(ids(&store.unpublished(10)), ["evt-2", "evt-3"]);
        assert_eq!(store.unpublished_count(), 2, "a marked row stops counting");
        let tx = store.transactor().begin();
        store.append(tx, "invoice-7", 3, vec![envelope("evt-4", Some(4))]).expect("stages");
        assert_eq!(store.unpublished_count(), 2, "a staged append is not waiting to publish");
        assert!(store.transactor().rollback(tx));
    }

    #[test]
    fn save_snapshot_branches() {
        // The seeded stream is at version 1; `stage` appends a staged tail under the same token first.
        struct Case {
            name: &'static str,
            /// Snapshot versions saved before the one under test, auto-committed.
            prior: &'static [u64],
            /// Events staged under the transaction first, moving the stream it sees to 3.
            stage: bool,
            version: u64,
            want: Result<(), &'static str>,
            /// The stored snapshot's version afterwards, as the transaction sees it.
            want_version: Option<u64>,
        }
        let cases = [
            Case { name: "first snapshot at the stream's version", prior: &[], stage: false, version: 1, want: Ok(()), want_version: Some(1) },
            Case { name: "a lagging snapshot is fine", prior: &[], stage: false, version: 0, want: Ok(()), want_version: Some(0) },
            Case {
                name: "a snapshot leading the stream is refused",
                prior: &[],
                stage: false,
                version: 2,
                want: Err("codec: snapshot of invoice-7 at version 2 leads its stream at version 1"),
                want_version: None,
            },
            Case {
                name: "the transaction's own appends count",
                prior: &[],
                stage: true,
                version: 3,
                want: Ok(()),
                want_version: Some(3),
            },
            Case {
                name: "an older snapshot never replaces a newer one",
                prior: &[1],
                stage: false,
                version: 0,
                want: Ok(()),
                want_version: Some(1),
            },
            Case { name: "an equal version replaces", prior: &[1], stage: false, version: 1, want: Ok(()), want_version: Some(1) },
        ];
        for c in cases {
            let store = seeded_event_store();
            for &v in c.prior {
                store.save_snapshot(NO_TX, "invoice-7", "Invoice", v, b"prior".to_vec()).expect("prior snapshot");
            }
            let tx = store.transactor().begin();
            if c.stage {
                store.append(tx, "invoice-7", 1, vec![envelope("evt-2", Some(2)), envelope("evt-3", Some(3))]).unwrap();
            }
            let got = store.save_snapshot(tx, "invoice-7", "Invoice", c.version, b"this".to_vec());
            assert_eq!(wire(got), c.want.map_err(str::to_owned), "{}", c.name);
            let seen = store.load_snapshot(tx, "invoice-7").unwrap();
            assert_eq!(seen.as_ref().map(|s| s.version), c.want_version, "{}: version", c.name);
            if c.want.is_ok() && c.want_version == Some(c.version) {
                assert_eq!(seen.map(|s| s.data), Some(b"this".to_vec()), "{}: data", c.name);
            }
            store.transactor().rollback(tx);
        }
    }

    #[test]
    fn concurrent_snapshot_saves_keep_the_higher_version_whichever_commits_last() {
        let store = seeded_event_store();
        store.append(NO_TX, "invoice-7", 1, vec![envelope("evt-2", Some(2))]).unwrap();
        for (name, order) in [("newer commits last", [2u64, 1]), ("older commits last", [1, 2])] {
            let txs: Vec<i64> = order.iter().map(|_| store.transactor().begin()).collect();
            // Saved highest first, committed in `order`.
            for (&tx, &version) in txs.iter().zip([2u64, 1].iter()) {
                store.save_snapshot(tx, "invoice-7", "Invoice", version, version.to_string().into_bytes()).unwrap();
            }
            for &version in &order {
                let tx = if version == 2 { txs[0] } else { txs[1] };
                store.transactor().commit(tx).unwrap();
            }
            let snap = store.load_snapshot(NO_TX, "invoice-7").unwrap().expect("a snapshot");
            assert_eq!((snap.version, snap.data), (2, b"2".to_vec()), "{name}");
        }
        assert!(store.load_snapshot(NO_TX, "invoice-404").unwrap().is_none(), "snapshots do not leak across aggregates");
    }

    #[test]
    fn a_known_envelope_id_is_refused_and_writes_nothing() {
        struct Case {
            name: &'static str,
            /// Staged by another open transaction first.
            other: Option<(&'static str, &'static str)>,
            /// Staged by this transaction first, on another stream.
            own: Option<&'static str>,
            events: Vec<edax_event::EventEnvelope>,
            want: Result<(), &'static str>,
        }
        let cases = [
            Case { name: "a fresh id appends", other: None, own: None, events: vec![envelope("evt-2", Some(2))], want: Ok(()) },
            Case {
                name: "an id already stored on this stream",
                other: None,
                own: None,
                events: vec![envelope("evt-1", Some(2))],
                want: Err("codec: envelope id evt-1 is already stored"),
            },
            Case {
                name: "an id another open transaction staged on another stream",
                other: Some(("order-3", "evt-2")),
                own: None,
                events: vec![envelope("evt-2", Some(2))],
                want: Err("codec: envelope id evt-2 is already stored"),
            },
            Case {
                name: "an id this transaction staged on another stream",
                other: None,
                own: Some("evt-2"),
                events: vec![envelope("evt-2", Some(2))],
                want: Err("codec: envelope id evt-2 is already stored"),
            },
            Case {
                name: "an id repeated inside the batch",
                other: None,
                own: None,
                events: vec![envelope("evt-2", Some(2)), envelope("evt-2", Some(3))],
                want: Err("codec: envelope id evt-2 is already stored"),
            },
        ];
        for c in cases {
            let store = seeded_event_store();
            // An id stored on another aggregate counts too: ids are unique store-wide.
            store.append(NO_TX, "order-9", 0, vec![envelope("evt-9", Some(1))]).unwrap();
            let other = store.transactor().begin();
            if let Some((aggregate_id, id)) = c.other {
                store.append(other, aggregate_id, 0, vec![envelope(id, Some(1))]).unwrap();
            }
            let tx = store.transactor().begin();
            if let Some(id) = c.own {
                store.append(tx, "order-4", 0, vec![envelope(id, Some(1))]).unwrap();
            }
            let got = store.append(tx, "invoice-7", 1, c.events);
            assert_eq!(wire(got), c.want.map_err(str::to_owned), "{}", c.name);
            let stored = store.load(tx, "invoice-7", 0).unwrap();
            let want_len = if c.want.is_ok() { 2 } else { 1 };
            assert_eq!(stored.len(), want_len, "{}: a refused batch stages nothing", c.name);
            store.transactor().rollback(tx);
            store.transactor().rollback(other);
        }
        let store = seeded_event_store();
        assert_eq!(
            wire(store.append(NO_TX, "order-9", 0, vec![envelope("evt-1", Some(1))])),
            Err("codec: envelope id evt-1 is already stored".to_owned()),
            "an id stored on another aggregate"
        );
    }

    #[test]
    fn mark_published_never_prunes_another_aggregates_row() {
        // The reviewer's E4: two aggregates' events under one host-chosen id. The second append is
        // refused, so publishing the first and marking its id cannot drop an unpublished row.
        let store = MemoryEventStore::new();
        store.append(NO_TX, "invoice-7", 0, vec![envelope("dup", Some(1))]).unwrap();
        assert!(store.append(NO_TX, "invoice-8", 0, vec![envelope("dup", Some(1))]).is_err());
        store.append(NO_TX, "invoice-8", 0, vec![envelope("evt-8", Some(1))]).unwrap();
        let first = store.unpublished(1);
        store.mark_published(first.iter().map(|e| e.id.clone()).collect());
        assert_eq!(ids(&store.unpublished(10)), ["evt-8"], "the other aggregate's row is still waiting");
    }
}
