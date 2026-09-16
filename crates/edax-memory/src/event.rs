//! The event store view: the per-aggregate log, its snapshots and the transactional outbox.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use edax_core::AdapterError;

use crate::tx::{held_by_other, lock, sequence_of, store_view, MemoryState, MemoryTransactor};

/// A stored aggregate snapshot: the host-marshaled state plus its type and version.
#[derive(Clone)]
pub struct Snapshot {
    pub type_name: String,
    pub version: i64,
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
    pub fn load(&self, tx: i64, aggregate_id: &str, after: i64) -> Result<Vec<edax_event::EventEnvelope>, AdapterError> {
        lock(&self.state).read(tx, |committed, own| {
            let committed_stream = committed.events.get(aggregate_id).into_iter().flatten();
            let staged_tail = own.and_then(|set| set.streams.get(aggregate_id)).into_iter().flatten();
            committed_stream.chain(staged_tail).filter(|e| sequence_of(e) > after).cloned().collect()
        })
    }

    /// Append `events` and enqueue the same onto the outbox, atomically. The stream's last sequence
    /// as `tx` sees it must equal `expected_version`, else a conflict error is returned; the batch
    /// must carry sequences `expected_version + 1, + 2, ...` in order, else a codec error is returned
    /// and nothing is written. A stream another open transaction has appended to is held until that
    /// transaction ends: the conflict reports the version it would commit.
    ///
    /// An empty batch is a version check, not a repeatable read: it stages no tail, so it holds
    /// nothing, and another transaction may append to the stream before this token commits. A caller
    /// that needs the version to stay put must stage a write under the token.
    pub fn append(
        &self,
        tx: i64,
        aggregate_id: &str,
        expected_version: i64,
        events: Vec<edax_event::EventEnvelope>,
    ) -> Result<(), AdapterError> {
        lock(&self.state).write(tx, |committed, others, own| {
            // The stale-writer conflict is checked first: it is the documented reload-and-retry signal,
            // and a stale caller's sequences are wrong by construction.
            if let Some(stored) = held_by_other(others, |set| set.stream_version(aggregate_id)) {
                return Err(AdapterError::Concurrency { expected: expected_version, stored });
            }
            let have = own
                .stream_version(aggregate_id)
                .or_else(|| committed.events.get(aggregate_id).and_then(|s| s.last()).map(sequence_of))
                .unwrap_or(0);
            if have != expected_version {
                return Err(AdapterError::Concurrency { expected: expected_version, stored: have });
            }
            // The dense-batch rule is the kernel's, so every store enforces the same one; the vector
            // `testdata/golden/sequence_batch.json` pins it. Whole-batch rejection, nothing written.
            //
            // Tagged `codec:` (terminal), not `store:`: the same batch fails identically every time, so a
            // transient tag would have a host retry its whole budget and then redeliver it forever. The
            // taxonomy's codes encode *transience*, not failure kind, and `codec:` is the
            // deterministic-terminal slot.
            let sequences: Vec<Option<u64>> = events.iter().map(|e| e.sequence).collect();
            edax_event::sequence::check_batch(expected_version, &sequences).map_err(AdapterError::codec)?;
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
    pub fn load_snapshot(&self, tx: i64, aggregate_id: &str) -> Result<Option<Snapshot>, AdapterError> {
        lock(&self.state).read(tx, |committed, own| {
            own.and_then(|set| set.snapshots.get(aggregate_id))
                .or_else(|| committed.snapshots.get(aggregate_id))
                .cloned()
        })
    }

    /// Store a snapshot for `aggregate_id` under `tx`, replacing any prior one. `Err` only if `tx`
    /// names no open transaction.
    ///
    /// Last writer wins: there is no version check and no exclusion, so two transactions may both
    /// save and the later commit keeps its snapshot, even if it is the older state. A snapshot is a
    /// cache of the event log, so a lost write costs a longer replay, never a wrong aggregate.
    pub fn save_snapshot(&self, tx: i64, aggregate_id: &str, type_name: &str, version: i64, data: Vec<u8>) -> Result<(), AdapterError> {
        let snapshot = Snapshot { type_name: type_name.to_owned(), version, data };
        lock(&self.state).write(tx, |_, _, own| {
            own.snapshots.insert(aggregate_id.to_owned(), snapshot);
            Ok(())
        })
    }

    /// Committed outbox events not yet published, capped at `limit`. The outbox holds only
    /// unpublished rows (published ones are pruned by [`mark_published`](Self::mark_published)), so
    /// this is a bounded prefix scan rather than a filter over an ever-growing log. The relay reads
    /// outside any transaction, so there is no token: a staged append is not a row to publish.
    pub fn unpublished(&self, limit: i64) -> Vec<edax_event::EventEnvelope> {
        // Clamp, then convert: `as usize` truncates on a 32-bit target, silently turning a
        // read-everything cap into a short page.
        let limit = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
        lock(&self.state).committed.outbox.iter().take(limit).cloned().collect()
    }

    /// Mark outbox rows published by envelope id — pruning them from the outbox so it stays bounded.
    pub fn mark_published(&self, ids: Vec<String>) {
        let set: HashSet<String> = ids.into_iter().collect();
        lock(&self.state).committed.outbox.retain(|e| !set.contains(&e.id));
    }
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
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
            expected_version: i64,
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
                // Saturating, not wrapping: a wrapping cast would read this as -1 and reject it as
                // "sequence -1", losing the value the caller actually sent.
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
            // A wrapping `as i64` would yield -1, which sorts before every real sequence and makes
            // `load(after)` drop the event instead of returning it.
            ("past_i64_max_saturates", Some(u64::MAX), i64::MAX),
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
            ("negative_limit_reads_nothing", -1i64, vec![]),
            ("zero_limit_reads_nothing", 0, vec![]),
            ("limit_below_size_reads_a_prefix", 2, vec!["evt-1", "evt-2"]),
            ("limit_above_size_reads_everything", 10, vec!["evt-1", "evt-2", "evt-3"]),
        ] {
            assert_eq!(ids(&store.unpublished(limit)), expected, "{name}");
        }

        // mark_published prunes only the named rows; an unknown id is a no-op.
        store.mark_published(vec!["evt-1".to_owned(), "evt-404".to_owned()]);
        assert_eq!(ids(&store.unpublished(10)), ["evt-2", "evt-3"]);
        store.mark_published(vec!["evt-404".to_owned()]);
        assert_eq!(ids(&store.unpublished(10)), ["evt-2", "evt-3"]);
    }

    #[test]
    fn snapshots_are_per_aggregate_and_replaceable() {
        let store = MemoryEventStore::new();
        assert!(store.load_snapshot(NO_TX, "invoice-7").unwrap().is_none(), "absent before any save");

        store.save_snapshot(NO_TX, "invoice-7", "Invoice", 2, br#"{"total":5000}"#.to_vec()).unwrap();
        let snap = store.load_snapshot(NO_TX, "invoice-7").unwrap().expect("present after save");
        assert_eq!((snap.type_name.as_str(), snap.version, snap.data), ("Invoice", 2, br#"{"total":5000}"#.to_vec()));

        store.save_snapshot(NO_TX, "invoice-7", "Invoice", 3, br#"{"total":7500}"#.to_vec()).unwrap();
        let replaced = store.load_snapshot(NO_TX, "invoice-7").unwrap().expect("present after replace");
        assert_eq!((replaced.version, replaced.data), (3, br#"{"total":7500}"#.to_vec()));
        assert!(store.load_snapshot(NO_TX, "invoice-404").unwrap().is_none(), "snapshots do not leak across aggregates");
    }
}
