//! The event store view: the per-aggregate log, its snapshots and the transactional outbox.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error};
use edax_event::{check_snapshot, EventDraft, EventMessage};

use crate::tx::{held_by_other, store_view, Committed, MemoryState, MemoryTransactor, Tx, WriteSet};

/// An event's sequence, with an unsequenced one reading as 0. The stream's version space is the
/// kernel's `u64`, so no conversion can clamp or wrap it.
pub(crate) fn sequence_of(event: &EventMessage) -> u64 {
    event.sequence.unwrap_or(0)
}

impl WriteSet {
    /// The version another writer of `aggregate_id` would commit, if this set stages a tail for it.
    pub(crate) fn stream_version(&self, aggregate_id: &str) -> Option<u64> {
        self.streams
            .get(aggregate_id)
            .and_then(|tail| tail.last())
            .map(sequence_of)
    }
}

impl Committed {
    /// Merge a transaction's appended tails, their outbox rows and its snapshots.
    pub(crate) fn apply_events(
        &mut self,
        streams: HashMap<String, Vec<EventMessage>>,
        outbox: Vec<EventMessage>,
        snapshots: HashMap<String, Snapshot>,
    ) {
        for (aggregate_id, tail) in streams {
            self.event_ids.extend(tail.iter().map(|e| e.id.clone()));
            self.events.entry(aggregate_id).or_default().extend(tail);
        }
        self.event_outbox.apply(outbox);
        for (aggregate_id, snapshot) in snapshots {
            // Snapshots are not held keys, so two transactions may both have saved one: the higher
            // version wins whichever commits last.
            if snapshot.supersedes(self.snapshots.get(&aggregate_id)) {
                self.snapshots.insert(aggregate_id, snapshot);
            }
        }
    }
}

/// Refuse a batch carrying an event id the store already holds, another open transaction or this
/// one has staged, or the batch repeats. Deterministic, so `codec`.
fn refuse_known_ids(
    committed: &Committed,
    others: &HashMap<Tx, WriteSet>,
    own: &WriteSet,
    drafts: &[EventDraft],
) -> Result<(), Error> {
    // Every staged id once, then the batch's own: a batch id already in the set is a repeat.
    let mut known: HashSet<&str> = std::iter::once(own)
        .chain(others.values())
        .flat_map(|set| set.streams.values().flatten())
        .map(|e| e.id.as_str())
        .collect();
    for draft in drafts {
        if committed.event_ids.contains(&draft.id) || !known.insert(draft.id.as_str()) {
            return Err(Error::codec(format_args!("event id {} is already stored", draft.id)));
        }
    }
    Ok(())
}

/// A stored aggregate snapshot: the host-marshaled state plus its type and version.
#[derive(Clone)]
pub struct Snapshot {
    /// The aggregate type the host's codec names, so a load ignores another type's snapshot.
    pub type_name: String,
    /// The aggregate version the snapshot was taken at.
    pub version: u64,
    /// The state as the host marshaled it; this store never reads it.
    pub data: Vec<u8>,
}

impl Snapshot {
    /// Whether this snapshot replaces `stored`, the one already saved: the higher version wins and an
    /// equal one replaces it, so a snapshot never moves back. One rule for a save and for a commit.
    pub(crate) fn supersedes(&self, stored: Option<&Snapshot>) -> bool {
        stored.is_none_or(|stored| stored.version <= self.version)
    }
}

/// In-memory event store for development and tests: a per-aggregate event log with optimistic
/// concurrency on the sequence, one snapshot per aggregate, and a transactional outbox that holds
/// only unpublished rows. Events are held as the kernel's own structured [`EventMessage`], with no
/// serialization at the binding. The store stamps each event's `recorded_at` as it appends. Appends and
/// snapshots under an open token are staged until the transactor commits; the outbox is read by the
/// relay outside any transaction and holds committed rows only. Not for production: a durable store
/// appends events and enqueues the outbox in one database transaction.
#[derive(Clone)]
pub struct MemoryEventStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryEventStore, event_store);

impl MemoryEventStore {
    /// Events of `aggregate_id` whose sequence is greater than `after`, including the ones `tx`
    /// itself appended. `Err` only if `tx` names no open transaction.
    pub fn load(&self, tx: Tx, aggregate_id: &str, after: u64) -> Result<Vec<EventMessage>, Error> {
        lock(&self.state).read(tx, |committed, own| {
            let committed_stream = committed.events.get(aggregate_id).into_iter().flatten();
            let staged_tail = own.and_then(|set| set.streams.get(aggregate_id)).into_iter().flatten();
            committed_stream
                .chain(staged_tail)
                .filter(|e| sequence_of(e) > after)
                .cloned()
                .collect()
        })
    }

    /// Append `drafts`, stamped at `recorded_at`, and enqueue the same onto the outbox, atomically.
    /// Returns the stored events. Under an open token they are staged: the caller holds them before
    /// they commit, and a rollback discards them.
    ///
    /// `recorded_at` is the store's clock reading, in unix nanoseconds. The caller passes it because a
    /// binding cannot hand this store a clock to call. A durable store reads its own clock instead.
    ///
    /// The stream's last sequence as `tx` sees it must equal `expected_version`, else a conflict error
    /// is returned. The batch must carry sequences `expected_version + 1, + 2, ...` in order, and every
    /// event id must be new to the whole store (stored, staged by any open transaction, or earlier in
    /// the batch), else a codec error is returned and nothing is written. A stream another open
    /// transaction has appended to is held until that transaction ends: the conflict reports the
    /// committed version, not the holder's, because the holder may yet roll back.
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
        tx: Tx,
        aggregate_id: &str,
        expected_version: u64,
        drafts: Vec<EventDraft>,
        recorded_at: i64,
    ) -> Result<Vec<EventMessage>, Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            // The stale-writer conflict is checked first: it is the documented reload-and-retry signal,
            // and a stale caller's sequences are wrong by construction.
            // A held stream reports the committed version: the holder's own may yet roll back.
            let stored = committed
                .events
                .get(aggregate_id)
                .and_then(|s| s.last())
                .map_or(0, sequence_of);
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
            // A `codec` error (terminal), not `store`: the same batch fails identically every time, so a
            // transient code would have a host retry its whole budget and then redeliver it forever.
            let sequences: Vec<Option<u64>> = drafts.iter().map(|d| d.sequence).collect();
            edax_event::check_batch(expected_version, &sequences)?;
            refuse_known_ids(committed, others, own, &drafts)?;
            // An empty batch stages no tail, so it holds nothing: it was a version check only.
            if drafts.is_empty() {
                return Ok(Vec::new());
            }
            let events: Vec<EventMessage> = drafts.into_iter().map(|d| d.record(recorded_at)).collect();
            own.streams
                .entry(aggregate_id.to_owned())
                .or_default()
                .extend(events.iter().cloned());
            own.event_outbox.extend(events.iter().cloned());
            Ok(events)
        })
    }

    /// The snapshot for `aggregate_id` as `tx` sees it, or `None`. `Err` only if `tx` names no open
    /// transaction.
    pub fn load_snapshot(&self, tx: Tx, aggregate_id: &str) -> Result<Option<Snapshot>, Error> {
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
    pub fn save_snapshot(
        &self,
        tx: Tx,
        aggregate_id: &str,
        type_name: &str,
        version: u64,
        data: Vec<u8>,
    ) -> Result<(), Error> {
        let snapshot = Snapshot {
            type_name: type_name.to_owned(),
            version,
            data,
        };
        lock(&self.state).write(tx, |committed, _, own| {
            let stream = own
                .stream_version(aggregate_id)
                .or_else(|| {
                    committed
                        .events
                        .get(aggregate_id)
                        .and_then(|s| s.last())
                        .map(sequence_of)
                })
                .unwrap_or(0);
            check_snapshot(aggregate_id, version, stream)?;
            let current = own
                .snapshots
                .get(aggregate_id)
                .or_else(|| committed.snapshots.get(aggregate_id));
            if snapshot.supersedes(current) {
                own.snapshots.insert(aggregate_id.to_owned(), snapshot);
            }
            Ok(())
        })
    }

    /// Committed outbox events not yet published, capped at `limit`. The outbox holds only
    /// unpublished rows (published ones are pruned by [`mark_published`](Self::mark_published)), so
    /// this is a bounded prefix scan rather than a filter over an ever-growing log. The relay reads
    /// outside any transaction, so there is no token: a staged append is not a row to publish.
    pub fn unpublished(&self, limit: usize) -> Vec<EventMessage> {
        lock(&self.state).committed.event_outbox.page(limit)
    }

    /// How many committed outbox rows are waiting: the backlog depth a relay's health check reads,
    /// without paging the rows.
    pub fn unpublished_count(&self) -> usize {
        lock(&self.state).committed.event_outbox.count()
    }

    /// Mark outbox rows published by event id — pruning them from the outbox so it stays bounded.
    pub fn mark_published(&self, ids: &[String]) {
        lock(&self.state).committed.event_outbox.prune(ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{draft, event, ids, wire, RECORDED_AT};
    use crate::tx::Tx;
    /// An event store holding one committed event at sequence 1 (also sitting in the outbox).
    fn seeded_event_store() -> MemoryEventStore {
        let store = MemoryEventStore::new();
        store
            .append(
                Tx::auto_commit(),
                "invoice-7",
                0,
                vec![draft("evt-1", Some(1))],
                RECORDED_AT,
            )
            .expect("seed appends");
        store
    }

    #[test]
    fn append_branches() {
        struct Case {
            name: &'static str,
            expected_version: u64,
            events: Vec<EventDraft>,
            error_code: Option<&'static str>,
            stored: Vec<&'static str>,
            outbox: Vec<&'static str>,
        }
        let cases = [
            Case {
                name: "matching_version_appends_and_enqueues",
                expected_version: 1,
                events: vec![draft("evt-2", Some(2))],
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
                events: vec![draft("evt-2", Some(2))],
                error_code: Some("concurrency: expected 0, stored 1"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                name: "sequence_less_event_is_rejected",
                expected_version: 1,
                events: vec![draft("evt-2", None)],
                error_code: Some("codec: event 0 has no sequence"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // The whole batch is rejected on the first sequence-less event, not partially applied.
                name: "sequence_less_event_rejects_the_whole_batch",
                expected_version: 1,
                events: vec![draft("evt-2", Some(2)), draft("evt-3", None)],
                error_code: Some("codec: event 1 has no sequence"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // A gap would pass the last-sequence version check yet break the gap-free `load`.
                name: "gapped_sequence_is_rejected",
                expected_version: 1,
                events: vec![draft("evt-2", Some(3))],
                error_code: Some("codec: event 0 has sequence 3, expected 2"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // Reordered within the batch: the last element is right, the first is not.
                name: "reordered_batch_is_rejected",
                expected_version: 1,
                events: vec![draft("evt-3", Some(3)), draft("evt-2", Some(2))],
                error_code: Some("codec: event 0 has sequence 3, expected 2"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // Sequences are `u64` end to end, so the error names the value the caller sent.
                name: "sequence_past_i64_max_is_rejected_by_its_real_value",
                expected_version: 1,
                events: vec![draft("evt-2", Some(u64::MAX))],
                error_code: Some("codec: event 0 has sequence 18446744073709551615, expected 2"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
            Case {
                // A stale writer sees the conflict, not a sequence complaint: reload-and-retry first.
                name: "stale_version_conflicts_before_the_sequence_check",
                expected_version: 0,
                events: vec![draft("evt-2", None)],
                error_code: Some("concurrency: expected 0, stored 1"),
                stored: vec!["evt-1"],
                outbox: vec!["evt-1"],
            },
        ];

        for case in cases {
            let store = seeded_event_store();
            // On success the store returns the batch as it stored it: every draft stamped at `RECORDED_AT`.
            let recorded: Vec<EventMessage> = case.events.iter().map(|d| d.clone().record(RECORDED_AT)).collect();
            let got = store.append(
                Tx::auto_commit(),
                "invoice-7",
                case.expected_version,
                case.events,
                RECORDED_AT,
            );
            match case.error_code {
                Some(message) => assert_eq!(wire(got), Err(message.to_owned()), "{}", case.name),
                None => assert_eq!(got, Ok(recorded), "{}", case.name),
            }
            assert_eq!(
                ids(&store.load(Tx::auto_commit(), "invoice-7", 0).unwrap()),
                case.stored,
                "{} stored",
                case.name
            );
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
            assert_eq!(sequence_of(&event("evt-1", sequence)), want, "{name}");
        }
    }

    #[test]
    fn load_filters_by_sequence_and_aggregate() {
        let store = seeded_event_store();
        store
            .append(
                Tx::auto_commit(),
                "invoice-7",
                1,
                vec![draft("evt-2", Some(2))],
                RECORDED_AT,
            )
            .expect("appends");

        assert_eq!(
            ids(&store.load(Tx::auto_commit(), "invoice-7", 0).unwrap()),
            ["evt-1", "evt-2"]
        );
        assert_eq!(
            ids(&store.load(Tx::auto_commit(), "invoice-7", 1).unwrap()),
            ["evt-2"],
            "after is exclusive"
        );
        assert!(
            store.load(Tx::auto_commit(), "invoice-7", 2).unwrap().is_empty(),
            "nothing past the last sequence"
        );
        assert!(
            store.load(Tx::auto_commit(), "invoice-404", 0).unwrap().is_empty(),
            "unknown aggregate reads empty"
        );

        // The stored event survives the round trip field for field, not just by id, and carries the
        // store's clock reading.
        let loaded = store.load(Tx::auto_commit(), "invoice-7", 1).unwrap().remove(0);
        assert_eq!(loaded, event("evt-2", Some(2)));
    }

    #[test]
    fn unpublished_limit_branches() {
        let store = seeded_event_store();
        store
            .append(
                Tx::auto_commit(),
                "invoice-7",
                1,
                vec![draft("evt-2", Some(2)), draft("evt-3", Some(3))],
                RECORDED_AT,
            )
            .expect("appends");

        for (name, limit, expected) in [
            ("zero_limit_reads_nothing", 0usize, vec![]),
            ("limit_below_size_reads_a_prefix", 2, vec!["evt-1", "evt-2"]),
            ("limit_above_size_reads_everything", 10, vec!["evt-1", "evt-2", "evt-3"]),
        ] {
            assert_eq!(ids(&store.unpublished(limit)), expected, "{name}");
        }

        assert_eq!(
            store.unpublished_count(),
            3,
            "the count is the whole backlog, whatever the page"
        );

        // mark_published prunes only the named rows; an unknown id is a no-op.
        store.mark_published(&["evt-1".to_owned(), "evt-404".to_owned()]);
        assert_eq!(ids(&store.unpublished(10)), ["evt-2", "evt-3"]);
        store.mark_published(&["evt-404".to_owned()]);
        assert_eq!(ids(&store.unpublished(10)), ["evt-2", "evt-3"]);
        assert_eq!(store.unpublished_count(), 2, "a marked row stops counting");
        let tx = store.transactor().begin();
        store
            .append(tx, "invoice-7", 3, vec![draft("evt-4", Some(4))], RECORDED_AT)
            .expect("stages");
        assert_eq!(
            store.unpublished_count(),
            2,
            "a staged append is not waiting to publish"
        );
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
            Case {
                name: "first snapshot at the stream's version",
                prior: &[],
                stage: false,
                version: 1,
                want: Ok(()),
                want_version: Some(1),
            },
            Case {
                name: "a lagging snapshot is fine",
                prior: &[],
                stage: false,
                version: 0,
                want: Ok(()),
                want_version: Some(0),
            },
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
            Case {
                name: "an equal version replaces",
                prior: &[1],
                stage: false,
                version: 1,
                want: Ok(()),
                want_version: Some(1),
            },
        ];
        for c in cases {
            let store = seeded_event_store();
            for &v in c.prior {
                store
                    .save_snapshot(Tx::auto_commit(), "invoice-7", "Invoice", v, b"prior".to_vec())
                    .expect("prior snapshot");
            }
            let tx = store.transactor().begin();
            if c.stage {
                store
                    .append(
                        tx,
                        "invoice-7",
                        1,
                        vec![draft("evt-2", Some(2)), draft("evt-3", Some(3))],
                        RECORDED_AT,
                    )
                    .unwrap();
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
        store
            .append(
                Tx::auto_commit(),
                "invoice-7",
                1,
                vec![draft("evt-2", Some(2))],
                RECORDED_AT,
            )
            .unwrap();
        for (name, order) in [("newer commits last", [2u64, 1]), ("older commits last", [1, 2])] {
            let txs: Vec<Tx> = order.iter().map(|_| store.transactor().begin()).collect();
            // Saved highest first, committed in `order`.
            for (&tx, &version) in txs.iter().zip([2u64, 1].iter()) {
                store
                    .save_snapshot(tx, "invoice-7", "Invoice", version, version.to_string().into_bytes())
                    .unwrap();
            }
            for &version in &order {
                let tx = if version == 2 { txs[0] } else { txs[1] };
                store.transactor().commit(tx).unwrap();
            }
            let snap = store
                .load_snapshot(Tx::auto_commit(), "invoice-7")
                .unwrap()
                .expect("a snapshot");
            assert_eq!((snap.version, snap.data), (2, b"2".to_vec()), "{name}");
        }
        assert!(
            store.load_snapshot(Tx::auto_commit(), "invoice-404").unwrap().is_none(),
            "snapshots do not leak across aggregates"
        );
    }

    #[test]
    fn a_known_event_id_is_refused_and_writes_nothing() {
        struct Case {
            name: &'static str,
            /// Staged by another open transaction first.
            other: Option<(&'static str, &'static str)>,
            /// Staged by this transaction first, on another stream.
            own: Option<&'static str>,
            events: Vec<EventDraft>,
            want: Result<(), &'static str>,
        }
        let cases = [
            Case {
                name: "a fresh id appends",
                other: None,
                own: None,
                events: vec![draft("evt-2", Some(2))],
                want: Ok(()),
            },
            Case {
                name: "an id already stored on this stream",
                other: None,
                own: None,
                events: vec![draft("evt-1", Some(2))],
                want: Err("codec: event id evt-1 is already stored"),
            },
            Case {
                name: "an id another open transaction staged on another stream",
                other: Some(("order-3", "evt-2")),
                own: None,
                events: vec![draft("evt-2", Some(2))],
                want: Err("codec: event id evt-2 is already stored"),
            },
            Case {
                name: "an id this transaction staged on another stream",
                other: None,
                own: Some("evt-2"),
                events: vec![draft("evt-2", Some(2))],
                want: Err("codec: event id evt-2 is already stored"),
            },
            Case {
                name: "an id repeated inside the batch",
                other: None,
                own: None,
                events: vec![draft("evt-2", Some(2)), draft("evt-2", Some(3))],
                want: Err("codec: event id evt-2 is already stored"),
            },
        ];
        for c in cases {
            let store = seeded_event_store();
            // An id stored on another aggregate counts too: ids are unique store-wide.
            store
                .append(
                    Tx::auto_commit(),
                    "order-9",
                    0,
                    vec![draft("evt-9", Some(1))],
                    RECORDED_AT,
                )
                .unwrap();
            let other = store.transactor().begin();
            if let Some((aggregate_id, id)) = c.other {
                store
                    .append(other, aggregate_id, 0, vec![draft(id, Some(1))], RECORDED_AT)
                    .unwrap();
            }
            let tx = store.transactor().begin();
            if let Some(id) = c.own {
                store
                    .append(tx, "order-4", 0, vec![draft(id, Some(1))], RECORDED_AT)
                    .unwrap();
            }
            let got = store.append(tx, "invoice-7", 1, c.events, RECORDED_AT).map(|_| ());
            assert_eq!(wire(got), c.want.map_err(str::to_owned), "{}", c.name);
            let stored = store.load(tx, "invoice-7", 0).unwrap();
            let want_len = if c.want.is_ok() { 2 } else { 1 };
            assert_eq!(stored.len(), want_len, "{}: a refused batch stages nothing", c.name);
            store.transactor().rollback(tx);
            store.transactor().rollback(other);
        }
        let store = seeded_event_store();
        assert_eq!(
            wire(store.append(
                Tx::auto_commit(),
                "order-9",
                0,
                vec![draft("evt-1", Some(1))],
                RECORDED_AT
            )),
            Err("codec: event id evt-1 is already stored".to_owned()),
            "an id stored on another aggregate"
        );
    }

    #[test]
    fn mark_published_never_prunes_another_aggregates_row() {
        // Two aggregates' events under one host-chosen id. The second append is refused, so
        // publishing the first and marking its id cannot drop an unpublished row.
        let store = MemoryEventStore::new();
        store
            .append(
                Tx::auto_commit(),
                "invoice-7",
                0,
                vec![draft("dup", Some(1))],
                RECORDED_AT,
            )
            .unwrap();
        assert!(store
            .append(
                Tx::auto_commit(),
                "invoice-8",
                0,
                vec![draft("dup", Some(1))],
                RECORDED_AT
            )
            .is_err());
        store
            .append(
                Tx::auto_commit(),
                "invoice-8",
                0,
                vec![draft("evt-8", Some(1))],
                RECORDED_AT,
            )
            .unwrap();
        let first = store.unpublished(1);
        store.mark_published(&first.iter().map(|e| e.id.clone()).collect::<Vec<_>>());
        assert_eq!(
            ids(&store.unpublished(10)),
            ["evt-8"],
            "the other aggregate's row is still waiting"
        );
    }
}
