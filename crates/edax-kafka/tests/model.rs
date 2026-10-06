//! A model of a Kafka consumer driven over the [`OffsetTracker`]: a partitioned log, one fetch
//! position per partition, a key-ordered dispatcher whose handlers start and finish separately, a
//! committer, and the faults — nack, crash and rebalance — in random order. The tracker is the
//! system under test; everything around it does only what a shell does with its decisions.
//!
//! Invariants, checked after every step and at the end:
//! - a committed watermark never passes an offset whose handler has not succeeded;
//! - per-key order: a handler applies event `s` of a key only once `s - 1` has been applied;
//! - a commit never moves back;
//! - once the faults stop, every offset is settled — handled or dead-lettered — and committed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a model test's panic is the failure report"
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use edax_kafka::{AckDecision, NackDecision, OffsetTracker};
use proptest::prelude::*;

const TOPIC: &str = "t";
const PARTITIONS: usize = 2;
const KEYS: usize = 3;

/// One record: its key and the per-key sequence the producer wrote. A poison record fails
/// terminally every time and is dead-lettered.
#[derive(Clone, Copy, Debug)]
struct Rec {
    key: usize,
    seq: u32,
    poison: bool,
}

#[derive(Clone, Copy, Debug)]
enum Op {
    /// Fetch the next record of a partition, book it, and queue it on its key.
    Fetch(usize),
    /// A key's worker takes the head of its queue: skipped when not live, else its handler starts.
    Start(usize),
    /// A key's running handler finishes: success, a transient failure, or (for poison) dead-letter.
    Finish(usize, bool),
    /// The committer commits everything due.
    Commit,
    /// The consumer dies: queued and running work is lost, and it restarts from the commits.
    Crash,
    /// The partition is revoked and assigned back: drain what runs, commit, reset, refetch.
    Rebalance(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..PARTITIONS).prop_map(Op::Fetch),
        4 => (0..KEYS).prop_map(Op::Start),
        4 => ((0..KEYS), any::<bool>()).prop_map(|(k, ok)| Op::Finish(k, ok)),
        2 => Just(Op::Commit),
        1 => Just(Op::Crash),
        1 => (0..PARTITIONS).prop_map(Op::Rebalance),
    ]
}

/// A delivery as the dispatcher holds it.
#[derive(Clone, Copy, Debug)]
struct Held {
    token: i64,
    partition: usize,
    offset: i64,
    rec: Rec,
}

struct Consumer {
    log: Vec<Vec<Rec>>,
    tracker: OffsetTracker,
    position: Vec<i64>,
    /// The group's commits, which survive a crash.
    committed: Vec<i64>,
    queues: Vec<VecDeque<Held>>,
    running: Vec<Option<Held>>,
    /// Every (partition, offset) whose handler succeeded or that was dead-lettered at least once.
    settled: BTreeSet<(usize, i64)>,
    /// The highest sequence processed per key: handled, or dead-lettered.
    applied: HashMap<usize, u32>,
    dead_letters: Vec<(usize, i64)>,
}

impl Consumer {
    fn new(log: Vec<Vec<Rec>>) -> Self {
        Self {
            log,
            tracker: OffsetTracker::new(),
            position: vec![0; PARTITIONS],
            committed: vec![0; PARTITIONS],
            queues: vec![VecDeque::new(); KEYS],
            running: vec![None; KEYS],
            settled: BTreeSet::new(),
            applied: HashMap::new(),
            dead_letters: Vec::new(),
        }
    }

    fn partition_of(p: usize) -> i32 {
        i32::try_from(p).unwrap()
    }

    fn fetch(&mut self, p: usize) {
        let offset = self.position[p];
        let Some(&rec) = self.log[p].get(usize::try_from(offset).unwrap()) else {
            return;
        };
        let token = self.tracker.book(TOPIC, Self::partition_of(p), offset).unwrap();
        self.position[p] = offset + 1;
        self.queues[rec.key].push_back(Held {
            token,
            partition: p,
            offset,
            rec,
        });
    }

    fn start(&mut self, key: usize) {
        if self.running[key].is_some() {
            return; // one handler per key at a time: the dispatcher's per-key order
        }
        while let Some(held) = self.queues[key].pop_front() {
            if self.tracker.is_live(held.token) {
                self.running[key] = Some(held);
                return;
            }
            // A dead copy: skipped, never run and never settled. Its offset comes back by fetch.
        }
    }

    fn finish(&mut self, key: usize, ok: bool) {
        let Some(held) = self.running[key].take() else { return };
        // A poison record is terminal whatever `ok` says: the sink takes it, then it is acked like a
        // success. Either way the event is done with, so both count for the key's order.
        if held.rec.poison || ok {
            // The side effect lands whether or not the ack is stale: the handler ran.
            let last = self.applied.get(&key).copied().unwrap_or(0);
            assert!(
                held.rec.seq <= last + 1,
                "key {key} processed seq {} with only {last} processed before it: per-key order broke",
                held.rec.seq
            );
            self.applied.insert(key, last.max(held.rec.seq));
            if held.rec.poison {
                self.dead_letters.push((held.partition, held.offset));
            }
            self.settled.insert((held.partition, held.offset));
            let _ = self.tracker.ack(held.token);
        } else if let NackDecision::Seek { offset, .. } = self.tracker.nack(held.token) {
            self.position[held.partition] = offset;
        }
    }

    fn commit(&mut self) {
        for due in self.tracker.due() {
            let p = usize::try_from(due.partition).unwrap();
            assert!(
                due.offset > self.committed[p],
                "a due watermark never moves the commit back"
            );
            for offset in 0..due.offset {
                assert!(
                    self.settled.contains(&(p, offset)),
                    "commit {} on partition {p} passes offset {offset}, never settled",
                    due.offset
                );
            }
            self.committed[p] = due.offset;
            self.tracker.committed(TOPIC, due.partition, due.offset);
        }
    }

    /// The process dies: nothing queued or running survives, and a new tracker starts from the
    /// group's commits. The running handlers may have applied their effect; that is at-least-once.
    fn crash(&mut self) {
        self.tracker = OffsetTracker::new();
        self.queues = vec![VecDeque::new(); KEYS];
        self.running = vec![None; KEYS];
        self.position = self.committed.clone();
    }

    /// Revoke and assign back: what is running finishes (the drain), what is due is committed, the
    /// partition is reset, and the fetch restarts from its commit. Queued copies are retired by the
    /// reset, so the workers skip them.
    fn rebalance(&mut self, p: usize) {
        for key in 0..KEYS {
            if self.running[key].is_some_and(|held| held.partition == p) {
                self.finish(key, true);
            }
        }
        self.commit();
        self.tracker.reset(TOPIC, Self::partition_of(p));
        self.position[p] = self.committed[p];
    }

    /// No more faults: fetch, run and commit until nothing is left.
    fn drain(&mut self) {
        for _ in 0..10_000 {
            for p in 0..PARTITIONS {
                self.fetch(p);
            }
            for key in 0..KEYS {
                self.start(key);
                self.finish(key, true);
            }
            self.commit();
            let done = (0..PARTITIONS).all(|p| self.committed[p] == i64::try_from(self.log[p].len()).unwrap());
            if done && self.queues.iter().all(VecDeque::is_empty) {
                return;
            }
        }
        panic!(
            "the consumer never drained: positions {:?}, commits {:?}",
            self.position, self.committed
        );
    }
}

/// The log: a few events per key, each key on one partition, sequences in publish order.
fn log(spec: &[(usize, bool)]) -> Vec<Vec<Rec>> {
    let mut log = vec![Vec::new(); PARTITIONS];
    let mut seq: HashMap<usize, u32> = HashMap::new();
    for &(key, poison) in spec {
        let s = seq.entry(key).or_insert(0);
        *s += 1;
        log[key % PARTITIONS].push(Rec { key, seq: *s, poison });
    }
    log
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn the_tracker_keeps_order_and_never_commits_past_unsettled_work(
        spec in prop::collection::vec(((0..KEYS), prop::bool::weighted(0.1)), 1..12),
        ops in prop::collection::vec(op(), 0..80),
    ) {
        let mut consumer = Consumer::new(log(&spec));
        for op in ops {
            match op {
                Op::Fetch(p) => consumer.fetch(p),
                Op::Start(key) => consumer.start(key),
                Op::Finish(key, ok) => consumer.finish(key, ok),
                Op::Commit => consumer.commit(),
                Op::Crash => consumer.crash(),
                Op::Rebalance(p) => consumer.rebalance(p),
            }
        }
        consumer.drain();
        // Every offset settled, and the group committed past all of them.
        let mut want: BTreeSet<(usize, i64)> = BTreeSet::new();
        for (p, records) in consumer.log.iter().enumerate() {
            for offset in 0..records.len() {
                want.insert((p, i64::try_from(offset).unwrap()));
            }
        }
        prop_assert_eq!(&consumer.settled, &want);
        // Every key reached its last event, so no event of any key was skipped for good.
        let mut last: BTreeMap<usize, u32> = BTreeMap::new();
        for records in &consumer.log {
            for rec in records {
                last.insert(rec.key, rec.seq);
            }
        }
        for (key, seq) in last {
            prop_assert!(consumer.applied.get(&key).copied().unwrap_or(0) >= seq);
        }
        // A poison record is dead-lettered on each delivery that reaches it; dedup is by coordinates.
        let poison: BTreeSet<(usize, i64)> = consumer
            .log
            .iter()
            .enumerate()
            .flat_map(|(p, records)| {
                records.iter().enumerate().filter(|(_, rec)| rec.poison).map(move |(o, _)| (p, i64::try_from(o).unwrap()))
            })
            .collect();
        let dead: BTreeSet<(usize, i64)> = consumer.dead_letters.iter().copied().collect();
        prop_assert_eq!(dead, poison);
    }
}

/// Step by step: seq 1 fails, so the queued seq 2 copy must not run.
#[test]
fn a_nack_keeps_a_keys_later_event_behind_the_redelivered_earlier_one() {
    let mut consumer = Consumer::new(log(&[(0, false), (0, false)]));
    consumer.fetch(0);
    consumer.fetch(0);
    consumer.start(0);
    consumer.finish(0, false); // seq 1 fails transiently: nack rewinds to offset 0
    consumer.start(0); // the queued seq 2 copy is dead: skipped
    assert!(consumer.running[0].is_none(), "the stale seq 2 copy must not run");
    assert!(consumer.applied.is_empty());
    consumer.drain();
    assert_eq!(consumer.applied.get(&0), Some(&2));
    let tracker = OffsetTracker::new();
    let token = tracker.book(TOPIC, 0, 0).unwrap();
    assert_eq!(tracker.ack(token), AckDecision::Settled);
}
