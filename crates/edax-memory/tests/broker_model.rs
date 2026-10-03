//! The memory broker under random faults: two consumer groups, each with a key-ordered dispatcher
//! whose handlers start and finish separately, over a log of several partitions, with nacks and
//! crashes in random order.
//!
//! Invariants: a handler processes event `s` of a key only after `s - 1` (per-key order, per
//! group); once the faults stop, every group processes every message and reports empty — so no
//! commit ever passed a message that was not processed, or a crash would have skipped it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a model test's panic is the failure report"
)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use edax_event::{Event, EventEnvelope};
use edax_memory::{MemoryBroker, PolledDelivery};
use proptest::prelude::*;

const GROUPS: [&str; 2] = ["projection", "saga"];
const KEYS: usize = 4;

#[derive(Clone, Copy, Debug)]
enum Op {
    Publish(usize),
    Poll(usize),
    Start(usize, usize),
    Finish(usize, usize, bool),
    Crash(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..KEYS).prop_map(Op::Publish),
        4 => (0..GROUPS.len()).prop_map(Op::Poll),
        4 => ((0..GROUPS.len()), (0..KEYS)).prop_map(|(g, k)| Op::Start(g, k)),
        4 => ((0..GROUPS.len()), (0..KEYS), prop::bool::weighted(0.8)).prop_map(|(g, k, ok)| Op::Finish(g, k, ok)),
        1 => (0..GROUPS.len()).prop_map(Op::Crash),
    ]
}

fn envelope(key: usize, seq: u64) -> EventEnvelope {
    EventEnvelope {
        id: format!("k{key}-{seq}"),
        aggregate_id: format!("k{key}"),
        event: Event { name: "Tick".to_owned(), version: 1, data: Vec::new() },
        sequence: Some(seq),
        namespace: BTreeMap::new(),
        metadata: BTreeMap::new(),
        timestamp: 0,
    }
}

/// One group's consumer: a FIFO per key and at most one running handler per key.
struct Dispatcher {
    group: &'static str,
    queues: Vec<VecDeque<PolledDelivery>>,
    running: Vec<Option<PolledDelivery>>,
    processed: HashMap<String, u64>,
}

impl Dispatcher {
    fn new(group: &'static str) -> Self {
        Self { group, queues: vec![VecDeque::new(); KEYS], running: vec![None; KEYS], processed: HashMap::new() }
    }

    fn key(d: &PolledDelivery) -> usize {
        d.envelope.aggregate_id[1..].parse().unwrap()
    }

    fn poll(&mut self, broker: &MemoryBroker) -> bool {
        match broker.try_poll(self.group).unwrap() {
            Some(d) => {
                let key = Self::key(&d);
                self.queues[key].push_back(d);
                true
            }
            None => false,
        }
    }

    fn start(&mut self, broker: &MemoryBroker, key: usize) {
        if self.running[key].is_some() {
            return;
        }
        while let Some(d) = self.queues[key].pop_front() {
            if broker.is_live(self.group, d.token) {
                self.running[key] = Some(d);
                return;
            }
        }
    }

    fn finish(&mut self, broker: &MemoryBroker, key: usize, ok: bool) {
        let Some(d) = self.running[key].take() else { return };
        if !ok {
            let _ = broker.nack(self.group, d.token);
            return;
        }
        let seq = d.envelope.sequence.unwrap();
        let last = self.processed.get(&d.envelope.aggregate_id).copied().unwrap_or(0);
        assert!(
            seq <= last + 1,
            "{}: {} processed {seq} with only {last} before it: per-key order broke",
            self.group,
            d.envelope.aggregate_id
        );
        self.processed.insert(d.envelope.aggregate_id.clone(), last.max(seq));
        let _ = broker.ack(self.group, d.token);
    }

    /// The consumer process dies: its queues and running handlers are gone.
    fn crash(&mut self, broker: &MemoryBroker) {
        broker.crash(self.group);
        self.queues = vec![VecDeque::new(); KEYS];
        self.running = vec![None; KEYS];
    }

    fn drain(&mut self, broker: &MemoryBroker) {
        for _ in 0..10_000 {
            while self.poll(broker) {}
            for key in 0..KEYS {
                self.start(broker, key);
                self.finish(broker, key, true);
            }
            if broker.is_empty(self.group) && self.queues.iter().all(VecDeque::is_empty) {
                return;
            }
        }
        panic!("{} never drained", self.group);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn every_group_processes_every_message_in_key_order(
        partitions in 1u32..4,
        ops in prop::collection::vec(op(), 0..120),
    ) {
        let broker = MemoryBroker::with_partitions(partitions).unwrap();
        let mut groups: Vec<Dispatcher> = GROUPS.iter().map(|g| Dispatcher::new(g)).collect();
        let mut published: Vec<u64> = vec![0; KEYS];
        for op in ops {
            match op {
                Op::Publish(key) => {
                    published[key] += 1;
                    broker.publish(envelope(key, published[key]));
                }
                Op::Poll(g) => {
                    groups[g].poll(&broker);
                }
                Op::Start(g, key) => groups[g].start(&broker, key),
                Op::Finish(g, key, ok) => groups[g].finish(&broker, key, ok),
                Op::Crash(g) => groups[g].crash(&broker),
            }
        }
        let want: BTreeSet<(String, u64)> =
            (0..KEYS).filter(|&k| published[k] > 0).map(|k| (format!("k{k}"), published[k])).collect();
        for group in &mut groups {
            group.drain(&broker);
            let got: BTreeSet<(String, u64)> = group.processed.iter().map(|(k, s)| (k.clone(), *s)).collect();
            prop_assert_eq!(&got, &want, "{} processed every key to its last event", group.group);
            prop_assert!(broker.is_empty(group.group));
        }
    }
}
