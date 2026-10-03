//! Model test of the saga outbox: two workers folding one correlation with stale loads, crashes between
//! the fold and the commit, a relay that crashes after publishing and before marking, publish failures,
//! late redeliveries after completion, and redeliveries of events whose step already committed (a lost
//! ack, a crash replaying everything above the committed offset), replayed in random order. After every
//! step and at the end it checks the outbox's promises:
//!
//! - a command is sent only if the step that decided it committed: a fold that lost the version race, or
//!   whose unit of work rolled back, sends nothing;
//! - every committed command is sent eventually, and the consumer, deduplicating by command id, applies
//!   each exactly once (effectively-once over an at-least-once relay);
//! - nothing commits for a correlation after it completed: a late event never resurrects it;
//! - no event's step commits twice: a redelivered event the instance already folded is skipped.
//!
//! Run a longer sweep with `PROPTEST_CASES=2000 cargo test -p edax --test saga_model`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "an integration test's helpers panic as its failure report, like the tests that call them"
)]

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use edax::{
    Error, JsonRegistryCodec, MemorySagaStore, MemoryTx, Outbox, Pacing, Relay, Saga, SagaCommand, SagaInstance, SagaRunner,
    SagaStore, Transactor,
};
use edax_event::{Event, EventEnvelope};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Did {
    step: u32,
    event: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
enum CountCommand {
    Did(Did),
}

/// Counts the steps of one workflow: every event emits a command naming the step it made, and `Finish`
/// completes the instance.
struct CountSaga;

impl Saga for CountSaga {
    type State = u32;
    type Command = CountCommand;

    fn correlation_id(&self, _: &EventEnvelope) -> Option<String> {
        Some("flow-1".to_owned())
    }

    fn handle(&self, state: Option<u32>, env: &EventEnvelope) -> (Option<u32>, Vec<CountCommand>) {
        let step = state.unwrap_or(0) + 1;
        let did = CountCommand::Did(Did { step, event: env.id.clone() });
        match env.event.name.as_str() {
            "Finish" => (None, vec![did]),
            _ => (Some(step), vec![did]),
        }
    }
}

/// Event `id` of the flow, from an aggregate of its own: two workers may fold two of them at once, as
/// they may only for events of different aggregates (a dispatcher serializes one aggregate's events).
fn event(id: usize, name: &str) -> EventEnvelope {
    EventEnvelope {
        id: format!("evt-{id}"),
        aggregate_id: format!("agg-{id}"),
        event: Event { name: name.to_owned(), version: 1, data: Vec::new() },
        sequence: Some(1),
        namespace: Default::default(),
        metadata: Default::default(),
        timestamp: 0,
    }
}

/// The real store, except that `load` answers what one worker read earlier: a stale reader, so the
/// write that follows races whatever committed since.
struct StaleRead {
    inner: MemorySagaStore<u32>,
    loaded: Option<SagaInstance<u32>>,
}

impl Outbox<SagaCommand> for StaleRead {
    fn unpublished(&self, limit: usize) -> Result<Vec<SagaCommand>, Error> {
        self.inner.unpublished(limit)
    }
    fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
        self.inner.mark_published(ids)
    }
}

impl SagaStore for StaleRead {
    type State = u32;
    type Tx = MemoryTx;

    fn load(&self, _: &mut MemoryTx, _: &str) -> Result<Option<SagaInstance<u32>>, Error> {
        Ok(self.loaded.clone())
    }
    fn save(&self, tx: &mut MemoryTx, id: &str, state: &u32, version: u64, folded: &str, commands: &[SagaCommand]) -> Result<u64, Error> {
        self.inner.save(tx, id, state, version, folded, commands)
    }
    fn complete(&self, tx: &mut MemoryTx, id: &str, version: u64, commands: &[SagaCommand]) -> Result<u64, Error> {
        self.inner.complete(tx, id, version, commands)
    }
}

/// The outbox, whose `mark_published` fails while `crash` is set: the relay died after it sent.
struct CrashingMark {
    inner: MemorySagaStore<u32>,
    crash: Cell<bool>,
}

impl Outbox<SagaCommand> for CrashingMark {
    fn unpublished(&self, limit: usize) -> Result<Vec<SagaCommand>, Error> {
        self.inner.unpublished(limit)
    }
    fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
        if self.crash.get() {
            return Err(Error::store("relay crashed before the mark"));
        }
        self.inner.mark_published(ids)
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// Worker `w` takes the next pending event and reads the instance.
    Load(usize),
    /// Worker `w` folds and writes; `crash` rolls its unit of work back instead of committing.
    Write { w: usize, crash: bool },
    /// The relay drains; `fail_at` makes that row's publish fail, `crash_mark` loses the mark.
    Drain { fail_at: Option<u8>, crash_mark: bool },
    /// The broker hands over again every committed event from the `from`-th on, in order: one lost ack
    /// when it names the last, a crash above the committed offset when it names an earlier one.
    Redeliver { from: usize },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..2usize).prop_map(Op::Load),
        (0..2usize, prop::bool::weighted(0.2)).prop_map(|(w, crash)| Op::Write { w, crash }),
        (prop::option::weighted(0.2, 0..3u8), prop::bool::weighted(0.2)).prop_map(|(fail_at, crash_mark)| Op::Drain { fail_at, crash_mark }),
        (0..4usize).prop_map(|from| Op::Redeliver { from }),
    ]
}

/// One worker's view: the event it took and what it read, until it writes.
#[derive(Default)]
struct Worker {
    taken: Option<(EventEnvelope, Option<SagaInstance<u32>>)>,
}

struct World {
    store: MemorySagaStore<u32>,
    pending: VecDeque<EventEnvelope>,
    workers: [Worker; 2],
    /// Command ids of the steps that committed.
    committed: BTreeSet<String>,
    /// How often the relay sent each command id, before the consumer's dedup.
    sends: BTreeMap<String, u32>,
    /// How many re-sends each id is allowed: one per drain that sent it and then lost its mark.
    resends_allowed: BTreeMap<String, u32>,
    /// Commits seen after the instance completed: must stay zero.
    after_completion: u32,
    /// The events whose step committed, in commit order.
    landed_events: Vec<String>,
    /// The envelopes of those events, for the broker to hand over again.
    landed_envelopes: Vec<EventEnvelope>,
    /// The step number each committed command carries: a fold that read stale state and still landed
    /// would repeat one.
    steps: Vec<u32>,
}

impl World {
    fn new() -> Self {
        let mut pending: VecDeque<EventEnvelope> = ["Step", "Step", "Finish"].iter().enumerate().map(|(i, n)| event(i, n)).collect();
        // A late event, delivered after `Finish`: it must find the tombstone.
        pending.push_back(event(9, "Step"));
        Self {
            store: MemorySagaStore::new(),
            pending,
            workers: Default::default(),
            committed: BTreeSet::new(),
            sends: BTreeMap::new(),
            resends_allowed: BTreeMap::new(),
            after_completion: 0,
            landed_events: Vec::new(),
            landed_envelopes: Vec::new(),
            steps: Vec::new(),
        }
    }

    fn unsent(&self) -> BTreeSet<String> {
        self.store.unpublished(1000).unwrap().into_iter().map(|c| c.id).collect()
    }

    fn completed(&self) -> bool {
        matches!(self.store.load(&mut MemoryTx::auto_commit(), "flow-1").unwrap(), Some(SagaInstance::Completed { .. }))
    }

    fn load(&mut self, w: usize) {
        if self.workers[w].taken.is_some() {
            return;
        }
        let Some(env) = self.pending.pop_front() else { return };
        let loaded = self.store.load(&mut MemoryTx::auto_commit(), "flow-1").unwrap();
        self.workers[w].taken = Some((env, loaded));
    }

    fn write(&mut self, w: usize, crash: bool) {
        let Some((env, loaded)) = self.workers[w].taken.take() else { return };
        let was_completed = self.completed();
        let before = self.unsent();
        let runner = SagaRunner::new(CountSaga, StaleRead { inner: self.store.clone(), loaded }, JsonRegistryCodec::new(1).unwrap());
        let tr = self.store.transactor();
        let mut tx = tr.begin().unwrap();
        let out = runner.run(&mut tx, &env);
        if out.is_err() || crash {
            // A conflict or a crash: nothing lands, and the broker redelivers the event.
            tr.rollback(tx);
            self.pending.push_front(env);
            return;
        }
        tr.commit(tx).unwrap();
        let landed: BTreeSet<String> = self.unsent().difference(&before).cloned().collect();
        if was_completed && !landed.is_empty() {
            self.after_completion += 1;
        }
        if !landed.is_empty() {
            self.landed_events.push(env.id.clone());
            self.landed_envelopes.push(env.clone());
        }
        for row in self.store.unpublished(1000).unwrap().into_iter().filter(|row| landed.contains(&row.id)) {
            let CountCommand::Did(did) = runner.command(&row).unwrap();
            self.steps.push(did.step);
        }
        self.committed.extend(landed);
    }

    fn drain(&mut self, fail_at: Option<u8>, crash_mark: bool) -> Result<(), TestCaseError> {
        let outbox = CrashingMark { inner: self.store.clone(), crash: Cell::new(crash_mark) };
        let relay = Relay::<_, SagaCommand>::new(outbox).pacing(Pacing::instant());
        let mut index = 0u8;
        let mut sent: Vec<String> = Vec::new();
        let _ = relay.drain(10, |row| {
            let at = index;
            index += 1;
            if fail_at == Some(at) {
                return Err(Error::store("broker unavailable").into());
            }
            sent.push(row.id.clone());
            Ok(())
        });
        for id in sent {
            prop_assert!(self.committed.contains(&id), "sent {id}, which no committed step decided");
            let sends = self.sends.entry(id.clone()).or_insert(0);
            *sends += 1;
            let allowed = self.resends_allowed.get(&id).copied().unwrap_or(0);
            prop_assert!(*sends <= 1 + allowed, "{id} was sent {sends} times with {allowed} lost marks");
            if crash_mark {
                *self.resends_allowed.entry(id).or_insert(0) += 1;
            }
        }
        Ok(())
    }

    fn redeliver(&mut self, from: usize) {
        for env in self.landed_envelopes.iter().skip(from).rev() {
            self.pending.push_front(env.clone());
        }
    }

    /// Redeliver every pending event with fresh reads and drain with no faults until nothing is left.
    fn settle(&mut self) -> Result<(), TestCaseError> {
        for w in 0..2 {
            if let Some((env, _)) = self.workers[w].taken.take() {
                self.pending.push_front(env);
            }
        }
        while !self.pending.is_empty() {
            self.load(0);
            self.write(0, false);
        }
        for _ in 0..100 {
            if self.unsent().is_empty() {
                break;
            }
            self.drain(None, false)?;
        }
        Ok(())
    }
}

fn check(history: Vec<Op>) -> Result<(), TestCaseError> {
    let mut world = World::new();
    for op in history {
        match op {
            Op::Load(w) => world.load(w),
            Op::Write { w, crash } => world.write(w, crash),
            Op::Drain { fail_at, crash_mark } => world.drain(fail_at, crash_mark)?,
            Op::Redeliver { from } => world.redeliver(from),
        }
        prop_assert_eq!(world.after_completion, 0, "a commit landed after the instance completed");
    }
    world.settle()?;
    prop_assert!(world.unsent().is_empty(), "the outbox drains");
    prop_assert!(world.completed(), "every event was folded once the races settled");
    // The consumer deduplicates by command id, so what it applies is the set of ids sent: exactly the
    // committed ones, each once.
    let applied: BTreeSet<String> = world.sends.keys().cloned().collect();
    prop_assert_eq!(&applied, &world.committed, "every committed command sent, nothing else");
    // One command per committed transition, and no event's step committed twice.
    let events: BTreeSet<&String> = world.landed_events.iter().collect();
    prop_assert_eq!(events.len(), world.landed_events.len(), "an event's step committed twice");
    prop_assert_eq!(world.committed.len(), world.landed_events.len());
    // Every fold read the state the previous commit left: the steps run 1, 2, 3, ... without a repeat.
    let want: Vec<u32> = (1..=u32::try_from(world.steps.len()).unwrap()).collect();
    prop_assert_eq!(&world.steps, &want, "a stale fold landed");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn the_saga_outbox_sends_each_committed_command_once_and_nothing_else(history in prop::collection::vec(op(), 1..40)) {
        check(history)?;
    }
}

#[test]
fn a_crash_replay_of_committed_steps_is_one_of_the_histories() {
    // Two steps commit, then the broker replays both (the offset commit was lost), and the last one once
    // more: pinned so a shrink never loses it.
    check(vec![
        Op::Load(0),
        Op::Write { w: 0, crash: false },
        Op::Load(0),
        Op::Write { w: 0, crash: false },
        Op::Redeliver { from: 0 },
        Op::Redeliver { from: 1 },
        Op::Load(0),
        Op::Write { w: 0, crash: false },
        Op::Load(1),
        Op::Write { w: 1, crash: false },
        Op::Load(0),
        Op::Write { w: 0, crash: false },
    ])
    .unwrap();
}

#[test]
fn the_reviewers_race_is_one_of_the_histories() {
    // Both workers read the fresh instance, the first commits, the second's stale write conflicts and
    // is redelivered: the E4 interleaving, pinned so a shrink never loses it.
    check(vec![
        Op::Load(0),
        Op::Load(1),
        Op::Write { w: 0, crash: false },
        Op::Write { w: 1, crash: false },
        Op::Drain { fail_at: None, crash_mark: true },
        Op::Drain { fail_at: None, crash_mark: false },
    ])
    .unwrap();
}
