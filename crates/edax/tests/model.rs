//! Model-based tests: replay random operation histories against the shipped in-memory SDK stores and
//! compare every observable against a trivial reference model (a `Vec`/`HashMap` that obviously has
//! the intended semantics). Where a hand-written test pins one interleaving, these pin the rule over
//! thousands of them, and `proptest` shrinks a failure to a minimal history.
//!
//! Scope is the SDK orchestration — the storage seam, the command bus's idempotency middleware, the
//! dispatcher, the repository, the codecs, the projection seam. Kernel semantics (filter, query
//! ordering, dedup, partition, backoff) are pinned by the golden vectors in `testdata/golden`, not
//! re-derived here.
//!
//! Run a longer sweep with `PROPTEST_CASES=2000 cargo test -p edax --test model`. A failure writes a
//! regression seed to `tests/model.proptest-regressions`, which later runs replay first.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "an integration test's helpers panic as its failure report, like the tests that call them"
)]

mod support;

use support::{draft, Shared};

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use edax::command::{Claim, State};
use edax::Tenancy;
use edax::{
    idempotent, propagate, wrap, Aggregate, AggregateCodec, Command, CommandBus, Delivery, Entity, Error, ErrorCode, EventCodec,
    EventDraft, EventMessage, EventStore, Extensions, HandlerError, IdempotencyStore, JsonCodec, KeyOrderedDispatcher,
    MemoryEventStore, MemoryIdempotencyStore, MemoryProjectionStore, MemoryTx, Metadata, Outbox, Pacing, ProjectionStore, Query,
    Relay, Repository, Root,
};
use edax_cqrs::{claim_key, command_hash, SortKey};
use edax_event::Event;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde::{Deserialize, Serialize};
use serde_json::json;

// --- shared helpers -------------------------------------------------------

fn next_id() -> String {
    static N: AtomicU64 = AtomicU64::new(1);
    format!("e{}", N.fetch_add(1, Ordering::Relaxed))
}

/// Event context varied per append, so the model comparison covers the fields a store could silently
/// drop (extensions, metadata, the time it records) rather than just ids and sequences.
fn stamp(variant: u8) -> (Extensions, Metadata, i64) {
    let mut ext = Extensions::new();
    let mut md = Metadata::new();
    match variant % 3 {
        0 => {}
        1 => {
            ext.insert("traceparent".into(), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into());
            md.insert("tenant".into(), "t1".into());
        }
        _ => {
            ext.insert("traceparent".into(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into());
            ext.insert("http.origin_ip".into(), "203.0.113.7".into());
            // non-ASCII: the field has to survive the store round-trip byte for byte
            md.insert("tenant".into(), "déjà-vu".into());
        }
    }
    (ext, md, variant as i64 * 1000)
}

fn agg_id(agg: u8) -> String {
    format!("agg-{agg}")
}

/// A concurrency conflict carrying the expected/stored versions the CAS rejected — the shape P1, P4,
/// and P6 all assert on a stale write.
fn expect_conflict<T: std::fmt::Debug>(got: Result<T, Error>, expected: u64, stored: u64) -> Result<(), TestCaseError> {
    match got {
        Err(e) => {
            prop_assert_eq!(e, Error::concurrency(expected, stored));
            Ok(())
        }
        other => Err(TestCaseError::fail(format!("expected conflict, got {other:?}"))),
    }
}

/// The text of a dispatch failure, or a test failure — every P2 dispatch arm pulls the handler error
/// out this way before matching on its text.
fn handler_err(got: Result<u8, HandlerError>) -> Result<String, TestCaseError> {
    match got {
        Err(e) => Ok(e.to_string()),
        other => Err(TestCaseError::fail(format!("expected handler error, got {other:?}"))),
    }
}

/// The kernel error a dispatch failed with, or a test failure. Asserts the code, not just the text:
/// `is_terminal` reads the code, and that is what stops a redelivery loop. The bus and the event
/// router report a verdict as the same kernel error.
fn kernel_err(got: Result<u8, HandlerError>) -> Result<Error, TestCaseError> {
    match got {
        Err(e) => e.downcast_ref::<Error>().cloned().ok_or_else(|| TestCaseError::fail(format!("not a kernel error: {e}"))),
        other => Err(TestCaseError::fail(format!("expected handler error, got {other:?}"))),
    }
}

// =========================================================================
// P1 — event store + outbox relay
// =========================================================================

#[derive(Debug, Clone)]
enum StoreOp {
    /// `gap` skews the last draft's sequence by one, so the batch is no longer dense from
    /// `expected + 1` — the shape a store must reject whole, and the one a generator that only ever
    /// builds `expected + 1 + i` can never produce.
    Append { agg: u8, delta: i8, n: u8, ns: u8, gap: bool },
    Load { agg: u8, after: u64 },
    Drain { limit: u8, fail_at: Option<u8> },
}

fn store_op() -> impl Strategy<Value = StoreOp> {
    prop_oneof![
        (0u8..3, -2i8..=2, 0u8..=3, 0u8..3, any::<bool>())
            .prop_map(|(agg, delta, n, ns, gap)| StoreOp::Append { agg, delta, n, ns, gap }),
        (0u8..3, 0u64..9).prop_map(|(agg, after)| StoreOp::Load { agg, after }),
        (0u8..7, proptest::option::of(0u8..6)).prop_map(|(limit, fail_at)| StoreOp::Drain { limit, fail_at }),
    ]
}

/// The reference model: events per aggregate in append order, an outbox FIFO, and the ids the relay
/// has handed to `publish` (with repeats — a failed drain re-publishes its prefix).
#[derive(Default)]
struct StoreModel {
    events: HashMap<u8, Vec<EventMessage>>,
    outbox: Vec<EventMessage>,
    published: Vec<String>,
    failed_drains: usize,
}

/// P1 over any [`EventStore`]: the generic bound *is* the contract — the append CAS, the per-aggregate
/// load window, and the outbox FIFO hold for any backend, not just the in-memory one exercised here.
///
/// `new_store` builds the store over a clock that reads `clock`, so the model knows the time each
/// append is recorded at.
fn check_event_store<S>(new_store: impl FnOnce(Arc<AtomicI64>) -> S, history: Vec<StoreOp>) -> Result<(), TestCaseError>
where
    S: EventStore + Clone,
    S::Tx: Default,
{
    let clock = Arc::new(AtomicI64::new(0));
    let store = new_store(Arc::clone(&clock));
    let relay = Relay::new(store.clone()).pacing(Pacing::instant());
    let mut model = StoreModel::default();
    let mut tx = S::Tx::default(); // the auto-commit handle: the model describes committed state

    for op in history {
        match op {
            StoreOp::Append { agg, delta, n, ns, gap } => {
                let stored = model.events.entry(agg).or_default().len() as u64;
                // Signed delta off the model's own version, so conflicts and clean appends are
                // both common (a uniform `expected` would almost always conflict).
                let expected = (stored as i64 + delta as i64).max(0) as u64;
                let (extensions, metadata, recorded_at) = stamp(ns);
                let mut drafts: Vec<EventDraft> = (0..n as u64)
                    .map(|i| EventDraft {
                        data: vec![agg, i as u8],
                        extensions: extensions.clone(),
                        metadata: metadata.clone(),
                        ..draft(next_id(), &agg_id(agg), "Ticked", expected + 1 + i)
                    })
                    .collect();
                // One draft out of place is enough: a hole at the end for a multi-event batch, a
                // wrong start for a single one. An empty batch has nothing to skew.
                let gapped = gap && !drafts.is_empty();
                if let (true, Some(last)) = (gap, drafts.last_mut()) {
                    last.sequence = last.sequence.map(|s| s + 1);
                }
                let recorded: Vec<EventMessage> = drafts.iter().cloned().map(|d| d.record(recorded_at)).collect();

                clock.store(recorded_at, Ordering::Relaxed);
                let got = store.append(&mut tx, &agg_id(agg), expected, drafts);
                if expected != stored {
                    // The stale-writer conflict is decided before the batch is inspected: a stale
                    // caller's sequences are wrong by construction, so reporting the shape would bury
                    // the reload-and-retry signal.
                    expect_conflict(got, expected, stored)?;
                } else if gapped {
                    // Nothing written: the version check reads only the last stored sequence, so a hole
                    // inside the batch would slip past it and break the gap-free ascending `load`.
                    // Codec (terminal), not Store: the same batch fails identically every retry.
                    prop_assert!(
                        matches!(&got, Err(Error { code: ErrorCode::Codec, message: m }) if m.contains("expected")),
                        "a non-dense batch must be rejected whole and terminally, got {got:?}"
                    );
                } else {
                    // The store records each draft at its clock's reading, and returns what it stored.
                    prop_assert_eq!(got.ok(), Some(recorded.clone()), "clean append rejected or misrecorded");
                    model.events.get_mut(&agg).unwrap().extend(recorded.iter().cloned());
                    model.outbox.extend(recorded);
                }

                // Full event equality, not just ids: extensions, metadata and recorded_at fidelity.
                let want = model.events.get(&agg).cloned().unwrap_or_default();
                prop_assert_eq!(store.load(&mut tx, &agg_id(agg), 0).unwrap(), want.clone());
                let seqs: Vec<Option<u64>> = want.iter().map(|e| e.sequence).collect();
                let dense: Vec<Option<u64>> = (1..=want.len() as u64).map(Some).collect();
                prop_assert_eq!(seqs, dense);
            }

            StoreOp::Load { agg, after } => {
                let want: Vec<EventMessage> = model
                    .events
                    .get(&agg)
                    .map(|es| es.iter().filter(|e| e.sequence.unwrap_or(0) > after).cloned().collect())
                    .unwrap_or_default();
                prop_assert_eq!(store.load(&mut tx, &agg_id(agg), after).unwrap(), want);
            }

            StoreOp::Drain { limit, fail_at } => {
                let limit = limit as usize;
                let batch: Vec<EventMessage> = model.outbox.iter().take(limit).cloned().collect();

                let mut seen: Vec<EventMessage> = Vec::new();
                let got = relay.drain(limit, |env: &EventMessage| {
                    seen.push(env.clone());
                    if fail_at == Some(seen.len() as u8 - 1) {
                        Err(Error::store("publisher unavailable").into())
                    } else {
                        Ok(())
                    }
                });

                let failed = fail_at.is_some_and(|k| (k as usize) < batch.len());
                if failed {
                    let k = fail_at.unwrap() as usize;
                    prop_assert!(
                        matches!(got.as_ref().map_err(|e| e.downcast_ref::<Error>().map(|e| e.code)), Err(Some(ErrorCode::Store))),
                        "drain should surface the publish error"
                    );
                    prop_assert_eq!(&seen, &batch[..=k].to_vec());
                    // The published prefix is marked, and the failed row and everything behind it
                    // stay for the next drain (at-least-once, never a drop, never out of order).
                    model.outbox.drain(..k);
                    prop_assert_eq!(store.unpublished(1000).unwrap(), model.outbox.clone());
                    model.failed_drains += 1;
                } else {
                    prop_assert_eq!(got.ok(), Some(batch.len()));
                    prop_assert_eq!(&seen, &batch);
                    model.outbox.drain(..batch.len());
                    prop_assert_eq!(store.unpublished(1000).unwrap(), model.outbox.clone());
                }
                model.published.extend(seen.iter().map(|e| e.id.clone()));
            }
        }
    }

    // Drain what's left with an infallible publisher: every appended event must have been
    // published at least once, in append order, and at most once per failed drain extra.
    let mut tail: Vec<EventMessage> = Vec::new();
    relay
        .drain(1000, |env: &EventMessage| {
            tail.push(env.clone());
            Ok(())
        })
        .unwrap();
    model.published.extend(tail.iter().map(|e| e.id.clone()));
    prop_assert_eq!(store.unpublished(1000).unwrap(), Vec::new());

    let mut first_seen: Vec<String> = Vec::new();
    for id in &model.published {
        if !first_seen.contains(id) {
            first_seen.push(id.clone());
        }
    }
    // Ids come from a monotonic counter, so sorting by it *is* global append order. The outbox
    // is a FIFO drained by prefixes, so first publication must follow that same order.
    let mut appended: Vec<String> = model.events.values().flatten().map(|e| e.id.clone()).collect();
    appended.sort_by_key(|id| id[1..].parse::<u64>().unwrap());
    prop_assert_eq!(first_seen.clone(), appended);
    for id in &first_seen {
        let n = model.published.iter().filter(|p| *p == id).count();
        prop_assert!(n <= 1 + model.failed_drains, "{id} published {n} times");
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn store_matches_model(history in prop::collection::vec(store_op(), 1..24)) {
        check_event_store(|clock| MemoryEventStore::new().with_clock(move || clock.load(Ordering::Relaxed)), history)?;
    }
}

// =========================================================================
// P2 — command bus: routing, idempotency claim, replay, pending
// =========================================================================

/// Only `p` is on the wire: routing, key and outcome are test-side steering, kept out of the hash
/// so two dispatches of one payload under one key are one command whatever their outcome.
#[derive(Serialize)]
struct Cmd {
    #[serde(skip)]
    kind: u8,
    #[serde(skip)]
    key: u8,
    #[serde(rename = "p")]
    payload: u8,
    #[serde(skip)]
    outcome: u8,
}

impl Command for Cmd {
    fn command_type(&self) -> &str {
        match self.kind {
            0 => "A",
            1 => "B",
            _ => "Z", // never registered: routing must reject it before any middleware runs
        }
    }
}

/// The scope both command types' guards claim under: one key space, so a key reused across types is
/// a payload mismatch, the type name being part of the hash.
const SCOPE: &str = "model";

fn cmd_hash(kind: u8, payload: u8) -> u64 {
    // Hash through the same kernel rule the middleware uses, so the model can never drift from it.
    let cmd = Cmd { kind, key: 0, payload, outcome: 0 };
    command_hash(cmd.command_type(), &cmd).expect("the command hashes")
}

/// The ledger key the guard claims for model key `key`.
fn claimed(key: u8) -> String {
    claim_key(Tenancy::Silo, None, SCOPE, &key.to_string()).unwrap()
}

#[derive(Debug, Clone, PartialEq)]
enum Slot {
    Pending,
    Done(u8),
    Failed(String),
}

#[derive(Debug, Clone)]
enum BusOp {
    Dispatch {
        kind: u8,
        key: u8,
        payload: u8,
        outcome: u8,
    },
    /// A winner took the claim and has not completed — the state a concurrent dispatch must see.
    SeedPending { key: u8, payload: u8 },
    /// The winner gave up (transient failure); the key is free again.
    Release { key: u8 },
}

fn bus_op() -> impl Strategy<Value = BusOp> {
    prop_oneof![
        6 => (0u8..3, 0u8..3, 0u8..3, 0u8..3)
            .prop_map(|(kind, key, payload, outcome)| BusOp::Dispatch { kind, key, payload, outcome }),
        1 => (0u8..3, 0u8..3).prop_map(|(key, payload)| BusOp::SeedPending { key, payload }),
        1 => (0u8..3).prop_map(|key| BusOp::Release { key }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn idempotent_bus_matches_model(history in prop::collection::vec(bus_op(), 1..24)) {
        let memory = MemoryIdempotencyStore::<u8>::new();
        let store: Arc<dyn IdempotencyStore<u8, Tx = MemoryTx> + Send + Sync> = Arc::new(memory.clone());
        let calls = [Arc::new(Shared::<usize>::default()), Arc::new(Shared::<usize>::default())];

        let mut bus = CommandBus::new();
        for (kind, name) in [(0u8, "A"), (1u8, "B")] {
            let counter = Arc::clone(&calls[kind as usize]);
            let guard = idempotent(Arc::clone(&store), SCOPE, Tenancy::Silo, |c: &Cmd| Some(c.key.to_string()));
            let handler: edax::Handler<Cmd, u8> = Box::new(move |cmd: &Cmd| {
                counter.set(counter.get() + 1);
                match cmd.outcome {
                    0 => Ok(cmd.payload + 10 * kind),
                    1 => Err("transient".into()),
                    // a Codec error is the kernel's terminal class: cached, never retried
                    _ => Err(Error::codec("terminal").into()),
                }
            });
            bus.register(name, wrap(handler, vec![guard])).unwrap();
        }

        let mut model: HashMap<u8, (u64, Slot)> = HashMap::new();
        let mut tx = MemoryTx::auto_commit();

        for op in history {
            match op {
                BusOp::Dispatch { kind, key, payload, outcome } => {
                    let hash = cmd_hash(kind, payload);
                    let before: Vec<usize> = calls.iter().map(|c| c.get()).collect();
                    let got = bus.dispatch::<Cmd, u8>(&Cmd { kind, key, payload, outcome });

                    if kind == 2 {
                        // Routing precedes the middleware chain: no claim, no handler, no record.
                        prop_assert_eq!(kernel_err(got)?, Error::new(ErrorCode::HandlerNotFound, "Z"));
                        prop_assert_eq!(calls.iter().map(|c| c.get()).collect::<Vec<_>>(), before);
                        continue;
                    }

                    let prior = model.get(&key).cloned();
                    match prior {
                        // Free key: this dispatch wins the claim and runs the handler exactly once.
                        None => {
                            prop_assert_eq!(calls[kind as usize].get(), before[kind as usize] + 1);
                            match outcome {
                                0 => {
                                    prop_assert_eq!(got.ok(), Some(payload + 10 * kind));
                                    model.insert(key, (hash, Slot::Done(payload + 10 * kind)));
                                }
                                1 => {
                                    // transient: the claim is released, so the key is free again
                                    prop_assert_eq!(handler_err(got)?, "transient");
                                }
                                _ => {
                                    // terminal: cached with its code so a redelivery replays the failure
                                    prop_assert_eq!(kernel_err(got)?, Error::codec("terminal"));
                                    model.insert(key, (hash, Slot::Failed("terminal".to_owned())));
                                }
                            }
                        }

                        // Taken key: the kernel decides, and the handler never runs.
                        Some((prior_hash, slot)) => {
                            prop_assert_eq!(calls.iter().map(|c| c.get()).collect::<Vec<_>>(), before);
                            if prior_hash != hash {
                                prop_assert_eq!(kernel_err(got)?, Error::new(ErrorCode::PayloadMismatch, claimed(key)));
                            } else {
                                match slot {
                                    Slot::Pending => {
                                        prop_assert_eq!(kernel_err(got)?, Error::new(ErrorCode::InProgress, claimed(key)));
                                    }
                                    Slot::Done(v) => prop_assert_eq!(got.ok(), Some(v)),
                                    // The cached failure replays as `cached_failure`, terminal, with the
                                    // original code in its message — so `retry` returns at once and a
                                    // broker stops redelivering a command that already failed.
                                    Slot::Failed(msg) => {
                                        prop_assert_eq!(kernel_err(got)?, Error::new(ErrorCode::CachedFailure, format!("codec: {msg}")))
                                    }
                                }
                            }
                        }
                    }
                }

                BusOp::SeedPending { key, payload } => {
                    let hash = cmd_hash(0, payload);
                    // Another worker's live claim: whatever the key held lapses, and a fresh claim takes it.
                    memory.expire(&claimed(key));
                    let won = store.claim(&mut tx, &claimed(key), hash).unwrap();
                    prop_assert!(matches!(won, Claim::Won(_)), "a lapsed key is free");
                    model.insert(key, (hash, Slot::Pending));
                }

                BusOp::Release { key } => {
                    // The key is freed whoever holds it, as a claim or record TTL running out frees it.
                    memory.expire(&claimed(key));
                    model.remove(&key);
                }
            }
        }

        // Every modeled record is readable back through the seam with the same hash and state.
        for (key, (hash, slot)) in &model {
            match store.claim(&mut tx, &claimed(*key), *hash).unwrap() {
                Claim::Prior(r) => {
                    prop_assert_eq!(r.payload_hash, *hash);
                    match slot {
                        Slot::Pending => prop_assert_eq!(&r.state, &State::Pending),
                        Slot::Done(v) => prop_assert_eq!(&r.state, &State::Done(*v)),
                        Slot::Failed(msg) => {
                            prop_assert_eq!(&r.state, &State::Failed { code: Some(ErrorCode::Codec), message: msg.clone() })
                        }
                    }
                }
                Claim::Won(_) => prop_assert!(false, "key {key} should still hold a record"),
            }
        }
    }
}

// =========================================================================
// P3 — dispatcher: per-aggregate FIFO, settle by outcome
// =========================================================================

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn dispatcher_keeps_per_aggregate_fifo(
        workers in 1usize..=4,
        submits in prop::collection::vec((0u8..4, any::<bool>()), 1..64),
    ) {
        let handled: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let acked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let nacked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&handled);
        // No wall time per nack: `Pacing::instant()` waits out the kernel's schedule in no time.
        let dispatcher = KeyOrderedDispatcher::builder(workers, move |env: &EventMessage| {
            sink.lock().unwrap().push((env.aggregate_id.clone(), env.sequence.unwrap()));
            if env.name == "Fail" {
                Err(Error::store("handler failed").into())
            } else {
                Ok(())
            }
        })
        .pacing(Pacing::instant())
        .build()
        .expect("the dispatcher's workers spawn");

        let mut seq: HashMap<u8, u64> = HashMap::new();
        let mut expected_ack: Vec<String> = Vec::new();
        let mut expected_nack: Vec<String> = Vec::new();
        for (agg, fail) in &submits {
            let n = seq.entry(*agg).or_insert(0);
            *n += 1;
            let name = if *fail { "Fail" } else { "Tick" };
            let env = draft(format!("{}-{}", agg_id(*agg), n), &agg_id(*agg), name, *n).record(0);
            let id = env.id.clone();
            if *fail { expected_nack.push(id.clone()) } else { expected_ack.push(id.clone()) }
            let (a, na) = (Arc::clone(&acked), Arc::clone(&nacked));
            let (ack_id, nack_id) = (id.clone(), id);
            dispatcher.deliver(Delivery::new(
                env,
                move || a.lock().unwrap().push(ack_id),
                move || na.lock().unwrap().push(nack_id),
            ));
        }
        dispatcher.stop();

        let got = handled.lock().unwrap().clone();
        prop_assert_eq!(got.len(), submits.len());
        for (agg, count) in &seq {
            let seqs: Vec<u64> = got
                .iter()
                .filter(|(a, _)| *a == agg_id(*agg))
                .map(|(_, s)| *s)
                .collect();
            // Same aggregate -> same worker -> submitted order, whatever the worker count.
            prop_assert_eq!(seqs, (1..=*count).collect::<Vec<u64>>());
        }

        let (mut a, mut n) = (acked.lock().unwrap().clone(), nacked.lock().unwrap().clone());
        a.sort();
        n.sort();
        expected_ack.sort();
        expected_nack.sort();
        prop_assert_eq!(a, expected_ack);
        prop_assert_eq!(n, expected_nack);
    }
}

// =========================================================================
// P4 — Repository: save/load against a pure fold
// =========================================================================

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Tick(u8);

struct Counter {
    root: Root<Tick>,
    n: i64,
}

impl Aggregate for Counter {
    type Event = Tick;
    fn root(&self) -> &Root<Tick> {
        &self.root
    }
    fn root_mut(&mut self) -> &mut Root<Tick> {
        &mut self.root
    }
    fn transition(&mut self, event: &Tick) {
        self.n += event.0 as i64;
    }
}

struct CounterCodec;

impl AggregateCodec for CounterCodec {
    type Aggregate = Counter;
    fn type_name(&self) -> &str {
        "Counter"
    }
    fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Counter, Error> {
        let n = match data {
            Some(d) => std::str::from_utf8(d)
                .map_err(Error::codec)?
                .parse()
                .map_err(Error::codec)?,
            None => 0,
        };
        Ok(Counter {
            root: Root::new(id, version),
            n,
        })
    }
    fn dump(&self, agg: &Counter) -> Result<Vec<u8>, Error> {
        Ok(agg.n.to_string().into_bytes())
    }
}

type CounterRepo = Repository<MemoryEventStore, JsonCodec<Tick>, CounterCodec>;

fn counter_repo(store: MemoryEventStore) -> CounterRepo {
    Repository::new(store, JsonCodec::<Tick>::new("Tick", 1), CounterCodec).with_ids(next_id)
}

#[derive(Debug, Clone)]
enum RepoOp {
    Apply {
        agg: u8,
        ticks: Vec<u8>,
        ns: u8,
        stale: bool,
    },
    Reload {
        agg: u8,
    },
}

fn repo_op() -> impl Strategy<Value = RepoOp> {
    prop_oneof![
        3 => (0u8..3, prop::collection::vec(0u8..=3, 0..4), 0u8..3, any::<bool>())
            .prop_map(|(agg, ticks, ns, stale)| RepoOp::Apply { agg, ticks, ns, stale }),
        1 => (0u8..3).prop_map(|agg| RepoOp::Reload { agg }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn repository_matches_fold(history in prop::collection::vec(repo_op(), 1..16)) {
        let store = MemoryEventStore::new();
        let repo = counter_repo(store.clone());
        // The fold input per aggregate; the version is just its length.
        let mut model: HashMap<u8, Vec<u8>> = HashMap::new();
        let mut live: HashMap<u8, Counter> = HashMap::new();
        let mut tx = MemoryTx::auto_commit();

        for op in history {
            match op {
                RepoOp::Apply { agg, ticks, ns, stale } => {
                    let ticks_model = model.entry(agg).or_default();
                    let version = ticks_model.len() as u64;
                    let (extensions, metadata, _) = stamp(ns);
                    let lineage = propagate(&format!("cmd-{agg}-{version}"), "corr-1");
                    // A stale save only conflicts if the live save actually bumps the version.
                    let stale = stale && !ticks.is_empty() && version > 0;

                    // Loaded *before* the live instance saves, so it holds the old version.
                    let mut stale_agg = if stale { Some(repo.load(&agg_id(agg)).unwrap()) } else { None };

                    let instance = match live.entry(agg) {
                        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(repo.load(&agg_id(agg)).unwrap())
                        }
                    };
                    for t in &ticks {
                        instance.apply(Tick(*t));
                    }
                    let saved = repo
                        .save_with(instance, lineage.clone(), extensions.clone(), metadata.clone())
                        .expect("live save");
                    // Every event of the batch carries the context it was saved with.
                    let carries = |e: &EventMessage| {
                        e.extensions == extensions
                            && e.metadata == metadata
                            && e.correlation_id == lineage.correlation_id
                            && e.causation_id == lineage.causation_id
                    };

                    let want_seqs: Vec<Option<u64>> =
                        (version + 1..=version + ticks.len() as u64).map(Some).collect();
                    prop_assert_eq!(saved.iter().map(|e| e.sequence).collect::<Vec<_>>(), want_seqs);
                    prop_assert!(saved.iter().all(carries));
                    prop_assert!(instance.root().pending().is_empty());
                    prop_assert_eq!(instance.root().version, version + ticks.len() as u64);
                    prop_assert!(instance.root().pending().is_empty());
                    ticks_model.extend(ticks.iter().copied());

                    // The context survives the store, not just the returned events.
                    let stored = store.load(&mut tx, &agg_id(agg), version).unwrap();
                    prop_assert_eq!(&stored, &saved);

                    if let Some(agg_stale) = stale_agg.as_mut() {
                        for t in &ticks {
                            agg_stale.apply(Tick(*t));
                        }
                        expect_conflict(
                            repo.save_with(agg_stale, lineage, extensions, metadata),
                            version,
                            version + ticks.len() as u64,
                        )?;
                        // The failed save leaves the aggregate untouched so the caller can retry.
                        prop_assert_eq!(agg_stale.root().pending().len(), ticks.len());
                        prop_assert_eq!(agg_stale.root().pending().len() as u64, ticks.len() as u64);
                    }
                }

                RepoOp::Reload { agg } => {
                    let ticks = model.get(&agg).cloned().unwrap_or_default();
                    let want: i64 = ticks.iter().map(|t| *t as i64).sum();
                    let loaded = repo.load(&agg_id(agg)).unwrap();
                    prop_assert_eq!(loaded.n, want);
                    prop_assert_eq!(loaded.root().version, ticks.len() as u64);

                    // The snapshot path must agree with a pure replay: rebuild the same stream in a
                    // store that has no snapshot at all and load again.
                    let fresh = MemoryEventStore::new();
                    let events = store.load(&mut tx, &agg_id(agg), 0).unwrap();
                    // The replay folds only the name, payload and sequence; the rest is the base draft's.
                    let drafts: Vec<EventDraft> = events
                        .iter()
                        .map(|e| {
                            let sequence = e.sequence.expect("a stream event is sequenced");
                            EventDraft { data: e.data.clone(), ..draft(&e.id, &e.aggregate_id, &e.name, sequence) }
                        })
                        .collect();
                    fresh.append(&mut MemoryTx::auto_commit(), &agg_id(agg), 0, drafts).unwrap();
                    let replayed = counter_repo(fresh).load(&agg_id(agg)).unwrap();
                    prop_assert_eq!(replayed.n, want);
                    prop_assert_eq!(replayed.root().version, ticks.len() as u64);

                    live.insert(agg, loaded);
                }
            }
        }

        // Every save that appends snapshots (the default interval); a save with nothing pending, none.
        for (agg, ticks) in &model {
            let snap = store.load_snapshot(&mut tx, &agg_id(*agg)).unwrap();
            prop_assert_eq!(snap.map(|s| s.version), (!ticks.is_empty()).then_some(ticks.len() as u64));
        }
    }
}

// =========================================================================
// P5 — event codecs round-trip
// =========================================================================

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
enum Payload {
    Empty,
    Scalar {
        label: String,
        count: i64,
        flag: Option<bool>,
    },
    Blob(Vec<u8>),
    Nested {
        tags: BTreeMap<String, i64>,
        label: String,
    },
}

prop_compose! {
    fn payload_strategy()(choice in 0u8..4, label in ".*", count in any::<i64>(),
                          flag in proptest::option::of(any::<bool>()),
                          blob in prop::collection::vec(any::<u8>(), 0..24),
                          tags in prop::collection::btree_map(".*", any::<i64>(), 0..4))
                         -> Payload {
        match choice {
            0 => Payload::Empty,
            1 => Payload::Scalar { label, count, flag },
            2 => Payload::Blob(blob),
            _ => Payload::Nested { tags, label },
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn codecs_roundtrip(event in payload_strategy()) {
        let json = JsonCodec::<Payload>::new("Payload", 3);
        let raw = json.marshal(&event).unwrap();
        prop_assert_eq!(&raw.name, "Payload");
        prop_assert_eq!(raw.version, 3);
        prop_assert_eq!(json.unmarshal(&raw).unwrap(), event.clone());

        #[cfg(feature = "msgpack")]
        {
            let mp = edax::MsgpackCodec::<Payload>::new("Payload", 3);
            let raw = mp.marshal(&event).unwrap();
            prop_assert_eq!(&raw.name, "Payload");
            prop_assert_eq!(mp.unmarshal(&raw).unwrap(), event);
        }
    }

    #[test]
    fn codecs_reject_garbage(bytes in prop::collection::vec(any::<u8>(), 1..16)) {
        let raw = Event { name: "Payload".into(), version: 3, data: bytes };
        // Whatever the bytes are, a codec either decodes them or returns Error::codec — never panics.
        let json = JsonCodec::<Payload>::new("Payload", 3);
        if let Err(e) = json.unmarshal(&raw) {
            prop_assert_eq!(e.code, ErrorCode::Codec);
        }
        #[cfg(feature = "msgpack")]
        if let Err(e) = edax::MsgpackCodec::<Payload>::new("Payload", 3).unmarshal(&raw) {
            prop_assert_eq!(e.code, ErrorCode::Codec);
        }
    }
}

// =========================================================================
// P6 — projection store: OCC + paging
// =========================================================================

#[derive(Debug, Clone)]
enum ProjOp {
    Save {
        id: u8,
        delta: i8,
        n: u8,
    },
    Get {
        id: u8,
    },
    Page {
        offset: usize,
        limit: Option<usize>,
        desc: bool,
    },
}

fn proj_op() -> impl Strategy<Value = ProjOp> {
    prop_oneof![
        3 => (0u8..6, -2i8..=2, 0u8..10).prop_map(|(id, delta, n)| ProjOp::Save { id, delta, n }),
        1 => (0u8..6).prop_map(|id| ProjOp::Get { id }),
        2 => (0usize..6, proptest::option::of(0usize..6), any::<bool>())
            .prop_map(|(offset, limit, desc)| ProjOp::Page { offset, limit, desc }),
    ]
}

/// P6 over any [`ProjectionStore`]: the generic bound *is* the contract — the OCC on `version + 1`
/// and the sort/offset/limit paging hold for any backend, not just the in-memory one exercised here.
fn check_projection_store<S>(new_store: impl Fn() -> S, history: Vec<ProjOp>) -> Result<(), TestCaseError>
where
    S: ProjectionStore,
    S::Tx: Default,
{
    let store = new_store();
    let mut model: BTreeMap<u8, (u64, u8)> = BTreeMap::new();
    let mut tx = S::Tx::default(); // the auto-commit handle: the model describes committed state

    for op in history {
        match op {
            ProjOp::Save { id, delta, n } => {
                let stored = model.get(&id).map(|(v, _)| *v).unwrap_or(0);
                let expected = (stored as i64 + delta as i64).max(0) as u64;
                let mut entity = Entity {
                    id: format!("u{id}"),
                    version: expected,
                    document: json!({ "n": n }),
                };
                let got = store.save(&mut tx, &mut entity);
                if expected == stored {
                    prop_assert!(got.is_ok(), "clean save rejected: {:?}", got.err());
                    prop_assert_eq!(entity.version, stored + 1); // advanced in place
                    model.insert(id, (stored + 1, n));
                } else {
                    expect_conflict(got, expected, stored)?;
                    prop_assert_eq!(entity.version, expected); // unchanged on conflict
                }
            }

            ProjOp::Get { id } => {
                let got = store.get(&mut tx, &format!("u{id}")).unwrap();
                match model.get(&id) {
                    Some((version, n)) => {
                        let e = got.expect("stored entity");
                        prop_assert_eq!(e.version, *version);
                        prop_assert_eq!(&e.document["n"], &json!(n));
                    }
                    None => prop_assert!(got.is_none()),
                }
            }

            ProjOp::Page { offset, limit, desc } => {
                let page = store
                    .get_many(
                        &mut tx,
                        &Query {
                            sort: vec![SortKey {
                                path: "id".into(),
                                desc,
                            }],
                            offset,
                            limit,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                let mut ids: Vec<String> = model.keys().map(|id| format!("u{id}")).collect();
                if desc {
                    ids.reverse();
                }
                let want: Vec<String> = ids.into_iter().skip(offset).take(limit.unwrap_or(usize::MAX)).collect();
                let got: Vec<String> = page.rows.iter().map(|d| d["id"].as_str().unwrap().to_string()).collect();
                prop_assert_eq!(got, want);
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn projection_matches_model(history in prop::collection::vec(proj_op(), 1..24)) {
        check_projection_store(MemoryProjectionStore::new, history)?;
    }
}

// =========================================================================
// Thread race — the store's optimistic concurrency under real contention
// =========================================================================

/// Eight threads append to one aggregate at the same expected version: exactly one wins, the rest
/// get a conflict carrying the version that beat them, and the stream gains exactly one event.
/// Deterministic (no proptest) — this is a race, not a history.
#[test]
fn concurrent_appends_one_winner() {
    let store = MemoryEventStore::new(); // Clone shares the Arc, like handing one store to N workers
    for round in 0..3u64 {
        let results: Vec<Result<Vec<EventMessage>, Error>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8u64)
                .map(|t| {
                    let st = store.clone();
                    s.spawn(move || {
                        let event = draft(format!("r{round}-t{t}"), "a", "Ticked", round + 1);
                        st.append(&mut MemoryTx::auto_commit(), "a", round, vec![event])
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().expect("worker thread")).collect()
        });

        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "exactly one append may win round {round}");
        for e in results.iter().filter_map(|r| r.as_ref().err()) {
            assert_eq!(*e, Error::concurrency(round, round + 1), "loser should see the winner's version");
        }

        let mut tx = MemoryTx::auto_commit();
        assert_eq!(store.load(&mut tx, "a", 0).unwrap().len() as u64, round + 1);
        assert_eq!(store.unpublished(100).unwrap().len() as u64, round + 1);
    }
}
