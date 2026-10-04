//! Saga / process manager: react to committed events, keep per-instance state, emit commands.
//!
//! A saga coordinates a workflow that spans several aggregates. Each event it cares about is routed to one
//! running *instance*, folded into that instance's state, and may emit commands that drive the next step. The seam is [`Saga`] (the decision) and [`SagaStore`] (where instance state and the saga
//! outbox live), plus a [`SagaRunner`] that folds an event and persists the step.
//!
//! The runner never sends a command. It saves the new state and the commands the fold decided in one
//! versioned write, the saga outbox, and a [`Relay`](crate::Relay) over the store sends them once that write
//! has committed. A fold that loses the version race to another worker therefore sends nothing: its commands
//! roll back with its state, and the redelivery folds the winner's state. A crash after the commit re-sends
//! from the outbox instead of losing the command, so a command is sent at least once per committed step and a
//! consumer deduplicates it by its idempotency key. Completion writes a tombstone: a late or redelivered event finds the
//! instance completed and is ignored, and a writer that loaded the instance before it completed conflicts.
//! A running instance remembers what it folded, so an event redelivered after its step committed (a lost
//! ack, a crash before the offset commit) is skipped instead of folded again under new command ids.

use edax_event::{saga_admit, saga_command, saga_write, CommandMessage, EventMessage, Loaded, SagaAdmit, SagaWrite};

use crate::command::HandlerError;
use crate::store::Outbox;
use crate::{Error, EventCodec};

/// The decision half of a saga: route an event to an instance and fold it into that instance.
pub trait Saga {
    /// Per-instance state this saga accumulates.
    type State;
    /// Command type this saga emits.
    type Command;

    /// The saga's name, stable across deploys. It scopes the idempotency key of every command the saga
    /// sends, so two sagas that share an instance id and a bus never replay each other's commands.
    fn name(&self) -> &str;

    /// The id of the running instance this event belongs to, or `None` to ignore it. Not the event's
    /// `correlation_id`: that names the flow, while one flow may drive several instances. The instance
    /// id becomes the `aggregate_id` of every command the instance sends, so its commands keep their
    /// order.
    fn instance_id(&self, event: &EventMessage) -> Option<String>;

    /// Fold `event` into the instance. `state` is `None` for a new instance. Return
    /// `(new_state, commands)`; a `new_state` of `None` completes the instance. Pure: the runner may
    /// discard the result when the write loses a race.
    fn handle(&self, state: Option<Self::State>, event: &EventMessage) -> (Option<Self::State>, Vec<Self::Command>);
}

/// A stored saga instance: running, with the state a fold starts from, or completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SagaInstance<S> {
    /// A running instance and the version it was loaded at.
    Running {
        /// The accumulated state.
        state: S,
        /// The version a write is conditional on.
        version: u64,
        /// The kernel's marks of the events folded so far ([`edax_event::saga_admit`]): opaque text the
        /// store keeps as the last write saved it.
        folded: String,
    },
    /// A completed instance: a tombstone that keeps its version, so no write lands on it again.
    Completed {
        /// The tombstone's version.
        version: u64,
    },
}

/// Where saga instance state and the saga outbox live, keyed by instance id, with optimistic
/// concurrency. What a backend must guarantee:
///
/// - `save` and `complete` write at `version + 1` only if the stored version still equals `version` (an
///   absent instance is at version 0), and return the new version; otherwise they return a
///   [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency) error and write nothing. A completed
///   instance takes no write at all.
/// - The step's `commands` are written in the same write as the state, so they commit or roll back
///   together, and the [`Outbox`] supertrait shows them only once committed, oldest first. Each is the
///   kernel's [`saga_command`] row for that write, keyed by its id.
/// - `save` keeps `folded` as it is and `load` returns it unchanged in [`SagaInstance::Running`]: the
///   runner skips an event it already folded by it.
/// - `complete` keeps the row as a tombstone at its new version rather than removing it; a durable store
///   purges tombstones by retention.
pub trait SagaStore: Outbox<CommandMessage> {
    type State;
    /// Backend transaction handle — the same one the [`Transactor`](crate::Transactor) hands out, so a
    /// saga state write can enlist in the events' unit of work. Use `()` for a store that needs none.
    type Tx;

    /// The instance for `id`, or `None` if there is none.
    fn load(&self, tx: &mut Self::Tx, id: &str) -> Result<Option<SagaInstance<Self::State>>, Error>;
    /// Persist `state`, `folded` and `commands` for `id` at `version + 1`; see the trait for the rule.
    fn save(&self, tx: &mut Self::Tx, id: &str, state: &Self::State, version: u64, folded: &str, commands: &[CommandMessage]) -> Result<u64, Error>;
    /// Complete `id`: write its tombstone and `commands` at `version + 1`; see the trait for the rule.
    fn complete(&self, tx: &mut Self::Tx, id: &str, version: u64, commands: &[CommandMessage]) -> Result<u64, Error>;
}

/// Drives a [`Saga`] over committed events: route to an instance, fold, and persist the step, state and
/// commands in one write. Pass [`SagaRunner::run`] (closing over a `tx`) to an event router / consumer as the
/// handler, and drain the store's command outbox with a [`Relay`](crate::Relay), decoding each row with
/// [`SagaRunner::command`].
pub struct SagaRunner<SG, ST, CC> {
    saga: SG,
    store: ST,
    commands: CC,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl<SG, ST, CC> SagaRunner<SG, ST, CC>
where
    SG: Saga,
    ST: SagaStore<State = SG::State>,
    CC: EventCodec<Domain = SG::Command>,
{
    /// A runner folding with `saga`, persisting to `store`, and marshaling commands with `commands`.
    pub fn new(saga: SG, store: ST, commands: CC) -> Self {
        Self { saga, store, commands, now: Box::new(edax_core::current_timestamp) }
    }

    /// Override the clock that stamps each command's `issued_at`, for deterministic tests.
    pub fn with_clock(mut self, now: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Box::new(now);
        self
    }

    /// The store the runner persists to: build the command [`Relay`](crate::Relay) over a clone of it.
    pub fn store(&self) -> &ST {
        &self.store
    }

    /// Decode an outbox row back into the saga's command, for the relay's publish.
    pub fn command(&self, row: &CommandMessage) -> Result<SG::Command, Error> {
        self.commands.unmarshal(&row.payload())
    }

    /// Handle one committed event: load its instance, fold the event, and write the new state
    /// (or the tombstone) with the commands the fold emitted, at the loaded version. Thread the same `tx`
    /// the events committed under so the write joins that unit of work. A conflict means another worker
    /// advanced the instance first; its error is transient, so the event is redelivered and folds the
    /// winner's state, and nothing this fold decided is ever sent. An event the instance already folded
    /// (redelivered after its step committed) is skipped: no fold, no write, no command.
    pub fn run(&self, tx: &mut ST::Tx, event: &EventMessage) -> Result<(), HandlerError> {
        let Some(instance_id) = self.saga.instance_id(event) else { return Ok(()) };
        let (loaded, state, folded, completed) = match self.store.load(tx, &instance_id)? {
            None => (Loaded::Absent, None, String::new(), false),
            Some(SagaInstance::Running { state, version, folded }) => (Loaded::Active { version }, Some(state), folded, false),
            Some(SagaInstance::Completed { .. }) => (Loaded::Absent, None, String::new(), true),
        };
        // The kernel skips a tombstone and an event the instance folded already: no fold, no write.
        let SagaAdmit::Fold { folded } = saga_admit(completed, &folded, event)? else { return Ok(()) };
        let (new_state, commands) = self.saga.handle(state, event);
        let (expected, version) = match saga_write(loaded, new_state.is_none(), !commands.is_empty())? {
            SagaWrite::Save { expected, version } | SagaWrite::Complete { expected, version } => (expected, version),
            SagaWrite::Nothing => return Ok(()),
        };
        let rows = self.rows(&instance_id, version, &commands, event)?;
        // The kernel answers `Save` exactly when the fold keeps state, and `Complete` when it does not.
        match new_state {
            Some(state) => self.store.save(tx, &instance_id, &state, expected, &folded, &rows)?,
            None => self.store.complete(tx, &instance_id, expected, &rows)?,
        };
        Ok(())
    }

    /// The outbox rows for the commands a write at `version` sends: the kernel's command messages, each
    /// a child of `cause`.
    fn rows(
        &self,
        instance_id: &str,
        version: u64,
        commands: &[SG::Command],
        cause: &EventMessage,
    ) -> Result<Vec<CommandMessage>, Error> {
        let issued_at = (self.now)();
        (0u64..)
            .zip(commands)
            .map(|(index, command)| {
                let payload = self.commands.marshal(command)?;
                Ok(saga_command(self.saga.name(), instance_id, version, index, cause.tenant.as_deref(), payload, &cause.id, &cause.correlation_id, issued_at))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemorySagaStore, MemoryTx};
    use crate::{
        idempotent, wrap, Command, CommandBus, ErrorCode, JsonRegistryCodec, Pacing, Relay, Terminal, Transactor,
    };
    use crate::testkit::event;
    use serde::{Deserialize, Serialize};
    use std::cell::{Cell, RefCell};
    use std::sync::{Arc, Mutex};

    // --- an order saga over a cross-aggregate instance ---

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Fulfill {
        order_id: String,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Refund {
        order_id: String,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum OrderCommand {
        Fulfill(Fulfill),
        Refund(Refund),
    }

    impl Command for OrderCommand {
        fn command_type(&self) -> &str {
            match self {
                OrderCommand::Fulfill(_) => "Fulfill",
                OrderCommand::Refund(_) => "Refund",
            }
        }
    }

    /// `Created` opens the instance, `Paid` fulfills it and `Cancelled` refunds it, both completing it.
    /// The instance is the order, while the events come from the order and the payment aggregates:
    /// two workers can fold one instance at once, which is what the version is for.
    struct OrderSaga;

    impl Saga for OrderSaga {
        type State = String;
        type Command = OrderCommand;

        fn name(&self) -> &str {
            "order"
        }

        fn instance_id(&self, event: &EventMessage) -> Option<String> {
            event.metadata.get("order").cloned()
        }

        fn handle(&self, state: Option<String>, event: &EventMessage) -> (Option<String>, Vec<OrderCommand>) {
            let order_id = event.metadata.get("order").cloned().unwrap_or_default();
            match (state.as_deref(), event.name.as_str()) {
                (None, "Created") => (Some("open".to_owned()), vec![]),
                (Some("open"), "Paid") => (None, vec![OrderCommand::Fulfill(Fulfill { order_id })]),
                (Some("open"), "Cancelled") => (None, vec![OrderCommand::Refund(Refund { order_id })]),
                (Some("open"), "Noted") => (Some("open".to_owned()), vec![]),
                // Nothing loaded and nothing to start: a stray event writes nothing.
                (None, _) => (None, vec![]),
                (Some(s), _) => (Some(s.to_owned()), vec![]),
            }
        }
    }

    /// Event `name` of `aggregate_id`, for the order `order-7`, with id `"{aggregate_id}-{name}"`.
    fn order_event(aggregate_id: &str, name: &str) -> EventMessage {
        let mut env = event(aggregate_id, name, 1);
        env.id = format!("{aggregate_id}-{name}");
        env.metadata.insert("order".to_owned(), "order-7".to_owned());
        env
    }

    type Runner<ST> = SagaRunner<OrderSaga, ST, JsonRegistryCodec<OrderCommand>>;

    fn runner<ST: SagaStore<State = String>>(store: ST) -> Runner<ST> {
        SagaRunner::new(OrderSaga, store, JsonRegistryCodec::new(1).unwrap()).with_clock(|| 1_714_564_800_000_000_000)
    }


    /// Drain every committed command through `send`, recording the decoded commands.
    fn drain_all(runner: &Runner<MemorySagaStore<String>>) -> Vec<OrderCommand> {
        let relay = Relay::<_, CommandMessage>::new(runner.store().clone()).pacing(Pacing::instant());
        let sent = RefCell::new(Vec::new());
        relay
            .drain(100, |row| {
                sent.borrow_mut().push(runner.command(row)?);
                Ok(())
            })
            .expect("a healthy drain");
        sent.into_inner()
    }

    fn instance(store: &MemorySagaStore<String>) -> Option<SagaInstance<String>> {
        store.load(&mut MemoryTx::auto_commit(), "order-7").unwrap()
    }

    #[test]
    fn a_step_persists_its_state_and_commands_and_a_completed_instance_takes_no_event() {
        struct Step {
            event: &'static str,
            want: Option<SagaInstance<String>>,
            want_outbox: &'static [&'static str],
        }
        let running = |version, last: &str| {
            let folded = format!(r#"{{"order-7":{{"sequence":1,"id":"{last}"}}}}"#);
            Some(SagaInstance::Running { state: "open".to_owned(), version, folded })
        };
        let steps = [
            Step { event: "Stray", want: None, want_outbox: &[] },
            Step { event: "Created", want: running(1, "order-7-Created"), want_outbox: &[] },
            Step { event: "Noted", want: running(2, "order-7-Noted"), want_outbox: &[] },
            Step { event: "Paid", want: Some(SagaInstance::Completed { version: 3 }), want_outbox: &["order-7:3:0"] },
            // A redelivered `Paid` and a late `Cancelled` find a tombstone: no fold, no write, no command.
            Step { event: "Paid", want: Some(SagaInstance::Completed { version: 3 }), want_outbox: &["order-7:3:0"] },
            Step { event: "Cancelled", want: Some(SagaInstance::Completed { version: 3 }), want_outbox: &["order-7:3:0"] },
            // Not even a fresh `Created` resurrects it.
            Step { event: "Created", want: Some(SagaInstance::Completed { version: 3 }), want_outbox: &["order-7:3:0"] },
        ];
        let runner = runner(MemorySagaStore::<String>::new());
        for step in steps {
            runner.run(&mut MemoryTx::auto_commit(), &order_event("order-7", step.event)).expect(step.event);
            assert_eq!(instance(runner.store()), step.want, "{}", step.event);
            let outbox: Vec<String> = Outbox::<CommandMessage>::unpublished(runner.store(), 10).unwrap().into_iter().map(|c| c.id).collect();
            assert_eq!(outbox, step.want_outbox, "{}: outbox", step.event);
        }
        let row = Outbox::<CommandMessage>::unpublished(runner.store(), 1).unwrap().remove(0);
        // The row is the kernel's command message: keyed by the saga and instance, a child of the `Paid` event.
        assert_eq!(
            (row.idempotency_key.as_deref(), row.aggregate_id.as_deref(), row.name.as_str(), row.data.as_slice(), row.issued_at),
            (Some("5:order:order-7:3:0"), Some("order-7"), "Fulfill", &br#"{"order_id":"order-7"}"#[..], 1_714_564_800_000_000_000)
        );
        assert_eq!((row.correlation_id.as_str(), row.causation_id.as_str()), ("corr-1", "order-7-Paid"));
        assert_eq!(drain_all(&runner), [OrderCommand::Fulfill(Fulfill { order_id: "order-7".into() })]);
        assert!(drain_all(&runner).is_empty(), "marked sent");
    }

    /// The store, with a hook that runs between a worker's load and its write: the interleaving the
    /// version exists for, forced deterministically instead of with threads and sleeps.
    struct RacingStore {
        inner: MemorySagaStore<String>,
        after_load: RefCell<Option<Box<dyn FnOnce()>>>,
        fail_write: Cell<bool>,
        /// The next write lands, then reports a failure: its reply was lost.
        lose_reply: Cell<bool>,
    }

    impl RacingStore {
        fn new(inner: MemorySagaStore<String>) -> Self {
            Self { inner, after_load: RefCell::new(None), fail_write: Cell::new(false), lose_reply: Cell::new(false) }
        }

        fn landed<T>(&self, wrote: Result<T, Error>) -> Result<T, Error> {
            match wrote {
                Ok(_) if self.lose_reply.replace(false) => Err(Error::store("reply lost after the write")),
                other => other,
            }
        }
    }

    impl Outbox<CommandMessage> for RacingStore {
        fn unpublished(&self, limit: usize) -> Result<Vec<CommandMessage>, Error> {
            self.inner.unpublished(limit)
        }
        fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
            self.inner.mark_published(ids)
        }
    }

    impl SagaStore for RacingStore {
        type State = String;
        type Tx = MemoryTx;

        fn load(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<SagaInstance<String>>, Error> {
            let loaded = self.inner.load(tx, id);
            if let Some(hook) = self.after_load.borrow_mut().take() {
                hook();
            }
            loaded
        }
        fn save(&self, tx: &mut MemoryTx, id: &str, state: &String, version: u64, folded: &str, commands: &[CommandMessage]) -> Result<u64, Error> {
            if self.fail_write.get() {
                return Err(Error::store("saga store unavailable"));
            }
            self.landed(self.inner.save(tx, id, state, version, folded, commands))
        }
        fn complete(&self, tx: &mut MemoryTx, id: &str, version: u64, commands: &[CommandMessage]) -> Result<u64, Error> {
            if self.fail_write.get() {
                return Err(Error::store("saga store unavailable"));
            }
            self.landed(self.inner.complete(tx, id, version, commands))
        }
    }

    #[test]
    fn the_fold_that_loses_the_version_race_sends_nothing() {
        // `Paid` (payment worker) loads the open order; before it writes, `Cancelled` (order worker)
        // folds the same instance and completes it with a `Refund`. `Paid`'s write conflicts, and
        // because the runner never sends, its `Fulfill` dies with its fold. The redelivered `Paid`
        // finds the tombstone. Exactly one compensating decision goes out.
        let inner = MemorySagaStore::<String>::new();
        let racing = RacingStore::new(inner.clone());
        let first = runner(racing);
        first.run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created")).unwrap();

        let second = runner(inner.clone());
        *first.store().after_load.borrow_mut() = Some(Box::new(move || {
            second.run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Cancelled")).expect("the winner commits");
        }));
        let lost = first.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"));
        let conflict = lost.expect_err("the loser's write conflicts");
        assert_eq!(conflict.downcast_ref::<Error>().map(|e| e.code), Some(ErrorCode::Concurrency), "transient: redelivered");

        first.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid")).expect("the redelivery is ignored");
        let check = runner(inner);
        assert_eq!(instance(check.store()), Some(SagaInstance::Completed { version: 2 }));
        assert_eq!(drain_all(&check), [OrderCommand::Refund(Refund { order_id: "order-7".into() })], "no Fulfill escaped");
    }

    #[test]
    fn a_write_that_fails_or_rolls_back_sends_nothing() {
        // Crash between the decision and the send, in the only order the runner allows: the commands
        // are part of the write, so a write that does not land leaves nothing to send, and the
        // redelivery decides again from the state that did land.
        for (name, fail_write) in [("the store refuses the write", true), ("the unit of work rolls back", false)] {
            let inner = MemorySagaStore::<String>::new();
            let store = RacingStore::new(inner.clone());
            let runner = runner(store);
            runner.run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created")).unwrap();
            runner.store().fail_write.set(fail_write);
            let tr = inner.transactor();
            let tx = tr.begin().unwrap();
            let mut handle = tx;
            let out = runner.run(&mut handle, &order_event("payment-7", "Paid"));
            assert_eq!(out.is_err(), fail_write, "{name}");
            tr.rollback(handle);
            let folded = r#"{"order-7":{"sequence":1,"id":"order-7-Created"}}"#.to_owned();
            assert_eq!(instance(&inner), Some(SagaInstance::Running { state: "open".to_owned(), version: 1, folded }), "{name}");
            assert!(Outbox::<CommandMessage>::unpublished(&inner, 10).unwrap().is_empty(), "{name}: nothing to send");

            runner.store().fail_write.set(false);
            runner.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid")).unwrap();
            assert_eq!(drain_all(&self::runner(inner)), [OrderCommand::Fulfill(Fulfill { order_id: "order-7".into() })], "{name}: redelivery");
        }
    }

    /// Every event it folds emits one command naming the event, so a fold run twice shows as a
    /// second command. The instance is fixed, whatever the source aggregate.
    struct Echo;

    impl Saga for Echo {
        type State = u32;
        type Command = OrderCommand;

        fn name(&self) -> &str {
            "echo"
        }

        fn instance_id(&self, _: &EventMessage) -> Option<String> {
            Some("order-7".to_owned())
        }

        fn handle(&self, state: Option<u32>, event: &EventMessage) -> (Option<u32>, Vec<OrderCommand>) {
            (Some(state.unwrap_or(0) + 1), vec![OrderCommand::Fulfill(Fulfill { order_id: event.id.clone() })])
        }
    }

    #[test]
    fn an_event_redelivered_after_its_step_committed_is_not_folded_again() {
        // The ack of a committed step was lost, or the consumer crashed before the offset commit:
        // the broker hands the same events over again. Folding one twice would decide its command
        // again under the next version's id, which a consumer deduplicating by command id runs twice.
        let at = |aggregate_id: &str, sequence: Option<u64>, id: &str| EventMessage {
            id: id.to_owned(),
            sequence,
            ..order_event(aggregate_id, "Created")
        };
        let cases: [(&str, Vec<EventMessage>, &[&str]); 5] = [
            ("the last event again", vec![at("order-7", Some(1), "o-1"), at("order-7", Some(1), "o-1")], &["o-1"]),
            (
                "a crash replays every event above the commit",
                vec![at("order-7", Some(1), "o-1"), at("order-7", Some(2), "o-2"), at("order-7", Some(1), "o-1"), at("order-7", Some(2), "o-2")],
                &["o-1", "o-2"],
            ),
            (
                "another aggregate's event between them",
                vec![at("order-7", Some(1), "o-1"), at("payment-7", Some(1), "p-1"), at("order-7", Some(1), "o-1"), at("payment-7", Some(1), "p-1")],
                &["o-1", "p-1"],
            ),
            ("an unsequenced event again", vec![at("order-7", None, "u-1"), at("order-7", None, "u-1")], &["u-1"]),
            (
                // With no sequence there is no order to compare: only the last one is known.
                "an older unsequenced event is folded again",
                vec![at("order-7", None, "u-1"), at("order-7", None, "u-2"), at("order-7", None, "u-1")],
                &["u-1", "u-2", "u-1"],
            ),
        ];
        for (name, deliveries, want) in cases {
            let runner = SagaRunner::new(Echo, MemorySagaStore::<u32>::new(), JsonRegistryCodec::new(1).unwrap());
            for env in &deliveries {
                runner.run(&mut MemoryTx::auto_commit(), env).expect(name);
            }
            let rows = Outbox::<CommandMessage>::unpublished(runner.store(), 10).unwrap();
            let sent: Vec<OrderCommand> = rows.iter().map(|row| runner.command(row).unwrap()).collect();
            let want_sent: Vec<OrderCommand> =
                want.iter().map(|id| OrderCommand::Fulfill(Fulfill { order_id: (*id).to_owned() })).collect();
            assert_eq!(sent, want_sent, "{name}: commands");
            // The command id carries the version its write produced: one write per fold, none skipped.
            let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
            let want_ids: Vec<String> = (1..=want.len()).map(|version| format!("order-7:{version}:0")).collect();
            assert_eq!(ids, want_ids, "{name}: one write per fold");
        }
    }

    #[test]
    fn a_lost_write_reply_converges_on_redelivery() {
        // The completion lands, then the runner sees a store error as if it had not. The redelivery
        // reads the tombstone, so it neither folds again nor commits a second command.
        let inner = MemorySagaStore::<String>::new();
        let runner = runner(RacingStore::new(inner.clone()));
        runner.run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created")).unwrap();
        runner.store().lose_reply.set(true);
        let lost = runner.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"));
        assert!(lost.is_err(), "the caller saw the lost reply");
        runner.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid")).expect("the redelivery is ignored");
        assert_eq!(instance(&inner), Some(SagaInstance::Completed { version: 2 }));
        assert_eq!(drain_all(&self::runner(inner)), [OrderCommand::Fulfill(Fulfill { order_id: "order-7".into() })]);
    }

    #[test]
    fn a_command_committed_before_a_crash_is_sent_after_it_and_a_resend_is_deduplicated() {
        // Crash after the commit: the command waits in the outbox, so the next relay sends it. Crash
        // after the send but before `mark_published`: the relay sends it again (at-least-once), and
        // the consumer's guard keyed by the row's idempotency key runs the handler once.
        let store = MemorySagaStore::<String>::new();
        let runner = runner(store.clone());
        runner.run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created")).unwrap();
        runner.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid")).unwrap();

        /// The outbox, whose `mark_published` fails while `crash` is set: the process died after the send.
        struct CrashingMark(MemorySagaStore<String>, Cell<bool>);
        impl Outbox<CommandMessage> for CrashingMark {
            fn unpublished(&self, limit: usize) -> Result<Vec<CommandMessage>, Error> {
                self.0.unpublished(limit)
            }
            fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
                if self.1.get() {
                    return Err(Error::store("connection lost"));
                }
                self.0.mark_published(ids)
            }
        }
        let relay = Relay::<_, CommandMessage>::new(CrashingMark(store.clone(), Cell::new(true))).pacing(Pacing::instant());
        let ledger: Arc<dyn crate::IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> = Arc::new(crate::MemoryIdempotencyStore::<()>::new());
        let current_row: Arc<Mutex<String>> = Arc::default();
        let key_row = Arc::clone(&current_row);
        let guard = idempotent(ledger, "fulfillment", edax_core::Tenancy::Silo, move |_: &OrderCommand| Some(key_row.lock().expect("no panics").clone()));
        let runs: Arc<Mutex<Vec<OrderCommand>>> = Arc::default();
        let effect = Arc::clone(&runs);
        let handler: crate::Handler<OrderCommand, ()> = Box::new(move |cmd: &OrderCommand| {
            effect.lock().expect("no panics").push(cmd.clone());
            Ok(())
        });
        let mut bus = CommandBus::new();
        bus.register("Fulfill", wrap(handler, vec![guard])).unwrap();
        let publish = |row: &CommandMessage| {
            *current_row.lock().expect("no panics") = row.idempotency_key.clone().unwrap_or_default();
            bus.dispatch::<OrderCommand, ()>(&runner.command(row)?)
        };

        let crashed = relay.drain(10, publish);
        assert!(crashed.is_err(), "the mark did not land");
        relay.store().1.set(false);
        assert_eq!(relay.drain(10, publish).unwrap(), 1, "re-sent after the crash");
        assert_eq!(relay.drain(10, publish).unwrap(), 0, "and then marked");
        assert_eq!(*runs.lock().unwrap(), [OrderCommand::Fulfill(Fulfill { order_id: "order-7".into() })], "the handler ran once");
    }

    #[test]
    fn a_saga_command_relayed_through_the_bus_keeps_the_commands_verdict() {
        // The relay is where the command runs, so it is where its verdict lands: a terminal
        // failure, the handler's own or a host rejection, dead-letters the row once and the rows
        // behind it go out; a transient one hands the row back.
        #[derive(Clone, Copy, Debug)]
        enum Fails {
            Never,
            Codec,
            Rejected,
            Transient,
        }
        let cases = [
            ("success is marked", Fails::Never, 0, 0),
            ("a terminal command failure dead-letters the row", Fails::Codec, 1, 0),
            ("a host rejection dead-letters the row", Fails::Rejected, 1, 0),
            ("a transient command failure hands the row back", Fails::Transient, 0, 1),
        ];
        for (name, fails, want_dead, want_left) in cases {
            let store = MemorySagaStore::<String>::new();
            let runner = runner(store.clone());
            runner.run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created")).unwrap();
            runner.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid")).unwrap();
            let handler: crate::Handler<OrderCommand, ()> = Box::new(move |_: &OrderCommand| match fails {
                Fails::Never => Ok(()),
                Fails::Codec => Err(Error::codec("unknown field `sku`").into()),
                Fails::Rejected => Err(Terminal::new("order already shipped").into()),
                Fails::Transient => Err("warehouse API timed out".into()),
            });
            let mut bus = CommandBus::new();
            bus.register("Fulfill", handler).unwrap();
            let dead: Arc<Mutex<Vec<String>>> = Arc::default();
            let sink = Arc::clone(&dead);
            let relay = Relay::<_, CommandMessage>::new(store.clone()).pacing(Pacing::instant()).on_dead(move |row: &CommandMessage, _| {
                sink.lock().expect("no panics").push(row.id.clone());
                Ok(())
            });
            let out = relay.drain(10, |row| bus.dispatch::<OrderCommand, ()>(&runner.command(row)?));
            assert_eq!(out.is_err(), want_left == 1, "{name}: drain result");
            assert_eq!(dead.lock().unwrap().len(), want_dead, "{name}: dead-lettered");
            assert_eq!(Outbox::<CommandMessage>::unpublished(&store, 10).unwrap().len(), want_left, "{name}: left");
        }
    }
}
