//! Saga / process manager: react to committed events, keep per-correlation state, emit commands.
//!
//! A saga coordinates a workflow that spans several aggregates. Each event it cares about is routed to a
//! *correlation* (one running instance), folded into that instance's state, and may emit commands that drive
//! the next step. The seam is [`Saga`] (the decision) and [`SagaStore`] (where instance state lives), plus a
//! [`SagaRunner`] that wires them to a command dispatcher and plugs into the event router as a handler.

use edax_event::EventEnvelope;

use crate::Error;

/// The decision half of a saga: route an event to a correlation and fold it into that instance.
pub trait Saga {
    /// Per-instance state this saga accumulates.
    type State;
    /// Command type this saga emits.
    type Command;

    /// Which running instance this event belongs to, or `None` to ignore it.
    fn correlation_id(&self, env: &EventEnvelope) -> Option<String>;

    /// Fold `env` into the instance. `state` is `None` for a new instance. Return
    /// `(new_state, commands)`; a `new_state` of `None` completes (and removes) the instance.
    fn handle(&self, state: Option<Self::State>, env: &EventEnvelope) -> (Option<Self::State>, Vec<Self::Command>);
}

/// A running saga instance: the accumulated `state` and the `version` it was loaded at. The version is
/// optimistic concurrency. The dispatcher serializes same-`aggregate_id` events, so a saga whose
/// `correlation_id` is the aggregate id has one writer; a saga correlating across aggregates can have two,
/// and the version catches the racing writer.
pub struct SagaInstance<S> {
    pub state: S,
    pub version: u64,
}

/// Where saga instance state lives, keyed by correlation id, with optimistic concurrency.
pub trait SagaStore {
    type State;
    /// Backend transaction handle — the same one the [`Transactor`](crate::Transactor) hands out, so a
    /// saga state write can enlist in the events' unit of work. Use `()` for a store that needs none.
    type Tx;

    /// The instance for `id` (state + version), or `None` if there is none.
    fn load(&self, tx: &mut Self::Tx, id: &str) -> Result<Option<SagaInstance<Self::State>>, Error>;
    /// Persist `state` for `id` at `version + 1` if the stored version still equals `version`; return
    /// [`Error::Concurrency`] otherwise, and the new version on success.
    fn save(&self, tx: &mut Self::Tx, id: &str, state: &Self::State, version: u64) -> Result<u64, Error>;
    /// Remove the instance at `id` if the stored version still equals `version`; a no-op if absent.
    fn delete(&self, tx: &mut Self::Tx, id: &str, version: u64) -> Result<(), Error>;
}

/// Drives a [`Saga`] over committed events: route to a correlation, fold, dispatch commands, persist.
/// Pass [`SagaRunner::run`] (closing over a `tx` and a dispatcher) to an event router / consumer as
/// the handler; the in-memory store's handle is [`MemoryTx`](crate::MemoryTx), so a demo closes over
/// `&mut MemoryTx::auto_commit()`.
pub struct SagaRunner<SG, ST> {
    saga: SG,
    store: ST,
}

impl<SG, ST> SagaRunner<SG, ST>
where
    SG: Saga,
    ST: SagaStore<State = SG::State>,
{
    pub fn new(saga: SG, store: ST) -> Self {
        Self { saga, store }
    }

    /// Handle one committed event: load the correlation's instance, fold the event, dispatch any
    /// emitted commands, then persist at the loaded version (or delete on completion). Thread the same
    /// `tx` the events committed under so the state write joins that unit of work.
    // at-least-once — dispatch before persist; redelivery re-dispatches, route commands through an
    // idempotent bus. The optimistic version guards a cross-aggregate correlation's concurrent writer;
    // it does not make the dispatch+persist atomic (add a saga-outbox to close that window).
    pub fn run<F>(&self, tx: &mut ST::Tx, env: &EventEnvelope, mut dispatch: F) -> Result<(), Error>
    where
        F: FnMut(&SG::Command) -> Result<(), Error>,
    {
        let Some(correlation_id) = self.saga.correlation_id(env) else { return Ok(()) };
        let instance = self.store.load(tx, &correlation_id)?;
        let version = instance.as_ref().map_or(0, |i| i.version);
        let existed = instance.is_some();
        let (new_state, commands) = self.saga.handle(instance.map(|i| i.state), env);
        for cmd in &commands {
            dispatch(cmd)?;
        }
        match new_state {
            // Nothing loaded and nothing produced. A versioned delete here would turn a no-op into a
            // write, and a concurrent creator of the same correlation would make that write conflict.
            None if !existed => Ok(()),
            None => self.store.delete(tx, &correlation_id, version),
            Some(state) => self.store.save(tx, &correlation_id, &state, version).map(|_| ()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemorySagaStore, MemoryTx};
    use crate::{Command, CommandBus, KeyOrderedDispatcher};
    use edax_event::Event;
    use serde::Serialize;
    use std::cell::{Cell, RefCell};
    use std::sync::{Arc, Mutex};

    // --- fault injection over the real in-memory store ---

    /// The shared `MemorySagaStore` plus one knob: when `fail_write` is set, every write (save *and*
    /// delete) fails, standing in for a store that dies after the runner has already dispatched its
    /// commands. Only the fault is local; the concurrency rule is the shared adapter's.
    #[derive(Default)]
    struct FailingSagaStore {
        inner: MemorySagaStore<String>,
        fail_write: Cell<bool>,
    }

    impl SagaStore for FailingSagaStore {
        type State = String;
        type Tx = MemoryTx;

        fn load(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<SagaInstance<String>>, Error> {
            self.inner.load(tx, id)
        }
        fn save(&self, tx: &mut MemoryTx, id: &str, state: &String, version: u64) -> Result<u64, Error> {
            if self.fail_write.get() {
                return Err(Error::Store("saga store unavailable".into()));
            }
            self.inner.save(tx, id, state, version)
        }
        fn delete(&self, tx: &mut MemoryTx, id: &str, version: u64) -> Result<(), Error> {
            if self.fail_write.get() {
                return Err(Error::Store("saga store unavailable".into()));
            }
            self.inner.delete(tx, id, version)
        }
    }

    // --- a tiny order saga ---

    struct OrderSaga;

    impl Saga for OrderSaga {
        type State = String;
        type Command = String;

        fn correlation_id(&self, env: &EventEnvelope) -> Option<String> {
            match env.event.name.as_str() {
                "Created" | "Paid" => Some(env.aggregate_id.clone()),
                _ => None,
            }
        }

        fn handle(&self, state: Option<String>, env: &EventEnvelope) -> (Option<String>, Vec<String>) {
            match env.event.name.as_str() {
                "Created" => (Some("open".to_string()), Vec::new()),
                "Paid" => {
                    if state.is_some() {
                        // Complete the instance and emit a fulfillment command.
                        (None, vec![format!("Fulfill:{}", env.aggregate_id)])
                    } else {
                        // No live instance (e.g. redelivery after completion): nothing to do.
                        (None, Vec::new())
                    }
                }
                _ => (state, Vec::new()),
            }
        }
    }

    fn envelope(aggregate_id: &str, name: &str) -> EventEnvelope {
        EventEnvelope {
            id: format!("{aggregate_id}-{name}"),
            aggregate_id: aggregate_id.to_string(),
            event: Event {
                name: name.to_string(),
                version: 1,
                data: Vec::new(),
            },
            sequence: Some(1),
            namespace: Default::default(),
            metadata: Default::default(),
            timestamp: 0,
        }
    }

    #[test]
    fn saga_lifecycle() {
        let runner = SagaRunner::new(OrderSaga, FailingSagaStore::default());
        let dispatched = RefCell::new(Vec::<String>::new());
        let mut dispatch = |cmd: &String| {
            dispatched.borrow_mut().push(cmd.clone());
            Ok(())
        };

        // Unrelated event: ignored, dispatches nothing, stores nothing.
        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Shipped"), &mut dispatch)
            .unwrap();
        assert!(dispatched.borrow().is_empty());

        // Created: saves state, no commands.
        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Created"), &mut dispatch)
            .unwrap();
        assert!(dispatched.borrow().is_empty());

        // Paid: dispatches one command and deletes state.
        runner.run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut dispatch).unwrap();
        assert_eq!(*dispatched.borrow(), vec!["Fulfill:ord-1".to_string()]);

        // Redelivered Paid (state already gone): dispatches nothing more.
        runner.run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut dispatch).unwrap();
        assert_eq!(dispatched.borrow().len(), 1);
    }

    #[test]
    fn saga_partial_failure_redelivers_at_least_once() {
        // The runner dispatches before it persists, so a failure on either side leaves the instance
        // alive and the event redeliverable — at-least-once, with duplicate commands the idempotent
        // bus is expected to absorb. This pins that: state never advances past a failed step, and
        // the completed instance stays completed.
        let store = FailingSagaStore::default();
        let runner = SagaRunner::new(OrderSaga, store);
        let dispatched = RefCell::new(Vec::<String>::new());

        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Created"), &mut |_: &String| Ok(()))
            .unwrap();

        // Dispatch fails: the instance must survive so the redelivery can re-emit the command.
        let out = runner.run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut |cmd: &String| {
            dispatched.borrow_mut().push(cmd.clone());
            Err(Error::Store("command bus unavailable".into()))
        });
        assert!(matches!(out, Err(Error::Store(_))));
        assert_eq!(dispatched.borrow().len(), 1);

        // Redelivery: the command dispatches, but persisting the completion fails. The instance is
        // still open, so the event redelivers again and the command is emitted a second time.
        runner.store.fail_write.set(true);
        let out = runner.run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut |cmd: &String| {
            dispatched.borrow_mut().push(cmd.clone());
            Ok(())
        });
        assert!(matches!(out, Err(Error::Store(_))));
        assert_eq!(dispatched.borrow().len(), 2);
        assert_eq!(
            runner.store.load(&mut MemoryTx::auto_commit(), "ord-1").unwrap().map(|i| i.state).as_deref(),
            Some("open")
        );

        // Healthy redelivery: completes and removes the instance (third dispatch of the same command).
        runner.store.fail_write.set(false);
        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut |cmd: &String| {
                dispatched.borrow_mut().push(cmd.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(dispatched.borrow().len(), 3);
        assert!(runner.store.load(&mut MemoryTx::auto_commit(), "ord-1").unwrap().is_none());

        // A fourth redelivery finds no instance: no command, no error.
        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut |cmd: &String| {
                dispatched.borrow_mut().push(cmd.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(dispatched.borrow().len(), 3);
    }

    #[test]
    fn an_event_with_no_instance_and_no_new_state_writes_nothing() {
        // `handle` produced neither state nor commands and nothing was loaded, so the run must not
        // touch the store: a versioned delete would be a write a concurrent creator could conflict
        // with. `fail_write` makes any write at all fail, so a clean run proves none happened.
        let runner = SagaRunner::new(OrderSaga, FailingSagaStore::default());
        runner.store.fail_write.set(true);
        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut |_: &String| Ok(()))
            .expect("an ignored correlation writes nothing");
        assert!(runner.store.load(&mut MemoryTx::auto_commit(), "ord-1").unwrap().is_none());
    }

    /// The saga's step, as a command the bus can route.
    #[derive(Serialize)]
    struct Fulfill {
        order_id: String,
    }

    impl Command for Fulfill {
        fn command_type(&self) -> &str {
            "Fulfill"
        }
    }

    /// [`OrderSaga`] emitting a real command type instead of a string.
    struct FulfillSaga;

    impl Saga for FulfillSaga {
        type State = String;
        type Command = Fulfill;

        fn correlation_id(&self, env: &EventEnvelope) -> Option<String> {
            OrderSaga.correlation_id(env)
        }

        fn handle(&self, state: Option<String>, env: &EventEnvelope) -> (Option<String>, Vec<Fulfill>) {
            match env.event.name.as_str() {
                "Created" => (Some("open".to_string()), Vec::new()),
                "Paid" if state.is_some() => (None, vec![Fulfill { order_id: env.aggregate_id.clone() }]),
                _ => (state, Vec::new()),
            }
        }
    }

    /// The documented saga-to-command topology: a runner inside a dispatcher worker, emitting
    /// through one `CommandBus` every worker shares. It only compiles because the command path is
    /// `Arc` + `Send + Sync`.
    #[test]
    fn a_saga_in_a_dispatcher_worker_dispatches_through_a_shared_bus() {
        let fulfilled: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&fulfilled);
        let mut bus: CommandBus<Fulfill, ()> = CommandBus::new();
        bus.register(
            "Fulfill",
            Box::new(move |cmd: &Fulfill| {
                sink.lock().expect("no test thread panics while holding the sink").push(cmd.order_id.clone());
                Ok(())
            }),
        )
        .unwrap();

        let bus = Arc::new(bus);
        let runner = Arc::new(SagaRunner::new(FulfillSaga, MemorySagaStore::<String>::default()));
        let dispatcher = KeyOrderedDispatcher::new(2, move |env: &EventEnvelope| {
            runner.run(&mut MemoryTx::auto_commit(), env, |cmd: &Fulfill| {
                bus.dispatch(cmd).map_err(|e| Error::Store(e.to_string()))
            })
        });

        // Two correlations, so both workers run a saga against the one bus.
        for order in ["ord-1", "ord-2"] {
            dispatcher.submit(envelope(order, "Created"));
            dispatcher.submit(envelope(order, "Paid"));
        }
        dispatcher.stop();

        let mut got = fulfilled.lock().expect("no worker panicked").clone();
        got.sort();
        assert_eq!(got, vec!["ord-1".to_string(), "ord-2".to_string()]);
    }

    #[test]
    fn saga_write_at_loaded_version_catches_a_racing_writer() {
        // The runner persists at the version it *loaded*, not a re-read: a writer that lands between
        // load and persist (here, during dispatch) makes the completion a conflict, never a clobber.
        let runner = SagaRunner::new(OrderSaga, FailingSagaStore::default());
        runner
            .run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Created"), &mut |_: &String| Ok(()))
            .unwrap(); // instance at version 1

        let out = runner.run(&mut MemoryTx::auto_commit(), &envelope("ord-1", "Paid"), &mut |_: &String| {
            runner.store.save(&mut MemoryTx::auto_commit(), "ord-1", &"racing".to_string(), 1).map(|_| ()) // version 2
        });
        assert!(matches!(out, Err(Error::Concurrency { expected: 1, stored: 2 })), "got {out:?}");
        assert_eq!(
            runner.store.load(&mut MemoryTx::auto_commit(), "ord-1").unwrap().map(|i| (i.version, i.state)),
            Some((2, "racing".to_string())),
            "the racing writer's state survives"
        );
    }
}
