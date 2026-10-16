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
//!
//! A command that dies in the relay (its handler rejects it) saves no event, so nothing would tell the
//! instance. Wire [`SagaRunner::command_failed`] (or [`SagaRunner::command_failed_committed`] for a
//! protected runner) as the relay's dead-letter sink: it folds a
//! [`SAGA_COMMAND_FAILED`] event into the instance that sent the command, and the saga compensates.

use edax_event::{
    saga_admit, saga_command, saga_command_failed, saga_write, Cause, CommandMessage, EventMessage, Loaded, SagaAdmit,
    SagaWrite,
};
pub use edax_event::{SagaCommandFailed, SAGA_COMMAND_FAILED};

use crate::failure::{code_of, failure_message, HandlerError};
use crate::outbox::{CountedOutbox, Outbox};
use crate::sealing::{Sealing, StateContext};
use crate::settle::DeadLetter;
use crate::{Error, PayloadCodec};
use edax_event::sealing::{check_owner, decode_protected_state, encode_protected_state, ProtectedState};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

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

/// Where saga instance state and the saga outbox live, keyed by the saga's name and the instance id, with
/// optimistic concurrency. What a backend must guarantee:
///
/// - Every row and every outbox command belongs to the saga named by `saga`: two sagas over one database,
///   as they must be for their writes to join the events' unit of work, never see each other's instance
///   under one instance id nor send each other's commands. Key both tables by `(saga, id)`.
/// - `save` and `complete` write at `version + 1` only if the stored version still equals `version` (an
///   absent instance is at version 0), and return the new version; otherwise they return a
///   [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency) error and write nothing. A completed
///   instance takes no write at all.
/// - The step's `commands` are written in the same write as the state, so they commit or roll back
///   together, and `unpublished` shows them only once committed, oldest first, until `mark_published`
///   prunes them by id, idempotently. Each is the kernel's [`saga_command`] row for that write.
/// - `save` keeps `folded` as it is and `load` returns it unchanged in [`SagaInstance::Running`]: the
///   runner skips an event it already folded by it.
/// - `complete` keeps the row as a tombstone at its new version rather than removing it; a durable store
///   purges tombstones by retention.
///
/// A [`Relay`](crate::Relay) drains one saga's outbox through [`SagaOutbox`], which
/// [`SagaRunner::outbox`] builds.
pub trait SagaStore {
    type State;
    /// Backend transaction handle — the same one the [`Transactor`](crate::Transactor) hands out, so a
    /// saga state write can enlist in the events' unit of work. Use `()` for a store that needs none.
    type Tx;

    /// Instance `id` of `saga`, or `None` if there is none.
    fn load(&self, tx: &mut Self::Tx, saga: &str, id: &str) -> Result<Option<SagaInstance<Self::State>>, Error>;
    /// Persist `state`, `folded` and `commands` for instance `id` of `saga` at `version + 1`; see the
    /// trait for the rule.
    #[allow(
        clippy::too_many_arguments,
        reason = "the port's own arguments: the row's key, its version and its contents"
    )]
    fn save(
        &self,
        tx: &mut Self::Tx,
        saga: &str,
        id: &str,
        state: &Self::State,
        version: u64,
        folded: &str,
        commands: &[CommandMessage],
    ) -> Result<u64, Error>;
    /// Complete instance `id` of `saga`: write its tombstone and `commands` at `version + 1`; see the
    /// trait for the rule.
    fn complete(
        &self,
        tx: &mut Self::Tx,
        saga: &str,
        id: &str,
        version: u64,
        commands: &[CommandMessage],
    ) -> Result<u64, Error>;
    /// Up to `limit` of `saga`'s committed unsent commands, oldest commit first. Outside any unit of work.
    fn unpublished(&self, saga: &str, limit: usize) -> Result<Vec<CommandMessage>, Error>;
    /// Mark `saga`'s commands sent by id. Idempotent; an unknown id is a no-op.
    fn mark_published(&self, saga: &str, ids: &[String]) -> Result<(), Error>;
    /// How many of `saga`'s committed commands wait to be sent, counted without paging them: the backlog
    /// depth [`SagaOutbox`] reports as a [`CountedOutbox`]. Outside any unit of work.
    fn unpublished_count(&self, saga: &str) -> Result<usize, Error>;
}

/// One saga's command outbox over its store: the [`Outbox`] a [`Relay`](crate::Relay) drains. Built by
/// [`SagaRunner::outbox`], so the relay reads the rows of the saga the runner writes and no other.
#[derive(Clone)]
pub struct SagaOutbox<ST> {
    store: ST,
    saga: String,
}

impl<ST> SagaOutbox<ST> {
    /// The outbox of `saga` in `store`.
    pub fn new(store: ST, saga: impl Into<String>) -> Self {
        Self {
            store,
            saga: saga.into(),
        }
    }

    /// The store the outbox reads.
    pub fn store(&self) -> &ST {
        &self.store
    }
}

impl<ST: SagaStore> Outbox<CommandMessage> for SagaOutbox<ST> {
    fn unpublished(&self, limit: usize) -> Result<Vec<CommandMessage>, Error> {
        self.store.unpublished(&self.saga, limit)
    }

    fn mark_published(&self, ids: &[String]) -> Result<(), Error> {
        self.store.mark_published(&self.saga, ids)
    }
}

impl<ST: SagaStore> CountedOutbox<CommandMessage> for SagaOutbox<ST> {
    fn unpublished_count(&self) -> Result<usize, Error> {
        self.store.unpublished_count(&self.saga)
    }
}

/// Drives a [`Saga`] over committed events: route to an instance, fold, and persist the step, state and
/// commands in one write. Pass [`SagaRunner::run`] (closing over a `tx`), or
/// [`SagaRunner::run_committed`] for protected state, to an event router / consumer as the handler,
/// and drain the saga's command outbox ([`SagaRunner::outbox`]) with a [`Relay`](crate::Relay),
/// decoding each row with [`SagaRunner::command_delivery`]. A `None` result settles the row without
/// publishing: return `Ok(())` from the relay's publish closure so it marks the row published.
pub struct SagaRunner<SG: Saga, ST: SagaStore, CC> {
    saga: SG,
    store: ST,
    commands: CC,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
    sealing: Option<Arc<Sealing>>,
    decode_state: DecodeState<SG::State, ST::State>,
    encode_state: EncodeState<SG::State, ST::State>,
}

type DecodeState<S, Stored> = fn(Stored, &StateContext<'_>, Option<&Sealing>) -> Result<(Option<S>, bool), Error>;
type EncodeState<S, Stored> = fn(Option<S>, &StateContext<'_>, Option<&Sealing>) -> Result<Stored, Error>;

impl<SG, ST, CC> SagaRunner<SG, ST, CC>
where
    SG: Saga,
    ST: SagaStore,
    CC: PayloadCodec<Domain = SG::Command>,
{
    /// A runner folding with `saga`, persisting to `store`, and marshaling commands with `commands`.
    pub fn new(saga: SG, store: ST, commands: CC) -> Self
    where
        ST: SagaStore<State = SG::State>,
    {
        Self {
            saga,
            store,
            commands,
            now: Box::new(edax_core::current_timestamp),
            sealing: None,
            decode_state: |state, _, _| Ok((Some(state), false)),
            encode_state: |state, _, _| state.ok_or_else(|| Error::codec("plain saga completion has no state")),
        }
    }

    /// Protect arbitrary serializable domain state using a store whose state is an opaque JSON value.
    /// The kernel envelope remains a JSON object, matching the Python and Go storage contract.
    /// Use `run_committed`: protected runners must retain the provider fence through commit.
    pub fn new_sealed(saga: SG, store: ST, commands: CC, sealing: Arc<Sealing>) -> Self
    where
        ST: SagaStore<State = serde_json::Value>,
        SG::State: Serialize + DeserializeOwned,
    {
        Self {
            saga,
            store,
            commands,
            now: Box::new(edax_core::current_timestamp),
            sealing: Some(sealing),
            decode_state: |stored, context, sealing| {
                let sealing = sealing.ok_or_else(|| Error::bad_handle("protected saga requires sealing"))?;
                let stored = serde_json::to_string(&stored).map_err(Error::codec)?;
                let protected = decode_protected_state(&stored)?;
                let (plaintext, completed) = match protected {
                    Some(protected) => {
                        check_owner(true, protected.tenant.as_deref(), context.tenant)?;
                        check_owner(true, protected.subject.as_deref(), context.subject)?;
                        let bound = StateContext {
                            completed: protected.completed,
                            ..*context
                        };
                        (
                            sealing.open_state(&bound, Some(&protected.encryption), &protected.data)?,
                            protected.completed,
                        )
                    }
                    None => (sealing.open_state(context, None, stored.as_bytes())?, false),
                };
                if completed {
                    // The encrypted completion contains no application state and is authenticated first.
                    let value: serde_json::Value = serde_json::from_slice(&plaintext).map_err(Error::codec)?;
                    if !value.is_null() {
                        return Err(Error::codec("protected saga completion contains application state"));
                    }
                    Ok((None, true))
                } else {
                    Ok((Some(serde_json::from_slice(&plaintext).map_err(Error::codec)?), false))
                }
            },
            encode_state: |state, context, sealing| {
                let sealing = sealing.ok_or_else(|| Error::bad_handle("protected saga requires sealing"))?;
                let data = match state {
                    Some(state) => serde_json::to_vec(&state).map_err(Error::codec)?,
                    None => b"null".to_vec(),
                };
                let sealed = sealing.seal_state(context, &data)?;
                let encoded = encode_protected_state(&ProtectedState::new(
                    context.tenant.map(str::to_owned),
                    context.subject.map(str::to_owned),
                    context.completed,
                    sealed.encryption,
                    sealed.data,
                ))?;
                serde_json::from_str(&encoded).map_err(Error::codec)
            },
        }
    }

    /// Override the clock that stamps each command's `issued_at`, for deterministic tests.
    pub fn with_clock(mut self, now: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Box::new(now);
        self
    }

    /// The store the runner persists to.
    pub fn store(&self) -> &ST {
        &self.store
    }

    /// This saga's command outbox over a clone of the store: build the command [`Relay`](crate::Relay)
    /// over it.
    pub fn outbox(&self) -> SagaOutbox<ST>
    where
        ST: Clone,
    {
        SagaOutbox::new(self.store.clone(), self.saga.name())
    }

    /// Decode an outbox row explicitly. Destroyed keys raise `Shredded`; relay consumers use
    /// [`Self::command_delivery`] to settle those rows without publishing them.
    pub fn command(&self, row: &CommandMessage) -> Result<SG::Command, Error> {
        if let Some(sealing) = &self.sealing {
            let mut opened = row.clone();
            opened.data = sealing.open_command(row)?;
            opened.encryption = None;
            return self.commands.unmarshal(&opened.payload());
        }
        if row.encryption.is_some() {
            return Err(Error::bad_handle("encrypted saga commands require sealing"));
        }
        self.commands.unmarshal(&row.payload())
    }

    /// Decode a command for delivery. `None` means trusted scoped key resolution confirmed destruction.
    /// Return success from the relay's publish closure for `None`, without invoking a command handler.
    /// Malformed envelopes, authentication failures and codec errors remain errors.
    pub fn command_delivery(&self, row: &CommandMessage) -> Result<Option<SG::Command>, Error> {
        let Some(sealing) = &self.sealing else {
            return self.command(row).map(Some);
        };
        let Some(data) = sealing.open_command_delivery(row)? else {
            return Ok(None);
        };
        let mut payload = row.payload();
        payload.data = data;
        self.commands.unmarshal(&payload).map(Some)
    }

    /// Handle one committed event: load its instance, fold the event, and write the new state
    /// (or the tombstone) with the commands the fold emitted, at the loaded version. Thread the same `tx`
    /// the events committed under so the write joins that unit of work. A conflict means another worker
    /// advanced the instance first; its error is transient, so the event is redelivered and folds the
    /// winner's state, and nothing this fold decided is ever sent. An event the instance already folded
    /// (redelivered after its step committed) is skipped: no fold, no write, no command. So is an event
    /// whose sequence is below the one the instance folded from its aggregate: in order delivery folded
    /// it before, but a dead-lettered event re-driven after a later one lands there too, and never folds.
    pub fn run(&self, tx: &mut ST::Tx, event: &EventMessage) -> Result<(), HandlerError> {
        if self.sealing.is_some() {
            return Err(Error::bad_handle(
                "protected saga writes require run_committed to retain the scope fence through commit",
            )
            .into());
        }
        let raw = crate::router::original_event(event);
        if raw.encryption.is_some() {
            return Err(Error::bad_handle("encrypted saga events require sealing").into());
        }
        self.run_view(tx, event, &raw)
    }

    fn run_view(&self, tx: &mut ST::Tx, event: &EventMessage, raw: &EventMessage) -> Result<(), HandlerError> {
        let Some(instance_id) = self.saga.instance_id(event) else {
            return Ok(());
        };
        self.fold(tx, &instance_id, event, raw)
    }

    /// Authenticate and fold while retaining the write fence through this transaction's commit.
    /// An ambient transaction is rejected: this runner cannot extend a borrowed transaction's lifetime.
    pub fn run_committed<T: crate::Transactor<Tx = ST::Tx>>(
        &self,
        transactor: &T,
        event: &EventMessage,
    ) -> Result<(), HandlerError> {
        let raw = crate::router::original_event(event);
        let event = &raw;
        if crate::unit_of_work::in_unit_of_work() {
            return Err(Error::bad_handle("run_committed cannot join an ambient transaction").into());
        }
        let Some(sealing) = &self.sealing else {
            return transactor.transaction(|tx| self.run(tx, event));
        };
        let Some(data) = sealing.open_event_delivery(event)? else {
            return Ok(());
        };
        let scope = sealing.scope(event.tenant.as_deref(), event.subject.as_deref())?;
        let _guard = match sealing.hold(&scope) {
            Err(error) if error.code == crate::ErrorCode::Shredded => return Ok(()),
            result => result?,
        };
        let mut opened = event.clone();
        opened.data = data;
        opened.encryption = None;
        transactor.transaction(|tx| self.run_view(tx, &opened, event))
    }

    /// The dead-letter sink for this runner's command relay: fold the [`SAGA_COMMAND_FAILED`] event for
    /// the dead row into the instance that sent it, so the saga can compensate. The event's payload is
    /// [`SagaCommandFailed`]: the command's id and name, and the failure's code and message. The fold
    /// joins `tx`, and skips the event as `run` would: a completed instance folds nothing, so a saga that
    /// compensates stays open until its commands have run. A failed fold fails the sink, so the relay
    /// hands the row back and the sink runs again.
    pub fn command_failed(
        &self,
        tx: &mut ST::Tx,
        dead: DeadLetter<&CommandMessage, &HandlerError>,
    ) -> Result<(), HandlerError> {
        if self.sealing.is_some() {
            return Err(Error::bad_handle("protected saga failures require command_failed_committed").into());
        }
        if dead.message.encryption.is_some() {
            return Err(Error::bad_handle("encrypted saga commands require sealing").into());
        }
        self.fold_failure(tx, dead)
    }

    /// Authenticate a dead command and retain the scope fence through compensation's commit.
    pub fn command_failed_committed<T: crate::Transactor<Tx = ST::Tx>>(
        &self,
        transactor: &T,
        dead: DeadLetter<&CommandMessage, &HandlerError>,
    ) -> Result<(), HandlerError> {
        if crate::unit_of_work::in_unit_of_work() {
            return Err(Error::bad_handle("command_failed_committed cannot join an ambient transaction").into());
        }
        let Some(sealing) = &self.sealing else {
            return transactor.transaction(|tx| self.command_failed(tx, dead));
        };
        if sealing.open_command_delivery(dead.message)?.is_none() {
            return Ok(());
        }
        let scope = sealing.scope(dead.message.tenant.as_deref(), dead.message.subject.as_deref())?;
        let _guard = match sealing.hold(&scope) {
            Err(error) if error.code == crate::ErrorCode::Shredded => return Ok(()),
            result => result?,
        };
        transactor.transaction(|tx| self.fold_failure(tx, dead))
    }

    fn fold_failure(
        &self,
        tx: &mut ST::Tx,
        dead: DeadLetter<&CommandMessage, &HandlerError>,
    ) -> Result<(), HandlerError> {
        let error = dead.failure;
        let event = saga_command_failed(
            dead.message,
            code_of(&**error),
            &failure_message(&**error),
            (self.now)(),
        )?;
        self.fold(tx, &event.aggregate_id, &event, &event)
    }

    /// Fold `event` into the instance `instance_id` and write the step.
    fn fold(
        &self,
        tx: &mut ST::Tx,
        instance_id: &str,
        event: &EventMessage,
        raw: &EventMessage,
    ) -> Result<(), HandlerError> {
        let saga = self.saga.name();
        let (loaded, state, folded) = match self.store.load(tx, saga, instance_id)? {
            None => (Loaded::Absent, None, String::new()),
            Some(SagaInstance::Running { state, version, folded }) => {
                let context = StateContext {
                    kind: "saga",
                    identity: instance_id,
                    type_name: saga,
                    version,
                    tenant: raw.tenant.as_deref(),
                    subject: raw.subject.as_deref(),
                    marks: &folded,
                    completed: false,
                };
                let (state, completed) = (self.decode_state)(state, &context, self.sealing.as_deref())?;
                (
                    if completed {
                        Loaded::Completed { version }
                    } else {
                        Loaded::Active { version }
                    },
                    state,
                    folded,
                )
            }
            Some(SagaInstance::Completed { version }) => {
                if let Some(sealing) = &self.sealing {
                    let context = StateContext {
                        kind: "saga",
                        identity: instance_id,
                        type_name: saga,
                        version,
                        tenant: raw.tenant.as_deref(),
                        subject: raw.subject.as_deref(),
                        marks: "",
                        completed: true,
                    };
                    // Historical native tombstones have no authenticated owner or encryption record.
                    // Only explicit legacy compatibility may read them in a protected runner.
                    sealing.open_state(&context, None, b"null")?;
                }
                (Loaded::Completed { version }, None, String::new())
            }
        };
        // The kernel skips a tombstone, an event the instance folded already and one below its mark:
        // no fold, no write.
        let SagaAdmit::Fold { folded } = saga_admit(saga, loaded, &folded, raw)? else {
            return Ok(());
        };
        let (new_state, commands) = self.saga.handle(state, event);
        let write = saga_write(loaded, new_state.is_none(), !commands.is_empty())?;
        match (write, new_state) {
            (SagaWrite::Nothing, _) => {}
            (SagaWrite::Save { expected, version }, Some(state)) => {
                let rows = self.rows(instance_id, version, &commands, event)?;
                let context = StateContext {
                    kind: "saga",
                    identity: instance_id,
                    type_name: saga,
                    version,
                    tenant: raw.tenant.as_deref(),
                    subject: raw.subject.as_deref(),
                    marks: &folded,
                    completed: false,
                };
                let state = (self.encode_state)(Some(state), &context, self.sealing.as_deref())?;
                self.store
                    .save(tx, saga, instance_id, &state, expected, &folded, &rows)?;
            }
            (SagaWrite::Complete { expected, version }, _) => {
                let rows = self.rows(instance_id, version, &commands, event)?;
                if self.sealing.is_some() {
                    let context = StateContext {
                        kind: "saga",
                        identity: instance_id,
                        type_name: saga,
                        version,
                        tenant: raw.tenant.as_deref(),
                        subject: raw.subject.as_deref(),
                        marks: &folded,
                        completed: true,
                    };
                    let state = (self.encode_state)(None, &context, self.sealing.as_deref())?;
                    self.store
                        .save(tx, saga, instance_id, &state, expected, &folded, &rows)?;
                } else {
                    self.store.complete(tx, saga, instance_id, expected, &rows)?;
                }
            }
            // The kernel answers `Save` only for a fold that keeps state, so this is a kernel change no
            // runner arm covers: refused the same on every retry.
            (SagaWrite::Save { .. }, None) => {
                return Err(Error::codec("the kernel saves a saga step that keeps no state").into())
            }
        }
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
                let row = saga_command(
                    self.saga.name(),
                    instance_id,
                    version,
                    index,
                    Cause::from(cause),
                    payload,
                    issued_at,
                );
                match &self.sealing {
                    Some(sealing) => sealing.seal_command(row),
                    None => Ok(row),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemorySagaStore, MemoryTx};
    use crate::testkit::event;
    use crate::{
        idempotent, Command, CommandBus, ErrorCode, Handler, JsonCodec, JsonRegistryCodec, Pacing, Relay, Terminal,
        Transactor,
    };
    use serde::{Deserialize, Serialize};
    use std::cell::{Cell, RefCell};
    use std::sync::{Arc, Mutex};

    #[derive(Serialize, Deserialize)]
    struct PrivateState {
        email: String,
    }

    struct PrivateSaga {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Saga for PrivateSaga {
        type State = PrivateState;
        type Command = OrderCommand;
        fn name(&self) -> &str {
            "private-saga"
        }
        fn instance_id(&self, _: &EventMessage) -> Option<String> {
            Some("workflow".into())
        }
        fn handle(
            &self,
            state: Option<PrivateState>,
            event: &EventMessage,
        ) -> (Option<PrivateState>, Vec<OrderCommand>) {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(event.data, b"private@example.test");
            assert!(event.encryption.is_none());
            if let Some(state) = state {
                assert_eq!(state.email, "private@example.test");
            }
            if event.name == "Complete" {
                return (None, vec![]);
            }
            (
                Some(PrivateState {
                    email: "private@example.test".into(),
                }),
                vec![OrderCommand::Fulfill(Fulfill {
                    order_id: "private@example.test".into(),
                })],
            )
        }
    }

    fn private_event(sealing: &Sealing, version: u64, name: &str, subject: &str) -> EventMessage {
        let mut draft = crate::testkit::draft("aggregate", name, version, b"private@example.test");
        draft.subject = Some(subject.into());
        sealing.seal_event(draft).unwrap().record(42)
    }

    #[test]
    fn sealed_saga_command_delivery_distinguishes_destruction_from_invalid_rows() {
        use crate::backend::MemoryTransactor;
        use crate::sealing::{Cipher, KeyProvider, MemoryKeys};
        for (case, expected) in [
            ("valid", None),
            ("bad ciphertext", Some(ErrorCode::Codec)),
            ("bad nonce", Some(ErrorCode::Codec)),
            ("bad nonce after destruction", Some(ErrorCode::Codec)),
            ("bad payload", Some(ErrorCode::Codec)),
            ("unknown generation", Some(ErrorCode::KeyMissing)),
            ("destroyed", None),
        ] {
            let keys = Arc::new(MemoryKeys::new());
            let sealing = Arc::new(Sealing::new(keys.clone(), "production", Cipher::default()).unwrap());
            let db = MemoryTransactor::new();
            let runner = SagaRunner::new_sealed(
                PrivateSaga { calls: Arc::default() },
                db.saga_store::<serde_json::Value>(),
                JsonRegistryCodec::new().unwrap(),
                sealing.clone(),
            );
            runner
                .run_committed(&db, &private_event(&sealing, 1, "Changed", "person"))
                .unwrap();
            let mut row = runner.outbox().unpublished(1).unwrap().remove(0);
            match case {
                "bad ciphertext" => row.data[0] ^= 1,
                "bad nonce" => row.encryption.as_mut().unwrap().nonce.clear(),
                "bad nonce after destruction" => {
                    row.encryption.as_mut().unwrap().nonce.clear();
                    keys.destroy(&sealing.scope(None, Some("person")).unwrap()).unwrap();
                }
                "bad payload" => {
                    row.encryption = None;
                    row.data = b"{".to_vec();
                    row = sealing.seal_command(row).unwrap();
                }
                "unknown generation" => row.encryption.as_mut().unwrap().key_id = "missing-generation".into(),
                "destroyed" => {
                    keys.destroy(&sealing.scope(None, Some("person")).unwrap()).unwrap();
                }
                _ => {}
            }
            let result = runner.command_delivery(&row);
            if let Some(expected) = expected {
                assert_eq!(result.unwrap_err().code, expected, "{case}");
            } else if case == "destroyed" {
                assert!(result.unwrap().is_none());
                assert_eq!(runner.command(&row).unwrap_err().code, ErrorCode::Shredded);
                let relay = Relay::<_, CommandMessage>::new(runner.outbox());
                assert_eq!(
                    relay
                        .drain(10, |raw| {
                            assert!(runner.command_delivery(raw)?.is_none());
                            Ok(())
                        })
                        .unwrap(),
                    1
                );
                assert!(runner.outbox().unpublished(1).unwrap().is_empty());
            } else {
                assert_eq!(
                    result.unwrap(),
                    Some(OrderCommand::Fulfill(Fulfill {
                        order_id: "private@example.test".into()
                    }))
                );
            }
        }
    }

    #[test]
    fn sealed_saga_failure_compensates_once_without_persisting_error_text() {
        use crate::backend::MemoryTransactor;
        use crate::sealing::{Cipher, MemoryKeys};
        let sealing = Arc::new(Sealing::new(Arc::new(MemoryKeys::new()), "production", Cipher::default()).unwrap());
        let db = MemoryTransactor::new();
        let store = db.saga_store::<serde_json::Value>();
        let saga = OrderSaga::default();
        let failures = saga.failures.clone();
        let runner = SagaRunner::new_sealed(saga, store.clone(), JsonRegistryCodec::new().unwrap(), sealing.clone());
        for (version, name) in [(1, "Created"), (2, "Shipped")] {
            let mut draft = crate::testkit::draft("order-7", name, version, br#"{"order_id":"order-7"}"#);
            draft.subject = Some("person-42".into());
            draft.tenant = Some("tenant-7".into());
            draft.metadata.insert("order".into(), "order-7".into());
            runner
                .run_committed(&db, &sealing.seal_event(draft).unwrap().record(42))
                .unwrap();
        }
        let failed = runner.outbox().unpublished(1).unwrap().remove(0);
        let secret = "private@example.test: card declined at 17 Example Lane";
        let failure: HandlerError = Error::new(ErrorCode::Rejected, secret).into();
        for _ in 0..2 {
            runner
                .command_failed_committed(
                    &db,
                    DeadLetter {
                        message: &failed,
                        attempt: 1,
                        failure: &failure,
                    },
                )
                .unwrap();
        }
        let failures = failures.lock().unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].command_id, failed.id);
        assert_eq!(failures[0].code.as_deref(), Some("rejected"));
        assert_eq!(failures[0].error, "");
        let Some(SagaInstance::Running { state, version, folded }) =
            store.load(&mut MemoryTx::auto_commit(), "order", "order-7").unwrap()
        else {
            panic!("missing protected completion");
        };
        assert_eq!(version, 3);
        assert!(!state.to_string().contains(secret));
        assert!(!folded.contains(secret));
        let protected = decode_protected_state(&state.to_string()).unwrap().unwrap();
        assert!(protected.completed);
        assert_eq!(protected.subject.as_deref(), Some("person-42"));
        assert_eq!(protected.tenant.as_deref(), Some("tenant-7"));
        let rows = runner.outbox().unpublished(10).unwrap();
        assert_eq!(rows.len(), 2, "one original command and one compensation");
        for row in &rows {
            assert_eq!(row.subject.as_deref(), Some("person-42"));
            assert_eq!(row.tenant.as_deref(), Some("tenant-7"));
            assert!(row.encryption.is_some());
            let plaintext = sealing.open_command(row).unwrap();
            assert!(!String::from_utf8(plaintext).unwrap().contains(secret));
        }
        assert_eq!(
            runner.command(&rows[1]).unwrap(),
            OrderCommand::Refund(Refund {
                order_id: "order-7".into()
            })
        );
    }

    #[test]
    fn sealed_saga_keeps_provider_guard_until_commit_returns() {
        use crate::backend::MemoryTransactor;
        use crate::sealing::{Cipher, KeyProvider, MemoryKeys};
        struct CommitProbe {
            db: MemoryTransactor,
            keys: Arc<MemoryKeys>,
            scope: edax_seal::KeyScope,
        }
        impl Transactor for CommitProbe {
            type Tx = MemoryTx;
            fn identity(&self) -> usize {
                self.db.identity()
            }
            fn begin(&self) -> Result<Self::Tx, Error> {
                self.db.begin()
            }
            fn commit(&self, tx: Self::Tx) -> Result<(), Error> {
                // MemoryKeys refuses same-thread revocation while admitted work is still fenced.
                assert_eq!(self.keys.revoke(&self.scope).unwrap_err().code, ErrorCode::BadHandle);
                self.db.commit(tx)
            }
            fn rollback(&self, tx: Self::Tx) {
                self.db.rollback(tx);
            }
        }
        let keys = Arc::new(MemoryKeys::new());
        let sealing = Arc::new(Sealing::new(keys.clone(), "production", Cipher::default()).unwrap());
        let db = MemoryTransactor::new();
        let runner = SagaRunner::new_sealed(
            PrivateSaga { calls: Arc::default() },
            db.saga_store::<serde_json::Value>(),
            JsonRegistryCodec::new().unwrap(),
            sealing.clone(),
        );
        let transactor = CommitProbe {
            db,
            keys: keys.clone(),
            scope: sealing.scope(None, Some("person")).unwrap(),
        };
        runner
            .run_committed(&transactor, &private_event(&sealing, 1, "Changed", "person"))
            .unwrap();
        keys.revoke(&transactor.scope).unwrap();
    }

    #[test]
    fn sealed_router_preserves_raw_envelope_for_nested_saga() {
        use crate::backend::MemoryTransactor;
        use crate::sealing::{Cipher, MemoryKeys};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sealing = Arc::new(Sealing::new(Arc::new(MemoryKeys::new()), "production", Cipher::default()).unwrap());
        let db = MemoryTransactor::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = SagaRunner::new_sealed(
            PrivateSaga { calls: calls.clone() },
            db.saga_store::<serde_json::Value>(),
            JsonRegistryCodec::new().unwrap(),
            sealing.clone(),
        );
        let mut router = crate::EventRouter::default().sealing(sealing.clone());
        router
            .register("Changed", Box::new(move |view| runner.run_committed(&db, view)))
            .unwrap();
        let event = private_event(&sealing, 1, "Changed", "person");
        router.dispatch(&event).unwrap();
        router.dispatch(&event).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sealed_saga_encrypts_state_commands_and_owner_preserving_completion() {
        use crate::backend::MemoryTransactor;
        use crate::sealing::{Cipher, KeyProvider, MemoryKeys};
        use std::sync::atomic::{AtomicUsize, Ordering};
        for cipher in [Cipher::Aes256Gcm, Cipher::XChaCha20Poly1305] {
            let keys = Arc::new(MemoryKeys::new());
            let sealing = Arc::new(Sealing::new(keys.clone(), "production", cipher).unwrap());
            let db = MemoryTransactor::new();
            let store = db.saga_store::<serde_json::Value>();
            let calls = Arc::new(AtomicUsize::new(0));
            let runner = SagaRunner::new_sealed(
                PrivateSaga { calls: calls.clone() },
                store.clone(),
                JsonRegistryCodec::new().unwrap(),
                sealing.clone(),
            );
            let first = private_event(&sealing, 1, "Changed", "person");
            runner.run_committed(&db, &first).unwrap();
            runner.run_committed(&db, &first).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let Some(SagaInstance::Running { state, .. }) = store
                .load(&mut MemoryTx::auto_commit(), "private-saga", "workflow")
                .unwrap()
            else {
                panic!("running protected state");
            };
            let record = state.to_string();
            assert!(!record.contains("private@example.test"));
            let protected = decode_protected_state(&record).unwrap().unwrap();
            assert_eq!(protected.subject.as_deref(), Some("person"));
            assert!(!protected.completed);
            let outbox = runner.outbox().unpublished(100).unwrap();
            assert_eq!(outbox.len(), 1);
            assert!(outbox[0].encryption.is_some());
            assert_eq!(
                runner.command(&outbox[0]).unwrap(),
                OrderCommand::Fulfill(Fulfill {
                    order_id: "private@example.test".into()
                })
            );
            let other = private_event(&sealing, 2, "Changed", "different-person");
            assert!(runner.run_committed(&db, &other).is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let finished = private_event(&sealing, 2, "Complete", "person");
            runner.run_committed(&db, &finished).unwrap();
            runner.run_committed(&db, &finished).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            let Some(SagaInstance::Running { state, version, folded }) = store
                .load(&mut MemoryTx::auto_commit(), "private-saga", "workflow")
                .unwrap()
            else {
                panic!("protected completion record");
            };
            let protected = decode_protected_state(&state.to_string()).unwrap().unwrap();
            assert!(protected.completed);
            let context = StateContext {
                kind: "saga",
                identity: "workflow",
                type_name: "private-saga",
                version,
                tenant: None,
                subject: Some("person"),
                marks: &folded,
                completed: true,
            };
            assert_eq!(
                sealing
                    .open_state(&context, Some(&protected.encryption), &protected.data)
                    .unwrap(),
                b"null"
            );
            assert!(runner.run_committed(&db, &other).is_err());
            let rewrite = store.save(
                &mut MemoryTx::auto_commit(),
                "private-saga",
                "workflow",
                &state,
                version,
                &folded,
                &[],
            );
            assert_eq!(rewrite.unwrap_err().code, ErrorCode::Concurrency);
            let scope = sealing.scope(None, Some("person")).unwrap();
            keys.revoke(&scope).unwrap();
            keys.destroy(&scope).unwrap();
            runner.run_committed(&db, &finished).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }

    #[test]
    fn sealed_saga_refuses_borrowed_transactions_plaintext_and_tampered_state() {
        use crate::backend::MemoryTransactor;
        use crate::sealing::{Cipher, MemoryKeys};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sealing = Arc::new(Sealing::new(Arc::new(MemoryKeys::new()), "production", Cipher::default()).unwrap());
        let db = MemoryTransactor::new();
        let store = db.saga_store::<serde_json::Value>();
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = SagaRunner::new_sealed(
            PrivateSaga { calls: calls.clone() },
            store.clone(),
            JsonRegistryCodec::new().unwrap(),
            sealing.clone(),
        );
        let first = private_event(&sealing, 1, "Changed", "person");
        assert!(runner.run(&mut MemoryTx::auto_commit(), &first).is_err());
        let ambient: Result<(), HandlerError> = db.transaction(|_| runner.run_committed(&db, &first));
        assert!(ambient.is_err());
        let mut plaintext = first.clone();
        plaintext.encryption = None;
        assert!(runner.run_committed(&db, &plaintext).is_err());
        runner.run_committed(&db, &first).unwrap();
        let Some(SagaInstance::Running { state, version, folded }) = store
            .load(&mut MemoryTx::auto_commit(), "private-saga", "workflow")
            .unwrap()
        else {
            panic!("protected state");
        };
        let mut protected = decode_protected_state(&state.to_string()).unwrap().unwrap();
        protected.completed = true;
        let forged = serde_json::from_str(&encode_protected_state(&protected).unwrap()).unwrap();
        store
            .save(
                &mut MemoryTx::auto_commit(),
                "private-saga",
                "workflow",
                &forged,
                version,
                &folded,
                &[],
            )
            .unwrap();
        assert!(runner
            .run_committed(&db, &private_event(&sealing, 2, "Changed", "person"))
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // --- an order saga over a cross-aggregate instance ---

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Fulfill {
        order_id: String,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Refund {
        order_id: String,
    }

    impl Command for Fulfill {
        const TYPE: &'static str = "Fulfill";
        type Output = ();
    }

    impl Command for Refund {
        const TYPE: &'static str = "Refund";
        type Output = ();
    }

    /// The commands the order saga emits: its codec writes each variant's own value, so a row's payload
    /// is the command's, as a Python saga writes it. The bus never sees the enum: [`send`] runs each row
    /// as the command its name names.
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum OrderCommand {
        Fulfill(Fulfill),
        Refund(Refund),
    }

    /// Run the outbox `row` on `bus` as the command its name names: the relay's publish. The message is
    /// what the guard claims, so a re-sent row replays under its own idempotency key.
    fn send(bus: &CommandBus, row: &CommandMessage) -> Result<(), HandlerError> {
        match row.name.as_str() {
            Fulfill::TYPE => bus.dispatch_message::<Fulfill>(row),
            Refund::TYPE => bus.dispatch_message::<Refund>(row),
            other => Err(Error::new(ErrorCode::HandlerNotFound, other).into()),
        }
    }

    /// `Created` opens the instance, `Paid` fulfills it and `Cancelled` refunds it, both completing it.
    /// `Shipped` fulfills it and waits: a failed `Fulfill` refunds and completes it.
    /// The instance is the order, while the events come from the order and the payment aggregates:
    /// two workers can fold one instance at once, which is what the version is for.
    #[derive(Default)]
    struct OrderSaga {
        /// The payload of every failure it folded, so a test reads what the sink built.
        failures: Arc<Mutex<Vec<SagaCommandFailed>>>,
    }

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
                // A step that waits for its command's outcome: a dead `Fulfill` comes back as a failure.
                (Some("open"), "Shipped") => (
                    Some("shipping".to_owned()),
                    vec![OrderCommand::Fulfill(Fulfill { order_id })],
                ),
                (Some("shipping"), SAGA_COMMAND_FAILED) => {
                    self.failures
                        .lock()
                        .expect("no panics")
                        .push(serde_json::from_slice(&event.data).expect("the kernel's payload"));
                    (
                        None,
                        vec![OrderCommand::Refund(Refund {
                            order_id: "order-7".into(),
                        })],
                    )
                }
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
        SagaRunner::new(OrderSaga::default(), store, JsonRegistryCodec::new().unwrap())
            .with_clock(|| 1_714_564_800_000_000_000)
    }

    /// Drain every committed command through `send`, recording the decoded commands.
    fn drain_all(runner: &Runner<MemorySagaStore<String>>) -> Vec<OrderCommand> {
        let relay = Relay::<_, CommandMessage>::new(runner.outbox()).pacing(Pacing::instant());
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
        store.load(&mut MemoryTx::auto_commit(), "order", "order-7").unwrap()
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
            Some(SagaInstance::Running {
                state: "open".to_owned(),
                version,
                folded,
            })
        };
        let steps = [
            Step {
                event: "Stray",
                want: None,
                want_outbox: &[],
            },
            Step {
                event: "Created",
                want: running(1, "order-7-Created"),
                want_outbox: &[],
            },
            Step {
                event: "Noted",
                want: running(2, "order-7-Noted"),
                want_outbox: &[],
            },
            Step {
                event: "Paid",
                want: Some(SagaInstance::Completed { version: 3 }),
                want_outbox: &["5:order:order-7:3:0"],
            },
            // A redelivered `Paid` and a late `Cancelled` find a tombstone: no fold, no write, no command.
            Step {
                event: "Paid",
                want: Some(SagaInstance::Completed { version: 3 }),
                want_outbox: &["5:order:order-7:3:0"],
            },
            Step {
                event: "Cancelled",
                want: Some(SagaInstance::Completed { version: 3 }),
                want_outbox: &["5:order:order-7:3:0"],
            },
            // Not even a fresh `Created` resurrects it.
            Step {
                event: "Created",
                want: Some(SagaInstance::Completed { version: 3 }),
                want_outbox: &["5:order:order-7:3:0"],
            },
        ];
        let runner = runner(MemorySagaStore::<String>::new());
        for step in steps {
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("order-7", step.event))
                .expect(step.event);
            assert_eq!(instance(runner.store()), step.want, "{}", step.event);
            let outbox: Vec<String> = SagaStore::unpublished(runner.store(), "order", 10)
                .unwrap()
                .into_iter()
                .map(|c| c.id)
                .collect();
            assert_eq!(outbox, step.want_outbox, "{}: outbox", step.event);
        }
        let row = SagaStore::unpublished(runner.store(), "order", 1).unwrap().remove(0);
        // The row is the kernel's command message: keyed by the saga and instance, a child of the `Paid` event.
        assert_eq!(
            (
                row.idempotency_key.as_deref(),
                row.aggregate_id.as_deref(),
                row.name.as_str(),
                row.data.as_slice(),
                row.issued_at
            ),
            (
                Some("5:order:order-7:3:0"),
                Some("order-7"),
                "Fulfill",
                &br#"{"order_id":"order-7"}"#[..],
                1_714_564_800_000_000_000
            )
        );
        assert_eq!(
            (row.correlation_id.as_str(), row.causation_id.as_str()),
            ("corr-1", "order-7-Paid")
        );
        assert_eq!(
            drain_all(&runner),
            [OrderCommand::Fulfill(Fulfill {
                order_id: "order-7".into()
            })]
        );
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
            Self {
                inner,
                after_load: RefCell::new(None),
                fail_write: Cell::new(false),
                lose_reply: Cell::new(false),
            }
        }

        fn landed<T>(&self, wrote: Result<T, Error>) -> Result<T, Error> {
            match wrote {
                Ok(_) if self.lose_reply.replace(false) => Err(Error::store("reply lost after the write")),
                other => other,
            }
        }
    }

    impl SagaStore for RacingStore {
        type State = String;
        type Tx = MemoryTx;

        fn load(&self, tx: &mut MemoryTx, saga: &str, id: &str) -> Result<Option<SagaInstance<String>>, Error> {
            let loaded = self.inner.load(tx, saga, id);
            if let Some(hook) = self.after_load.borrow_mut().take() {
                hook();
            }
            loaded
        }
        fn save(
            &self,
            tx: &mut MemoryTx,
            saga: &str,
            id: &str,
            state: &String,
            version: u64,
            folded: &str,
            commands: &[CommandMessage],
        ) -> Result<u64, Error> {
            if self.fail_write.get() {
                return Err(Error::store("saga store unavailable"));
            }
            self.landed(self.inner.save(tx, saga, id, state, version, folded, commands))
        }
        fn complete(
            &self,
            tx: &mut MemoryTx,
            saga: &str,
            id: &str,
            version: u64,
            commands: &[CommandMessage],
        ) -> Result<u64, Error> {
            if self.fail_write.get() {
                return Err(Error::store("saga store unavailable"));
            }
            self.landed(self.inner.complete(tx, saga, id, version, commands))
        }
        fn unpublished(&self, saga: &str, limit: usize) -> Result<Vec<CommandMessage>, Error> {
            self.inner.unpublished(saga, limit)
        }
        fn mark_published(&self, saga: &str, ids: &[String]) -> Result<(), Error> {
            self.inner.mark_published(saga, ids)
        }
        fn unpublished_count(&self, saga: &str) -> Result<usize, Error> {
            self.inner.unpublished_count(saga)
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
        first
            .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
            .unwrap();

        let second = runner(inner.clone());
        *first.store().after_load.borrow_mut() = Some(Box::new(move || {
            second
                .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Cancelled"))
                .expect("the winner commits");
        }));
        let lost = first.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"));
        let conflict = lost.expect_err("the loser's write conflicts");
        assert_eq!(
            conflict.downcast_ref::<Error>().map(|e| e.code),
            Some(ErrorCode::Concurrency),
            "transient: redelivered"
        );

        first
            .run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"))
            .expect("the redelivery is ignored");
        let check = runner(inner);
        assert_eq!(instance(check.store()), Some(SagaInstance::Completed { version: 2 }));
        assert_eq!(
            drain_all(&check),
            [OrderCommand::Refund(Refund {
                order_id: "order-7".into()
            })],
            "no Fulfill escaped"
        );
    }

    #[test]
    fn a_write_that_fails_or_rolls_back_sends_nothing() {
        // Crash between the decision and the send, in the only order the runner allows: the commands
        // are part of the write, so a write that does not land leaves nothing to send, and the
        // redelivery decides again from the state that did land.
        for (name, fail_write) in [
            ("the store refuses the write", true),
            ("the unit of work rolls back", false),
        ] {
            let inner = MemorySagaStore::<String>::new();
            let store = RacingStore::new(inner.clone());
            let runner = runner(store);
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
                .unwrap();
            runner.store().fail_write.set(fail_write);
            let tr = inner.transactor();
            let tx = tr.begin().unwrap();
            let mut handle = tx;
            let out = runner.run(&mut handle, &order_event("payment-7", "Paid"));
            assert_eq!(out.is_err(), fail_write, "{name}");
            tr.rollback(handle);
            let folded = r#"{"order-7":{"sequence":1,"id":"order-7-Created"}}"#.to_owned();
            assert_eq!(
                instance(&inner),
                Some(SagaInstance::Running {
                    state: "open".to_owned(),
                    version: 1,
                    folded
                }),
                "{name}"
            );
            assert!(
                SagaStore::unpublished(&inner, "order", 10).unwrap().is_empty(),
                "{name}: nothing to send"
            );

            runner.store().fail_write.set(false);
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"))
                .unwrap();
            assert_eq!(
                drain_all(&self::runner(inner)),
                [OrderCommand::Fulfill(Fulfill {
                    order_id: "order-7".into()
                })],
                "{name}: redelivery"
            );
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
            (
                Some(state.unwrap_or(0) + 1),
                vec![OrderCommand::Fulfill(Fulfill {
                    order_id: event.id.clone(),
                })],
            )
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
        // The failure of one of the instance's own commands, as the kernel builds it: its cause is the command.
        let failed = |command_id: &str| EventMessage {
            name: SAGA_COMMAND_FAILED.to_owned(),
            causation_id: command_id.to_owned(),
            ..at("order-7", None, &format!("{command_id}:failed"))
        };
        let cases: [(&str, Vec<EventMessage>, &[&str]); 8] = [
            (
                "the last event again",
                vec![at("order-7", Some(1), "o-1"), at("order-7", Some(1), "o-1")],
                &["o-1"],
            ),
            (
                "a crash replays every event above the commit",
                vec![
                    at("order-7", Some(1), "o-1"),
                    at("order-7", Some(2), "o-2"),
                    at("order-7", Some(1), "o-1"),
                    at("order-7", Some(2), "o-2"),
                ],
                &["o-1", "o-2"],
            ),
            (
                "another aggregate's event between them",
                vec![
                    at("order-7", Some(1), "o-1"),
                    at("payment-7", Some(1), "p-1"),
                    at("order-7", Some(1), "o-1"),
                    at("payment-7", Some(1), "p-1"),
                ],
                &["o-1", "p-1"],
            ),
            (
                "an unsequenced event again",
                vec![at("order-7", None, "u-1"), at("order-7", None, "u-1")],
                &["u-1"],
            ),
            (
                // With no sequence there is no order to compare: only the last one is known.
                "an older unsequenced event is folded again",
                vec![
                    at("order-7", None, "u-1"),
                    at("order-7", None, "u-2"),
                    at("order-7", None, "u-1"),
                ],
                &["u-1", "u-2", "u-1"],
            ),
            (
                // A relay that lost its mark replays the first dead row after the second died: the
                // instance keeps every failure it folded, so neither compensates twice.
                "two failures, then the first replayed",
                vec![
                    failed("4:echo:order-7:1:0"),
                    failed("4:echo:order-7:1:1"),
                    failed("4:echo:order-7:1:0"),
                ],
                &["4:echo:order-7:1:0:failed", "4:echo:order-7:1:1:failed"],
            ),
            (
                "a crash replays one aggregate's sequenced and unsequenced events",
                vec![
                    at("order-7", Some(2), "o-2"),
                    at("order-7", None, "u-1"),
                    at("order-7", Some(2), "o-2"),
                    at("order-7", None, "u-1"),
                ],
                &["o-2", "u-1"],
            ),
            (
                // The limit of the sequence mark: once a later event folded, a dead letter re-driven
                // is below the mark, so the instance never folds it.
                "a re-driven dead letter below the mark is not folded",
                vec![
                    at("order-7", Some(3), "o-3"),
                    at("order-7", Some(5), "o-5"),
                    at("order-7", Some(4), "o-4"),
                ],
                &["o-3", "o-5"],
            ),
        ];
        for (name, deliveries, want) in cases {
            let runner = SagaRunner::new(Echo, MemorySagaStore::<u32>::new(), JsonRegistryCodec::new().unwrap());
            for env in &deliveries {
                runner.run(&mut MemoryTx::auto_commit(), env).expect(name);
            }
            let rows = SagaStore::unpublished(runner.store(), "echo", 10).unwrap();
            let sent: Vec<OrderCommand> = rows.iter().map(|row| runner.command(row).unwrap()).collect();
            let want_sent: Vec<OrderCommand> = want
                .iter()
                .map(|id| {
                    OrderCommand::Fulfill(Fulfill {
                        order_id: (*id).to_owned(),
                    })
                })
                .collect();
            assert_eq!(sent, want_sent, "{name}: commands");
            // The command id carries the version its write produced: one write per fold, none skipped.
            let ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
            let want_ids: Vec<String> = (1..=want.len())
                .map(|version| format!("4:echo:order-7:{version}:0"))
                .collect();
            assert_eq!(ids, want_ids, "{name}: one write per fold");
        }
    }

    #[test]
    fn a_lost_write_reply_converges_on_redelivery() {
        // The completion lands, then the runner sees a store error as if it had not. The redelivery
        // reads the tombstone, so it neither folds again nor commits a second command.
        let inner = MemorySagaStore::<String>::new();
        let runner = runner(RacingStore::new(inner.clone()));
        runner
            .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
            .unwrap();
        runner.store().lose_reply.set(true);
        let lost = runner.run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"));
        assert!(lost.is_err(), "the caller saw the lost reply");
        runner
            .run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"))
            .expect("the redelivery is ignored");
        assert_eq!(instance(&inner), Some(SagaInstance::Completed { version: 2 }));
        assert_eq!(
            drain_all(&self::runner(inner)),
            [OrderCommand::Fulfill(Fulfill {
                order_id: "order-7".into()
            })]
        );
    }

    #[test]
    fn a_command_committed_before_a_crash_is_sent_after_it_and_a_resend_is_deduplicated() {
        // Crash after the commit: the command waits in the outbox, so the next relay sends it. Crash
        // after the send but before `mark_published`: the relay sends it again (at-least-once), and
        // the consumer's guard keyed by the row's idempotency key runs the handler once.
        let store = MemorySagaStore::<String>::new();
        let runner = runner(store.clone());
        runner
            .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
            .unwrap();
        runner
            .run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"))
            .unwrap();

        /// The outbox, whose `mark_published` fails while `crash` is set: the process died after the send.
        struct CrashingMark(SagaOutbox<MemorySagaStore<String>>, Cell<bool>);
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
        let relay =
            Relay::<_, CommandMessage>::new(CrashingMark(runner.outbox(), Cell::new(true))).pacing(Pacing::instant());
        let ledger: Arc<dyn crate::IdempotencyStore<(), Tx = MemoryTx> + Send + Sync> =
            Arc::new(crate::MemoryIdempotencyStore::<()>::new());
        let guard = idempotent(ledger, "fulfillment", edax_core::Tenancy::Silo);
        let runs: Arc<Mutex<Vec<Fulfill>>> = Arc::default();
        let effect = Arc::clone(&runs);
        let handler: Handler<Fulfill> = Box::new(move |cmd: &Fulfill| {
            effect.lock().expect("no panics").push(cmd.clone());
            Ok(())
        });
        let mut bus = CommandBus::new();
        bus.register_with_codec(handler, JsonCodec::<Fulfill>::new(Fulfill::TYPE), vec![guard])
            .unwrap();
        let publish = |row: &CommandMessage| send(&bus, row);

        let crashed = relay.drain(10, publish);
        assert!(crashed.is_err(), "the mark did not land");
        relay.store().1.set(false);
        assert_eq!(relay.drain(10, publish).unwrap(), 1, "re-sent after the crash");
        assert_eq!(relay.drain(10, publish).unwrap(), 0, "and then marked");
        assert_eq!(
            *runs.lock().unwrap(),
            [Fulfill {
                order_id: "order-7".into()
            }],
            "the handler ran once"
        );
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
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
                .unwrap();
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"))
                .unwrap();
            let handler: Handler<Fulfill> = Box::new(move |_: &Fulfill| match fails {
                Fails::Never => Ok(()),
                Fails::Codec => Err(Error::codec("unknown field `sku`").into()),
                Fails::Rejected => Err(Terminal::new("order already shipped").into()),
                Fails::Transient => Err("warehouse API timed out".into()),
            });
            let mut bus = CommandBus::new();
            bus.register_with_codec(handler, JsonCodec::<Fulfill>::new(Fulfill::TYPE), vec![])
                .unwrap();
            let dead: Arc<Mutex<Vec<String>>> = Arc::default();
            let sink = Arc::clone(&dead);
            let relay = Relay::<_, CommandMessage>::new(runner.outbox())
                .pacing(Pacing::instant())
                .on_dead(move |dead: DeadLetter<&CommandMessage, &HandlerError>| {
                    sink.lock().expect("no panics").push(dead.message.id.clone());
                    Ok(())
                });
            let out = relay.drain(10, |row| send(&bus, row));
            assert_eq!(out.is_err(), want_left == 1, "{name}: drain result");
            assert_eq!(dead.lock().unwrap().len(), want_dead, "{name}: dead-lettered");
            assert_eq!(
                SagaStore::unpublished(&store, "order", 10).unwrap().len(),
                want_left,
                "{name}: left"
            );
        }
    }

    #[test]
    fn a_dead_command_folds_back_into_its_instance_as_a_failure() {
        // `Fulfill` dies, so it saves no event: without the sink the instance would wait forever. The
        // sink folds the failure into the instance, which compensates with a `Refund`. The payload
        // carries the failure's taxonomy code apart from its message, whatever the error's kind.
        #[derive(Clone, Copy, Debug)]
        enum Fails {
            Rejected,
            Kernel,
            Unclassified,
        }
        let error = |fails: Fails| -> HandlerError {
            match fails {
                Fails::Rejected => Terminal::new("out of stock").into(),
                Fails::Kernel => Error::store("warehouse API timed out").into(),
                Fails::Unclassified => std::io::Error::other("disk full").into(),
            }
        };
        let failed = |code: Option<&str>, error: &str| SagaCommandFailed {
            command_id: "5:order:order-7:2:0".into(),
            command: "Fulfill".into(),
            code: code.map(ToOwned::to_owned),
            error: error.into(),
        };
        let cases = [
            (
                "a terminal rejection",
                Fails::Rejected,
                failed(Some("rejected"), "out of stock"),
            ),
            (
                "a kernel error, without its code in the message",
                Fails::Kernel,
                failed(Some("store"), "warehouse API timed out"),
            ),
            (
                "an error no code classifies",
                Fails::Unclassified,
                failed(None, "disk full"),
            ),
        ];
        for (name, fails, want) in cases {
            let store = MemorySagaStore::<String>::new();
            let saga = OrderSaga::default();
            let failures = Arc::clone(&saga.failures);
            let runner = Arc::new(
                SagaRunner::new(saga, store.clone(), JsonRegistryCodec::new().unwrap())
                    .with_clock(|| 1_714_564_800_000_000_000),
            );
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
                .unwrap();
            runner
                .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Shipped"))
                .unwrap();
            let refunded: Arc<Mutex<Vec<Refund>>> = Arc::default();
            let effect = Arc::clone(&refunded);
            let refund: Handler<Refund> = Box::new(move |cmd: &Refund| {
                effect.lock().expect("no panics").push(cmd.clone());
                Ok(())
            });
            let fulfill: Handler<Fulfill> = Box::new(move |_: &Fulfill| Err(error(fails)));
            let mut bus = CommandBus::new();
            bus.register_with_codec(fulfill, JsonCodec::<Fulfill>::new(Fulfill::TYPE), vec![])
                .unwrap();
            bus.register_with_codec(refund, JsonCodec::<Refund>::new(Refund::TYPE), vec![])
                .unwrap();
            let dead: Arc<Mutex<Vec<CommandMessage>>> = Arc::default();
            let (sink, kept) = (Arc::clone(&runner), Arc::clone(&dead));
            // One attempt: a transient failure dies at once too, as it would at the last of several.
            let relay = Relay::<_, CommandMessage>::new(runner.outbox())
                .pacing(Pacing::instant())
                .max_attempts(1)
                .on_dead(move |dead: DeadLetter<&CommandMessage, &HandlerError>| {
                    kept.lock().expect("no panics").push(dead.message.clone());
                    sink.command_failed(&mut MemoryTx::auto_commit(), dead)
                });
            let publish = |row: &CommandMessage| send(&bus, row);

            assert_eq!(
                relay.drain(10, publish).unwrap(),
                1,
                "{name}: the dead Fulfill is settled"
            );
            assert_eq!(*failures.lock().unwrap(), [want], "{name}: the folded payload");
            let rows = SagaStore::unpublished(&store, "order", 10).unwrap();
            let compensation: Vec<(&str, &str)> = rows
                .iter()
                .map(|row| (row.id.as_str(), row.causation_id.as_str()))
                .collect();
            assert_eq!(
                compensation,
                [("5:order:order-7:3:0", "5:order:order-7:2:0:failed")],
                "{name}: the Refund is the failure's child"
            );
            assert_eq!(instance(&store), Some(SagaInstance::Completed { version: 3 }), "{name}");
            assert_eq!(relay.drain(10, publish).unwrap(), 1, "{name}: the Refund is sent");
            assert_eq!(
                *refunded.lock().unwrap(),
                [Refund {
                    order_id: "order-7".into()
                }],
                "{name}"
            );

            // The sink runs again for the same row (its mark was lost): the completed instance folds nothing.
            let row = dead.lock().unwrap().remove(0);
            let failure = error(fails);
            runner
                .command_failed(
                    &mut MemoryTx::auto_commit(),
                    DeadLetter {
                        message: &row,
                        attempt: 1,
                        failure: &failure,
                    },
                )
                .unwrap();
            assert_eq!(
                instance(&store),
                Some(SagaInstance::Completed { version: 3 }),
                "{name}: no second fold"
            );
            assert!(
                SagaStore::unpublished(&store, "order", 10).unwrap().is_empty(),
                "{name}: no second Refund"
            );
            assert_eq!(failures.lock().unwrap().len(), 1, "{name}: folded once");
        }
    }

    #[test]
    fn a_saga_command_hashes_as_the_same_command_dispatched_alone() {
        // The row's payload is the command's own value, and the bus dispatches the command as its own
        // type, so the guard binds the hash a plain dispatch of the struct binds, and the one a Python bus
        // computes from the row's name and bytes. An enum dispatched whole would hash `{"Fulfill":{..}}`.
        let store = MemorySagaStore::<String>::new();
        let runner = runner(store.clone());
        runner
            .run(&mut MemoryTx::auto_commit(), &order_event("order-7", "Created"))
            .unwrap();
        runner
            .run(&mut MemoryTx::auto_commit(), &order_event("payment-7", "Paid"))
            .unwrap();
        let row = SagaStore::unpublished(&store, "order", 1).unwrap().remove(0);

        let ledger = crate::MemoryIdempotencyStore::<()>::new();
        let guard = idempotent(ledger.clone(), "fulfillment", edax_core::Tenancy::Silo);
        let mut bus = CommandBus::new();
        bus.register_with_codec(
            Box::new(|_: &Fulfill| Ok(())),
            JsonCodec::<Fulfill>::new(Fulfill::TYPE),
            vec![guard],
        )
        .unwrap();
        send(&bus, &row).unwrap();

        let claim = edax_cqrs::claim_key(
            edax_core::Tenancy::Silo,
            None,
            "fulfillment",
            row.idempotency_key.as_deref().unwrap(),
        )
        .unwrap();
        let crate::Claim::Prior(record) =
            crate::IdempotencyStore::claim(&ledger, &mut MemoryTx::auto_commit(), &claim, 0).unwrap()
        else {
            panic!("the dispatch left a record")
        };
        let alone = edax_cqrs::command_hash(
            Fulfill::TYPE,
            &Fulfill {
                order_id: "order-7".into(),
            },
        )
        .unwrap();
        let from_row = edax_cqrs::command_hash_json(&row.name, &row.data).unwrap();
        assert_eq!(
            (record.payload_hash, from_row),
            (alone, alone),
            "one hash for the saga's row, the struct and the bytes"
        );
    }
}
