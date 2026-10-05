//! The unit of work: a [`Transactor`] owns the transaction boundary, and [`transaction`] runs a body in
//! one. The stores that take a `&mut Tx` are participants, so any number of them commit together.

use std::cell::RefCell;

use std::num::NonZeroU32;

use crate::failure::{failure_of, HandlerError};
use crate::pacing::Pacing;
use crate::Error;

/// Owns the transaction boundary: begin a unit of work, commit it, or roll it back. The stores that
/// take a `&mut Tx` ([`EventStore`](crate::EventStore), [`ProjectionStore`](crate::ProjectionStore),
/// [`IdempotencyStore`](crate::IdempotencyStore)) are pure participants, so any number of them commit
/// together. An [`EventStore`](crate::EventStore) *is* a [`Transactor`]; a backend can also implement this on its own
/// and hand the same `Tx` to every store. Prefer [`Transactor::transaction`] over calling
/// begin/commit/rollback by hand.
pub trait Transactor {
    /// Backend transaction handle (a connection / transaction). Use `()` for a store that needs none.
    type Tx;

    /// The database this transactor opens units of work on. Two transactors over one database must
    /// return the same value, so [`transaction`] can refuse a second unit of work on it while one is
    /// open on this thread. There is no default: a store that shares a pool with others, and answered
    /// with its own address, would escape that refusal silently. The memory backend returns its
    /// database's.
    fn identity(&self) -> usize;

    fn begin(&self) -> Result<Self::Tx, Error>;
    fn commit(&self, tx: Self::Tx) -> Result<(), Error>;
    fn rollback(&self, tx: Self::Tx);

    /// Scoped unit of work: run `body`, commit on `Ok`, roll back on `Err`. Thread the `&mut Tx` to
    /// every store that should commit together (events, projection, idempotency record). `body` fails
    /// with any error an [`Error`] converts into — a [`HandlerError`] included, so
    /// a command dispatched inside keeps its verdict. A second unit of work on the same database while
    /// this one is open on the thread is refused; see [`transaction`].
    fn transaction<R, E: From<Error>>(&self, body: impl FnOnce(&mut Self::Tx) -> Result<R, E>) -> Result<R, E>
    where
        Self: Sized,
    {
        transaction(self, body)
    }
}

/// Holds the open handle until the unit of work settles it. Dropping it unsettled — the body
/// returned `Err`, or it unwound — rolls back. Without this, a panic would leak the handle: a
/// backend that holds the transaction's keys until it ends would hold them forever, and every
/// later write to those keys would fail with [`ErrorCode::Concurrency`](crate::ErrorCode::Concurrency).
struct Rollback<'a, T: Transactor> {
    transactor: &'a T,
    tx: Option<T::Tx>,
}

impl<T: Transactor> Rollback<'_, T> {
    /// Settle by committing. Consumes the guard, so its `Drop` finds no handle to roll back.
    fn commit(mut self) -> Result<(), Error> {
        match self.tx.take() {
            Some(tx) => self.transactor.commit(tx),
            None => Ok(()),
        }
    }
}

impl<T: Transactor> Drop for Rollback<'_, T> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            self.transactor.rollback(tx);
        }
    }
}

thread_local! {
    /// The databases a [`transaction`] body is running on, on this thread, innermost last. A handler is
    /// a sync closure that runs on its caller's thread, so a dispatch made inside a body runs inside that
    /// unit of work: [`retry`](crate::retry) reads this to refuse it, and [`transaction`] to refuse a
    /// second unit of work on a database that already has one open.
    static OPEN_UNITS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Marks a [`transaction`] body as running on this thread until dropped, panics included.
struct OpenUnit;

impl OpenUnit {
    /// Mark `database` open, or refuse it when a body on this thread already holds it.
    fn enter(database: usize) -> Result<Self, Error> {
        OPEN_UNITS.with_borrow_mut(|open| {
            if open.contains(&database) {
                // The inner unit of work would commit on its own, and the outer rollback could not take
                // it back. Terminal: the same wiring fails the same way on every retry.
                return Err(Error::bad_handle(
                    "a unit of work was opened on a database that already has one open on this thread: the inner \
                     one would commit independently of the outer one. Pass the open `&mut Tx` down (`save_in`, \
                     `load_in`, `dispatch` inside the body) instead of opening another",
                ));
            }
            open.push(database);
            Ok(OpenUnit)
        })
    }
}

impl Drop for OpenUnit {
    fn drop(&mut self) {
        OPEN_UNITS.with_borrow_mut(|open| {
            open.pop();
        });
    }
}

/// Whether a [`transaction`] body is running on this thread.
pub(crate) fn in_unit_of_work() -> bool {
    OPEN_UNITS.with_borrow(|open| !open.is_empty())
}

/// The free-function form of [`Transactor::transaction`]. A panic in `body` rolls back and keeps
/// unwinding; the caller sees the panic, the backend sees a settled transaction.
///
/// Opening it on a database that already has a unit of work open on this thread is refused with the
/// terminal [`ErrorCode::BadHandle`](crate::ErrorCode::BadHandle), before anything begins: the inner
/// unit of work would commit on its own, and the outer rollback could not take it back. So
/// [`Repository::save`](crate::Repository::save) (which opens its own) inside a host's body is refused rather than committing
/// apart from it; [`Repository::save_in`](crate::Repository::save_in) joins the open one. The check keys on
/// [`Transactor::identity`], so it catches every transactor over one database only when the backend
/// reports one identity for them. Two databases nest.
pub fn transaction<T, R, E: From<Error>>(tr: &T, body: impl FnOnce(&mut T::Tx) -> Result<R, E>) -> Result<R, E>
where
    T: Transactor,
{
    let open = OpenUnit::enter(tr.identity())?;
    let mut guard = Rollback {
        transactor: tr,
        tx: Some(tr.begin()?),
    };
    // `None` is unreachable: only `commit` takes the handle, and it consumes the guard.
    let outcome = guard.tx.as_mut().map(body);
    drop(open);
    match outcome {
        Some(Ok(r)) => {
            guard.commit()?;
            Ok(r)
        }
        Some(Err(e)) => Err(e),
        None => Err(Error::bad_handle("unit of work has no open transaction").into()),
    }
}

/// Run `body` in a unit of work, and on a transient failure run it again in a fresh one: up to
/// `attempts` runs, waiting each step of `pacing`. A terminal error returns at once.
///
/// This is the safe retry over a transaction. Each attempt commits or rolls back whole, so a second
/// attempt never re-applies the first one's staged writes. Use it to retry a dispatch through
/// [`idempotent_in`](crate::idempotent_in): [`retry`](crate::retry) refuses to run inside a
/// [`transaction`] body, for exactly that reason. The sleep blocks the calling thread.
pub fn retry_transaction<T, R>(
    tr: &T,
    attempts: NonZeroU32,
    pacing: &Pacing,
    mut body: impl FnMut(&mut T::Tx) -> Result<R, HandlerError>,
) -> Result<R, HandlerError>
where
    T: Transactor,
{
    retry_with(attempts, pacing, || transaction(tr, &mut body))
}

/// Run `attempt` up to `attempts` times, waiting each step of `pacing` after a
/// transient failure, and return the last error; a terminal one returns at once. The one retry loop
/// behind [`retry`](crate::retry) and [`retry_transaction`]; when to stop and how long to
/// wait is the kernel's [`edax_event::retry_step`], so the two SDKs retry alike.
pub(crate) fn retry_with<R>(
    attempts: NonZeroU32,
    pacing: &Pacing,
    mut attempt: impl FnMut() -> Result<R, HandlerError>,
) -> Result<R, HandlerError> {
    let mut failed: u32 = 0;
    loop {
        let e = match attempt() {
            Ok(out) => return Ok(out),
            Err(e) => e,
        };
        let Some(step) = edax_event::retry_step(attempts, failed, failure_of(&*e)) else {
            return Err(e);
        };
        pacing.wait(step);
        failed = failed.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::account::deposited;
    use crate::{EventDraft, EventStore, MemoryEventStore, MemoryTx};

    #[test]
    fn transaction_branches() {
        // The boundary's whole contract: commit on a clean exit, roll back on a raise, exactly one of
        // the two and only after `begin` succeeded. A spy pins *which* one ran and in what order;
        // what a commit or rollback does to the data is the memory backend's own test.
        /// `begin` / `commit` fail with the message given, if any; every call is logged in order.
        struct Spy {
            begin: Option<&'static str>,
            commit: Option<&'static str>,
            log: RefCell<Vec<&'static str>>,
        }
        impl Spy {
            fn record(&self, step: &'static str, failure: Option<&'static str>) -> Result<(), Error> {
                self.log.borrow_mut().push(step);
                failure.map_or(Ok(()), |m| Err(Error::store(m)))
            }
        }
        impl Transactor for Spy {
            type Tx = ();
            fn identity(&self) -> usize {
                std::ptr::from_ref(self).addr()
            }
            fn begin(&self) -> Result<(), Error> {
                self.record("begin", self.begin)
            }
            fn commit(&self, _tx: ()) -> Result<(), Error> {
                self.record("commit", self.commit)
            }
            fn rollback(&self, _tx: ()) {
                let _ = self.record("rollback", None);
            }
        }

        struct Case {
            name: &'static str,
            begin: Option<&'static str>,
            commit: Option<&'static str>,
            body: fn() -> Result<u8, Error>,
            want_log: &'static [&'static str],
            /// The value a clean unit of work yields, or the message the error must carry.
            want: Result<u8, &'static str>,
        }
        let cases = [
            Case {
                name: "clean body commits",
                begin: None,
                commit: None,
                body: || Ok(7),
                want_log: &["begin", "commit"],
                want: Ok(7),
            },
            Case {
                name: "failing body rolls back and never commits",
                begin: None,
                commit: None,
                body: || Err(Error::store("body gave up")),
                want_log: &["begin", "rollback"],
                want: Err("store: body gave up"),
            },
            Case {
                name: "a refused commit surfaces and does not also roll back",
                begin: None,
                commit: Some("commit refused"),
                body: || Ok(7),
                want_log: &["begin", "commit"],
                want: Err("store: commit refused"),
            },
            Case {
                name: "a refused begin runs neither the body nor a rollback",
                begin: Some("begin refused"),
                commit: None,
                body: || panic!("the body must not run without a transaction"),
                want_log: &["begin"],
                want: Err("store: begin refused"),
            },
        ];
        for c in cases {
            let spy = Spy {
                begin: c.begin,
                commit: c.commit,
                log: RefCell::new(Vec::new()),
            };
            let got = transaction(&spy, |_tx| (c.body)());
            assert_eq!(spy.log.borrow().as_slice(), c.want_log, "{}", c.name);
            match c.want {
                Ok(value) => assert_eq!(got.ok(), Some(value), "{}", c.name),
                Err(message) => assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name),
            }
        }
    }

    #[test]
    fn a_panic_inside_transaction_rolls_back_and_frees_the_stream() {
        // Without the drop guard the token stays in the adapter's open map, which holds the
        // stream's keys forever: every later append on it would fail with `Concurrency`.
        let store = MemoryEventStore::new();
        let deposit = |seq: u64| EventDraft {
            id: format!("panic-{seq}"),
            ..deposited("acct-panic", seq, b"1")
        };

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<(), Error> = store.transaction(|tx| {
                store.append(tx, "acct-panic", 0, vec![deposit(1)])?;
                panic!("the handler blew up mid-transaction");
            });
        }));
        assert!(unwound.is_err(), "the panic keeps unwinding past transaction");

        // Rolled back, so the stream is still at version 0 and the same append lands.
        let second = store.append(&mut MemoryTx::auto_commit(), "acct-panic", 0, vec![deposit(1)]);
        assert!(second.is_ok(), "append after the panic: {:?}", second.err());
        let stored = store.load(&mut MemoryTx::auto_commit(), "acct-panic", 0).unwrap();
        assert_eq!(stored.len(), 1, "only the second append is stored");
    }
}
