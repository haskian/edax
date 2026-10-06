//! What a handler failure is, and the kernel's verdict on it: every consumer of a failure (a retry, a
//! guard, a relay, a dispatcher, a saga) reads it through these functions, so none of them re-derives it.

use edax_core::ErrorCode;
use edax_event::Failure;

use crate::Error;

/// A handler's failure: any error, boxed. One contract for command and event handlers alike, so a
/// failure keeps its verdict whichever path it crosses — a saga that dispatches a command returns the
/// command's error with `?`. [`retry`](crate::retry), both idempotency guards and the dispatcher downcast it to
/// classify terminal (deterministic, so a redelivery cannot change it) against transient: an
/// [`Error`] by its code, a [`Terminal`] as terminal, anything else by the kernel's default for an
/// unclassified failure (transient). `Send + Sync` so it can cross a thread.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// A host's own business rejection, marked terminal: "insufficient funds" fails the same way on every
/// retry, so the guard caches it (code `rejected`) and a dispatcher dead-letters it instead of
/// redelivering it forever. Wrap the host's error in it; without it a host error is unclassified and
/// therefore transient.
#[derive(Debug)]
pub struct Terminal(pub HandlerError);

impl Terminal {
    /// Mark `cause` terminal.
    pub fn new(cause: impl Into<HandlerError>) -> Self {
        Terminal(cause.into())
    }
}

impl std::fmt::Display for Terminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Terminal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// The taxonomy code a handler failure carries: an [`Error`]'s own, `rejected` for a [`Terminal`], and
/// none for anything else.
pub(crate) fn code_of(e: &(dyn std::error::Error + 'static)) -> Option<ErrorCode> {
    if let Some(err) = e.downcast_ref::<Error>() {
        return Some(err.code);
    }
    e.is::<Terminal>().then_some(ErrorCode::Rejected)
}

/// The kernel's kind of a handler failure ([`edax_event::Failure::of`] on [`code_of`]): what a retry, a
/// relay and a dispatcher hand their kernel decision.
pub(crate) fn failure_of(e: &(dyn std::error::Error + 'static)) -> Failure {
    Failure::of(code_of(e))
}

/// Whether an error is *fatal*: a wiring fault (a refused credential, topic or setting, the
/// `bad_handle` code) that fails every message alike. A [`Relay`](crate::Relay) drain or a
/// [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher) hands the message back uncounted and reports
/// it; the host's loop stops on it rather than dead-letter every message after it. The kind is the
/// kernel's ([`edax_event::Failure::of`]).
pub fn is_fatal(e: &(dyn std::error::Error + 'static)) -> bool {
    failure_of(e) == Failure::Fatal
}

/// What a failure caches as its message: an [`Error`]'s message without its code (the code is cached
/// beside it), anything else as it displays.
pub(crate) fn failure_message(e: &(dyn std::error::Error + 'static)) -> String {
    e.downcast_ref::<Error>()
        .map_or_else(|| e.to_string(), |err| err.message.clone())
}
