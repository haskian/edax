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

/// Why a host's rejection refused (`not_found`): a name the host branches on, kept beside the `rejected`
/// code in the cached record so a replay tells two refusals apart. Checked when it is built
/// ([`edax_cqrs::check_reason`]), so a bad one fails there, never when the failure settles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reason(String);

impl Reason {
    /// `name` as a reason, or the kernel's `codec` error for one that is not 1 to 64 bytes of
    /// `[a-z0-9._-]` starting with `[a-z0-9]`.
    pub fn new(name: impl Into<String>) -> Result<Reason, Error> {
        let name = name.into();
        edax_cqrs::check_reason(&name)?;
        Ok(Reason(name))
    }

    /// The name, as the record stores it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A host's rejection that names why it refused: terminal like [`Terminal`], cached under `rejected`
/// with its [`Reason`] beside the code.
#[derive(Debug)]
pub struct Rejected {
    reason: Reason,
    cause: HandlerError,
}

impl Rejected {
    /// Reject with `cause`, for `reason`.
    pub fn new(reason: Reason, cause: impl Into<HandlerError>) -> Self {
        Rejected {
            reason,
            cause: cause.into(),
        }
    }

    /// Why it refused.
    pub fn reason(&self) -> &Reason {
        &self.reason
    }
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}

impl std::error::Error for Rejected {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.cause)
    }
}

/// The taxonomy code a handler failure carries: an [`Error`]'s own, `rejected` for a [`Terminal`] or a
/// [`Rejected`], and none for anything else.
pub(crate) fn code_of(e: &(dyn std::error::Error + 'static)) -> Option<ErrorCode> {
    if let Some(err) = e.downcast_ref::<Error>() {
        return Some(err.code);
    }
    (e.is::<Terminal>() || e.is::<Rejected>()).then_some(ErrorCode::Rejected)
}

/// Why a handler failure refused: a [`Rejected`]'s reason, none for anything else.
pub(crate) fn reason_of<'a>(e: &'a (dyn std::error::Error + 'static)) -> Option<&'a str> {
    e.downcast_ref::<Rejected>().map(|rejected| rejected.reason.as_str())
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
/// beside it), a [`Rejected`]'s cause's (its reason is cached beside it too), anything else as it
/// displays.
pub(crate) fn failure_message(e: &(dyn std::error::Error + 'static)) -> String {
    if let Some(rejected) = e.downcast_ref::<Rejected>() {
        return failure_message(&*rejected.cause);
    }
    e.downcast_ref::<Error>()
        .map_or_else(|| e.to_string(), |err| err.message.clone())
}
