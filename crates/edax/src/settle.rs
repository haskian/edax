//! What a [`Relay`](crate::Relay) and a [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher) share when a
//! message dies: the [`DeadLetter`] their sinks take, and the sink step [`run_sink`].

use std::panic::{catch_unwind, AssertUnwindSafe};

use edax_event::{EventMessage, Failure};

use crate::dispatcher::HandlerFailure;
use crate::failure::{failure_of, HandlerError};

/// What a dead-letter sink takes, from a [`KeyOrderedDispatcher`](crate::KeyOrderedDispatcher) and a
/// [`Relay`](crate::Relay) alike: Python's `DeadLetter`. A dispatcher's owns the message its worker held,
/// so nothing is copied on the success path and a sink can publish it without a clone, and its `failure`
/// is the [`HandlerFailure`]. A relay's lends the outbox row and the publish error,
/// `DeadLetter<&R, &HandlerError>`, since the relay still marks the row and may raise the error after.
#[derive(Debug)]
pub struct DeadLetter<M = EventMessage, F = HandlerFailure> {
    pub message: M,
    /// The failed attempt that killed it, as the kernel counts it: `max(broker count, in-process count) +
    /// 1`. `1` for a terminal error on first delivery. At least `max_attempts` for an exhausted transient
    /// one, and more when the broker's own count is higher or a sink failed on an earlier try.
    pub attempt: u32,
    pub failure: F,
}

/// What a dead-letter sink answered, in the two shapes its caller needs.
pub(crate) struct SinkAnswer {
    /// What the retry policy's `sunk` takes: `None` once the sink took the message, else the kernel's kind
    /// of its refusal. A missing sink and a panic carry no code, so they are transient.
    pub(crate) failure: Option<Failure>,
    /// The error the sink refused with, `None` when it took the message, was missing or panicked.
    pub(crate) refusal: Option<HandlerError>,
}

/// Run the dead-letter `sink` on a message the retry policy declared dead. The one sink step behind the
/// dispatcher and the [`Relay`](crate::Relay): edax runs the sink inside a protocol whose bookkeeping it
/// owns, so a panic must not skip that bookkeeping.
pub(crate) fn run_sink(sink: Option<impl FnOnce() -> Result<(), HandlerError>>) -> SinkAnswer {
    let refused = match sink.map(|sink| catch_unwind(AssertUnwindSafe(sink))) {
        Some(Ok(Ok(()))) => {
            return SinkAnswer {
                failure: None,
                refusal: None,
            }
        }
        Some(Ok(Err(error))) => Some(error),
        None | Some(Err(_)) => None,
    };
    let failure = refused.as_ref().map_or(Failure::of(None), |error| failure_of(&**error));
    SinkAnswer {
        failure: Some(failure),
        refusal: refused,
    }
}
