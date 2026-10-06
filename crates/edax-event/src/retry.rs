//! The consumer's retry and dead-letter policy: when a failed message is handed back, after how
//! long, and when it dies.
//!
//! Two SDKs consuming one topic must agree when a message dies, or the same poison message is
//! dead-lettered by one service and retried forever by the other. So the rule is here, sans-IO, in
//! the [`OffsetTracker`](https://docs.rs/edax-kafka) style: the host runs the handler, the sink, the
//! wait and the ack or nack; [`Retries`] only says which, and keeps the per-message counts the rule
//! reads. The same policy drains an outbox: a relay keys it by row id instead of message id.
//!
//! The rule, pinned by `testdata/golden/retries.json`:
//!
//! - the attempt a run is: `max(broker_attempt, recorded) + 1`, so a broker that counts its own
//!   redeliveries raises it, and a restart that forgot the local count does not lower it;
//! - a failure's kind is its error code's ([`Failure::of`]): `in_progress` is contention, `bad_handle`
//!   is fatal, another terminal code is terminal, and any other code, or none, is transient;
//! - a fatal failure stops the consumer: the message is handed back uncounted and never dead-lettered,
//!   since every message after it fails the same way and would follow it into the sink;
//! - a message dies when its failure is terminal, or when `0 < max_attempts <= attempt`; a dead
//!   message goes to the sink if the policy was built with one, and is nacked like a transient
//!   failure if it was not;
//! - a nack waits backoff step `attempt - 1`, so the wait grows across redeliveries;
//! - contention (another consumer holds the message's idempotency claim) is not a failed attempt: it
//!   waits its own growing step and never counts toward `max_attempts`;
//! - a sink that fails nacks the message after step `attempt - 1` and keeps the attempt, so the
//!   redelivery dies again at the next attempt and the sink gets another try; a sink that fails
//!   fatally stops the consumer like any fatal failure.
//!
//! An in-process retry, a caller waiting on a command, reads its own rule, [`retry_step`], pinned by
//! `testdata/golden/retry_step.json`. It keeps no state, and counts contention as an attempt.

use std::collections::HashMap;
use std::num::NonZeroU32;

use edax_core::{Error, ErrorCode};

edax_core::closed_names! {
    /// How a run failed, as the policy reads it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Failure {
        /// The wiring is wrong, not the message: a refused credential, topic or setting. Every message
        /// fails the same way, so the consumer stops rather than dead-letter them one by one.
        Fatal = "fatal",
        /// Deterministic: a retry fails the same way, so the message dies at once.
        Terminal = "terminal",
        /// May succeed on a later attempt; counts toward `max_attempts`.
        Transient = "transient",
        /// Another consumer holds the message's idempotency claim. Not a failed attempt.
        Contended = "contended",
    }
}

impl Failure {
    /// The kind of a failure that carries `code`, `None` for one no code classifies: contention for
    /// [`ErrorCode::InProgress`], fatal for a fatal code ([`ErrorCode::is_fatal`]: a wiring mistake,
    /// which fails every message alike), terminal for another terminal code, and transient otherwise (the
    /// kernel's default for an unclassified failure, [`edax_core::is_terminal`]). One rule, so two SDKs
    /// consuming one topic count, kill and stop on the same failures.
    pub fn of(code: Option<ErrorCode>) -> Failure {
        match code {
            Some(ErrorCode::InProgress) => Failure::Contended,
            Some(code) if code.is_fatal() => Failure::Fatal,
            code if edax_core::is_terminal(code) => Failure::Terminal,
            _ => Failure::Transient,
        }
    }
}

/// What the host does with a failed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Verdict {
    /// Wait `step` of the backoff schedule, then hand the message back for redelivery.
    Nack {
        /// The backoff step to wait out first.
        step: u64,
    },
    /// Hand the message to the dead-letter sink, then ack it. Report the sink's answer with
    /// [`Retries::sunk`].
    Dead {
        /// The failed attempt that killed it: `1` for a terminal failure on first delivery.
        attempt: u32,
    },
    /// Hand the message back uncounted and stop consuming: the failure is fatal, so every later
    /// message would fail alike. The host raises the failure out of its consumer or relay loop.
    Stop,
}

impl Verdict {
    /// The verdict's name, what a binding branches on.
    pub fn kind(&self) -> VerdictKind {
        match self {
            Verdict::Nack { .. } => VerdictKind::Nack,
            Verdict::Dead { .. } => VerdictKind::Dead,
            Verdict::Stop => VerdictKind::Stop,
        }
    }
}

edax_core::closed_names! {
    /// The name of a [`Verdict`]. Exhaustive on purpose: a host must decide about every verdict, so a new
    /// one is a compile error in every SDK rather than a wildcard's default.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum VerdictKind {
        /// [`Verdict::Nack`].
        Nack = "nack",
        /// [`Verdict::Dead`].
        Dead = "dead",
        /// [`Verdict::Stop`].
        Stop = "stop",
    }
}

/// How many messages [`Retries`] is tracking: a gauge for entries that never come back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Tracked {
    /// Messages with a recorded failed attempt.
    pub attempts: usize,
    /// Messages currently losing a claim race.
    pub contended: usize,
}

/// The retry and dead-letter policy over the messages one consumer has seen fail. Keyed by message
/// id (a message id, an outbox row id). An entry stays until the id succeeds or dies; one whose
/// message never comes back (a broker-side dead-letter took it) stays for the life of the value.
#[derive(Debug, Clone, Default)]
pub struct Retries {
    max_attempts: u32,
    sink: bool,
    attempts: HashMap<String, u32>,
    contended: HashMap<String, u32>,
}

impl Retries {
    /// A policy that dead-letters a transient failure at its `max_attempts`-th attempt; `0` disables
    /// that ceiling, so only a terminal failure dies. `sink` is whether the host has a dead-letter sink:
    /// without one nothing dies, and every failure is handed back with a growing step.
    pub fn new(max_attempts: u32, sink: bool) -> Self {
        Self {
            max_attempts,
            sink,
            ..Self::default()
        }
    }

    /// The attempt a run of `id` is: `max(broker_attempt, recorded) + 1`, saturating.
    pub fn attempt(&self, id: &str, broker_attempt: u32) -> u32 {
        broker_attempt
            .max(self.attempts.get(id).copied().unwrap_or(0))
            .saturating_add(1)
    }

    /// `id` succeeded: forget its counts.
    pub fn succeeded(&mut self, id: &str) {
        self.attempts.remove(id);
        self.contended.remove(id);
    }

    /// `id` failed with `failure`, at the broker's own redelivery count `broker_attempt`.
    pub fn failed(&mut self, id: &str, broker_attempt: u32, failure: Failure) -> Verdict {
        if failure == Failure::Fatal {
            // Not an attempt of this message: the wiring failed it, and fixing the wiring must not
            // find it one step nearer its ceiling.
            return Verdict::Stop;
        }
        if failure == Failure::Contended {
            let entry = self.contended.entry(id.to_owned()).or_insert(0);
            let step = *entry;
            *entry = entry.saturating_add(1);
            return Verdict::Nack { step: u64::from(step) };
        }
        self.contended.remove(id);
        let attempt = self.attempt(id, broker_attempt);
        let exhausted = self.max_attempts > 0 && attempt >= self.max_attempts;
        if self.sink && (failure == Failure::Terminal || exhausted) {
            return Verdict::Dead { attempt };
        }
        // Recorded whatever the ceiling, so the wait grows across redeliveries: unrecorded, every
        // attempt would be the first and every wait the first step.
        self.attempts.insert(id.to_owned(), attempt);
        Verdict::Nack {
            step: u64::from(attempt.saturating_sub(1)),
        }
    }

    /// The sink's answer for the message `id`, dead at `attempt`: `None` if it took the message, else
    /// the failure it refused it with. Returns the backoff step to wait before the nack when the
    /// message goes back, `None` when nothing waits.
    ///
    /// - taken: the counts are forgotten, and the host acks the message;
    /// - refused: the attempt is kept, so the redelivery dies again at the next one;
    /// - refused fatally: `None`, as for a taken message, but the host stops on the sink's failure and
    ///   hands the message back uncounted, since the sink's wiring fails every dead message alike.
    pub fn sunk(&mut self, id: &str, attempt: u32, refusal: Option<Failure>) -> Option<u64> {
        match refusal {
            None => {
                self.succeeded(id);
                None
            }
            Some(Failure::Fatal) => None,
            Some(_) => {
                self.attempts.insert(id.to_owned(), attempt);
                Some(u64::from(attempt.saturating_sub(1)))
            }
        }
    }

    /// How many messages the policy is tracking.
    pub fn tracked(&self) -> Tracked {
        Tracked {
            attempts: self.attempts.len(),
            contended: self.contended.len(),
        }
    }
}

/// A loop's run of failed calls to the source it reads: the store a relay drains, or the broker a
/// consumer polls. Each failure in a row waits one backoff step more than the last, so a source that
/// stays down is neither spun on nor hammered; a call that answers starts the count over. Which failure
/// stops the loop is [`Failure::of`]'s: a fatal one, as everywhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Streak {
    failures: u64,
}

impl Streak {
    /// A call failed: the backoff step to wait before the next call, `0` for the first failure in a row.
    pub fn failed(&mut self) -> u64 {
        let step = self.failures;
        self.failures = self.failures.saturating_add(1);
        step
    }

    /// A call answered: the next failure waits the first step again.
    pub fn answered(&mut self) {
        self.failures = 0;
    }
}

/// The attempts of an in-process retry, `n`: at least one. `0` is the terminal `bad_handle` error, since
/// no attempt at all is not a retry policy.
pub fn retry_attempts(n: u32) -> Result<NonZeroU32, Error> {
    NonZeroU32::new(n).ok_or_else(|| Error::bad_handle("retry needs at least one attempt, got 0"))
}

/// One decision of an in-process retry that may run its work `attempts` times: after the work failed with
/// `failure`, having failed `failed` times before, wait the returned backoff step and run it again, or
/// stop on `None`. It stops on a terminal or fatal failure, and on the `attempts`-th failure; the wait
/// is step `failed`, so the first retry waits step 0.
///
/// Unlike [`Retries`], contention spends an attempt here. A caller is waiting on the result, so a winner
/// that never finishes must not hold it forever; a consumer hands the message back instead, so its
/// contention is free.
pub fn retry_step(attempts: NonZeroU32, failed: u32, failure: Failure) -> Option<u64> {
    match failure {
        Failure::Fatal | Failure::Terminal => None,
        Failure::Transient | Failure::Contended if failed.saturating_add(1) >= attempts.get() => None,
        Failure::Transient | Failure::Contended => Some(u64::from(failed)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each failure in a row waits one step more; an answer starts over.
    #[test]
    fn streak_steps() {
        let mut streak = Streak::default();
        assert_eq!(
            [streak.failed(), streak.failed(), streak.failed()],
            [0, 1, 2],
            "a growing step per failure in a row"
        );
        streak.answered();
        assert_eq!(streak.failed(), 0, "an answer starts the count over");
        let mut saturated = Streak { failures: u64::MAX };
        assert_eq!(
            [saturated.failed(), saturated.failed()],
            [u64::MAX, u64::MAX],
            "the step saturates"
        );
    }

    #[test]
    fn retry_step_branches() {
        let three = NonZeroU32::new(3).expect("non-zero");
        let cases = [
            (
                "the first transient failure waits step 0",
                three,
                0,
                Failure::Transient,
                Some(0),
            ),
            ("a later failure waits its count", three, 1, Failure::Transient, Some(1)),
            ("the last attempt stops", three, 2, Failure::Transient, None),
            ("a terminal failure stops at once", three, 0, Failure::Terminal, None),
            ("a fatal failure stops at once", three, 0, Failure::Fatal, None),
            ("contention spends an attempt", three, 2, Failure::Contended, None),
            (
                "contention retries below the ceiling",
                three,
                0,
                Failure::Contended,
                Some(0),
            ),
            (
                "one attempt never retries",
                NonZeroU32::MIN,
                0,
                Failure::Transient,
                None,
            ),
            (
                "the count saturates",
                NonZeroU32::MAX,
                u32::MAX,
                Failure::Transient,
                None,
            ),
        ];
        for (name, attempts, failed, failure, want) in cases {
            assert_eq!(retry_step(attempts, failed, failure), want, "{name}");
        }
        assert_eq!(retry_attempts(0).map_err(|e| e.code), Err(ErrorCode::BadHandle));
        assert_eq!(retry_attempts(1), Ok(NonZeroU32::MIN));
    }

    #[test]
    fn failure_of_branches() {
        assert_eq!(
            Failure::of(None),
            Failure::Transient,
            "an unclassified failure is transient"
        );
        assert_eq!(
            Failure::of(Some(ErrorCode::InProgress)),
            Failure::Contended,
            "in_progress is contention"
        );
        assert_eq!(
            Failure::of(Some(ErrorCode::BadHandle)),
            Failure::Fatal,
            "bad_handle is a wiring fault"
        );
        for &code in ErrorCode::ALL.iter().filter(|&&c| c != ErrorCode::InProgress) {
            let want = match (code.is_fatal(), code.is_terminal()) {
                (true, _) => Failure::Fatal,
                (false, true) => Failure::Terminal,
                (false, false) => Failure::Transient,
            };
            assert_eq!(Failure::of(Some(code)), want, "{code}");
        }
    }

    #[test]
    fn failed_branches() {
        struct Case {
            name: &'static str,
            max_attempts: u32,
            recorded: Option<u32>,
            broker_attempt: u32,
            failure: Failure,
            sink: bool,
            want: Verdict,
            /// The attempt recorded afterwards.
            want_recorded: Option<u32>,
        }
        let base = Case {
            name: "",
            max_attempts: 3,
            recorded: None,
            broker_attempt: 0,
            failure: Failure::Transient,
            sink: true,
            want: Verdict::Nack { step: 0 },
            want_recorded: Some(1),
        };
        let cases = [
            Case {
                name: "first transient failure nacks at step 0",
                ..base
            },
            Case {
                name: "a recorded attempt grows the step",
                recorded: Some(1),
                want: Verdict::Nack { step: 1 },
                want_recorded: Some(2),
                ..base
            },
            Case {
                name: "the ceiling kills a transient failure",
                recorded: Some(2),
                want: Verdict::Dead { attempt: 3 },
                want_recorded: Some(2),
                ..base
            },
            Case {
                name: "a terminal failure dies at once",
                failure: Failure::Terminal,
                want: Verdict::Dead { attempt: 1 },
                want_recorded: None,
                ..base
            },
            Case {
                name: "no sink: a terminal failure nacks",
                failure: Failure::Terminal,
                sink: false,
                ..base
            },
            Case {
                name: "no sink: an exhausted one nacks and keeps growing",
                recorded: Some(5),
                sink: false,
                want: Verdict::Nack { step: 5 },
                want_recorded: Some(6),
                ..base
            },
            Case {
                name: "max_attempts 0 has no ceiling",
                max_attempts: 0,
                recorded: Some(40),
                want: Verdict::Nack { step: 40 },
                want_recorded: Some(41),
                ..base
            },
            Case {
                name: "a broker attempt raises the count",
                broker_attempt: 2,
                want: Verdict::Dead { attempt: 3 },
                want_recorded: None,
                ..base
            },
            Case {
                name: "a lower broker attempt does not lower it",
                recorded: Some(1),
                broker_attempt: 0,
                want: Verdict::Nack { step: 1 },
                want_recorded: Some(2),
                ..base
            },
            Case {
                name: "the count saturates",
                max_attempts: 0,
                broker_attempt: u32::MAX,
                want: Verdict::Nack {
                    step: u64::from(u32::MAX - 1),
                },
                want_recorded: Some(u32::MAX),
                ..base
            },
            Case {
                name: "contention never counts",
                failure: Failure::Contended,
                recorded: Some(2),
                want: Verdict::Nack { step: 0 },
                want_recorded: Some(2),
                ..base
            },
            Case {
                name: "a fatal failure stops and never counts",
                failure: Failure::Fatal,
                recorded: Some(1),
                want: Verdict::Stop,
                want_recorded: Some(1),
                ..base
            },
            Case {
                name: "no sink: a fatal failure still stops",
                failure: Failure::Fatal,
                sink: false,
                want: Verdict::Stop,
                want_recorded: None,
                ..base
            },
        ];
        for c in cases {
            let mut retries = Retries::new(c.max_attempts, c.sink);
            if let Some(n) = c.recorded {
                let _ = retries.sunk("evt-1", n, Some(Failure::Transient));
            }
            assert_eq!(
                retries.failed("evt-1", c.broker_attempt, c.failure),
                c.want,
                "{}",
                c.name
            );
            assert_eq!(
                retries.attempts.get("evt-1").copied(),
                c.want_recorded,
                "{}: recorded",
                c.name
            );
        }
    }

    #[test]
    fn contention_grows_its_own_step_until_a_real_outcome_clears_it() {
        let mut retries = Retries::new(3, true);
        for step in 0..3 {
            assert_eq!(retries.failed("evt-1", 0, Failure::Contended), Verdict::Nack { step });
        }
        assert_eq!(
            retries.tracked(),
            Tracked {
                attempts: 0,
                contended: 1
            }
        );
        // A real failure clears the contention count and starts the attempt count at 1.
        assert_eq!(
            retries.failed("evt-1", 0, Failure::Transient),
            Verdict::Nack { step: 0 }
        );
        assert_eq!(
            retries.tracked(),
            Tracked {
                attempts: 1,
                contended: 0
            }
        );
        assert_eq!(
            retries.failed("evt-1", 0, Failure::Contended),
            Verdict::Nack { step: 0 },
            "contention starts over"
        );
        retries.succeeded("evt-1");
        assert_eq!(retries.tracked(), Tracked::default(), "success forgets both");
    }

    #[test]
    fn a_failed_sink_keeps_the_attempt_and_the_redelivery_dies_again() {
        let mut retries = Retries::new(2, true);
        assert_eq!(
            retries.failed("evt-1", 0, Failure::Transient),
            Verdict::Nack { step: 0 }
        );
        assert_eq!(
            retries.failed("evt-1", 0, Failure::Transient),
            Verdict::Dead { attempt: 2 }
        );
        assert_eq!(retries.sunk("evt-1", 2, Some(Failure::Transient)), Some(1));
        assert_eq!(retries.attempt("evt-1", 0), 3);
        assert_eq!(
            retries.failed("evt-1", 0, Failure::Transient),
            Verdict::Dead { attempt: 3 }
        );
        assert_eq!(retries.sunk("evt-1", 3, None), None, "a taken message waits nothing");
        assert_eq!(retries.tracked(), Tracked::default());
    }

    #[test]
    fn a_fatal_sink_failure_stops_and_keeps_no_attempt() {
        let mut retries = Retries::new(2, true);
        assert_eq!(
            retries.failed("evt-1", 0, Failure::Terminal),
            Verdict::Dead { attempt: 1 }
        );
        assert_eq!(retries.sunk("evt-1", 1, Some(Failure::Fatal)), None);
        assert_eq!(
            retries.attempt("evt-1", 0),
            1,
            "the sink's wiring failed it, not the message"
        );
    }
}
