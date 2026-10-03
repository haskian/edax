//! The consumer's retry and dead-letter policy: when a failed message is handed back, after how
//! long, and when it dies.
//!
//! Two SDKs consuming one topic must agree when a message dies, or the same poison message is
//! dead-lettered by one service and retried forever by the other. So the rule is here, sans-IO, in
//! the [`OffsetTracker`](https://docs.rs/edax-kafka) style: the host runs the handler, the sink, the
//! wait and the ack or nack; [`Retries`] only says which, and keeps the per-message counts the rule
//! reads. The same policy drains an outbox: a relay keys it by row id instead of envelope id.
//!
//! The rule, pinned by `testdata/golden/retries.json`:
//!
//! - the attempt a run is: `max(broker_attempt, recorded) + 1`, so a broker that counts its own
//!   redeliveries raises it, and a restart that forgot the local count does not lower it;
//! - a failure's kind is its error code's ([`Failure::of`]): `in_progress` is contention, a terminal
//!   code is terminal, and any other code, or none, is transient;
//! - a message dies when its failure is terminal, or when `0 < max_attempts <= attempt`; a dead
//!   message goes to the sink if the policy was built with one, and is nacked like a transient
//!   failure if it was not;
//! - a nack waits backoff step `attempt - 1`, so the wait grows across redeliveries;
//! - contention (another consumer holds the message's idempotency claim) is not a failed attempt: it
//!   waits its own growing step and never counts toward `max_attempts`;
//! - a sink that fails nacks the message after step `attempt - 1` and keeps the attempt, so the
//!   redelivery dies again at the next attempt and the sink gets another try.

use std::collections::HashMap;

use edax_core::ErrorCode;

/// How a run failed, as the policy reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// Deterministic: a retry fails the same way, so the message dies at once.
    Terminal,
    /// May succeed on a later attempt; counts toward `max_attempts`.
    Transient,
    /// Another consumer holds the message's idempotency claim. Not a failed attempt.
    Contended,
}

impl Failure {
    /// The kind of a failure that carries `code`, `None` for one no code classifies: contention for
    /// [`ErrorCode::InProgress`], terminal for a terminal code, and transient otherwise (the kernel's
    /// default for an unclassified failure, [`edax_core::verdict`]). One rule, so two SDKs consuming one
    /// topic count and kill the same failures.
    pub fn of(code: Option<ErrorCode>) -> Failure {
        match code {
            Some(ErrorCode::InProgress) => Failure::Contended,
            code if edax_core::verdict(code) => Failure::Terminal,
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
    /// Hand the message to the dead-letter sink, then ack it. Report the sink's outcome with
    /// [`Retries::dead_lettered`] or [`Retries::sink_failed`].
    Dead {
        /// The failed attempt that killed it: `1` for a terminal failure on first delivery.
        attempt: u32,
    },
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
/// id (an envelope id, an outbox row id). An entry stays until the id succeeds or dies; one whose
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
        Self { max_attempts, sink, ..Self::default() }
    }

    /// The attempt a run of `id` is: `max(broker_attempt, recorded) + 1`, saturating.
    pub fn attempt(&self, id: &str, broker_attempt: u32) -> u32 {
        broker_attempt.max(self.attempts.get(id).copied().unwrap_or(0)).saturating_add(1)
    }

    /// `id` succeeded: forget its counts.
    pub fn succeeded(&mut self, id: &str) {
        self.attempts.remove(id);
        self.contended.remove(id);
    }

    /// `id` failed with `failure`, at the broker's own redelivery count `broker_attempt`.
    pub fn failed(&mut self, id: &str, broker_attempt: u32, failure: Failure) -> Verdict {
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
        Verdict::Nack { step: u64::from(attempt.saturating_sub(1)) }
    }

    /// The sink took the dead message: forget its counts.
    pub fn dead_lettered(&mut self, id: &str) {
        self.succeeded(id);
    }

    /// The sink refused the dead message at `attempt`: keep the attempt, so the redelivery dies
    /// again at the next one. Returns the backoff step to wait before the nack.
    pub fn sink_failed(&mut self, id: &str, attempt: u32) -> u64 {
        self.attempts.insert(id.to_owned(), attempt);
        u64::from(attempt.saturating_sub(1))
    }

    /// How many messages the policy is tracking.
    pub fn tracked(&self) -> Tracked {
        Tracked { attempts: self.attempts.len(), contended: self.contended.len() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_of_branches() {
        assert_eq!(Failure::of(None), Failure::Transient, "an unclassified failure is transient");
        assert_eq!(Failure::of(Some(ErrorCode::InProgress)), Failure::Contended, "in_progress is contention");
        for &code in ErrorCode::ALL.iter().filter(|&&c| c != ErrorCode::InProgress) {
            let want = if code.is_terminal() { Failure::Terminal } else { Failure::Transient };
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
            Case { name: "first transient failure nacks at step 0", ..base },
            Case { name: "a recorded attempt grows the step", recorded: Some(1), want: Verdict::Nack { step: 1 }, want_recorded: Some(2), ..base },
            Case { name: "the ceiling kills a transient failure", recorded: Some(2), want: Verdict::Dead { attempt: 3 }, want_recorded: Some(2), ..base },
            Case { name: "a terminal failure dies at once", failure: Failure::Terminal, want: Verdict::Dead { attempt: 1 }, want_recorded: None, ..base },
            Case { name: "no sink: a terminal failure nacks", failure: Failure::Terminal, sink: false, ..base },
            Case { name: "no sink: an exhausted one nacks and keeps growing", recorded: Some(5), sink: false, want: Verdict::Nack { step: 5 }, want_recorded: Some(6), ..base },
            Case { name: "max_attempts 0 has no ceiling", max_attempts: 0, recorded: Some(40), want: Verdict::Nack { step: 40 }, want_recorded: Some(41), ..base },
            Case { name: "a broker attempt raises the count", broker_attempt: 2, want: Verdict::Dead { attempt: 3 }, want_recorded: None, ..base },
            Case { name: "a lower broker attempt does not lower it", recorded: Some(1), broker_attempt: 0, want: Verdict::Nack { step: 1 }, want_recorded: Some(2), ..base },
            Case { name: "the count saturates", max_attempts: 0, broker_attempt: u32::MAX, want: Verdict::Nack { step: u64::from(u32::MAX - 1) }, want_recorded: Some(u32::MAX), ..base },
            Case { name: "contention never counts", failure: Failure::Contended, recorded: Some(2), want: Verdict::Nack { step: 0 }, want_recorded: Some(2), ..base },
        ];
        for c in cases {
            let mut retries = Retries::new(c.max_attempts, c.sink);
            if let Some(n) = c.recorded {
                let _ = retries.sink_failed("evt-1", n);
            }
            assert_eq!(retries.failed("evt-1", c.broker_attempt, c.failure), c.want, "{}", c.name);
            assert_eq!(retries.attempts.get("evt-1").copied(), c.want_recorded, "{}: recorded", c.name);
        }
    }

    #[test]
    fn contention_grows_its_own_step_until_a_real_outcome_clears_it() {
        let mut retries = Retries::new(3, true);
        for step in 0..3 {
            assert_eq!(retries.failed("evt-1", 0, Failure::Contended), Verdict::Nack { step });
        }
        assert_eq!(retries.tracked(), Tracked { attempts: 0, contended: 1 });
        // A real failure clears the contention count and starts the attempt count at 1.
        assert_eq!(retries.failed("evt-1", 0, Failure::Transient), Verdict::Nack { step: 0 });
        assert_eq!(retries.tracked(), Tracked { attempts: 1, contended: 0 });
        assert_eq!(retries.failed("evt-1", 0, Failure::Contended), Verdict::Nack { step: 0 }, "contention starts over");
        retries.succeeded("evt-1");
        assert_eq!(retries.tracked(), Tracked::default(), "success forgets both");
    }

    #[test]
    fn a_failed_sink_keeps_the_attempt_and_the_redelivery_dies_again() {
        let mut retries = Retries::new(2, true);
        assert_eq!(retries.failed("evt-1", 0, Failure::Transient), Verdict::Nack { step: 0 });
        assert_eq!(retries.failed("evt-1", 0, Failure::Transient), Verdict::Dead { attempt: 2 });
        assert_eq!(retries.sink_failed("evt-1", 2), 1);
        assert_eq!(retries.attempt("evt-1", 0), 3);
        assert_eq!(retries.failed("evt-1", 0, Failure::Transient), Verdict::Dead { attempt: 3 });
        retries.dead_lettered("evt-1");
        assert_eq!(retries.tracked(), Tracked::default());
    }
}
