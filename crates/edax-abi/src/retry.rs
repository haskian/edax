//! The retry and dead-letter policy of `edax-event`, as a consumer and an outbox relay perform it:
//! [`Retries`], [`Streak`], [`retry_step`] and the relay's [`Drain`].

use std::sync::{Arc, Mutex};

use crate::{count, failure_of, names, to_wire};

/// What [`Retries::failed`] tells the host to do: `action` is one of [`retry_actions`]. `"nack"` hands
/// the message back after backoff step `step`; `"dead"` hands it to the sink, then acks it, `attempt`
/// being the attempt that killed it; `"stop"` hands it back uncounted and stops the consumer or the
/// relay on the failure. A field the action does not use is `None`. `failure` is the kind the kernel read off the failure's
/// code, one of [`retry_failures`], so a host labels a nack for contention without classifying the
/// failure itself. See [`edax_event::Verdict`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryVerdict {
    pub action: String,
    pub attempt: Option<u32>,
    pub step: Option<u64>,
    pub failure: String,
}

fn retry_verdict(verdict: edax_event::Verdict, failure: edax_event::Failure) -> RetryVerdict {
    let action = verdict.kind().as_str().to_owned();
    let failure = failure.as_str().to_owned();
    match verdict {
        edax_event::Verdict::Nack { step } => RetryVerdict {
            action,
            attempt: None,
            step: Some(step),
            failure,
        },
        edax_event::Verdict::Dead { attempt } => RetryVerdict {
            action,
            attempt: Some(attempt),
            step: None,
            failure,
        },
        edax_event::Verdict::Stop => RetryVerdict {
            action,
            attempt: None,
            step: None,
            failure,
        },
    }
}

/// Every name [`RetryVerdict::action`] takes, in the kernel's order. A host checks the verdicts it
/// branches on against this list. See [`edax_event::VerdictKind`].
pub fn retry_actions() -> Vec<String> {
    names(edax_event::VerdictKind::NAMES)
}

/// The kind of a failure that carries the taxonomy code `code`, `None` for one no code classifies:
/// one of [`retry_failures`]. `"fatal"` is a wiring fault that stops a consumer or a relay. An unknown
/// code is a `codec:` error. See [`edax_event::Failure::of`].
pub fn retry_failure(code: Option<String>) -> Result<String, String> {
    Ok(failure_of(code)?.as_str().to_owned())
}

/// Every name [`retry_failure`] returns, in the kernel's order. See [`edax_event::Failure`].
pub fn retry_failures() -> Vec<String> {
    names(edax_event::Failure::NAMES)
}

/// How many messages a [`Retries`] tracks. See [`edax_event::Tracked`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RetriesTracked {
    pub attempts: u64,
    pub contended: u64,
}

impl From<edax_event::Tracked> for RetriesTracked {
    fn from(tracked: edax_event::Tracked) -> Self {
        Self {
            attempts: count(tracked.attempts),
            contended: count(tracked.contended),
        }
    }
}

/// The consumer's retry and dead-letter policy, keyed by message id. Every decision is data the host
/// performs: the handler, the sink, the wait and the ack or nack stay the host's. Construct it with
/// [`retries`]. See [`edax_event::Retries`].
pub struct Retries {
    inner: Arc<Mutex<edax_event::Retries>>,
}

impl Retries {
    /// The attempt a run of `id` is: `max(broker_attempt, recorded) + 1`.
    pub fn attempt(&self, id: &str, broker_attempt: u32) -> u32 {
        edax_core::lock(&self.inner).attempt(id, broker_attempt)
    }

    /// `id` succeeded: forget its counts.
    pub fn succeeded(&self, id: &str) {
        edax_core::lock(&self.inner).succeeded(id);
    }

    /// `id` failed with a failure carrying the taxonomy code `code`, `None` for one no code classifies;
    /// the kernel reads the kind off the code ([`edax_event::Failure::of`]). An unknown code is a
    /// `codec:` error.
    pub fn failed(&self, id: &str, broker_attempt: u32, code: Option<String>) -> Result<RetryVerdict, String> {
        let failure = failure_of(code)?;
        Ok(retry_verdict(
            edax_core::lock(&self.inner).failed(id, broker_attempt, failure),
            failure,
        ))
    }

    /// The sink's answer for the message `id`, dead at `attempt`: `taken`, or refused with a failure
    /// carrying the taxonomy `code` (`None` for one no code classifies; read only when not `taken`).
    /// Returns the backoff step to wait before the nack of a refused message, or `None` when nothing
    /// waits: the sink took it (ack it), or refused it fatally (stop the consumer). An unknown code is a
    /// `codec:` error. See [`edax_event::Retries::sunk`].
    // `code` is optional, so it trails: alef moves optional parameters to the end of the signature.
    pub fn sunk(&self, id: &str, attempt: u32, taken: bool, code: Option<String>) -> Result<Option<u64>, String> {
        let refusal = refusal(taken, code)?;
        Ok(edax_core::lock(&self.inner).sunk(id, attempt, refusal))
    }

    /// How many messages the policy tracks.
    pub fn tracked(&self) -> RetriesTracked {
        RetriesTracked::from(edax_core::lock(&self.inner).tracked())
    }
}

/// The kernel's form of a sink's answer: `None` when it took the message, else the kind of failure its
/// refusal carries `code`. An unknown code is a `codec:` error, whatever `taken` says.
fn refusal(taken: bool, code: Option<String>) -> Result<Option<edax_event::Failure>, String> {
    let failure = failure_of(code)?;
    Ok((!taken).then_some(failure))
}

/// A retry policy that dead-letters a transient failure at its `max_attempts`-th attempt (`0`: only
/// a terminal one dies). `sink` is whether the host has a dead-letter sink: without one nothing dies.
pub fn retries(max_attempts: u32, sink: bool) -> Retries {
    Retries {
        inner: Arc::new(Mutex::new(edax_event::Retries::new(max_attempts, sink))),
    }
}

/// A loop's run of failed calls to the source it reads, a broker it polls: each failure in a row waits
/// one backoff step more. Construct it with [`streak`]. See [`edax_event::Streak`].
pub struct Streak {
    inner: Arc<Mutex<edax_event::Streak>>,
}

impl Streak {
    /// A call failed: the backoff step to wait before the next call.
    pub fn failed(&self) -> u64 {
        edax_core::lock(&self.inner).failed()
    }

    /// A call answered: the next failure waits the first step again.
    pub fn answered(&self) {
        edax_core::lock(&self.inner).answered();
    }
}

/// A streak with no failure yet.
pub fn streak() -> Streak {
    Streak {
        inner: Arc::new(Mutex::new(edax_event::Streak::default())),
    }
}

/// Refuse an in-process retry of `attempts` the kernel refuses: `0` is a `bad_handle:` error, since no
/// attempt at all is not a retry policy. A host checks the count once, at wiring, before any decision.
/// See [`edax_event::retry_attempts`].
pub fn retry_attempts(attempts: u32) -> Result<(), String> {
    edax_event::retry_attempts(attempts).map(drop).map_err(to_wire)
}

/// One decision of an in-process retry that may run its work `attempts` times, after the work failed
/// with the taxonomy `code` (`None` for a failure no code classifies), having failed `failed` times
/// before: the backoff step to wait before running it again, or `None` to stop and raise the failure.
/// `attempts` of `0` is a `bad_handle:` error, and an unknown code a `codec:` error. See
/// [`edax_event::retry_step`].
// `code` is optional, so it trails: alef moves optional parameters to the end of the signature.
pub fn retry_step(attempts: u32, failed: u32, code: Option<String>) -> Result<Option<u64>, String> {
    let attempts = edax_event::retry_attempts(attempts).map_err(to_wire)?;
    Ok(edax_event::retry_step(attempts, failed, failure_of(code)?))
}

/// What [`Drain::failed`] tells the host to do with a row: `action` is one of [`drain_actions`].
/// `"hand_back"` leaves it, and its key's later rows, for the next drain; `"dead_letter"` hands it to the
/// sink, `attempt` being the attempt that killed it, then reports its answer through [`Drain::sunk`];
/// `"stop"` publishes nothing more. `attempt` is set only when dead. See [`edax_event::RowFailed`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DrainRowFailed {
    pub action: String,
    pub attempt: Option<u32>,
}

impl From<edax_event::RowFailed> for DrainRowFailed {
    fn from(answer: edax_event::RowFailed) -> Self {
        let attempt = match answer {
            edax_event::RowFailed::DeadLetter { attempt } => Some(attempt),
            edax_event::RowFailed::HandBack | edax_event::RowFailed::Stop => None,
        };
        Self {
            action: answer.kind().as_str().to_owned(),
            attempt,
        }
    }
}

/// Every name [`DrainRowFailed::action`] takes, in the kernel's order. See [`edax_event::RowFailedKind`].
pub fn drain_actions() -> Vec<String> {
    names(edax_event::RowFailedKind::NAMES)
}

/// What one drain did: the row ids to mark published, the counts, the backoff step to wait before
/// returning, if any, and the row whose failure to raise, if any. See [`edax_event::Drained`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Drained {
    #[serde(default)]
    pub settled: Vec<String>,
    pub published: u64,
    pub dead_lettered: u64,
    pub wait_step: Option<u64>,
    pub raise_id: Option<String>,
}

/// The keys of one outbox row a [`Drain`] decides over: an event's or a command's `id`, `tenant` and
/// `aggregate_id`.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DrainRow {
    pub id: String,
    pub tenant: Option<String>,
    pub aggregate_id: Option<String>,
}

/// The drain rule of one outbox relay: which rows a drain publishes, what a failed row does to the rows
/// behind it, and how long the drain waits. Every decision is data the host performs. Construct it with
/// [`drain`]. See [`edax_event::Drain`].
pub struct Drain {
    inner: Arc<Mutex<edax_event::Drain>>,
}

impl Drain {
    /// Start a drain of the batch an outbox returned, given as each row's keys only: the drain reads
    /// nothing else of a row, so its payload never crosses. Every row is keyed by its tenant and its
    /// ordering key: its aggregate, or its own id when it targets none. See
    /// [`edax_event::Drain::begin_keys`].
    pub fn begin(&self, rows: Vec<DrainRow>) {
        let keys = rows.iter().map(|row| {
            (
                row.id.as_str(),
                row.tenant.as_deref(),
                edax_event::ordering_key(row.aggregate_id.as_deref(), &row.id),
            )
        });
        edax_core::lock(&self.inner).begin_keys(keys);
    }

    /// Whether the drain publishes the row `id`.
    pub fn admits(&self, id: &str) -> bool {
        edax_core::lock(&self.inner).admits(id)
    }

    /// The row `id` was published.
    pub fn published(&self, id: &str) {
        edax_core::lock(&self.inner).published(id);
    }

    /// The row `id` failed to publish with a failure carrying the taxonomy `code`, `None` for one no code
    /// classifies. An unknown code is a `codec:` error.
    pub fn failed(&self, id: &str, code: Option<String>) -> Result<DrainRowFailed, String> {
        let failure = failure_of(code)?;
        Ok(DrainRowFailed::from(edax_core::lock(&self.inner).failed(id, failure)))
    }

    /// The sink's answer for the dead row `id`, dead at `attempt`: `taken`, or refused with a failure
    /// carrying the taxonomy `code` (`None` for one no code classifies; read only when not `taken`). A
    /// taken row is settled, and the answer is `None`. A refused row is `"hand_back"`, or `"stop"` for a
    /// fatal refusal, whose failure the host raises for `id`. An unknown code is a `codec:` error. See
    /// [`edax_event::Drain::sunk`].
    // `code` is optional, so it trails: alef moves optional parameters to the end of the signature.
    pub fn sunk(
        &self,
        id: &str,
        attempt: u32,
        taken: bool,
        code: Option<String>,
    ) -> Result<Option<DrainRowFailed>, String> {
        let refusal = refusal(taken, code)?;
        let answer = edax_core::lock(&self.inner).sunk(id, attempt, refusal);
        Ok(answer.map(DrainRowFailed::from))
    }

    /// End the drain: what to mark, what to wait and what to raise.
    pub fn finish(&self) -> Drained {
        let drained = edax_core::lock(&self.inner).finish();
        Drained {
            settled: drained.settled,
            published: drained.published,
            dead_lettered: drained.dead_lettered,
            wait_step: drained.wait,
            raise_id: drained.raise,
        }
    }

    /// A store call failed: the backoff step to wait before raising it.
    pub fn store_failed(&self) -> u64 {
        edax_core::lock(&self.inner).store_failed()
    }

    /// Every store call of a drain succeeded.
    pub fn store_answered(&self) {
        edax_core::lock(&self.inner).store_answered();
    }

    /// How many rows the retry policy tracks.
    pub fn tracked(&self) -> RetriesTracked {
        RetriesTracked::from(edax_core::lock(&self.inner).tracked())
    }
}

/// The drain rule of a relay whose rows die at their `max_attempts`-th transient failure (`0`: never) or
/// at a terminal one, when it has a dead-letter `sink`.
pub fn drain(max_attempts: u32, sink: bool) -> Drain {
    Drain {
        inner: Arc::new(Mutex::new(edax_event::Drain::new(max_attempts, sink))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_cross_their_decisions_structurally() {
        let code = |c: &str| Some(c.to_owned());
        let policy = retries(2, true);
        let nack = policy.failed("evt-1", 0, code("store")).unwrap();
        assert_eq!((nack.action.as_str(), nack.attempt, nack.step), ("nack", None, Some(0)));
        let dead = policy.failed("evt-1", 0, None).unwrap();
        assert_eq!(
            (dead.action.as_str(), dead.attempt, dead.step),
            ("dead", Some(2), None),
            "an unclassified failure is transient and counts"
        );
        assert_eq!(
            policy.sunk("evt-1", 2, false, None),
            Ok(Some(1)),
            "a refusal with no code is transient"
        );
        assert_eq!(policy.attempt("evt-1", 0), 3);
        assert_eq!(
            policy.sunk("evt-1", 3, true, None),
            Ok(None),
            "a taken message waits nothing"
        );
        assert_eq!(
            policy.sunk("evt-1", 3, true, code("poison")).err().as_deref(),
            Some("codec: unknown error code: poison")
        );
        let contended = policy.failed("evt-2", 0, code("in_progress")).unwrap();
        assert_eq!((contended.action.as_str(), contended.step), ("nack", Some(0)));
        let tracked = policy.tracked();
        assert_eq!((tracked.attempts, tracked.contended), (0, 1));
        policy.succeeded("evt-2");
        assert_eq!(policy.tracked().contended, 0);
        assert_eq!(
            policy.failed("evt-1", 0, code("poison")).err().as_deref(),
            Some("codec: unknown error code: poison")
        );
        let terminal = retries(2, false).failed("evt-3", 0, code("codec")).unwrap();
        assert_eq!(
            (terminal.action.as_str(), terminal.step),
            ("nack", Some(0)),
            "no sink: a terminal failure nacks"
        );
        let fatal = retries(2, true).failed("evt-4", 0, code("bad_handle")).unwrap();
        assert_eq!(
            (fatal.action.as_str(), fatal.attempt, fatal.step),
            ("stop", None, None),
            "a wiring fault stops"
        );
        assert_eq!(retry_actions(), ["nack", "dead", "stop"]);
        assert_eq!(retry_failure(code("bad_handle")).as_deref(), Ok("fatal"));
        assert_eq!(
            retry_failure(None).as_deref(),
            Ok("transient"),
            "unclassified is the kernel's default"
        );
        assert_eq!(
            retry_failure(code("poison")).err().as_deref(),
            Some("codec: unknown error code: poison")
        );
        assert_eq!(retry_failures(), ["fatal", "terminal", "transient", "contended"]);
    }

    #[test]
    fn retry_step_crosses_the_decision() {
        let code = |c: &str| Some(c.to_owned());
        assert_eq!(retry_step(3, 0, code("store")), Ok(Some(0)));
        assert_eq!(retry_step(3, 2, None), Ok(None), "the last attempt stops");
        assert_eq!(retry_step(3, 0, code("codec")), Ok(None), "a terminal failure stops");
        assert_eq!(
            retry_step(0, 0, None),
            Err("bad_handle: retry needs at least one attempt, got 0".to_owned())
        );
        assert_eq!(
            retry_step(3, 0, code("poison")).err().as_deref(),
            Some("codec: unknown error code: poison")
        );
        assert_eq!(retry_attempts(1), Ok(()));
        assert_eq!(
            retry_attempts(0),
            Err("bad_handle: retry needs at least one attempt, got 0".to_owned())
        );
    }

    #[test]
    fn drain_crosses_its_decisions_structurally() {
        let row = |id: &str, aggregate: &str| DrainRow {
            id: id.to_owned(),
            tenant: None,
            aggregate_id: Some(aggregate.to_owned()),
        };
        let drain = drain(0, true);
        drain.begin(vec![row("a-1", "acct-A"), row("b-1", "acct-B"), row("a-2", "acct-A")]);
        let dead = drain.failed("a-1", Some("codec".to_owned())).unwrap();
        assert_eq!((dead.action.as_str(), dead.attempt), ("dead_letter", Some(1)));
        let refused = drain
            .sunk("a-1", 1, false, None)
            .unwrap()
            .map(|answer| (answer.action, answer.attempt));
        assert_eq!(
            refused,
            Some(("hand_back".to_owned(), None)),
            "only a dead row carries an attempt"
        );
        assert!(
            drain.admits("b-1") && !drain.admits("a-2"),
            "a refused sink blocks its key only"
        );
        drain.published("b-1");
        let drained = drain.finish();
        assert_eq!(drained.settled, ["b-1"]);
        assert_eq!(
            (drained.published, drained.dead_lettered, drained.wait_step),
            (1, 0, Some(0))
        );
        assert_eq!(drained.raise_id.as_deref(), Some("a-1"));
        assert_eq!((drain.store_failed(), drain.store_failed()), (0, 1));
        drain.store_answered();
        assert_eq!(drain.store_failed(), 0);
        assert_eq!(
            drain.failed("a-1", Some("poison".to_owned())).err().as_deref(),
            Some("codec: unknown error code: poison")
        );
        assert_eq!(drain.tracked().attempts, 1);
    }
}

/// A sink result with an owned outer record for Go's generated return conversion.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DrainReply {
    pub value: Option<DrainRowFailed>,
}

impl Drain {
    /// The ownership-safe C result shape of [`Self::sunk`].
    pub fn sunk_result(&self, id: &str, attempt: u32, taken: bool, code: Option<String>) -> Result<DrainReply, String> {
        self.sunk(id, attempt, taken, code).map(|value| DrainReply { value })
    }
}
