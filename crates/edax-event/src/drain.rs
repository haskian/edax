//! The outbox relay's drain: which rows of a batch it publishes, what a failed row does to the rows
//! behind it, and how long the drain waits before it returns.
//!
//! Two SDKs relaying one outbox must publish, skip and dead-letter the same rows, so the rule is here,
//! sans-IO, beside [`Retries`], which it keeps per row id. The host reads the batch, publishes each row
//! [`Drain::admits`], reports each publish and each dead-letter, marks the settled rows and waits; the
//! drain only says which. Pinned by `testdata/golden/relay_drain.json`.
//!
//! The rule:
//!
//! - order is promised per key only: a row's key is its tenant and its
//!   [`ordering_key`](crate::Message::ordering_key), so a failed row blocks the rows behind it of its
//!   own key for the rest of the drain, and every other key goes on;
//! - a failed row is handed back, dead-lettered or stops the drain as [`Retries`] answers; a dead row
//!   the sink took is settled, and its key goes on; a sink that refuses it hands it back, or stops the
//!   drain when the refusal is fatal;
//! - a fatal failure stops the drain at once, with no wait: every row behind it would fail alike;
//! - a second transient failure with no successful publish between them stops the drain too: a
//!   transport-wide outage fails every key, and publishing the rest would only spend each row an
//!   attempt and each drain a send timeout per row. Contention, a terminal failure and a dead letter
//!   are the row's own, so they neither count nor reset;
//! - a drain that handed a row back waits the largest backoff step among those rows, then raises the
//!   first one's failure, or the fatal one's. A drain the outage stopped with no row handed back (the
//!   sink took the failed rows) waits the step of the row that stopped it and raises its failure, so the
//!   outage is reported and the host's loop is paced;
//! - a drain whose store call fails waits one step per consecutive store failure; a drain whose store
//!   calls succeed starts that count over.

use std::collections::{HashMap, HashSet};

use crate::retry::{Failure, Retries, Streak, Tracked, Verdict};
use crate::Message;

/// How many transient failures in a row, with no successful publish between them, stop a drain.
const OUTAGE: u64 = 2;

/// The key a row is ordered by: its tenant and its ordering key, exact under every tenancy mode.
type RowKey = (Option<String>, String);

/// What the host does with a row whose publish failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum RowFailed {
    /// The row stays in the outbox, and so do the rows behind it of its key, for the next drain.
    HandBack,
    /// Hand the row to the dead-letter sink, then report its answer with [`Drain::sunk`].
    DeadLetter {
        /// The failed attempt that killed it.
        attempt: u32,
    },
    /// The failure is fatal: publish nothing more, and raise it at once.
    Stop,
}

impl RowFailed {
    /// The answer's name, what a binding branches on.
    pub fn kind(&self) -> RowFailedKind {
        match self {
            RowFailed::HandBack => RowFailedKind::HandBack,
            RowFailed::DeadLetter { .. } => RowFailedKind::DeadLetter,
            RowFailed::Stop => RowFailedKind::Stop,
        }
    }
}

edax_core::closed_names! {
    /// The name of a [`RowFailed`]. Exhaustive on purpose: a host must decide about every answer.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum RowFailedKind {
        /// [`RowFailed::HandBack`].
        HandBack = "hand_back",
        /// [`RowFailed::DeadLetter`].
        DeadLetter = "dead_letter",
        /// [`RowFailed::Stop`].
        Stop = "stop",
    }
}

/// What one drain did, once its rows are published.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Drained {
    /// The ids to mark published: every row published or dead-lettered, in publish order.
    pub settled: Vec<String>,
    /// How many rows were published.
    pub published: u64,
    /// How many rows the sink took.
    pub dead_lettered: u64,
    /// The backoff step to wait before returning, `None` for no wait.
    pub wait: Option<u64>,
    /// The id of the row whose failure the drain raises, `None` when no row was handed back and no
    /// outage stopped the drain.
    pub raise: Option<String>,
}

/// The state of the batch being drained.
#[derive(Debug, Clone, Default)]
struct Batch {
    keys: HashMap<String, RowKey>,
    blocked: HashSet<RowKey>,
    /// The transient failures since the last publish.
    transient: Streak,
    stopped: bool,
    fatal: bool,
    /// The row whose transient failure stopped the drain as an outage, and its backoff step.
    outage: Option<(String, u64)>,
    drained: Drained,
}

/// The drain rule of one relay: the retry policy over its rows, the store's failure count, and the batch
/// it is draining. Build it once per relay, and keep it across drains, so a row's attempts and the
/// store's step grow.
#[derive(Debug, Clone, Default)]
pub struct Drain {
    retries: Retries,
    store: Streak,
    batch: Batch,
}

impl Drain {
    /// A drain whose rows die as [`Retries::new`] says: a transient failure at its `max_attempts`-th
    /// attempt (`0`: never), a terminal one at once, and only when the relay has a `sink`.
    pub fn new(max_attempts: u32, sink: bool) -> Self {
        Self {
            retries: Retries::new(max_attempts, sink),
            ..Self::default()
        }
    }

    /// Start a drain of `rows`, the batch the store returned: every row is keyed by its tenant and its
    /// ordering key. Whatever the last drain left unfinished is dropped.
    pub fn begin<M: Message>(&mut self, rows: &[M]) {
        self.begin_keys(rows.iter().map(|row| (row.id(), row.tenant(), row.ordering_key())));
    }

    /// Start a drain of a batch given as each row's id, tenant and ordering key: [`begin`](Drain::begin)
    /// for a host that holds the rows itself, so only their keys cross. Whatever the last drain left
    /// unfinished is dropped.
    pub fn begin_keys<'a>(&mut self, rows: impl IntoIterator<Item = (&'a str, Option<&'a str>, &'a str)>) {
        let keys = rows
            .into_iter()
            .map(|(id, tenant, ordering_key)| (id.to_owned(), (tenant.map(ToOwned::to_owned), ordering_key.to_owned())))
            .collect();
        self.batch = Batch {
            keys,
            ..Batch::default()
        };
    }

    /// Whether the drain publishes the row `id`: `false` once its key is blocked or the drain stopped.
    pub fn admits(&self, id: &str) -> bool {
        if self.batch.stopped {
            return false;
        }
        match self.batch.keys.get(id) {
            Some(key) => !self.batch.blocked.contains(key),
            None => !self.batch.blocked.contains(&(None, id.to_owned())),
        }
    }

    /// The row `id` was published.
    pub fn published(&mut self, id: &str) {
        self.retries.succeeded(id);
        self.batch.transient.answered();
        self.batch.drained.published = self.batch.drained.published.saturating_add(1);
        self.batch.drained.settled.push(id.to_owned());
    }

    /// The row `id` failed to publish with `failure`.
    pub fn failed(&mut self, id: &str, failure: Failure) -> RowFailed {
        let verdict = self.retries.failed(id, 0, failure);
        if failure == Failure::Transient {
            // `failed` answers the failures before this one, so this one makes it one more.
            let streak = self.batch.transient.failed().saturating_add(1);
            if streak >= OUTAGE && !self.batch.stopped {
                self.batch.stopped = true;
                let step = match verdict {
                    Verdict::Nack { step } => step,
                    Verdict::Dead { attempt } => u64::from(attempt.saturating_sub(1)),
                    Verdict::Stop => 0,
                };
                self.batch.outage = Some((id.to_owned(), step));
            }
        }
        match verdict {
            Verdict::Stop => {
                self.stop(id);
                RowFailed::Stop
            }
            Verdict::Nack { step } => {
                self.hand_back(id, step);
                RowFailed::HandBack
            }
            Verdict::Dead { attempt } => RowFailed::DeadLetter { attempt },
        }
    }

    /// The sink's answer for the dead row `id`, dead at `attempt`: `None` if it took the row, else the
    /// failure it refused it with. A taken row is settled and its key goes on, and the answer is `None`.
    /// A refused row is `HandBack`, or `Stop` for a fatal refusal, whose failure the host then raises
    /// for `id`; never `DeadLetter`, since the sink just answered for the row.
    pub fn sunk(&mut self, id: &str, attempt: u32, refusal: Option<Failure>) -> Option<RowFailed> {
        let step = self.retries.sunk(id, attempt, refusal);
        match (refusal, step) {
            (None, _) => {
                self.batch.drained.dead_lettered = self.batch.drained.dead_lettered.saturating_add(1);
                self.batch.drained.settled.push(id.to_owned());
                None
            }
            (Some(_), Some(step)) => {
                self.hand_back(id, step);
                Some(RowFailed::HandBack)
            }
            (Some(_), None) => {
                self.stop(id);
                Some(RowFailed::Stop)
            }
        }
    }

    /// End the drain: what to mark, what to wait and what to raise.
    pub fn finish(&mut self) -> Drained {
        let batch = std::mem::take(&mut self.batch);
        let mut drained = batch.drained;
        if let (None, Some((id, step))) = (&drained.raise, batch.outage) {
            // The sink took every failed row, so nothing was handed back, yet the outage stopped the
            // drain: report it, or a host would drain again at once and spend each row an attempt.
            drained.raise = Some(id);
            drained.wait = Some(step);
        }
        if batch.fatal {
            // The host stops on a fatal failure, so a wait would only delay the report.
            drained.wait = None;
        }
        drained
    }

    /// A store call (the read or the mark) failed: the backoff step to wait before raising it.
    pub fn store_failed(&mut self) -> u64 {
        self.store.failed()
    }

    /// Every store call of a drain succeeded: the next store failure waits the first step again.
    pub fn store_answered(&mut self) {
        self.store.answered();
    }

    /// How many rows the retry policy is tracking.
    pub fn tracked(&self) -> Tracked {
        self.retries.tracked()
    }

    /// The key of row `id`. A row the batch did not hold is a key of its own.
    fn key(&self, id: &str) -> RowKey {
        self.batch
            .keys
            .get(id)
            .cloned()
            .unwrap_or_else(|| (None, id.to_owned()))
    }

    /// A fatal failure of row `id`: publish nothing more, and raise its failure.
    fn stop(&mut self, id: &str) {
        self.batch.stopped = true;
        self.batch.fatal = true;
        self.batch.drained.raise = Some(id.to_owned());
    }

    fn hand_back(&mut self, id: &str, step: u64) {
        let key = self.key(id);
        self.batch.blocked.insert(key);
        let drained = &mut self.batch.drained;
        drained.wait = Some(drained.wait.map_or(step, |wait| wait.max(step)));
        drained.raise.get_or_insert_with(|| id.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::base_event;
    use crate::EventMessage;

    fn row(id: &str, aggregate: &str, tenant: Option<&str>) -> EventMessage {
        EventMessage {
            id: id.to_owned(),
            aggregate_id: aggregate.to_owned(),
            tenant: tenant.map(ToOwned::to_owned),
            ..base_event()
        }
    }

    #[test]
    fn a_failed_row_blocks_its_own_key_only() {
        let mut drain = Drain::new(0, false);
        drain.begin(&[
            row("a-1", "acct-A", None),
            row("b-1", "acct-B", None),
            row("a-2", "acct-A", None),
            row("a-3", "acct-A", Some("acme")),
        ]);
        assert_eq!(drain.failed("a-1", Failure::Terminal), RowFailed::HandBack);
        assert!(drain.admits("b-1"), "another key goes on");
        drain.published("b-1");
        assert!(!drain.admits("a-2"), "the same key is blocked");
        assert!(
            drain.admits("a-3"),
            "the same aggregate of another tenant is another key"
        );
        let drained = drain.finish();
        assert_eq!(drained.settled, ["b-1"]);
        assert_eq!((drained.wait, drained.raise.as_deref()), (Some(0), Some("a-1")));
    }

    #[test]
    fn two_transient_failures_in_a_row_stop_the_drain() {
        let mut drain = Drain::new(0, false);
        drain.begin(&[
            row("a-1", "acct-A", None),
            row("b-1", "acct-B", None),
            row("c-1", "acct-C", None),
        ]);
        assert_eq!(drain.failed("a-1", Failure::Transient), RowFailed::HandBack);
        assert!(drain.admits("b-1"));
        assert_eq!(drain.failed("b-1", Failure::Transient), RowFailed::HandBack);
        assert!(!drain.admits("c-1"), "the outage stops the drain");
        assert_eq!(drain.finish().raise.as_deref(), Some("a-1"));
        drain.begin(&[row("c-1", "acct-C", None)]);
        assert!(drain.admits("c-1"), "the next drain starts over");
    }

    #[test]
    fn an_outage_whose_rows_the_sink_took_still_waits_and_raises() {
        let mut drain = Drain::new(1, true);
        drain.begin(&[
            row("a-1", "acct-A", None),
            row("b-1", "acct-B", None),
            row("c-1", "acct-C", None),
        ]);
        for id in ["a-1", "b-1"] {
            assert_eq!(
                drain.failed(id, Failure::Transient),
                RowFailed::DeadLetter { attempt: 1 }
            );
            assert_eq!(drain.sunk(id, 1, None), None, "the sink took {id}");
        }
        assert!(!drain.admits("c-1"), "the outage stops the drain");
        let drained = drain.finish();
        assert_eq!(drained.settled, ["a-1", "b-1"]);
        assert_eq!(
            (drained.wait, drained.raise.as_deref()),
            (Some(0), Some("b-1")),
            "the outage is reported and paced"
        );
    }

    #[test]
    fn a_fatal_failure_stops_at_once_and_waits_nothing() {
        let mut drain = Drain::new(0, true);
        drain.begin(&[
            row("a-1", "acct-A", None),
            row("b-1", "acct-B", None),
            row("c-1", "acct-C", None),
        ]);
        assert_eq!(drain.failed("a-1", Failure::Contended), RowFailed::HandBack);
        assert_eq!(drain.failed("b-1", Failure::Fatal), RowFailed::Stop);
        assert!(!drain.admits("c-1"));
        let drained = drain.finish();
        assert_eq!((drained.wait, drained.raise.as_deref()), (None, Some("b-1")));
    }
}
