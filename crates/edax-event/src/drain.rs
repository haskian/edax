//! The outbox relay's drain: which rows of a batch it publishes, what a failed row does to the rows
//! behind it, and how long the drain waits before it returns.
//!
//! Two SDKs relaying one outbox must publish, skip and dead-letter the same rows, so the rule is here,
//! sans-IO, beside [`Retries`], which it keeps per row id. The host reads the batch, publishes each row
//! [`Drain::ready`] hands it, reports each publish and each dead-letter, marks the settled rows and waits;
//! the drain only says which. Pinned by `testdata/golden/relay_drain.json`.
//!
//! The rule:
//!
//! - order is promised per key only: a row's key is its tenant and its
//!   [`ordering_key`](crate::Message::ordering_key), so a failed row blocks the rows behind it of its
//!   own key for the rest of the drain, and every other key goes on;
//! - up to [`in_flight`](Drain::in_flight) rows publish at once, never two of one key: a key's next row
//!   waits until its row in flight is published, handed back or dead-lettered. A row in flight when the
//!   drain stops is still reported: settled if it was published, else handed back uncounted, as a row the
//!   serial drain would never have sent;
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

use edax_core::Error;

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
    /// The batch's row ids, in the order the store returned them.
    order: Vec<String>,
    /// Every row before this index is sent or blocked for good, so [`Drain::ready`] starts here.
    cursor: usize,
    /// The rows [`Drain::ready`] handed out or the host reported.
    sent: HashSet<String>,
    /// The keys with a row in flight: handed out and not yet published, handed back or dead-lettered.
    in_flight: HashSet<RowKey>,
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
    /// Keyed by row id with no tenant: a row id is unique within the one outbox a relay drains.
    retries: Retries,
    store: Streak,
    /// How many rows publish at once.
    in_flight: usize,
    batch: Batch,
}

impl Drain {
    /// A drain whose rows die as [`Retries::new`] says: a transient failure at its `max_attempts`-th
    /// attempt (`0`: never), a terminal one at once, and only when the relay has a `sink`.
    pub fn new(max_attempts: u32, sink: bool) -> Self {
        Self {
            retries: Retries::new(max_attempts, sink),
            in_flight: 1,
            ..Self::default()
        }
    }

    /// The drain with up to `rows` rows publishing at once, never two of one key. `1`, the default,
    /// publishes the batch one row at a time; `0` is refused, since it would publish nothing.
    pub fn in_flight(mut self, rows: u32) -> Result<Self, Error> {
        if rows == 0 {
            return Err(Error::bad_handle("a relay publishes at least one row at a time"));
        }
        self.in_flight = usize::try_from(rows).unwrap_or(usize::MAX);
        Ok(self)
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
        let mut keys = HashMap::new();
        let mut order = Vec::new();
        for (id, tenant, ordering_key) in rows {
            keys.insert(id.to_owned(), (tenant.map(ToOwned::to_owned), ordering_key.to_owned()));
            order.push(id.to_owned());
        }
        self.batch = Batch {
            keys,
            order,
            ..Batch::default()
        };
    }

    /// The rows to publish now, in batch order: the first unsent row of each key with no row in flight
    /// and not blocked, as many as fit beside the rows already in flight. Empty once the drain stopped,
    /// or while every admitted key waits on its row in flight. Each row it names is in flight until the
    /// host reports it.
    pub fn ready(&mut self) -> Vec<String> {
        let batch = &mut self.batch;
        let mut ids = Vec::new();
        if batch.stopped {
            return ids;
        }
        let room = self.in_flight.saturating_sub(batch.in_flight.len());
        while batch.order.get(batch.cursor).is_some_and(|id| {
            batch.sent.contains(id) || batch.keys.get(id).is_some_and(|key| batch.blocked.contains(key))
        }) {
            batch.cursor = batch.cursor.saturating_add(1);
        }
        for id in batch.order.iter().skip(batch.cursor) {
            if ids.len() >= room {
                break;
            }
            let Some(key) = batch.keys.get(id) else {
                continue;
            };
            if batch.sent.contains(id) || batch.blocked.contains(key) || batch.in_flight.contains(key) {
                continue;
            }
            batch.sent.insert(id.clone());
            batch.in_flight.insert(key.clone());
            ids.push(id.clone());
        }
        ids
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
        self.landed(id);
        self.retries.succeeded(id, None);
        self.batch.transient.answered();
        self.batch.drained.published = self.batch.drained.published.saturating_add(1);
        self.batch.drained.settled.push(id.to_owned());
    }

    /// The row `id` failed to publish with `failure`.
    pub fn failed(&mut self, id: &str, failure: Failure) -> RowFailed {
        if self.batch.stopped {
            // Sent before the drain stopped: the serial drain would never have sent it, so it spends no
            // attempt and never reaches the sink.
            self.landed(id);
            self.hand_back(id, 0);
            return RowFailed::HandBack;
        }
        let verdict = self.retries.failed(id, None, 0, failure);
        if !matches!(verdict, Verdict::Dead { .. }) {
            // A dead row keeps its key in flight until the sink answers for it.
            self.landed(id);
        }
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
        self.landed(id);
        let step = self.retries.sunk(id, None, attempt, refusal);
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

    /// The row `id` is no longer in flight, and never handed out again.
    fn landed(&mut self, id: &str) {
        let key = self.key(id);
        self.batch.in_flight.remove(&key);
        self.batch.sent.insert(id.to_owned());
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
    fn ready_hands_out_one_row_per_key_up_to_the_window() {
        let mut drain = Drain::new(0, false).in_flight(3).expect("a positive window");
        drain.begin(&[
            row("a-1", "acct-A", None),
            row("a-2", "acct-A", None),
            row("b-1", "acct-B", None),
            row("c-1", "acct-C", None),
            row("d-1", "acct-D", None),
        ]);
        assert_eq!(
            drain.ready(),
            ["a-1", "b-1", "c-1"],
            "a-2 waits behind a-1, d-1 for room"
        );
        assert!(drain.ready().is_empty(), "the window is full");
        drain.published("b-1");
        assert_eq!(drain.ready(), ["d-1"], "a-2 still waits behind a-1");
        drain.published("a-1");
        assert_eq!(drain.ready(), ["a-2"]);
        drain.published("a-2");
        drain.published("c-1");
        drain.published("d-1");
        assert!(drain.ready().is_empty());
        assert_eq!(drain.finish().settled, ["b-1", "a-1", "a-2", "c-1", "d-1"]);
    }

    #[test]
    fn a_dead_row_holds_its_key_until_the_sink_answers() {
        let mut drain = Drain::new(0, true).in_flight(2).expect("a positive window");
        drain.begin(&[row("a-1", "acct-A", None), row("a-2", "acct-A", None)]);
        assert_eq!(drain.ready(), ["a-1"]);
        assert_eq!(
            drain.failed("a-1", Failure::Terminal),
            RowFailed::DeadLetter { attempt: 1 }
        );
        assert!(drain.ready().is_empty(), "a-2 waits for the sink");
        assert_eq!(drain.sunk("a-1", 1, None), None);
        assert_eq!(drain.ready(), ["a-2"]);
    }

    #[test]
    fn a_row_in_flight_when_the_drain_stops_is_still_settled() {
        let mut drain = Drain::new(0, false).in_flight(2).expect("a positive window");
        drain.begin(&[
            row("a-1", "acct-A", None),
            row("b-1", "acct-B", None),
            row("c-1", "acct-C", None),
        ]);
        assert_eq!(drain.ready(), ["a-1", "b-1"]);
        assert_eq!(drain.failed("a-1", Failure::Fatal), RowFailed::Stop);
        assert!(drain.ready().is_empty(), "the drain stopped");
        drain.published("b-1");
        let drained = drain.finish();
        assert_eq!(
            (drained.settled, drained.raise.as_deref()),
            (vec!["b-1".to_owned()], Some("a-1"))
        );
    }

    #[test]
    fn a_row_failing_after_a_fatal_stop_is_handed_back_uncounted() {
        let mut drain = Drain::new(0, true).in_flight(2).expect("a positive window");
        drain.begin(&[row("a-1", "acct-A", None), row("b-1", "acct-B", None)]);
        assert_eq!(drain.ready(), ["a-1", "b-1"]);
        assert_eq!(drain.failed("a-1", Failure::Fatal), RowFailed::Stop);
        assert_eq!(
            drain.failed("b-1", Failure::Terminal),
            RowFailed::HandBack,
            "never to the sink"
        );
        assert_eq!(drain.tracked().attempts, 0);
        let drained = drain.finish();
        assert_eq!((drained.wait, drained.raise.as_deref()), (None, Some("a-1")));
    }

    #[test]
    fn a_window_of_zero_is_refused() {
        assert!(Drain::new(0, false).in_flight(0).is_err());
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
