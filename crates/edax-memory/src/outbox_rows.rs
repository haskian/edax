//! The committed rows of one outbox: the event outbox, each saga's outbox and the command outbox are
//! three instances of this one table, so their paging, counting, pruning and id rule cannot drift.

use std::collections::HashSet;

use edax_core::Error;
use edax_event::Message;

/// One outbox's committed, unpublished rows, oldest commit first. Published rows are pruned, so the
/// table holds only what still waits and a page is a bounded prefix scan.
pub(crate) struct OutboxRows<R> {
    rows: Vec<R>,
    /// The ids of `rows`, kept beside them so a stage checks the backlog without scanning it.
    ids: HashSet<String>,
}

// By hand: a derive would demand `R: Default`, which an empty table does not need.
impl<R> Default for OutboxRows<R> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            ids: HashSet::new(),
        }
    }
}

impl<R: Message + Clone> OutboxRows<R> {
    /// The first `limit` committed rows, oldest commit first.
    pub(crate) fn page(&self, limit: usize) -> Vec<R> {
        self.rows.iter().take(limit).cloned().collect()
    }

    /// How many committed rows wait: the backlog depth, without paging.
    pub(crate) fn count(&self) -> usize {
        self.rows.len()
    }

    /// Drop the rows marked sent. Idempotent; an unknown id is a no-op.
    pub(crate) fn prune(&mut self, ids: &[String]) {
        let sent: HashSet<&str> = ids.iter().map(String::as_str).collect();
        self.rows.retain(|row| !sent.contains(row.id()));
        for id in sent {
            self.ids.remove(id);
        }
    }

    /// Commit a transaction's staged rows behind the committed ones, in the order it staged them.
    pub(crate) fn apply(&mut self, staged: Vec<R>) {
        self.ids.extend(staged.iter().map(|row| row.id().to_owned()));
        self.rows.extend(staged);
    }

    /// Stage `batch` into `own`, a transaction's staged rows for this outbox. A row whose id already
    /// waits (committed here, staged by `own` or by another open transaction) or repeats within the
    /// batch is refused, and nothing is staged: a relay marks rows sent by id, so two rows under one id
    /// could not be told apart, and marking one would prune both. Deterministic, so `codec`, naming the
    /// row as `what`.
    pub(crate) fn stage<'a>(
        &'a self,
        staged_by_others: impl Iterator<Item = &'a R>,
        own: &mut Vec<R>,
        batch: Vec<R>,
        what: &str,
    ) -> Result<(), Error> {
        let mut staged: HashSet<&str> = staged_by_others.map(Message::id).collect();
        staged.extend(own.iter().map(Message::id));
        if let Some(repeat) = batch
            .iter()
            .find(|row| self.ids.contains(row.id()) || !staged.insert(row.id()))
        {
            return Err(Error::codec(format_args!(
                "{what} {} is already in the outbox",
                repeat.id()
            )));
        }
        own.extend(batch);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{command, ids, wire};
    use edax_event::CommandMessage;

    fn table(ids: &[&str]) -> OutboxRows<CommandMessage> {
        let mut rows = OutboxRows::default();
        rows.apply(ids.iter().map(|id| command(id, Some("invoice-7"))).collect());
        rows
    }

    #[test]
    fn page_count_and_prune() {
        let mut rows = table(&["cmd-1", "cmd-2", "cmd-3"]);
        for (name, limit, want) in [
            ("zero_limit_reads_nothing", 0, vec![]),
            ("limit_below_size_reads_a_prefix", 2, vec!["cmd-1", "cmd-2"]),
            ("limit_above_size_reads_everything", 10, vec!["cmd-1", "cmd-2", "cmd-3"]),
        ] {
            assert_eq!(ids(&rows.page(limit)), want, "{name}");
        }
        assert_eq!(rows.count(), 3);
        rows.prune(&["cmd-2".to_owned(), "never-existed".to_owned()]);
        rows.prune(&["cmd-2".to_owned()]);
        assert_eq!(
            ids(&rows.page(10)),
            ["cmd-1", "cmd-3"],
            "idempotent, unknown ids ignored"
        );
        assert_eq!(rows.count(), 2);
        rows.prune(&[]);
        assert_eq!(rows.count(), 2, "an empty mark prunes nothing");
    }

    #[test]
    fn stage_branches() {
        struct Case {
            name: &'static str,
            staged_by_other: &'static [&'static str],
            own: &'static [&'static str],
            batch: &'static [&'static str],
            want: Result<(), &'static str>,
            want_own: &'static [&'static str],
        }
        let base = Case {
            name: "",
            staged_by_other: &[],
            own: &[],
            batch: &["cmd-2"],
            want: Ok(()),
            want_own: &["cmd-2"],
        };
        let cases = [
            Case {
                name: "a_new_id_is_staged",
                ..base
            },
            Case {
                name: "an_empty_batch_stages_nothing",
                batch: &[],
                want_own: &[],
                ..base
            },
            Case {
                name: "an_id_already_committed_is_refused",
                batch: &["cmd-1"],
                want: Err("codec: command cmd-1 is already in the outbox"),
                want_own: &[],
                ..base
            },
            Case {
                name: "an_id_another_transaction_staged_is_refused",
                staged_by_other: &["cmd-2"],
                want: Err("codec: command cmd-2 is already in the outbox"),
                want_own: &[],
                ..base
            },
            Case {
                name: "an_id_this_transaction_staged_is_refused",
                own: &["cmd-2"],
                want: Err("codec: command cmd-2 is already in the outbox"),
                want_own: &["cmd-2"],
                ..base
            },
            Case {
                name: "an_id_repeated_in_the_batch_is_refused_and_nothing_is_staged",
                batch: &["cmd-3", "cmd-3"],
                want: Err("codec: command cmd-3 is already in the outbox"),
                want_own: &[],
                ..base
            },
        ];
        for c in cases {
            let rows = table(&["cmd-1"]);
            let other: Vec<CommandMessage> = c.staged_by_other.iter().map(|id| command(id, None)).collect();
            let mut own: Vec<CommandMessage> = c.own.iter().map(|id| command(id, None)).collect();
            let batch = c.batch.iter().map(|id| command(id, None)).collect();
            let got = rows.stage(other.iter(), &mut own, batch, "command");
            assert_eq!(wire(got), c.want.map_err(str::to_owned), "{}", c.name);
            assert_eq!(ids(&own), c.want_own, "{}: staged", c.name);
            assert_eq!(rows.count(), 1, "{}: nothing committed by a stage", c.name);
        }
    }
}
