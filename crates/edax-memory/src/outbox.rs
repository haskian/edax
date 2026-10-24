//! The command outbox: commands a unit of work decided, committed with its events and sent by a
//! relay after the commit. It is the optional third route for a command, beside dispatching it in
//! process and publishing it directly. A direct publish is a dual write: the state change and the
//! send are not atomic. A command enqueued here is sent only if its transaction commits, and sent
//! again after a crash, so its consumer must be idempotent.

use std::sync::{Arc, Mutex};

use edax_core::{lock, Error};
use edax_event::{CommandMessage, Message};

use crate::tx::{store_view, MemoryState, MemoryTransactor, Tx};

/// The command outbox view: enqueue under a transaction, read and prune outside one.
pub struct MemoryCommandOutbox {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemoryCommandOutbox, command_outbox);

impl MemoryCommandOutbox {
    /// Stage `commands` under `tx`, to commit with every other write the transaction makes. An id
    /// already waiting in the outbox, staged by any open transaction, or repeated in the batch, is a
    /// `codec` error: the relay marks rows sent by id, so two rows under one id could not be told
    /// apart, and marking one would prune both. So is a command [`Message::check`] refuses. A refused batch
    /// stages nothing.
    pub fn enqueue(&self, tx: Tx, commands: Vec<CommandMessage>) -> Result<(), Error> {
        commands.iter().try_for_each(Message::check)?;
        lock(&self.state).write(tx, |committed, others, own| {
            let staged_by_others = others.values().flat_map(|set| &set.command_outbox);
            committed
                .command_outbox
                .stage(staged_by_others, &mut own.command_outbox, commands, "command")
        })
    }

    /// Committed commands not yet sent, oldest commit first, capped at `limit`. The relay reads
    /// outside any transaction, so there is no token: a staged command is not one to send.
    pub fn unpublished(&self, limit: usize) -> Vec<CommandMessage> {
        lock(&self.state).committed.command_outbox.page(limit)
    }

    /// How many committed commands wait, without paging them.
    pub fn unpublished_count(&self) -> usize {
        lock(&self.state).committed.command_outbox.count()
    }

    /// Mark commands sent by id, pruning them. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, ids: &[String]) {
        lock(&self.state).committed.command_outbox.prune(ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{command, draft, ids, wire, RECORDED_AT};
    use crate::tx::Tx;

    #[test]
    fn enqueued_commands_commit_and_roll_back_with_the_events() {
        let tr = MemoryTransactor::new();
        let (events, outbox) = (tr.event_store(), tr.command_outbox());

        let tx = tr.begin();
        events
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        outbox.enqueue(tx, vec![command("cmd-1", Some("invoice-7"))]).unwrap();
        assert!(outbox.unpublished(10).is_empty(), "a staged command is not one to send");
        assert!(tr.rollback(tx));
        assert!(
            outbox.unpublished(10).is_empty(),
            "a rolled-back command never reaches the relay"
        );

        let tx = tr.begin();
        events
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        outbox
            .enqueue(tx, vec![command("cmd-1", Some("invoice-7")), command("cmd-2", None)])
            .unwrap();
        tr.commit(tx).unwrap();
        assert_eq!(ids(&outbox.unpublished(10)), ["cmd-1", "cmd-2"]);
        assert_eq!(outbox.unpublished_count(), 2);
        assert_eq!(ids(&outbox.unpublished(1)), ["cmd-1"], "the limit caps the page");

        outbox.mark_published(&["cmd-1".into(), "unknown".into()]);
        assert_eq!(ids(&outbox.unpublished(10)), ["cmd-2"]);
    }

    #[test]
    fn enqueue_refusals() {
        let outbox = MemoryCommandOutbox::new();
        outbox.enqueue(Tx::auto_commit(), vec![command("cmd-1", None)]).unwrap();
        let cases = [
            (
                "an_id_waiting_in_the_outbox",
                vec![command("cmd-1", None)],
                "codec: command cmd-1 is already in the outbox",
            ),
            (
                "an_id_repeated_in_the_batch",
                vec![command("cmd-2", None), command("cmd-2", None)],
                "codec: command cmd-2 is already in the outbox",
            ),
            (
                "a_nul_in_a_command_is_refused",
                vec![command("cmd-2", None), command("cmd-3\0", None)],
                "codec: command id holds a NUL character",
            ),
        ];
        for (name, batch, want) in cases {
            assert_eq!(
                wire(outbox.enqueue(Tx::auto_commit(), batch)),
                Err(want.to_owned()),
                "{name}"
            );
        }
        assert_eq!(
            ids(&outbox.unpublished(10)),
            ["cmd-1"],
            "a refused batch stages nothing"
        );
    }

    #[test]
    fn an_id_staged_by_an_open_transaction_is_refused() {
        let tr = MemoryTransactor::new();
        let outbox = tr.command_outbox();
        let (mine, theirs) = (tr.begin(), tr.begin());
        outbox.enqueue(mine, vec![command("cmd-1", None)]).unwrap();
        let want = Err("codec: command cmd-1 is already in the outbox".to_owned());
        assert_eq!(
            wire(outbox.enqueue(mine, vec![command("cmd-1", None)])),
            want,
            "staged earlier in the same transaction"
        );
        assert_eq!(
            wire(outbox.enqueue(theirs, vec![command("cmd-1", None)])),
            want,
            "staged by another open transaction"
        );
        tr.commit(mine).unwrap();
        tr.commit(theirs).unwrap();
        assert_eq!(
            ids(&outbox.unpublished(10)),
            ["cmd-1"],
            "one row, so marking it sent prunes only it"
        );
    }
}
