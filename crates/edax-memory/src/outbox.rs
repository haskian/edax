//! The command outbox: commands a unit of work decided, committed with its events and sent by a
//! relay after the commit. It is the optional third route for a command, beside dispatching it in
//! process and publishing it directly. A direct publish is a dual write: the state change and the
//! send are not atomic. A command enqueued here is sent only if its transaction commits, and sent
//! again after a crash, so its consumer must be idempotent.

use std::collections::HashSet;
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
    ///
    /// `after_event` is the last event the same save appended, if any: each command waits behind it, so
    /// the relay never sends a command before the event it follows from.
    pub fn enqueue(&self, tx: Tx, commands: Vec<CommandMessage>, after_event: Option<&str>) -> Result<(), Error> {
        commands.iter().try_for_each(Message::check)?;
        if let Some(event) = after_event {
            edax_core::check_text("after event", event)?;
        }
        lock(&self.state).write(tx, |committed, others, own| {
            let waits: Vec<(String, String)> = after_event
                .map(|event| commands.iter().map(|c| (c.id.clone(), event.to_owned())).collect())
                .unwrap_or_default();
            let staged_by_others = others.values().flat_map(|set| &set.command_outbox);
            committed
                .command_outbox
                .stage(staged_by_others, &mut own.command_outbox, commands, "command")?;
            // Only after the stage succeeded: a refused batch stages nothing, its waits included.
            own.command_after.extend(waits);
            Ok(())
        })
    }

    /// Committed commands not yet sent, oldest commit first, capped at `limit`. The relay reads
    /// outside any transaction, so there is no token: a staged command is not one to send. A command
    /// whose save's last event still waits in the event outbox is held back until that event is
    /// published, and so is every later command for the same aggregate and tenant, so the aggregate's
    /// commands still leave in order.
    pub fn unpublished(&self, limit: usize) -> Vec<CommandMessage> {
        let state = lock(&self.state);
        let committed = &state.committed;
        let mut blocked: HashSet<(&str, Option<&str>)> = HashSet::new();
        committed.command_outbox.page_where(limit, |command| {
            let held = committed
                .command_after
                .get(&command.id)
                .is_some_and(|event| committed.event_outbox.contains(event));
            // A command with no aggregate is ordered by its own id, so it holds back nothing else.
            let Some(aggregate) = command.aggregate_id.as_deref() else {
                return !held;
            };
            let key = (aggregate, command.tenant.as_deref());
            if held {
                blocked.insert(key);
                return false;
            }
            !blocked.contains(&key)
        })
    }

    /// How many committed commands wait, without paging them.
    pub fn unpublished_count(&self) -> usize {
        lock(&self.state).committed.command_outbox.count()
    }

    /// Mark commands sent by id, pruning them. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, ids: &[String]) {
        let mut state = lock(&self.state);
        state.committed.command_outbox.prune(ids);
        for id in ids {
            state.committed.command_after.remove(id);
        }
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
        outbox
            .enqueue(tx, vec![command("cmd-1", Some("invoice-7"))], None)
            .unwrap();
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
            .enqueue(
                tx,
                vec![command("cmd-1", Some("invoice-7")), command("cmd-2", None)],
                None,
            )
            .unwrap();
        tr.commit(tx).unwrap();
        assert_eq!(ids(&outbox.unpublished(10)), ["cmd-1", "cmd-2"]);
        assert_eq!(outbox.unpublished_count(), 2);
        assert_eq!(ids(&outbox.unpublished(1)), ["cmd-1"], "the limit caps the page");

        outbox.mark_published(&["cmd-1".into(), "unknown".into()]);
        assert_eq!(ids(&outbox.unpublished(10)), ["cmd-2"]);
    }

    #[test]
    fn a_command_waits_behind_its_saves_last_event() {
        let tr = MemoryTransactor::new();
        let (events, outbox) = (tr.event_store(), tr.command_outbox());
        let tx = tr.begin();
        events
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        outbox
            .enqueue(tx, vec![command("cmd-1", Some("order-1"))], Some("evt-1"))
            .unwrap();
        outbox.enqueue(tx, vec![command("cmd-2", None)], None).unwrap();
        tr.commit(tx).unwrap();
        assert_eq!(
            ids(&outbox.unpublished(10)),
            ["cmd-2"],
            "a command whose event still waits is held; one with no event is not"
        );
        assert_eq!(outbox.unpublished_count(), 2, "a held command still counts as waiting");
        events.mark_published(&["evt-1".into()]);
        assert_eq!(
            ids(&outbox.unpublished(10)),
            ["cmd-1", "cmd-2"],
            "the event went, so the command goes"
        );
        outbox.mark_published(&["cmd-1".into(), "cmd-2".into()]);
        assert!(outbox.unpublished(10).is_empty());
    }

    #[test]
    fn a_held_command_holds_back_its_aggregates_later_commands() {
        let tr = MemoryTransactor::new();
        let (events, outbox) = (tr.event_store(), tr.command_outbox());
        let tx = tr.begin();
        events
            .append(tx, "invoice-7", 0, vec![draft("evt-1", Some(1))], RECORDED_AT)
            .unwrap();
        outbox
            .enqueue(tx, vec![command("cmd-a", Some("order-1"))], Some("evt-1"))
            .unwrap();
        tr.commit(tx).unwrap();
        // A later save with no event: nothing holds it, but its aggregate's earlier command is held.
        outbox
            .enqueue(Tx::auto_commit(), vec![command("cmd-b", Some("order-1"))], None)
            .unwrap();
        let other_tenant = CommandMessage {
            tenant: Some("globex".into()),
            ..command("cmd-c", Some("order-1"))
        };
        outbox
            .enqueue(
                Tx::auto_commit(),
                vec![other_tenant, command("cmd-d", Some("order-2")), command("cmd-e", None)],
                None,
            )
            .unwrap();
        assert_eq!(
            ids(&outbox.unpublished(10)),
            ["cmd-c", "cmd-d", "cmd-e"],
            "order-1's later command waits; another tenant's, another aggregate's and an unkeyed one do not"
        );
        events.mark_published(&["evt-1".into()]);
        assert_eq!(
            ids(&outbox.unpublished(10)),
            ["cmd-a", "cmd-b", "cmd-c", "cmd-d", "cmd-e"],
            "released in order"
        );
    }

    #[test]
    fn enqueue_refusals() {
        let outbox = MemoryCommandOutbox::new();
        outbox
            .enqueue(Tx::auto_commit(), vec![command("cmd-1", None)], None)
            .unwrap();
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
                wire(outbox.enqueue(Tx::auto_commit(), batch, None)),
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
        outbox.enqueue(mine, vec![command("cmd-1", None)], None).unwrap();
        let want = Err("codec: command cmd-1 is already in the outbox".to_owned());
        assert_eq!(
            wire(outbox.enqueue(mine, vec![command("cmd-1", None)], None)),
            want,
            "staged earlier in the same transaction"
        );
        assert_eq!(
            wire(outbox.enqueue(theirs, vec![command("cmd-1", None)], None)),
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
