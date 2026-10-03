//! The saga store view: per-correlation state with optimistic concurrency, a tombstone on
//! completion, and the saga outbox the step's commands commit to.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error, ErrorCode};
use edax_event::{saga_command_id, SagaCommand};
use serde_json::Value;

use crate::projection::Versioned;
use crate::tx::{held_by_other, store_view, Committed, MemoryState, MemoryTransactor, WriteSet};

/// A stored saga instance: running with its state, or completed. A completed instance is a
/// tombstone that keeps its version, so a stale writer still conflicts and a late event finds it
/// completed instead of starting the workflow again.
#[derive(Debug, Clone, PartialEq)]
pub enum SagaRow {
    /// A running instance.
    Active {
        /// The state the saga folded.
        state: Value,
        /// The kernel's marks of the events folded so far (`edax_event::saga_admit`): opaque text the
        /// store keeps as it was written.
        folded: String,
    },
    /// The tombstone of a completed instance.
    Completed,
}

/// In-memory saga instance store for development and tests: per-correlation state with optimistic
/// concurrency on the version, and the saga outbox. State is any JSON
/// value (a saga that keeps `null` is legitimate). The rule is written once here, so every SDK's
/// saga runner is held to the same one:
///
/// - a `save` or `complete` at a stale version is a conflict, and so is any write to a completed
///   instance: completion is final;
/// - `complete` writes a tombstone at the next version instead of removing the row, so the version
///   never goes back to 0 and a stale writer that loaded the old instance cannot re-create it (ABA);
/// - the commands a step decided are staged with its state in the same write, so they commit or
///   roll back together, and each must carry the kernel's [`saga_command_id`] for that write, or the
///   write is a `codec` error and stages nothing;
/// - [`unpublished`](Self::unpublished) shows committed commands only, oldest commit first, until
///   [`mark_published`](Self::mark_published) prunes them.
///
/// A write under an open token is staged until the transactor commits. Not for production: a
/// durable store enlists the write in the events' transaction and purges tombstones by retention.
#[derive(Clone)]
pub struct MemorySagaStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemorySagaStore, saga_store);

/// Refuse a write to an instance another open transaction has written: the holder's commit would
/// overwrite it. The conflict names the committed version, since the holder's own may yet roll back.
fn refuse_held_saga(committed: &Committed, others: &HashMap<i64, WriteSet>, id: &str, expected: u64) -> Result<(), Error> {
    match held_by_other(others, |set| set.sagas.get(id)) {
        Some(_) => Err(Error::concurrency(expected, committed.sagas.get(id).map_or(0, |(v, _)| *v))),
        None => Ok(()),
    }
}

/// The saga row as `tx` sees it: staged in the transaction, else committed.
fn saga_row<'a>(committed: &'a Committed, own: Option<&'a WriteSet>, id: &str) -> Option<&'a (u64, SagaRow)> {
    own.and_then(|set| set.sagas.get(id)).or_else(|| committed.sagas.get(id))
}

/// Refuse a command batch that does not belong to the write at `next`: every id must be the kernel's
/// for its index, and no id may already wait in the outbox. Both are deterministic, so `codec`.
fn check_commands(committed: &Committed, own: &WriteSet, id: &str, version: u64, commands: &[SagaCommand]) -> Result<(), Error> {
    let mut waiting: HashSet<&str> = committed.saga_commands.iter().chain(&own.saga_commands).map(|c| c.id.as_str()).collect();
    for (index, command) in (0u64..).zip(commands) {
        let want = saga_command_id(id, version, index);
        if command.id != want || command.correlation_id != id {
            return Err(Error::codec(format_args!(
                "saga command {index} of {id} at version {version} is {:?} for {:?}, expected {want:?}",
                command.id, command.correlation_id
            )));
        }
        if !waiting.insert(command.id.as_str()) {
            return Err(Error::codec(format_args!("saga command {} is already in the outbox", command.id)));
        }
    }
    Ok(())
}

impl MemorySagaStore {
    /// The stored instance for `id` and its version as `tx` sees it — running with its state, or the
    /// tombstone of a completed one — or `None` if there is none.
    pub fn load(&self, tx: i64, id: &str) -> Result<Option<Versioned<SagaRow>>, Error> {
        lock(&self.state).read(tx, |committed, own| {
            saga_row(committed, own, id).map(|(version, row)| Versioned { version: *version, value: row.clone() })
        })
    }

    /// Persist `state`, the `folded` marks and the step's `commands` at `version + 1` if the version
    /// `tx` sees still equals `version` (an absent instance is at version 0); returns the new version.
    /// A version mismatch, a completed instance, or an id another open transaction has written and not
    /// yet committed is a conflict error; a command that does not belong to this write is a codec error.
    /// A refused write stages nothing.
    pub fn save(&self, tx: i64, id: &str, version: u64, state: Value, folded: String, commands: Vec<SagaCommand>) -> Result<u64, Error> {
        self.write(tx, id, version, SagaRow::Active { state, folded }, commands)
    }

    /// Complete the instance: write its tombstone and the step's `commands` at `version + 1` under
    /// the same rules as [`save`](Self::save); returns the tombstone's version.
    pub fn complete(&self, tx: i64, id: &str, version: u64, commands: Vec<SagaCommand>) -> Result<u64, Error> {
        self.write(tx, id, version, SagaRow::Completed, commands)
    }

    fn write(&self, tx: i64, id: &str, version: u64, row: SagaRow, commands: Vec<SagaCommand>) -> Result<u64, Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            refuse_held_saga(committed, others, id, version)?;
            let stored = match saga_row(committed, Some(own), id) {
                Some((stored, SagaRow::Completed)) => {
                    return Err(Error::new(
                        ErrorCode::Concurrency,
                        format_args!("saga {id} completed at version {stored}: a completed instance takes no write"),
                    ));
                }
                Some((stored, SagaRow::Active { .. })) => *stored,
                None => 0,
            };
            if stored != version {
                return Err(Error::concurrency(version, stored));
            }
            let next = version.checked_add(1).ok_or_else(|| Error::codec(format_args!("saga {id} is at the last version")))?;
            check_commands(committed, own, id, next, &commands)?;
            own.sagas.insert(id.to_owned(), (next, row));
            own.saga_commands.extend(commands);
            Ok(next)
        })
    }

    /// Committed outbox commands not yet sent, oldest commit first, capped at `limit`. The relay
    /// reads outside any transaction, so there is no token: a staged command is not one to send.
    pub fn unpublished(&self, limit: usize) -> Vec<SagaCommand> {
        lock(&self.state).committed.saga_commands.iter().take(limit).cloned().collect()
    }

    /// Mark outbox commands sent by id, pruning them. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, ids: Vec<String>) {
        let set: HashSet<String> = ids.into_iter().collect();
        lock(&self.state).committed.saga_commands.retain(|c| !set.contains(&c.id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::wire;
    use crate::tx::NO_TX;
    use edax_event::Event;
    use serde_json::json;

    fn open() -> Value {
        json!({"status": "awaiting_payment", "total": 1200})
    }

    /// The `index`-th command a write of `id` at `version` sends: a real `Fulfill` payload.
    fn command(id: &str, version: u64, index: u64) -> SagaCommand {
        SagaCommand {
            id: saga_command_id(id, version, index),
            correlation_id: id.to_owned(),
            command: Event { name: "Fulfill".to_owned(), version: 1, data: format!(r#"{{"order_id":"{id}"}}"#).into_bytes() },
            timestamp: 1_714_564_800_000_000_000,
        }
    }

    /// A saga store holding one open instance at version 1.
    fn seeded() -> MemorySagaStore {
        let store = MemorySagaStore::new();
        store.save(NO_TX, "order-1", 0, open(), String::new(), vec![]).expect("seed saves");
        store
    }

    /// The stored row as `(version, status or "completed")`, or `None`.
    fn row(store: &MemorySagaStore, id: &str) -> Option<(u64, String)> {
        store.load(NO_TX, id).unwrap().map(|stored| {
            let status = match stored.value {
                SagaRow::Completed => "completed".to_owned(),
                SagaRow::Active { state, .. } => state["status"].as_str().unwrap().to_owned(),
            };
            (stored.version, status)
        })
    }

    fn unsent_ids(store: &MemorySagaStore) -> Vec<String> {
        store.unpublished(100).into_iter().map(|c| c.id).collect()
    }

    #[derive(Clone, Copy)]
    enum Write {
        Save(&'static str),
        Complete,
    }

    #[test]
    fn write_branches() {
        struct Case {
            name: &'static str,
            /// Complete `order-1` first, so it is a tombstone at version 2.
            tombstone: bool,
            id: &'static str,
            version: u64,
            write: Write,
            commands: fn() -> Vec<SagaCommand>,
            want: Result<u64, &'static str>,
            want_row: Option<(u64, &'static str)>,
            want_unsent: &'static [&'static str],
        }
        let base = Case {
            name: "",
            tombstone: false,
            id: "order-1",
            version: 1,
            write: Write::Save(r#"{"status":"paid","total":1200}"#),
            commands: Vec::new,
            want: Ok(2),
            want_row: Some((2, "paid")),
            want_unsent: &[],
        };
        let cases = [
            Case { name: "save at the stored version bumps it", ..base },
            Case {
                name: "save stages the commands with the state",
                commands: || vec![command("order-1", 2, 0), command("order-1", 2, 1)],
                want_unsent: &["order-1:2:0", "order-1:2:1"],
                ..base
            },
            Case {
                name: "a stale save conflicts and stages no command",
                version: 0,
                commands: || vec![command("order-1", 1, 0)],
                want: Err("concurrency: expected 0, stored 1"),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                name: "absent at a nonzero version conflicts",
                id: "order-404",
                want: Err("concurrency: expected 1, stored 0"),
                want_row: None,
                ..base
            },
            Case { name: "absent at version zero is created", id: "order-404", version: 0, want: Ok(1), want_row: Some((1, "paid")), ..base },
            Case {
                name: "complete writes a tombstone at the next version with its commands",
                write: Write::Complete,
                commands: || vec![command("order-1", 2, 0)],
                want_row: Some((2, "completed")),
                want_unsent: &["order-1:2:0"],
                ..base
            },
            Case {
                name: "a stale complete conflicts",
                write: Write::Complete,
                version: 0,
                want: Err("concurrency: expected 0, stored 1"),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                // ABA: a writer that loaded the instance before it completed cannot write over the tombstone.
                name: "a stale writer after completion conflicts",
                tombstone: true,
                want: Err("concurrency: saga order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                // Resurrection: a late event that would start the saga again at version 0.
                name: "a fresh start over a tombstone conflicts",
                tombstone: true,
                version: 0,
                want: Err("concurrency: saga order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                name: "a write at the tombstone's own version is refused",
                tombstone: true,
                version: 2,
                commands: || vec![command("order-1", 3, 0)],
                want: Err("concurrency: saga order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                name: "completing a completed instance is refused",
                tombstone: true,
                version: 2,
                write: Write::Complete,
                want: Err("concurrency: saga order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                name: "a command id of another version is a codec error",
                commands: || vec![command("order-1", 1, 0)],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "order-1:1:0" for "order-1", expected "order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                name: "a command of another correlation is a codec error",
                commands: || vec![SagaCommand { correlation_id: "order-2".to_owned(), ..command("order-1", 2, 0) }],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "order-1:2:0" for "order-2", expected "order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                name: "commands out of index order are a codec error",
                commands: || vec![command("order-1", 2, 1), command("order-1", 2, 0)],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "order-1:2:1" for "order-1", expected "order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
        ];
        for c in cases {
            let store = seeded();
            if c.tombstone {
                assert_eq!(store.complete(NO_TX, "order-1", 1, vec![]), Ok(2), "{}: tombstone", c.name);
            }
            let got = match c.write {
                Write::Save(state) => store.save(NO_TX, c.id, c.version, serde_json::from_str(state).unwrap(), String::new(), (c.commands)()),
                Write::Complete => store.complete(NO_TX, c.id, c.version, (c.commands)()),
            };
            assert_eq!(wire(got), c.want.map_err(str::to_owned), "{}", c.name);
            assert_eq!(row(&store, c.id), c.want_row.map(|(v, s)| (v, s.to_owned())), "{}: row", c.name);
            assert_eq!(unsent_ids(&store), c.want_unsent, "{}: outbox", c.name);
        }
    }

    #[test]
    fn a_command_already_waiting_in_the_outbox_is_refused() {
        // Only a store that lost track of a tombstone could reach this, so it is pinned directly: a
        // second write may not enqueue an id the outbox already holds.
        let store = seeded();
        let tx = store.transactor().begin();
        let duplicate = vec![command("order-1", 2, 0), command("order-1", 2, 0)];
        assert_eq!(
            wire(store.save(tx, "order-1", 1, open(), String::new(), duplicate)),
            Err(r#"codec: saga command 1 of order-1 at version 2 is "order-1:2:0" for "order-1", expected "order-1:2:1""#.to_owned())
        );
        store.transactor().rollback(tx);
        let mut state = lock(&store.state);
        state.committed.saga_commands.push(command("order-1", 2, 0));
        drop(state);
        assert_eq!(
            wire(store.save(NO_TX, "order-1", 1, open(), String::new(), vec![command("order-1", 2, 0)])),
            Err("codec: saga command order-1:2:0 is already in the outbox".to_owned())
        );
    }

    #[test]
    fn the_outbox_shows_committed_commands_until_marked_sent() {
        let store = seeded();
        let tx = store.transactor().begin();
        store.save(tx, "order-1", 1, open(), String::new(), vec![command("order-1", 2, 0)]).unwrap();
        assert!(unsent_ids(&store).is_empty(), "a staged command is not one to send");
        store.transactor().commit(tx).unwrap();
        store.complete(NO_TX, "order-1", 2, vec![command("order-1", 3, 0), command("order-1", 3, 1)]).unwrap();
        assert_eq!(unsent_ids(&store), ["order-1:2:0", "order-1:3:0", "order-1:3:1"], "commit order, then index order");

        for (name, limit, want) in [
            ("zero_limit_reads_nothing", 0usize, vec![]),
            ("limit_below_size_reads_a_prefix", 2, vec!["order-1:2:0", "order-1:3:0"]),
        ] {
            assert_eq!(store.unpublished(limit).into_iter().map(|c| c.id).collect::<Vec<_>>(), want, "{name}");
        }
        let first = store.unpublished(1).remove(0);
        assert_eq!(first, command("order-1", 2, 0), "the row survives field for field");

        store.mark_published(vec!["order-1:2:0".to_owned(), "never-existed".to_owned()]);
        store.mark_published(vec!["order-1:2:0".to_owned()]);
        assert_eq!(unsent_ids(&store), ["order-1:3:0", "order-1:3:1"], "idempotent, unknown ids ignored");

        let rolled_back = store.transactor().begin();
        store.save(rolled_back, "order-9", 0, open(), String::new(), vec![command("order-9", 1, 0)]).unwrap();
        store.transactor().rollback(rolled_back);
        assert_eq!(unsent_ids(&store), ["order-1:3:0", "order-1:3:1"], "a rollback takes its commands with it");
    }

    #[test]
    fn load_returns_the_versioned_instance_its_tombstone_or_none() {
        let store = seeded();
        assert_eq!(store.load(NO_TX, "order-1").unwrap(), Some(Versioned { version: 1, value: SagaRow::Active { state: open(), folded: String::new() } }));
        assert_eq!(store.load(NO_TX, "order-404").unwrap(), None);

        // Any JSON value is a state: a saga that keeps `null` is distinct from a completed one.
        // The folded marks come back as they were written.
        let folded = r#"{"order-2":{"sequence":1,"id":"evt-1"}}"#.to_owned();
        store.save(NO_TX, "order-2", 0, Value::Null, folded.clone(), vec![]).unwrap();
        assert_eq!(store.load(NO_TX, "order-2").unwrap(), Some(Versioned { version: 1, value: SagaRow::Active { state: Value::Null, folded } }));
        store.complete(NO_TX, "order-2", 1, vec![]).unwrap();
        assert_eq!(store.load(NO_TX, "order-2").unwrap(), Some(Versioned { version: 2, value: SagaRow::Completed }));
    }

    #[test]
    fn a_held_instance_conflicts_naming_the_committed_version_and_the_last_version_takes_no_write() {
        let store = seeded();
        let tr = store.transactor();
        let holder = tr.begin();
        store.save(holder, "order-1", 1, open(), String::new(), vec![]).expect("the holder saves");
        assert_eq!(wire(store.save(NO_TX, "order-1", 2, open(), String::new(), vec![])), Err("concurrency: expected 2, stored 1".to_owned()));
        assert!(tr.rollback(holder));

        lock(&store.state).committed.sagas.insert("order-9".to_owned(), (u64::MAX, SagaRow::Active { state: open(), folded: String::new() }));
        assert_eq!(wire(store.save(NO_TX, "order-9", u64::MAX, open(), String::new(), vec![])), Err("codec: saga order-9 is at the last version".to_owned()));
    }
}
