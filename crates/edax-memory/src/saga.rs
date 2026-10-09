//! The saga store view: per-instance state with optimistic concurrency, a tombstone on completion,
//! and the saga outbox the step's commands commit to. Rows and outbox are keyed by the saga's name, so
//! several sagas share one database without sharing an instance or a command.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use edax_core::{lock, Error, ErrorCode};
use edax_event::{check_saga_commands, CommandMessage};
use serde_json::Value;

use crate::tx::{store_view, Committed, MemoryState, MemoryTransactor, Tx, WriteSet};
use crate::versioned::{Staged, Versioned};

/// The key of a saga instance: the saga's name, then the instance id. Two sagas that share a database
/// (as they must, for their writes to join the events' unit of work) and an instance id are two rows.
pub(crate) type SagaKey = (String, String);

impl Committed {
    /// Merge a transaction's saga rows and the commands each saga's steps staged.
    pub(crate) fn apply_sagas(&mut self, rows: Staged<SagaKey, SagaRow>, outbox: HashMap<String, Vec<CommandMessage>>) {
        self.sagas.apply(rows);
        for (saga, commands) in outbox {
            self.saga_outbox.entry(saga).or_default().apply(commands);
        }
    }
}

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

/// In-memory saga instance store for development and tests: per-instance state with optimistic
/// concurrency on the version, and the saga outbox. State is any JSON
/// value (a saga that keeps `null` is legitimate). The rule is written once here, so every SDK's
/// saga runner is held to the same one:
///
/// - every row and every outbox command belongs to one saga, named by the caller: two sagas over one
///   database never see each other's instances, even under one instance id, nor drain each other's
///   commands;
/// - a `save` or `complete` at a stale version is a conflict, and so is any write to a completed
///   instance: completion is final;
/// - `complete` writes a tombstone at the next version instead of removing the row, so the version
///   never goes back to 0 and a stale writer that loaded the old instance cannot re-create it (ABA);
/// - the commands a step decided are staged with its state in the same write, so they commit or
///   roll back together. Each is the kernel's `saga_command` row: its id is the kernel's
///   [`saga_command_id`](edax_event::saga_command_id) for that write and its `aggregate_id` is the instance, or the write is a
///   `codec` error and stages nothing;
/// - [`unpublished`](Self::unpublished) shows a saga's committed commands only, oldest commit first,
///   until [`mark_published`](Self::mark_published) prunes them.
///
/// A write under an open token is staged until the transactor commits. Not for production: a
/// durable store enlists the write in the events' transaction and purges tombstones by retention.
#[derive(Clone)]
pub struct MemorySagaStore {
    pub(crate) state: Arc<Mutex<MemoryState>>,
}

store_view!(MemorySagaStore, saga_store);

/// The key of instance `id` of `saga`.
fn key(saga: &str, id: &str) -> SagaKey {
    (saga.to_owned(), id.to_owned())
}

/// The refusal of any write to a completed instance: completion is final.
fn completed(saga: &str, id: &str, version: u64) -> Error {
    Error::new(
        ErrorCode::Concurrency,
        format_args!("saga {saga} instance {id} completed at version {version}: a completed instance takes no write"),
    )
}

impl MemorySagaStore {
    /// The stored instance `id` of `saga` and its version as `tx` sees it — running with its state, or
    /// the tombstone of a completed one — or `None` if there is none.
    pub fn load(&self, tx: Tx, saga: &str, id: &str) -> Result<Option<Versioned<SagaRow>>, Error> {
        lock(&self.state).read(tx, |committed, own| {
            committed.sagas.get(own.map(|set| &set.sagas), &key(saga, id)).cloned()
        })
    }

    /// Persist `state`, the `folded` marks and the step's `commands` for instance `id` of `saga` at
    /// `version + 1` if the version `tx` sees still equals `version` (an absent instance is at version
    /// 0); returns the new version. A version mismatch, a completed instance, or an instance another
    /// open transaction has written and not yet committed is a conflict error; a command that does not
    /// belong to this write is a codec error. A refused write stages nothing.
    #[allow(
        clippy::too_many_arguments,
        reason = "the port's own arguments: the row's key, its version and its contents"
    )]
    pub fn save(
        &self,
        tx: Tx,
        saga: &str,
        id: &str,
        version: u64,
        state: Value,
        folded: String,
        commands: Vec<CommandMessage>,
    ) -> Result<u64, Error> {
        self.write(tx, saga, id, version, SagaRow::Active { state, folded }, commands)
    }

    /// Complete instance `id` of `saga`: write its tombstone and the step's `commands` at
    /// `version + 1` under the same rules as [`save`](Self::save); returns the tombstone's version.
    pub fn complete(
        &self,
        tx: Tx,
        saga: &str,
        id: &str,
        version: u64,
        commands: Vec<CommandMessage>,
    ) -> Result<u64, Error> {
        self.write(tx, saga, id, version, SagaRow::Completed, commands)
    }

    fn write(
        &self,
        tx: Tx,
        saga: &str,
        id: &str,
        version: u64,
        row: SagaRow,
        commands: Vec<CommandMessage>,
    ) -> Result<u64, Error> {
        lock(&self.state).write(tx, |committed, others, own| {
            let key = key(saga, id);
            let held = others.values().map(|set| &set.sagas);
            let admit = |stored: u64, row: &SagaRow| match row {
                SagaRow::Completed => Err(completed(saga, id, stored)),
                SagaRow::Active { .. } => Ok(()),
            };
            let next = committed.sagas.next(
                held,
                &own.sagas,
                &key,
                version,
                &format!("saga {saga} instance {id}"),
                admit,
            )?;
            check_saga_commands(saga, id, next, &commands)?;
            stage_commands(committed.saga_outbox.get(saga), others, own, saga, commands)?;
            own.sagas.insert(
                key,
                Versioned {
                    version: next,
                    value: row,
                },
            );
            Ok(next)
        })
    }

    /// `saga`'s committed outbox commands not yet sent, oldest commit first, capped at `limit`. The
    /// relay reads outside any transaction, so there is no token: a staged command is not one to send.
    pub fn unpublished(&self, saga: &str, limit: usize) -> Vec<CommandMessage> {
        lock(&self.state)
            .committed
            .saga_outbox
            .get(saga)
            .map_or_else(Vec::new, |rows| rows.page(limit))
    }

    /// How many of `saga`'s committed outbox commands wait, without paging them.
    pub fn unpublished_count(&self, saga: &str) -> usize {
        lock(&self.state)
            .committed
            .saga_outbox
            .get(saga)
            .map_or(0, |rows| rows.count())
    }

    /// Mark `saga`'s outbox commands sent by id, pruning them. Idempotent; an unknown id is a no-op.
    pub fn mark_published(&self, saga: &str, ids: &[String]) {
        if let Some(rows) = lock(&self.state).committed.saga_outbox.get_mut(saga) {
            rows.prune(ids);
        }
    }
}

/// Stage `commands` in `saga`'s outbox under `own`, refusing an id that already waits there: committed,
/// or staged by any open transaction. `committed` is that saga's committed outbox, if it has one yet.
fn stage_commands(
    committed: Option<&crate::outbox_rows::OutboxRows<CommandMessage>>,
    others: &HashMap<Tx, WriteSet>,
    own: &mut WriteSet,
    saga: &str,
    commands: Vec<CommandMessage>,
) -> Result<(), Error> {
    let empty = crate::outbox_rows::OutboxRows::default();
    let staged_by_others = others.values().filter_map(|set| set.saga_outbox.get(saga)).flatten();
    let own_rows = own.saga_outbox.entry(saga.to_owned()).or_default();
    committed
        .unwrap_or(&empty)
        .stage(staged_by_others, own_rows, commands, "saga command")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{ids, wire};
    use crate::tx::Tx;
    use edax_event::testkit::saga_command;
    use serde_json::json;

    /// The saga every case writes as.
    const SAGA: &str = "shipping";

    fn open() -> Value {
        json!({"status": "awaiting_payment", "total": 1200})
    }

    /// The `index`-th command a write of instance `id` of [`SAGA`] at `version` sends, as the kernel
    /// builds it.
    fn command(id: &str, version: u64, index: u64) -> CommandMessage {
        saga_command(SAGA, id, version, index)
    }

    /// A saga store holding one open instance at version 1.
    fn seeded() -> MemorySagaStore {
        let store = MemorySagaStore::new();
        store
            .save(Tx::auto_commit(), SAGA, "order-1", 0, open(), String::new(), vec![])
            .expect("seed saves");
        store
    }

    /// The stored row as `(version, status or "completed")`, or `None`.
    fn row(store: &MemorySagaStore, id: &str) -> Option<(u64, String)> {
        store.load(Tx::auto_commit(), SAGA, id).unwrap().map(|stored| {
            let status = match stored.value {
                SagaRow::Completed => "completed".to_owned(),
                SagaRow::Active { state, .. } => state["status"].as_str().unwrap().to_owned(),
            };
            (stored.version, status)
        })
    }

    fn unsent_ids(store: &MemorySagaStore) -> Vec<String> {
        store.unpublished(SAGA, 100).into_iter().map(|c| c.id).collect()
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
            commands: fn() -> Vec<CommandMessage>,
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
                want_unsent: &["8:shipping:order-1:2:0", "8:shipping:order-1:2:1"],
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
                want_unsent: &["8:shipping:order-1:2:0"],
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
                want: Err("concurrency: saga shipping instance order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                // Resurrection: a late event that would start the saga again at version 0.
                name: "a fresh start over a tombstone conflicts",
                tombstone: true,
                version: 0,
                want: Err("concurrency: saga shipping instance order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                name: "a write at the tombstone's own version is refused",
                tombstone: true,
                version: 2,
                commands: || vec![command("order-1", 3, 0)],
                want: Err("concurrency: saga shipping instance order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                name: "completing a completed instance is refused",
                tombstone: true,
                version: 2,
                write: Write::Complete,
                want: Err("concurrency: saga shipping instance order-1 completed at version 2: a completed instance takes no write"),
                want_row: Some((2, "completed")),
                ..base
            },
            Case {
                name: "a command id of another version is a codec error",
                commands: || vec![command("order-1", 1, 0)],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "8:shipping:order-1:1:0" for instance Some("order-1"), expected "8:shipping:order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                name: "a command of another instance is a codec error",
                commands: || vec![CommandMessage { aggregate_id: Some("order-2".to_owned()), ..command("order-1", 2, 0) }],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "8:shipping:order-1:2:0" for instance Some("order-2"), expected "8:shipping:order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                name: "a command without an instance is a codec error",
                commands: || vec![CommandMessage { aggregate_id: None, ..command("order-1", 2, 0) }],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "8:shipping:order-1:2:0" for instance None, expected "8:shipping:order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
            Case {
                name: "commands out of index order are a codec error",
                commands: || vec![command("order-1", 2, 1), command("order-1", 2, 0)],
                want: Err(r#"codec: saga command 0 of order-1 at version 2 is "8:shipping:order-1:2:1" for instance Some("order-1"), expected "8:shipping:order-1:2:0""#),
                want_row: Some((1, "awaiting_payment")),
                ..base
            },
        ];
        for c in cases {
            let store = seeded();
            if c.tombstone {
                assert_eq!(
                    store.complete(Tx::auto_commit(), SAGA, "order-1", 1, vec![]),
                    Ok(2),
                    "{}: tombstone",
                    c.name
                );
            }
            let got = match c.write {
                Write::Save(state) => store.save(
                    Tx::auto_commit(),
                    SAGA,
                    c.id,
                    c.version,
                    serde_json::from_str(state).unwrap(),
                    String::new(),
                    (c.commands)(),
                ),
                Write::Complete => store.complete(Tx::auto_commit(), SAGA, c.id, c.version, (c.commands)()),
            };
            assert_eq!(wire(got), c.want.map_err(str::to_owned), "{}", c.name);
            assert_eq!(
                row(&store, c.id),
                c.want_row.map(|(v, s)| (v, s.to_owned())),
                "{}: row",
                c.name
            );
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
            wire(store.save(tx, SAGA, "order-1", 1, open(), String::new(), duplicate)),
            Err(r#"codec: saga command 1 of order-1 at version 2 is "8:shipping:order-1:2:0" for instance Some("order-1"), expected "8:shipping:order-1:2:1""#.to_owned())
        );
        store.transactor().rollback(tx);
        lock(&store.state)
            .committed
            .saga_outbox
            .entry(SAGA.to_owned())
            .or_default()
            .apply(vec![command("order-1", 2, 0)]);
        assert_eq!(
            wire(store.save(
                Tx::auto_commit(),
                SAGA,
                "order-1",
                1,
                open(),
                String::new(),
                vec![command("order-1", 2, 0)]
            )),
            Err("codec: saga command 8:shipping:order-1:2:0 is already in the outbox".to_owned())
        );
    }

    #[test]
    fn the_outbox_shows_committed_commands_until_marked_sent() {
        let store = seeded();
        let tx = store.transactor().begin();
        store
            .save(
                tx,
                SAGA,
                "order-1",
                1,
                open(),
                String::new(),
                vec![command("order-1", 2, 0)],
            )
            .unwrap();
        assert!(unsent_ids(&store).is_empty(), "a staged command is not one to send");
        store.transactor().commit(tx).unwrap();
        store
            .complete(
                Tx::auto_commit(),
                SAGA,
                "order-1",
                2,
                vec![command("order-1", 3, 0), command("order-1", 3, 1)],
            )
            .unwrap();
        assert_eq!(
            unsent_ids(&store),
            [
                "8:shipping:order-1:2:0",
                "8:shipping:order-1:3:0",
                "8:shipping:order-1:3:1"
            ],
            "commit order, then index order"
        );

        for (name, limit, want) in [
            ("zero_limit_reads_nothing", 0usize, vec![]),
            (
                "limit_below_size_reads_a_prefix",
                2,
                vec!["8:shipping:order-1:2:0", "8:shipping:order-1:3:0"],
            ),
        ] {
            assert_eq!(
                store
                    .unpublished(SAGA, limit)
                    .into_iter()
                    .map(|c| c.id)
                    .collect::<Vec<_>>(),
                want,
                "{name}"
            );
        }
        let first = store.unpublished(SAGA, 1).remove(0);
        assert_eq!(first, command("order-1", 2, 0), "the row survives field for field");

        store.mark_published(SAGA, &["8:shipping:order-1:2:0".to_owned(), "never-existed".to_owned()]);
        store.mark_published(SAGA, &["8:shipping:order-1:2:0".to_owned()]);
        assert_eq!(
            unsent_ids(&store),
            ["8:shipping:order-1:3:0", "8:shipping:order-1:3:1"],
            "idempotent, unknown ids ignored"
        );

        let rolled_back = store.transactor().begin();
        store
            .save(
                rolled_back,
                SAGA,
                "order-9",
                0,
                open(),
                String::new(),
                vec![command("order-9", 1, 0)],
            )
            .unwrap();
        store.transactor().rollback(rolled_back);
        assert_eq!(
            unsent_ids(&store),
            ["8:shipping:order-1:3:0", "8:shipping:order-1:3:1"],
            "a rollback takes its commands with it"
        );
    }

    #[test]
    fn load_returns_the_versioned_instance_its_tombstone_or_none() {
        let store = seeded();
        assert_eq!(
            store.load(Tx::auto_commit(), SAGA, "order-1").unwrap(),
            Some(Versioned {
                version: 1,
                value: SagaRow::Active {
                    state: open(),
                    folded: String::new()
                }
            })
        );
        assert_eq!(store.load(Tx::auto_commit(), SAGA, "order-404").unwrap(), None);

        // Any JSON value is a state: a saga that keeps `null` is distinct from a completed one.
        // The folded marks come back as they were written.
        let folded = r#"{"order-2":{"sequence":1,"id":"evt-1","unsequenced":"evt-u"}}"#.to_owned();
        store
            .save(
                Tx::auto_commit(),
                SAGA,
                "order-2",
                0,
                Value::Null,
                folded.clone(),
                vec![],
            )
            .unwrap();
        assert_eq!(
            store.load(Tx::auto_commit(), SAGA, "order-2").unwrap(),
            Some(Versioned {
                version: 1,
                value: SagaRow::Active {
                    state: Value::Null,
                    folded
                }
            })
        );
        store.complete(Tx::auto_commit(), SAGA, "order-2", 1, vec![]).unwrap();
        assert_eq!(
            store.load(Tx::auto_commit(), SAGA, "order-2").unwrap(),
            Some(Versioned {
                version: 2,
                value: SagaRow::Completed
            })
        );
    }

    #[test]
    fn a_held_instance_conflicts_naming_the_committed_version_and_the_last_version_takes_no_write() {
        let store = seeded();
        let tr = store.transactor();
        let holder = tr.begin();
        store
            .save(holder, SAGA, "order-1", 1, open(), String::new(), vec![])
            .expect("the holder saves");
        assert_eq!(
            wire(store.save(Tx::auto_commit(), SAGA, "order-1", 2, open(), String::new(), vec![])),
            Err("concurrency: expected 2, stored 1".to_owned())
        );
        assert!(tr.rollback(holder));

        lock(&store.state).committed.sagas.seed(
            key(SAGA, "order-9"),
            u64::MAX,
            SagaRow::Active {
                state: open(),
                folded: String::new(),
            },
        );
        assert_eq!(
            wire(store.save(
                Tx::auto_commit(),
                SAGA,
                "order-9",
                u64::MAX,
                open(),
                String::new(),
                vec![]
            )),
            Err("codec: saga shipping instance order-9 at version 18446744073709551615 has no next version".to_owned())
        );
    }

    #[test]
    fn two_sagas_over_one_database_share_neither_an_instance_nor_a_command() {
        // Both write `order-1` at version 1: under one key the second would conflict. Each saga's
        // command id names the saga, and each outbox holds its own saga's rows only.
        let tr = MemoryTransactor::new();
        let store = tr.saga_store();
        let sagas = [
            ("shipping", "awaiting_payment", "8:shipping:order-1:1:0"),
            ("billing", "invoiced", "7:billing:order-1:1:0"),
        ];
        for (saga, status, _) in sagas {
            let wrote = store.save(
                Tx::auto_commit(),
                saga,
                "order-1",
                0,
                json!({"status": status}),
                String::new(),
                vec![saga_command(saga, "order-1", 1, 0)],
            );
            assert_eq!(wrote, Ok(1), "{saga}: its own instance");
        }
        for (saga, status, id) in sagas {
            let loaded = store
                .load(Tx::auto_commit(), saga, "order-1")
                .unwrap()
                .expect("each saga's own row");
            assert_eq!(
                loaded.value,
                SagaRow::Active {
                    state: json!({"status": status}),
                    folded: String::new()
                },
                "{saga}"
            );
            assert_eq!(
                (store.unpublished_count(saga), ids(&store.unpublished(saga, 10))),
                (1, vec![id.to_owned()]),
                "{saga}: its own outbox"
            );
        }
        let another = wire(store.save(
            Tx::auto_commit(),
            "returns",
            "order-1",
            0,
            json!({}),
            String::new(),
            vec![command("order-1", 1, 0)],
        ));
        assert!(
            another.is_err_and(|e| e.starts_with("codec: saga command 0 of order-1")),
            "a saga refuses another saga's command"
        );
        store.mark_published("shipping", &["8:shipping:order-1:1:0".to_owned()]);
        assert_eq!(
            (store.unpublished_count("shipping"), store.unpublished_count("billing")),
            (0, 1),
            "a mark prunes one saga's row"
        );
        assert_eq!(
            store.load(Tx::auto_commit(), "returns", "order-1"),
            Ok(None),
            "a saga that never wrote has no instance"
        );
        assert_eq!(
            (store.unpublished("returns", 10), store.unpublished_count("returns")),
            (vec![], 0),
            "and no outbox"
        );
        store.mark_published("returns", &["8:shipping:order-1:1:0".to_owned()]);
    }
}
