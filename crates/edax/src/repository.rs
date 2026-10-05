//! Save/load orchestration over an [`EventStore`]: a [`Repository`] rehydrates an aggregate from its
//! snapshot and events, and saves its pending events and a snapshot in one unit of work.

use edax_core::{tenant_key, Tenancy};
use edax_event::{draft_batch, originate, EventMessage, Extensions, Lineage, Metadata, SaveContext};

use crate::store::{EventStore, Snapshot};
use crate::{Aggregate, AggregateCodec, Error, EventCodec};

/// The context a [`Repository::save`] stamps on every event of its batch. Every field defaults to empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SaveOptions {
    /// The flow the events continue: [`propagate`](crate::propagate) from the message that caused them,
    /// or [`originate`] from the ids an inbound request holds. `None` starts a new flow, one fresh lineage
    /// every event of the batch shares.
    pub lineage: Option<Lineage>,
    /// Edax extension context, such as a trace parent, under the kernel's key rule.
    pub extensions: Extensions,
    /// The application's own context. Keep per-delivery and per-request ids here, not in a command
    /// payload, so they never change an idempotency hash.
    pub metadata: Metadata,
    /// The valid time of the batch in unix nanoseconds, when the facts happened earlier than the store
    /// records them (an import, an offline client). `None` means they happened when the store records them.
    pub occurred_at: Option<i64>,
}

/// Loads aggregates (snapshot then replay the rest) and saves them (append the pending events and, at
/// the [`snapshot_every`](Self::snapshot_every) interval, a fresh snapshot in one transaction; the
/// store also enqueues the outbox).
pub struct Repository<S, EC, AC> {
    store: S,
    events: EC,
    aggregates: AC,
    new_id: Box<dyn Fn() -> String + Send + Sync>,
    snapshot_every: u64,
    tenancy: Tenancy,
}

impl<S, EC, AC> Repository<S, EC, AC>
where
    S: EventStore,
    AC: AggregateCodec,
    EC: EventCodec<Domain = <AC::Aggregate as Aggregate>::Event>,
{
    pub fn new(store: S, events: EC, aggregates: AC) -> Self {
        Self {
            store,
            events,
            aggregates,
            new_id: Box::new(edax_core::guid),
            snapshot_every: 1,
            tenancy: Tenancy::Silo,
        }
    }

    /// How this repository separates tenants, fixed here rather than per call, so no two calls of one
    /// service can key differently. The default is [`Tenancy::Silo`]: no tenant, no composed key.
    ///
    /// Under [`Tenancy::Pool`] every stream and snapshot key is the aggregate id composed with its
    /// tenant ([`edax_core::tenant_key`]), and a load or save that names no tenant is a terminal
    /// `codec` error rather than a read of the whole pool. Under [`Tenancy::Bridge`] a tenant is
    /// required and keys stay bare, because this repository's store already holds one tenant.
    pub fn tenancy(mut self, mode: Tenancy) -> Self {
        self.tenancy = mode;
        self
    }

    /// How often a save writes a snapshot: each time the aggregate's version crosses a multiple of
    /// `every`, so `1` (the default) snapshots on every save and `0` never does. The kernel's rule
    /// ([`edax_event::snapshot_due`]). A snapshot doubles the writes of the save that takes it; a
    /// stream that is cheap to replay wants a larger interval.
    pub fn snapshot_every(mut self, every: u64) -> Self {
        self.snapshot_every = every;
        self
    }

    /// Override the id source, for deterministic tests. Each id names one event, or roots the flow of
    /// a plain [`save`](Self::save). A closure, so a caller can capture a seeded counter instead of
    /// reaching for a global. The time an event is recorded at is the store's: see
    /// [`EventStore::append`].
    pub fn with_ids(mut self, new_id: impl Fn() -> String + Send + Sync + 'static) -> Self {
        self.new_id = Box::new(new_id);
        self
    }

    /// Rehydrate the aggregate (snapshot, then replay the events after it) for `tenant`, in a unit of work
    /// of its own. The loaded aggregate carries the tenant ([`Root::tenant`](crate::Root::tenant)), so the
    /// save that follows stamps and keys the same one. Inside a transaction the caller already opened, use
    /// [`Repository::load_in`].
    pub fn load(&self, tenant: Option<&str>, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        self.store.transaction(|tx| self.load_in(tx, tenant, aggregate_id))
    }

    /// [`Repository::load`] inside the caller's transaction: the read sees that transaction's own
    /// appends, so a save then a load in one unit of work agree (read-your-writes).
    pub fn load_in(&self, tx: &mut S::Tx, tenant: Option<&str>, aggregate_id: &str) -> Result<AC::Aggregate, Error> {
        let key = tenant_key(self.tenancy, tenant, aggregate_id)?;
        let mut agg = self.replay(tx, &key, aggregate_id)?;
        agg.root_mut().set_tenant(tenant);
        Ok(agg)
    }

    /// Persist the aggregate's pending events in a unit of work of its own, with the context `options`
    /// stamps on every event of the batch; return the committed events (publish them eagerly, or leave
    /// them to the [`Relay`](crate::Relay)). On a clean commit the aggregate is reset to its new version; on
    /// error it is left untouched, so a retry (worth it for a `store` error, not for a terminal `codec`
    /// error) re-sends the same pending events. Inside a transaction the caller already opened, use
    /// [`Repository::save_in`].
    ///
    /// # Errors
    /// A `codec` error for an extension key outside the kernel's rule, before anything is written: the
    /// relay could never encode the event.
    pub fn save(&self, agg: &mut AC::Aggregate, options: SaveOptions) -> Result<Vec<EventMessage>, Error> {
        let lineage = self.lineage(options.lineage.as_ref());
        // Stage inside the transaction, reset outside it. This method owns the commit, so it waits for the
        // commit to land before clearing the aggregate: a failed commit leaves the pending events in place and
        // the caller can save again. `save_in` cannot, because its caller commits after it returns.
        let (events, current) = self.store.transaction(|tx| self.stage(tx, agg, &lineage, &options))?;
        agg.root_mut().settle(current);
        Ok(events)
    }

    /// [`Repository::save`] inside the caller's transaction, so the events, the snapshot and the
    /// outbox rows commit or roll back with whatever else the caller writes under `tx` — a
    /// projection row, an idempotency claim. The aggregate is reset as soon as the store accepts the
    /// writes, before the caller commits: after a rollback it no longer holds the events, so reload
    /// it rather than save it again. The returned events are not committed until the caller commits.
    pub fn save_in(
        &self,
        tx: &mut S::Tx,
        agg: &mut AC::Aggregate,
        options: SaveOptions,
    ) -> Result<Vec<EventMessage>, Error> {
        let lineage = self.lineage(options.lineage.as_ref());
        let (events, current) = self.stage(tx, agg, &lineage, &options)?;
        agg.root_mut().settle(current);
        Ok(events)
    }

    /// The lineage every event of the batch shares: the one given, else a new flow minted from this
    /// repository's id source.
    fn lineage(&self, given: Option<&Lineage>) -> Lineage {
        given
            .cloned()
            .unwrap_or_else(|| originate(None, None, &(self.new_id)()))
    }

    /// Marshal the pending events, append them and write the snapshot under `tx`. Returns the stored
    /// events and the aggregate's new version; resetting the aggregate is the caller's, because only
    /// the caller knows whether the commit has landed yet.
    fn stage(
        &self,
        tx: &mut S::Tx,
        agg: &AC::Aggregate,
        lineage: &Lineage,
        options: &SaveOptions,
    ) -> Result<(Vec<EventMessage>, u64), Error> {
        let root = agg.root();
        let initial = root.version();
        // The tenant comes from the aggregate the caller loaded, never from this call, so a save cannot
        // name a tenant the load did not.
        let tenant = root.tenant();
        let key = tenant_key(self.tenancy, tenant, root.id())?;

        // Marshal the whole batch before the first write: a payload that cannot be written fails the
        // save outright, so a half-marshaled batch can never reach the store.
        let payloads = root
            .pending()
            .iter()
            .map(|event| self.events.marshal(event))
            .collect::<Result<Vec<_>, Error>>()?;
        let ids = payloads.iter().map(|_| (self.new_id)()).collect();
        let context = SaveContext {
            tenant,
            lineage,
            extensions: &options.extensions,
            metadata: &options.metadata,
            occurred_at: options.occurred_at,
        };
        let batch = draft_batch(root.id(), initial, self.snapshot_every, payloads, ids, &context)?;

        // Dumped before the first write, like the events: a snapshot that cannot be written fails the
        // save outright.
        let snapshot = if batch.snapshot {
            let data = self.aggregates.dump(agg)?;
            Some(Snapshot {
                aggregate_id: key.clone(),
                type_name: self.aggregates.type_name().to_string(),
                version: batch.version,
                data,
            })
        } else {
            None
        };

        let events = self.store.append(tx, &key, initial, batch.drafts)?;
        if let Some(snapshot) = snapshot {
            self.store.save_snapshot(tx, &snapshot)?;
        }
        Ok((events, batch.version))
    }
    /// Rebuild from the snapshot and the events after it. A snapshot is a cache of the event log, so
    /// one this codec cannot use — another aggregate type's under the same id, or one whose `dump`
    /// format no longer loads — is ignored and the stream replays from version 0. The loaded tail is
    /// held to the kernel's dense-run rule, so a store that returns a gap or a repeat fails the load
    /// instead of folding the wrong state.
    fn replay(&self, tx: &mut S::Tx, key: &str, id: &str) -> Result<AC::Aggregate, Error> {
        let cached = self
            .store
            .load_snapshot(tx, key)?
            .filter(|s| s.type_name == self.aggregates.type_name())
            .and_then(|s| {
                self.aggregates
                    .load(id, s.version, Some(&s.data))
                    .ok()
                    .map(|agg| (s.version, agg))
            });
        let (base, mut agg) = match cached {
            Some(found) => found,
            None => (0, self.aggregates.load(id, 0, None)?),
        };
        let events = self.store.load(tx, key, base)?;
        let sequences: Vec<Option<u64>> = events.iter().map(|e| e.sequence).collect();
        let version = edax_event::check_batch(base, &sequences)?;
        for env in events {
            // Replay drives state directly — not apply(), which would re-queue these as pending.
            agg.transition(&self.events.unmarshal(&env.into_payload())?);
        }
        agg.root_mut().settle(version);
        Ok(agg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::account::{deposited, next_id, repo, Account, AccountCodec, AccountEvent, AccountEvents};
    use crate::testkit::RECORDED_AT;
    use crate::{ErrorCode, EventDraft, JsonCodec, MemoryEventStore, MemoryTx, Outbox, Root, Transactor};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    #[test]
    fn save_assigns_sequences_and_resets_aggregate() {
        let r = repo(MemoryEventStore::new());
        let mut a = Account::open("acct-1");
        a.apply(AccountEvent::Deposited(10));
        a.apply(AccountEvent::Deposited(5));
        let envs = r.save(&mut a, SaveOptions::default()).unwrap();
        assert_eq!(
            envs.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
        );
        assert!(a.root().pending().is_empty());
        assert_eq!((a.root().version(), a.root().pending().len()), (2, 0));
    }

    #[test]
    fn save_stamps_one_lineage_and_context_on_every_event() {
        // A plain save roots a new flow: every event of the batch shares one lineage, and the store records
        // them at its own clock. A refused context writes nothing. a save carrying the lineage and
        // context it is handed is the model test's (`tests/model.rs`, `repository_matches_fold`).
        struct Case {
            name: &'static str,
            /// `None` saves plainly; `Some` passes this lineage and context in the `SaveOptions`.
            context: Option<(Lineage, Extensions, Metadata)>,
            want_err: Option<ErrorCode>,
        }
        let cases = [
            Case {
                name: "a plain save roots a new flow",
                context: None,
                want_err: None,
            },
            Case {
                name: "an extension key outside the rule is refused before any write",
                context: Some((
                    crate::propagate("cmd-9", "corr-9"),
                    Extensions::from([("Traceparent".to_owned(), "x".to_owned())]),
                    Metadata::from([("tenant".to_owned(), "acme".to_owned())]),
                )),
                want_err: Some(ErrorCode::Codec),
            },
        ];
        for c in cases {
            let store = MemoryEventStore::new().with_clock(|| RECORDED_AT);
            let r = repo(store.clone());
            let mut a = Account::open("acct-l");
            a.apply(AccountEvent::Deposited(10));
            a.apply(AccountEvent::Deposited(5));
            let got = match c.context {
                None => r.save(&mut a, SaveOptions::default()),
                Some((lineage, extensions, metadata)) => r.save(
                    &mut a,
                    SaveOptions {
                        lineage: Some(lineage),
                        extensions,
                        metadata,
                        occurred_at: None,
                    },
                ),
            };
            if let Some(want) = c.want_err {
                assert_eq!(got.err().map(|e| e.code), Some(want), "{}", c.name);
                assert!(
                    store
                        .load(&mut MemoryTx::auto_commit(), "acct-l", 0)
                        .unwrap()
                        .is_empty(),
                    "{}: nothing appended",
                    c.name
                );
                assert_eq!(
                    a.root().pending().len(),
                    2,
                    "{}: the aggregate keeps its events",
                    c.name
                );
                continue;
            }
            let events = got.unwrap_or_else(|e| panic!("{}: {e}", c.name));
            assert_eq!(
                events,
                store.load(&mut MemoryTx::auto_commit(), "acct-l", 0).unwrap(),
                "{}: returned what was stored",
                c.name
            );
            let first = &events[0];
            assert_eq!(
                first.causation_id, first.correlation_id,
                "{}: a root causes itself",
                c.name
            );
            assert!(
                events.iter().all(|e| e.id != first.correlation_id),
                "{}: the fresh id is no event's id",
                c.name
            );
            for e in &events {
                assert_eq!(
                    (&e.correlation_id, &e.causation_id),
                    (&first.correlation_id, &first.causation_id),
                    "{}: one lineage",
                    c.name
                );
                assert_eq!(
                    (e.recorded_at, e.occurred_at),
                    (RECORDED_AT, None),
                    "{}: the store's time",
                    c.name
                );
                assert!(
                    e.extensions.is_empty() && e.metadata.is_empty(),
                    "{}: no context",
                    c.name
                );
            }
        }
    }

    #[test]
    fn reload_via_snapshot() {
        let r = repo(MemoryEventStore::new());
        let mut a = Account::open("acct-2");
        a.apply(AccountEvent::Deposited(10));
        a.apply(AccountEvent::Deposited(5));
        r.save(&mut a, SaveOptions::default()).unwrap(); // snapshots at version 2
        let b = r.load(None, "acct-2").unwrap();
        assert_eq!(b.balance, 15);
        assert_eq!(b.root().version(), 2);
    }

    #[test]
    fn repository_tenancy_branches() {
        // One row per mode and per way to name no tenant: the key the stream and the snapshot land
        // under, the tenant every event is stamped with, or the refusal before any write.
        const ID: &str = "acct-t";
        struct Case {
            name: &'static str,
            mode: Tenancy,
            /// The tenant the aggregate is loaded for.
            tenant: Option<&'static str>,
            /// Build the aggregate with its constructor instead of loading it, so it carries no tenant.
            constructed: bool,
            /// The stream key the events land under, or the code the load or the save fails with.
            want: Result<&'static str, ErrorCode>,
        }
        let cases = [
            Case {
                name: "a pool composes the key and stamps the tenant",
                mode: Tenancy::Pool,
                tenant: Some("acme"),
                constructed: false,
                want: Ok("4:acme:acct-t"),
            },
            Case {
                name: "a pool refuses a load with no tenant",
                mode: Tenancy::Pool,
                tenant: None,
                constructed: false,
                want: Err(ErrorCode::Codec),
            },
            Case {
                name: "a pool refuses to save a constructed aggregate",
                mode: Tenancy::Pool,
                tenant: None,
                constructed: true,
                want: Err(ErrorCode::Codec),
            },
            Case {
                name: "a bridge keeps the key bare and stamps the tenant",
                mode: Tenancy::Bridge,
                tenant: Some("acme"),
                constructed: false,
                want: Ok(ID),
            },
            Case {
                name: "a bridge refuses a load with no tenant",
                mode: Tenancy::Bridge,
                tenant: None,
                constructed: false,
                want: Err(ErrorCode::Codec),
            },
            Case {
                name: "a bridge refuses to save a constructed aggregate",
                mode: Tenancy::Bridge,
                tenant: None,
                constructed: true,
                want: Err(ErrorCode::Codec),
            },
            Case {
                name: "a silo keeps the key bare and stamps none",
                mode: Tenancy::Silo,
                tenant: None,
                constructed: false,
                want: Ok(ID),
            },
            Case {
                name: "a silo saves a constructed aggregate",
                mode: Tenancy::Silo,
                tenant: None,
                constructed: true,
                want: Ok(ID),
            },
        ];
        for c in cases {
            let store = MemoryEventStore::new();
            let r = repo(store.clone()).tenancy(c.mode);
            let loaded = if c.constructed {
                Ok(Account::open(ID))
            } else {
                r.load(c.tenant, ID)
            };
            let saved = loaded.and_then(|mut a| {
                a.apply(AccountEvent::Deposited(10));
                r.save(&mut a, SaveOptions::default()).map(|events| (a, events))
            });
            let stream = |key: &str| store.load(&mut MemoryTx::auto_commit(), key, 0).unwrap();
            match (saved, c.want) {
                (Ok((a, events)), Ok(key)) => {
                    assert_eq!(
                        a.root().tenant(),
                        c.tenant,
                        "{}: the aggregate carries its tenant",
                        c.name
                    );
                    assert_eq!(stream(key), events, "{}: the events landed under {key}", c.name);
                    for e in &events {
                        assert_eq!(
                            (e.aggregate_id.as_str(), e.tenant.as_deref()),
                            (ID, c.tenant),
                            "{}: the id stays bare, the tenant is stamped",
                            c.name
                        );
                    }
                    if key != ID {
                        assert!(stream(ID).is_empty(), "{}: nothing under the bare id", c.name);
                    }
                    let snapshot = store.load_snapshot(&mut MemoryTx::auto_commit(), key).unwrap();
                    assert_eq!(
                        snapshot.map(|s| s.version),
                        Some(1),
                        "{}: the snapshot under the same key",
                        c.name
                    );
                    let reloaded = r.load(c.tenant, ID).unwrap();
                    assert_eq!(
                        (reloaded.balance, reloaded.root().version()),
                        (10, 1),
                        "{}: the load reads it back",
                        c.name
                    );
                }
                (Err(error), Err(code)) => {
                    assert_eq!(error.code, code, "{}: {error}", c.name);
                    assert!(
                        store.unpublished(10).unwrap().is_empty(),
                        "{}: refused before any write",
                        c.name
                    );
                }
                (got, want) => panic!("{}: got {:?}, want {want:?}", c.name, got.map(|(_, events)| events)),
            }
        }
    }

    #[test]
    fn a_pool_keeps_two_tenants_streams_apart_inside_the_callers_transaction() {
        // One store, one aggregate id, two tenants: two streams, each at its own version, through the
        // `_in` path a handler takes inside its own unit of work.
        let store = MemoryEventStore::new();
        let r = repo(store.clone()).tenancy(Tenancy::Pool);
        for (tenant, amount) in [("acme", 10), ("globex", 70)] {
            store
                .transaction(|tx| {
                    let mut a = r.load_in(tx, Some(tenant), "order-1")?;
                    a.apply(AccountEvent::Deposited(amount));
                    r.save_in(tx, &mut a, SaveOptions::default())
                })
                .unwrap();
        }
        for (tenant, amount) in [("acme", 10), ("globex", 70)] {
            let a = r.load(Some(tenant), "order-1").unwrap();
            assert_eq!(
                (a.balance, a.root().version(), a.root().tenant()),
                (amount, 1, Some(tenant))
            );
        }
    }

    #[test]
    fn snapshot_every_writes_one_where_a_save_crosses_its_interval() {
        // Two saves of one event each: the snapshot left behind, per interval. `1` takes both saves,
        // `2` the second, `3` neither, and `0` never snapshots.
        for (every, want) in [(1, Some(2)), (2, Some(2)), (3, None), (0, None)] {
            let store = MemoryEventStore::new();
            let r = repo(store.clone()).snapshot_every(every);
            let mut a = Account::open("acct-9");
            for amount in [10, 5] {
                a.apply(AccountEvent::Deposited(amount));
                r.save(&mut a, SaveOptions::default()).unwrap();
            }
            let snapshot = store.load_snapshot(&mut MemoryTx::auto_commit(), "acct-9").unwrap();
            assert_eq!(snapshot.map(|s| s.version), want, "every {every}");
            assert_eq!(
                r.load(None, "acct-9").unwrap().balance,
                15,
                "every {every}: the load replays what the snapshot lacks"
            );
        }
    }

    #[test]
    fn load_pure_replay_no_snapshot() {
        let store = MemoryEventStore::new();
        // Seed events through the public append (no snapshot) so load() must pure-replay.
        store
            .append(
                &mut MemoryTx::auto_commit(),
                "acct-3",
                0,
                vec![deposited("acct-3", 1, b"7"), deposited("acct-3", 2, b"3")],
            )
            .unwrap();
        let a = repo(store).load(None, "acct-3").unwrap();
        assert_eq!(a.balance, 10);
        assert_eq!(a.root().version(), 2);
    }

    #[test]
    fn empty_save_appends_nothing_and_still_checks_the_version() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-6");
        a.apply(AccountEvent::Deposited(4));
        r.save(&mut a, SaveOptions::default()).unwrap();

        let envs = r.save(&mut a, SaveOptions::default()).unwrap(); // zero-iteration marshal loop
        assert!(envs.is_empty());
        assert_eq!((a.root().version(), a.root().pending().len()), (1, 0));
        assert_eq!(
            store.load(&mut MemoryTx::auto_commit(), "acct-6", 0).unwrap().len(),
            1,
            "nothing appended"
        );
        assert_eq!(store.unpublished(10).unwrap().len(), 1, "nothing enqueued");

        // The empty append is still a compare-and-set, so a stale caller that applied nothing is told
        // so — and its snapshot write is short-circuited rather than rolling the stored one backwards.
        let mut stale = r.load(None, "acct-6").unwrap(); // at version 1
        let mut winner = r.load(None, "acct-6").unwrap();
        winner.apply(AccountEvent::Deposited(6));
        r.save(&mut winner, SaveOptions::default()).unwrap(); // store -> version 2, balance 10
        let got = r.save(&mut stale, SaveOptions::default());
        assert_eq!(got.err(), Some(Error::concurrency(1, 2)));
        let snap = store
            .load_snapshot(&mut MemoryTx::auto_commit(), "acct-6")
            .unwrap()
            .expect("the winner's snapshot");
        assert_eq!((snap.version, snap.data), (2, b"10".to_vec()));
    }

    #[test]
    fn load_replays_events_after_the_snapshot() {
        // Snapshot at v1, then two events land behind it (another writer): load must start from the
        // snapshot and fold only the tail, ending at the tail's last sequence.
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-7");
        a.apply(AccountEvent::Deposited(10));
        r.save(&mut a, SaveOptions::default()).unwrap(); // snapshot at v1, balance 10

        store
            .append(
                &mut MemoryTx::auto_commit(),
                "acct-7",
                1,
                vec![deposited("acct-7", 2, b"5"), deposited("acct-7", 3, b"7")],
            )
            .unwrap();

        let b = r.load(None, "acct-7").unwrap();
        assert_eq!(b.balance, 22);
        assert_eq!((b.root().version(), b.root().pending().len()), (3, 0));
    }

    #[test]
    fn load_surfaces_a_codec_error_from_replay() {
        let store = MemoryEventStore::new();
        store
            .append(
                &mut MemoryTx::auto_commit(),
                "acct-8",
                0,
                vec![deposited("acct-8", 1, b"not a number")],
            )
            .unwrap();
        let got = repo(store).load(None, "acct-8");
        assert!(
            matches!(
                got,
                Err(Error {
                    code: ErrorCode::Codec,
                    ..
                })
            ),
            "got {:?}",
            got.err()
        );
    }

    #[test]
    fn stale_expected_version_conflicts() {
        let store = MemoryEventStore::new();
        let r = repo(store.clone());
        let mut a = Account::open("acct-4");
        a.apply(AccountEvent::Deposited(1));
        r.save(&mut a, SaveOptions::default()).unwrap();

        let mut stale = r.load(None, "acct-4").unwrap(); // initial 1
        let mut other = r.load(None, "acct-4").unwrap();
        other.apply(AccountEvent::Deposited(2));
        r.save(&mut other, SaveOptions::default()).unwrap(); // store now at version 2

        stale.apply(AccountEvent::Deposited(3));
        match r.save(&mut stale, SaveOptions::default()) {
            Err(e) if e == Error::concurrency(1, 2) => {}
            other => panic!("expected concurrency error, got {:?}", other.err()),
        }

        // The rejected append short-circuits the snapshot write: the stored snapshot is still the
        // winner's (version 2, balance 3), never the loser's state — a snapshot may lag, never lead.
        let snap = store
            .load_snapshot(&mut MemoryTx::auto_commit(), "acct-4")
            .unwrap()
            .expect("winner's snapshot");
        assert_eq!((snap.version, snap.data), (2, b"3".to_vec()));
        assert_eq!(
            store.load(&mut MemoryTx::auto_commit(), "acct-4", 0).unwrap().len(),
            2,
            "nothing appended by the loser"
        );
    }

    // --- a domain whose codecs genuinely fail ---

    /// A posting keyed by `(account, leg)`. `serde_json` cannot write a non-string map key, so an
    /// event carrying entries really fails to marshal and a memo posting (`entries` empty) really
    /// succeeds — no test double pretends to fail.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct Posted {
        reference: String,
        entries: BTreeMap<(String, u8), i64>,
    }

    struct Ledger {
        root: Root<Posted>,
        balances: BTreeMap<(String, u8), i64>,
    }

    impl Aggregate for Ledger {
        type Event = Posted;
        fn root(&self) -> &Root<Self::Event> {
            &self.root
        }
        fn root_mut(&mut self) -> &mut Root<Self::Event> {
            &mut self.root
        }
        fn transition(&mut self, event: &Self::Event) {
            for (account, minor_units) in &event.entries {
                *self.balances.entry(account.clone()).or_default() += minor_units;
            }
        }
    }

    /// Snapshots the balances as JSON, so the snapshot seam fails on exactly the keys the event
    /// seam fails on.
    struct LedgerCodec;

    impl AggregateCodec for LedgerCodec {
        type Aggregate = Ledger;
        fn type_name(&self) -> &str {
            "Ledger"
        }
        fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Ledger, Error> {
            let balances = match data {
                Some(d) => serde_json::from_slice(d).map_err(Error::codec)?,
                None => BTreeMap::new(),
            };
            Ok(Ledger {
                root: Root::new(id, version),
                balances,
            })
        }
        fn dump(&self, agg: &Ledger) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(&agg.balances).map_err(Error::codec)
        }
    }

    #[test]
    fn save_reports_codec_failures_and_appends_nothing() {
        /// One posting line: account, leg, minor units.
        type Entry = (&'static str, u8, i64);

        fn posted(reference: &str, entries: &[Entry]) -> Posted {
            let entries = entries.iter().map(|(a, leg, n)| ((a.to_string(), *leg), *n)).collect();
            Posted {
                reference: reference.to_string(),
                entries,
            }
        }

        struct Case {
            name: &'static str,
            /// Opening balances, which drive the snapshot codec.
            opening: &'static [Entry],
            /// Entries per applied event, which drive the event codec.
            events: &'static [&'static [Entry]],
            /// The sequences committed on success, or the message the error must carry.
            want: Result<&'static [u64], &'static str>,
        }
        let cases = [
            Case {
                name: "every seam writes: the batch commits",
                opening: &[],
                events: &[&[], &[]],
                want: Ok(&[1, 2]),
            },
            Case {
                name: "the only event fails to marshal",
                opening: &[],
                events: &[&[("cash", 0, 1_250)]],
                want: Err("codec: key must be a string"),
            },
            Case {
                name: "the third of three fails: no partial batch reaches the store",
                opening: &[],
                events: &[&[], &[], &[("cash", 0, 1_250)]],
                want: Err("codec: key must be a string"),
            },
            Case {
                name: "every event marshals but the snapshot does not",
                opening: &[("cash", 0, 1_250)],
                events: &[&[]],
                want: Err("codec: key must be a string"),
            },
        ];

        for (i, c) in cases.into_iter().enumerate() {
            let id = format!("ledger-{i}");
            let store = MemoryEventStore::new();
            let repo =
                Repository::new(store.clone(), JsonCodec::<Posted>::new("Posted"), LedgerCodec).with_ids(next_id);
            let mut ledger = Ledger {
                root: Root::new(&id, 0),
                balances: posted("opening", c.opening).entries,
            };
            for (n, entries) in c.events.iter().enumerate() {
                ledger.apply(posted(&format!("inv-{n}"), entries));
            }

            let got = repo.save(&mut ledger, SaveOptions::default());
            let appended = store.load(&mut MemoryTx::auto_commit(), &id, 0).unwrap();
            let snapshot = store.load_snapshot(&mut MemoryTx::auto_commit(), &id).unwrap();
            match c.want {
                Ok(sequences) => {
                    let events = got.unwrap_or_else(|e| panic!("{}: want Ok, got {e:?}", c.name));
                    let committed: Vec<u64> = events.iter().filter_map(|e| e.sequence).collect();
                    assert_eq!(committed, sequences, "{}", c.name);
                    assert_eq!(appended.len(), sequences.len(), "{}", c.name);
                    assert_eq!(store.unpublished(10).unwrap().len(), sequences.len(), "{}", c.name);
                    assert_eq!(snapshot.map(|s| s.data), Some(b"{}".to_vec()), "{}", c.name);
                    assert!(ledger.root().pending().is_empty(), "{}", c.name);
                    assert_eq!(ledger.root().version(), sequences.len() as u64, "{}", c.name);
                }
                Err(message) => {
                    assert!(
                        matches!(
                            &got,
                            Err(Error {
                                code: ErrorCode::Codec,
                                ..
                            })
                        ),
                        "{}: got {:?}",
                        c.name,
                        got.as_ref().err()
                    );
                    assert_eq!(got.err().map(|e| e.to_string()).as_deref(), Some(message), "{}", c.name);
                    assert!(appended.is_empty(), "{}: appended {appended:?}", c.name);
                    assert!(
                        store.unpublished(10).unwrap().is_empty(),
                        "{}: outbox not empty",
                        c.name
                    );
                    assert!(snapshot.is_none(), "{}: a snapshot was written", c.name);
                    // The aggregate is untouched, so the caller still owns the events it applied.
                    assert_eq!(ledger.root().pending().len(), c.events.len(), "{}", c.name);
                }
            }
        }
    }

    // --- replay ---

    /// The event store, with `load` replaced by a fixed answer: a third-party store that breaks the
    /// gap-free promise.
    struct Lying {
        inner: MemoryEventStore,
        /// Answered verbatim instead of the stored stream, when set.
        load: Option<Vec<EventMessage>>,
    }

    impl Transactor for Lying {
        type Tx = MemoryTx;
        fn identity(&self) -> usize {
            self.inner.identity()
        }
        fn begin(&self) -> Result<MemoryTx, Error> {
            self.inner.begin()
        }
        fn commit(&self, tx: MemoryTx) -> Result<(), Error> {
            self.inner.commit(tx)
        }
        fn rollback(&self, tx: MemoryTx) {
            self.inner.rollback(tx);
        }
    }

    impl EventStore for Lying {
        fn load(&self, tx: &mut MemoryTx, id: &str, after: u64) -> Result<Vec<EventMessage>, Error> {
            match &self.load {
                Some(answer) => Ok(answer.clone()),
                None => self.inner.load(tx, id, after),
            }
        }
        fn load_snapshot(&self, tx: &mut MemoryTx, id: &str) -> Result<Option<Snapshot>, Error> {
            self.inner.load_snapshot(tx, id)
        }
        fn save_snapshot(&self, tx: &mut MemoryTx, snapshot: &Snapshot) -> Result<(), Error> {
            self.inner.save_snapshot(tx, snapshot)
        }
        fn append(
            &self,
            tx: &mut MemoryTx,
            id: &str,
            expected: u64,
            drafts: Vec<EventDraft>,
        ) -> Result<Vec<EventMessage>, Error> {
            self.inner.append(tx, id, expected, drafts)
        }
    }

    #[test]
    fn replay_branches() {
        struct Case {
            name: &'static str,
            /// The snapshot stored for the account, if any: `(type_name, version, data)`.
            snapshot: Option<(&'static str, u64, &'static [u8])>,
            /// What `load` answers, as `(sequence, amount)`; the stored stream is three deposits of 1.
            load: Option<&'static [(u64, &'static [u8])]>,
            want: Result<(i64, u64), &'static str>,
        }
        let cases = [
            Case {
                name: "no snapshot: replay from 0",
                snapshot: None,
                load: None,
                want: Ok((3, 3)),
            },
            Case {
                name: "own snapshot: replay the tail",
                snapshot: Some(("Account", 2, b"40")),
                load: None,
                want: Ok((41, 3)),
            },
            Case {
                name: "another aggregate type's snapshot is ignored",
                snapshot: Some(("Ledger", 2, b"40")),
                load: None,
                want: Ok((3, 3)),
            },
            Case {
                name: "a snapshot that does not load is ignored",
                snapshot: Some(("Account", 2, b"not a number")),
                load: None,
                want: Ok((3, 3)),
            },
            Case {
                name: "a store that returns a gap fails the load",
                snapshot: None,
                load: Some(&[(1, b"1"), (3, b"1")]),
                want: Err("codec: event 1 has sequence 3, expected 2"),
            },
            Case {
                name: "a store that repeats an event fails the load",
                snapshot: None,
                load: Some(&[(1, b"1"), (1, b"1")]),
                want: Err("codec: event 1 has sequence 1, expected 2"),
            },
            Case {
                name: "an unsequenced event fails the load",
                snapshot: None,
                load: Some(&[(0, b"1")]),
                want: Err("codec: event 0 has no sequence"),
            },
        ];
        for c in cases {
            let load = c.load.map(|rows| {
                rows.iter()
                    .map(|&(seq, data)| EventMessage {
                        sequence: (seq > 0).then_some(seq),
                        ..deposited("acct-s", seq, data).record(RECORDED_AT)
                    })
                    .collect()
            });
            let store = Lying {
                inner: MemoryEventStore::new(),
                load,
            };
            let stream: Vec<EventDraft> = (1..=3).map(|seq| deposited("acct-s", seq, b"1")).collect();
            let ids: Vec<String> = stream.iter().map(|e| e.id.clone()).collect();
            store.append(&mut MemoryTx::auto_commit(), "acct-s", 0, stream).unwrap();
            if let Some((type_name, version, data)) = c.snapshot {
                let snapshot = Snapshot {
                    aggregate_id: "acct-s".into(),
                    type_name: type_name.into(),
                    version,
                    data: data.to_vec(),
                };
                store.save_snapshot(&mut MemoryTx::auto_commit(), &snapshot).unwrap();
            }
            // The outbox is untouched by a load: the three appended rows still wait, and marking clears them.
            assert_eq!(Outbox::unpublished(&store.inner, 10).unwrap().len(), 3, "{}", c.name);
            store.inner.mark_published(&ids).unwrap();
            let repo = Repository::new(store, AccountEvents, AccountCodec).with_ids(next_id);
            let got = repo
                .load(None, "acct-s")
                .map(|a| (a.balance, a.root().version()))
                .map_err(|e| e.to_string());
            assert_eq!(got, c.want.map_err(str::to_owned), "{}", c.name);
        }
    }
}
