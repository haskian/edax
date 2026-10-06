//! A tiny event-sourced domain every suite of the SDK shares, its unit suites and its integration
//! suites alike: an account that takes deposits, its two codecs, and a repository over the in-memory
//! store.

use std::sync::atomic::{AtomicU64, Ordering};

use edax_event::{EventDraft, Payload};

use super::draft;
use crate::{Aggregate, AggregateCodec, Error, EventCodec, MemoryEventStore, Repository, Root};

/// The one thing an account records.
#[derive(Clone)]
pub enum AccountEvent {
    Deposited(i64),
}

/// An account: its root and the balance its deposits sum to.
pub struct Account {
    pub root: Root<AccountEvent>,
    pub balance: i64,
}

impl Account {
    /// A new account `id`, at version 0 with no balance.
    pub fn open(id: &str) -> Self {
        Self {
            root: Root::new(id, 0),
            balance: 0,
        }
    }
}

impl Aggregate for Account {
    type Event = AccountEvent;
    fn root(&self) -> &Root<Self::Event> {
        &self.root
    }
    fn root_mut(&mut self) -> &mut Root<Self::Event> {
        &mut self.root
    }
    fn transition(&mut self, event: &Self::Event) {
        match event {
            AccountEvent::Deposited(n) => self.balance += n,
        }
    }
}

/// The event codec: a deposit's amount as its decimal text.
pub struct AccountEvents;

impl EventCodec for AccountEvents {
    type Domain = AccountEvent;
    fn marshal(&self, e: &AccountEvent) -> Result<Payload, Error> {
        let AccountEvent::Deposited(n) = e;
        Ok(Payload {
            name: "Deposited".into(),
            version: 1,
            data: n.to_string().into_bytes(),
        })
    }
    fn unmarshal(&self, raw: &Payload) -> Result<AccountEvent, Error> {
        let s = std::str::from_utf8(&raw.data).map_err(Error::codec)?;
        let n = s.parse::<i64>().map_err(Error::codec)?;
        Ok(AccountEvent::Deposited(n))
    }
}

/// The aggregate codec: a snapshot is the balance as decimal text.
pub struct AccountCodec;

impl AggregateCodec for AccountCodec {
    type Aggregate = Account;
    fn type_name(&self) -> &str {
        "Account"
    }
    fn load(&self, id: &str, version: u64, data: Option<&[u8]>) -> Result<Account, Error> {
        let mut a = Account {
            root: Root::new(id, version),
            balance: 0,
        };
        if let Some(d) = data {
            let s = std::str::from_utf8(d).map_err(Error::codec)?;
            a.balance = s.parse().map_err(Error::codec)?;
        }
        Ok(a)
    }
    fn dump(&self, agg: &Account) -> Result<Vec<u8>, Error> {
        Ok(agg.balance.to_string().into_bytes())
    }
}

/// A fresh event id per call, unique within the process.
pub fn next_id() -> String {
    static N: AtomicU64 = AtomicU64::new(1);
    format!("e{}", N.fetch_add(1, Ordering::Relaxed))
}

/// A repository of accounts over `store`, minting ids with [`next_id`].
pub fn repo(store: MemoryEventStore) -> Repository<MemoryEventStore, AccountEvents, AccountCodec> {
    Repository::new(store, AccountEvents, AccountCodec).with_ids(next_id)
}

/// A `Deposited` draft as another writer would append it behind the repository's back.
pub fn deposited(aggregate_id: &str, sequence: u64, data: &[u8]) -> EventDraft {
    draft(aggregate_id, "Deposited", sequence, data)
}
