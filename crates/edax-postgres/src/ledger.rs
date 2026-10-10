//! The idempotency ledger on Postgres: the claim owner, what a row is to a claimant, and the fence on
//! each settling write.
//!
//! A row holds the kernel's record text, the `owner` while it is a pending claim, and the `settled_by`
//! owner once a claim settled it. The handle a claimant gets is its owner token, so a settling write acts
//! only while the row still holds that claim: the same fence every ledger runs
//! ([`edax_cqrs::complete_fence`], [`edax_cqrs::release_fence`]), decided here from the row the
//! `ledger_hold` statement locked.
//!
//! Inside a unit of work a pending claim never commits: the claim and its record commit together, or
//! roll back together. Only a claim made outside one commits pending, and it lapses after the claim TTL
//! ([`crate::Settings::claim_ttl_ms`]), so a crashed claimant does not hold its key forever. That mode is
//! at-least-once: a handler slower than the TTL can run twice.

use edax_core::Error;
use edax_cqrs::{complete_fence, release_fence, Fence, Holder, Record};
use serde::Deserialize;
use serde_json::Value;

use crate::config::CLAIM_READS;
use crate::rows::{text_param, Param};

/// A ledger row, as `claim_read` and `ledger_hold` return it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LedgerRow {
    /// The owner token of the pending claim, `None` once settled.
    pub owner: Option<String>,
    /// The owner token of the claim that settled the row, `None` while pending or for a row no claim
    /// wrote.
    pub settled_by: Option<String>,
    /// The kernel's record text.
    pub record: String,
    /// Whether a pending claim is past its TTL (`claim_read` only).
    #[serde(default)]
    pub lapsed: bool,
}

/// The ledger row a statement returned.
pub fn decode_row(row: &str) -> Result<LedgerRow, Error> {
    serde_json::from_str(row).map_err(|e| Error::codec(format_args!("ledger row is not the stored layout: {e}")))
}

/// A fresh owner token for a claim: also its handle.
pub fn new_owner() -> String {
    edax_core::guid_simple()
}

/// The parameters of `claim_insert` for `key`, claimed by `owner` for `payload_hash`: the key, the
/// owner and the pending record. `claim_take_over` binds them too, then the lapsed owner. A key holding
/// NUL is a `codec` error.
pub fn claim_params(key: &str, owner: &str, payload_hash: u64) -> Result<Vec<Param>, Error> {
    Ok(vec![
        text_param("idempotency key", key)?,
        text_param("claim owner", owner)?,
        Some(pending_record(payload_hash)?),
    ])
}

/// The parameters of `claim_read`: the key, and the claim TTL in milliseconds.
pub fn claim_read_params(key: &str, claim_ttl_ms: i64) -> Result<Vec<Param>, Error> {
    Ok(vec![
        text_param("idempotency key", key)?,
        Some(claim_ttl_ms.to_string()),
    ])
}

/// The parameters of `ledger_hold` and `ledger_delete`: the key.
pub fn key_params(key: &str) -> Result<Vec<Param>, Error> {
    Ok(vec![text_param("idempotency key", key)?])
}

/// The parameters of `ledger_settle`: the key, the settling claim's owner (its `handle`) and the record.
/// A handle that is not an owner token's text is a `bad_handle` error.
pub fn settle_params(key: &str, handle: &[u8], record: &str) -> Result<Vec<Param>, Error> {
    let owner = std::str::from_utf8(handle)
        .map_err(|_| Error::bad_handle("a postgres claim handle is the text of its owner token"))?;
    Ok(vec![
        text_param("idempotency key", key)?,
        text_param("claim owner", owner)?,
        text_param("record", record)?,
    ])
}

/// The pending record a claim inserts.
pub fn pending_record(payload_hash: u64) -> Result<String, Error> {
    let bytes = edax_cqrs::encode_record(&Record::<Value>::pending(payload_hash))?;
    String::from_utf8(bytes).map_err(Error::codec)
}

edax_core::closed_names! {
    /// What a claim that lost the insert does with the row it then read, by name: [`ClaimRead`]'s variants.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClaimAction {
        /// The key holds a record: [`ClaimRead::Prior`].
        Prior = "prior",
        /// The key holds a lapsed claim: [`ClaimRead::TakeOver`].
        TakeOver = "take_over",
        /// The row was gone: [`ClaimRead::Vanished`].
        Vanished = "vanished",
    }
}

/// What a claim that lost the insert does with the row it then read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimRead {
    /// The key holds this record: the claimant's prior, for `edax_cqrs::decode_prior`.
    Prior(String),
    /// The key holds a pending claim past its TTL: take it over from this owner (`claim_take_over`).
    TakeOver(String),
    /// The row was gone by the read: claim again.
    Vanished,
}

impl ClaimRead {
    /// This answer's name.
    pub fn action(&self) -> ClaimAction {
        match self {
            ClaimRead::Prior(_) => ClaimAction::Prior,
            ClaimRead::TakeOver(_) => ClaimAction::TakeOver,
            ClaimRead::Vanished => ClaimAction::Vanished,
        }
    }
}

/// What a claim of `key` that lost its insert does with `row`, the row its `attempt`-th `claim_read`
/// returned (`None` when none). A claim that lost the take-over of a lapsed claim asks again with `None`:
/// the row it read is gone. A row gone on the last of [`CLAIM_READS`] attempts is the transient `store`
/// error: the key kept changing under the claim.
pub fn claim_read(key: &str, row: Option<&LedgerRow>, attempt: u32) -> Result<ClaimRead, Error> {
    Ok(match row {
        None if attempt >= CLAIM_READS => {
            return Err(Error::store(format_args!(
                "postgres claim of {key:?}: the key kept changing under {CLAIM_READS} reads"
            )))
        }
        None => ClaimRead::Vanished,
        Some(LedgerRow {
            owner: Some(owner),
            lapsed: true,
            ..
        }) => ClaimRead::TakeOver(owner.clone()),
        Some(row) => ClaimRead::Prior(row.record.clone()),
    })
}

/// What `row` is to the claimant whose handle is `handle`, settling with `settling` (`None` for a
/// release): the class the kernel's fence decides from. A settled row is this claimant's own only when
/// this claim settled it with the same record.
fn holder(row: Option<&LedgerRow>, handle: &[u8], settling: Option<&str>) -> Holder {
    let Some(row) = row else { return Holder::Free };
    let own = |owner: &str| owner.as_bytes() == handle;
    match (&row.owner, &row.settled_by) {
        (Some(owner), _) if own(owner) => Holder::OwnClaim,
        (Some(_), _) => Holder::OtherClaim,
        (None, Some(by)) if own(by) && Some(row.record.as_str()) == settling => Holder::SameRecord,
        (None, _) => Holder::OtherRecord,
    }
}

/// Whether a `complete` of `key` writes `record` (`ledger_settle`) over `row`, the row `ledger_hold`
/// locked: `true` over the caller's own claim, `false` over the very record it settles with (a re-sent
/// complete). Over anything else the claim lapsed: the kernel's `store` error. A pending `record` is the
/// kernel's `bad_handle` error, and the claim stays settleable.
pub fn complete_write(key: &str, row: Option<&LedgerRow>, handle: &[u8], record: &str) -> Result<bool, Error> {
    edax_cqrs::check_settled(record.as_bytes())?;
    Ok(complete_fence(key, holder(row, handle, Some(record)))? == Fence::Apply)
}

/// Whether a `release` deletes the key (`ledger_delete`): only while `row` is the caller's own claim.
pub fn release_write(row: Option<&LedgerRow>, handle: &[u8]) -> bool {
    release_fence(holder(row, handle, None)) == Fence::Apply
}

/// Whether a `forget` of `key` deletes `row` (`ledger_delete`): a settled record yes, nothing to delete
/// for an absent key, and a pending claim the kernel's `in_progress` error.
pub fn forget_write(key: &str, row: Option<&LedgerRow>) -> Result<bool, Error> {
    match row {
        None => Ok(false),
        Some(row) => edax_cqrs::forget(key, row.record.as_bytes()).map(|()| true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DONE: &str = r#"{"payload_hash":7,"result":1,"error":null,"error_code":null,"pending":false}"#;

    fn row(owner: Option<&str>, settled_by: Option<&str>, record: &str, lapsed: bool) -> LedgerRow {
        LedgerRow {
            owner: owner.map(str::to_owned),
            settled_by: settled_by.map(str::to_owned),
            record: record.to_owned(),
            lapsed,
        }
    }

    /// `idempotency_fence.json`, the table every ledger replays, through the Postgres holder.
    #[test]
    fn golden_idempotency_fence() {
        let pending = pending_record(7).unwrap();
        for case in edax_golden::read("idempotency_fence.json") {
            let name = case["name"].as_str().unwrap();
            let (key, me) = (case["key"].as_str().unwrap(), b"me".as_slice());
            let held = match case["holds"].as_str().unwrap() {
                "free" => None,
                "own_claim" => Some(row(Some("me"), None, &pending, false)),
                "other_claim" => Some(row(Some("other"), None, &pending, false)),
                "same_record" => Some(row(None, Some("me"), DONE, false)),
                "other_record" => Some(row(None, Some("other"), DONE, false)),
                other => panic!("{name}: unknown class {other}"),
            };
            let applied = match case["op"].as_str().unwrap() {
                "complete" => complete_write(key, held.as_ref(), me, DONE),
                "release" => Ok(release_write(held.as_ref(), me)),
                other => panic!("{name}: unknown op {other}"),
            };
            match case["want"].get("error") {
                Some(error) => assert_eq!(
                    applied.map_err(|e| e.to_string()).err().as_deref(),
                    error.as_str(),
                    "{name}"
                ),
                None => assert!(applied.is_ok(), "{name}: {applied:?}"),
            }
        }
    }

    #[test]
    fn claim_read_branches() {
        let pending = pending_record(7).unwrap();
        let lapsed = row(Some("o"), None, &pending, true);
        assert_eq!(claim_read("k", None, 1), Ok(ClaimRead::Vanished));
        assert_eq!(
            claim_read("k", None, CLAIM_READS).map_err(|e| e.to_string()),
            Err("store: postgres claim of \"k\": the key kept changing under 3 reads".into())
        );
        assert_eq!(
            claim_read("k", Some(&lapsed), CLAIM_READS),
            Ok(ClaimRead::TakeOver("o".into())),
            "a row still answers on the last read"
        );
        assert_eq!(
            claim_read("k", Some(&row(Some("o"), None, &pending, false)), 1),
            Ok(ClaimRead::Prior(pending.clone()))
        );
        assert_eq!(
            claim_read("k", Some(&row(None, Some("o"), DONE, true)), 1),
            Ok(ClaimRead::Prior(DONE.into())),
            "a settled row never lapses"
        );
        assert_eq!(ClaimRead::Vanished.action(), ClaimAction::Vanished);
    }

    #[test]
    fn settle_branches() {
        let pending = pending_record(7).unwrap();
        let own = row(Some("me"), None, &pending, false);
        assert_eq!(complete_write("k", Some(&own), b"me", DONE), Ok(true));
        assert!(
            complete_write("k", Some(&own), b"me", &pending).is_err(),
            "pending cannot settle"
        );
        assert_eq!(
            complete_write("k", Some(&row(None, Some("me"), DONE, false)), b"me", DONE),
            Ok(false)
        );
        assert!(release_write(Some(&own), b"me"));
        assert!(!release_write(Some(&row(None, Some("me"), DONE, false)), b"me"));
        assert_eq!(forget_write("k", None), Ok(false));
        assert_eq!(forget_write("k", Some(&row(None, Some("me"), DONE, false))), Ok(true));
        assert!(forget_write("k", Some(&own)).is_err(), "a pending claim is in progress");
        assert_eq!(
            forget_write("k", Some(&row(None, None, "garbage", false))),
            Ok(true),
            "an unreadable record goes"
        );
        assert!(claim_params("k\0", "o", 1).is_err());
        assert!(settle_params("k", &[0xff], DONE).is_err(), "a handle is owner text");
        assert_eq!(
            settle_params("k", b"me", DONE),
            Ok(vec![Some("k".into()), Some("me".into()), Some(DONE.into())])
        );
    }
}
