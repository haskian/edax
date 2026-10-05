//! The rules every idempotency ledger applies, whatever backs it: what a claim finds, and the fence
//! a settling write passes.
//!
//! A ledger store hands a claimant a handle that names its claim, and every later write of that
//! claimant acts only while the key still holds that claim. The memory store calls [`complete_fence`]
//! and [`release_fence`] directly. A Redis ledger cannot call Rust from its Lua, so
//! `testdata/golden/idempotency_fence.json` pins the same table, and both the memory store and the
//! Redis scripts replay it.

use edax_core::Error;

use crate::record::lapsed_claim;

/// What an idempotency claim found. `#[must_use]` and exhaustive: a host that ignores
/// [`Claimed::Won`] never settles its claim, and every variant needs a decision.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claimed {
    /// This caller holds the claim: run the handler, then settle the claim with this handle. The
    /// handle is opaque bytes to the caller; the store that minted it compares it exactly.
    Won(Vec<u8>),
    /// Another claim or a settled record holds the key: its record bytes, which the host reads with
    /// [`decode_prior`](crate::decode_prior).
    Prior(Vec<u8>),
}

edax_core::closed_names! {
    /// What a key holds, as one claimant's settling write sees it. The store classifies its own value;
    /// the fence decides from the class alone, so every store fences alike.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Holder {
        /// The key holds nothing: the claim lapsed, or it was released.
        Free = "free",
        /// The key holds the claim the caller's handle names.
        OwnClaim = "own_claim",
        /// The key holds a claim another handle names: this one lapsed and another took the key over.
        OtherClaim = "other_claim",
        /// The key holds the very record the write settles with: a `complete` that landed, re-sent after
        /// its reply was lost.
        SameRecord = "same_record",
        /// The key holds any other settled record, a value no reader decodes included.
        OtherRecord = "other_record",
    }
}

/// What a fenced write does to the key. `#[must_use]`: a dropped verdict is a write that never lands.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fence {
    /// Perform the write the call names: store the settled record, or delete the claim.
    Apply,
    /// Leave the key as it is.
    Leave,
}

/// The fence on a `complete` of `key`, from what the key holds. The caller's own claim is replaced by
/// the settled record. The same record is already there: a re-sent `complete` succeeds and changes
/// nothing. Anything else means the claim lapsed, so the outcome was not recorded:
/// [`lapsed_claim`], a `store` error, and the caller must not report success.
pub fn complete_fence(key: &str, holder: Holder) -> Result<Fence, Error> {
    match holder {
        Holder::OwnClaim => Ok(Fence::Apply),
        Holder::SameRecord => Ok(Fence::Leave),
        Holder::Free | Holder::OtherClaim | Holder::OtherRecord => Err(lapsed_claim(key)),
    }
}

/// The fence on a `release`, from what the key holds. Only the caller's own claim is deleted. A
/// settled record stays, even this claim's own, whose `complete` landed with its reply lost: it is
/// the outcome of committed work. A claim that took a lapsed one over stays too. A release never
/// fails: the guard sends it as compensation, and there is nothing left to compensate.
pub fn release_fence(holder: Holder) -> Fence {
    match holder {
        Holder::OwnClaim => Fence::Apply,
        Holder::Free | Holder::OtherClaim | Holder::SameRecord | Holder::OtherRecord => Fence::Leave,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_branches() {
        let lapsed = || Err(lapsed_claim("order-7"));
        let cases: [(Holder, Result<Fence, Error>, Fence); 5] = [
            (Holder::Free, lapsed(), Fence::Leave),
            (Holder::OwnClaim, Ok(Fence::Apply), Fence::Apply),
            (Holder::OtherClaim, lapsed(), Fence::Leave),
            (Holder::SameRecord, Ok(Fence::Leave), Fence::Leave),
            (Holder::OtherRecord, lapsed(), Fence::Leave),
        ];
        for (holder, complete, release) in cases {
            assert_eq!(
                complete_fence("order-7", holder),
                complete,
                "complete over {}",
                holder.as_str()
            );
            assert_eq!(release_fence(holder), release, "release over {}", holder.as_str());
        }
    }
}
