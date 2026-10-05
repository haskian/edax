//! Causation / correlation propagation: the rule that links messages into a traceable graph, agreed
//! by every binding so a chain spanning services (one emits, another consumes and emits the next)
//! stays connected.
//!
//! Every message carries two typed ids, its [`Lineage`]:
//!
//! - `correlation_id` — constant for every message in one logical flow (a request, a saga run).
//! - `causation_id` — the id of the single message that directly caused this one.
//!
//! When a parent message causes a child, [`propagate`] computes the child's lineage: its causation is
//! the parent's id and it inherits the parent's correlation. A message with no parent starts a chain;
//! [`originate`] computes its lineage. Every event one save writes shares one lineage: the cause of
//! all of them is the same message, never the event saved beside it.

use serde::{Deserialize, Serialize};

/// The two ids that place a message in its flow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    /// Constant across one logical flow.
    pub correlation_id: String,
    /// The id of the message that directly caused this one; a root's own correlation id.
    pub causation_id: String,
}

/// The lineage of a child of the message `parent_id` whose correlation is `parent_correlation_id`:
/// the child's causation is the parent's id, and it inherits the parent's correlation.
pub fn propagate(parent_id: &str, parent_correlation_id: &str) -> Lineage {
    Lineage {
        correlation_id: parent_correlation_id.to_owned(),
        causation_id: parent_id.to_owned(),
    }
}

/// The lineage of a message with no parent message, given the ids the caller already holds (from an
/// inbound request, say) and one freshly minted id.
///
/// Fills only what is missing; a supplied id is never overwritten. `correlation_id` defaults to
/// `fresh_id` (this message roots a new flow) and `causation_id` defaults to the resulting
/// `correlation_id` (a root causes itself). An empty string is a supplied id, whatever its
/// truthiness in a host language.
///
/// `fresh_id` comes from the caller because minting an id is non-deterministic. Keeping that out
/// leaves this function pure, so one golden vector pins the rule for every binding. A root's
/// causation points at the flow it roots, never at `fresh_id` as such: `fresh_id` is not a message id
/// and must never be claimed as one.
pub fn originate(correlation_id: Option<&str>, causation_id: Option<&str>, fresh_id: &str) -> Lineage {
    // `correlation_id` alone decides whether the caller is already on a chain: a lone
    // `causation_id` does not suppress the fresh correlation id.
    let correlation_id = correlation_id.unwrap_or(fresh_id).to_owned();
    let causation_id = causation_id.map_or_else(|| correlation_id.clone(), str::to_owned);
    Lineage {
        correlation_id,
        causation_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_root_plus_one_hop_is_a_connected_chain() {
        let root = originate(None, None, "corr-1");
        let child = propagate("evt-root", &root.correlation_id);
        assert_eq!(root.causation_id, root.correlation_id); // the root causes itself
        assert_eq!(child.correlation_id, root.correlation_id); // one flow across the hop
        assert_eq!(child.causation_id, "evt-root"); // points back at the message that caused it
    }
}
