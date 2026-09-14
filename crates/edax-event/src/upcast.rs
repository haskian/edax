//! Versioned-event upcast planning: the order in which schema-migration steps run, and the gap
//! check that stops a missing step from silently corrupting rebuilt state.
//!
//! An event payload is stored at some `version`. To raise it to the latest known version, the host
//! applies a chain of single-step transforms: step `v` upgrades a `v` payload to `v+1`. The
//! transform functions themselves are domain code and live in the bindings — but *which* steps run
//! and *in what order* is a consistency decision: if one binding skipped a step, or ran them out of
//! order, two services replaying the same stored events would rebuild different state.
//!
//! So the kernel owns the plan only: [`upcast_plan`] returns the ordered step versions
//! `[from, from+1, ..., to-1]`, erroring if any step in that span is absent (a gap) or if
//! `from > to`. `from == to` yields an empty plan (already current).

use std::collections::BTreeSet;

/// Ordered upcast steps to raise a payload from version `from` to version `to`, given the step
/// versions that have a registered transform in `available` (a step `v` upgrades `v` -> `v+1`).
///
/// Returns `[from, from+1, ..., to-1]`, or an error naming the first missing step (a gap) — applying
/// a chain with a gap would skip a migration and desync state. `from == to` -> empty plan;
/// `from > to` is an error (downgrades are not modeled).
pub fn upcast_plan(from: i32, to: i32, available: &[i32]) -> Result<Vec<i32>, String> {
    if from > to {
        return Err(format!("cannot upcast downward: from {from} > to {to}"));
    }
    let have: BTreeSet<i32> = available.iter().copied().collect();
    if let Some(step) = (from..to).find(|step| !have.contains(step)) {
        return Err(format!(
            "missing upcast step {step} (no transform for v{step} -> v{})",
            step + 1
        ));
    }
    Ok((from..to).collect())
}

/// Boundary helper for the bindings: `available` is a JSON array of integers; returns the plan as a
/// JSON array of step versions (or a flat error string).
pub fn upcast_plan_json(from: i32, to: i32, available: &[u8]) -> Result<Vec<u8>, String> {
    let steps: Vec<i32> = serde_json::from_slice(available).map_err(|e| e.to_string())?;
    let plan = upcast_plan(from, to, &steps)?;
    serde_json::to_vec(&plan).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_chain_and_noop() {
        assert_eq!(upcast_plan(1, 1, &[]).unwrap(), Vec::<i32>::new());
        assert_eq!(upcast_plan(1, 3, &[1, 2]).unwrap(), vec![1, 2]);
        assert_eq!(upcast_plan(2, 5, &[2, 3, 4, 9]).unwrap(), vec![2, 3, 4]); // extra step ignored
    }

    #[test]
    fn gap_and_downgrade_error() {
        assert!(upcast_plan(1, 3, &[1]).is_err()); // missing step 2
        assert!(upcast_plan(1, 3, &[2]).is_err()); // missing step 1
        assert!(upcast_plan(3, 2, &[2]).is_err()); // downgrade
    }

    #[test]
    fn json_boundary() {
        assert_eq!(upcast_plan_json(1, 3, b"[1,2]").unwrap(), b"[1,2]");
        assert!(upcast_plan_json(1, 3, b"[1]").is_err());
        assert!(upcast_plan_json(1, 3, b"not json").is_err());
    }
}
