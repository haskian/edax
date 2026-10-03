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
//! `from > to`. `from == to` yields an empty plan (already current). A codec asks on *every*
//! unmarshal, also for a name with no transforms registered, so a payload from a newer writer is
//! refused the same way in every SDK instead of decoding silently under an older schema.

use std::collections::BTreeSet;

use edax_core::Error;

/// Ordered upcast steps to raise a payload from version `from` to version `to`.
///
/// `available` names the step versions that have a registered transform (a step `v` upgrades
/// `v` -> `v+1`), or is `None` when the event name has no transforms at all. `None` makes every
/// step the identity: the payload's shape never changed, so the plan is empty and only the
/// downgrade rule applies.
///
/// Returns `[from, from+1, ..., to-1]`, or an error naming the first missing step (a gap) — applying
/// a chain with a gap would skip a migration and desync state. `from == to` -> empty plan;
/// `from > to` is an error (downgrades are not modeled: the payload comes from a newer writer than
/// this reader). Both errors are `codec`: the event cannot be unmarshaled to the target version, and
/// no retry changes that.
pub fn upcast_plan(from: i32, to: i32, available: Option<&[i32]>) -> Result<Vec<i32>, Error> {
    if from > to {
        return Err(Error::codec(format_args!(
            "cannot upcast downward: from {from} > to {to} (the payload comes from a newer writer)"
        )));
    }
    let Some(available) = available else { return Ok(Vec::new()) };
    let have: BTreeSet<i32> = available.iter().copied().collect();
    if let Some(step) = (from..to).find(|step| !have.contains(step)) {
        return Err(Error::codec(format_args!(
            "missing upcast step {step} (no transform for v{step} -> v{})",
            step.saturating_add(1)
        )));
    }
    Ok((from..to).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upcast_plan_branches() {
        struct Case {
            name: &'static str,
            from: i32,
            to: i32,
            available: Option<&'static [i32]>,
            want: Result<Vec<i32>, &'static str>,
        }
        let cases = [
            Case { name: "already current", from: 1, to: 1, available: Some(&[]), want: Ok(vec![]) },
            Case { name: "contiguous chain", from: 1, to: 3, available: Some(&[1, 2]), want: Ok(vec![1, 2]) },
            Case { name: "extra step ignored", from: 2, to: 5, available: Some(&[2, 3, 4, 9]), want: Ok(vec![2, 3, 4]) },
            Case {
                name: "gap",
                from: 1,
                to: 3,
                available: Some(&[1]),
                want: Err("codec: missing upcast step 2 (no transform for v2 -> v3)"),
            },
            Case {
                name: "downgrade with steps",
                from: 3,
                to: 2,
                available: Some(&[2]),
                want: Err("codec: cannot upcast downward: from 3 > to 2 (the payload comes from a newer writer)"),
            },
            Case { name: "no transforms: older payload is identity", from: 1, to: 3, available: None, want: Ok(vec![]) },
            Case { name: "no transforms: current payload", from: 2, to: 2, available: None, want: Ok(vec![]) },
            Case {
                name: "no transforms: newer writer is refused",
                from: 5,
                to: 1,
                available: None,
                want: Err("codec: cannot upcast downward: from 5 > to 1 (the payload comes from a newer writer)"),
            },
        ];
        for c in cases {
            let got = upcast_plan(c.from, c.to, c.available).map_err(|e| e.to_string());
            assert_eq!(got, c.want.map_err(str::to_owned), "{}", c.name);
        }
    }
}
