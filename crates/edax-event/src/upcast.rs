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
//!
//! The kernel also owns each name's target version, [`upcast_target`]: one past the name's highest
//! step, or [`FIRST_VERSION`] for a name with none. Its upcaster is its only version source, so a codec
//! cannot declare a version its steps do not reach, and bumping one name never moves another's.

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
pub fn upcast_plan(from: u32, to: u32, available: Option<&[u32]>) -> Result<Vec<u32>, Error> {
    if from > to {
        return Err(Error::codec(format_args!(
            "cannot upcast downward: from {from} > to {to} (the payload comes from a newer writer)"
        )));
    }
    let Some(available) = available else { return Ok(Vec::new()) };
    let have: BTreeSet<u32> = available.iter().copied().collect();
    if let Some(step) = (from..to).find(|step| !have.contains(step)) {
        return Err(Error::codec(format_args!(
            "missing upcast step {step} (no transform for v{step} -> v{})",
            step.saturating_add(1)
        )));
    }
    Ok((from..to).collect())
}

/// The schema version of every event name that has never changed shape: what it writes with no steps.
pub const FIRST_VERSION: u32 = 1;

/// The schema version a codec writes for one event name and raises its older payloads to: one past the
/// name's highest upcast step, or [`FIRST_VERSION`] when `available` is `None` or empty.
///
/// The steps are the version's only source, so the target can never sit past the last step. A chain
/// with a hole (`[1, 3]`) is a `codec` error, because a payload stored at the missing version could
/// never be raised; a chain that starts above 1 (`[2, 3]`) is not, because its older versions may be
/// retired. A step at `u32::MAX` is a `codec` error too: its output has no schema version. A codec
/// asks once per name when it is built, so either mistake fails at startup, not on the first event.
pub fn upcast_target(available: Option<&[u32]>) -> Result<u32, Error> {
    let have: BTreeSet<u32> = available.unwrap_or_default().iter().copied().collect();
    let Some(&last) = have.last() else { return Ok(FIRST_VERSION) };
    // Pairwise, not `first..last`: a chain `[0, u32::MAX]` must not walk 2^32 versions to find its hole.
    if let Some((&below, _)) = have.iter().zip(have.iter().skip(1)).find(|(a, b)| a.checked_add(1) != Some(**b)) {
        let missing = below.saturating_add(1);
        return Err(Error::codec(format_args!(
            "upcast steps skip v{missing}: a payload stored at v{missing} could never be raised"
        )));
    }
    last.checked_add(1).ok_or_else(|| {
        Error::codec(format_args!("upcast step {last} raises past the largest schema version {}", u32::MAX))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upcast_plan_branches() {
        struct Case {
            name: &'static str,
            from: u32,
            to: u32,
            available: Option<&'static [u32]>,
            want: Result<Vec<u32>, &'static str>,
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

    #[test]
    fn upcast_target_branches() {
        type Want = Result<u32, &'static str>;
        let cases: [(&str, Option<&[u32]>, Want); 8] = [
            ("no upcaster writes the first version", None, Ok(1)),
            ("an upcaster with no steps writes the first version", Some(&[]), Ok(1)),
            ("one step raises to v2", Some(&[1]), Ok(2)),
            ("unsorted, repeated steps are a set", Some(&[2, 1, 2]), Ok(3)),
            ("a chain may start above 1", Some(&[2, 3]), Ok(4)),
            ("a hole is refused", Some(&[1, 3]), Err("codec: upcast steps skip v2: a payload stored at v2 could never be raised")),
            ("a hole across the u32 span is found without walking it", Some(&[0, u32::MAX]), Err("codec: upcast steps skip v1: a payload stored at v1 could never be raised")),
            ("a step at the ceiling has no target", Some(&[u32::MAX - 1, u32::MAX]), Err("codec: upcast step 4294967295 raises past the largest schema version 4294967295")),
        ];
        for (name, available, want) in cases {
            let got = upcast_target(available).map_err(|e| e.to_string());
            assert_eq!(got, want.map_err(str::to_owned), "{name}");
        }
    }
}
