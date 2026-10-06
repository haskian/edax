//! The next version of a versioned row: one rule and one wording for every row a store versions
//! (a saga instance, a projection document).

use crate::Error;

/// The version a write at `current` commits: `current + 1`. A row at `u64::MAX` has none, which is a
/// `codec` error naming `what`, since the same write fails the same way on every retry. `what` names
/// the row, as in `"saga instance"` or `"projection user-1"`.
pub fn next_version(what: &str, current: u64) -> Result<u64, Error> {
    current
        .checked_add(1)
        .ok_or_else(|| Error::codec(format_args!("{what} at version {current} has no next version")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_version_branches() {
        for (name, current, want) in [
            ("absent_row_is_created_at_one", 0, Ok(1)),
            ("the_version_before_the_last_steps_to_it", u64::MAX - 1, Ok(u64::MAX)),
            (
                "the_last_version_has_none",
                u64::MAX,
                Err("codec: projection user-1 at version 18446744073709551615 has no next version".to_owned()),
            ),
        ] {
            assert_eq!(
                next_version("projection user-1", current).map_err(|e| e.to_string()),
                want,
                "{name}"
            );
        }
    }
}
