//! A table of versioned rows under optimistic concurrency: the projection documents and the saga
//! instances. One read and one write rule for both, so the two seams cannot drift apart.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use edax_core::Error;

/// A stored value and the version it was read at.
#[derive(Clone, Debug, PartialEq)]
pub struct Versioned<T> {
    /// The row's version: the next write expects it.
    pub version: u64,
    /// The stored value.
    pub value: T,
}

/// What one open transaction staged for a [`VersionedTable`], by key.
pub(crate) type Staged<K, T> = HashMap<K, Versioned<T>>;

/// The committed rows of one versioned table, in key order: a projection page iterates its documents
/// in id order, so an unsorted query is deterministic across runs and languages.
pub(crate) struct VersionedTable<K, T> {
    rows: BTreeMap<K, Versioned<T>>,
}

// By hand: a derive would demand `K: Default` and `T: Default`, which an empty map does not need.
impl<K, T> Default for VersionedTable<K, T> {
    fn default() -> Self {
        Self { rows: BTreeMap::new() }
    }
}

impl<K: Ord + Hash, T> VersionedTable<K, T> {
    /// The row for `key` as a transaction sees it: its own staged write, else the committed one.
    pub(crate) fn get<'a>(&'a self, own: Option<&'a Staged<K, T>>, key: &K) -> Option<&'a Versioned<T>> {
        own.and_then(|staged| staged.get(key)).or_else(|| self.rows.get(key))
    }

    /// The version a write of `key` at `expected` commits, checked in this order:
    /// - another open transaction staged `key`: a conflict naming the committed version, since the
    ///   holder's own may yet roll back;
    /// - `admit` refuses the stored row (a saga's tombstone takes no write);
    /// - the version `own` sees is not `expected` (an absent row is at 0): a conflict;
    /// - `expected` is the last version: [`edax_core::next_version`]'s `codec` error, naming `what`.
    pub(crate) fn next<'a>(
        &self,
        mut others: impl Iterator<Item = &'a Staged<K, T>>,
        own: &Staged<K, T>,
        key: &K,
        expected: u64,
        what: &str,
        admit: impl FnOnce(u64, &T) -> Result<(), Error>,
    ) -> Result<u64, Error>
    where
        K: 'a,
        T: 'a,
    {
        if others.any(|staged| staged.contains_key(key)) {
            return Err(Error::concurrency(
                expected,
                self.rows.get(key).map_or(0, |row| row.version),
            ));
        }
        let stored = match self.get(Some(own), key) {
            Some(row) => {
                admit(row.version, &row.value)?;
                row.version
            }
            None => 0,
        };
        if stored != expected {
            return Err(Error::concurrency(expected, stored));
        }
        edax_core::next_version(what, expected)
    }

    /// Commit a transaction's staged rows. Infallible: [`next`](Self::next) ran when each was staged,
    /// and a staged key is held until its transaction ends.
    pub(crate) fn apply(&mut self, staged: Staged<K, T>) {
        self.rows.extend(staged);
    }

    /// Every committed row, in key order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &Versioned<T>)> {
        self.rows.iter()
    }

    /// Store a row as committed, bypassing every check: a test seeds an edge (the last version) with it.
    #[cfg(test)]
    pub(crate) fn seed(&mut self, key: K, version: u64, value: T) {
        self.rows.insert(key, Versioned { version, value });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_core::ErrorCode;

    /// A row that refuses every write, as a saga tombstone does.
    const TOMBSTONE: &str = "tombstone";

    fn refuse_tombstone(version: u64, value: &&str) -> Result<(), Error> {
        match *value {
            TOMBSTONE => Err(Error::new(
                ErrorCode::Concurrency,
                format_args!("row completed at version {version}"),
            )),
            _ => Ok(()),
        }
    }

    #[test]
    fn next_branches() {
        struct Case {
            name: &'static str,
            /// The committed row for `"k"`, if any.
            committed: Option<(u64, &'static str)>,
            /// What another open transaction staged for `"k"`, if anything.
            other: Option<(u64, &'static str)>,
            /// What this transaction staged for `"k"` already, if anything.
            own: Option<(u64, &'static str)>,
            expected: u64,
            want: Result<u64, &'static str>,
        }
        let base = Case {
            name: "",
            committed: Some((1, "row")),
            other: None,
            own: None,
            expected: 1,
            want: Ok(2),
        };
        let cases = [
            Case {
                name: "an_absent_row_at_zero_is_created",
                committed: None,
                expected: 0,
                want: Ok(1),
                ..base
            },
            Case {
                name: "an_absent_row_ahead_conflicts",
                committed: None,
                want: Err("concurrency: expected 1, stored 0"),
                ..base
            },
            Case {
                name: "the_stored_version_steps_on",
                ..base
            },
            Case {
                name: "a_stale_version_conflicts",
                expected: 0,
                want: Err("concurrency: expected 0, stored 1"),
                ..base
            },
            Case {
                name: "a_version_ahead_conflicts",
                expected: 7,
                want: Err("concurrency: expected 7, stored 1"),
                ..base
            },
            Case {
                name: "a_row_another_transaction_holds_names_the_committed_version",
                other: Some((2, "theirs")),
                expected: 2,
                want: Err("concurrency: expected 2, stored 1"),
                ..base
            },
            Case {
                name: "its_own_staged_row_is_the_version_it_sees",
                own: Some((2, "mine")),
                expected: 2,
                want: Ok(3),
                ..base
            },
            Case {
                name: "the_last_version_has_no_next",
                committed: Some((u64::MAX, "row")),
                expected: u64::MAX,
                want: Err("codec: projection k at version 18446744073709551615 has no next version"),
                ..base
            },
            Case {
                name: "admit_refuses_before_the_compare",
                committed: Some((2, TOMBSTONE)),
                expected: 0,
                want: Err("concurrency: row completed at version 2"),
                ..base
            },
        ];
        for c in cases {
            let mut table = VersionedTable::default();
            if let Some((version, value)) = c.committed {
                table.seed("k", version, value);
            }
            let staged = |row: Option<(u64, &'static str)>| -> Staged<&str, &str> {
                row.map(|(version, value)| ("k", Versioned { version, value }))
                    .into_iter()
                    .collect()
            };
            let (other, own) = (staged(c.other), staged(c.own));
            let got = table.next(
                [&other].into_iter(),
                &own,
                &"k",
                c.expected,
                "projection k",
                refuse_tombstone,
            );
            assert_eq!(
                got.map_err(|e| e.to_string()),
                c.want.map_err(str::to_owned),
                "{}",
                c.name
            );
        }
    }

    #[test]
    fn get_reads_its_own_staged_row_over_the_committed_one() {
        let mut table = VersionedTable::default();
        table.seed("k", 1, "committed");
        let staged = Versioned {
            version: 2,
            value: "staged",
        };
        let own: Staged<&str, &str> = [("k", staged.clone())].into_iter().collect();
        assert_eq!(
            table.get(None, &"k"),
            Some(&Versioned {
                version: 1,
                value: "committed"
            }),
            "outside any transaction"
        );
        assert_eq!(table.get(Some(&own), &"k"), Some(&staged), "read-your-writes");
        assert_eq!(table.get(Some(&own), &"absent"), None);
        table.apply(own);
        assert_eq!(
            table.iter().collect::<Vec<_>>(),
            [(&"k", &staged)],
            "a commit lands the staged row"
        );
    }
}
