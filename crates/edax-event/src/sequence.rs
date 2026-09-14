//! The dense-batch rule an event append must satisfy.
//!
//! A store checks only the stream's last stored sequence, so a batch with a gap, a repeat or a
//! reordering would pass that check and still break the gap-free ascending read every store
//! promises. Every durable store needs the same rule, so it lives here and is pinned by
//! `testdata/golden/sequence_batch.json` rather than re-derived per backend.

/// Check that `sequences` is the dense ascending run `expected + 1, expected + 2, ...`.
///
/// `expected` is the stream version the append is made against, and `sequences` holds each
/// envelope's sequence in batch order. An envelope with no sequence is refused: it is invisible to a
/// sequenced read and never bumps the version. An empty batch passes — it is a version check only.
///
/// The error names the 0-based batch index, the one identifier the kernel shares with every host,
/// and describes the deterministic fault: the same batch fails the same way on every retry, so a
/// caller tags it terminal.
pub fn check_batch(expected: i64, sequences: &[Option<u64>]) -> Result<(), String> {
    for (index, sequence) in sequences.iter().enumerate() {
        let offset = i64::try_from(index).unwrap_or(i64::MAX);
        let want = expected.saturating_add(1).saturating_add(offset);
        let Some(sequence) = *sequence else {
            return Err(format!("envelope {index} has no sequence"));
        };
        // Saturating rather than rejecting: a sequence above the i64 ceiling cannot equal any
        // version the stores use, so it falls into the mismatch arm with its own value reported.
        if i64::try_from(sequence).unwrap_or(i64::MAX) != want {
            return Err(format!("envelope {index} has sequence {sequence}, expected {want}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_batch_branches() {
        struct Case {
            name: &'static str,
            expected: i64,
            sequences: Vec<Option<u64>>,
            want: Result<(), String>,
        }
        let cases = vec![
            Case { name: "empty_batch_is_a_version_check", expected: 7, sequences: vec![], want: Ok(()) },
            Case { name: "dense_run", expected: 2, sequences: vec![Some(3), Some(4), Some(5)], want: Ok(()) },
            Case { name: "first_append_starts_at_one", expected: 0, sequences: vec![Some(1)], want: Ok(()) },
            Case {
                name: "missing_sequence",
                expected: 0,
                sequences: vec![Some(1), None],
                want: Err("envelope 1 has no sequence".to_owned()),
            },
            Case {
                name: "gap",
                expected: 1,
                sequences: vec![Some(2), Some(4)],
                want: Err("envelope 1 has sequence 4, expected 3".to_owned()),
            },
            Case {
                name: "reordered",
                expected: 0,
                sequences: vec![Some(2), Some(1)],
                want: Err("envelope 0 has sequence 2, expected 1".to_owned()),
            },
            Case {
                name: "repeat",
                expected: 0,
                sequences: vec![Some(1), Some(1)],
                want: Err("envelope 1 has sequence 1, expected 2".to_owned()),
            },
            Case {
                name: "above_the_i64_ceiling",
                expected: 1,
                sequences: vec![Some(u64::MAX)],
                want: Err("envelope 0 has sequence 18446744073709551615, expected 2".to_owned()),
            },
        ];
        for c in cases {
            assert_eq!(check_batch(c.expected, &c.sequences), c.want, "{}", c.name);
        }
    }
}
