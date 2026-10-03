//! The dense-batch rule an event append must satisfy.
//!
//! A store checks only the stream's last stored sequence, so a batch with a gap, a repeat or a
//! reordering would pass that check and still break the gap-free ascending read every store
//! promises. Every durable store needs the same rule, so it lives here and is pinned by
//! `testdata/golden/sequence_batch.json` rather than re-derived per backend.

use edax_core::Error;

/// Check that `sequences` is the dense ascending run `expected + 1, expected + 2, ...`.
///
/// `expected` is the stream version the append is made against, and `sequences` holds each
/// envelope's sequence in batch order. An envelope with no sequence is refused: it is invisible to a
/// sequenced read and never bumps the version. An empty batch passes — it is a version check only.
/// Versions and sequences are one `u64` space, so the comparison is exact everywhere; a batch that
/// would run past `u64::MAX` is refused rather than clamped, since a clamp would let two envelopes
/// share the ceiling.
///
/// The error names the 0-based batch index, the one identifier the kernel shares with every host.
/// It is a `codec` error: the same batch fails the same way on every retry.
pub fn check_batch(expected: u64, sequences: &[Option<u64>]) -> Result<(), Error> {
    let mut want = expected;
    for (index, sequence) in sequences.iter().enumerate() {
        let Some(next) = want.checked_add(1) else {
            return Err(Error::codec(format_args!("envelope {index} would run the stream past sequence {}", u64::MAX)));
        };
        want = next;
        let Some(sequence) = *sequence else {
            return Err(Error::codec(format_args!("envelope {index} has no sequence")));
        };
        if sequence != want {
            return Err(Error::codec(format_args!("envelope {index} has sequence {sequence}, expected {want}")));
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
            expected: u64,
            sequences: Vec<Option<u64>>,
            want: Result<(), Error>,
        }
        let cases = vec![
            Case { name: "empty_batch_is_a_version_check", expected: 7, sequences: vec![], want: Ok(()) },
            Case { name: "dense_run", expected: 2, sequences: vec![Some(3), Some(4), Some(5)], want: Ok(()) },
            Case { name: "first_append_starts_at_one", expected: 0, sequences: vec![Some(1)], want: Ok(()) },
            Case {
                name: "missing_sequence",
                expected: 0,
                sequences: vec![Some(1), None],
                want: Err(Error::codec("envelope 1 has no sequence")),
            },
            Case {
                name: "gap",
                expected: 1,
                sequences: vec![Some(2), Some(4)],
                want: Err(Error::codec("envelope 1 has sequence 4, expected 3")),
            },
            Case {
                name: "reordered",
                expected: 0,
                sequences: vec![Some(2), Some(1)],
                want: Err(Error::codec("envelope 0 has sequence 2, expected 1")),
            },
            Case {
                name: "repeat",
                expected: 0,
                sequences: vec![Some(1), Some(1)],
                want: Err(Error::codec("envelope 1 has sequence 1, expected 2")),
            },
            Case {
                name: "past_the_i64_range_is_exact",
                expected: i64::MAX as u64,
                sequences: vec![Some(i64::MAX as u64 + 1), Some(i64::MAX as u64 + 2)],
                want: Ok(()),
            },
            Case {
                name: "the_last_sequence_is_u64_max",
                expected: u64::MAX - 1,
                sequences: vec![Some(u64::MAX)],
                want: Ok(()),
            },
            // A clamp would let both envelopes sit at the ceiling; the run is refused instead.
            Case {
                name: "a_repeat_at_the_ceiling_is_refused",
                expected: u64::MAX - 1,
                sequences: vec![Some(u64::MAX), Some(u64::MAX)],
                want: Err(Error::codec("envelope 1 would run the stream past sequence 18446744073709551615")),
            },
            Case {
                name: "a_stream_already_at_the_ceiling_takes_nothing",
                expected: u64::MAX,
                sequences: vec![Some(u64::MAX)],
                want: Err(Error::codec("envelope 0 would run the stream past sequence 18446744073709551615")),
            },
            Case {
                name: "a_sequence_far_ahead",
                expected: 1,
                sequences: vec![Some(u64::MAX)],
                want: Err(Error::codec("envelope 0 has sequence 18446744073709551615, expected 2")),
            },
        ];
        for c in cases {
            assert_eq!(check_batch(c.expected, &c.sequences), c.want, "{}", c.name);
        }
    }
}
