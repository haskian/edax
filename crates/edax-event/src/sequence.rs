//! The dense-batch rule an event append must satisfy, the drafts a save appends, and when it writes a
//! snapshot.
//!
//! A store checks only the stream's last stored sequence, so a batch with a gap, a repeat or a
//! reordering would pass that check and still break the gap-free ascending read every store
//! promises. Every durable store needs the same rule, so it lives here and is pinned by
//! `testdata/golden/sequence_batch.json` rather than re-derived per backend. The snapshot interval
//! is a version rule too, pinned by `testdata/golden/snapshot_due.json`, so two SDKs saving one
//! aggregate type snapshot at the same versions. The drafts a save appends are numbered and stamped
//! here too ([`draft_batch`], pinned by `testdata/golden/draft_batch.json`), so two SDKs saving one
//! aggregate write the same batch.

use edax_core::Error;

use crate::causation::Lineage;
use crate::message::{check_extensions, EventDraft, Extensions, Metadata, Payload};

/// Check that `sequences` is the dense ascending run `expected + 1, expected + 2, ...`.
///
/// `expected` is the stream version the append is made against, and `sequences` holds each
/// event's sequence in batch order. An event with no sequence is refused: it is invisible to a
/// sequenced read and never bumps the version. An empty batch passes — it is a version check only.
/// Versions and sequences are one `u64` space, so the comparison is exact everywhere; a batch that
/// would run past `u64::MAX` is refused rather than clamped, since a clamp would let two events
/// share the ceiling.
///
/// Returns the stream version after the batch: the last sequence, or `expected` for an empty batch.
/// The error names the 0-based batch index, the one identifier the kernel shares with every host.
/// It is a `codec` error: the same batch fails the same way on every retry.
pub fn check_batch(expected: u64, sequences: &[Option<u64>]) -> Result<u64, Error> {
    let mut want = expected;
    for (index, sequence) in sequences.iter().enumerate() {
        let Some(next) = want.checked_add(1) else {
            return Err(Error::codec(format_args!(
                "event {index} would run the stream past sequence {}",
                u64::MAX
            )));
        };
        want = next;
        let Some(sequence) = *sequence else {
            return Err(Error::codec(format_args!("event {index} has no sequence")));
        };
        if sequence != want {
            return Err(Error::codec(format_args!(
                "event {index} has sequence {sequence}, expected {want}"
            )));
        }
    }
    Ok(want)
}

/// Refuse a snapshot of `stream` at `version` that leads the stream, at `stream_version`, as the store's
/// write sees it. A snapshot may lag its stream but never lead it, or a load would fold events the stream
/// does not hold. Every store refuses it the same way, a `codec` error, since the same save fails again.
pub fn check_snapshot(stream: &str, version: u64, stream_version: u64) -> Result<(), Error> {
    if version > stream_version {
        return Err(Error::codec(format_args!(
            "snapshot of {stream} at version {version} leads its stream at version {stream_version}"
        )));
    }
    Ok(())
}

/// Whether a save that moves an aggregate from version `initial` to `current` writes a snapshot under
/// an interval of `every` versions: when the save crosses a multiple of `every`. `1` snapshots on
/// every save that appends, `0` never does, and a save with nothing pending crosses no multiple.
pub fn snapshot_due(every: u64, initial: u64, current: u64) -> bool {
    every > 0 && current / every > initial / every
}

/// The batch a save appends: its drafts, the aggregate's version after it, and whether the save writes
/// a snapshot at that version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftBatch {
    /// One draft per payload, sequenced `initial + 1, initial + 2, ...`.
    pub drafts: Vec<EventDraft>,
    /// The aggregate's version once the batch lands: `initial` plus the number of drafts.
    pub version: u64,
    /// Whether the save writes a snapshot at `version` ([`snapshot_due`]).
    pub snapshot: bool,
}

/// One save's context: what [`draft_batch`] stamps on every draft of the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SaveContext<'a> {
    /// The tenant the aggregate belongs to, `None` for none.
    pub tenant: Option<&'a str>,
    /// The flow every event of the batch continues.
    pub lineage: &'a Lineage,
    /// Edax extension context, under [`check_extensions`]'s key rule.
    pub extensions: &'a Extensions,
    /// The application's own context.
    pub metadata: &'a Metadata,
    /// The valid time of the batch in unix nanoseconds, `None` for the time the store records it.
    pub occurred_at: Option<i64>,
}

/// The drafts a save of the aggregate `aggregate_id`, at version `initial`, appends for `payloads`, each
/// named by the id in `ids` at its index and stamped with `context`. `snapshot_every` is the snapshot
/// interval.
///
/// Refused before anything is written: as `bad_handle`, a count of `ids` that is not the count of
/// `payloads`; as `codec`, an extension key outside [`check_extensions`], a tenant outside
/// [`edax_core::check_tenant`], and a batch that would run the stream past sequence `u64::MAX`.
pub fn draft_batch(
    aggregate_id: &str,
    initial: u64,
    snapshot_every: u64,
    payloads: Vec<Payload>,
    ids: Vec<String>,
    context: &SaveContext<'_>,
) -> Result<DraftBatch, Error> {
    let SaveContext {
        tenant,
        lineage,
        extensions,
        metadata,
        occurred_at,
    } = *context;
    if ids.len() != payloads.len() {
        return Err(Error::bad_handle(format_args!(
            "{} event ids for {} events",
            ids.len(),
            payloads.len()
        )));
    }
    check_extensions(extensions)?;
    if let Some(tenant) = tenant {
        edax_core::check_tenant(tenant)?;
    }
    let version = u64::try_from(payloads.len())
        .ok()
        .and_then(|count| initial.checked_add(count))
        .ok_or_else(|| {
            Error::codec(format_args!(
                "the batch would run the stream past sequence {}",
                u64::MAX
            ))
        })?;
    let drafts = payloads
        .into_iter()
        .zip(ids)
        .zip(initial.saturating_add(1)..=version)
        .map(|((payload, id), sequence)| EventDraft {
            id,
            aggregate_id: aggregate_id.to_owned(),
            tenant: tenant.map(ToOwned::to_owned),
            sequence: Some(sequence),
            name: payload.name,
            version: payload.version,
            data: payload.data,
            correlation_id: lineage.correlation_id.clone(),
            causation_id: lineage.causation_id.clone(),
            occurred_at,
            extensions: extensions.clone(),
            metadata: metadata.clone(),
        })
        .collect();
    Ok(DraftBatch {
        drafts,
        version,
        snapshot: snapshot_due(snapshot_every, initial, version),
    })
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
            want: Result<u64, Error>,
        }
        let cases = vec![
            Case {
                name: "empty_batch_is_a_version_check",
                expected: 7,
                sequences: vec![],
                want: Ok(7),
            },
            Case {
                name: "dense_run",
                expected: 2,
                sequences: vec![Some(3), Some(4), Some(5)],
                want: Ok(5),
            },
            Case {
                name: "first_append_starts_at_one",
                expected: 0,
                sequences: vec![Some(1)],
                want: Ok(1),
            },
            Case {
                name: "missing_sequence",
                expected: 0,
                sequences: vec![Some(1), None],
                want: Err(Error::codec("event 1 has no sequence")),
            },
            Case {
                name: "gap",
                expected: 1,
                sequences: vec![Some(2), Some(4)],
                want: Err(Error::codec("event 1 has sequence 4, expected 3")),
            },
            Case {
                name: "reordered",
                expected: 0,
                sequences: vec![Some(2), Some(1)],
                want: Err(Error::codec("event 0 has sequence 2, expected 1")),
            },
            Case {
                name: "repeat",
                expected: 0,
                sequences: vec![Some(1), Some(1)],
                want: Err(Error::codec("event 1 has sequence 1, expected 2")),
            },
            Case {
                name: "past_the_i64_range_is_exact",
                expected: i64::MAX as u64,
                sequences: vec![Some(i64::MAX as u64 + 1), Some(i64::MAX as u64 + 2)],
                want: Ok(i64::MAX as u64 + 2),
            },
            Case {
                name: "the_last_sequence_is_u64_max",
                expected: u64::MAX - 1,
                sequences: vec![Some(u64::MAX)],
                want: Ok(u64::MAX),
            },
            // A clamp would let both events sit at the ceiling; the run is refused instead.
            Case {
                name: "a_repeat_at_the_ceiling_is_refused",
                expected: u64::MAX - 1,
                sequences: vec![Some(u64::MAX), Some(u64::MAX)],
                want: Err(Error::codec(
                    "event 1 would run the stream past sequence 18446744073709551615",
                )),
            },
            Case {
                name: "a_stream_already_at_the_ceiling_takes_nothing",
                expected: u64::MAX,
                sequences: vec![Some(u64::MAX)],
                want: Err(Error::codec(
                    "event 0 would run the stream past sequence 18446744073709551615",
                )),
            },
            Case {
                name: "a_sequence_far_ahead",
                expected: 1,
                sequences: vec![Some(u64::MAX)],
                want: Err(Error::codec("event 0 has sequence 18446744073709551615, expected 2")),
            },
        ];
        for c in cases {
            assert_eq!(check_batch(c.expected, &c.sequences), c.want, "{}", c.name);
        }
    }
}
