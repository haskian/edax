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
use crate::message::{EventDraft, Extensions, Metadata, Payload};

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

edax_core::closed_names! {
    /// What keeps a save's commands fenced to the aggregate version they were decided at, and in order
    /// with the commands of the aggregate's next save. A host picks one per repository; every writer of
    /// one stream must pick the same.
    ///
    /// Exhaustive on purpose: each fence is a different promise, so a new one is a compile error in every
    /// SDK rather than a wildcard's silent default.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum CommandFence {
        /// A save that enqueues commands must append an event. The event's unique `(stream, sequence)`
        /// key fences the save: a rival save of the same version fails, and the next save waits for this
        /// one to commit.
        Event = "event",
        /// Every save takes its stream's lock first, so a save may enqueue commands without an event. The
        /// store serializes the stream's saves, at the cost of one more round trip per save.
        Lock = "lock",
    }
}

impl CommandFence {
    /// The inverse of [`as_str`](CommandFence::as_str). Anything else is a `bad_handle` error: the fence
    /// is wiring, chosen where a repository is built.
    pub fn from_name(name: &str) -> Result<CommandFence, Error> {
        CommandFence::by_name(name).ok_or_else(|| {
            Error::bad_handle(format_args!(
                "unknown command fence {name:?}: use one of {:?}",
                CommandFence::NAMES
            ))
        })
    }
}

/// What a save does beside its append, under its repository's [`CommandFence`]: the decisions every
/// SDK's repository runs rather than makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavePlan {
    /// Take the stream's lock before the append (and before a prepared save's reload).
    pub lock_stream: bool,
    /// The event the save's commands wait behind in the outbox: the last event it appends, if any.
    pub after_event: Option<String>,
}

/// The plan for a save that appends the events `event_ids`, in order, and enqueues `commands` commands,
/// under `fence`.
///
/// Under [`CommandFence::Event`] a save that enqueues commands must append an event, else it is refused as
/// a `bad_handle` error: nothing would fence it, so it could send commands decided on a stale aggregate,
/// or out of order with a concurrent save's. The same save fails the same way on every retry. Under
/// [`CommandFence::Lock`] every save locks its stream first. Either way the commands wait behind the
/// last event, so the relay never sends a command before the event it follows from.
pub fn plan_save(fence: CommandFence, event_ids: &[String], commands: usize) -> Result<SavePlan, Error> {
    if fence == CommandFence::Event && event_ids.is_empty() && commands > 0 {
        return Err(Error::bad_handle(
            "a save that enqueues commands must append an event under command fence \"event\": record \
             the decision as an event, or build the repository with command fence \"lock\"",
        ));
    }
    Ok(SavePlan {
        lock_stream: fence == CommandFence::Lock,
        after_event: event_ids.last().cloned(),
    })
}

/// Refuse a batch for `stream` that holds another aggregate's draft, as a `codec` error naming the 0-based
/// index of the first such draft. A draft belongs to `stream` when its aggregate id, bare or scoped by its
/// tenant ([`edax_core::scoped`], the pool form of [`edax_core::tenant_key`]), spells `stream`. An append
/// writes every draft into `stream` and checks that stream's version only, so a foreign draft would join
/// another aggregate's stream and skip its own aggregate's version check.
pub fn check_stream(stream: &str, drafts: &[EventDraft]) -> Result<(), Error> {
    let foreign = drafts.iter().enumerate().find(|(_, draft)| {
        draft.aggregate_id != stream
            && draft
                .tenant
                .as_deref()
                .is_none_or(|tenant| edax_core::scoped(tenant, &draft.aggregate_id) != stream)
    });
    match foreign {
        Some((index, draft)) => Err(Error::codec(format_args!(
            "event {index} belongs to aggregate {}, not to stream {stream}",
            draft.aggregate_id
        ))),
        None => Ok(()),
    }
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
    /// Immutable erasure owner of protected state.
    pub subject: Option<&'a str>,
    /// The flow every event of the batch continues.
    pub lineage: &'a Lineage,
    /// Edax extension context, under [`check_extensions`](crate::check_extensions)'s key rule.
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
/// `payloads`; as `codec`, a batch that would run the stream past sequence `u64::MAX`, and a draft
/// [`EventDraft::check`] refuses (an extension key outside
/// [`check_extensions`](crate::check_extensions), a tenant outside [`edax_core::check_tenant`], or NUL
/// in text).
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
        subject,
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
    let version = u64::try_from(payloads.len())
        .ok()
        .and_then(|count| initial.checked_add(count))
        .ok_or_else(|| {
            Error::codec(format_args!(
                "the batch would run the stream past sequence {}",
                u64::MAX
            ))
        })?;
    let drafts: Vec<EventDraft> = payloads
        .into_iter()
        .zip(ids)
        .zip(initial.saturating_add(1)..=version)
        .map(|((payload, id), sequence)| EventDraft {
            id,
            aggregate_id: aggregate_id.to_owned(),
            tenant: tenant.map(ToOwned::to_owned),
            subject: subject.map(ToOwned::to_owned),
            encryption: None,
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
    drafts.iter().try_for_each(EventDraft::check)?;
    Ok(DraftBatch {
        drafts,
        version,
        snapshot: snapshot_due(snapshot_every, initial, version),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::base_draft;

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

    #[test]
    fn plan_save_branches() {
        let refused = "bad_handle: a save that enqueues commands must append an event under command fence \"event\": \
                       record the decision as an event, or build the repository with command fence \"lock\"";
        let ids = |ids: &[&str]| ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
        let plan = |lock_stream: bool, after_event: Option<&str>| SavePlan {
            lock_stream,
            after_event: after_event.map(str::to_owned),
        };
        type Case<'a> = (&'a str, CommandFence, Vec<String>, usize, Result<SavePlan, &'a str>);
        let cases: Vec<Case> = vec![
            (
                "event_fence_waits_behind_the_last_event",
                CommandFence::Event,
                ids(&["e1", "e2"]),
                1,
                Ok(plan(false, Some("e2"))),
            ),
            (
                "event_fence_events_only",
                CommandFence::Event,
                ids(&["e1"]),
                0,
                Ok(plan(false, Some("e1"))),
            ),
            (
                "event_fence_nothing",
                CommandFence::Event,
                ids(&[]),
                0,
                Ok(plan(false, None)),
            ),
            (
                "event_fence_commands_only",
                CommandFence::Event,
                ids(&[]),
                1,
                Err(refused),
            ),
            (
                "lock_fence_commands_only",
                CommandFence::Lock,
                ids(&[]),
                3,
                Ok(plan(true, None)),
            ),
            (
                "lock_fence_with_events",
                CommandFence::Lock,
                ids(&["e1"]),
                1,
                Ok(plan(true, Some("e1"))),
            ),
        ];
        for (name, fence, event_ids, commands, want) in cases {
            let got = plan_save(fence, &event_ids, commands).map_err(|e| e.to_string());
            assert_eq!(got, want.map_err(str::to_owned), "{name}");
        }
        assert_eq!(CommandFence::from_name("lock").ok(), Some(CommandFence::Lock));
        assert_eq!(
            CommandFence::from_name("none").map_err(|e| e.to_string()),
            Err("bad_handle: unknown command fence \"none\": use one of [\"event\", \"lock\"]".to_owned())
        );
    }

    fn draft(aggregate_id: &str, tenant: Option<&str>) -> EventDraft {
        EventDraft {
            aggregate_id: aggregate_id.to_owned(),
            tenant: tenant.map(ToOwned::to_owned),
            ..base_draft()
        }
    }

    #[test]
    fn check_stream_branches() {
        type Case<'a> = (&'a str, &'a str, Vec<EventDraft>, Result<(), &'a str>);
        let cases: Vec<Case> = vec![
            ("empty_batch", "invoice-7", vec![], Ok(())),
            (
                "bare_stream",
                "invoice-7",
                vec![draft("invoice-7", Some("acme"))],
                Ok(()),
            ),
            (
                "pooled_stream",
                "4:acme:invoice-7",
                vec![draft("invoice-7", Some("acme"))],
                Ok(()),
            ),
            (
                "another_aggregate",
                "invoice-8",
                vec![draft("invoice-7", Some("acme"))],
                Err("codec: event 0 belongs to aggregate invoice-7, not to stream invoice-8"),
            ),
            (
                "another_tenants_stream",
                "6:globex:invoice-7",
                vec![draft("invoice-7", Some("acme"))],
                Err("codec: event 0 belongs to aggregate invoice-7, not to stream 6:globex:invoice-7"),
            ),
            (
                "pooled_stream_without_tenant",
                "4:acme:invoice-7",
                vec![draft("invoice-7", None)],
                Err("codec: event 0 belongs to aggregate invoice-7, not to stream 4:acme:invoice-7"),
            ),
            (
                "second_draft_foreign",
                "invoice-7",
                vec![draft("invoice-7", None), draft("invoice-8", None)],
                Err("codec: event 1 belongs to aggregate invoice-8, not to stream invoice-7"),
            ),
        ];
        for (name, stream, drafts, want) in cases {
            let got = check_stream(stream, &drafts).map_err(|e| e.to_string());
            assert_eq!(got, want.map_err(str::to_owned), "{name}");
        }
    }
}
