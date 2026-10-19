//! What a failed call means: the verdict on a server error (its SQLSTATE and the unique key it names,
//! read against the operation that ran), and on a failure the client reports without a SQLSTATE.
//!
//! The operation matters. A lock wait that runs out is contention on a ledger claim or a projection
//! generation, an ingestion row, or an erasure fence, which a dispatcher or runner retries without counting an attempt (`in_progress`). On a compare-and-set write it is a
//! lost race (`concurrency`). Anywhere else it is a transient `store` failure, because the store
//! contracts keep `concurrency` for a version conflict. A unique violation is a version conflict on an
//! event stream and a reused id on an outbox row. `testdata/golden/postgres_error.json` pins every row.

use edax_core::{Error, ErrorCode};

use crate::schema::UniqueKey;

edax_core::closed_names! {
    /// The operation a statement ran for: what the verdict reads a server error against.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Op {
        /// Checking the server and the tables against the contract.
        Check = "check",
        /// Opening, committing or rolling back a unit of work.
        Transaction = "transaction",
        /// An event store append.
        Append = "append",
        /// An event store read: `load` or `load_snapshot`.
        Load = "load",
        /// An event store `save_snapshot`.
        SaveSnapshot = "save_snapshot",
        /// An outbox read side: `unpublished`, `mark_published` or `unpublished_count`.
        Outbox = "outbox",
        /// A command outbox `enqueue`.
        Enqueue = "enqueue",
        /// A projection `get` or `get_many`.
        ProjectionRead = "projection_read",
        /// A projection `save`.
        ProjectionSave = "projection_save",
        /// A ledger `claim`.
        Claim = "claim",
        /// A ledger `complete`, `release` or `forget`.
        Settle = "settle",
        /// A saga `load`.
        SagaLoad = "saga_load",
        /// A saga `save` or `complete`.
        SagaWrite = "saga_write",
        /// Any projection state operation, each run under its generation's advisory lock.
        ProjectionState = "projection_state",
        /// An ingestion commit, rejection read or publication mark.
        Ingest = "ingest",
        /// Acquiring a revocation or writer fence before reading protected state.
        ErasureFence = "erasure_fence",
    }
}

impl Op {
    /// The operation named `name`, or a `codec` error.
    pub fn from_name(name: &str) -> Result<Op, Error> {
        Op::by_name(name).ok_or_else(|| Error::codec(format_args!("unknown postgres operation: {name}")))
    }

    /// Whether a lock wait that runs out here means another writer holds the row: an append, a projection
    /// save and a saga write compare a version, and a settle is refused a key another open transaction
    /// holds (the idempotency store contract). A read, even a locking one, compares no version, and a
    /// snapshot, an outbox row or an enqueued command carries none, so their lock waits are `store`.
    fn waits_on_a_writer(self) -> bool {
        matches!(self, Op::Append | Op::ProjectionSave | Op::SagaWrite | Op::Settle)
    }
}

/// The verdict on a server error: the kernel error for SQLSTATE `sqlstate`, raised by `op`, with the
/// server's `message`. `unique` is the key the violated unique index guards, read from the name the server
/// reports through the [`crate::Compatibility`] the table check answered; `None` when no key of the
/// contract was violated.
///
/// Exact states first:
///
/// - `23505`, a unique violation: of the events' `(stream, sequence)` a `concurrency` conflict (another
///   writer appended first); on an ingestion commit or projection state `concurrency` too (another
///   transaction inserted the key first, so a retry reads the committed row); of any other key a reused
///   id, `codec`, which fails the same way again.
/// - `55P03` (a lock wait ran out) and `57014` (a statement ran out) on a claim: `in_progress`, since
///   another unit of work holds the key. Both states on erasure fence acquisition are `in_progress`.
///   `55P03` on projection state or ingestion is `in_progress`: another runner holds its lock.
/// - `55P03` on an append, a projection save, a saga write or a settle: `concurrency`, since another unit
///   of work holds the row it would write. Elsewhere, and `57014` everywhere but a claim, `store`: a read
///   or an unversioned write that ran out of time is transient, not a version conflict.
/// - `40001`, `40P01`, `25006` (a read-only server after a failover) and `25P02` (an earlier statement
///   aborted the transaction): `store`, transient.
///
/// Then by class:
///
/// - Data the server cannot store or a constraint it refuses (`22`, `23`, `54`): `codec`, since the same
///   write fails the same way again.
/// - A refused login, a missing database or schema, a syntax or access rule (`28`, `3D`, `3F`, `42`):
///   `bad_handle`, a wiring mistake no retry fixes.
/// - Anything else, the `08` connection and `53` resource classes among them: `store`, transient.
pub fn verdict(op: Op, sqlstate: &str, unique: Option<UniqueKey>, message: &str) -> Error {
    let code = match (sqlstate, op) {
        ("23505", _) if unique == Some(UniqueKey::EventsStreamSequence) => ErrorCode::Concurrency,
        ("23505", Op::Ingest | Op::ProjectionState) => ErrorCode::Concurrency,
        ("55P03" | "57014", Op::Claim | Op::ErasureFence) | ("55P03", Op::ProjectionState | Op::Ingest) => {
            ErrorCode::InProgress
        }
        ("55P03", _) if op.waits_on_a_writer() => ErrorCode::Concurrency,
        ("40001" | "40P01" | "25006" | "25P02" | "55P03" | "57014", _) => ErrorCode::Store,
        _ => match sqlstate.get(..2) {
            Some("22" | "23" | "54") => ErrorCode::Codec,
            Some("28" | "3D" | "3F" | "42") => ErrorCode::BadHandle,
            _ => ErrorCode::Store,
        },
    };
    let named = unique.map_or_else(String::new, |key| format!(" on {}", key.as_str()));
    Error::new(
        code,
        format_args!("postgres {} failed with {sqlstate}{named}: {message}", op.as_str()),
    )
}

edax_core::closed_names! {
    /// A failure a client reports without a SQLSTATE.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ClientFailure {
        /// The client refused to encode a parameter.
        Encode = "encode",
        /// Any other client-side failure: a timeout, a broken connection.
        Other = "other",
    }
}

/// The verdict on a failure the client reports without a SQLSTATE, by its `kind` name. A parameter the
/// client cannot encode fails the same way again: `codec`. Anything else, a timeout or a broken connection
/// among them, is `store`, transient. An unknown kind is a `codec` error.
pub fn client_verdict(op: Op, kind: &str, message: &str) -> Error {
    let Some(failure) = ClientFailure::by_name(kind) else {
        return Error::codec(format_args!("unknown postgres client failure: {kind}"));
    };
    let code = match failure {
        ClientFailure::Encode => ErrorCode::Codec,
        ClientFailure::Other => ErrorCode::Store,
    };
    Error::new(
        code,
        format_args!("postgres {} failed on the client ({kind}): {message}", op.as_str()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_branches() {
        let cases: [(&str, Op, &str, Option<UniqueKey>, ErrorCode); 23] = [
            (
                "stream_race",
                Op::Append,
                "23505",
                Some(UniqueKey::EventsStreamSequence),
                ErrorCode::Concurrency,
            ),
            (
                "reused_event_id",
                Op::Append,
                "23505",
                Some(UniqueKey::EventsId),
                ErrorCode::Codec,
            ),
            ("unknown_unique", Op::Enqueue, "23505", None, ErrorCode::Codec),
            ("claim_lock_wait", Op::Claim, "55P03", None, ErrorCode::InProgress),
            (
                "claim_statement_timeout",
                Op::Claim,
                "57014",
                None,
                ErrorCode::InProgress,
            ),
            ("settle_lock_wait", Op::Settle, "55P03", None, ErrorCode::Concurrency),
            ("read_lock_wait", Op::ProjectionRead, "55P03", None, ErrorCode::Store),
            ("append_statement_timeout", Op::Append, "57014", None, ErrorCode::Store),
            ("serialization", Op::ProjectionSave, "40001", None, ErrorCode::Store),
            ("aborted_earlier", Op::Load, "25P02", None, ErrorCode::Store),
            ("nul_text", Op::ProjectionSave, "22P05", None, ErrorCode::Codec),
            ("missing_table", Op::Load, "42P01", None, ErrorCode::BadHandle),
            ("check_violation", Op::Ingest, "23514", None, ErrorCode::Codec),
            ("ingest_unique_race", Op::Ingest, "23505", None, ErrorCode::Concurrency),
            (
                "projection_insert_race",
                Op::ProjectionState,
                "23505",
                None,
                ErrorCode::Concurrency,
            ),
            ("ingest_lock_wait", Op::Ingest, "55P03", None, ErrorCode::InProgress),
            (
                "erasure_lock_wait",
                Op::ErasureFence,
                "55P03",
                None,
                ErrorCode::InProgress,
            ),
            (
                "erasure_lock_deadline",
                Op::ErasureFence,
                "57014",
                None,
                ErrorCode::InProgress,
            ),
            ("data_class", Op::Ingest, "22012", None, ErrorCode::Codec),
            ("missing_database", Op::Ingest, "3D000", None, ErrorCode::BadHandle),
            ("short_state", Op::Ingest, "2", None, ErrorCode::Store),
            ("connection_class", Op::Outbox, "08006", None, ErrorCode::Store),
            ("unknown_state", Op::Outbox, "XX000", None, ErrorCode::Store),
        ];
        for (name, op, state, unique, want) in cases {
            assert_eq!(verdict(op, state, unique, "boom").code, want, "{name}");
        }
        assert_eq!(
            verdict(Op::Append, "23505", Some(UniqueKey::EventsStreamSequence), "dup").to_string(),
            "concurrency: postgres append failed with 23505 on events_stream_sequence: dup"
        );
    }

    #[test]
    fn client_verdict_branches() {
        assert_eq!(client_verdict(Op::Load, "other", "o").code, ErrorCode::Store);
        assert_eq!(client_verdict(Op::Load, "encode", "e").code, ErrorCode::Codec);
        assert_eq!(
            client_verdict(Op::Load, "bogus", "x").to_string(),
            "codec: unknown postgres client failure: bogus"
        );
    }
}

/// Classify a partitioned payload write from PostgreSQL diagnostics. A missing destination
/// has no named constraint and identifies a routing relation (possibly a child). Ordinary CHECK failures stay codec.
pub fn partitioned_ingest_verdict(sqlstate: &str, constraint: &str, table: &str, message: &str) -> Error {
    if sqlstate == "23514" && constraint.is_empty() && !table.is_empty() {
        return Error::new(
            ErrorCode::Store,
            format_args!("postgres ingestion partition unavailable: {message}"),
        );
    }
    verdict(Op::Ingest, sqlstate, None, message)
}

#[cfg(test)]
mod partition_tests {
    use super::*;
    #[test]
    fn missing_partition_is_distinct_from_named_checks() {
        for (state, constraint, table, expected) in [
            ("23514", "", "events", ErrorCode::Store),
            ("23514", "valid_pulse", "events", ErrorCode::Codec),
            ("23514", "", "child", ErrorCode::Store),
            ("23514", "", "", ErrorCode::Codec),
            ("23505", "events_pkey", "events", ErrorCode::Concurrency),
        ] {
            assert_eq!(
                partitioned_ingest_verdict(state, constraint, table, "failure").code,
                expected
            );
        }
    }
}
