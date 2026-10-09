//! What a failed call means: the verdict on a server error (its SQLSTATE and the unique key it names,
//! read against the operation that ran), and on a failure the client reports without a SQLSTATE.
//!
//! The operation matters. A lock wait that runs out is contention on a ledger claim, which a
//! dispatcher retries without counting an attempt (`in_progress`). On a compare-and-set write it is a
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
/// - `23505`, a unique violation: of the events' `(stream, sequence)` a `concurrency` conflict (another
///   writer appended first); of any other key a reused id, `codec`, which fails the same way again.
/// - `55P03` (a lock wait ran out) and `57014` (a statement ran out) on a claim: `in_progress`, since
///   another unit of work holds the key.
/// - `55P03` on an append, a projection save, a saga write or a settle: `concurrency`, since another unit
///   of work holds the row it would write. Elsewhere, and `57014` everywhere but a claim, `store`: a read
///   or an unversioned write that ran out of time is transient, not a version conflict.
/// - `40001`, `40P01`, the `08` connection class, the `53` resource class, `25006` (a read-only server
///   after a failover) and `25P02` (an earlier statement aborted the transaction): `store`, transient.
/// - Data the server cannot store (`22P05`, `22021`, `22P02`, `22003`, `2201W`, `54000`): `codec`.
/// - A missing table, column, function or schema, or a refused privilege or login (`42P01`,
///   `42703`, `42883`, `3F000`, `42501`, `28P01`): `bad_handle`, a wiring mistake no retry fixes.
/// - Anything else: `store`.
pub fn verdict(op: Op, sqlstate: &str, unique: Option<UniqueKey>, message: &str) -> Error {
    let code = match (sqlstate, op) {
        ("23505", _) if unique == Some(UniqueKey::EventsStreamSequence) => ErrorCode::Concurrency,
        ("23505", _) => ErrorCode::Codec,
        ("55P03" | "57014", Op::Claim) => ErrorCode::InProgress,
        ("55P03", _) if op.waits_on_a_writer() => ErrorCode::Concurrency,
        ("40001" | "40P01" | "25006" | "25P02", _) => ErrorCode::Store,
        ("22P05" | "22021" | "22P02" | "22003" | "2201W" | "54000", _) => ErrorCode::Codec,
        ("42P01" | "42703" | "42883" | "3F000" | "42501" | "28P01", _) => ErrorCode::BadHandle,
        // The `08` connection and `53` resource classes land here too: transient.
        _ => ErrorCode::Store,
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
        /// The call, or the wait for a pooled connection, ran out of time on the client.
        Timeout = "timeout",
        /// The connection broke or closed.
        Connection = "connection",
        /// The client refused to encode a parameter.
        Encode = "encode",
        /// Any other client-side failure.
        Other = "other",
    }
}

/// The verdict on a failure the client reports without a SQLSTATE, by its `kind` name. A parameter the
/// client cannot encode fails the same way again: `codec`. A timeout, a broken connection or anything
/// else is `store`, transient. An unknown kind is a `codec` error.
pub fn client_verdict(op: Op, kind: &str, message: &str) -> Error {
    let Some(failure) = ClientFailure::by_name(kind) else {
        return Error::codec(format_args!("unknown postgres client failure: {kind}"));
    };
    let code = match failure {
        ClientFailure::Encode => ErrorCode::Codec,
        ClientFailure::Timeout | ClientFailure::Connection | ClientFailure::Other => ErrorCode::Store,
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
        let cases: [(&str, Op, &str, Option<UniqueKey>, ErrorCode); 14] = [
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
        assert_eq!(client_verdict(Op::Load, "timeout", "t").code, ErrorCode::Store);
        assert_eq!(client_verdict(Op::Load, "connection", "c").code, ErrorCode::Store);
        assert_eq!(client_verdict(Op::Load, "other", "o").code, ErrorCode::Store);
        assert_eq!(client_verdict(Op::Load, "encode", "e").code, ErrorCode::Codec);
        assert_eq!(
            client_verdict(Op::Load, "bogus", "x").to_string(),
            "codec: unknown postgres client failure: bogus"
        );
    }
}
