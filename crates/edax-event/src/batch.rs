//! Microbatch admission, flush and recovery decisions shared by ingestion SDKs.
//!
//! Infrastructure failures stop at the retry ceiling. Only a known rolled-back,
//! record-local failure permits isolation and rejection. Hosts retain admission
//! credits until the tasks and serialized data that consume them are released.

use edax_core::{Error, ErrorCode};
use serde::{Deserialize, Serialize};

edax_core::closed_names! {
    /// The failure classification supplied to microbatch recovery.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FailureKind {
        /// A configuration or ownership error that stops the runner.
        Fatal = "fatal",
        /// An infrastructure error that may succeed on another attempt.
        Retry = "retry",
        /// The previous commit may have succeeded; replay the originals.
        Uncertain = "uncertain",
        /// A rolled-back record-local error that permits isolation.
        Isolate = "isolate",
        /// A deterministic invalid-record verdict.
        Invalid = "invalid",
    }
}

edax_core::closed_names! {
    /// The action a host must take after a microbatch failure.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Action {
        /// Stop without acknowledging or rejecting the originals.
        Stop = "stop",
        /// Back off and retry the unchanged originals.
        Retry = "retry",
        /// Split originals in order and execute the left child first.
        Split = "split",
        /// Persist deterministic rejections for the originals.
        Reject = "reject",
    }
}

/// Limits for a microbatch runner. Missing JSON fields take their defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Maximum originals in one batch.
    pub max_records: usize,
    /// Serialized bytes that trigger a batch flush.
    pub max_bytes: usize,
    /// Maximum batch age in milliseconds.
    pub flush_ms: u64,
    /// Maximum concurrent sink operations.
    pub concurrency: usize,
    /// Maximum originals retained across all partitions and retries.
    pub retained_records: usize,
    /// Maximum measured serialized bytes retained by the runner.
    pub retained_bytes: usize,
    /// Maximum serialized source record size.
    pub max_record_bytes: usize,
    /// Maximum serialized mapper output size.
    pub max_mapped_bytes: usize,
    /// Failed attempts after which infrastructure recovery stops.
    pub max_attempts: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_records: 500,
            max_bytes: 1_048_576,
            flush_ms: 50,
            concurrency: 4,
            retained_records: 2_000,
            retained_bytes: 16_777_216,
            max_record_bytes: 1_048_576,
            max_mapped_bytes: 1_048_576,
            max_attempts: 5,
        }
    }
}

/// Validate limits, including headroom for processing one additional record.
/// The host must still reserve actual output and parameter bytes before allocating them.
pub fn check(config: &Config) -> Result<(), Error> {
    if config.max_records == 0
        || config.max_bytes == 0
        || config.flush_ms == 0
        || config.concurrency == 0
        || config.retained_records == 0
        || config.retained_bytes == 0
        || config.max_record_bytes == 0
        || config.max_mapped_bytes == 0
        || config.max_attempts == 0
    {
        return Err(invalid("microbatch limits must be positive"));
    }
    if config.max_record_bytes > config.max_bytes {
        return Err(invalid("max_record_bytes must not exceed max_bytes"));
    }
    if config.retained_records <= config.max_records {
        return Err(invalid(
            "retained_records must exceed max_records for admission headroom",
        ));
    }
    let minimum_bytes = reserve(config)?;
    if config.retained_bytes < minimum_bytes {
        return Err(invalid(
            "retained_bytes must cover six source and mapped serialization copies",
        ));
    }
    Ok(())
}

/// Bytes reserved before fetching and preparing one original, including serialization copies.
pub fn reserve(config: &Config) -> Result<usize, Error> {
    config
        .max_record_bytes
        .checked_add(config.max_mapped_bytes)
        .and_then(|bytes| bytes.checked_mul(6))
        .ok_or_else(|| invalid("microbatch byte limits overflow"))
}

/// Whether the current batch has reached a record, byte or age threshold.
/// The host uses monotonic elapsed time and does not submit an empty batch.
pub fn cut(records: usize, bytes: usize, age_ms: u64, config: &Config) -> bool {
    records >= config.max_records || bytes >= config.max_bytes || age_ms >= config.flush_ms
}

/// An overflow-safe admission check against both retained-resource limits.
/// Execution slots and reserved headroom are managed separately by the host.
pub fn admit(used_records: usize, used_bytes: usize, records: usize, bytes: usize, config: &Config) -> bool {
    used_records
        .checked_add(records)
        .is_some_and(|total| total <= config.retained_records)
        && used_bytes
            .checked_add(bytes)
            .is_some_and(|total| total <= config.retained_bytes)
}

/// A recovery instruction. Hosts must reject unknown action names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    /// One of `retry`, `stop`, `split` or `reject`.
    pub action: String,
    /// Zero-based backoff step for `retry`; zero for other actions.
    pub step: u64,
    /// Original-order left child length for `split`; zero for other actions.
    pub split: usize,
    /// Bounded durable diagnostic for `reject`. Driver error text may contain an entire input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection: Option<String>,
}

/// Decide recovery after an attempt has failed.
///
/// `retry` and `uncertain` replay the unchanged originals until the ceiling,
/// then stop without acknowledging or rejecting them. `isolate` requires a
/// known rollback and a record-local error: split original order and process
/// the left child first, with each child's attempt count reset. `invalid` is a
/// deterministic invalid-record verdict. `fatal` always stops the runner.
pub fn recover(kind: &str, attempt: u32, count: usize, max_attempts: u32) -> Result<Recovery, Error> {
    recover_with_rejections(kind, attempt, count, max_attempts, false)
}

/// Recovery when every original already carries a rejection. A failure to persist those
/// rejections is an infrastructure failure; rejecting them again cannot make progress.
pub fn recover_with_rejections(
    kind: &str,
    attempt: u32,
    count: usize,
    max_attempts: u32,
    rejected: bool,
) -> Result<Recovery, Error> {
    if count == 0 || max_attempts == 0 {
        return Err(invalid("recovery requires records and a positive attempt limit"));
    }
    let kind =
        FailureKind::by_name(kind).ok_or_else(|| invalid(format_args!("unknown microbatch recovery kind: {kind}")))?;
    let kind = if rejected && matches!(kind, FailureKind::Isolate | FailureKind::Invalid) {
        FailureKind::Retry
    } else {
        kind
    };
    let (action, step, split) = match kind {
        FailureKind::Fatal => (Action::Stop, 0, 0),
        FailureKind::Retry | FailureKind::Uncertain if attempt < max_attempts => {
            (Action::Retry, u64::from(attempt.saturating_sub(1)), 0)
        }
        FailureKind::Retry | FailureKind::Uncertain => (Action::Stop, 0, 0),
        FailureKind::Isolate if count > 1 => (Action::Split, 0, count / 2),
        FailureKind::Isolate | FailureKind::Invalid => (Action::Reject, 0, 0),
    };
    Ok(Recovery {
        action: action.as_str().to_owned(),
        step,
        split,
        rejection: (action == Action::Reject)
            .then(|| Error::new(ErrorCode::Rejected, "ingestion sink refused this original").to_string()),
    })
}

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::BadHandle, message)
}
