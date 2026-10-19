//! Microbatch admission, flush and recovery decisions shared by ingestion SDKs.
//!
//! Recovery reads a failure by its error code through [`crate::retry::Failure::of`], the one
//! classifier every runner shares. Infrastructure failures stop at the retry ceiling. Only a
//! deterministic failure with a proven rollback permits isolation and rejection. Hosts retain admission
//! credits until the tasks and serialized data that consume them are released.

use edax_core::{Error, ErrorCode};
use serde::{Deserialize, Serialize};

use crate::retry::Failure;

edax_core::closed_names! {
    /// The action a host must take after a microbatch failure.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Action {
        /// Stop without acknowledging or rejecting the originals.
        Stop = "stop",
        /// Back off and retry the unchanged originals.
        Retry = "retry",
        /// Another owner holds the work: back off and retry without spending an attempt.
        Wait = "wait",
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

/// Retained bytes a prepared input of `serialized_bytes` counts against `retained_bytes`: the input and
/// its transaction copies. One rule for every runner. An input whose charge exceeds the admission
/// reserve is refused, since admission could not have made room for it.
pub fn charge(serialized_bytes: usize, config: &Config) -> Result<usize, Error> {
    let charge = serialized_bytes
        .checked_mul(SERIALIZED_COPIES)
        .ok_or_else(|| invalid("prepared input size overflows"))?;
    if charge > reserve(config)? {
        return Err(Error::bad_handle(
            "prepared input exceeds the reserved serialized memory",
        ));
    }
    Ok(charge)
}

/// The prepared input, the copy a transaction encodes and the copy the store reads back.
const SERIALIZED_COPIES: usize = 3;

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
    /// One of `stop`, `retry`, `wait`, `split` or `reject`.
    pub action: String,
    /// Zero-based backoff step for `retry` and `wait`; zero for other actions.
    pub step: u64,
    /// Original-order left child length for `split`; zero for other actions.
    pub split: usize,
    /// Bounded durable diagnostic for `reject`. Driver error text may contain an entire input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection: Option<String>,
}

/// What the host knows about one failed attempt, beyond its error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attempt {
    /// Failed attempts of these originals so far, this one included.
    pub attempt: u32,
    /// Consecutive `wait` verdicts so far, this one included; paces contention.
    pub waits: u32,
    /// Originals in the failed attempt.
    pub count: usize,
    /// The attempt ceiling for transient failures.
    pub max_attempts: u32,
    /// Whether the store proved nothing was written: a confirmed rollback before any commit.
    pub rolled_back: bool,
    /// Whether the host may split and reject these originals. False when they already carry a
    /// rejection, so rejecting them again cannot make progress, or when the host has no rejection path.
    pub isolable: bool,
}

/// Decide recovery after an attempt failed with error `code` (its wire name; `None` for a failure
/// no code classifies).
///
/// - `fatal`: `stop`; the wiring is wrong, so every attempt fails alike.
/// - `contended`: `wait` at step `waits - 1`, without spending an attempt.
/// - `transient`, or `terminal` without a proven rollback (the write may have landed, so the replay
///   decides): `retry` at step `attempt - 1` until `max_attempts`, then `stop` without
///   acknowledging or rejecting the originals.
/// - `terminal` with a proven rollback: `split` in original order, left child first, while more
///   than one original remains, then `reject`; `stop` when the originals are not isolable.
pub fn recover(code: Option<&str>, attempt: Attempt) -> Result<Recovery, Error> {
    if attempt.count == 0 || attempt.max_attempts == 0 {
        return Err(invalid("recovery requires records and a positive attempt limit"));
    }
    let code = code.map(str::parse::<ErrorCode>).transpose()?;
    let (action, step, split) = match Failure::of(code) {
        Failure::Fatal => (Action::Stop, 0, 0),
        Failure::Contended => (Action::Wait, u64::from(attempt.waits.saturating_sub(1)), 0),
        Failure::Terminal if attempt.rolled_back && !attempt.isolable => (Action::Stop, 0, 0),
        Failure::Terminal if attempt.rolled_back && attempt.count > 1 => (Action::Split, 0, attempt.count / 2),
        Failure::Terminal if attempt.rolled_back => (Action::Reject, 0, 0),
        Failure::Transient | Failure::Terminal if attempt.attempt < attempt.max_attempts => {
            (Action::Retry, u64::from(attempt.attempt.saturating_sub(1)), 0)
        }
        Failure::Transient | Failure::Terminal => (Action::Stop, 0, 0),
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
