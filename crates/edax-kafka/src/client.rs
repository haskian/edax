//! What a shell's Kafka client settings and client failures mean: the durations and the client
//! configurations a shell refuses at build, the producer setting it warns about, and the verdict on a
//! broker's protocol error.
//!
//! Both are decisions every SDK must make alike. A setting one shell refuses and another accepts is a
//! deployment that works in one language and spins in the other. A broker error one SDK dead-letters
//! and another retries is a record that dies in one service and blocks the outbox in the other.

use edax_core::{Error, ErrorCode};

/// Refuse the durations a Kafka shell is built with, in milliseconds, as a `bad_handle` error naming
/// the first one refused. `None` is a setting this shell or this call site does not take. Every
/// duration must be positive:
///
/// - `publish_timeout_ms` bounds one send, metadata lookup and ack included. With no time to land,
///   every send times out.
/// - `commit_interval_ms` paces the commit loop. At zero or below it sleeps for nothing and spins.
/// - `commit_timeout_ms` bounds one commit. With no time to land, nothing is ever committed.
/// - `drain_timeout_ms` bounds the wait for running handlers on a revoke. With no wait, a key still
///   running here is handed to the new owner at once, and two members run it together.
///
/// A shell refuses them when it is built, so a misconfigured consumer fails at startup rather than on
/// every record.
pub fn check_config(
    publish_timeout_ms: Option<i64>,
    commit_interval_ms: Option<i64>,
    commit_timeout_ms: Option<i64>,
    drain_timeout_ms: Option<i64>,
) -> Result<(), Error> {
    let settings = [
        ("publish_timeout_ms", publish_timeout_ms, "every send would time out"),
        ("commit_interval_ms", commit_interval_ms, "the commit loop would spin"),
        ("commit_timeout_ms", commit_timeout_ms, "no commit would ever land"),
        (
            "drain_timeout_ms",
            drain_timeout_ms,
            "a revoke would hand a running key to the new owner",
        ),
    ];
    for (name, value, why) in settings {
        if let Some(ms) = value.filter(|ms| *ms <= 0) {
            return Err(Error::bad_handle(format_args!(
                "{name} is {ms} ms; a duration must be positive, or {why}"
            )));
        }
    }
    Ok(())
}

/// Refuse a consumer client that loses or never commits offsets, as a `bad_handle` error. The shell
/// reads both facts from its client library; a fact it cannot read is passed as the safe value, so a
/// client the shell cannot judge is not refused.
///
/// - `auto_commit`: the client commits its fetch position on its own. That position runs ahead of the
///   deliveries a dispatcher still holds, so a restart skips them: lost, not redelivered.
/// - `has_group`: the client belongs to a consumer group. Without one there is nowhere to commit, so
///   every commit fails and a restart replays the partition from its reset policy.
pub fn check_consumer(auto_commit: bool, has_group: bool) -> Result<(), Error> {
    if auto_commit {
        return Err(Error::bad_handle(
            "the consumer client auto-commits its fetch position, which commits past deliveries still in flight and \
             loses them on a restart; turn auto-commit off",
        ));
    }
    if !has_group {
        return Err(Error::bad_handle(
            "the consumer client has no group id, so every commit fails and nothing is ever committed; give it a group id",
        ));
    }
    Ok(())
}

/// Refuse a consumer that runs with no dead-letter sink, as a `bad_handle` error. A Kafka consumer
/// rewinds its partition on a nack, so a message that fails for good comes back forever and stalls
/// every message behind it; only a sink settles it. The shell calls this when a dispatcher is handed
/// to the consumer, which may be after the consumer is built.
pub fn check_dead_sink(has_dead_sink: bool) -> Result<(), Error> {
    if has_dead_sink {
        return Ok(());
    }
    Err(Error::bad_handle(
        "the consumer runs a dispatcher with no dead-letter sink: a nack rewinds the partition, so a message that \
         fails for good stalls its partition forever; wire a dead-letter sink",
    ))
}

/// The `acks` setting that acknowledges a send only once every in-sync replica has it.
pub const ACKS_ALL: i64 = -1;

/// The warning a producer client built with `acks` earns, or `None` for [`ACKS_ALL`]. An outbox relay
/// marks a row published once the send is acknowledged, so an ack a failed leader takes with it is a
/// lost event. A warning and not a refusal: a producer that never relays an outbox may choose speed.
pub fn acks_warning(acks: i64) -> Option<String> {
    (acks != ACKS_ALL).then(|| {
        format!(
            "the producer client acks with acks={acks}, so a leader failover loses an acknowledged outbox event; build \
             it with acks=all ({ACKS_ALL}), which an idempotent producer implies"
        )
    })
}

// The Kafka protocol error codes this kernel classifies, by their protocol names. Every client
// reports the same number: aiokafka's `errno`, librdkafka's `code()`, Java's `Errors.code()`.
const MESSAGE_TOO_LARGE: i64 = 10;
const INVALID_TOPIC_EXCEPTION: i64 = 17;
const RECORD_LIST_TOO_LARGE: i64 = 18;
const INVALID_REQUIRED_ACKS: i64 = 21;
const TOPIC_AUTHORIZATION_FAILED: i64 = 29;
const GROUP_AUTHORIZATION_FAILED: i64 = 30;
const CLUSTER_AUTHORIZATION_FAILED: i64 = 31;
const INVALID_TIMESTAMP: i64 = 32;
const UNSUPPORTED_SASL_MECHANISM: i64 = 33;
const ILLEGAL_SASL_STATE: i64 = 34;
const UNSUPPORTED_VERSION: i64 = 35;
const UNSUPPORTED_FOR_MESSAGE_FORMAT: i64 = 43;
const INVALID_PRODUCER_EPOCH: i64 = 47;
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i64 = 53;
const SASL_AUTHENTICATION_FAILED: i64 = 58;
const INVALID_RECORD: i64 = 87;

/// The kernel error for a Kafka broker error, by its protocol error code. `message` is the client's
/// own text, kept for the log.
///
/// The code decides the verdict by blast radius:
/// - One record the broker refuses (`MESSAGE_TOO_LARGE`, `RECORD_LIST_TOO_LARGE`, `INVALID_RECORD`):
///   terminal `codec`. The same record fails the same way every time, and the next record may pass, so
///   the record is dead-lettered.
/// - A client the broker refuses: terminal `bad_handle`, a wiring fault. Every record fails the same
///   way, so the retry policy stops the consumer or the relay instead of dead-lettering them one by one
///   (`edax_event::Failure::Fatal`). That is a refused credential (the `*_AUTHORIZATION_FAILED` codes,
///   `SASL_AUTHENTICATION_FAILED`, `UNSUPPORTED_SASL_MECHANISM`, `ILLEGAL_SASL_STATE`), a refused setting
///   (`INVALID_TOPIC_EXCEPTION`, `INVALID_REQUIRED_ACKS`, `UNSUPPORTED_VERSION`,
///   `UNSUPPORTED_FOR_MESSAGE_FORMAT`), a fenced producer (`INVALID_PRODUCER_EPOCH`, which every later
///   send of that producer gets too), and `INVALID_TIMESTAMP`: no edax producer sets a record timestamp,
///   so the client stamps its clock and the broker refuses it only when that clock is skewed past the
///   topic's bound, for every record alike.
/// - Anything else is transient `store`: a code Kafka marks retriable (`CORRUPT_MESSAGE`, a checksum
///   failure in transit, `UNKNOWN_TOPIC_OR_PARTITION`, which a partition move reports for a moment,
///   `NOT_LEADER_OR_FOLLOWER`, `REQUEST_TIMED_OUT`) or a code this kernel does not know, a number outside
///   Kafka's 16 bits included. That is `edax_core::is_terminal`'s default: a wrongly transient failure is
///   retried loudly, a wrongly terminal one loses a record to the dead-letter queue.
///
/// A client failure with no protocol code (a client-side timeout, a lost connection) is not a broker
/// error: the shell leaves it unclassified, which is transient too.
pub fn broker_error(error_code: i64, message: &str) -> Error {
    let code = match error_code {
        MESSAGE_TOO_LARGE | RECORD_LIST_TOO_LARGE | INVALID_RECORD => ErrorCode::Codec,
        TOPIC_AUTHORIZATION_FAILED
        | GROUP_AUTHORIZATION_FAILED
        | CLUSTER_AUTHORIZATION_FAILED
        | TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        | SASL_AUTHENTICATION_FAILED
        | UNSUPPORTED_SASL_MECHANISM
        | ILLEGAL_SASL_STATE
        | INVALID_TOPIC_EXCEPTION
        | INVALID_REQUIRED_ACKS
        | UNSUPPORTED_VERSION
        | UNSUPPORTED_FOR_MESSAGE_FORMAT
        | INVALID_PRODUCER_EPOCH
        | INVALID_TIMESTAMP => ErrorCode::BadHandle,
        _ => ErrorCode::Store,
    };
    Error::new(code, format_args!("kafka broker error {error_code}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_config_branches() {
        struct Case {
            name: &'static str,
            settings: [Option<i64>; 4],
            refused: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "nothing set",
                settings: [None; 4],
                refused: None,
            },
            Case {
                name: "one millisecond is the floor",
                settings: [Some(1); 4],
                refused: None,
            },
            Case {
                name: "zero publish timeout",
                settings: [Some(0), None, None, None],
                refused: Some("publish_timeout_ms is 0 ms"),
            },
            Case {
                name: "negative commit interval",
                settings: [None, Some(-1), None, None],
                refused: Some("commit_interval_ms is -1 ms"),
            },
            Case {
                name: "zero commit timeout",
                settings: [None, Some(1), Some(0), None],
                refused: Some("commit_timeout_ms is 0 ms"),
            },
            Case {
                name: "zero drain timeout",
                settings: [None, None, None, Some(0)],
                refused: Some("drain_timeout_ms is 0 ms"),
            },
            Case {
                name: "the first refused is named",
                settings: [Some(0), Some(0), None, None],
                refused: Some("publish_timeout_ms"),
            },
        ];
        for c in cases {
            let [publish, interval, commit, drain] = c.settings;
            let got = check_config(publish, interval, commit, drain);
            match c.refused {
                None => assert_eq!(got, Ok(()), "{}", c.name),
                Some(text) => {
                    let error = got.expect_err(c.name);
                    assert_eq!(error.code, ErrorCode::BadHandle, "{}", c.name);
                    assert!(error.message.starts_with(text), "{}: {}", c.name, error.message);
                }
            }
        }
    }

    #[test]
    fn check_consumer_branches() {
        for (name, auto_commit, has_group, refused) in [
            ("a_group_without_auto_commit", false, true, None),
            ("auto_commit", true, true, Some("the consumer client auto-commits")),
            (
                "auto_commit_is_named_first",
                true,
                false,
                Some("the consumer client auto-commits"),
            ),
            ("no_group", false, false, Some("the consumer client has no group id")),
        ] {
            match (check_consumer(auto_commit, has_group), refused) {
                (Ok(()), None) => {}
                (Err(error), Some(text)) => {
                    assert_eq!(error.code, ErrorCode::BadHandle, "{name}");
                    assert!(error.message.starts_with(text), "{name}: {}", error.message);
                }
                (got, _) => panic!("{name}: {got:?}"),
            }
        }
    }

    #[test]
    fn check_dead_sink_branches() {
        assert_eq!(check_dead_sink(true), Ok(()), "a sink");
        let error = check_dead_sink(false).expect_err("no sink");
        assert_eq!(error.code, ErrorCode::BadHandle);
        assert!(
            error
                .message
                .starts_with("the consumer runs a dispatcher with no dead-letter sink"),
            "{}",
            error.message
        );
    }

    #[test]
    fn acks_warning_branches() {
        assert_eq!(acks_warning(ACKS_ALL), None, "all");
        for acks in [0, 1, 2] {
            let warning = acks_warning(acks).expect("a leader-only ack");
            assert!(
                warning.starts_with(&format!("the producer client acks with acks={acks},")),
                "{warning}"
            );
        }
    }

    #[test]
    fn broker_error_classifies_by_blast_radius() {
        for code in [MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE, INVALID_RECORD] {
            assert_eq!(broker_error(code, "x").code, ErrorCode::Codec, "{code}");
        }
        for code in [
            TOPIC_AUTHORIZATION_FAILED,
            GROUP_AUTHORIZATION_FAILED,
            CLUSTER_AUTHORIZATION_FAILED,
            TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            SASL_AUTHENTICATION_FAILED,
            UNSUPPORTED_SASL_MECHANISM,
            ILLEGAL_SASL_STATE,
            INVALID_TOPIC_EXCEPTION,
            INVALID_REQUIRED_ACKS,
            UNSUPPORTED_VERSION,
            UNSUPPORTED_FOR_MESSAGE_FORMAT,
            INVALID_PRODUCER_EPOCH,
            INVALID_TIMESTAMP,
        ] {
            assert_eq!(broker_error(code, "x").code, ErrorCode::BadHandle, "{code}");
        }
        // CORRUPT_MESSAGE (2) is a checksum failure in transit, which Kafka retries.
        for code in [2, 3, 6, 7, 999, i64::from(i16::MAX) + 1, -1] {
            assert_eq!(broker_error(code, "x").code, ErrorCode::Store, "{code}");
        }
        assert_eq!(
            broker_error(29, "Topic authorization failed").to_string(),
            "bad_handle: kafka broker error 29: Topic authorization failed"
        );
    }
}
