use edax_core::ErrorCode;
use edax_event::batch::{admit, check, cut, recover, Attempt, Config};

#[test]
fn defaults_and_partial_json_are_valid() {
    let config = Config::default();
    assert_eq!(check(&config), Ok(()));
    assert_eq!(serde_json::from_str::<Config>("{}").unwrap(), config);
    let configured: Config = serde_json::from_str(r#"{"concurrency":2}"#).unwrap();
    assert_eq!(
        configured,
        Config {
            concurrency: 2,
            ..config
        }
    );
    for json in [
        r#"{"max_records":-1}"#,
        r#"{"max_records":1.5}"#,
        r#"{"flush_ms":true}"#,
        r#"{"unknown":1}"#,
    ] {
        assert!(serde_json::from_str::<Config>(json).is_err(), "{json}");
    }
}

#[test]
fn invalid_limits_fail_before_admission() {
    let defaults = Config::default();
    let cases = [
        (
            "records",
            Config {
                max_records: 0,
                ..defaults.clone()
            },
        ),
        (
            "bytes",
            Config {
                max_bytes: 0,
                ..defaults.clone()
            },
        ),
        (
            "age",
            Config {
                flush_ms: 0,
                ..defaults.clone()
            },
        ),
        (
            "concurrency",
            Config {
                concurrency: 0,
                ..defaults.clone()
            },
        ),
        (
            "retained records",
            Config {
                retained_records: 0,
                ..defaults.clone()
            },
        ),
        (
            "retained bytes",
            Config {
                retained_bytes: 0,
                ..defaults.clone()
            },
        ),
        (
            "source bytes",
            Config {
                max_record_bytes: 0,
                ..defaults.clone()
            },
        ),
        (
            "mapped bytes",
            Config {
                max_mapped_bytes: 0,
                ..defaults.clone()
            },
        ),
        (
            "attempts",
            Config {
                max_attempts: 0,
                ..defaults.clone()
            },
        ),
        (
            "record exceeds batch",
            Config {
                max_record_bytes: defaults.max_bytes + 1,
                ..defaults.clone()
            },
        ),
        (
            "no record headroom",
            Config {
                retained_records: defaults.max_records,
                ..defaults.clone()
            },
        ),
        (
            "insufficient byte headroom",
            Config {
                retained_bytes: 4_194_303,
                ..defaults.clone()
            },
        ),
        (
            "addition overflow",
            Config {
                max_mapped_bytes: usize::MAX,
                ..defaults.clone()
            },
        ),
        (
            "doubling overflow",
            Config {
                max_mapped_bytes: usize::MAX / 2,
                ..defaults.clone()
            },
        ),
    ];
    for (name, config) in cases {
        assert_eq!(check(&config).unwrap_err().code, ErrorCode::BadHandle, "{name}");
    }
    assert_eq!(
        check(&Config {
            retained_bytes: 12_582_912,
            ..defaults
        }),
        Ok(())
    );
}

#[test]
fn cut_honors_each_threshold() {
    let config = Config::default();
    for (records, bytes, age, expected) in [
        (499, 1_048_575, 49, false),
        (500, 1, 0, true),
        (1, 1_048_576, 0, true),
        (1, 1, 50, true),
        (501, 1_048_577, 51, true),
        (0, 0, 0, false),
    ] {
        assert_eq!(cut(records, bytes, age, &config), expected);
    }
}

#[test]
fn admission_enforces_exact_caps_without_overflow() {
    let config = Config::default();
    for (used_records, used_bytes, records, bytes, expected) in [
        (0, 0, 1, 1, true),
        (1_999, 16_777_215, 1, 1, true),
        (2_000, 0, 1, 0, false),
        (0, 16_777_216, 0, 1, false),
        (usize::MAX, 0, 1, 0, false),
        (0, usize::MAX, 0, 1, false),
    ] {
        assert_eq!(admit(used_records, used_bytes, records, bytes, &config), expected);
    }
}

fn attempt(attempt: u32, count: usize, rolled_back: bool) -> Attempt {
    Attempt {
        attempt,
        waits: 0,
        count,
        max_attempts: 5,
        rolled_back,
        isolable: true,
    }
}

#[test]
fn outage_and_uncertain_commit_never_reject_or_split() {
    // A transient code, and a terminal one whose write may have landed, replay the originals.
    for (code, rolled_back) in [(Some("store"), true), (None, true), (Some("codec"), false)] {
        for n in 0..=6 {
            let decision = recover(code, attempt(n, 500, rolled_back)).unwrap();
            assert_eq!(decision.action, if n < 5 { "retry" } else { "stop" });
            assert_eq!(decision.step, if n < 5 { u64::from(n.saturating_sub(1)) } else { 0 });
            assert_eq!(decision.split, 0);
        }
    }
    assert_eq!(
        recover(Some("bad_handle"), attempt(1, 500, true)).unwrap().action,
        "stop"
    );
}

#[test]
fn contention_waits_without_spending_attempts() {
    for waits in 1..=3 {
        let decision = recover(
            Some("in_progress"),
            Attempt {
                waits,
                ..attempt(99, 500, false)
            },
        )
        .unwrap();
        assert_eq!(
            (decision.action.as_str(), decision.step),
            ("wait", u64::from(waits - 1))
        );
    }
}

#[test]
fn isolation_splits_originals_into_nonempty_ordered_children() {
    let originals = [
        "order-created",
        "order-paid",
        "order-shipped",
        "order-delivered",
        "order-closed",
    ];
    for count in 2..=originals.len() {
        let decision = recover(Some("codec"), attempt(1, count, true)).unwrap();
        assert_eq!(decision.action, "split");
        assert_eq!(decision.split, count / 2);
        let (left, right) = originals[..count].split_at(decision.split);
        assert!(!left.is_empty() && !right.is_empty());
        assert_eq!([left, right].concat(), originals[..count]);
    }
    let decision = recover(Some("codec"), attempt(1, 1, true)).unwrap();
    assert_eq!(decision.action, "reject");
    assert_eq!((decision.step, decision.split), (0, 0));
    let held = recover(
        Some("codec"),
        Attempt {
            isolable: false,
            ..attempt(1, 1, true)
        },
    )
    .unwrap();
    assert_eq!(held.action, "stop");
    for (code, count, attempts) in [("store", 0, 5), ("store", 1, 0)] {
        assert_eq!(
            recover(
                Some(code),
                Attempt {
                    max_attempts: attempts,
                    ..attempt(1, count, true)
                }
            )
            .unwrap_err()
            .code,
            ErrorCode::BadHandle
        );
    }
    assert_eq!(
        recover(Some("unknown"), attempt(1, 1, true)).unwrap_err().code,
        ErrorCode::Codec
    );
}
