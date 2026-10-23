//! Golden vectors for the shared primitives. Every binding's test suite reads the same files.
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-core --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax_core::{payload_hash, tenant_key, Backoff, ErrorCode, Tenancy, TENANT_MAX};
use edax_golden::{refusal, write_or_check};
use serde_json::{json, Value};

/// One case per code: the wire name, the terminal / transient verdict, whether it is fatal (never
/// cached, the consumer stops on it), and a realistic message in
/// its wire form. The verdict is what an idempotency guard persists on, so two SDKs that disagree
/// on one row leave one caching a failure the other releases. The message pins the `"<code>: "`
/// framing every binding parses.
fn error_taxonomy_cases() -> Vec<Value> {
    // Exhaustive: a new code needs an example message here before the vector regenerates.
    let message = |code: ErrorCode| match code {
        ErrorCode::Shredded => "scope destroyed",
        ErrorCode::KeyUnavailable => "provider unavailable",
        ErrorCode::KeyRevoking => "scope revoking",
        ErrorCode::KeyMissing => "generation missing",
        ErrorCode::KeyUnauthorized => "access denied",
        ErrorCode::OwnerMismatch => "protected state owner is immutable",
        ErrorCode::Concurrency => "expected 3, stored 4",
        ErrorCode::Codec => "invalid type: string, expected u64",
        ErrorCode::Store => "connection to the event store was reset",
        ErrorCode::PayloadMismatch => "acct-1-3",
        ErrorCode::InProgress => "acct-1-3",
        ErrorCode::CachedFailure => "codec: invalid type: string, expected u64",
        ErrorCode::HandlerNotFound => "Deposit",
        ErrorCode::AlreadyRegistered => "Deposit",
        ErrorCode::BadHandle => "unknown transaction 7: not begun, or already committed or rolled back",
        ErrorCode::Rejected => "insufficient funds: balance 300, withdrawal 500",
    };
    ErrorCode::ALL
        .iter()
        .map(|&code| {
            json!({
                "code": code.as_str(),
                "terminal": code.is_terminal(),
                "fatal": code.is_fatal(),
                "message": message(code),
                "wire": code.tag(message(code)),
            })
        })
        .collect()
}

/// The failure any wire string names and its verdict, the negative rows included: a binding that
/// splits the string or defaults the class itself would own a second copy of these rules. `code` is
/// `store` when the kernel finds no code, and `message` is then the whole string.
///
/// Each row carries its expected `(code, message, terminal)` written by hand, and the generator asserts
/// the kernel agrees before it writes the row: the vector is derived from the kernel, so without this a
/// wrong rule would regenerate as the contract.
fn error_wire_cases() -> Vec<Value> {
    use ErrorCode::{Codec, Concurrency, Store};
    let rows: [(&str, &str, (ErrorCode, &str, bool)); 11] = [
        (
            "known_terminal_code",
            "codec: invalid type: string, expected u64",
            (Codec, "invalid type: string, expected u64", true),
        ),
        (
            "known_transient_code",
            "store: connection to the event store was reset",
            (Store, "connection to the event store was reset", false),
        ),
        (
            "separator_inside_the_message",
            "store: redis: connection reset",
            (Store, "redis: connection reset", false),
        ),
        ("empty_message", "concurrency: ", (Concurrency, "", false)),
        (
            "second_space_stays_in_the_message",
            "codec:  padded",
            (Codec, " padded", true),
        ),
        // No code the kernel knows: the whole string is a store failure's message.
        (
            "unknown_code",
            "timeout: deadline exceeded",
            (Store, "timeout: deadline exceeded", false),
        ),
        (
            "no_separator",
            "connection reset by peer",
            (Store, "connection reset by peer", false),
        ),
        (
            "colon_without_space",
            "codec:no space",
            (Store, "codec:no space", false),
        ),
        (
            "uppercase_code",
            "CODEC: invalid type",
            (Store, "CODEC: invalid type", false),
        ),
        (
            "leading_space",
            " codec: invalid type",
            (Store, " codec: invalid type", false),
        ),
        ("empty", "", (Store, "", false)),
    ];
    assert_eq!(
        edax_core::is_terminal(None),
        Store.is_terminal(),
        "a failure no code classifies is a store failure"
    );
    rows.into_iter()
        .map(|(name, wire, (code, message, terminal))| {
            let error = edax_core::Error::from_wire(wire);
            assert_eq!(
                (error.code, error.message.as_str(), error.is_terminal()),
                (code, message, terminal),
                "{name}"
            );
            json!({
                "name": name,
                "wire": wire,
                "code": error.code.as_str(),
                "message": error.message,
                "terminal": error.is_terminal(),
            })
        })
        .collect()
}

/// `(policy, attempt, entropy) -> millis`. The default policy walks the schedule past the cap and
/// across the shift traps (`63`/`64` catch a naive `1 << attempt` port, `u32::MAX` and `u64::MAX` a
/// port that doubles in a loop); the other policies pin the edges of the policy itself.
fn backoff_cases() -> Vec<Value> {
    let default = Backoff::DEFAULT;
    let attempts = (0u64..12).chain([20, 63, 64, 100, 1000, u64::from(u32::MAX), u64::MAX]);
    let mut cases: Vec<(Backoff, u64, u64)> = Vec::new();
    for attempt in attempts {
        // The ceiling itself, one past it, and a large draw: the three sides of the modulo.
        let ceiling = default.ceiling_millis(attempt);
        for entropy in [ceiling, ceiling + 1, 0x9e37_79b9_7f4a_7c15] {
            cases.push((default, attempt, entropy));
        }
    }
    let unbounded = Backoff {
        base_millis: u64::MAX,
        cap_millis: u64::MAX,
    };
    cases.extend([
        (
            Backoff {
                base_millis: 0,
                cap_millis: 30_000,
            },
            5,
            u64::MAX,
        ),
        (
            Backoff {
                base_millis: 500,
                cap_millis: 50,
            },
            0,
            77,
        ),
        (
            Backoff {
                base_millis: 250,
                cap_millis: 2_000,
            },
            3,
            1_999,
        ),
        (unbounded, 0, u64::MAX),
        (unbounded, 7, 12_345),
    ]);
    cases
        .into_iter()
        .map(|(policy, attempt, entropy)| {
            json!({
                "base_millis": policy.base_millis,
                "cap_millis": policy.cap_millis,
                "attempt": attempt,
                "entropy": entropy,
                "ceiling_millis": policy.ceiling_millis(attempt),
                "expected_millis": edax_core::backoff_millis(policy, attempt, entropy),
            })
        })
        .collect()
}

/// `(mode, tenant, key) -> the key a seam is called with`, and the error when the mode wants a
/// tenant and the call has none. Only a pool composes, so a binding that composed in a bridge would
/// read keys no other service writes. The odd tenants pin the injectivity the length prefix buys:
/// `a:b` + `c` and `a` + `b:c` must not meet. The long ones pin the limit in bytes, not characters:
/// 32 `é` are 64 bytes and pass, 33 are 33 characters and 66 bytes and fail, which a binding that
/// counts characters would accept.
fn tenancy_cases() -> Vec<Value> {
    let keys = ["order-1", "", "a:b", "héllo"];
    let at_limit = "a".repeat(TENANT_MAX);
    let over_limit = "a".repeat(TENANT_MAX + 1);
    let wide_at_limit = "é".repeat(TENANT_MAX / 2);
    let wide_over_limit = "é".repeat(TENANT_MAX / 2 + 1);
    let tenants = [
        None,
        Some(""),
        Some("acme"),
        Some("a:b"),
        Some("a"),
        Some("é"),
        Some(at_limit.as_str()),
        Some(over_limit.as_str()),
        Some(wide_at_limit.as_str()),
        Some(wide_over_limit.as_str()),
    ];
    // One row per branch of the rule, its expected key or refusal written by hand: the vector is
    // derived from the kernel, so these are the check that the rule is the intended one.
    let pinned: [(Tenancy, Option<&str>, Result<&str, &str>); 9] = [
        (Tenancy::Silo, None, Ok("order-1")),
        (
            Tenancy::Silo,
            Some(""),
            Err("codec: tenant id is empty: pass none, or pass a name"),
        ),
        (Tenancy::Pool, Some("acme"), Ok("4:acme:order-1")),
        (Tenancy::Pool, Some("a:b"), Ok("3:a:b:order-1")),
        (
            Tenancy::Pool,
            None,
            Err("codec: tenancy pool requires a tenant, and none was given"),
        ),
        (Tenancy::Bridge, Some("acme"), Ok("order-1")),
        (
            Tenancy::Bridge,
            None,
            Err("codec: tenancy bridge requires a tenant, and none was given"),
        ),
        (Tenancy::Silo, Some(wide_at_limit.as_str()), Ok("order-1")),
        (
            Tenancy::Silo,
            Some(wide_over_limit.as_str()),
            Err("codec: tenant id is 66 bytes, over the 64 the key rule allows"),
        ),
    ];
    for (mode, tenant, want) in pinned {
        let got = tenant_key(mode, tenant, "order-1").map_err(|e| e.to_string());
        assert_eq!(got.as_deref().map_err(String::as_str), want, "{mode:?} {tenant:?}");
    }
    let mut cases: Vec<Value> = Vec::new();
    for &mode in Tenancy::ALL {
        for tenant in tenants {
            for key in keys {
                let outcome = tenant_key(mode, tenant, key);
                cases.push(json!({
                    "mode": mode.as_str(),
                    "requires_tenant": mode.requires_tenant(),
                    "composes_keys": mode.composes_keys(),
                    "tenant": tenant,
                    "key": key,
                    "expected": outcome.as_ref().ok(),
                    "error": refusal(&outcome),
                }));
            }
        }
    }
    cases
}

/// The hash every idempotency guard binds its key to, over bytes: FNV-1a 64-bit.
fn payload_hash_cases() -> Vec<Value> {
    // Last case hashes multibyte UTF-8 bytes, pinning agreement on non-ASCII input.
    ["", "a", "foobar", r#"{"email":"a@b.c"}"#, "user.create", "café ☕ 🦀"]
        .iter()
        .map(|s| json!({ "input": s, "expected": payload_hash(s.as_bytes()) }))
        .collect()
}

#[test]
fn golden() {
    write_or_check("error_taxonomy.json", error_taxonomy_cases());
    write_or_check("error_wire.json", error_wire_cases());
    write_or_check("backoff.json", backoff_cases());
    write_or_check("tenancy.json", tenancy_cases());
    write_or_check("payload_hash.json", payload_hash_cases());
}
