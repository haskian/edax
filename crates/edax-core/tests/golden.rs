//! Golden vectors for the shared primitives. Every binding's test suite reads the same files.
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-core --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use edax_core::{tenant_key, Backoff, ErrorCode, Tenancy, TENANT_MAX};
use edax_golden::write_or_check;
use serde_json::{json, Value};

/// One case per code: the wire name, the terminal / transient verdict, and a realistic message in
/// its wire form. The verdict is what an idempotency guard persists on, so two SDKs that disagree
/// on one row leave one caching a failure the other releases. The message pins the `"<code>: "`
/// framing every binding parses.
fn error_taxonomy_cases() -> Vec<Value> {
    // Exhaustive: a new code needs an example message here before the vector regenerates.
    let message = |code: ErrorCode| match code {
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
                "message": message(code),
                "wire": code.tag(message(code)),
            })
        })
        .collect()
}

/// The failure any wire string names and its verdict, the negative rows included: a binding that
/// splits the string or defaults the class itself would own a second copy of these rules. `code` is
/// `store` when the kernel finds no code, and `message` is then the whole string.
fn error_wire_cases() -> Vec<Value> {
    [
        ("known_terminal_code", "codec: invalid type: string, expected u64"),
        ("known_transient_code", "store: connection to the event store was reset"),
        ("separator_inside_the_message", "store: redis: connection reset"),
        ("empty_message", "concurrency: "),
        ("second_space_stays_in_the_message", "codec:  padded"),
        ("unknown_code", "timeout: deadline exceeded"),
        ("no_separator", "connection reset by peer"),
        ("colon_without_space", "codec:no space"),
        ("uppercase_code", "CODEC: invalid type"),
        ("leading_space", " codec: invalid type"),
        ("empty", ""),
    ]
    .into_iter()
    .map(|(name, wire)| {
        let error = edax_core::Error::from_wire(wire);
        json!({
            "name": name,
            "wire": wire,
            "code": error.code.as_str(),
            "message": error.message,
            "terminal": edax_core::verdict_of_wire(wire),
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
    let unbounded = Backoff { base_millis: u64::MAX, cap_millis: u64::MAX };
    cases.extend([
        (Backoff { base_millis: 0, cap_millis: 30_000 }, 5, u64::MAX),
        (Backoff { base_millis: 500, cap_millis: 50 }, 0, 77),
        (Backoff { base_millis: 250, cap_millis: 2_000 }, 3, 1_999),
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
    let mut cases: Vec<Value> = Vec::new();
    for mode in [Tenancy::Silo, Tenancy::Pool, Tenancy::Bridge] {
        for tenant in tenants {
            for key in keys {
                let outcome = tenant_key(mode, tenant, key);
                cases.push(json!({
                    "mode": mode.name(),
                    "requires_tenant": mode.requires_tenant(),
                    "composes_keys": mode.composes_keys(),
                    "tenant": tenant,
                    "key": key,
                    "expected": outcome.as_ref().ok(),
                    "error": outcome.as_ref().err().map(ToString::to_string),
                }));
            }
        }
    }
    cases
}

#[test]
fn golden() {
    write_or_check("error_taxonomy.json", error_taxonomy_cases());
    write_or_check("error_wire.json", error_wire_cases());
    write_or_check("backoff.json", backoff_cases());
    write_or_check("tenancy.json", tenancy_cases());
}
