//! Golden vectors for the shared primitives. Every binding's test suite reads the same files.
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-core --test golden`.

use edax_core::ErrorCode;
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
        ErrorCode::CachedFailure => "codec error: invalid type: string, expected u64",
        ErrorCode::HandlerNotFound => "Deposit",
        ErrorCode::AlreadyRegistered => "Deposit",
        ErrorCode::BadHandle => "unknown transaction 7: not begun, or already committed or rolled back",
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

#[test]
fn golden() {
    write_or_check("error_taxonomy.json", error_taxonomy_cases());
}
