//! The failure type every adapter returns, and its wire form.

use edax_core::ErrorCode;

/// A failure of an in-memory adapter. Typed, so a Rust consumer reads the fields instead of parsing
/// them back out of a message. Four codes of the taxonomy are here — `concurrency`, `codec`, `store`
/// and `bad_handle`; the rest are raised inside an SDK, never by a store.
///
/// `Display` is the one place that renders the `"<code>: <message>"` wire form, through
/// [`ErrorCode::tag`]. A binding boundary calls `to_string()` and adds nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError {
    /// The expected version did not match the stored one: a concurrent writer got there first.
    /// Versions are the adapters' `i64`; the SDK converts at its seam.
    Concurrency { expected: i64, stored: i64 },
    /// A deterministic failure: malformed JSON, or a batch that fails the same way on every retry.
    Codec(String),
    /// The store failed for a reason other than a conflict. No in-memory adapter raises it; it is
    /// the variant a durable store maps a backend failure to.
    Store(String),
    /// The transaction token names no open transaction: never begun, or already settled. Terminal,
    /// because the same token fails the same way on every retry.
    BadHandle(String),
}

impl AdapterError {
    /// The kernel's code for this failure, which carries the terminal / transient verdict. Exhaustive: a new
    /// variant must name its code.
    pub fn code(&self) -> ErrorCode {
        match self {
            AdapterError::Concurrency { .. } => ErrorCode::Concurrency,
            AdapterError::Codec(_) => ErrorCode::Codec,
            AdapterError::Store(_) => ErrorCode::Store,
            AdapterError::BadHandle(_) => ErrorCode::BadHandle,
        }
    }
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let wire = match self {
            AdapterError::Concurrency { expected, stored } => {
                self.code().tag(format_args!("expected {expected}, stored {stored}"))
            }
            AdapterError::Codec(message) | AdapterError::Store(message) | AdapterError::BadHandle(message) => {
                self.code().tag(message)
            }
        };
        f.write_str(&wire)
    }
}

impl std::error::Error for AdapterError {}

/// A deterministic marshaling failure, so a host does not retry it.
pub(crate) fn codec_error(cause: impl std::fmt::Display) -> AdapterError {
    AdapterError::Codec(cause.to_string())
}

#[cfg(test)]
// A test asserts on the value it just built; a panic there is the failure report.
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    #[test]
    fn adapter_error_display_is_the_tagged_wire_form() {
        // One case per variant. The wire string is what a binding crosses and what its parser
        // (`ErrorCode::split`, Python's `from_runtime`) takes apart, so the round trip is pinned too.
        struct Case {
            name: &'static str,
            error: AdapterError,
            code: ErrorCode,
            wire: &'static str,
        }
        let cases = [
            Case {
                name: "concurrency_carries_both_versions_in_order",
                error: AdapterError::Concurrency { expected: 3, stored: 4 },
                code: ErrorCode::Concurrency,
                wire: "concurrency: expected 3, stored 4",
            },
            Case {
                name: "concurrency_renders_a_negative_version_as_is",
                error: AdapterError::Concurrency { expected: -1, stored: i64::MAX },
                code: ErrorCode::Concurrency,
                wire: "concurrency: expected -1, stored 9223372036854775807",
            },
            Case {
                name: "codec_is_terminal",
                error: AdapterError::Codec("invalid type: string, expected u64".to_owned()),
                code: ErrorCode::Codec,
                wire: "codec: invalid type: string, expected u64",
            },
            Case {
                name: "store_is_transient",
                error: AdapterError::Store("connection to the event store was reset".to_owned()),
                code: ErrorCode::Store,
                wire: "store: connection to the event store was reset",
            },
            Case {
                // The message may itself hold the separator; the code is still the first segment.
                name: "codec_message_with_a_separator_inside",
                error: codec_error("redis: connection reset"),
                code: ErrorCode::Codec,
                wire: "codec: redis: connection reset",
            },
        ];
        for case in cases {
            assert_eq!(case.error.code(), case.code, "{} code", case.name);
            assert_eq!(case.error.to_string(), case.wire, "{} wire", case.name);
            let message = case.wire.split_once(": ").map(|(_, m)| m);
            assert_eq!(ErrorCode::split(case.wire), message.map(|m| (case.code, m)), "{} split", case.name);
        }
    }
}
