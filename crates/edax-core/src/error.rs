//! The error taxonomy: every kind of failure the toolkit reports, and the one decision that must
//! match across languages — whether the kind is *terminal*.
//!
//! Terminality decides what gets persisted. A terminal failure fails the same way on every retry,
//! so the idempotency guard caches it under the key and every later claimant replays it. A
//! transient failure releases the claim so the next attempt re-runs. Two SDKs that share one
//! idempotency store and disagree on a verdict leave one caching a failure the other would release:
//! a durable replay inconsistency. So the verdict lives here, pinned by
//! `testdata/golden/error_taxonomy.json`, and every SDK derives its own from [`ErrorCode::is_terminal`].
//! The verdict for a failure with no code is here too ([`is_terminal`]): such a failure is a `store`
//! failure, the same class and the same verdict, pinned by `testdata/golden/error_wire.json`.
//!
//! A *fatal* code ([`ErrorCode::is_fatal`]) is terminal but blames the wiring, not the work: it is
//! never cached, so the work runs again once the host fixes the wiring.
//!
//! Every kernel function fails with the one [`Error`] type, so the code comes from the kernel and
//! never from a caller's choice. Failures cross a native binding as `"<code>: <message>"`:
//! [`ErrorCode::tag`] is the one place that builds that string and [`Error::from_wire`] the one
//! place that takes it apart.

/// The `"<code>: <message>"` wire form splits on the first `": "`. A bare colon is part of the message.
const SEPARATOR: &str = ": ";

/// The taxonomy table. One row per code: the variant, its wire code and its verdict.
///
/// A macro rather than a hand-written enum so that the variant list, [`ErrorCode::ALL`],
/// [`ErrorCode::as_str`] and [`ErrorCode::is_terminal`] come from one row list and cannot drift: a
/// new code is one new row, and a row cannot be added without a verdict. The expanded matches are
/// exhaustive with no wildcard.
macro_rules! error_codes {
    ($( $(#[$doc:meta])* $variant:ident = $code:literal => $verdict:ident ),+ $(,)?) => {
        /// A kind of failure. Not `#[non_exhaustive]`: a downstream `match` over it must stay
        /// exhaustive, so that a new code is a compile error in every SDK instead of a wildcard's
        /// silent default.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $( $(#[$doc])* $variant, )+
        }

        impl ErrorCode {
            /// Every code, in declaration order. The order is what the golden vector pins.
            pub const ALL: &'static [ErrorCode] = &[ $( ErrorCode::$variant, )+ ];

            /// Every code's name, in declaration order: what a binding hands its host to check against.
            pub const NAMES: &'static [&'static str] = &[ $( $code, )+ ];

            /// Stable lowercase name — the single source of truth for the string form across bindings.
            pub fn as_str(self) -> &'static str {
                match self {
                    $( ErrorCode::$variant => $code, )+
                }
            }

            /// Whether the failure repeats identically on retry. Terminal means: never re-run the same
            /// work, it fails alike. Transient means: release the claim and let a retry re-run. Every
            /// fatal code is terminal.
            pub fn is_terminal(self) -> bool {
                match self {
                    $( ErrorCode::$variant => verdict!($verdict), )+
                }
            }

            /// Whether the failure is the wiring's, not the work's: a refused credential, topic or
            /// setting fails every message alike until the host fixes it. A consumer or a relay stops on
            /// it and hands the message back uncounted, and an idempotency guard releases the claim
            /// rather than cache it, so the work runs again once the wiring is fixed.
            pub fn is_fatal(self) -> bool {
                match self {
                    $( ErrorCode::$variant => fatal!($verdict), )+
                }
            }
        }
    };
}

/// A row's verdict, spelled as the word each SDK uses: whether it is terminal.
macro_rules! verdict {
    (terminal) => {
        true
    };
    (transient) => {
        false
    };
    (fatal) => {
        true
    };
}

/// Whether a row's verdict is fatal.
macro_rules! fatal {
    (fatal) => {
        true
    };
    ($verdict:ident) => {
        false
    };
}

error_codes! {
    /// An expected version did not match the stored one: a concurrent writer got there first.
    /// Reload and retry.
    Concurrency = "concurrency" => transient,
    /// A value could not be marshaled or unmarshaled. The same bytes fail the same way every time.
    Codec = "codec" => terminal,
    /// The backing store failed for a reason other than a version conflict.
    Store = "store" => transient,
    /// An idempotency key was reused with a different payload. An integrity error, not a duplicate.
    PayloadMismatch = "payload_mismatch" => terminal,
    /// The idempotency key is claimed by another run that has not completed. Retry so the winner's
    /// outcome can replay once it settles.
    InProgress = "in_progress" => transient,
    /// Replay of a terminal failure cached under the idempotency key. Terminal in its own right.
    CachedFailure = "cached_failure" => terminal,
    /// No handler is registered for the command type. A redelivery finds the same registry.
    HandlerNotFound = "handler_not_found" => terminal,
    /// A handler is already registered for this type: a wiring mistake at startup.
    AlreadyRegistered = "already_registered" => terminal,
    /// A transaction handle names no open transaction — never begun, already settled, or belonging
    /// to another store — or a store refuses its own configuration or a server refuses the command.
    /// A wiring mistake: the same call fails the same way every time, so a transient verdict would
    /// have a host retry it forever. Fatal: the message is not at fault, so it is neither cached nor
    /// dead-lettered.
    BadHandle = "bad_handle" => fatal,
    /// A host's own business rejection ("insufficient funds"), marked terminal by the host. The same
    /// command is rejected the same way on every retry, so the guard caches it.
    Rejected = "rejected" => terminal,
    /// The trusted key provider confirms permanent destruction of this scope.
    Shredded = "shredded" => terminal,
    /// Provider I/O is unavailable; defer without charging poison-message attempts.
    KeyUnavailable = "key_unavailable" => transient,
    /// Permanent revocation is installed and destruction is still in progress.
    KeyRevoking = "key_revoking" => transient,
    /// An expected generation does not exist; absence is not evidence of erasure.
    KeyMissing = "key_missing" => fatal,
    /// Provider access or configuration does not authorize the requested scope.
    KeyUnauthorized = "key_unauthorized" => fatal,
}

impl ErrorCode {
    /// Build the wire form `"<code>: <message>"`.
    pub fn tag(self, message: impl std::fmt::Display) -> String {
        format!("{}{SEPARATOR}{message}", self.as_str())
    }

    /// Take a wire form apart into its code and message. `None` if there is no `": "` separator or
    /// the code before it is unknown; [`Error::from_wire`] then classifies the whole string.
    pub fn split(wire: &str) -> Option<(ErrorCode, &str)> {
        let (code, message) = wire.split_once(SEPARATOR)?;
        code.parse().ok().map(|c| (c, message))
    }
}

/// Whether a failure is terminal, from its code if it has one. A failure no code classifies — a host's
/// own error, a wire string with an unknown code — is a `store` failure, so transient: a wrongly
/// transient failure is redelivered loudly and the work survives, while a wrongly terminal one is
/// cached under its key and poisons it. The default lives here, pinned by
/// `testdata/golden/error_wire.json`, so two SDKs sharing one idempotency store cannot pick
/// different ones.
pub fn is_terminal(code: Option<ErrorCode>) -> bool {
    code.unwrap_or(ErrorCode::Store).is_terminal()
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ErrorCode {
    type Err = Error;

    /// The code for a stable name. A name the taxonomy does not know is a `codec` error: the same
    /// name fails the same way every time.
    fn from_str(code: &str) -> Result<Self, Self::Err> {
        ErrorCode::ALL
            .iter()
            .copied()
            .find(|c| c.as_str() == code)
            .ok_or_else(|| Error::codec(format_args!("unknown error code: {code}")))
    }
}

/// A failure, as every kernel function and every SDK reports it: a [`code`](Error::code) from the
/// taxonomy and a human message. One type for the whole toolkit, so a verdict has one source and
/// a failure renders one way in every SDK.
///
/// `Display` is the wire form `"<code>: <message>"`, built by [`ErrorCode::tag`]. A binding crosses
/// it with `to_string()` and adds nothing, and an idempotency guard caches `code` and `message`
/// side by side, so a replay in any SDK keeps the original code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// The kind of failure, which carries the terminal / transient verdict.
    pub code: ErrorCode,
    /// What went wrong, without the code.
    pub message: String,
}

impl Error {
    /// A failure of kind `code`.
    pub fn new(code: ErrorCode, message: impl std::fmt::Display) -> Self {
        Error {
            code,
            message: message.to_string(),
        }
    }

    /// A deterministic marshaling failure: malformed JSON, an unknown format, a batch that fails
    /// the same way on every retry. Takes any `Display`, so a serializer's own error passes as is.
    pub fn codec(cause: impl std::fmt::Display) -> Self {
        Error::new(ErrorCode::Codec, cause)
    }

    /// The backing store failed for a reason other than a version conflict.
    pub fn store(cause: impl std::fmt::Display) -> Self {
        Error::new(ErrorCode::Store, cause)
    }

    /// An optimistic write found another version than the one it expected.
    pub fn concurrency(expected: impl std::fmt::Display, stored: impl std::fmt::Display) -> Self {
        Error::new(
            ErrorCode::Concurrency,
            format_args!("expected {expected}, stored {stored}"),
        )
    }

    /// A transaction handle that names no open transaction.
    pub fn bad_handle(cause: impl std::fmt::Display) -> Self {
        Error::new(ErrorCode::BadHandle, cause)
    }

    /// The failure a wire string names: its code and message if [`ErrorCode::split`] finds a code,
    /// else a `store` failure carrying the whole string, so a host still logs the original text and
    /// decides nothing itself. The inverse of `Display` for every failure this kernel renders.
    pub fn from_wire(wire: &str) -> Self {
        match ErrorCode::split(wire) {
            Some((code, message)) => Error::new(code, message),
            None => Error::store(wire),
        }
    }

    /// The code's verdict: whether the failure repeats identically on retry.
    pub fn is_terminal(&self) -> bool {
        self.code.is_terminal()
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.code.tag(&self.message))
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_round_trips_through_its_name_and_wire_form() {
        for code in ErrorCode::ALL {
            let name = code.as_str();
            assert_eq!(name.parse::<ErrorCode>(), Ok(*code), "{name}: FromStr");
            assert_eq!(code.to_string(), name, "{name}: Display");
            assert_eq!(
                ErrorCode::split(&code.tag("expected 3, stored 4")),
                Some((*code, "expected 3, stored 4")),
                "{name}: split(tag)"
            );
        }
    }

    #[test]
    fn names_are_distinct() {
        let mut names: Vec<&str> = ErrorCode::ALL.iter().map(|c| c.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ErrorCode::ALL.len());
    }

    #[test]
    fn an_unknown_name_is_a_codec_error() {
        // The same name fails the same way every time, so a binding that reports it must not read
        // as a transient store failure.
        let err = "timeout".parse::<ErrorCode>().expect_err("not a code");
        assert_eq!(err, Error::codec("unknown error code: timeout"));
        assert_eq!(err.to_string(), "codec: unknown error code: timeout");
        assert!(err.is_terminal());
    }

    #[test]
    fn error_display_is_the_tagged_wire_form() {
        // One case per constructor. The wire string is what a binding crosses and what its parser
        // takes apart, so the round trip is pinned too.
        struct Case {
            name: &'static str,
            error: Error,
            code: ErrorCode,
            wire: &'static str,
        }
        let cases = [
            Case {
                name: "concurrency_carries_both_versions_in_order",
                error: Error::concurrency(3, 4),
                code: ErrorCode::Concurrency,
                wire: "concurrency: expected 3, stored 4",
            },
            Case {
                name: "concurrency_renders_a_negative_version_as_is",
                error: Error::concurrency(-1, i64::MAX),
                code: ErrorCode::Concurrency,
                wire: "concurrency: expected -1, stored 9223372036854775807",
            },
            Case {
                name: "codec_is_terminal",
                error: Error::codec("invalid type: string, expected u64"),
                code: ErrorCode::Codec,
                wire: "codec: invalid type: string, expected u64",
            },
            Case {
                name: "store_is_transient",
                error: Error::store("connection to the event store was reset"),
                code: ErrorCode::Store,
                wire: "store: connection to the event store was reset",
            },
            Case {
                name: "bad_handle_names_the_token",
                error: Error::bad_handle("unknown transaction 7"),
                code: ErrorCode::BadHandle,
                wire: "bad_handle: unknown transaction 7",
            },
            Case {
                name: "any_code_through_new",
                error: Error::new(ErrorCode::Rejected, "insufficient funds"),
                code: ErrorCode::Rejected,
                wire: "rejected: insufficient funds",
            },
            Case {
                // The message may itself hold the separator; the code is still the first segment.
                name: "codec_message_with_a_separator_inside",
                error: Error::codec("redis: connection reset"),
                code: ErrorCode::Codec,
                wire: "codec: redis: connection reset",
            },
        ];
        for case in cases {
            assert_eq!(case.error.code, case.code, "{} code", case.name);
            assert_eq!(
                case.error.is_terminal(),
                case.code.is_terminal(),
                "{} verdict",
                case.name
            );
            assert_eq!(case.error.to_string(), case.wire, "{} wire", case.name);
            let message = case.wire.split_once(": ").map(|(_, m)| m);
            assert_eq!(
                ErrorCode::split(case.wire),
                message.map(|m| (case.code, m)),
                "{} split",
                case.name
            );
            assert_eq!(Some(case.error.message.as_str()), message, "{} message", case.name);
        }
    }
}
