//! The error taxonomy: every kind of failure the toolkit reports, and the one decision that must
//! match across languages — whether the kind is *terminal*.
//!
//! Terminality decides what gets persisted. A terminal failure fails the same way on every retry,
//! so the idempotency guard caches it under the key and every later claimant replays it. A
//! transient failure releases the claim so the next attempt re-runs. Two SDKs that share one
//! idempotency store and disagree on a verdict leave one caching a failure the other would release:
//! a durable replay inconsistency. So the verdict lives here, pinned by
//! `testdata/golden/error_taxonomy.json`, and every SDK derives its own from [`ErrorCode::is_terminal`].
//!
//! Failures cross a native binding as `"<code>: <message>"`. [`ErrorCode::tag`] is the one place
//! that builds that string and [`ErrorCode::split`] the one place that takes it apart. Only some
//! codes cross the wire today (the store adapters emit `concurrency`, `codec`, `store` and
//! `bad_handle`); the
//! others are raised inside an SDK. One enum covers both: a binding parses every code so a kind
//! that starts crossing later needs no new parser, and terminality is pinned for all of them.

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

            /// Stable lowercase name — the single source of truth for the string form across bindings.
            pub fn as_str(self) -> &'static str {
                match self {
                    $( ErrorCode::$variant => $code, )+
                }
            }

            /// Whether the failure repeats identically on retry. Terminal means: cache the failure
            /// and never re-run. Transient means: release the claim and let a retry re-run.
            pub fn is_terminal(self) -> bool {
                match self {
                    $( ErrorCode::$variant => verdict!($verdict), )+
                }
            }
        }
    };
}

/// A row's verdict, spelled as the word each SDK uses.
macro_rules! verdict {
    (terminal) => {
        true
    };
    (transient) => {
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
    /// to another store. A wiring mistake: the same handle fails the same way every time, so a
    /// transient verdict would have a host retry it forever.
    BadHandle = "bad_handle" => terminal,
}

impl ErrorCode {
    /// The code for a stable name, or `None` for a name the taxonomy does not know. A caller keeps
    /// its own default for `None`; the kernel does not guess.
    pub fn parse(code: &str) -> Option<ErrorCode> {
        ErrorCode::ALL.iter().copied().find(|c| c.as_str() == code)
    }

    /// Build the wire form `"<code>: <message>"`.
    pub fn tag(self, message: impl std::fmt::Display) -> String {
        format!("{}{SEPARATOR}{message}", self.as_str())
    }

    /// Take a wire form apart into its code and message. `None` if there is no `": "` separator or
    /// the code before it is unknown; the caller then keeps the whole string.
    pub fn split(wire: &str) -> Option<(ErrorCode, &str)> {
        let (code, message) = wire.split_once(SEPARATOR)?;
        ErrorCode::parse(code).map(|c| (c, message))
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ErrorCode {
    type Err = String;

    fn from_str(code: &str) -> Result<Self, Self::Err> {
        ErrorCode::parse(code).ok_or_else(|| format!("unknown error code: {code}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_round_trips_through_its_name_and_wire_form() {
        for code in ErrorCode::ALL {
            let name = code.as_str();
            assert_eq!(ErrorCode::parse(name), Some(*code), "{name}: parse(as_str)");
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
    fn split_branches() {
        struct Case {
            name: &'static str,
            wire: &'static str,
            want: Option<(ErrorCode, &'static str)>,
        }
        let cases = [
            Case {
                name: "known_code",
                wire: "codec: bad json",
                want: Some((ErrorCode::Codec, "bad json")),
            },
            // The message may itself contain the separator; only the first one splits.
            Case {
                name: "separator_inside_the_message",
                wire: "store: redis: connection reset",
                want: Some((ErrorCode::Store, "redis: connection reset")),
            },
            Case {
                name: "empty_message",
                wire: "concurrency: ",
                want: Some((ErrorCode::Concurrency, "")),
            },
            Case {
                name: "unknown_code",
                wire: "timeout: deadline exceeded",
                want: None,
            },
            Case {
                name: "no_separator",
                wire: "connection reset by peer",
                want: None,
            },
            Case {
                name: "colon_without_space",
                wire: "codec:no space",
                want: None,
            },
            Case {
                name: "empty",
                wire: "",
                want: None,
            },
        ];
        for c in cases {
            assert_eq!(ErrorCode::split(c.wire), c.want, "{}", c.name);
        }
        assert_eq!(
            "timeout".parse::<ErrorCode>(),
            Err("unknown error code: timeout".to_owned())
        );
    }
}
