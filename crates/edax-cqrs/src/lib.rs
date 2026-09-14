//! edax-cqrs — the command-side consistency kernel.
//!
//! The command bus (handler registration, typed dispatch, the middleware chain) is host-local
//! ergonomics with no cross-language requirement, so it lives in the bindings. The kernel owns
//! only the command-side decisions that must agree across languages and processes:
//!
//! - [`canonical_json`] — re-emit JSON in a canonical form so the same logical command serializes
//!   identically regardless of how the host produced it.
//! - [`payload_hash`] — a stable hash of bytes, for idempotency-key payload binding.
//! - [`command_hash`] — *which* bytes a command's idempotency hash covers: the whole command value,
//!   serialized, canonicalized, hashed. The input rule is the kernel's, not the host's.
//! - [`decide`] — given any prior record for an idempotency key, whether to run the handler,
//!   replay the cached outcome, or reject a payload mismatch.
//! - [`Record`] / [`outcome`] — the stored idempotency record's wire shape, its reading order, and
//!   the verdict a losing claimant reaches from it (in flight, cached failure, cached success).

// Every public item is a cross-language contract, so it states what it promises.
#![warn(missing_docs)]

pub mod record;

pub use record::{decode_record, encode_record, outcome, Outcome, Record, State};

/// Re-emit JSON in a canonical form (sorted object keys, minimal whitespace) so that two hosts
/// serializing the same logical value with different key order, whitespace, or escaping produce
/// identical bytes. The canonical form is whatever this one parser/serializer emits, so it is the
/// same in every binding. Hash the result with [`payload_hash`] to bind a command to an
/// idempotency key consistently across services.
///
/// Numbers keep the form they were written in — `1` and `1.0` are distinct canonical values — except
/// for an integer outside `i64`/`u64` range, which is carried as a 64-bit float. Two such integers
/// that round to the same float share an idempotency hash, so a payload needing exact integers wider
/// than 64 bits must carry them as strings.
pub fn canonical_json(json: &[u8]) -> Result<Vec<u8>, String> {
    // serde_json::Map is a BTreeMap here (no `preserve_order` feature), so keys come out sorted.
    let value: serde_json::Value = serde_json::from_slice(json).map_err(|e| e.to_string())?;
    serde_json::to_vec(&value).map_err(|e| e.to_string())
}

/// Stable 64-bit hash of bytes (FNV-1a). Identical across every binding. For commands, hash the
/// output of [`canonical_json`] so two services agree on whether a reused idempotency key carries
/// the same command.
pub fn payload_hash(bytes: &[u8]) -> u64 {
    edax_core::fnv1a64(bytes)
}

/// The idempotency hash of a command: the **whole command value** serialized as one JSON object,
/// then [`canonical_json`], then [`payload_hash`].
///
/// This is the input rule, and it is the kernel's so that no SDK (and no user) picks the bytes. A
/// host that cannot hand the kernel a Rust value serializes the command with its own standard
/// encoder (Python: `msgspec.json.encode`) and hands the kernel the JSON — that is what the ABI's
/// `payload_hash_json` is. Both paths land on the same bytes because canonicalization removes
/// everything an encoder is free to choose: key order, whitespace, string escaping.
///
/// What the value *contains* is its wire shape — the field names and which fields exist — and
/// that belongs to the host, as it does for events: two services that share an idempotency store
/// declare the command with the same fields. Two consequences follow from "the whole value":
///
/// - The field the idempotency key was read from is hashed too. Reusing a key with a different
///   command is a mismatch even when only that field moved.
/// - Routing metadata is not a field. A command's type name is `command_type()` in Rust and a
///   class attribute in Python, so it stays out of the hash on both sides alike.
///
/// `testdata/golden/command_hash.json` maps commands to hashes; both SDK suites dispatch each one
/// through their own `idempotent` middleware and compare the stored hash.
pub fn command_hash<C: serde::Serialize + ?Sized>(command: &C) -> Result<u64, String> {
    // Serialization then canonicalization report separately: a value serde cannot encode is a
    // different defect from JSON the kernel cannot read, and the message says which.
    let json = serde_json::to_vec(command).map_err(|e| format!("command payload is not JSON-encodable: {e}"))?;
    let canonical = canonical_json(&json).map_err(|e| format!("command payload canonicalization failed: {e}"))?;
    Ok(payload_hash(&canonical))
}

/// What to do with an incoming command given the prior record (if any) for its idempotency key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Decision {
    /// No prior record — claim the key and run the handler.
    Proceed,
    /// A prior record with a matching payload hash — return the cached outcome, don't re-run.
    Replay,
    /// A prior record whose payload hash differs — the key was reused with a different command.
    PayloadMismatch,
}

impl Decision {
    /// Stable lowercase name — the single source of truth for the string form across bindings.
    pub fn as_str(self) -> &'static str {
        match self {
            Decision::Proceed => "proceed",
            Decision::Replay => "replay",
            Decision::PayloadMismatch => "payload_mismatch",
        }
    }
}

/// Decide what to do with a command whose idempotency key may already have a record.
/// `prior` is the stored payload hash for the key (`None` if the key is unseen); `current` is this
/// command's payload hash.
pub fn decide(prior: Option<u64>, current: u64) -> Decision {
    match prior {
        None => Decision::Proceed,
        Some(h) if h == current => Decision::Replay,
        Some(_) => Decision::PayloadMismatch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A realistic command payload; every case overrides only the form under test.
    const COMMAND: &str = r#"{"age":36,"email":"ada@example.com","tags":["eu","paid"]}"#;

    #[test]
    fn canonical_json_branches() {
        struct Case {
            name: &'static str,
            input: &'static [u8],
            /// The canonical bytes, or the substring an error must name.
            expected: Result<&'static str, &'static str>,
        }
        let cases = [
            Case {
                name: "object_keys_are_sorted_and_whitespace_dropped",
                input: br#"{ "tags": ["eu", "paid"], "email": "ada@example.com", "age": 36 }"#,
                expected: Ok(COMMAND),
            },
            Case {
                name: "nested_object_keys_are_sorted_too",
                input: br#"{"address":{"zip":"10001","city":"NYC"}}"#,
                expected: Ok(r#"{"address":{"city":"NYC","zip":"10001"}}"#),
            },
            Case {
                // Array order is data, not formatting, so it is preserved.
                name: "array_order_is_preserved",
                input: br#"{"tags":["paid","eu"]}"#,
                expected: Ok(r#"{"tags":["paid","eu"]}"#),
            },
            Case {
                // A host that escapes non-ASCII (Python's `json.dumps(ensure_ascii=True)`) must still
                // land on these bytes, or the same command would hash differently per language.
                name: "non_ascii_is_emitted_unescaped",
                input: "{\"name\":\"café\"}".as_bytes(),
                expected: Ok("{\"name\":\"café\"}"),
            },
            Case {
                name: "integer_and_float_stay_distinct",
                input: br#"{"total":1.0}"#,
                expected: Ok(r#"{"total":1.0}"#),
            },
            Case {
                name: "duplicate_key_keeps_the_last_value",
                input: br#"{"age":36,"age":37}"#,
                expected: Ok(r#"{"age":37}"#),
            },
            Case {
                name: "empty_object",
                input: b"{}",
                expected: Ok("{}"),
            },
            Case {
                // Postel's law stops at the parser: the kernel canonicalizes JSON, not JSON-ish text.
                name: "malformed_json_names_the_position",
                input: br#"{"email":"ada@example.com",}"#,
                expected: Err("trailing comma at line 1 column 28"),
            },
            Case {
                name: "empty_input",
                input: b"",
                expected: Err("EOF while parsing a value"),
            },
            Case {
                name: "trailing_characters",
                input: br#"{"age":36} 37"#,
                expected: Err("trailing characters"),
            },
            Case {
                name: "invalid_utf8_bytes",
                input: &[b'{', b'"', 0xff, b'"', b':', b'1', b'}'],
                expected: Err("invalid unicode code point"),
            },
            Case {
                // Rejected, not silently coerced to infinity — an unrepresentable number must never
                // canonicalize to `null` and collide with a different payload.
                name: "float_out_of_range",
                input: br#"{"total":1e400}"#,
                expected: Err("number out of range"),
            },
        ];

        for case in cases {
            let got = canonical_json(case.input);
            match case.expected {
                Ok(canonical) => assert_eq!(got.as_deref(), Ok(canonical.as_bytes()), "{}", case.name),
                Err(cause) => {
                    let message = got.expect_err(case.name);
                    assert!(message.contains(cause), "{}: {message}", case.name);
                }
            }
        }
    }

    #[test]
    fn canonical_json_is_the_hash_input() {
        // The point of canonicalizing: two spellings of one command bind to the same idempotency key.
        let terse = canonical_json(COMMAND.as_bytes()).unwrap();
        let roomy = canonical_json(br#"{ "age": 36, "tags": ["eu", "paid"], "email": "ada@example.com" }"#).unwrap();
        assert_eq!(payload_hash(&terse), payload_hash(&roomy));

        // ...and two different commands do not.
        let other = canonical_json(br#"{"email":"grace@example.com","age":36,"tags":["eu","paid"]}"#).unwrap();
        assert_ne!(payload_hash(&terse), payload_hash(&other));
    }

    #[test]
    fn payload_hash_is_fnv1a64() {
        // Known FNV-1a vectors: every binding must reproduce these bit for bit.
        assert_eq!(payload_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(payload_hash(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(
            payload_hash("café ☕".as_bytes()),
            edax_core::fnv1a64("café ☕".as_bytes())
        );
    }

    #[test]
    fn command_hash_is_the_whole_value_canonicalized() {
        #[derive(serde::Serialize)]
        struct PlaceOrder {
            // Declared out of alphabetical order: canonicalization, not declaration order, decides.
            total_cents: i64,
            order_id: &'static str,
            request_id: &'static str,
        }
        let cmd = PlaceOrder { total_cents: 4200, order_id: "ord-7f3c", request_id: "req-1" };
        let expected = payload_hash(br#"{"order_id":"ord-7f3c","request_id":"req-1","total_cents":4200}"#);
        assert_eq!(command_hash(&cmd), Ok(expected));

        // The idempotency-key field is part of the value, so it is part of the hash.
        let body_only = payload_hash(br#"{"order_id":"ord-7f3c","total_cents":4200}"#);
        assert_ne!(command_hash(&cmd), Ok(body_only));

        // A serde_json::Value with the same fields is the same command: the golden generator builds
        // its cases this way, the SDKs from typed structs, and both must agree.
        let value = serde_json::json!({ "request_id": "req-1", "total_cents": 4200, "order_id": "ord-7f3c" });
        assert_eq!(command_hash(&value), Ok(expected));
    }

    #[test]
    fn command_hash_reports_a_value_serde_cannot_encode() {
        struct Unencodable;
        impl serde::Serialize for Unencodable {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("secret field must not leave the process"))
            }
        }
        let message = command_hash(&Unencodable).expect_err("serialization fails");
        assert_eq!(
            message,
            "command payload is not JSON-encodable: secret field must not leave the process"
        );
    }

    #[test]
    fn decide_branches() {
        let current = payload_hash(&canonical_json(COMMAND.as_bytes()).unwrap());
        let other = payload_hash(b"a different command");
        for (name, prior, expected, expected_str) in [
            ("unseen_key_proceeds", None, Decision::Proceed, "proceed"),
            ("same_payload_replays", Some(current), Decision::Replay, "replay"),
            (
                "different_payload_mismatches",
                Some(other),
                Decision::PayloadMismatch,
                "payload_mismatch",
            ),
            // A stored hash of zero is a real prior, not "absent" — the golden vector pins this.
            (
                "stored_zero_is_a_real_prior",
                Some(0),
                Decision::PayloadMismatch,
                "payload_mismatch",
            ),
        ] {
            let decision = decide(prior, current);
            assert_eq!(decision, expected, "{name}");
            assert_eq!(decision.as_str(), expected_str, "{name}");
        }
        assert_eq!(
            decide(Some(0), 0),
            Decision::Replay,
            "a zero hash still replays against itself"
        );
    }
}
