//! The one home of the golden-vector harness.
//!
//! `testdata/golden/*.json` is the cross-language contract. Every kernel derives its vectors in a
//! Rust integration test and hands them to [`write_or_check`]. The harness then either regenerates the
//! file (`EDAX_REGEN=1`) or asserts the committed one still matches.
//!
//! This crate is `publish = false` and only ever a `[dev-dependency]`. It exists as a crate rather
//! than a copied helper so that the rule enforcing the contract cannot drift between kernels: one
//! writer, one reader, one path to the golden directory, one failure message.

use serde_json::Value;
use std::convert::Infallible;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};

/// Bytes as lowercase hexadecimal, the form a vector carries binary in: JSON has no byte string,
/// and hex is the one encoding every binding reads back identically.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A call's refusal as every vector records it, under the field `error`: `null` when the call succeeded,
/// else the error's wire form `"<code>: <message>"`, which is the kernel `Error`'s `Display`. One field and
/// one shape across the vectors, so a binding's replayer reads every refusal one way. A refusal whose
/// message comes from a host library rather than the kernel (a payload decoder) matches across SDKs by its
/// code only, the text before the first `": "`.
pub fn refusal<T, E: Display>(result: &Result<T, E>) -> Value {
    match result {
        Ok(_) => Value::Null,
        Err(error) => Value::String(error.to_string()),
    }
}

/// The marker a vector wraps a host decimal in: `{"$decimal": "9.50"}`. JSON has no decimal type, so the
/// marker says which strings an SDK builds as its own decimal type (`edax::Decimal`, Python's `Decimal`)
/// before it hashes or encodes the value.
pub const DECIMAL_MARKER: &str = "$decimal";

/// The marker a vector wraps a host instant in: `{"$instant": "2024-01-01T01:30:00+01:00"}`, built as
/// `edax::Instant` or an aware Python `datetime`.
pub const INSTANT_MARKER: &str = "$instant";

/// `value` with each host-value marker replaced by the text it wraps, as is: what a Rust host type
/// such as `edax::Decimal` deserializes from, canonicalizing it on the way in.
pub fn host_text(value: &Value) -> Value {
    let Ok(value) = map_markers(value, &|_, text| Ok::<_, Infallible>(Value::String(text.to_owned())));
    value
}

/// `value` with each host-value marker (`{"<marker>": "<text>"}`) replaced by `leaf(marker, text)`, or
/// `leaf`'s first error. The one walk over the markers every reader shares, so a marker added here
/// reaches each of them.
pub fn map_markers<E>(value: &Value, leaf: &impl Fn(&str, &str) -> Result<Value, E>) -> Result<Value, E> {
    Ok(match value {
        Value::Object(fields) => match fields.iter().next() {
            Some((marker, Value::String(text)))
                if fields.len() == 1 && (marker == DECIMAL_MARKER || marker == INSTANT_MARKER) =>
            {
                leaf(marker, text)?
            }
            _ => Value::Object(
                fields
                    .iter()
                    .map(|(key, item)| Ok((key.clone(), map_markers(item, leaf)?)))
                    .collect::<Result<_, E>>()?,
            ),
        },
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| map_markers(item, leaf))
                .collect::<Result<_, E>>()?,
        ),
        other => other.clone(),
    })
}

/// Set to `1` to regenerate instead of assert. Unset or empty asserts; any other value panics.
pub const ENV_REGEN: &str = "EDAX_REGEN";

/// What a golden run does with the derived vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Write the vector to disk. The next commit carries the new contract.
    Regenerate,
    /// Compare the vector with the committed file and fail on drift.
    Assert,
}

/// Only `1` regenerates. Any other non-empty value panics rather than guess: someone who sets
/// `EDAX_REGEN=0` to turn regeneration off must not have the contract silently rewritten.
fn mode_from(regen: Option<&str>) -> Mode {
    match regen {
        None | Some("") => Mode::Assert,
        Some("1") => Mode::Regenerate,
        Some(other) => panic!("{ENV_REGEN}={other:?} is not a mode: set it to 1 to regenerate, or unset it to assert"),
    }
}

fn mode() -> Mode {
    mode_from(std::env::var(ENV_REGEN).ok().as_deref())
}

/// `testdata/golden` at the workspace root.
///
/// Resolved from this crate's own manifest so every kernel gets the same directory whatever its
/// test working directory is. This crate lives at `crates/edax-golden`, hence two levels up.
pub fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/golden")
}

/// Regenerate `testdata/golden/<name>` from `cases` under `EDAX_REGEN`, else assert it matches.
///
/// `cases` becomes a JSON array; the file is pretty-printed with a trailing newline.
#[track_caller]
pub fn write_or_check(name: &str, cases: Vec<Value>) {
    write_or_check_in(&golden_dir(), mode(), name, &Value::Array(cases));
}

/// Read `testdata/golden/<name>` as its array of cases: the one reader a test that replays a vector
/// through an SDK layer uses, so no suite re-derives the path or the parse.
#[track_caller]
pub fn read(name: &str) -> Vec<Value> {
    read_in(&golden_dir(), name)
}

#[track_caller]
fn read_in(dir: &Path, name: &str) -> Vec<Value> {
    let path = dir.join(name);
    match load(&path) {
        Value::Array(cases) => cases,
        _ => panic!("{} is not a JSON array of cases", path.display()),
    }
}

/// The JSON a golden file holds: the one read and parse both the reader and the assert run.
#[track_caller]
fn load(path: &Path) -> Value {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) => panic!("cannot read {}: {error}; regenerate with {ENV_REGEN}=1", path.display()),
    };
    match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(error) => panic!("{} is not valid JSON: {error}", path.display()),
    }
}

#[track_caller]
fn write_or_check_in(dir: &Path, mode: Mode, name: &str, expected: &Value) {
    let path = dir.join(name);
    match mode {
        Mode::Regenerate => regenerate(&path, expected),
        Mode::Assert => assert_matches(name, &path, expected),
    }
}

/// The exact bytes of a golden file. Byte-identical output is what makes `git status` a proof.
fn render(value: &Value) -> String {
    format!("{}\n", pretty(value))
}

#[track_caller]
fn regenerate(path: &Path, expected: &Value) {
    let dir = path.parent().expect("a golden path always has a parent directory");
    if let Err(error) = fs::create_dir_all(dir) {
        panic!("cannot create {}: {error}", dir.display());
    }
    if let Err(error) = fs::write(path, render(expected)) {
        panic!("cannot write {}: {error}", path.display());
    }
}

#[track_caller]
fn assert_matches(name: &str, path: &Path, expected: &Value) {
    let on_disk = load(path);
    if on_disk != *expected {
        panic!("{}", drift_report(name, path, &on_disk, expected));
    }
}

/// Points at the first case that differs instead of dumping both whole vectors. A vector holds
/// dozens of cases; the reader needs the one that moved.
fn drift_report(name: &str, path: &Path, on_disk: &Value, expected: &Value) -> String {
    let mut report = format!("golden drift in {name} ({})\n", path.display());
    match (on_disk, expected) {
        (Value::Array(disk), Value::Array(want)) => {
            let differing: Vec<usize> = (0..disk.len().max(want.len()))
                .filter(|&i| disk.get(i) != want.get(i))
                .collect();
            let first = *differing.first().expect("unequal arrays differ at some index");
            report.push_str(&format!(
                "{} of {} expected cases differ (on disk: {} cases); first at index {first}\n",
                differing.len(),
                want.len(),
                disk.len()
            ));
            report.push_str(&format!("--- on disk [{first}]\n{}\n", side(disk.get(first))));
            report.push_str(&format!("+++ expected [{first}]\n{}\n", side(want.get(first))));
        }
        _ => {
            report.push_str(&format!("--- on disk\n{}\n", pretty(on_disk)));
            report.push_str(&format!("+++ expected\n{}\n", pretty(expected)));
        }
    }
    report.push_str(&format!("regenerate with {ENV_REGEN}=1 if the change is intentional"));
    report
}

fn side(case: Option<&Value>) -> String {
    match case {
        Some(case) => pretty(case),
        None => "<missing>".to_string(),
    }
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).expect("a serde_json::Value always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn refusal_is_null_or_the_wire_form() {
        let ok: Result<u8, String> = Ok(7);
        let refused: Result<u8, String> = Err("codec: unknown field `vlaue`".to_owned());
        assert_eq!(refusal(&ok), Value::Null, "a call that succeeded");
        assert_eq!(
            refusal(&refused),
            json!("codec: unknown field `vlaue`"),
            "the error's display, as it is"
        );
    }

    /// A fresh directory per test so parallel tests never share a file.
    fn scratch_dir(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("edax-golden-{}-{test}", std::process::id()));
        fs::create_dir_all(&dir).expect("temp dir is writable");
        dir
    }

    /// Two cases shaped like a real routing vector; the drift tests move one field of one case.
    fn cases() -> Vec<Value> {
        vec![
            json!({ "id": "agg-1", "n": 7, "expected": 3 }),
            json!({ "id": "order-42", "n": 256, "expected": 91 }),
        ]
    }

    fn panic_message(run: impl FnOnce()) -> String {
        let payload = catch_unwind(AssertUnwindSafe(run)).expect_err("the run should panic");
        match payload.downcast::<String>() {
            Ok(message) => *message,
            Err(payload) => payload
                .downcast::<&str>()
                .expect("panic payload is a string")
                .to_string(),
        }
    }

    #[test]
    fn mode_regenerates_only_on_one() {
        struct Case {
            name: &'static str,
            regen: Option<&'static str>,
            /// `None` when the value must panic instead of selecting a mode.
            expected: Option<Mode>,
        }
        let table = [
            Case {
                name: "unset_asserts",
                regen: None,
                expected: Some(Mode::Assert),
            },
            Case {
                name: "empty_asserts",
                regen: Some(""),
                expected: Some(Mode::Assert),
            },
            Case {
                name: "one_regenerates",
                regen: Some("1"),
                expected: Some(Mode::Regenerate),
            },
            Case {
                name: "zero_panics",
                regen: Some("0"),
                expected: None,
            },
            Case {
                name: "true_panics",
                regen: Some("true"),
                expected: None,
            },
        ];
        for case in table {
            match case.expected {
                Some(mode) => assert_eq!(mode_from(case.regen), mode, "{}", case.name),
                None => {
                    let message = panic_message(|| {
                        mode_from(case.regen);
                    });
                    assert!(
                        message.contains("set it to 1 to regenerate"),
                        "{}: {message}",
                        case.name
                    );
                }
            }
        }
    }

    #[test]
    fn read_returns_the_cases_or_names_the_bad_file() {
        let dir = scratch_dir("read");
        fs::write(dir.join("vector.json"), render(&Value::Array(cases()))).expect("scratch file is writable");
        assert_eq!(read_in(&dir, "vector.json"), cases());

        fs::write(dir.join("object.json"), r#"{"id":"agg-1"}"#).expect("scratch file is writable");
        let not_array = panic_message(|| {
            read_in(&dir, "object.json");
        });
        assert!(not_array.contains("object.json is not a JSON array"), "{not_array}");

        fs::write(dir.join("broken.json"), "[{").expect("scratch file is writable");
        let malformed = panic_message(|| {
            read_in(&dir, "broken.json");
        });
        assert!(malformed.contains("broken.json is not valid JSON"), "{malformed}");

        let missing = panic_message(|| {
            read_in(&dir, "absent.json");
        });
        assert!(
            missing.contains("cannot read") && missing.contains("absent.json"),
            "{missing}"
        );
        fs::remove_dir_all(dir).expect("scratch dir removable");
    }

    #[test]
    fn golden_dir_is_the_workspace_testdata() {
        let dir = golden_dir();
        assert!(dir.is_dir(), "{} is not a directory", dir.display());
        let workspace_manifest = dir.join("../../Cargo.toml");
        let manifest = fs::read_to_string(&workspace_manifest)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", workspace_manifest.display()));
        assert!(
            manifest.contains("[workspace]"),
            "{} is not the workspace root",
            workspace_manifest.display()
        );
    }

    #[test]
    fn regenerate_writes_pretty_json_with_trailing_newline() {
        let dir = scratch_dir("regenerate");
        write_or_check_in(&dir, Mode::Regenerate, "vector.json", &Value::Array(cases()));
        let written = fs::read_to_string(dir.join("vector.json")).expect("the golden was written");
        let expected = concat!(
            "[\n",
            "  {\n    \"expected\": 3,\n    \"id\": \"agg-1\",\n    \"n\": 7\n  },\n",
            "  {\n    \"expected\": 91,\n    \"id\": \"order-42\",\n    \"n\": 256\n  }\n",
            "]\n"
        );
        assert_eq!(written, expected);
        fs::remove_dir_all(dir).expect("scratch dir is removable");
    }

    #[test]
    fn regenerate_creates_a_missing_directory() {
        let dir = scratch_dir("create-dir").join("nested");
        write_or_check_in(&dir, Mode::Regenerate, "vector.json", &Value::Array(cases()));
        assert!(dir.join("vector.json").is_file());
        fs::remove_dir_all(dir.parent().expect("nested has a parent")).expect("scratch dir is removable");
    }

    #[test]
    fn assert_accepts_a_matching_file_whatever_its_formatting() {
        let dir = scratch_dir("assert-ok");
        // Compact and key-reordered on disk: the contract is the JSON value, not the bytes.
        fs::write(
            dir.join("vector.json"),
            r#"[{"n":7,"id":"agg-1","expected":3},{"n":256,"id":"order-42","expected":91}]"#,
        )
        .expect("scratch file is writable");
        write_or_check_in(&dir, Mode::Assert, "vector.json", &Value::Array(cases()));
        fs::remove_dir_all(dir).expect("scratch dir is removable");
    }

    #[test]
    fn assert_reports_the_first_drifting_case() {
        struct Case {
            name: &'static str,
            on_disk: Value,
            expected_fragments: &'static [&'static str],
        }
        let mut moved = cases();
        moved[1]["expected"] = json!(92);
        let mut short = cases();
        short.pop();
        let table = [
            Case {
                name: "one_field_moved",
                on_disk: Value::Array(moved),
                expected_fragments: &[
                    "golden drift in vector.json",
                    "1 of 2 expected cases differ (on disk: 2 cases); first at index 1",
                    "--- on disk [1]",
                    "\"expected\": 92",
                    "+++ expected [1]",
                    "\"expected\": 91",
                    "regenerate with EDAX_REGEN=1",
                ],
            },
            Case {
                name: "case_missing_on_disk",
                on_disk: Value::Array(short),
                expected_fragments: &[
                    "1 of 2 expected cases differ (on disk: 1 cases); first at index 1",
                    "<missing>",
                ],
            },
            Case {
                name: "not_an_array_on_disk",
                on_disk: json!({"oops": true}),
                expected_fragments: &["--- on disk\n{\n  \"oops\": true\n}", "+++ expected\n["],
            },
        ];
        for case in table {
            let dir = scratch_dir(case.name);
            fs::write(dir.join("vector.json"), render(&case.on_disk)).expect("scratch file is writable");
            let message =
                panic_message(|| write_or_check_in(&dir, Mode::Assert, "vector.json", &Value::Array(cases())));
            for fragment in case.expected_fragments {
                assert!(
                    message.contains(fragment),
                    "{}: missing {fragment:?} in:\n{message}",
                    case.name
                );
            }
            fs::remove_dir_all(dir).expect("scratch dir is removable");
        }
    }

    #[test]
    fn assert_names_the_file_when_it_is_missing_or_malformed() {
        let dir = scratch_dir("assert-unreadable");
        let missing = panic_message(|| write_or_check_in(&dir, Mode::Assert, "absent.json", &Value::Array(cases())));
        assert!(missing.contains("cannot read"), "{missing}");
        assert!(missing.contains("absent.json"), "{missing}");
        assert!(missing.contains("regenerate with EDAX_REGEN=1"), "{missing}");

        fs::write(dir.join("broken.json"), "[{").expect("scratch file is writable");
        let malformed = panic_message(|| write_or_check_in(&dir, Mode::Assert, "broken.json", &Value::Array(cases())));
        assert!(malformed.contains("broken.json is not valid JSON"), "{malformed}");
        fs::remove_dir_all(dir).expect("scratch dir is removable");
    }

    /// One case per branch of the walk: each marker, a marker beside another key (no marker), a marker
    /// over a non-string (no marker), and markers nested in a map and in a list.
    #[test]
    fn host_text_unwraps_each_marker() {
        let cases: [(&str, Value, Value); 6] = [
            ("decimal_marker", json!({"$decimal": "9.50"}), json!("9.50")),
            (
                "instant_marker",
                json!({"$instant": "2024-01-01T01:30:00+01:00"}),
                json!("2024-01-01T01:30:00+01:00"),
            ),
            (
                "a_second_key_is_a_plain_map",
                json!({"$decimal": "9.50", "x": 1}),
                json!({"$decimal": "9.50", "x": 1}),
            ),
            (
                "a_marker_over_a_number_is_a_plain_map",
                json!({"$decimal": 9.5}),
                json!({"$decimal": 9.5}),
            ),
            (
                "nested_in_a_map",
                json!({"account": "a", "amount": {"$decimal": "9.50"}}),
                json!({"account": "a", "amount": "9.50"}),
            ),
            ("nested_in_a_list", json!([{"$instant": "t"}, 1]), json!(["t", 1])),
        ];
        for (name, input, want) in cases {
            assert_eq!(host_text(&input), want, "{name}");
        }
    }
}
