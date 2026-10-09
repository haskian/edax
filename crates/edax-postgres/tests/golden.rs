//! Golden vectors for the Postgres store kernel: the cross-language consistency contract.
//!
//! Derived from edax-postgres and written to `testdata/golden/postgres_*.json`. A second SDK sharing a
//! database replays them: the same statements, the same parameters for the same values, the same rows
//! read back, the same verdict on a server error, the same fence, and the same SQL and page for a query.
//! `crates/edax-postgres/live-check` runs `postgres_query.json` against a real server.
//!
//! Regenerate after an intentional change: `EDAX_REGEN=1 cargo test -p edax-postgres --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a golden generator runs only as a test, and its panic is the failure report"
)]

use std::collections::BTreeMap;

use edax_event::{CommandMessage, EventDraft};
use edax_golden::{refusal, write_or_check};
use edax_postgres::ledger::{claim_read, complete_write, forget_write, release_write, LedgerRow};
use edax_postgres::{
    check_catalog, compile, error, reference_ddl, rows, statements, stored_document, verdict, Logical, Op, Tables,
    UniqueKey, CONTRACT_VERSION,
};
use serde_json::{json, Value};

fn schema() -> Tables {
    Tables::default()
}

fn draft(sequence: u64) -> EventDraft {
    EventDraft {
        id: format!("evt-{sequence}"),
        aggregate_id: "invoice-7".into(),
        tenant: Some("acme".into()),
        sequence: Some(sequence),
        name: "InvoiceIssued".into(),
        version: 2,
        data: br#"{"total":"9.50"}"#.to_vec(),
        correlation_id: "corr-1".into(),
        causation_id: "cmd-1".into(),
        occurred_at: Some(1_700_000_000_123_456_000),
        extensions: BTreeMap::from([("traceparent".into(), "00-4bf9-00f0-01".into())]),
        metadata: BTreeMap::from([("origin".into(), "web".into())]),
    }
}

fn command(id: &str, aggregate: &str) -> CommandMessage {
    CommandMessage {
        id: id.into(),
        idempotency_key: Some("req-7".into()),
        aggregate_id: Some(aggregate.into()),
        tenant: None,
        name: "ShipOrder".into(),
        version: 1,
        data: vec![0, 1, 255],
        correlation_id: "corr-1".into(),
        causation_id: "evt-1".into(),
        issued_at: 1_700_000_000_000_000_000,
        extensions: BTreeMap::new(),
        metadata: BTreeMap::from([("note".into(), "a\u{0}b".into())]),
    }
}

/// The catalog row the `catalog` statement reports for `role` when the table is exactly what
/// `reference_ddl` creates for `tables`: the base a mismatch case edits.
fn reference_catalog(tables: &Tables, role: Logical) -> Value {
    let table = edax_postgres::schema::table_contract(role);
    let columns: Vec<Value> = table
        .columns
        .iter()
        .map(|c| {
            let ty = c.column_type.accepts()[0];
            let collation = match (c.key, c.column_type) {
                (true, _) => json!("C"),
                (false, edax_postgres::schema::ColumnType::Text) => json!("default"),
                _ => Value::Null,
            };
            json!({"name": c.name, "type": ty, "not_null": !c.nullable, "default": c.defaulted, "collation": collation})
        })
        .collect();
    let unique: Vec<Value> = table
        .unique
        .iter()
        .enumerate()
        .map(|(i, (_, cols))| {
            let name = if i == 0 {
                format!("{}_pkey", tables.name(role))
            } else {
                format!("{}_{}_key", tables.name(role), cols.join("_"))
            };
            let collations: Vec<&str> = cols
                .iter()
                .map(|col| {
                    if table.columns.iter().any(|c| c.name == *col && c.key) {
                        "C"
                    } else {
                        ""
                    }
                })
                .collect();
            json!({"name": name, "columns": cols, "collations": collations, "partial": false,
                   "expressions": false, "deferrable": false, "valid": true})
        })
        .collect();
    json!({"table": role.as_str(), "kind": "r", "columns": columns, "unique": unique})
}

/// `postgres_contract.json`: the table contract, the reference schema it implies, the table names a host
/// may choose, the verdict on catalog snapshots (a compatible one and each kind of mismatch), and the server
/// checks. The adapter runs the `catalog` statement and hands its reply to the kernel; these rows are those
/// replies.
#[test]
fn golden_contract() {
    let billing =
        Tables::from_json(r#"{"schema":"billing","names":{"events":"billing_events","ledger":"idem"}}"#).unwrap();
    let mut cases = vec![
        json!({"name": "contract", "version": CONTRACT_VERSION, "contract": serde_json::to_value(edax_postgres::contract()).unwrap()}),
        json!({"name": "reference_ddl_default", "tables": Tables::default().to_json(), "ddl": reference_ddl(&Tables::default())}),
        json!({"name": "reference_ddl_named", "tables": billing.to_json(), "ddl": reference_ddl(&billing)}),
    ];
    for (name, tables) in [
        ("schema_upper_case", r#"{"schema":"Edax"}"#),
        ("schema_quote", r#"{"schema":"a\"b"}"#),
        (
            "table_name_dash",
            r#"{"schema":"public","names":{"events":"my-events"}}"#,
        ),
        (
            "two_roles_one_name",
            r#"{"schema":"public","names":{"ledger":"edax_events"}}"#,
        ),
        ("unknown_role", r#"{"schema":"public","names":{"evnts":"x"}}"#),
        ("unknown_field", r#"{"schema":"public","prefix":"x"}"#),
    ] {
        cases.push(json!({"name": format!("tables_refused_{name}"), "tables": tables, "error": refusal(&Tables::from_json(tables))}));
    }
    let all: Vec<Logical> = Logical::ALL.to_vec();
    let full = |tables: &Tables| Value::Array(all.iter().map(|role| reference_catalog(tables, *role)).collect());
    for (name, tables) in [
        ("compatible_default", Tables::default()),
        ("compatible_named", billing.clone()),
    ] {
        let catalog = full(&tables).to_string();
        cases.push(json!({
            "name": name, "tables": tables.to_json(), "roles": all, "catalog": catalog,
            "compatibility": check_catalog(&tables, &all, &catalog).ok(),
        }));
    }
    let ledger = |edit: &dyn Fn(&mut Value)| {
        let mut row = reference_catalog(&Tables::default(), Logical::Ledger);
        edit(&mut row);
        Value::Array(vec![row]).to_string()
    };
    type Edit = Box<dyn Fn(&mut Value)>;
    let mismatches: Vec<(&str, Edit)> = vec![
        ("missing_table", Box::new(|r| r["kind"] = Value::Null)),
        ("partitioned_table_accepted", Box::new(|r| r["kind"] = json!("p"))),
        (
            "missing_column",
            Box::new(|r| drop(r["columns"].as_array_mut().unwrap().remove(3))),
        ),
        ("wrong_type", Box::new(|r| r["columns"][3]["type"] = json!("json"))),
        (
            "varchar_accepted",
            Box::new(|r| r["columns"][3]["type"] = json!("character varying")),
        ),
        (
            "not_null_where_edax_writes_null",
            Box::new(|r| r["columns"][1]["not_null"] = json!(true)),
        ),
        (
            "key_collation",
            Box::new(|r| r["columns"][0]["collation"] = json!("en_US")),
        ),
        (
            "posix_collation_accepted",
            Box::new(|r| r["columns"][0]["collation"] = json!("POSIX")),
        ),
        (
            "extra_not_null_column",
            Box::new(|r| {
                r["columns"].as_array_mut().unwrap().push(json!({"name": "region", "type": "text", "not_null": true, "default": false, "collation": "default"}))
            }),
        ),
        (
            "extra_defaulted_column_accepted",
            Box::new(|r| {
                r["columns"].as_array_mut().unwrap().push(json!({"name": "region", "type": "text", "not_null": true, "default": true, "collation": "default"}))
            }),
        ),
        ("missing_unique", Box::new(|r| r["unique"] = json!([]))),
        ("partial_unique", Box::new(|r| r["unique"][0]["partial"] = json!(true))),
        (
            "expression_unique",
            Box::new(|r| r["unique"][0]["expressions"] = json!(true)),
        ),
        (
            "deferrable_unique",
            Box::new(|r| r["unique"][0]["deferrable"] = json!(true)),
        ),
        ("invalid_unique", Box::new(|r| r["unique"][0]["valid"] = json!(false))),
        (
            "unique_over_more_columns",
            Box::new(|r| r["unique"][0]["columns"] = json!(["key", "owner"])),
        ),
        (
            "unique_collation",
            Box::new(|r| r["unique"][0]["collations"] = json!(["en_US"])),
        ),
        (
            "several_mismatches_listed",
            Box::new(|r| {
                r["columns"][3]["type"] = json!("json");
                r["unique"] = json!([]);
            }),
        ),
    ];
    for (name, edit) in mismatches {
        let catalog = ledger(edit.as_ref());
        let checked = check_catalog(&Tables::default(), &[Logical::Ledger], &catalog);
        cases.push(json!({
            "name": format!("catalog_{name}"), "tables": Tables::default().to_json(), "roles": ["ledger"],
            "catalog": catalog, "compatibility": checked.as_ref().ok(), "error": refusal(&checked),
        }));
    }
    for (name, encoding, version) in [
        ("utf8_17", "UTF8", 170_004),
        ("latin1", "LATIN1", 170_004),
        ("pg_13", "UTF8", 130_011),
    ] {
        cases.push(json!({
            "name": format!("server_{name}"), "server_encoding": encoding, "server_version_num": version,
            "deadlock_timeout_ms": 1_000, "settings": edax_postgres::Settings::default(),
            "error": refusal(&edax_postgres::check_server(encoding, version, 1_000, &edax_postgres::Settings::default())),
        }));
    }
    for deadlock_timeout_ms in [1_999, 2_000, 2_001] {
        cases.push(json!({
            "name": format!("server_deadlock_timeout_{deadlock_timeout_ms}"),
            "server_encoding": "UTF8", "server_version_num": 170_004,
            "deadlock_timeout_ms": deadlock_timeout_ms, "settings": edax_postgres::Settings::default(),
            "error": refusal(&edax_postgres::check_server("UTF8", 170_004, deadlock_timeout_ms, &edax_postgres::Settings::default())),
        }));
    }
    write_or_check("postgres_contract.json", cases);
}

/// `docs/backends/postgres-schema.sql` is the reference schema for the default names, as the kernel
/// writes it: regenerated with the vectors, asserted otherwise, so the documentation cannot drift.
#[test]
fn reference_schema_document() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/backends/postgres-schema.sql");
    let ddl = reference_ddl(&Tables::default());
    if std::env::var(edax_golden::ENV_REGEN).as_deref() == Ok("1") {
        std::fs::write(&path, &ddl).unwrap();
    } else {
        let have = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(
            have, ddl,
            "docs/backends/postgres-schema.sql drifted: regenerate with EDAX_REGEN=1"
        );
    }
}

/// `postgres_statements.json`: every statement, byte for byte, for the `public` schema.
#[test]
fn golden_statements() {
    let rendered = serde_json::to_value(statements(&schema())).unwrap();
    let cases = rendered
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, sql)| json!({"name": name, "sql": sql}))
        .collect();
    write_or_check("postgres_statements.json", cases);
}

/// `postgres_rows.json`: the parameters each write binds and the values each read decodes.
#[test]
fn golden_rows() {
    let drafts = [draft(1), draft(2)];
    let append = rows::append_params("acme:invoice-7", 0, &drafts).unwrap();
    let stored: Vec<Value> = serde_json::from_str::<Vec<Value>>(append[2].as_deref().unwrap())
        .unwrap()
        .into_iter()
        .map(|mut row| {
            row["recorded_at"] = json!(1_700_000_001_000_000_000_i64);
            row
        })
        .collect();
    let result = json!({"stored": 0, "events": stored}).to_string();
    let event_row = stored[0].to_string();
    let commands = [command("cmd-1", "order-1"), command("cmd-2", "order-2")];
    let command_rows: Vec<String> = serde_json::from_str::<Vec<Value>>(&rows::commands_param(&commands).unwrap())
        .unwrap()
        .iter()
        .map(Value::to_string)
        .collect();
    let saga_commands = [command(
        &edax_event::saga_command_id("ship", "order-1", 1, 0),
        "order-1",
    )];
    let mut nul = draft(1);
    nul.correlation_id = "c\0".into();
    let cases = vec![
        json!({"name": "append_params", "stream": "acme:invoice-7", "expected": 0, "drafts": drafts.len(), "params": append}),
        json!({"name": "append_result", "expected": 0, "result": result,
               "events": rows::append_result(0, 2, &result).unwrap().iter().map(|e| e.id.clone()).collect::<Vec<_>>()}),
        json!({"name": "append_result_conflict", "expected": 0, "result": r#"{"stored":3,"events":[]}"#,
               "error": refusal(&rows::append_result(0, 1, r#"{"stored":3,"events":[]}"#))}),
        json!({"name": "append_refuses_a_gap", "error": refusal(&rows::append_params("s", 0, &[draft(2)]))}),
        json!({"name": "append_refuses_nul", "error": refusal(&rows::append_params("s", 0, &[nul]))}),
        json!({"name": "event_row", "row": event_row, "decoded": rows::decode_events(std::slice::from_ref(&event_row)).unwrap().len()}),
        json!({"name": "commands_param", "params": rows::commands_param(&commands).unwrap(),
               "decoded": rows::decode_commands(&command_rows).unwrap().iter().map(|c| c.id.clone()).collect::<Vec<_>>()}),
        json!({"name": "ids_param_drops_nul", "params": rows::ids_param(&["a".into(), "b\0".into()]).unwrap()}),
        json!({"name": "bound_param_clamps", "params": [rows::bound_param(5), rows::bound_param(u64::MAX)]}),
        json!({"name": "snapshot_params", "params": rows::snapshot_params("invoice-7", "Invoice", 3, &[1, 2]).unwrap()}),
        json!({"name": "snapshot_leads", "error": refusal(&rows::snapshot_result("invoice-7", 4, 3))}),
        json!({"name": "projection_conflict", "error": refusal(&rows::projection_result("acct-1", 2, None))}),
        json!({"name": "saga_insert_params", "params": rows::saga_params("ship", "order-1", 0, Some(r#"{"step":1}"#), "", &saga_commands).unwrap()}),
        json!({"name": "saga_complete_params", "params": rows::saga_params("ship", "order-1", 3, None, "m", &[]).unwrap()}),
        json!({"name": "saga_foreign_command", "error": refusal(&rows::saga_params("ship", "order-1", 0, None, "", &[command("x", "order-1")]))}),
        json!({"name": "saga_completed", "result": r#"{"written":null,"stored":4,"completed":true}"#,
               "error": refusal(&rows::saga_result("ship", "order-1", 3, r#"{"written":null,"stored":4,"completed":true}"#))}),
        json!({"name": "saga_conflict", "result": r#"{"written":null,"stored":4,"completed":false}"#,
               "error": refusal(&rows::saga_result("ship", "order-1", 3, r#"{"written":null,"stored":4,"completed":false}"#))}),
        json!({"name": "stored_document_exact", "id": "acct-1", "document": r#"{"n":5.808820077685227e16,"m":0.30000000000000000001}"#,
               "stored": stored_document("acct-1", br#"{"n":5.808820077685227e16,"m":0.30000000000000000001}"#).unwrap()}),
        json!({"name": "stored_document_nul", "error": refusal(&stored_document("acct-1", br#"{"s":"\u0000"}"#))}),
    ];
    write_or_check("postgres_rows.json", cases);
}

/// `postgres_error.json`: the verdict per operation, SQLSTATE and the unique key a violation names.
#[test]
fn golden_error() {
    let mut cases = Vec::new();
    let states = [
        ("23505", Some(UniqueKey::EventsStreamSequence)),
        ("23505", Some(UniqueKey::EventsId)),
        ("23505", Some(UniqueKey::CommandOutboxId)),
        ("23505", None),
        ("55P03", None),
        ("57014", None),
        ("40001", None),
        ("40P01", None),
        ("25006", None),
        ("25P02", None),
        ("08006", None),
        ("53300", None),
        ("22P05", None),
        ("22021", None),
        ("54000", None),
        ("42P01", None),
        ("42501", None),
        ("XX000", None),
    ];
    for op in [Op::Append, Op::Claim, Op::ProjectionSave] {
        for (state, constraint) in states {
            cases.push(json!({
                "name": format!("{}_{state}_{}", op.as_str(), constraint.map_or("none", UniqueKey::as_str)),
                "op": op.as_str(), "sqlstate": state, "unique": constraint.map(UniqueKey::as_str), "message": "server text",
                "error": verdict(op, state, constraint, "server text").to_string(),
            }));
        }
    }
    for kind in ["timeout", "connection", "encode", "other", "bogus"] {
        cases.push(json!({
            "name": format!("client_{kind}"), "op": "load", "kind": kind, "message": "client text",
            "error": error::client_verdict(Op::Load, kind, "client text").to_string(),
        }));
    }
    write_or_check("postgres_error.json", cases);
}

/// `postgres_ledger.json`: what a claim does with the row it read, and each settling write's fence.
#[test]
fn golden_ledger() {
    const DONE: &str = r#"{"payload_hash":7,"result":1,"error":null,"error_code":null,"pending":false}"#;
    let pending = edax_postgres::ledger::pending_record(7).unwrap();
    let row = |owner: Option<&str>, by: Option<&str>, record: &str, lapsed: bool| LedgerRow {
        owner: owner.map(str::to_owned),
        settled_by: by.map(str::to_owned),
        record: record.to_owned(),
        lapsed,
    };
    let rows_by_class = [
        ("free", None),
        ("own_claim", Some(row(Some("me"), None, &pending, false))),
        ("lapsed_claim", Some(row(Some("other"), None, &pending, true))),
        ("other_claim", Some(row(Some("other"), None, &pending, false))),
        ("same_record", Some(row(None, Some("me"), DONE, false))),
        ("other_record", Some(row(None, Some("other"), DONE, false))),
        ("unreadable", Some(row(None, None, "not a record", false))),
    ];
    let mut cases = vec![json!({"name": "pending_record", "payload_hash": 7, "record": pending})];
    for (class, held) in &rows_by_class {
        let held = held.as_ref();
        let row_json =
            held.map(|r| json!({"owner": r.owner, "settled_by": r.settled_by, "record": r.record, "lapsed": r.lapsed}));
        let read = match claim_read(held) {
            edax_postgres::ClaimRead::Prior(record) => json!({"prior": record}),
            edax_postgres::ClaimRead::TakeOver(owner) => json!({"take_over": owner}),
            edax_postgres::ClaimRead::Vanished => json!("vanished"),
        };
        let complete = complete_write("order-7", held, b"me", DONE);
        let forget = forget_write("order-7", held);
        cases.push(json!({
            "name": class, "row": row_json, "handle": "me", "settling": DONE,
            "claim": read,
            "complete": {"write": complete.as_ref().ok(), "error": refusal(&complete)},
            "release": {"write": release_write(held, b"me")},
            "forget": {"write": forget.as_ref().ok(), "error": refusal(&forget)},
        }));
    }
    let pending_settle = complete_write("order-7", rows_by_class[1].1.as_ref(), b"me", &pending);
    cases.push(json!({"name": "pending_cannot_settle", "error": refusal(&pending_settle)}));
    write_or_check("postgres_ledger.json", cases);
}

/// `postgres_query.json`: documents, a query, the SQL and parameters it compiles to, and the page the
/// kernel's reference applier answers, which the live check requires the server to return.
#[test]
fn golden_query() {
    let docs = vec![
        json!({"id": "a", "n": 3, "s": "Z", "p": "9.5", "tags": ["x", 1]}),
        json!({"id": "b", "n": 1.0, "s": "a", "p": "10", "nested": {"k": true}}),
        json!({"id": "c", "n": 5.808820077685227e16, "s": "é", "p": "n/a"}),
        json!({"id": "d", "n": 58_088_200_776_852_271_u64, "s": null, "p": "07"}),
        json!({"id": "e", "s": "\u{1f980}", "p": "7", "tags": []}),
        json!({"id": "f", "n": "3", "s": "\u{ff71}", "p": 7}),
    ];
    let stored: Vec<Value> = docs
        .iter()
        .map(|d| {
            serde_json::from_str(&stored_document(d["id"].as_str().unwrap(), d.to_string().as_bytes()).unwrap())
                .unwrap()
        })
        .collect();
    let first = edax_cqrs::encode_cursor(&stored[0], &[edax_cqrs::SortKey::new("n", false)]).unwrap();
    let queries = [
        ("all_unsorted", json!({})),
        (
            "eq_number_matches_double",
            json!({"filter": {"compare": {"path": "n", "op": "eq", "value": 1}}}),
        ),
        (
            "ne_skips_missing_and_null",
            json!({"filter": {"compare": {"path": "s", "op": "ne", "value": "a"}}}),
        ),
        (
            "not_eq_keeps_missing_and_null",
            json!({"filter": {"not": {"compare": {"path": "s", "op": "eq", "value": "a"}}}}),
        ),
        (
            "gt_number_past_2_53",
            json!({"filter": {"compare": {"path": "n", "op": "gt", "value": 58_088_200_776_852_271_u64}}}),
        ),
        ("strings_by_code_point", json!({"sort": [{"path": "s"}]})),
        (
            "decimal_collation_sort",
            json!({"sort": [{"path": "p", "collation": "decimal"}]}),
        ),
        (
            "decimal_collation_filter",
            json!({"filter": {"compare": {"path": "p", "op": "lt", "value": "10", "collation": "decimal"}}}),
        ),
        ("type_rank_desc", json!({"sort": [{"path": "n", "desc": true}]})),
        (
            "contains_array_element",
            json!({"filter": {"contains": {"path": "tags", "value": 1.0}}}),
        ),
        (
            "nested_path_is_object_only",
            json!({"filter": {"exists": {"path": "tags.0", "exists": true}}}),
        ),
        (
            "starts_with",
            json!({"filter": {"compare": {"path": "s", "op": "starts_with", "value": "a"}}}),
        ),
        (
            "keyset_after_first",
            json!({"sort": [{"path": "n"}], "after": first, "limit": 2}),
        ),
        (
            "offset_and_limit",
            json!({"sort": [{"path": "n"}], "offset": 1, "limit": 2}),
        ),
        ("refused_cursor_unsorted", json!({"after": first})),
    ];
    let mut cases: Vec<Value> = queries
        .iter()
        .map(|(name, query)| {
            let spec = query.to_string();
            let compiled = compile(&schema(), "accounts", spec.as_bytes());
            let reference = edax_cqrs::query_spec(spec.as_bytes()).and_then(|q| edax_cqrs::query(&q, stored.clone()));
            json!({
                "name": name,
                "documents": docs,
                "query": query,
                "sql": compiled.as_ref().ok().map(|c| c.sql.clone()),
                "params": compiled.as_ref().ok().map(|c| c.params.clone()),
                "expected_page": reference.as_ref().ok(),
                "page_error": if compiled.is_ok() { refusal(&reference) } else { Value::Null },
                "error": refusal(&compiled),
            })
        })
        .collect();
    for (name, prefix, ids) in [
        ("empty", "", vec!["", "a", "\u{10ffff}"]),
        (
            "ascii",
            "account-",
            vec!["account", "account-", "account-7", "account."],
        ),
        ("wildcards", "a%_\\", vec!["a%_", "a%_\\", "a%_\\7", "a%_]", "aaaa"]),
        ("unicode", "é", vec!["è", "é", "é7", "ê"]),
        (
            "surrogate_gap",
            "\u{d7ff}",
            vec!["\u{d7fe}", "\u{d7ff}", "\u{d7ff}7", "\u{e000}"],
        ),
        (
            "scalar_width",
            "\u{ffff}",
            vec!["\u{fffe}", "\u{ffff}", "\u{ffff}7", "\u{10000}"],
        ),
        (
            "max_suffix",
            "a\u{10ffff}\u{10ffff}",
            vec!["a\u{10ffff}", "a\u{10ffff}\u{10ffff}", "a\u{10ffff}\u{10ffff}7", "b"],
        ),
        (
            "only_max",
            "\u{10ffff}",
            vec!["\u{10fffe}", "\u{10ffff}", "\u{10ffff}7"],
        ),
        (
            "all_max",
            "\u{10ffff}\u{10ffff}",
            vec![
                "\u{10ffff}",
                "\u{10ffff}7",
                "\u{10ffff}\u{10ffff}",
                "\u{10ffff}\u{10ffff}7",
            ],
        ),
        ("nul", "a\0", vec!["a", "a7", "b"]),
    ] {
        let docs: Vec<Value> = ids.iter().map(|id| json!({"id": id})).collect();
        let query = json!({"filter": {"compare": {"path": "id", "op": "starts_with", "value": prefix}}, "sort": [{"path": "id"}]});
        let spec = query.to_string();
        let compiled = compile(&schema(), "accounts", spec.as_bytes()).unwrap();
        let reference = edax_cqrs::query(&edax_cqrs::query_spec(spec.as_bytes()).unwrap(), docs.clone()).unwrap();
        cases.push(json!({
            "name": format!("id_prefix_{name}"), "documents": docs, "query": query,
            "sql": compiled.sql, "params": compiled.params,
            "expected_page": reference, "page_error": null, "error": null,
        }));
    }
    write_or_check("postgres_query.json", cases);
}
