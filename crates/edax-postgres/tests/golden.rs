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
use edax_golden::{hex, refusal, write_or_check};
use edax_postgres::ledger::{claim_read, complete_write, forget_write, release_write, LedgerRow};
use edax_postgres::rows::{Call, Outbox, Param};
use edax_postgres::{
    begin, check_catalog, compile, error, ledger, reference_ddl, rows, statements, stored_document, verdict, Logical,
    Op, Settings, Tables, UniqueKey, CONTRACT_VERSION,
};
use serde_json::{json, Value};

fn schema() -> Tables {
    Tables::default()
}

fn draft(sequence: u64) -> EventDraft {
    EventDraft {
        subject: None,
        encryption: None,
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
        subject: None,
        encryption: None,
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
        metadata: BTreeMap::from([("note".into(), "é \"quoted\"".into())]),
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
            let sequence = c
                .defaulted
                .then(|| json!({"cache": 1, "increment": 1, "cycle": false, "fills": true}));
            json!({"name": c.name, "type": ty, "not_null": !c.nullable, "default": c.defaulted, "collation": collation,
                "sequence": sequence})
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
    // Mismatches on other tables: an outbox position edax never writes, and a second index on one key,
    // which the compatibility map must name too, or a violation on it reads as no key at all.
    let role_edits: Vec<(&str, Logical, Edit)> = vec![
        (
            "position_without_default",
            Logical::EventOutbox,
            Box::new(|r| r["columns"][0]["default"] = json!(false)),
        ),
        (
            "position_default_without_sequence",
            Logical::EventOutbox,
            Box::new(|r| r["columns"][0]["sequence"] = Value::Null),
        ),
        (
            "position_default_beside_an_unused_sequence",
            Logical::CommandOutbox,
            Box::new(|r| r["columns"][0]["sequence"]["fills"] = json!(false)),
        ),
        (
            "position_sequence_caches_a_block",
            Logical::CommandOutbox,
            Box::new(|r| r["columns"][0]["sequence"]["cache"] = json!(20)),
        ),
        (
            "position_sequence_counts_down",
            Logical::SagaOutbox,
            Box::new(|r| r["columns"][0]["sequence"]["increment"] = json!(-1)),
        ),
        (
            "position_sequence_cycles",
            Logical::EventOutbox,
            Box::new(|r| r["columns"][0]["sequence"]["cycle"] = json!(true)),
        ),
        (
            "key_collation_in_another_schema",
            Logical::Events,
            Box::new(|r| r["columns"][0]["collation"] = json!("public.C")),
        ),
        (
            "two_indexes_on_one_key",
            Logical::Events,
            Box::new(|r| {
                let mut again = r["unique"][1].clone();
                again["name"] = json!("edax_events_stream_sequence_again");
                r["unique"].as_array_mut().unwrap().push(again);
            }),
        ),
    ];
    for (name, role, edit) in role_edits {
        let mut row = reference_catalog(&Tables::default(), role);
        edit(&mut row);
        let catalog = Value::Array(vec![row]).to_string();
        let checked = check_catalog(&Tables::default(), &[role], &catalog);
        cases.push(json!({
            "name": format!("catalog_{name}"), "tables": Tables::default().to_json(), "roles": [role],
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

/// The erasure schema has the same drift protection as the application-store schema.
#[test]
fn reference_erasure_schema_document() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/backends/postgres-erasure-schema.sql");
    let ddl = edax_postgres::erasure::reference_ddl("{}").unwrap();
    if std::env::var(edax_golden::ENV_REGEN).as_deref() == Ok("1") {
        std::fs::write(&path, &ddl).unwrap();
    } else {
        assert_eq!(
            std::fs::read_to_string(&path).unwrap_or_default(),
            ddl,
            "docs/backends/postgres-erasure-schema.sql drifted: regenerate with EDAX_REGEN=1"
        );
    }
}

/// `postgres_statements.json`: every statement, byte for byte, for the `public` schema, and what opens a
/// transaction under the default settings.
#[test]
fn golden_statements() {
    let rendered = serde_json::to_value(statements(&schema())).unwrap();
    let mut cases: Vec<Value> = rendered
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, sql)| json!({"name": name, "sql": sql}))
        .collect();
    let settings = Settings::default();
    cases.push(json!({"name": "begin", "settings": settings, "sql": begin(&settings).unwrap()}));
    write_or_check("postgres_statements.json", cases);
}

/// A parameter call's case: `call`, named as the binding names it without its `postgres_` prefix, given
/// `args` in the binding's order, answers its statement (when it chooses one) and parameters, or `error`.
fn call_case(name: &str, call: &str, args: Value, answer: Result<Value, edax_core::Error>) -> Value {
    let error = refusal(&answer);
    json!({"name": name, "call": call, "args": args, "want": answer.ok(), "error": error})
}

fn params(answer: Result<Vec<Param>, edax_core::Error>) -> Result<Value, edax_core::Error> {
    answer.map(|params| json!({"params": params}))
}

fn chosen(answer: Result<Call, edax_core::Error>) -> Result<Value, edax_core::Error> {
    answer.map(|call| json!({"statement": call.statement, "params": call.params}))
}

/// The writer fence's lock order: every SDK fencing the same scopes in one transaction takes the same
/// advisory locks in the same order, whatever order and repeats its caller passed.
fn erasure_lock_cases() -> Vec<Value> {
    let scope = |tenant: &str, subject: Option<&str>| edax_seal::KeyScope {
        key_namespace: "production-eu".into(),
        tenant: Some(tenant.into()),
        subject: subject.map(str::to_owned),
    };
    let case = |name: &str, scopes: &[edax_seal::KeyScope]| {
        let answer = edax_postgres::erasure::writer_locks(scopes);
        let error = refusal(&answer);
        json!({"name": name, "kind": "erasure_writer_locks", "scopes": scopes, "want": answer.ok(), "error": error})
    };
    let one = [scope("clinic-7", Some("patient-42"))];
    let several = [
        scope("clinic-8", Some("patient-c")),
        scope("clinic-7", Some("patient-b")),
        scope("clinic-7", None),
        scope("clinic-7", Some("patient-a")),
        scope("clinic-7", Some("patient-b")),
    ];
    let mut bad = scope("clinic-7", Some("patient-42"));
    bad.key_namespace = String::new();
    [
        ("erasure_writer_locks_one_subject", &one[..]),
        ("erasure_writer_locks_several_scopes", &several[..]),
        ("erasure_writer_locks_none", &[][..]),
        ("erasure_writer_locks_refuses_a_bad_scope", std::slice::from_ref(&bad)),
    ]
    .into_iter()
    .map(|(name, scopes)| case(name, scopes))
    .collect()
}

fn sealed_row_cases() -> Vec<Value> {
    const KEY: [u8; 32] = [0x47; 32];
    const STREAM: &str = "4:acme:invoice-7";
    let mut cases = Vec::new();
    for cipher in edax_seal::Cipher::ALL {
        let info = edax_seal::EncryptionInfo {
            format_version: 1,
            key_namespace: "production-eu".into(),
            key_id: "data-key-1".into(),
            cipher: *cipher,
            nonce: vec![0x24; cipher.nonce_len()],
        };
        let original = EventDraft {
            subject: Some("customer-42".into()),
            ..draft(1)
        };
        let sealed = edax_event::sealing::seal_event(original.clone(), info.clone(), &KEY).unwrap();
        let append = rows::append_params(STREAM, 0, std::slice::from_ref(&sealed)).unwrap();
        let result = json!({"stored":0,"recorded_at":1_700_000_001_000_000_000_i64}).to_string();
        let event = rows::append_result(0, vec![sealed], &result).unwrap().remove(0);
        let mut event_row: Value = serde_json::from_str::<Vec<Value>>(append[2].as_deref().unwrap())
            .unwrap()
            .remove(0);
        event_row["recorded_at"] = json!(event.recorded_at);
        let decoded = rows::decode_events(&[event_row.to_string()]).unwrap().remove(0);
        assert_eq!(decoded, event);
        assert_eq!(edax_event::sealing::open_event(&decoded, &KEY).unwrap(), original.data);
        cases.push(json!({
            "name":format!("sealed_event_{}",cipher.as_str()), "kind":"sealed_event",
            "stream":STREAM,"expected":0,"params":append,"result":result,
            "row":event_row.to_string(),"message":event,"key_hex":hex(&KEY),"plaintext_hex":hex(&original.data),
        }));

        let original = CommandMessage {
            tenant: Some("acme".into()),
            subject: Some("customer-42".into()),
            ..command("cmd-1", "order-1")
        };
        let sealed = edax_event::sealing::seal_command(original.clone(), info.clone(), &KEY).unwrap();
        let enqueue = rows::enqueue_params(std::slice::from_ref(&sealed), None).unwrap();
        let command_row = serde_json::from_str::<Vec<Value>>(enqueue[0].as_deref().unwrap())
            .unwrap()
            .remove(0)
            .to_string();
        let decoded = rows::decode_commands(std::slice::from_ref(&command_row))
            .unwrap()
            .remove(0);
        assert_eq!(decoded, sealed);
        assert_eq!(
            edax_event::sealing::open_command(&decoded, &KEY).unwrap(),
            original.data
        );
        cases.push(json!({
            "name":format!("sealed_command_{}",cipher.as_str()), "kind":"sealed_command",
            "params":enqueue,"row":command_row,"message":sealed,"key_hex":hex(&KEY),"plaintext_hex":hex(&original.data),
        }));

        let plaintext = br#"{"invoice_id":"invoice-7","total":"9.50"}"#;
        let aad = edax_event::sealing::state_context(
            "snapshot",
            STREAM,
            "Invoice",
            1,
            Some("acme"),
            Some("customer-42"),
            "",
            false,
            &info,
        )
        .unwrap();
        let ciphertext = edax_seal::seal(&info, &KEY, &aad, plaintext).unwrap();
        let snapshot_params = rows::protected_snapshot_params(
            STREAM,
            "Invoice",
            1,
            &ciphertext,
            Some("acme"),
            Some("customer-42"),
            Some(&info),
        )
        .unwrap();
        let snapshot_row = json!({"type_name":"Invoice","version":1,"data":hex(&ciphertext),"tenant":"acme","subject":"customer-42","encryption":info}).to_string();
        let snapshot = rows::decode_snapshot(&snapshot_row).unwrap();
        assert_eq!(snapshot.data, ciphertext);
        assert_eq!(snapshot.encryption.as_ref(), Some(&info));
        assert_eq!(
            edax_seal::open(snapshot.encryption.as_ref().unwrap(), &KEY, &aad, &snapshot.data).unwrap(),
            plaintext
        );
        cases.push(json!({
            "name":format!("sealed_snapshot_{}",cipher.as_str()),"kind":"sealed_snapshot",
            "stream":STREAM,"params":snapshot_params,"row":snapshot_row,"key_hex":hex(&KEY),
            "plaintext_hex":hex(plaintext),"aad_hex":hex(&aad),
        }));
    }
    cases
}

/// `postgres_rows.json`: the parameters each statement binds, the statement a call chooses, and the values
/// each read decodes.
#[test]
fn golden_rows() {
    let drafts = [draft(1), draft(2)];
    let append = rows::append_params("4:acme:invoice-7", 0, &drafts).unwrap();
    let draft_rows: Vec<Value> = serde_json::from_str(append[2].as_deref().unwrap()).unwrap();
    let recorded_at = 1_700_000_001_000_000_000_i64;
    let result = json!({"stored": 0, "recorded_at": recorded_at}).to_string();
    let mut event_row = draft_rows[0].clone();
    event_row["recorded_at"] = json!(recorded_at);
    let event_row = event_row.to_string();
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
    let saga_command_rows: Value = serde_json::from_str(&rows::commands_param(&saga_commands).unwrap()).unwrap();
    let mut nul = draft(1);
    nul.correlation_id = "c\0".into();
    // The refused inputs as rows a replay rebuilds its messages from: a draft row is the one an accepted
    // append binds, edited where the refusal needs a value no append binds.
    let draft_row = |expected: u64, draft: &EventDraft| -> Value {
        let params = rows::append_params("invoice-7", expected, std::slice::from_ref(draft)).unwrap();
        serde_json::from_str::<Vec<Value>>(params[2].as_deref().unwrap())
            .unwrap()
            .remove(0)
    };
    let mut nul_row = draft_row(0, &draft(1));
    nul_row["correlation_id"] = json!("c\u{0}");
    // A NUL metadata note, on the message a refusal is called with and on the row a replay rebuilds it from.
    let nul_note = |metadata: &mut edax_event::Metadata, row: &mut Value| {
        metadata.insert("note".into(), "a\0b".into());
        row["metadata"]["note"] = json!("a\u{0}b");
    };
    let (mut nul_metadata, mut nul_metadata_row) = (draft(1), draft_row(0, &draft(1)));
    nul_note(&mut nul_metadata.metadata, &mut nul_metadata_row);
    let mut nul_command = command("cmd-1", "order-1");
    let mut nul_command_rows: Value =
        serde_json::from_str(&rows::commands_param(&[nul_command.clone()]).unwrap()).unwrap();
    nul_note(&mut nul_command.metadata, &mut nul_command_rows[0]);
    let (mut nul_saga_command, mut nul_saga_rows) = (saga_commands[0].clone(), saga_command_rows.clone());
    nul_note(&mut nul_saga_command.metadata, &mut nul_saga_rows[0]);
    let foreign = [command("x", "order-1")];
    let foreign_rows: Value = serde_json::from_str(&rows::commands_param(&foreign).unwrap()).unwrap();
    let nul_document = r#"{"s":"\u0000"}"#;
    let exact_document = r#"{"n":5.808820077685227e16,"m":0.30000000000000000001}"#;
    let mut cases = vec![
        json!({"name": "append_params", "stream": "4:acme:invoice-7", "expected": 0, "drafts": drafts.len(), "params": append}),
        json!({"name": "append_result", "expected": 0, "draft_rows": draft_rows, "result": result,
               "events": rows::append_result(0, drafts.to_vec(), &result).unwrap()
                   .iter().map(|e| json!({"id": e.id, "recorded_at": e.recorded_at})).collect::<Vec<_>>()}),
        json!({"name": "append_result_conflict", "expected": 0, "draft_rows": draft_rows, "result": r#"{"stored":3,"recorded_at":5}"#,
               "error": refusal(&rows::append_result(0, drafts.to_vec(), r#"{"stored":3,"recorded_at":5}"#))}),
        json!({"name": "append_refuses_a_gap", "stream": "invoice-7", "expected": 0, "draft_rows": [draft_row(1, &draft(2))],
               "error": refusal(&rows::append_params("invoice-7", 0, &[draft(2)]))}),
        json!({"name": "append_refuses_another_aggregate", "stream": "invoice-8", "expected": 0,
               "draft_rows": [draft_row(0, &draft(1))],
               "error": refusal(&rows::append_params("invoice-8", 0, &[draft(1)]))}),
        json!({"name": "append_refuses_nul", "stream": "invoice-7", "expected": 0, "draft_rows": [nul_row],
               "error": refusal(&rows::append_params("invoice-7", 0, &[nul]))}),
        json!({"name": "append_refuses_nul_in_metadata", "stream": "invoice-7", "expected": 0, "draft_rows": [nul_metadata_row],
               "error": refusal(&rows::append_params("invoice-7", 0, &[nul_metadata]))}),
        json!({"name": "enqueue_refuses_nul_in_metadata", "command_rows": nul_command_rows,
               "error": refusal(&rows::enqueue_params(&[nul_command], None))}),
        json!({"name": "event_row", "row": event_row, "decoded": rows::decode_events(std::slice::from_ref(&event_row)).unwrap().len()}),
        json!({"name": "enqueue_params", "after_event": "evt-2", "params": rows::enqueue_params(&commands, Some("evt-2")).unwrap(),
               "decoded": rows::decode_commands(&command_rows).unwrap().iter().map(|c| c.id.clone()).collect::<Vec<_>>()}),
        json!({"name": "snapshot_params", "params": rows::snapshot_params("invoice-7", "Invoice", 3, &[1, 2]).unwrap()}),
        json!({"name": "snapshot_leads", "error": refusal(&rows::snapshot_result("invoice-7", 4, 3))}),
        json!({"name": "projection_conflict", "error": refusal(&rows::projection_result("acct-1", 2, None))}),
        json!({"name": "saga_insert", "saga": "ship", "id": "order-1", "version": 0, "state": r#"{"step":1}"#, "folded": "",
               "command_rows": saga_command_rows,
               "want": chosen(rows::saga_write("ship", "order-1", 0, Some(r#"{"step":1}"#), "", &saga_commands)).unwrap()}),
        json!({"name": "saga_complete", "saga": "ship", "id": "order-1", "version": 3, "state": null, "folded": "m",
               "command_rows": [],
               "want": chosen(rows::saga_write("ship", "order-1", 3, None, "m", &[])).unwrap()}),
        json!({"name": "saga_foreign_command", "saga": "ship", "id": "order-1", "version": 0, "state": null, "folded": "",
               "command_rows": foreign_rows, "error": refusal(&rows::saga_write("ship", "order-1", 0, None, "", &foreign))}),
        json!({"name": "saga_refuses_nul_in_metadata", "saga": "ship", "id": "order-1", "version": 0, "state": null, "folded": "",
               "command_rows": nul_saga_rows,
               "error": refusal(&rows::saga_write("ship", "order-1", 0, None, "", &[nul_saga_command]))}),
        json!({"name": "saga_completed", "result": r#"{"written":null,"stored":4,"completed":true}"#,
               "error": refusal(&rows::saga_result("ship", "order-1", 3, r#"{"written":null,"stored":4,"completed":true}"#))}),
        json!({"name": "saga_conflict", "result": r#"{"written":null,"stored":4,"completed":false}"#,
               "error": refusal(&rows::saga_result("ship", "order-1", 3, r#"{"written":null,"stored":4,"completed":false}"#))}),
        json!({"name": "stored_document_exact", "id": "acct-1", "document": exact_document,
               "stored": stored_document("acct-1", exact_document.as_bytes()).unwrap()}),
        json!({"name": "stored_document_nul", "id": "acct-1", "document": nul_document,
               "error": refusal(&stored_document("acct-1", nul_document.as_bytes()))}),
    ];
    let lease = Some(("relay-a", 30_000));
    cases.extend([
        call_case(
            "load_clamps_after",
            "load_params",
            json!(["4:acme:invoice-7", u64::MAX]),
            params(rows::load_params("4:acme:invoice-7", u64::MAX)),
        ),
        call_case(
            "load_refuses_nul",
            "load_params",
            json!(["s\u{0}", 0]),
            params(rows::load_params("s\0", 0)),
        ),
        call_case(
            "load_snapshot",
            "load_snapshot_params",
            json!(["4:acme:invoice-7"]),
            params(rows::load_snapshot_params("4:acme:invoice-7")),
        ),
        call_case(
            "projection_get",
            "projection_get",
            json!(["accounts", "acct-1", false]),
            chosen(rows::projection_get("accounts", "acct-1", false)),
        ),
        call_case(
            "projection_get_locked",
            "projection_get",
            json!(["accounts", "acct-1", true]),
            chosen(rows::projection_get("accounts", "acct-1", true)),
        ),
        call_case(
            "projection_get_refuses_nul",
            "projection_get",
            json!(["accounts", "a\u{0}", false]),
            chosen(rows::projection_get("accounts", "a\0", false)),
        ),
        call_case(
            "projection_insert",
            "projection_save",
            json!(["accounts", "acct-1", 0, exact_document]),
            chosen(rows::projection_save(
                "accounts",
                "acct-1",
                0,
                exact_document.as_bytes(),
            )),
        ),
        call_case(
            "projection_update",
            "projection_save",
            json!(["accounts", "acct-1", 2, r#"{"n":1}"#]),
            chosen(rows::projection_save("accounts", "acct-1", 2, br#"{"n":1}"#)),
        ),
        call_case(
            "projection_update_refuses_a_version_past_bigint",
            "projection_save",
            json!(["accounts", "acct-1", u64::MAX, "{}"]),
            chosen(rows::projection_save("accounts", "acct-1", u64::MAX, b"{}")),
        ),
        call_case(
            "projection_save_refuses_nul",
            "projection_save",
            json!(["accounts", "acct-1", 0, nul_document]),
            chosen(rows::projection_save("accounts", "acct-1", 0, nul_document.as_bytes())),
        ),
        call_case(
            "saga_load",
            "saga_load",
            json!(["ship", "order-1", false]),
            chosen(rows::saga_load("ship", "order-1", false)),
        ),
        call_case(
            "saga_load_locked",
            "saga_load",
            json!(["ship", "order-1", true]),
            chosen(rows::saga_load("ship", "order-1", true)),
        ),
        call_case(
            "event_unpublished",
            "unpublished",
            json!(["event", 10, null, null, null]),
            chosen(rows::unpublished(Outbox::Event, None, 10, None)),
        ),
        call_case(
            "command_unpublished_leased",
            "unpublished",
            json!(["command", u64::MAX, null, "relay-a", 30_000]),
            chosen(rows::unpublished(Outbox::Command, None, u64::MAX, lease)),
        ),
        call_case(
            "saga_unpublished_leased",
            "unpublished",
            json!(["saga", 10, "ship", "relay-a", 30_000]),
            chosen(rows::unpublished(Outbox::Saga, Some("ship"), 10, lease)),
        ),
        call_case(
            "unpublished_refuses_a_lease_without_owner",
            "unpublished",
            json!(["event", 10, null, "", 30_000]),
            chosen(rows::unpublished(Outbox::Event, None, 10, Some(("", 30_000)))),
        ),
        call_case(
            "unpublished_refuses_a_saga_outbox_without_saga",
            "unpublished",
            json!(["saga", 10, null, null, null]),
            chosen(rows::unpublished(Outbox::Saga, None, 10, None)),
        ),
        call_case(
            "unpublished_refuses_an_unknown_outbox",
            "unpublished",
            json!(["evnt", 10, null, null, null]),
            chosen(Outbox::from_name("evnt").and_then(|o| rows::unpublished(o, None, 10, None))),
        ),
        call_case(
            "mark_published_drops_nul",
            "mark_published",
            json!(["event", ["a", "b\u{0}"], null]),
            chosen(rows::mark_published(Outbox::Event, None, &["a".into(), "b\0".into()])),
        ),
        call_case(
            "saga_mark_published",
            "mark_published",
            json!(["saga", ["cmd-1"], "ship"]),
            chosen(rows::mark_published(Outbox::Saga, Some("ship"), &["cmd-1".into()])),
        ),
        call_case(
            "saga_unpublished_count",
            "unpublished_count",
            json!(["saga", "ship"]),
            chosen(rows::unpublished_count(Outbox::Saga, Some("ship"))),
        ),
        call_case(
            "claim_read_params",
            "claim_read_params",
            json!(["order-7", 300_000]),
            params(ledger::claim_read_params("order-7", 300_000)),
        ),
        call_case(
            "ledger_key_params",
            "ledger_key_params",
            json!(["order-7"]),
            params(ledger::key_params("order-7")),
        ),
        call_case(
            "ledger_key_refuses_nul",
            "ledger_key_params",
            json!(["o\u{0}"]),
            params(ledger::key_params("o\0")),
        ),
    ]);
    cases.extend(erasure_lock_cases());
    cases.extend(sealed_row_cases());
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
        ("23514", None),
        ("22012", None),
        ("42P01", None),
        ("3D000", None),
        ("28P01", None),
        ("42501", None),
        ("XX000", None),
    ];
    // Every state on three operations, and the two timeout states, whose verdict turns on the operation,
    // on every operation.
    let full = [
        Op::Append,
        Op::Claim,
        Op::ProjectionSave,
        Op::Ingest,
        Op::ProjectionState,
    ];
    for op in Op::ALL.iter().copied() {
        let states: &[(&str, Option<UniqueKey>)] = if full.contains(&op) {
            &states
        } else {
            &[("55P03", None), ("57014", None)]
        };
        for &(state, constraint) in states {
            cases.push(json!({
                "name": format!("{}_{state}_{}", op.as_str(), constraint.map_or("none", UniqueKey::as_str)),
                "op": op.as_str(), "sqlstate": state, "unique": constraint.map(UniqueKey::as_str), "message": "server text",
                "error": verdict(op, state, constraint, "server text").to_string(),
            }));
        }
    }
    for kind in ["encode", "other", "timeout"] {
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
        let read = match claim_read("order-7", held, 1).unwrap() {
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
    // A row still gone on the last read gives the claim up; a row read then still answers.
    for attempt in 1..=edax_postgres::config::CLAIM_READS {
        let read = claim_read("order-7", None, attempt).map(|read| read.action().as_str());
        cases.push(
            json!({"name": format!("vanished_on_read_{attempt}"), "key": "order-7", "attempt": attempt,
                          "action": read.as_ref().ok(), "error": refusal(&read)}),
        );
    }
    let settle = ledger::settle_params("order-7", b"me", DONE);
    cases.push(
        json!({"name": "settle_params", "key": "order-7", "handle": "me", "record": DONE,
                      "params": settle.as_ref().ok(), "error": refusal(&settle)}),
    );
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

/// Optional ingestion SQL, shared by every ingestion adapter.
#[test]
fn golden_ingest() {
    let mut cases = Vec::new();
    for (name, config) in [
        ("default", "{}"),
        (
            "custom_schema_and_names",
            r#"{"schema":"billing","names":{"ingest_events":"incoming","ingest_state":"current_orders","ingest_rejections":"rejected_orders"}}"#,
        ),
        (
            "partial_names",
            r#"{"schema":"sales","names":{"ingest_state":"current_orders"}}"#,
        ),
        ("malformed_json", "{"),
        ("schema_uppercase", r#"{"schema":"Public"}"#),
        ("schema_injection", r#"{"schema":"public'; DROP TABLE events; --"}"#),
        ("table_quote", r#"{"names":{"ingest_events":"events\""}}"#),
        ("table_dash", r#"{"names":{"ingest_events":"incoming-events"}}"#),
        (
            "shared_name",
            r#"{"names":{"ingest_events":"shared","ingest_state":"shared"}}"#,
        ),
        // Ingestion tables are independent of the event store's, so its default names are free here.
        (
            "event_store_names_allowed",
            r#"{"names":{"ingest_events":"edax_events","ingest_state":"edax_projections"}}"#,
        ),
        ("unknown_role", r#"{"names":{"events":"incoming"}}"#),
        ("unknown_field", r#"{"prefix":"my_"}"#),
    ] {
        let result = edax_postgres::ingest::statements(config);
        cases.push(json!({
            "name": name, "op": "statements", "config": config,
            "want": result.as_ref().ok(), "error": refusal(&result),
        }));
    }
    let row = json!({"stream":"order-42","sequence":1,"fingerprint":"42", "private":{"total":5.808820077685227e16},"public":{"total":1e23,"nested":[-5.808820077685227e16,null]}});
    let rejection = json!({"fingerprint":"digest","error":"codec: rejected","raw":"original","published":false});
    let plan = json!({"outcomes":["accepted","accepted","rejected"],"writes":{"new":row,"changed":row},"rejections":{"new-rejection":rejection,"replay":rejection}});
    let stored = json!({"changed":row});
    let rejected = json!({"replay":rejection});
    let result = edax_postgres::ingest::write_plan(
        &serde_json::from_value(plan.clone()).unwrap(),
        &serde_json::from_value(stored.clone()).unwrap(),
        &serde_json::from_value(rejected.clone()).unwrap(),
    );
    cases.push(json!({"name":"write_plan_exact_numbers_and_first_decisions","op":"write_plan", "plan":plan,"stored":stored,"rejected":rejected,"want":result.as_ref().ok(),"error":refusal(&result)}));
    write_or_check("postgres_ingest.json", cases);
}
