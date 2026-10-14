use edax_postgres::ingest::{recovery, statements};

#[test]
fn ingestion_configuration_rejects_unsafe_or_ambiguous_names() {
    for (name, config) in [
        ("malformed", "{"),
        ("unknown_field", r#"{"table":"events"}"#),
        ("unknown_role", r#"{"names":{"events":"events"}}"#),
        ("injected_schema", r#"{"schema":"public'; DROP TABLE events; --"}"#),
        ("injected_name", r#"{"names":{"ingest_events":"events\""}}"#),
        (
            "duplicate_role_tables",
            r#"{"names":{"ingest_events":"same","ingest_state":"same"}}"#,
        ),
        ("uppercase", r#"{"schema":"Public"}"#),
    ] {
        assert!(statements(config).is_err(), "{name}");
    }
    let sql = statements(r#"{"schema":"billing","names":{"ingest_events":"incoming"}}"#).unwrap();
    assert!(sql["ddl"].contains("CREATE TABLE \"billing\".\"incoming\""));
    assert!(sql["ddl"].contains("CREATE TABLE \"billing\".\"edax_ingest_state\""));
    assert_eq!(sql["ddl"].matches("CREATE TABLE").count(), 3);
}

#[test]
fn ingestion_lock_order_and_table_contract_are_explicit() {
    let sql = statements("{}").unwrap();
    assert!(sql["lock"].contains("SELECT DISTINCT pg_catalog.hashtextextended"));
    assert!(sql["lock"].ends_with("ORDER BY lock_id"));
    let shared_state = statements(r#"{"schema":"billing","names":{"ingest_events":"different"}}"#).unwrap();
    assert_eq!(
        sql["lock"], shared_state["lock"],
        "table configuration cannot bypass exclusion"
    );
    assert!(sql["lock"].contains("edax_ingestion/v1:"));
    for clause in [
        "i.indimmediate",
        "i.indpred IS NULL",
        "i.indexprs IS NULL",
        "i.indisvalid",
        "i.indnkeyatts = 1",
        "extra.attnotnull",
        "k.attnotnull",
        "v.attnotnull",
        "op.opcname = 'text_ops'",
    ] {
        assert!(sql["check"].contains(clause), "{clause}");
    }
    assert!(sql["mark_published"].contains("value->>'fingerprint' = $2::text"));
    assert!(sql["pending"].contains("starts_with(key, $1::text)"));
    assert!(sql["pending"].contains("NOT (value->>'published')::boolean"));
    for role in ["events", "state", "rejections"] {
        assert!(sql[&format!("read_{role}")].contains("jsonb_array_elements_text($1::jsonb)"));
        assert!(sql[&format!("write_{role}")].contains(if role == "rejections" {
            "ON CONFLICT (key) DO NOTHING"
        } else {
            "ON CONFLICT (key) DO UPDATE"
        }));
    }
}

#[test]
fn only_record_failures_with_proven_rollback_are_isolated() {
    for (state, rolled_back, expected) in [
        (Some("22003"), true, "isolate"),
        (Some("23514"), true, "isolate"),
        (Some("54000"), true, "isolate"),
        (Some("54000"), false, "retry"),
        (Some("23505"), false, "retry"),
        (Some("22003"), false, "retry"),
        (Some("08006"), true, "retry"),
        (Some("40001"), true, "retry"),
        (Some("40P01"), true, "retry"),
        (Some("53000"), true, "retry"),
        (Some("57014"), true, "retry"),
        (Some("XX000"), true, "retry"),
        (None, true, "retry"),
        (None, false, "retry"),
        (Some(""), true, "retry"),
        (Some("28P01"), true, "fatal"),
        (Some("3D000"), false, "fatal"),
        (Some("3F000"), true, "fatal"),
        (Some("42P01"), false, "fatal"),
    ] {
        assert_eq!(
            recovery(state, rolled_back),
            expected,
            "{state:?}, rollback={rolled_back}"
        );
    }
}
