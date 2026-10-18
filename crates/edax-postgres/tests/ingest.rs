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
fn ingestion_locks_rows_not_keys_and_checks_the_table_contract() {
    let sql = statements("{}").unwrap();
    // Row locks and plain inserts bound the lock table: no statement takes an advisory lock.
    assert!(sql.values().all(|statement| !statement.contains("advisory")));
    assert!(sql["read_rejections_for_plan"].ends_with("FOR UPDATE OF saved"));
    for role in ["events", "state"] {
        assert!(sql[&format!("read_{role}_metadata")].ends_with("ORDER BY key FOR UPDATE"));
        assert!(sql[&format!("update_{role}")].contains("WHERE target.key = changed.key"));
    }
    for role in ["events", "state", "rejections"] {
        assert!(sql[&format!("read_{role}")].contains("jsonb_array_elements_text($1::jsonb)"));
        assert!(!sql[&format!("insert_{role}")].contains("ON CONFLICT"));
    }
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
    assert!(sql["mark_published"].contains("saved.value->>'fingerprint' = confirmed.fingerprint"));
    // A key range, not starts_with, so the key index serves it before Postgres 15.
    assert!(sql["pending"].contains("key >= $1::text AND key < left($1::text, -1) || ';'"));
    assert!(sql["pending"].contains("NOT (value->>'published')::boolean"));
}

#[test]
fn ingestion_ddl_indexes_pending_rejections_of_the_configured_table() {
    for (config, table) in [
        ("{}", "\"public\".\"edax_ingest_rejections\""),
        (
            r#"{"schema":"billing","names":{"ingest_rejections":"refused_orders"}}"#,
            "\"billing\".\"refused_orders\"",
        ),
    ] {
        let sql = statements(config).unwrap();
        let index = format!("CREATE INDEX ON {table} (key) WHERE NOT (value->>'published')::boolean;");
        assert_eq!(sql["ddl"].matches(&index).count(), 1);
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
        (Some("23505"), true, "retry"),
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
