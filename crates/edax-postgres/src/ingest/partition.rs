//! Opt-in append partition coordinates and the physical table's durable admission authority.

use std::collections::BTreeMap;

use crate::schema::RoleTables;

pub(super) const CONTROL_ROLE: &str = "ingest_partition_control";

pub(super) fn events_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (key text COLLATE \"C\" NOT NULL, partition_at timestamptz NOT NULL, value jsonb NOT NULL, PRIMARY KEY (key, partition_at)) PARTITION BY RANGE (partition_at);\n"
    )
}

/// Each component is an exact integer below 2^53, even at the largest UUIDv7 timestamp.
/// Arithmetic on a timestamp without time zone avoids session-zone daylight-saving shifts.
fn timestamp(micros: &str) -> String {
    format!(
        "((TIMESTAMP '1970-01-01 00:00:00' + (({micros}) / 86400000000) * INTERVAL '1 day' + (({micros}) % 86400000000) * INTERVAL '1 microsecond') AT TIME ZONE 'UTC')"
    )
}

/// Coordinate reads use exact matching plus explicit extrema for partition pruning.
fn read_events(table: &str, metadata: bool) -> String {
    let coordinate = timestamp("coordinate.value::bigint");
    let lower = timestamp("(SELECT min(value::bigint) FROM jsonb_each_text($1::jsonb))");
    let upper = timestamp("(SELECT max(value::bigint) FROM jsonb_each_text($1::jsonb))");
    let value = if metadata {
        "(saved.value - 'private' - 'public')"
    } else {
        "saved.value"
    };
    let lock = if metadata { " FOR UPDATE OF saved" } else { "" };
    format!(
        "SELECT saved.key, {value}::text AS value FROM {table} AS saved JOIN jsonb_each_text($1::jsonb) AS coordinate(key, value) ON saved.key = coordinate.key AND saved.partition_at = {coordinate} WHERE saved.partition_at >= {lower} AND saved.partition_at <= {upper} ORDER BY saved.key COLLATE \"C\"{lock}"
    )
}

pub(super) fn configure(tables: &RoleTables, sql: &mut BTreeMap<String, String>, ddl: &mut String) {
    let events = tables.qualified("ingest_events");
    let rejections = tables.qualified("ingest_rejections");
    let control = tables.qualified(CONTROL_ROLE);
    let paired = format!("key = '{events}' AND value->>'rejections' = '{rejections}'");
    let manifest_prefix = format!("{events}:manifest:");
    ddl.push_str(&format!(
        "CREATE TABLE IF NOT EXISTS {control} (key text COLLATE \"C\" PRIMARY KEY, value jsonb NOT NULL);\nINSERT INTO {control} (key, value) VALUES ('{events}', jsonb_build_object('rejections', '{rejections}', 'horizon', '0')) ON CONFLICT (key) DO NOTHING;\n"
    ));
    // This bounded gate is per physical table, never per event, source, tenant or generation.
    let gate = format!("pg_catalog.hashtextextended('edax:ingest:partition:{events}', 0)");
    sql.insert(
        "gate_shared".into(),
        format!("SELECT pg_catalog.pg_advisory_xact_lock_shared({gate})"),
    );
    sql.insert(
        "gate_exclusive".into(),
        format!("SELECT pg_catalog.pg_advisory_xact_lock({gate})"),
    );
    for (name, lock) in [("floor_read", "SHARE"), ("floor_write", "UPDATE")] {
        sql.insert(
            name.into(),
            format!("SELECT value->>'horizon' AS horizon FROM {control} WHERE {paired} FOR {lock}"),
        );
    }
    sql.insert("floor_advance".into(), format!(
        "UPDATE {control} SET value = jsonb_set(value, '{{horizon}}', to_jsonb(GREATEST((value->>'horizon')::bigint, $1::text::bigint)::text)) WHERE {paired} RETURNING value->>'horizon' AS horizon"
    ));
    sql.insert("read_manifest".into(), format!(
        "SELECT requested.key, saved.value::text AS value FROM {control} AS saved JOIN jsonb_array_elements_text($1::jsonb) AS requested(key) ON saved.key = '{manifest_prefix}' || requested.key ORDER BY requested.key COLLATE \"C\""
    ));
    let bind = format!(
        "INSERT INTO {control} (key, value) SELECT '{manifest_prefix}' || key, value FROM jsonb_each($1::jsonb) ON CONFLICT (key) DO NOTHING"
    );
    sql.insert("bind_manifest".into(), bind.clone());
    sql.insert("bind_events".into(), bind);
    sql.insert("read_events".into(), read_events(&events, false));
    sql.insert("read_events_metadata".into(), read_events(&events, true));
    let coordinate = timestamp("($2::jsonb ->> changed.key)::bigint");
    sql.insert("insert_events".into(), format!(
        "INSERT INTO {events} (key, partition_at, value) SELECT changed.key, {coordinate}, changed.value FROM jsonb_each($1::jsonb) AS changed(key, value) ORDER BY changed.key COLLATE \"C\""
    ));
    sql.insert("update_events".into(), format!(
        "UPDATE {events} AS saved SET value = changed.value FROM jsonb_each($1::jsonb) AS changed(key, value) WHERE saved.key = changed.key AND saved.partition_at = {coordinate}"
    ));
}

pub(super) fn events_check(schema: &str, name: &str) -> String {
    format!(
        r#"EXISTS (
 SELECT 1 FROM pg_catalog.pg_class t
 JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
 JOIN pg_catalog.pg_attribute k ON k.attrelid = t.oid AND k.attname = 'key' AND NOT k.attisdropped
 JOIN pg_catalog.pg_attribute v ON v.attrelid = t.oid AND v.attname = 'value' AND NOT v.attisdropped
 JOIN pg_catalog.pg_attribute p ON p.attrelid = t.oid AND p.attname = 'partition_at' AND NOT p.attisdropped
 JOIN pg_catalog.pg_collation col ON col.oid = k.attcollation
 JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = t.oid
 JOIN pg_catalog.pg_opclass pc ON pc.oid = pt.partclass[0]
 WHERE n.nspname = '{schema}' AND t.relname = '{name}' AND t.relkind = 'p'
 AND NOT t.relrowsecurity AND NOT t.relforcerowsecurity
 AND k.atttypid = 'pg_catalog.text'::regtype AND k.attnotnull AND k.attgenerated = ''
 AND col.collname IN ('C', 'POSIX') AND col.collnamespace = 'pg_catalog'::regnamespace
 AND v.atttypid = 'pg_catalog.jsonb'::regtype AND v.attnotnull AND v.attgenerated = ''
 AND p.atttypid = 'pg_catalog.timestamptz'::regtype AND p.attnotnull AND p.attgenerated = ''
 AND p.atttypmod IN (-1, 3, 4, 5, 6)
 AND pt.partstrat = 'r' AND pt.partnatts = 1 AND pt.partattrs[0] = p.attnum AND pt.partexprs IS NULL
 AND pc.opcname = 'timestamptz_ops' AND pc.opcnamespace = 'pg_catalog'::regnamespace
 AND pt.partcollation[0] = 0
 AND EXISTS (
  SELECT 1 FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_collation ic ON ic.oid = i.indcollation[0]
  JOIN pg_catalog.pg_opclass ko ON ko.oid = i.indclass[0]
  JOIN pg_catalog.pg_opclass po ON po.oid = i.indclass[1]
  JOIN pg_catalog.pg_am am ON am.oid = ko.opcmethod
  WHERE i.indrelid = t.oid AND i.indisunique AND i.indisvalid AND i.indisready
  AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL
  AND i.indnkeyatts = 2 AND i.indkey[0] = k.attnum AND i.indkey[1] = p.attnum
  AND ic.collname IN ('C', 'POSIX') AND ic.collnamespace = 'pg_catalog'::regnamespace
  AND i.indcollation[1] = 0
  AND am.amname = 'btree'
  AND ko.opcname = 'text_ops' AND ko.opcnamespace = 'pg_catalog'::regnamespace
  AND po.opcname = 'timestamptz_ops' AND po.opcnamespace = 'pg_catalog'::regnamespace
  AND po.opcmethod = am.oid
 )
 AND {omitted}
)"#,
        omitted = crate::schema::omitted_columns_check(&["key", "partition_at", "value"]),
    )
}

#[cfg(test)]
mod tests {
    use super::super::statements;

    fn psql(url: &str, sql: &str) -> String {
        let output = std::process::Command::new("psql")
            .args(["-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", url, "-c", sql])
            .output()
            .expect("psql is required when EDAX_POSTGRES_URL is set");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        String::from_utf8(output.stdout).unwrap()
    }

    #[test]
    fn partitioning_is_opt_in_and_its_control_table_is_a_named_role() {
        assert_eq!(
            statements("{}").unwrap(),
            statements(r#"{"partitioned":false}"#).unwrap()
        );
        let renamed =
            statements(r#"{"partitioned":true,"names":{"ingest_partition_control":"pulse_partitions"}}"#).unwrap();
        for name in [
            "ddl",
            "floor_read",
            "floor_write",
            "floor_advance",
            "read_manifest",
            "bind_manifest",
            "check",
        ] {
            assert!(renamed[name].contains("pulse_partitions"), "{name}");
            assert!(!renamed[name].contains("edax_ingest_partition_control"), "{name}");
        }
        // An unpartitioned sink accepts the control name and never uses it.
        let unused = statements(r#"{"names":{"ingest_partition_control":"pulse_partitions"}}"#).unwrap();
        assert_eq!(unused, statements("{}").unwrap());
        // A data table may never take the control table's name, partitioned or not yet.
        for partitioned in [true, false] {
            for role in ["ingest_events", "ingest_state", "ingest_rejections"] {
                let config =
                    format!(r#"{{"partitioned":{partitioned},"names":{{"{role}":"edax_ingest_partition_control"}}}}"#);
                let error = statements(&config).unwrap_err();
                assert!(error.message.contains("are both named"), "{role}: {error}");
            }
        }
    }

    #[test]
    fn partition_authority_is_shared_across_generations_and_bound_to_rejections() {
        let original = statements(r#"{"partitioned":true}"#).unwrap();
        let alternate = statements(r#"{"partitioned":true,"names":{"ingest_rejections":"other_rejections"}}"#).unwrap();
        assert_eq!(original["gate_shared"], alternate["gate_shared"]);
        assert_ne!(original["floor_read"], alternate["floor_read"]);
        assert_eq!(original["bind_manifest"], alternate["bind_manifest"]);
        assert!(original["floor_advance"].contains("GREATEST"));
        assert!(original["floor_read"].ends_with("FOR SHARE"));
        assert!(original["floor_write"].ends_with("FOR UPDATE"));
        assert!(original["ddl"].contains("PRIMARY KEY (key, partition_at)"));
        assert!(original["read_events_metadata"].ends_with("FOR UPDATE OF saved"));
        assert!(original["check"].contains("p.atttypmod IN (-1, 3, 4, 5, 6)"));
        assert!(!original["insert_events"].contains("ON CONFLICT"));
    }

    #[test]
    fn live_partition_catalog_requires_exact_coordinates_and_usable_uniqueness() {
        let Ok(url) = std::env::var("EDAX_POSTGRES_URL") else {
            return;
        };
        let schema = format!("partition_catalog_{}", edax_core::guid_simple());
        let sql = statements(&format!(r#"{{"schema":"{schema}","partitioned":true}}"#)).unwrap();
        let events = format!("\"{schema}\".edax_ingest_events");
        for (name, ddl, change, expected) in [
            ("reference", sql["ddl"].clone(), String::new(), true),
            ("ordinary_table", sql["ddl"].replace(" PARTITION BY RANGE (partition_at)", ""), String::new(), false),
            ("list_partitioning", sql["ddl"].replace("PARTITION BY RANGE", "PARTITION BY LIST"), String::new(), false),
            ("rounds_milliseconds", sql["ddl"].replace("partition_at timestamptz", "partition_at timestamptz(2)"), String::new(), false),
            ("keeps_milliseconds", sql["ddl"].replace("partition_at timestamptz", "partition_at timestamptz(3)"), String::new(), true),
            ("wrong_key_order", sql["ddl"].replace("PRIMARY KEY (key, partition_at)", "PRIMARY KEY (partition_at, key)"), String::new(), false),
            ("deferred_unique", sql["ddl"].replace("PRIMARY KEY (key, partition_at)", "PRIMARY KEY (key, partition_at) DEFERRABLE"), String::new(), false),
            ("missing_unique", sql["ddl"].clone(), format!("ALTER TABLE {events} DROP CONSTRAINT edax_ingest_events_pkey;"), false),
            ("invalid_parent_index", sql["ddl"].clone(), format!("CREATE TABLE \"{schema}\".child PARTITION OF {events} DEFAULT; ALTER TABLE {events} DROP CONSTRAINT edax_ingest_events_pkey; CREATE UNIQUE INDEX incomplete ON ONLY {events} (key, partition_at);"), false),
            ("pattern_opclass", sql["ddl"].clone(), format!("ALTER TABLE {events} DROP CONSTRAINT edax_ingest_events_pkey; CREATE UNIQUE INDEX patterns ON {events} (key text_pattern_ops, partition_at);"), false),
            ("row_security", sql["ddl"].clone(), format!("ALTER TABLE {events} ENABLE ROW LEVEL SECURITY;"), false),
            ("nullable_payload", sql["ddl"].clone(), format!("ALTER TABLE {events} ALTER COLUMN value DROP NOT NULL;"), false),
            ("required_extra", sql["ddl"].clone(), format!("ALTER TABLE {events} ADD COLUMN extra text NOT NULL;"), false),
            ("generated_public", sql["ddl"].clone(), format!("ALTER TABLE {events} ADD COLUMN public_id text GENERATED ALWAYS AS (value->'public'->>'id') STORED;"), true),
        ] {
            let result = psql(&url, &format!("BEGIN; CREATE SCHEMA \"{schema}\"; {ddl} {change} {}; ROLLBACK;", sql["check"]));
            assert_eq!(result.trim(), if expected { "t" } else { "f" }, "{name}");
        }
    }

    #[test]
    fn live_partition_writes_preserve_full_uuid7_precision_across_time_zones() {
        let Ok(url) = std::env::var("EDAX_POSTGRES_URL") else {
            return;
        };
        let schema = format!("partition_precision_{}", edax_core::guid_simple());
        let sql = statements(&format!(r#"{{"schema":"{schema}","partitioned":true}}"#)).unwrap();
        let coordinates = r#"{"zero":"0","one":"1000","ordinary":"1645557742000000","maximum":"281474976710655000"}"#;
        let rows = r#"{"zero":{"public":{"id":"zero"}},"one":{"public":{"id":"one"}},"ordinary":{"public":{"id":"ordinary"}},"maximum":{"public":{"id":"maximum"}}}"#;
        for timezone in ["UTC", "America/New_York", "Pacific/Auckland"] {
            let query = format!(
                "BEGIN; CREATE SCHEMA \"{schema}\"; {} CREATE TABLE \"{schema}\".payload PARTITION OF \"{schema}\".edax_ingest_events DEFAULT; SET TIME ZONE '{timezone}'; PREPARE write_rows(jsonb,jsonb) AS {}; EXECUTE write_rows('{rows}','{coordinates}'); SELECT key, (extract(epoch FROM partition_at) * 1000000)::bigint FROM \"{schema}\".edax_ingest_events ORDER BY key; PREPARE read_rows(jsonb) AS {}; EXECUTE read_rows('{coordinates}'); ROLLBACK;",
                sql["ddl"], sql["insert_events"], sql["read_events_metadata"],
            );
            let result = psql(&url, &query);
            assert_eq!(result, "maximum|281474976710655000\none|1000\nordinary|1645557742000000\nzero|0\nmaximum|{}\none|{}\nordinary|{}\nzero|{}\n", "{timezone}");
        }
    }

    #[test]
    fn live_partition_control_keeps_its_floor_and_immutable_manifests() {
        let Ok(url) = std::env::var("EDAX_POSTGRES_URL") else {
            return;
        };
        for control in [None, Some("pulse_partitions")] {
            let schema = format!("partition_control_{}", edax_core::guid_simple());
            let mut names = serde_json::Map::new();
            if let Some(name) = control {
                names.insert("ingest_partition_control".into(), name.into());
            }
            let sql_for = |names: &serde_json::Map<String, serde_json::Value>| {
                statements(&serde_json::json!({"schema": schema, "partitioned": true, "names": names}).to_string())
                    .unwrap()
            };
            let sql = sql_for(&names);
            names.insert("ingest_rejections".into(), "other_rejections".into());
            let alternate = sql_for(&names);
            let query = format!(
                "BEGIN; CREATE SCHEMA \"{schema}\"; {} {}; {}; PREPARE advance(text) AS {}; EXECUTE advance('100'); EXECUTE advance('50'); {}; {}; PREPARE bind(jsonb) AS {}; EXECUTE bind('{{\"generation\":{{\"mapper\":\"first\"}}}}'); EXECUTE bind('{{\"generation\":{{\"mapper\":\"second\"}}}}'); PREPARE read_manifest(jsonb) AS {}; EXECUTE read_manifest('[\"generation\"]'); {}; ROLLBACK;",
                sql["ddl"], sql["floor_read"], sql["floor_write"], sql["floor_advance"], sql["floor_read"], alternate["floor_read"], sql["bind_manifest"], sql["read_manifest"], sql["check"],
            );
            assert_eq!(
                psql(&url, &query),
                "0\n0\n100\n100\n100\ngeneration|{\"mapper\": \"first\"}\nt\n",
                "{control:?}"
            );
        }
    }
}
