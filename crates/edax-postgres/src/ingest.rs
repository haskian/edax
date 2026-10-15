//! Standalone ingestion storage. These optional tables do not change the ordinary store contract.
//! The host runs `ddl` through its migration tool. Every write must hold `lock` in the same transaction.

use std::collections::BTreeMap;

use edax_core::Error;
use serde::Deserialize;

use crate::schema::{Logical, Tables};

const ROLES: [(&str, Logical); 3] = [
    ("ingest_events", Logical::Events),
    ("ingest_state", Logical::Snapshots),
    ("ingest_rejections", Logical::EventOutbox),
];

fn default_schema() -> String {
    "public".to_owned()
}

fn tables(config_json: &str) -> Result<Tables, Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        #[serde(default = "default_schema")]
        schema: String,
        #[serde(default)]
        names: BTreeMap<String, String>,
    }
    let config: Config = serde_json::from_str(config_json)
        .map_err(|error| Error::bad_handle(format_args!("postgres ingestion config: {error}")))?;
    for role in config.names.keys() {
        if !ROLES.iter().any(|(name, _)| name == role) {
            return Err(Error::bad_handle(format_args!(
                "unknown postgres ingestion role: {role}"
            )));
        }
    }
    let names = ROLES
        .iter()
        .map(|(name, role)| {
            (
                *role,
                config
                    .names
                    .get(*name)
                    .cloned()
                    .unwrap_or_else(|| format!("edax_{name}")),
            )
        })
        .collect();
    Tables::new(&config.schema, names)
}

/// Parameterized statements for the standalone ingestion tables.
///
/// `lock` takes a JSON array of all source and destination keys. Reads take JSON key arrays;
/// writes take JSON objects mapping keys to records. `pending` takes the exact target prefix
/// and a textual limit. `mark_published` takes a source key, its original raw fingerprint,
/// and the exact target prefix.
/// `pending_index` is the host-run index migration, also included in `ddl` for new tables.
/// `check` returns one boolean column, `compatible`. No statement creates tables implicitly.
pub fn statements(config_json: &str) -> Result<BTreeMap<String, String>, Error> {
    let tables = tables(config_json)?;
    let mut sql = BTreeMap::new();
    let mut ddl = String::new();
    let mut checks = Vec::new();
    for (role, logical) in ROLES {
        let table = tables.qualified(logical);
        ddl.push_str(&format!(
            "CREATE TABLE {table} (key text COLLATE \"C\" PRIMARY KEY, value jsonb NOT NULL);\n"
        ));
        let short = role.strip_prefix("ingest_").unwrap_or(role);
        sql.insert(
            format!("read_{short}"),
            format!("SELECT key, value::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key"),
        );
        if short != "rejections" {
            // Planning compares owner, sequence and fingerprint, never the previous payload.
            sql.insert(
                format!("read_{short}_metadata"),
                format!("SELECT key, (value - 'value')::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key"),
            );
        }
        sql.insert(
            format!("write_{short}"),
            format!(
                "INSERT INTO {table} AS target (key, value) SELECT key, value FROM jsonb_each($1::jsonb) ORDER BY key COLLATE \"C\" ON CONFLICT (key) {}",
                if short == "rejections" { "DO NOTHING" } else { "DO UPDATE SET value = EXCLUDED.value" },
            ),
        );
        checks.push(key_value_check(&tables, logical));
    }
    let rejections = tables.qualified(Logical::EventOutbox);
    // Published decisions stay forever; paging must not scan their accumulated history.
    let pending_index = format!("CREATE INDEX ON {rejections} (key) WHERE NOT (value->>'published')::boolean;");
    ddl.push_str(&pending_index);
    ddl.push('\n');
    sql.insert("pending_index".to_owned(), pending_index);
    sql.insert("ddl".to_owned(), ddl);
    sql.insert(
        "check".to_owned(),
        format!("SELECT {} AS compatible", checks.join(" AND ")),
    );

    // Configurations can share a subset of the tables. A fixed namespace preserves exclusion
    // across those configurations; independent table sets can only incur extra contention.
    // Sort signed hashes themselves, not their inputs; collisions only add contention too.
    sql.insert(
        "lock".to_owned(),
        "SELECT pg_catalog.pg_advisory_xact_lock(lock_id) FROM (SELECT DISTINCT pg_catalog.hashtextextended('edax_ingestion/v1:' || key, 0) AS lock_id FROM jsonb_array_elements_text($1::jsonb) AS keys(key)) AS locks ORDER BY lock_id".to_owned(),
    );
    // An explicit digest identifies the current representation without fetching historical raw.
    // Legacy inputs still require their original raw bytes for the planner's exact comparison.
    sql.insert(
        "read_rejections_for_plan".to_owned(),
        format!("SELECT saved.key, (CASE WHEN input.value->>'raw_fingerprint' IS NULL THEN saved.value ELSE jsonb_set(saved.value, '{{raw}}', input.value->'raw') END)::text AS value FROM {rejections} AS saved JOIN (SELECT DISTINCT ON (value->>'source') value FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS inputs(value, position) ORDER BY value->>'source', position) AS input ON saved.key = input.value->>'source' ORDER BY saved.key"),
    );
    sql.insert(
        "pending".to_owned(),
        format!("SELECT key, value::text AS value FROM {rejections} WHERE starts_with(key, $1::text) AND NOT (value->>'published')::boolean ORDER BY key LIMIT $2::text::bigint"),
    );
    sql.insert(
        "mark_published".to_owned(),
        format!("UPDATE {rejections} SET value = jsonb_set(value, '{{published}}', 'true'::jsonb) WHERE key = $1::text AND value->>'fingerprint' = $2::text AND starts_with(key, $3::text) RETURNING key"),
    );
    Ok(sql)
}

/// Recovery after a failed ingestion transaction. Isolation requires proof of rollback.
/// An uncertain commit, connection failure, resource exhaustion or unknown failure retries unchanged.
pub fn recovery(sqlstate: Option<&str>, rolled_back: bool) -> &'static str {
    match sqlstate.and_then(|state| state.get(..2)) {
        Some("28" | "3D" | "3F" | "42") => "fatal",
        Some("22" | "23" | "54") if rolled_back => "isolate",
        _ => "retry",
    }
}

/// Catalog contract shared by ingestion stores with text keys and JSONB records.
pub(crate) fn key_value_check(tables: &Tables, logical: Logical) -> String {
    format!(
        r#"EXISTS (
 SELECT 1 FROM pg_catalog.pg_class t
 JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
 JOIN pg_catalog.pg_attribute k ON k.attrelid = t.oid AND k.attname = 'key' AND NOT k.attisdropped
 JOIN pg_catalog.pg_attribute v ON v.attrelid = t.oid AND v.attname = 'value' AND NOT v.attisdropped
 JOIN pg_catalog.pg_collation col ON col.oid = k.attcollation
 WHERE n.nspname = '{schema}' AND t.relname = '{name}' AND t.relkind IN ('r', 'p')
 AND NOT t.relrowsecurity AND NOT t.relforcerowsecurity
 AND k.atttypid = 'pg_catalog.text'::regtype AND k.attnotnull AND k.attgenerated = ''
 AND col.collname IN ('C', 'POSIX') AND col.collnamespace = 'pg_catalog'::regnamespace
 AND v.atttypid = 'pg_catalog.jsonb'::regtype AND v.attnotnull AND v.attgenerated = ''
 AND EXISTS (
  SELECT 1 FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_collation ic ON ic.oid = i.indcollation[0]
  JOIN pg_catalog.pg_opclass op ON op.oid = i.indclass[0]
  JOIN pg_catalog.pg_am am ON am.oid = op.opcmethod
  WHERE i.indrelid = t.oid AND i.indisunique AND i.indisvalid AND i.indisready
  AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL
  AND i.indnkeyatts = 1 AND i.indkey[0] = k.attnum
  AND ic.collname IN ('C', 'POSIX') AND ic.collnamespace = 'pg_catalog'::regnamespace
  AND am.amname = 'btree' AND op.opcname = 'text_ops' AND op.opcnamespace = 'pg_catalog'::regnamespace
 )
 AND NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_attribute extra
  WHERE extra.attrelid = t.oid AND extra.attnum > 0 AND NOT extra.attisdropped
  AND extra.attname NOT IN ('key', 'value') AND extra.attnotnull
  AND NOT extra.atthasdef AND extra.attidentity = '' AND extra.attgenerated = ''
 )
)"#,
        schema = tables.schema(),
        name = tables.name(logical),
    )
}
