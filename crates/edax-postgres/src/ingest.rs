//! Standalone ingestion storage. These optional tables do not change the ordinary store contract.
//! The host runs `ddl` through its migration tool. A plan read locks the rows it finds; an absent key
//! is inserted without `ON CONFLICT`, so a racing first insert fails as `concurrency` and retries.

use std::collections::BTreeMap;

use edax_core::Error;
use serde::Deserialize;

use crate::schema::identifier;

const ROLES: [&str; 3] = ["ingest_events", "ingest_state", "ingest_rejections"];

/// The configured schema and the quoted, qualified table of each role.
struct Tables {
    schema: String,
    names: BTreeMap<&'static str, String>,
}

impl Tables {
    fn qualified(&self, role: &str) -> String {
        format!(
            "\"{}\".\"{}\"",
            self.schema,
            self.names.get(role).map_or("", String::as_str)
        )
    }
}

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
        if !ROLES.contains(&role.as_str()) {
            return Err(Error::bad_handle(format_args!(
                "unknown postgres ingestion role: {role}"
            )));
        }
    }
    let mut names = BTreeMap::new();
    for role in ROLES {
        let name = config
            .names
            .get(role)
            .cloned()
            .unwrap_or_else(|| format!("edax_{role}"));
        let name = identifier("table name", &name)?;
        if names.values().any(|taken| *taken == name) {
            return Err(Error::bad_handle(format_args!(
                "postgres ingestion roles need distinct tables, {name:?} is named twice"
            )));
        }
        names.insert(role, name);
    }
    Ok(Tables {
        schema: identifier("schema", &config.schema)?,
        names,
    })
}

/// Parameterized statements for the standalone ingestion tables.
///
/// No statement takes an advisory lock. A plan read locks the rows it finds (`FOR UPDATE`, in key
/// order), so a concurrent writer of the same key waits. Keys it does not find are inserted without
/// `ON CONFLICT`, so a concurrent first insert fails with 23505 and the batch retries against the
/// committed row. Reads take JSON key arrays; writes take JSON objects mapping keys to records.
/// `read_rejections_for_plan` takes `[{"source", "fingerprinted"}]`. `pending` takes the exact
/// target prefix and a textual limit. `mark_published` takes `{source: fingerprint}` and the exact
/// target prefix, and returns how many rows it marked. `check` returns one boolean column,
/// `compatible`. No statement creates tables implicitly.
pub fn statements(config_json: &str) -> Result<BTreeMap<String, String>, Error> {
    let tables = tables(config_json)?;
    let mut sql = BTreeMap::new();
    let mut ddl = String::new();
    let mut checks = Vec::new();
    for role in ROLES {
        let table = tables.qualified(role);
        ddl.push_str(&format!(
            "CREATE TABLE {table} (key text COLLATE \"C\" PRIMARY KEY, value jsonb NOT NULL);\n"
        ));
        let short = role.strip_prefix("ingest_").unwrap_or(role);
        sql.insert(
            format!("read_{short}"),
            format!("SELECT key, value::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key"),
        );
        // The target manifest: a concurrent first bind inserts it once, and both compare it.
        sql.insert(
            format!("bind_{short}"),
            format!("INSERT INTO {table} (key, value) SELECT key, value FROM jsonb_each($1::jsonb) ON CONFLICT (key) DO NOTHING"),
        );
        sql.insert(
            format!("insert_{short}"),
            format!("INSERT INTO {table} (key, value) SELECT key, value FROM jsonb_each($1::jsonb) ORDER BY key COLLATE \"C\""),
        );
        if short != "rejections" {
            // Planning compares owner, sequence and fingerprint, never the previous payload.
            sql.insert(
                format!("read_{short}_metadata"),
                format!("SELECT key, (value - 'value')::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key FOR UPDATE"),
            );
            // Only rows the plan read, and so locked, are updated.
            sql.insert(
                format!("update_{short}"),
                format!("UPDATE {table} AS target SET value = changed.value FROM jsonb_each($1::jsonb) AS changed(key, value) WHERE target.key = changed.key"),
            );
        }
        checks.push(key_value_check_named(
            &tables.schema,
            tables.names.get(role).map_or("", String::as_str),
        ));
    }
    let rejections = tables.qualified("ingest_rejections");
    // Published decisions stay forever; paging must not scan their accumulated history.
    ddl.push_str(&format!(
        "CREATE INDEX ON {rejections} (key) WHERE NOT (value->>'published')::boolean;\n"
    ));
    sql.insert("ddl".to_owned(), ddl);
    sql.insert(
        "check".to_owned(),
        format!("SELECT {} AS compatible", checks.join(" AND ")),
    );
    // A target's keys share its prefix, which ends in ':'; the next character, ';', bounds the
    // range, so the key index serves it on every supported server version.
    let in_target = |column: &str, n: u8| format!("{column} >= ${n}::text AND {column} < left(${n}::text, -1) || ';'");
    // A fingerprinted input needs only the stored fingerprint: the plan takes the current raw
    // from the input, so the stored raw stays on the server.
    sql.insert(
        "read_rejections_for_plan".to_owned(),
        format!("SELECT saved.key, (CASE WHEN input.fingerprinted THEN jsonb_set(saved.value, '{{raw}}', '\"\"') ELSE saved.value END)::text AS value FROM {rejections} AS saved JOIN (SELECT DISTINCT ON (value->>'source') value->>'source' AS source, (value->>'fingerprinted')::boolean AS fingerprinted FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS inputs(value, position) ORDER BY value->>'source', position) AS input ON saved.key = input.source ORDER BY saved.key FOR UPDATE OF saved"),
    );
    sql.insert(
        "pending".to_owned(),
        format!("SELECT key, value::text AS value FROM {rejections} WHERE {} AND NOT (value->>'published')::boolean ORDER BY key LIMIT $2::text::bigint", in_target("key", 1)),
    );
    sql.insert(
        "mark_published".to_owned(),
        format!("WITH marked AS (UPDATE {rejections} AS saved SET value = jsonb_set(saved.value, '{{published}}', 'true'::jsonb) FROM jsonb_each_text($1::jsonb) AS confirmed(key, fingerprint) WHERE saved.key = confirmed.key AND saved.value->>'fingerprint' = confirmed.fingerprint AND {} RETURNING 1) SELECT count(*) FROM marked", in_target("saved.key", 2)),
    );
    Ok(sql)
}

/// The catalog check for a `key text COLLATE "C" PRIMARY KEY, value jsonb` table, shared by the
/// ingestion and projection state stores.
pub(crate) fn key_value_check_named(schema: &str, name: &str) -> String {
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
    )
}
