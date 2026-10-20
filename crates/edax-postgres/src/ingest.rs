//! Standalone ingestion storage. These optional tables do not change the ordinary store contract.
//! The host runs `ddl` through its migration tool. A plan read locks the rows it finds; an absent key
//! is inserted without `ON CONFLICT`, so a racing first insert fails as `concurrency` and retries.

use std::collections::BTreeMap;

use edax_core::Error;
use edax_cqrs::ingest::{Plan, Rejection, Stored};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::identifier;

mod partition;

const ROLES: [&str; 3] = ["ingest_events", "ingest_state", "ingest_rejections"];

/// Opaque JSON parameters for the three ingestion write statements. The host must pass these
/// strings unchanged: reparsing and serializing a large double can change its decimal value.
#[derive(Debug, Serialize)]
pub struct WritePlan {
    /// Previously absent target records, inserted without conflict suppression.
    pub inserts: String,
    /// Target records already locked by the planning read.
    pub updates: String,
    /// First durable rejection decisions only, never a replay's substitute raw representation.
    pub rejections: String,
}

/// Translate a decided batch and its locked snapshot into exact-number SQL parameters.
pub fn write_plan(
    plan: &Plan,
    stored: &BTreeMap<String, Stored>,
    rejected: &BTreeMap<String, Rejection>,
) -> Result<WritePlan, Error> {
    let mut inserts = BTreeMap::new();
    let mut updates = BTreeMap::new();
    for (key, row) in &plan.writes {
        if stored.contains_key(key) {
            updates.insert(key, row);
        } else {
            inserts.insert(key, row);
        }
    }
    let rejections: BTreeMap<_, _> = plan
        .rejections
        .iter()
        .filter(|(key, _)| !rejected.contains_key(*key))
        .collect();
    fn exact(value: &impl Serialize) -> Result<String, Error> {
        let value: Value = serde_json::to_value(value).map_err(Error::codec)?;
        Ok(crate::documents::exact_json(&value))
    }
    Ok(WritePlan {
        inserts: exact(&inserts)?,
        updates: exact(&updates)?,
        rejections: exact(&rejections)?,
    })
}

/// The configured schema and the quoted, qualified table of each role.
struct Tables {
    schema: String,
    names: BTreeMap<&'static str, String>,
    partitioned: bool,
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
        #[serde(default)]
        partitioned: bool,
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
        if config.partitioned && name == partition::CONTROL_TABLE {
            return Err(Error::bad_handle(
                "postgres ingestion table name is reserved for partition control",
            ));
        }
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
        partitioned: config.partitioned,
    })
}

/// Parameterized statements for the standalone ingestion tables.
///
/// Ordinary ingestion takes no advisory lock. A plan read locks the rows it finds (`FOR UPDATE`, in key
/// order), so a concurrent writer of the same key waits. Keys it does not find are inserted without
/// `ON CONFLICT`, so a concurrent first insert fails with 23505 and the batch retries against the
/// committed row. Reads take JSON key arrays; writes take JSON objects mapping keys to records.
/// Read target metadata before rejections: under READ COMMITTED, the rejection read must see any
/// rejection committed by the target writer the metadata read waited for. Reading rejections first
/// could combine their earlier absence with a later target version and accept a rejected input as
/// stale. The immutable mapper/source contract keeps a source's target key stable.
/// `delete_scope_{events,state}` take the bounds of `ingest::scope_range` and a positive textual page
/// limit and return how many rows they deleted. `read_rejections_for_plan` takes a JSON source-key array. `pending` takes the exact
/// target prefix and a textual limit. `mark_published` takes `{source: fingerprint}` and the exact
/// target prefix, and returns how many rows it marked. `check` returns one boolean column,
/// `compatible`. No statement creates tables implicitly.
/// Opt-in `partitioned` statements add a transaction advisory gate per physical events table,
/// a durable floor, and exact partition coordinates; ordinary statements remain unchanged.
pub fn statements(config_json: &str) -> Result<BTreeMap<String, String>, Error> {
    let tables = tables(config_json)?;
    let mut sql = BTreeMap::new();
    let mut ddl = String::new();
    let mut checks = Vec::new();
    for role in ROLES {
        let table = tables.qualified(role);
        if tables.partitioned && role == "ingest_events" {
            ddl.push_str(&partition::events_ddl(&table));
        } else {
            ddl.push_str(&format!(
                "CREATE TABLE {table} (key text COLLATE \"C\" PRIMARY KEY, value jsonb NOT NULL);\n"
            ));
        }
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
                format!("SELECT key, (value - 'private' - 'public')::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key FOR UPDATE"),
            );
            // An erasure cleanup: every row of one subject, the key range `ingest::scope_range` names.
            sql.insert(format!("delete_scope_{short}"), delete_range(&table));
            // Only rows the plan read, and so locked, are updated.
            sql.insert(
                format!("update_{short}"),
                format!("UPDATE {table} AS target SET value = changed.value FROM jsonb_each($1::jsonb) AS changed(key, value) WHERE target.key = changed.key"),
            );
        }
        let check = if tables.partitioned && role == "ingest_events" {
            partition::events_check
        } else {
            key_value_check_named
        };
        checks.push(check(&tables.schema, tables.names.get(role).map_or("", String::as_str)));
    }
    let rejections = tables.qualified("ingest_rejections");
    // Published decisions stay forever; paging must not scan their accumulated history.
    ddl.push_str(&format!(
        "CREATE INDEX ON {rejections} (key) WHERE NOT (value->>'published')::boolean;\n"
    ));
    if tables.partitioned {
        partition::configure(&tables, &mut sql, &mut ddl);
        checks.push(key_value_check_named(&tables.schema, partition::CONTROL_TABLE));
    }
    sql.insert("ddl".to_owned(), ddl);
    sql.insert(
        "check".to_owned(),
        format!("SELECT {} AS compatible", checks.join(" AND ")),
    );
    // A target's keys share its prefix, which ends in ':'; the next character, ';', bounds the
    // range, so the key index serves it on every supported server version.
    let in_target = |column: &str, n: u8| format!("{column} >= ${n}::text AND {column} < left(${n}::text, -1) || ';'");
    // The plan takes a durable rejection's raw from the verified replay, so the stored raw stays on
    // the server.
    sql.insert(
        "read_rejections_for_plan".to_owned(),
        format!("SELECT key, jsonb_set(value, '{{raw}}', '\"\"')::text AS value FROM {rejections} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key FOR UPDATE"),
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

/// An erasure cleanup page: delete at most `$3` keys in `[$1, $2)` and return how many it deleted.
/// Ingestion and projection state both delete a subject's key range this way.
pub(crate) fn delete_range(table: &str) -> String {
    format!(
        "WITH gone AS (DELETE FROM {table} WHERE key IN (SELECT key FROM {table} WHERE key >= $1::text AND key < $2::text ORDER BY key LIMIT $3::text::bigint) RETURNING 1) SELECT count(*) FROM gone"
    )
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
 AND {omitted}
)"#,
        omitted = crate::schema::omitted_columns_check(&["key", "value"]),
    )
}
