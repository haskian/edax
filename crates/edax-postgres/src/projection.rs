//! SQL for projection state. Two host-provisioned tables: `table` holds manifests, checkpoints,
//! states and receipts as key/value rows with no secondary index, so every state update can stay a
//! heap-only (HOT) update; `intents` holds effect intents, inserted once and deleted on delivery.
//! A transaction takes the locks `edax_cqrs::projection::lock` names before it reads anything.
use crate::schema::identifier;
use edax_core::Error;
use serde::Deserialize;
use std::collections::BTreeMap;

/// The statements, by name, for one configured pair of tables, and the reference DDL as `ddl`.
pub fn statements(config_json: &str) -> Result<BTreeMap<String, String>, Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        schema: String,
        table: String,
        intents: String,
    }
    let value: serde_json::Value = serde_json::from_str(config_json).map_err(Error::codec)?;
    let config: Config = serde_json::from_value(value).map_err(Error::codec)?;
    let schema = identifier("schema", &config.schema)?;
    let table_name = identifier("table name", &config.table)?;
    let intents_name = identifier("table name", &config.intents)?;
    if table_name == intents_name {
        return Err(Error::bad_handle("projection state and intents need two tables"));
    }
    let table = format!("\"{schema}\".\"{table_name}\"");
    let intents = format!("\"{schema}\".\"{intents_name}\"");
    // The generation lock is shared by partition work and taken alone by generation-wide changes.
    let generation_lock = "pg_catalog.hashtextextended('edax_projection/v1:' || $1::text, 0)";
    // Eligibility against the manifest's immutable cutoffs, the same rule as the kernel's `eligible`.
    let eligible = "(m.value->>'active')::boolean \
        AND i.source_offset >= (m.value->'cutoffs'->>(i.source_partition::text))::bigint \
        AND (NOT (m.value->>'frozen')::boolean OR i.source_offset < (m.value->'upper'->>(i.source_partition::text))::bigint)";
    Ok(BTreeMap::from([
        ("ddl".into(), format!(
            "CREATE TABLE {table} (key text COLLATE \"C\" PRIMARY KEY, value jsonb NOT NULL) WITH (fillfactor = 70);\n\
             CREATE TABLE {intents} (key text COLLATE \"C\" PRIMARY KEY, generation text NOT NULL, source_partition integer NOT NULL, source_offset bigint NOT NULL, ordinal integer NOT NULL, value jsonb NOT NULL);\n\
             CREATE INDEX ON {intents} (generation, source_partition, source_offset, ordinal);"
        )),
        ("check".into(), format!(
            "SELECT {} AND {} AS compatible",
            crate::ingest::key_value_check_named(&schema, &table_name),
            intents_check(&schema, &intents_name),
        )),
        ("lock_exclusive".into(), format!("SELECT pg_catalog.pg_advisory_xact_lock({generation_lock})")),
        ("lock_shared".into(), format!("SELECT pg_catalog.pg_advisory_xact_lock_shared({generation_lock})")),
        ("lock_partition".into(), "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('edax_projection/v1:' || $1::text || ':' || $2::integer::text, 0))".into()),
        ("clock".into(), "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint".into()),
        ("read".into(), format!("SELECT key, value::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key")),
        // No ON CONFLICT: a concurrent insert of the same key fails the transaction, which retries.
        ("insert".into(), format!("INSERT INTO {table} (key, value) SELECT key, value FROM jsonb_each($1::jsonb) ORDER BY key COLLATE \"C\"")),
        ("update".into(), format!("WITH changed AS (UPDATE {table} AS t SET value = u.value FROM jsonb_each($1::jsonb) AS u(key, value) WHERE t.key = u.key RETURNING 1) SELECT count(*) FROM changed")),
        ("insert_intents".into(), format!(
            "INSERT INTO {intents} (key, generation, source_partition, source_offset, ordinal, value) \
             SELECT key, value->>'generation', (value->>'partition')::integer, (value->>'offset')::bigint, (value->>'ordinal')::integer, value \
             FROM jsonb_each($1::jsonb) ORDER BY key COLLATE \"C\""
        )),
        // The manifest comes first: an inactive generation reads no intent. Each partition reads only
        // its eligible range through the index, so suppressed history is never scanned. Offsets of two
        // partitions are not comparable, so a page takes each partition's oldest in turn by rank.
        ("pending".into(), format!(
            "SELECT i.key, i.value::text AS value FROM {table} AS m \
             CROSS JOIN LATERAL jsonb_each_text(m.value->'cutoffs') AS c(part, cutoff) \
             CROSS JOIN LATERAL (SELECT key, value, source_partition, row_number() OVER (ORDER BY source_offset, ordinal) AS rank \
               FROM (SELECT key, value, source_partition, source_offset, ordinal FROM {intents} AS i \
               WHERE i.generation = $2 AND i.source_partition = c.part::integer AND i.source_offset >= c.cutoff::bigint \
               AND (NOT (m.value->>'frozen')::boolean OR i.source_offset < (m.value->'upper'->>c.part)::bigint) \
               ORDER BY i.source_offset, i.ordinal LIMIT $3::text::bigint) AS page) AS i \
             WHERE m.key = $1 AND (m.value->>'active')::boolean \
             ORDER BY i.rank, i.source_partition LIMIT $3::text::bigint"
        )),
        // Delivery deletes the intent. An unknown or ineligible one is a no-op.
        ("mark_published".into(), format!(
            "DELETE FROM {intents} AS i USING {table} AS m \
             WHERE i.key IN (SELECT jsonb_array_elements_text($1::jsonb)) AND m.key = $2 AND i.generation = $3 AND {eligible}"
        )),
        // Intents a cutover made permanently ineligible, removed a page at a time.
        ("prune".into(), format!(
            "WITH gone AS (DELETE FROM {intents} WHERE ctid IN (SELECT i.ctid FROM {intents} AS i JOIN {table} AS m ON m.key = $1 \
             WHERE i.generation = $2 AND (m.value->>'active')::boolean AND NOT ({eligible}) LIMIT $3::text::bigint) RETURNING 1) \
             SELECT count(*) FROM gone"
        )),
    ]))
}

fn intents_check(schema: &str, name: &str) -> String {
    format!(
        r#"EXISTS (
 SELECT 1 FROM pg_catalog.pg_class t
 JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
 JOIN pg_catalog.pg_attribute k ON k.attrelid = t.oid AND k.attname = 'key' AND NOT k.attisdropped
 WHERE n.nspname = '{schema}' AND t.relname = '{name}' AND t.relkind IN ('r', 'p')
 AND NOT t.relrowsecurity AND NOT t.relforcerowsecurity
 AND (SELECT count(*) FROM pg_catalog.pg_attribute a WHERE a.attrelid = t.oid AND NOT a.attisdropped AND a.attnotnull AND (a.attname, a.atttypid) IN (
   ('key', 'pg_catalog.text'::regtype), ('generation', 'pg_catalog.text'::regtype),
   ('source_partition', 'pg_catalog.int4'::regtype), ('source_offset', 'pg_catalog.int8'::regtype),
   ('ordinal', 'pg_catalog.int4'::regtype), ('value', 'pg_catalog.jsonb'::regtype))) = 6
 AND EXISTS (
  SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_collation ic ON ic.oid = i.indcollation[0]
  WHERE i.indrelid = t.oid AND i.indisunique AND i.indisvalid AND i.indimmediate AND i.indpred IS NULL
  AND i.indexprs IS NULL AND i.indnkeyatts = 1 AND i.indkey[0] = k.attnum
  AND ic.collname IN ('C', 'POSIX') AND ic.collnamespace = 'pg_catalog'::regnamespace
 )
 AND EXISTS (
  SELECT 1 FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_attribute g ON g.attrelid = t.oid AND g.attnum = i.indkey[0] AND g.attname = 'generation'
  JOIN pg_catalog.pg_attribute p ON p.attrelid = t.oid AND p.attnum = i.indkey[1] AND p.attname = 'source_partition'
  JOIN pg_catalog.pg_attribute o ON o.attrelid = t.oid AND o.attnum = i.indkey[2] AND o.attname = 'source_offset'
  WHERE i.indrelid = t.oid AND i.indisvalid AND i.indpred IS NULL
 )
)"#
    )
}
