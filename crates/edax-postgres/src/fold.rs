//! SQL for fold records. All mutations run under the same generation advisory lock.
use crate::schema::{Logical, Tables};
use edax_core::Error;
use serde::Deserialize;
use std::collections::BTreeMap;

/// A single optional host-provisioned table keeps restore boundaries explicit.
pub fn statements(config_json: &str) -> Result<BTreeMap<String, String>, Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        schema: String,
        table: String,
    }
    let value: serde_json::Value = serde_json::from_str(config_json).map_err(Error::codec)?;
    let config: Config = serde_json::from_value(value).map_err(Error::codec)?;
    let tables = Tables::new(&config.schema, BTreeMap::from([(Logical::Events, config.table)]))?;
    let table = tables.qualified(Logical::Events);
    Ok(BTreeMap::from([
        ("ddl".into(), format!("CREATE TABLE {table} (key text COLLATE \"C\" PRIMARY KEY, value jsonb NOT NULL);\nCREATE INDEX ON {table} (key) WHERE value->>'kind' = 'intent' AND value->>'published' = 'false' AND value->>'suppressed' = 'false';")),
        ("check".into(), format!("SELECT {} AS compatible", crate::ingest::key_value_check(&tables, Logical::Events))),
        ("lock".into(), "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('edax_fold/v1:' || $1::text, 0))".into()),
        ("clock".into(), "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint".into()),
        ("read".into(), format!("SELECT key, value::text AS value FROM {table} WHERE key IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY key")),
        ("write".into(), format!("INSERT INTO {table} (key, value) SELECT key, value FROM jsonb_each($1::jsonb) ORDER BY key COLLATE \"C\" ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")),
        ("suppress".into(), format!("UPDATE {table} SET value = jsonb_set(value, '{{suppressed}}', 'true'::jsonb) WHERE starts_with(key, $1::text) AND value->>'kind' = 'intent' AND value->>'suppressed' = 'false' AND (value->>'offset')::bigint < ($2::jsonb->>(value->>'partition'))::bigint")),
        ("pending".into(), format!("SELECT intent.key, intent.value::text AS value FROM {table} AS intent JOIN {table} AS manifest ON manifest.key = $1::text WHERE starts_with(intent.key, $2::text) AND intent.value->>'kind' = 'intent' AND intent.value->>'published' = 'false' AND intent.value->>'suppressed' = 'false' AND (manifest.value->>'active')::boolean AND (intent.value->>'offset')::bigint >= (manifest.value->'cutoffs'->>(intent.value->>'partition'))::bigint AND (NOT (manifest.value->>'frozen')::boolean OR (intent.value->>'offset')::bigint < (manifest.value->'upper'->>(intent.value->>'partition'))::bigint) ORDER BY intent.key LIMIT $3::text::bigint")),
    ]))
}
