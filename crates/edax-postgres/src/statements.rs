//! Every statement a Postgres store runs, byte for byte, rendered for one schema.
//!
//! Each parameter is declared `text` and cast in the statement, so any client binds strings and
//! `NULL`, and [`crate::rows`] owns how a value becomes that text. Every row a message read returns is
//! one JSON text column, which [`crate::rows`] decodes. Each statement opens with an `/* edax:<op> */`
//! tag, so `pg_stat_statements` groups it.
//!
//! The shapes that carry a decision:
//!
//! - `append` is a plain `INSERT`. Its version check reads the stream's last sequence in the same
//!   statement, and the unique index on `(stream, sequence)` catches the writer that raced past it.
//!   That violation aborts the transaction on purpose: an `ON CONFLICT DO NOTHING` would land the rest
//!   of a batch on top of the winner's event, and a caller that caught the conflict could commit it.
//! - Each write inserts its outbox rows in the order of its batch, so a relay reads them in sequence.
//! - `ledger_hold` locks the key's row (`FOR UPDATE`) and the settling write follows in the same
//!   transaction, so the fence and the write are one step. A store that runs outside a unit of work
//!   opens a transaction for the pair.
//! - The `*_leased` reads take or renew the outbox's relay lease in the same statement: a relay that
//!   does not hold it reads no rows. Rows are never leased one by one, which would let two relays send
//!   one aggregate's events out of order.

use serde::{Deserialize, Serialize};

use crate::schema::Tables;

/// Every statement, rendered for one schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statements {
    /// `$1` the tables to check ([`crate::schema::catalog_param`]): what the catalog holds for each, for
    /// [`crate::schema::check_catalog`].
    pub catalog: String,
    /// The server's encoding, version number and deadlock timeout in milliseconds.
    pub server: String,
    /// `$1..$3` the lock, statement and idle timeouts in milliseconds, for this transaction only.
    pub settings: String,
    /// `$1` stream, `$2` expected version, `$3` the drafts: `{"stored", "events"}`.
    pub append: String,
    /// `$1` stream, `$2` after: one event row each.
    pub load: String,
    /// `$1` stream: the snapshot row.
    pub load_snapshot: String,
    /// `$1` stream, `$2` type name, `$3` version, `$4` data: the stream's version.
    pub save_snapshot: String,
    /// `$1` limit: event rows, oldest first.
    pub event_unpublished: String,
    /// `$1` limit, `$2` lease name, `$3` owner, `$4` lease ms.
    pub event_unpublished_leased: String,
    /// `$1` the ids.
    pub event_mark_published: String,
    /// The waiting rows.
    pub event_unpublished_count: String,
    /// `$1` the commands.
    pub command_enqueue: String,
    /// `$1` limit: command rows, oldest first.
    pub command_unpublished: String,
    /// `$1` limit, `$2` lease name, `$3` owner, `$4` lease ms.
    pub command_unpublished_leased: String,
    /// `$1` the ids.
    pub command_mark_published: String,
    /// The waiting rows.
    pub command_unpublished_count: String,
    /// `$1` projection, `$2` id: version and document.
    pub projection_get: String,
    /// `projection_get`, holding the row until the transaction ends.
    pub projection_get_locked: String,
    /// `$1` projection, `$2` id, `$3` document: the new version, none when the row exists.
    pub projection_insert: String,
    /// `$1` projection, `$2` id, `$3` expected version, `$4` document: the new version, none on a mismatch.
    pub projection_update: String,
    /// `$1` key, `$2` owner, `$3` pending record: a row only when the key was free.
    pub claim_insert: String,
    /// `$1` key, `$2` claim TTL ms: the ledger row, `lapsed` set for a pending claim past its TTL.
    pub claim_read: String,
    /// `$1` key, `$2` new owner, `$3` pending record, `$4` the lapsed owner: a row when taken over.
    pub claim_take_over: String,
    /// `$1` key: the ledger row, locked until the transaction ends.
    pub ledger_hold: String,
    /// `$1` key, `$2` the settling owner, `$3` record.
    pub ledger_settle: String,
    /// `$1` key.
    pub ledger_delete: String,
    /// `$1` saga, `$2` id: the instance row.
    pub saga_load: String,
    /// `saga_load`, holding the row until the transaction ends.
    pub saga_load_locked: String,
    /// `$1` saga, `$2` id, `$3` state, `$4` folded, `$5` completed, `$6` commands.
    pub saga_insert: String,
    /// `$1` saga, `$2` id, `$3` expected version, `$4` state, `$5` folded, `$6` completed, `$7` commands.
    pub saga_update: String,
    /// `$1` saga, `$2` limit: command rows, oldest first.
    pub saga_unpublished: String,
    /// `$1` saga, `$2` limit, `$3` lease name, `$4` owner, `$5` lease ms.
    pub saga_unpublished_leased: String,
    /// `$1` saga, `$2` the ids.
    pub saga_mark_published: String,
    /// `$1` saga: the waiting rows.
    pub saga_unpublished_count: String,
}

/// What the catalog holds for each table `$1` names: its kind (`None` when it does not exist), its columns
/// and its unique indexes, as one JSON text column. Reads only the catalog.
const CATALOG: &str = "/* edax:catalog */ SELECT coalesce(json_agg(json_build_object('table', w.logical, 'kind', c.relkind, \
'columns', coalesce((SELECT json_agg(json_build_object('name', a.attname, 'type', format_type(a.atttypid, NULL), \
'not_null', a.attnotnull, 'default', a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '', 'collation', co.collname) \
ORDER BY a.attnum) FROM pg_attribute AS a LEFT JOIN pg_collation AS co ON co.oid = a.attcollation \
WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped), '[]'::json), \
'unique', coalesce((SELECT json_agg(json_build_object('name', ic.relname, \
'columns', (SELECT coalesce(json_agg(ka.attname ORDER BY k.n), '[]'::json) FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, n) \
JOIN pg_attribute AS ka ON ka.attrelid = c.oid AND ka.attnum = k.attnum WHERE k.n <= i.indnkeyatts), \
'collations', (SELECT coalesce(json_agg(coalesce(kc.collname, '') ORDER BY k.n), '[]'::json) \
FROM unnest(i.indcollation::oid[]) WITH ORDINALITY AS k(oid, n) LEFT JOIN pg_collation AS kc ON kc.oid = k.oid), \
'partial', i.indpred IS NOT NULL, 'expressions', i.indexprs IS NOT NULL, 'valid', i.indisvalid, \
'deferrable', EXISTS (SELECT 1 FROM pg_constraint AS pc WHERE pc.conindid = i.indexrelid AND pc.condeferrable))) \
FROM pg_index AS i JOIN pg_class AS ic ON ic.oid = i.indexrelid WHERE i.indrelid = c.oid AND i.indisunique), '[]'::json))), \
'[]'::json)::text FROM jsonb_to_recordset(($1::text)::jsonb) AS w(logical text, schema text, name text) \
LEFT JOIN pg_namespace AS n ON n.nspname = w.schema \
LEFT JOIN pg_class AS c ON c.relnamespace = n.oid AND c.relname = w.name AND c.relkind IN ('r', 'p')";

/// An event row as one JSON text column, from the events table alias `e`.
const EVENT_ROW: &str = "json_build_object('id', e.id, 'aggregate_id', e.aggregate_id, 'tenant', e.tenant, \
'sequence', e.sequence, 'name', e.name, 'version', e.version, 'data', encode(e.data, 'hex'), \
'correlation_id', e.correlation_id, 'causation_id', e.causation_id, 'occurred_at', e.occurred_at, \
'recorded_at', e.recorded_at, 'extensions', e.extensions, 'metadata', e.metadata)::text";

/// A command row as one JSON text column, from a command outbox alias `c`.
const COMMAND_ROW: &str = "json_build_object('id', c.id, 'idempotency_key', c.idempotency_key, \
'aggregate_id', c.aggregate_id, 'tenant', c.tenant, 'name', c.name, 'version', c.version, \
'data', encode(c.data, 'hex'), 'correlation_id', c.correlation_id, 'causation_id', c.causation_id, \
'issued_at', c.issued_at, 'extensions', c.extensions, 'metadata', c.metadata)::text";

/// The command rows of a JSON array parameter `$N`, in array order, as `r.n` and the record `c`.
const COMMAND_INPUT: &str = "jsonb_array_elements(($N::text)::jsonb) WITH ORDINALITY AS r(value, n) \
CROSS JOIN LATERAL jsonb_to_record(r.value) AS c(id text, idempotency_key text, aggregate_id text, \
tenant text, name text, version bigint, data text, correlation_id text, causation_id text, \
issued_at bigint, extensions text, metadata text)";

const COMMAND_COLUMNS: &str =
    "id, idempotency_key, aggregate_id, tenant, name, version, data, correlation_id, causation_id, issued_at, extensions, metadata";

const COMMAND_VALUES: &str = "c.id, c.idempotency_key, c.aggregate_id, c.tenant, c.name, c.version, \
decode(c.data, 'hex'), c.correlation_id, c.causation_id, c.issued_at, c.extensions, c.metadata";

/// Fresh holders and nonholders read without assigning a transaction ID. Only acquisition,
/// expiration and renewal below half the lease duration reach the write and its conflict fence.
const LEASE: &str = "lease_clock AS MATERIALIZED (SELECT clock_timestamp() AS now), \
held AS MATERIALIZED (SELECT owner, expires_at FROM {relay_leases} WHERE outbox = $NAME::text), \
renewed AS (INSERT INTO {relay_leases} AS l (outbox, owner, expires_at) \
SELECT $NAME::text, $OWNER::text, now + ($MS::text)::bigint * interval '1 millisecond' FROM lease_clock \
WHERE NOT EXISTS (SELECT 1 FROM held) OR EXISTS (SELECT 1 FROM held WHERE expires_at <= now \
OR (owner = $OWNER::text AND expires_at < now + ($MS::text)::bigint * interval '0.5 millisecond')) \
ON CONFLICT (outbox) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at \
WHERE l.owner = EXCLUDED.owner OR l.expires_at <= clock_timestamp() RETURNING l.owner), \
lease AS (SELECT owner FROM renewed UNION ALL SELECT owner FROM held CROSS JOIN lease_clock \
WHERE owner = $OWNER::text AND expires_at >= now + ($MS::text)::bigint * interval '0.5 millisecond')";

/// A ledger row as one JSON text column, from the ledger table alias `l`; `$TTL` is the claim TTL.
const LEDGER_ROW: &str = "json_build_object('owner', l.owner, 'settled_by', l.settled_by, 'record', l.record, \
'lapsed', l.owner IS NOT NULL AND l.claimed_at < clock_timestamp() - ($TTL::text)::bigint * interval '1 millisecond')::text";

/// A saga instance as one JSON text column, from the saga instances table alias `i`.
const SAGA_ROW: &str =
    "json_build_object('version', i.version, 'state', i.state, 'folded', i.folded, 'completed', i.completed)::text";

/// What a saga write returns: the version it wrote, or the instance it found instead.
const SAGA_RESULT: &str = "SELECT json_build_object('written', (SELECT version FROM written), \
'stored', (SELECT i.version FROM {saga_instances} AS i WHERE i.saga = $1::text AND i.id = $2::text), \
'completed', (SELECT i.completed FROM {saga_instances} AS i WHERE i.saga = $1::text AND i.id = $2::text))::text";

fn command_input(param: usize) -> String {
    COMMAND_INPUT.replace("$N", &format!("${param}"))
}

fn lease(name: usize, owner: usize, ms: usize) -> String {
    LEASE
        .replace("$NAME", &format!("${name}"))
        .replace("$OWNER", &format!("${owner}"))
        .replace("$MS", &format!("${ms}"))
}

fn saga_sent(commands: usize) -> String {
    format!(
        "sent AS (INSERT INTO {{saga_outbox}} (saga, {COMMAND_COLUMNS}) SELECT $1::text, {COMMAND_VALUES} \
FROM written CROSS JOIN {} ORDER BY r.n)",
        command_input(commands)
    )
}

/// Every statement, rendered for the tables `tables` names.
pub fn statements(tables: &Tables) -> Statements {
    let r = |template: &str| tables.render(template);
    let event_unpublished = format!(
        "SELECT {EVENT_ROW} FROM {{event_outbox}} AS o JOIN {{events}} AS e ON e.id = o.event_id \
WHERE EXISTS_LEASE ORDER BY o.position LIMIT ($1::text)::bigint"
    );
    let command_unpublished = format!(
        "SELECT {COMMAND_ROW} FROM {{command_outbox}} AS c WHERE EXISTS_LEASE ORDER BY c.position LIMIT ($1::text)::bigint"
    );
    let saga_unpublished = format!(
        "SELECT {COMMAND_ROW} FROM {{saga_outbox}} AS c WHERE c.saga = $1::text AND EXISTS_LEASE \
ORDER BY c.position LIMIT ($2::text)::bigint"
    );
    let unleased = |tag: &str, select: &str| r(&format!("/* edax:{tag} */ {}", select.replace("EXISTS_LEASE", "true")));
    let leased = |tag: &str, select: &str, lease_cte: String| {
        r(&format!(
            "/* edax:{tag} */ WITH {lease_cte} {}",
            select.replace("EXISTS_LEASE", "EXISTS (SELECT 1 FROM lease)")
        ))
    };
    Statements {
        catalog: CATALOG.to_owned(),
        server: r("/* edax:server */ SELECT current_setting('server_encoding'), current_setting('server_version_num')::bigint, \
(extract(epoch FROM current_setting('deadlock_timeout')::interval) * 1000)::bigint"),
        settings: "/* edax:settings */ SELECT set_config('lock_timeout', $1::text, true), \
set_config('statement_timeout', $2::text, true), set_config('idle_in_transaction_session_timeout', $3::text, true)"
            .to_owned(),
        append: r(&format!(
            "/* edax:append */ WITH clock AS (SELECT floor(extract(epoch FROM clock_timestamp()) * 1000000)::bigint * 1000 AS recorded_at), \
current AS (SELECT coalesce(max(sequence), 0) AS version FROM {{events}} WHERE stream = $1::text), \
inserted AS (INSERT INTO {{events}} (id, stream, sequence, aggregate_id, tenant, name, version, data, \
correlation_id, causation_id, occurred_at, recorded_at, extensions, metadata) \
SELECT d.id, $1::text, d.sequence, d.aggregate_id, d.tenant, d.name, d.version, decode(d.data, 'hex'), \
d.correlation_id, d.causation_id, d.occurred_at, clock.recorded_at, d.extensions, d.metadata \
FROM jsonb_array_elements(($3::text)::jsonb) WITH ORDINALITY AS r(value, n) \
CROSS JOIN LATERAL jsonb_to_record(r.value) AS d(id text, aggregate_id text, tenant text, sequence bigint, \
name text, version bigint, data text, correlation_id text, causation_id text, occurred_at bigint, \
extensions text, metadata text) CROSS JOIN clock CROSS JOIN current \
WHERE current.version = ($2::text)::bigint ORDER BY r.n RETURNING *), \
outbox AS (INSERT INTO {{event_outbox}} (event_id) SELECT id FROM inserted ORDER BY sequence) \
SELECT json_build_object('stored', (SELECT version FROM current), \
'events', coalesce((SELECT json_agg({} ORDER BY e.sequence) FROM inserted AS e), '[]'::json))::text",
            EVENT_ROW.trim_end_matches("::text")
        )),
        load: r(&format!(
            "/* edax:load */ SELECT {EVENT_ROW} FROM {{events}} AS e WHERE e.stream = $1::text \
AND e.sequence > ($2::text)::bigint ORDER BY e.sequence"
        )),
        load_snapshot: r("/* edax:load_snapshot */ SELECT json_build_object('type_name', s.type_name, 'version', s.version, \
'data', encode(s.data, 'hex'))::text FROM {snapshots} AS s WHERE s.stream = $1::text"),
        save_snapshot: r("/* edax:save_snapshot */ WITH current AS (SELECT coalesce(max(sequence), 0) AS version \
FROM {events} WHERE stream = $1::text), saved AS (INSERT INTO {snapshots} AS s (stream, type_name, version, data) \
SELECT $1::text, $2::text, ($3::text)::bigint, decode($4::text, 'hex') FROM current WHERE ($3::text)::bigint <= current.version \
ON CONFLICT (stream) DO UPDATE SET type_name = EXCLUDED.type_name, version = EXCLUDED.version, \
data = EXCLUDED.data WHERE s.version <= EXCLUDED.version) SELECT version FROM current"),
        event_unpublished_leased: leased("event_unpublished", &event_unpublished, lease(2, 3, 4)),
        event_unpublished: unleased("event_unpublished", &event_unpublished),
        event_mark_published: r("/* edax:event_mark_published */ DELETE FROM {event_outbox} \
WHERE event_id IN (SELECT jsonb_array_elements_text(($1::text)::jsonb))"),
        event_unpublished_count: r("/* edax:event_unpublished_count */ SELECT count(*) FROM {event_outbox}"),
        command_enqueue: r(&format!(
            "/* edax:command_enqueue */ INSERT INTO {{command_outbox}} ({COMMAND_COLUMNS}) SELECT {COMMAND_VALUES} \
FROM {} ORDER BY r.n",
            command_input(1)
        )),
        command_unpublished_leased: leased("command_unpublished", &command_unpublished, lease(2, 3, 4)),
        command_unpublished: unleased("command_unpublished", &command_unpublished),
        command_mark_published: r("/* edax:command_mark_published */ DELETE FROM {command_outbox} \
WHERE id IN (SELECT jsonb_array_elements_text(($1::text)::jsonb))"),
        command_unpublished_count: r("/* edax:command_unpublished_count */ SELECT count(*) FROM {command_outbox}"),
        projection_get: r("/* edax:projection_get */ SELECT p.version, p.document::text FROM {projections} AS p \
WHERE p.projection = $1::text AND p.id = $2::text"),
        projection_get_locked: r("/* edax:projection_get */ SELECT p.version, p.document::text FROM {projections} AS p \
WHERE p.projection = $1::text AND p.id = $2::text FOR NO KEY UPDATE"),
        projection_insert: r("/* edax:projection_insert */ INSERT INTO {projections} (projection, id, version, document) \
VALUES ($1::text, $2::text, 1, ($3::text)::jsonb) ON CONFLICT (projection, id) DO NOTHING RETURNING version"),
        projection_update: r("/* edax:projection_update */ UPDATE {projections} SET version = version + 1, \
document = ($4::text)::jsonb WHERE projection = $1::text AND id = $2::text AND version = ($3::text)::bigint RETURNING version"),
        claim_insert: r("/* edax:claim_insert */ INSERT INTO {ledger} (key, owner, record, claimed_at) \
VALUES ($1::text, $2::text, $3::text, clock_timestamp()) ON CONFLICT (key) DO NOTHING RETURNING true"),
        claim_read: r(&format!(
            "/* edax:claim_read */ SELECT {} FROM {{ledger}} AS l WHERE l.key = $1::text",
            LEDGER_ROW.replace("$TTL", "$2")
        )),
        claim_take_over: r("/* edax:claim_take_over */ UPDATE {ledger} SET owner = $2::text, record = $3::text, \
claimed_at = clock_timestamp() WHERE key = $1::text AND owner = $4::text RETURNING true"),
        ledger_hold: r(&format!(
            "/* edax:ledger_hold */ SELECT {} FROM {{ledger}} AS l WHERE l.key = $1::text FOR UPDATE",
            LEDGER_ROW.replace("($TTL::text)::bigint", "0")
        )),
        ledger_settle: r("/* edax:ledger_settle */ UPDATE {ledger} SET owner = NULL, settled_by = $2::text, \
record = $3::text, settled_at = clock_timestamp() WHERE key = $1::text"),
        ledger_delete: r("/* edax:ledger_delete */ DELETE FROM {ledger} WHERE key = $1::text"),
        saga_load: r(&format!(
            "/* edax:saga_load */ SELECT {SAGA_ROW} FROM {{saga_instances}} AS i WHERE i.saga = $1::text AND i.id = $2::text"
        )),
        saga_load_locked: r(&format!(
            "/* edax:saga_load */ SELECT {SAGA_ROW} FROM {{saga_instances}} AS i WHERE i.saga = $1::text AND i.id = $2::text \
FOR NO KEY UPDATE"
        )),
        saga_insert: r(&format!(
            "/* edax:saga_insert */ WITH written AS (INSERT INTO {{saga_instances}} (saga, id, version, state, folded, completed, completed_at) \
VALUES ($1::text, $2::text, 1, $3::text, $4::text, ($5::text)::boolean, CASE WHEN ($5::text)::boolean THEN clock_timestamp() END) \
ON CONFLICT (saga, id) DO NOTHING RETURNING version), {} {SAGA_RESULT}",
            saga_sent(6)
        )),
        saga_update: r(&format!(
            "/* edax:saga_update */ WITH written AS (UPDATE {{saga_instances}} SET version = version + 1, state = $4::text, \
folded = $5::text, completed = ($6::text)::boolean, completed_at = CASE WHEN ($6::text)::boolean THEN clock_timestamp() END \
WHERE saga = $1::text AND id = $2::text AND version = ($3::text)::bigint AND NOT completed RETURNING version), {} {SAGA_RESULT}",
            saga_sent(7)
        )),
        saga_unpublished_leased: leased("saga_unpublished", &saga_unpublished, lease(3, 4, 5)),
        saga_unpublished: unleased("saga_unpublished", &saga_unpublished),
        saga_mark_published: r("/* edax:saga_mark_published */ DELETE FROM {saga_outbox} WHERE saga = $1::text \
AND id IN (SELECT jsonb_array_elements_text(($2::text)::jsonb))"),
        saga_unpublished_count: r("/* edax:saga_unpublished_count */ SELECT count(*) FROM {saga_outbox} WHERE saga = $1::text"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_statement_is_rendered_and_tagged() {
        let rendered = statements(&Tables::default());
        let json = serde_json::to_value(&rendered).unwrap();
        for (name, text) in json.as_object().unwrap() {
            let text = text.as_str().unwrap();
            assert!(
                !text.contains("{events}") && !text.contains("{ledger}"),
                "{name} is rendered"
            );
            assert!(
                !text.contains("$N") && !text.contains("$TTL") && !text.contains("EXISTS_LEASE"),
                "{name}"
            );
            assert!(text.starts_with("/* edax:"), "{name} is tagged");
        }
        assert!(rendered
            .event_unpublished_leased
            .contains("EXISTS (SELECT 1 FROM lease)"));
        assert!(rendered.event_unpublished.contains("WHERE true"));
    }
}
