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
//!   one aggregate's events out of order. The lease is named for the schema-qualified outbox table (and
//!   the saga, for the saga outbox), so stores whose outboxes differ never share one, and no SDK names it.

use serde::{Deserialize, Serialize};

use crate::schema::{table_contract, ColumnType, Logical, Tables};

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
    /// `$1` limit, `$2` owner, `$3` lease ms.
    pub event_unpublished_leased: String,
    /// `$1` the ids.
    pub event_mark_published: String,
    /// The waiting rows.
    pub event_unpublished_count: String,
    /// `$1` the commands.
    pub command_enqueue: String,
    /// `$1` limit: command rows, oldest first.
    pub command_unpublished: String,
    /// `$1` limit, `$2` owner, `$3` lease ms.
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
    /// `$1` saga, `$2` limit, `$3` owner, `$4` lease ms.
    pub saga_unpublished_leased: String,
    /// `$1` saga, `$2` the ids.
    pub saga_mark_published: String,
    /// `$1` saga: the waiting rows.
    pub saga_unpublished_count: String,
}

/// What the catalog holds for each table `$1` names: its kind (`None` when it does not exist), its columns
/// and its unique indexes, as one JSON text column. Reads only the catalog.
const CATALOG: &str = r"/* edax:catalog */
SELECT coalesce(
  json_agg(
    json_build_object(
      'table', w.logical,
      'kind', c.relkind,
      'columns', coalesce(
        (
          SELECT json_agg(
            json_build_object(
              'name', a.attname,
              'type', format_type(a.atttypid, NULL),
              'not_null', a.attnotnull,
              'default', a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '',
              'collation', co.collname
            )
            ORDER BY a.attnum
          )
          FROM pg_attribute AS a
          LEFT JOIN pg_collation AS co ON co.oid = a.attcollation
          WHERE a.attrelid = c.oid
            AND a.attnum > 0
            AND NOT a.attisdropped
        ),
        '[]'::json
      ),
      'unique', coalesce(
        (
          SELECT json_agg(
            json_build_object(
              'name', ic.relname,
              'columns', (
                SELECT coalesce(json_agg(ka.attname ORDER BY k.n), '[]'::json)
                FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, n)
                JOIN pg_attribute AS ka ON ka.attrelid = c.oid AND ka.attnum = k.attnum
                WHERE k.n <= i.indnkeyatts
              ),
              'collations', (
                SELECT coalesce(json_agg(coalesce(kc.collname, '') ORDER BY k.n), '[]'::json)
                FROM unnest(i.indcollation::oid[]) WITH ORDINALITY AS k(oid, n)
                LEFT JOIN pg_collation AS kc ON kc.oid = k.oid
              ),
              'partial', i.indpred IS NOT NULL,
              'expressions', i.indexprs IS NOT NULL,
              'valid', i.indisvalid,
              'deferrable', EXISTS (
                SELECT 1
                FROM pg_constraint AS pc
                WHERE pc.conindid = i.indexrelid
                  AND pc.condeferrable
              )
            )
          )
          FROM pg_index AS i
          JOIN pg_class AS ic ON ic.oid = i.indexrelid
          WHERE i.indrelid = c.oid
            AND i.indisunique
        ),
        '[]'::json
      )
    )
  ),
  '[]'::json
)::text
FROM jsonb_to_recordset(($1::text)::jsonb) AS w(logical text, schema text, name text)
LEFT JOIN pg_namespace AS n ON n.nspname = w.schema
LEFT JOIN pg_class AS c ON c.relnamespace = n.oid AND c.relname = w.name AND c.relkind IN ('r', 'p')";

const SERVER: &str = r"/* edax:server */
SELECT current_setting('server_encoding'),
       current_setting('server_version_num')::bigint,
       (extract(epoch FROM current_setting('deadlock_timeout')::interval) * 1000)::bigint";

const SETTINGS: &str = r"/* edax:settings */
SELECT set_config('lock_timeout', $1::text, true),
       set_config('statement_timeout', $2::text, true),
       set_config('idle_in_transaction_session_timeout', $3::text, true)";

/// An event row as one JSON object, from the events table alias `e`.
const EVENT_JSON: &str = r"json_build_object(
  'id', e.id,
  'aggregate_id', e.aggregate_id,
  'tenant', e.tenant,
  'sequence', e.sequence,
  'name', e.name,
  'version', e.version,
  'data', encode(e.data, 'hex'),
  'correlation_id', e.correlation_id,
  'causation_id', e.causation_id,
  'occurred_at', e.occurred_at,
  'recorded_at', e.recorded_at,
  'extensions', e.extensions,
  'metadata', e.metadata
)";

const APPEND: &str = r"/* edax:append */
WITH clock AS (
  SELECT floor(extract(epoch FROM clock_timestamp()) * 1000000)::bigint * 1000 AS recorded_at
),
current AS (
  SELECT coalesce(max(sequence), 0) AS version
  FROM {events}
  WHERE stream = $1::text
),
inserted AS (
  INSERT INTO {events} (
    id, stream, sequence, aggregate_id, tenant, name, version, data,
    correlation_id, causation_id, occurred_at, recorded_at, extensions, metadata
  )
  SELECT
    d.id, $1::text, d.sequence, d.aggregate_id, d.tenant, d.name, d.version, decode(d.data, 'hex'),
    d.correlation_id, d.causation_id, d.occurred_at, clock.recorded_at, d.extensions, d.metadata
  FROM jsonb_array_elements(($3::text)::jsonb) WITH ORDINALITY AS r(value, n)
  CROSS JOIN LATERAL jsonb_to_record(r.value) AS d(
    id text, aggregate_id text, tenant text, sequence bigint, name text, version bigint, data text,
    correlation_id text, causation_id text, occurred_at bigint, extensions text, metadata text
  )
  CROSS JOIN clock
  CROSS JOIN current
  WHERE current.version = ($2::text)::bigint
  ORDER BY r.n
  RETURNING *
),
outbox AS (
  INSERT INTO {event_outbox} (event_id)
  SELECT id
  FROM inserted
  ORDER BY sequence
)
SELECT json_build_object(
  'stored', (SELECT version FROM current),
  'events', coalesce(
    (
      SELECT json_agg(
        {event_json}
        ORDER BY e.sequence
      )
      FROM inserted AS e
    ),
    '[]'::json
  )
)::text";

const LOAD: &str = r"/* edax:load */
SELECT {event_json}::text
FROM {events} AS e
WHERE e.stream = $1::text
  AND e.sequence > ($2::text)::bigint
ORDER BY e.sequence";

const LOAD_SNAPSHOT: &str = r"/* edax:load_snapshot */
SELECT json_build_object('type_name', s.type_name, 'version', s.version, 'data', encode(s.data, 'hex'))::text
FROM {snapshots} AS s
WHERE s.stream = $1::text";

const SAVE_SNAPSHOT: &str = r"/* edax:save_snapshot */
WITH current AS (
  SELECT coalesce(max(sequence), 0) AS version
  FROM {events}
  WHERE stream = $1::text
),
saved AS (
  INSERT INTO {snapshots} AS s (stream, type_name, version, data)
  SELECT $1::text, $2::text, ($3::text)::bigint, decode($4::text, 'hex')
  FROM current
  WHERE ($3::text)::bigint <= current.version
  ON CONFLICT (stream) DO UPDATE
  SET type_name = EXCLUDED.type_name,
      version = EXCLUDED.version,
      data = EXCLUDED.data
  WHERE s.version <= EXCLUDED.version
)
SELECT version
FROM current";

/// The relay lease `{lease_name}` for the relay `{owner}`, held for `{ms}` milliseconds: the CTEs that
/// take or renew it, ending in `lease`, one row while `{owner}` holds it. Fresh holders and nonholders read
/// without assigning a transaction ID. Only acquisition, expiration and renewal below half the lease
/// duration reach the write and its conflict fence.
const LEASE: &str = r"lease_clock AS MATERIALIZED (
  SELECT clock_timestamp() AS now
),
held AS MATERIALIZED (
  SELECT owner, expires_at
  FROM {relay_leases}
  WHERE outbox = {lease_name}
),
renewed AS (
  INSERT INTO {relay_leases} AS l (outbox, owner, expires_at)
  SELECT {lease_name}, {owner}::text, now + ({ms}::text)::bigint * interval '1 millisecond'
  FROM lease_clock
  WHERE NOT EXISTS (SELECT 1 FROM held)
     OR EXISTS (
       SELECT 1
       FROM held
       WHERE expires_at <= now
          OR (owner = {owner}::text AND expires_at < now + ({ms}::text)::bigint * interval '0.5 millisecond')
     )
  ON CONFLICT (outbox) DO UPDATE
  SET owner = EXCLUDED.owner,
      expires_at = EXCLUDED.expires_at
  WHERE l.owner = EXCLUDED.owner
     OR l.expires_at <= clock_timestamp()
  RETURNING l.owner
),
lease AS (
  SELECT owner
  FROM renewed
  UNION ALL
  SELECT owner
  FROM held
  CROSS JOIN lease_clock
  WHERE owner = {owner}::text
    AND expires_at >= now + ({ms}::text)::bigint * interval '0.5 millisecond'
)";

const EVENT_UNPUBLISHED: OutboxRead = OutboxRead {
    tag: "event_unpublished",
    role: Logical::EventOutbox,
    select: r"SELECT {event_json}::text
FROM {event_outbox} AS o
JOIN {events} AS e ON e.id = o.event_id",
    scope: &[],
    lease_scope: None,
    position: "o.position",
    limit: 1,
};

const EVENT_MARK_PUBLISHED: &str = r"/* edax:event_mark_published */
DELETE FROM {event_outbox}
WHERE event_id IN (SELECT jsonb_array_elements_text(($1::text)::jsonb))";

const EVENT_UNPUBLISHED_COUNT: &str = r"/* edax:event_unpublished_count */
SELECT count(*)
FROM {event_outbox}";

const COMMAND_ENQUEUE: &str = r"/* edax:command_enqueue */
INSERT INTO {command_outbox} (
  {command_columns}
)
SELECT
  {command_values}
FROM {command_input}
ORDER BY r.n";

const COMMAND_UNPUBLISHED: OutboxRead = OutboxRead {
    tag: "command_unpublished",
    role: Logical::CommandOutbox,
    select: r"SELECT {command_json}::text
FROM {command_outbox} AS c",
    scope: &[],
    lease_scope: None,
    position: "c.position",
    limit: 1,
};

const COMMAND_MARK_PUBLISHED: &str = r"/* edax:command_mark_published */
DELETE FROM {command_outbox}
WHERE id IN (SELECT jsonb_array_elements_text(($1::text)::jsonb))";

const COMMAND_UNPUBLISHED_COUNT: &str = r"/* edax:command_unpublished_count */
SELECT count(*)
FROM {command_outbox}";

const PROJECTION_GET: &str = r"/* edax:projection_get */
SELECT p.version, p.document::text
FROM {projections} AS p
WHERE p.projection = $1::text
  AND p.id = $2::text";

const PROJECTION_INSERT: &str = r"/* edax:projection_insert */
INSERT INTO {projections} (projection, id, version, document)
VALUES ($1::text, $2::text, 1, ($3::text)::jsonb)
ON CONFLICT (projection, id) DO NOTHING
RETURNING version";

const PROJECTION_UPDATE: &str = r"/* edax:projection_update */
UPDATE {projections}
SET version = version + 1,
    document = ($4::text)::jsonb
WHERE projection = $1::text
  AND id = $2::text
  AND version = ($3::text)::bigint
RETURNING version";

const CLAIM_INSERT: &str = r"/* edax:claim_insert */
INSERT INTO {ledger} (key, owner, record, claimed_at)
VALUES ($1::text, $2::text, $3::text, clock_timestamp())
ON CONFLICT (key) DO NOTHING
RETURNING true";

/// The fields of a ledger row, from the ledger table alias `l`.
const LEDGER_FIELDS: &str = r"'owner', l.owner,
'settled_by', l.settled_by,
'record', l.record";

const CLAIM_READ: &str = r"/* edax:claim_read */
SELECT json_build_object(
  {ledger_fields},
  'lapsed', l.owner IS NOT NULL AND l.claimed_at < clock_timestamp() - ($2::text)::bigint * interval '1 millisecond'
)::text
FROM {ledger} AS l
WHERE l.key = $1::text";

const CLAIM_TAKE_OVER: &str = r"/* edax:claim_take_over */
UPDATE {ledger}
SET owner = $2::text,
    record = $3::text,
    claimed_at = clock_timestamp()
WHERE key = $1::text
  AND owner = $4::text
RETURNING true";

const LEDGER_HOLD: &str = r"/* edax:ledger_hold */
SELECT json_build_object(
  {ledger_fields}
)::text
FROM {ledger} AS l
WHERE l.key = $1::text
FOR UPDATE";

const LEDGER_SETTLE: &str = r"/* edax:ledger_settle */
UPDATE {ledger}
SET owner = NULL,
    settled_by = $2::text,
    record = $3::text,
    settled_at = clock_timestamp()
WHERE key = $1::text";

const LEDGER_DELETE: &str = r"/* edax:ledger_delete */
DELETE FROM {ledger}
WHERE key = $1::text";

const SAGA_LOAD: &str = r"/* edax:saga_load */
SELECT json_build_object('version', i.version, 'state', i.state, 'folded', i.folded, 'completed', i.completed)::text
FROM {saga_instances} AS i
WHERE i.saga = $1::text
  AND i.id = $2::text";

/// The saga step's commands, inserted only when `written` wrote the instance.
const SAGA_SENT: &str = r"sent AS (
  INSERT INTO {saga_outbox} (
    saga,
    {command_columns}
  )
  SELECT
    $1::text,
    {command_values}
  FROM written
  CROSS JOIN {command_input}
  ORDER BY r.n
)";

/// What a saga write returns: the version it wrote, or the instance it found instead.
const SAGA_RESULT: &str = r"SELECT json_build_object(
  'written', (SELECT version FROM written),
  'stored', (SELECT i.version FROM {saga_instances} AS i WHERE i.saga = $1::text AND i.id = $2::text),
  'completed', (SELECT i.completed FROM {saga_instances} AS i WHERE i.saga = $1::text AND i.id = $2::text)
)::text";

const SAGA_INSERT: &str = r"/* edax:saga_insert */
WITH written AS (
  INSERT INTO {saga_instances} (saga, id, version, state, folded, completed, completed_at)
  VALUES (
    $1::text,
    $2::text,
    1,
    $3::text,
    $4::text,
    ($5::text)::boolean,
    CASE WHEN ($5::text)::boolean THEN clock_timestamp() END
  )
  ON CONFLICT (saga, id) DO NOTHING
  RETURNING version
),
{saga_sent}
{saga_result}";

const SAGA_UPDATE: &str = r"/* edax:saga_update */
WITH written AS (
  UPDATE {saga_instances}
  SET version = version + 1,
      state = $4::text,
      folded = $5::text,
      completed = ($6::text)::boolean,
      completed_at = CASE WHEN ($6::text)::boolean THEN clock_timestamp() END
  WHERE saga = $1::text
    AND id = $2::text
    AND version = ($3::text)::bigint
    AND NOT completed
  RETURNING version
),
{saga_sent}
{saga_result}";

const SAGA_UNPUBLISHED: OutboxRead = OutboxRead {
    tag: "saga_unpublished",
    role: Logical::SagaOutbox,
    select: r"SELECT {command_json}::text
FROM {saga_outbox} AS c",
    scope: &["c.saga = $1::text"],
    lease_scope: Some("$1::text"),
    position: "c.position",
    limit: 2,
};

const SAGA_MARK_PUBLISHED: &str = r"/* edax:saga_mark_published */
DELETE FROM {saga_outbox}
WHERE saga = $1::text
  AND id IN (SELECT jsonb_array_elements_text(($2::text)::jsonb))";

const SAGA_UNPUBLISHED_COUNT: &str = r"/* edax:saga_unpublished_count */
SELECT count(*)
FROM {saga_outbox}
WHERE saga = $1::text";

/// The SQL fragments of a command row, built from the command outbox's contract columns, so the
/// statements and the contract name the same columns.
struct CommandSql {
    /// The columns edax writes, one per line.
    columns: String,
    /// A row as one JSON object, from the alias `c`.
    json: String,
    /// The column values of the record `c`, in column order.
    values: String,
    /// The rows of the JSON array parameter `{commands}`, in array order, as `r.n` and the record `c`.
    input: String,
}

impl CommandSql {
    fn from_contract() -> CommandSql {
        let columns: Vec<(&str, ColumnType)> = table_contract(Logical::CommandOutbox)
            .columns
            .into_iter()
            .filter(|column| !column.defaulted)
            .map(|column| (column.name, column.column_type))
            .collect();
        let lines = |line: &dyn Fn(&str, ColumnType) -> String, separator: &str| {
            columns
                .iter()
                .map(|(name, column_type)| line(name, *column_type))
                .collect::<Vec<_>>()
                .join(separator)
        };
        let fields = lines(
            &|name, column_type| format!("'{name}', {}", read(name, column_type)),
            ",\n  ",
        );
        CommandSql {
            columns: lines(&|name, _| name.to_owned(), ",\n"),
            json: format!("json_build_object(\n  {fields}\n)"),
            values: lines(&|name, column_type| write(name, column_type), ",\n"),
            input: format!(
                "jsonb_array_elements(({{commands}}::text)::jsonb) WITH ORDINALITY AS r(value, n)\n\
                 CROSS JOIN LATERAL jsonb_to_record(r.value) AS c(\n  {}\n)",
                lines(
                    &|name, column_type| format!("{name} {}", wire_type(column_type)),
                    ",\n  "
                )
            ),
        }
    }
}

/// A column of the record `c` as a row returns it: bytes as hex text.
fn read(name: &str, column_type: ColumnType) -> String {
    match column_type {
        ColumnType::Bytea => format!("encode(c.{name}, 'hex')"),
        _ => format!("c.{name}"),
    }
}

/// A field of the record `c` as its column stores it: hex text as bytes.
fn write(name: &str, column_type: ColumnType) -> String {
    match column_type {
        ColumnType::Bytea => format!("decode(c.{name}, 'hex')"),
        _ => format!("c.{name}"),
    }
}

/// The type a column's value has in a JSON parameter: bytes travel as hex text.
fn wire_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Text | ColumnType::Bytea => "text",
        ColumnType::Bigint => "bigint",
        ColumnType::Jsonb => "jsonb",
        ColumnType::Boolean => "boolean",
        ColumnType::Timestamptz => "timestamptz",
    }
}

/// `text` with each `{name}` replaced by `value`. A value that spans lines takes the indentation of the
/// line it lands on, so a fragment keeps its layout inside the statement.
fn fill(text: &str, name: &str, value: &str) -> String {
    let placeholder = format!("{{{name}}}");
    text.split('\n')
        .map(|line| {
            if !line.contains(&placeholder) {
                return line.to_owned();
            }
            let indent = " ".repeat(line.len() - line.trim_start().len());
            line.replace(&placeholder, &value.replace('\n', &format!("\n{indent}")))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// One outbox's reads: its `SELECT` and `FROM`, the rows of one scope, and how they are paged. A leased
/// read binds the owner and the lease ms right after the limit.
struct OutboxRead {
    tag: &'static str,
    /// The outbox table, which names the lease.
    role: Logical,
    select: &'static str,
    /// The conditions that keep the scope's rows.
    scope: &'static [&'static str],
    /// The scope's own lease within the outbox, as SQL text: the saga for the saga outbox.
    lease_scope: Option<&'static str>,
    /// The outbox's `position` column, under its alias.
    position: &'static str,
    /// The parameter number of the limit.
    limit: usize,
}

/// The read `outbox` describes: its rows oldest first. With `lease`, the read first takes or renews that
/// lease and keeps rows only for its holder.
fn outbox_read(outbox: &OutboxRead, lease: Option<&str>) -> String {
    let OutboxRead {
        tag,
        select,
        scope,
        position,
        limit,
        ..
    } = outbox;
    let mut text = format!("/* edax:{tag} */\n");
    let mut conditions = scope.to_vec();
    if let Some(lease) = lease {
        text.push_str(&format!("WITH {lease}\n"));
        conditions.push("EXISTS (SELECT 1 FROM lease)");
    }
    text.push_str(select);
    if !conditions.is_empty() {
        text.push_str(&format!("\nWHERE {}", conditions.join("\n  AND ")));
    }
    text.push_str(&format!("\nORDER BY {position}\nLIMIT (${limit}::text)::bigint"));
    text
}

/// Every statement, rendered for the tables `tables` names.
pub fn statements(tables: &Tables) -> Statements {
    let command = CommandSql::from_contract();
    // Each fragment, in the order they nest: a fragment may hold the placeholders of those after it.
    let fragments: [(&str, &str); 8] = [
        ("saga_sent", SAGA_SENT),
        ("saga_result", SAGA_RESULT),
        ("event_json", EVENT_JSON),
        ("ledger_fields", LEDGER_FIELDS),
        ("command_json", &command.json),
        ("command_columns", &command.columns),
        ("command_values", &command.values),
        ("command_input", &command.input),
    ];
    // `template` with every fragment, then each of `params`, then every table filled in.
    let render = |template: &str, params: &[(&str, &str)]| {
        let text = fragments
            .iter()
            .chain(params)
            .fold(template.to_owned(), |text, (name, value)| fill(&text, name, value));
        tables.render(&text)
    };
    // The lease is named for the schema-qualified outbox table, a name no other outbox has.
    let leased = |outbox: &OutboxRead| {
        let table = format!("{}.{}", tables.schema(), tables.name(outbox.role));
        let name = match outbox.lease_scope {
            None => format!("'{table}'"),
            Some(scope) => format!("('{table}:' || {scope})"),
        };
        let owner = format!("${}", outbox.limit + 1);
        let ms = format!("${}", outbox.limit + 2);
        render(
            &outbox_read(outbox, Some(LEASE)),
            &[("lease_name", &name), ("owner", &owner), ("ms", &ms)],
        )
    };
    Statements {
        catalog: CATALOG.to_owned(),
        server: SERVER.to_owned(),
        settings: SETTINGS.to_owned(),
        append: render(APPEND, &[]),
        load: render(LOAD, &[]),
        load_snapshot: render(LOAD_SNAPSHOT, &[]),
        save_snapshot: render(SAVE_SNAPSHOT, &[]),
        event_unpublished: render(&outbox_read(&EVENT_UNPUBLISHED, None), &[]),
        event_unpublished_leased: leased(&EVENT_UNPUBLISHED),
        event_mark_published: render(EVENT_MARK_PUBLISHED, &[]),
        event_unpublished_count: render(EVENT_UNPUBLISHED_COUNT, &[]),
        command_enqueue: render(COMMAND_ENQUEUE, &[("commands", "$1")]),
        command_unpublished: render(&outbox_read(&COMMAND_UNPUBLISHED, None), &[]),
        command_unpublished_leased: leased(&COMMAND_UNPUBLISHED),
        command_mark_published: render(COMMAND_MARK_PUBLISHED, &[]),
        command_unpublished_count: render(COMMAND_UNPUBLISHED_COUNT, &[]),
        projection_get: render(PROJECTION_GET, &[]),
        projection_get_locked: render(&format!("{PROJECTION_GET}\nFOR NO KEY UPDATE"), &[]),
        projection_insert: render(PROJECTION_INSERT, &[]),
        projection_update: render(PROJECTION_UPDATE, &[]),
        claim_insert: render(CLAIM_INSERT, &[]),
        claim_read: render(CLAIM_READ, &[]),
        claim_take_over: render(CLAIM_TAKE_OVER, &[]),
        ledger_hold: render(LEDGER_HOLD, &[]),
        ledger_settle: render(LEDGER_SETTLE, &[]),
        ledger_delete: render(LEDGER_DELETE, &[]),
        saga_load: render(SAGA_LOAD, &[]),
        saga_load_locked: render(&format!("{SAGA_LOAD}\nFOR NO KEY UPDATE"), &[]),
        saga_insert: render(SAGA_INSERT, &[("commands", "$6")]),
        saga_update: render(SAGA_UPDATE, &[("commands", "$7")]),
        saga_unpublished: render(&outbox_read(&SAGA_UNPUBLISHED, None), &[]),
        saga_unpublished_leased: leased(&SAGA_UNPUBLISHED),
        saga_mark_published: render(SAGA_MARK_PUBLISHED, &[]),
        saga_unpublished_count: render(SAGA_UNPUBLISHED_COUNT, &[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Every `{word}` placeholder and every `$` followed by a letter left in `text`.
    fn leftovers(text: &str) -> Vec<String> {
        let mut found = Vec::new();
        for (start, ch) in text.char_indices() {
            let rest = &text[start + ch.len_utf8()..];
            let word: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            let placeholder = ch == '{' && !word.is_empty() && rest[word.len()..].starts_with('}');
            let named_param = ch == '$' && word.starts_with(|c: char| c.is_ascii_alphabetic());
            if placeholder {
                found.push(format!("{{{word}}}"));
            } else if named_param {
                found.push(format!("${word}"));
            }
        }
        found
    }

    #[test]
    fn every_statement_is_rendered_and_tagged() {
        let named = Tables::new(
            "billing",
            BTreeMap::from([(Logical::CommandOutbox, "billing_commands".to_owned())]),
        )
        .unwrap();
        for tables in [Tables::default(), named] {
            let rendered = statements(&tables);
            let json = serde_json::to_value(&rendered).unwrap();
            for (name, text) in json.as_object().unwrap() {
                let text = text.as_str().unwrap();
                assert_eq!(leftovers(text), Vec::<String>::new(), "{name} is fully rendered");
                assert!(text.starts_with("/* edax:"), "{name} is tagged");
                assert!(!text.contains("\n\n"), "{name} has no empty line");
            }
        }
    }

    #[test]
    fn leftovers_finds_each_placeholder_shape() {
        assert_eq!(
            leftovers("SELECT {events}, $NAME, $1, '{}', {x y}"),
            vec!["{events}".to_owned(), "$NAME".to_owned()]
        );
    }

    #[test]
    fn lease_is_named_for_its_outbox_table() {
        let named = Tables::new(
            "billing",
            BTreeMap::from([(Logical::CommandOutbox, "billing_commands".to_owned())]),
        )
        .unwrap();
        let rendered = statements(&named);
        let cases = [
            (
                "event",
                &rendered.event_unpublished_leased,
                "outbox = 'billing.edax_event_outbox'",
            ),
            (
                "command",
                &rendered.command_unpublished_leased,
                "outbox = 'billing.billing_commands'",
            ),
            (
                "saga",
                &rendered.saga_unpublished_leased,
                "outbox = ('billing.edax_saga_outbox:' || $1::text)",
            ),
        ];
        for (name, text, held) in cases {
            assert!(text.contains(held), "{name}: {text}");
            assert!(
                text.contains("WHERE EXISTS (SELECT 1 FROM lease)")
                    || text.contains("AND EXISTS (SELECT 1 FROM lease)"),
                "{name}"
            );
        }
        assert!(
            !rendered.event_unpublished.contains("lease"),
            "an unleased read takes no lease"
        );
        assert!(
            !rendered.event_unpublished.contains("WHERE"),
            "an unleased event read keeps every row"
        );
    }

    #[test]
    fn locked_reads_are_their_base_read_plus_the_lock() {
        let rendered = statements(&Tables::default());
        assert_eq!(
            rendered.projection_get_locked,
            format!("{}\nFOR NO KEY UPDATE", rendered.projection_get)
        );
        assert_eq!(
            rendered.saga_load_locked,
            format!("{}\nFOR NO KEY UPDATE", rendered.saga_load)
        );
    }

    #[test]
    fn only_claim_read_computes_lapsed() {
        let rendered = statements(&Tables::default());
        assert!(rendered.claim_read.contains("'lapsed'"));
        assert!(!rendered.ledger_hold.contains("'lapsed'"));
    }

    #[test]
    fn command_fragments_name_every_contract_column() {
        let rendered = statements(&Tables::default());
        for column in table_contract(Logical::CommandOutbox).columns {
            let name = column.name;
            let in_enqueue = rendered.command_enqueue.contains(&format!("\n  {name}"));
            assert_eq!(in_enqueue, !column.defaulted, "{name} in command_enqueue");
            let in_saga = rendered.saga_insert.contains(&format!("\n    {name}"));
            assert_eq!(in_saga, !column.defaulted, "{name} in saga_insert");
            assert_eq!(
                rendered.command_unpublished.contains(&format!("'{name}', ")),
                !column.defaulted,
                "{name} in command_unpublished"
            );
        }
        assert!(rendered.command_unpublished.contains("'data', encode(c.data, 'hex')"));
        assert!(rendered.command_enqueue.contains("decode(c.data, 'hex')"));
    }
}
