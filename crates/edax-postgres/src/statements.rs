//! Every statement a Postgres store runs, byte for byte, rendered for one schema.
//!
//! Each parameter is declared `text` and cast in the statement, so any client binds strings and
//! `NULL`, and [`crate::rows`] owns how a value becomes that text. Every row a message read returns is
//! one JSON text column, which [`crate::rows`] decodes. Each statement opens with an `/* edax:<op> */`
//! tag, so `pg_stat_statements` groups it.
//!
//! The shapes that carry a decision:
//!
//! - [`begin`] opens a transaction and sets its timeouts in one round trip, so a store's call alone costs
//!   three: open, the statement, [`Statements::commit`].
//! - `append` is a plain `INSERT`. Its version check reads the stream's last sequence in the same
//!   statement, and the unique index on `(stream, sequence)` catches the writer that raced past it.
//!   That violation aborts the transaction on purpose: an `ON CONFLICT DO NOTHING` would land the rest
//!   of a batch on top of the winner's event, and a caller that caught the conflict could commit it.
//!   It returns the stream's version before the write and its clock reading, not the events: the caller
//!   holds the drafts, and [`crate::rows::append_result`] stamps them.
//! - Each write inserts its outbox rows in the order of its batch, so a relay reads them in sequence.
//! - `ledger_hold` locks the key's row (`FOR UPDATE`) and the settling write follows in the same
//!   transaction, so the fence and the write are one step. A store that runs outside a unit of work
//!   opens a transaction for the pair.
//! - The `*_leased` reads take or renew the outbox's relay lease in the same statement: a relay that
//!   does not hold it reads no rows. Rows are never leased one by one, which would let two relays send
//!   one aggregate's events out of order. The lease is named for the schema-qualified outbox table (and
//!   the saga, for the saga outbox), so stores whose outboxes differ never share one, and no SDK names it.

use edax_core::Error;
use serde::{Deserialize, Serialize};

use crate::config::{check_config, Settings};
use crate::schema::{table_contract, Column, ColumnType, Logical, Tables};

/// Every statement, rendered for one schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statements {
    /// `$1` the tables to check ([`crate::schema::catalog_param`]): what the catalog holds for each, for
    /// [`crate::schema::check_catalog`].
    pub catalog: String,
    /// The server's encoding, version number and deadlock timeout in milliseconds.
    pub server: String,
    /// Ends the transaction [`begin`] opened and keeps its writes.
    pub commit: String,
    /// Ends the transaction [`begin`] opened and drops its writes.
    pub rollback: String,
    /// `$1` stream: takes the stream's transaction lock, held until the transaction ends.
    pub stream_lock: String,
    /// `$1` stream, `$2` expected version, `$3` the drafts: `{"stored", "recorded_at"}`, the stream's
    /// version before the write and the clock reading every event of the batch is stamped with.
    pub append: String,
    /// `$1` stream, `$2` after: one event row each.
    pub load: String,
    /// `$1` stream: the snapshot row.
    pub load_snapshot: String,
    /// `$1` stream, `$2` type name, `$3` version, `$4` data as hex, `$5` tenant,
    /// `$6` subject, `$7` encryption JSON: returns the stream's version.
    pub save_snapshot: String,
    /// `$1` limit: event rows, oldest first.
    pub event_unpublished: String,
    /// `$1` limit, `$2` owner, `$3` lease ms.
    pub event_unpublished_leased: String,
    /// `$1` the ids.
    pub event_mark_published: String,
    /// The waiting rows.
    pub event_unpublished_count: String,
    /// `$1` the commands, `$2` the last event the same save appended, which they wait behind, or `NULL`.
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
    /// `$1` saga, `$2` id, `$3` state (`NULL` completes the instance), `$4` folded, `$5` commands.
    pub saga_insert: String,
    /// `$1` saga, `$2` id, `$3` expected version, `$4` state (`NULL` completes the instance), `$5` folded,
    /// `$6` commands.
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
/// and its unique indexes, as one JSON text column. Reads only the catalog. A collation outside `pg_catalog`
/// is named with its schema, so a lookalike `"C"` in another schema never passes for the built-in one. A
/// column's `sequence` is its identity or owned sequence, `None` when there is none; `fills` says whether
/// the column takes its values from it, as an identity or a `nextval` default, rather than from another
/// default beside an unused owned sequence.
pub(crate) const CATALOG: &str = r"/* edax:catalog */
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
              'collation', CASE
                WHEN co.collnamespace = 'pg_catalog'::regnamespace THEN co.collname
                ELSE cn.nspname || '.' || co.collname
              END,
              'sequence', CASE WHEN a.attidentity <> '' OR a.atthasdef THEN (
                SELECT json_build_object(
                  'cache', s.seqcache,
                  'increment', s.seqincrement,
                  'cycle', s.seqcycle,
                  'fills', a.attidentity <> ''
                    OR pg_get_expr(d.adbin, d.adrelid) = format('nextval(%L::regclass)', s.seqrelid::regclass)
                )
                FROM pg_sequence AS s
                LEFT JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
                WHERE s.seqrelid = to_regclass(pg_get_serial_sequence(c.oid::regclass::text, a.attname))
              ) END
            )
            ORDER BY a.attnum
          )
          FROM pg_attribute AS a
          LEFT JOIN pg_collation AS co ON co.oid = a.attcollation
          LEFT JOIN pg_namespace AS cn ON cn.oid = co.collnamespace
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
                SELECT coalesce(
                  json_agg(
                    CASE
                      WHEN kc.oid IS NULL THEN ''
                      WHEN kc.collnamespace = 'pg_catalog'::regnamespace THEN kc.collname
                      ELSE kn.nspname || '.' || kc.collname
                    END
                    ORDER BY k.n
                  ),
                  '[]'::json
                )
                FROM unnest(i.indcollation::oid[]) WITH ORDINALITY AS k(oid, n)
                LEFT JOIN pg_collation AS kc ON kc.oid = k.oid
                LEFT JOIN pg_namespace AS kn ON kn.oid = kc.collnamespace
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

const COMMIT: &str = r"/* edax:commit */
COMMIT";

const ROLLBACK: &str = r"/* edax:rollback */
ROLLBACK";

// The stream's transaction lock, taken before `append` under command fence "lock". Its own statement:
// under READ COMMITTED each statement reads a new snapshot, so `append` after the wait sees the save that
// held the lock, and its version check fails a stale save.
const STREAM_LOCK: &str = r"/* edax:stream_lock */
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended({lock_name} || ':' || $1::text, 0))";

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
    {event_columns}
  )
  SELECT
    {event_values}
  FROM {event_input}
  CROSS JOIN clock
  CROSS JOIN current
  WHERE current.version = ($2::text)::bigint
  ORDER BY r.n
  RETURNING id, sequence
),
outbox AS (
  INSERT INTO {event_outbox} (event_id)
  SELECT id
  FROM inserted
  ORDER BY sequence
)
SELECT json_build_object(
  'stored', current.version,
  'recorded_at', clock.recorded_at
)::text
FROM current
CROSS JOIN clock";

const LOAD: &str = r"/* edax:load */
SELECT {event_json}::text
FROM {events} AS e
WHERE e.stream = $1::text
  AND e.sequence > ($2::text)::bigint
ORDER BY e.sequence";

const LOAD_SNAPSHOT: &str = r"/* edax:load_snapshot */
SELECT json_build_object('type_name', s.type_name, 'version', s.version, 'data', encode(s.data, 'hex'), 'tenant', s.tenant, 'subject', s.subject, 'encryption', s.encryption)::text
FROM {snapshots} AS s
WHERE s.stream = $1::text";

const SAVE_SNAPSHOT: &str = r"/* edax:save_snapshot */
WITH current AS (
  SELECT coalesce(max(sequence), 0) AS version
  FROM {events}
  WHERE stream = $1::text
),
saved AS (
  INSERT INTO {snapshots} AS s (stream, type_name, version, data, tenant, subject, encryption)
  SELECT $1::text, $2::text, ($3::text)::bigint, decode($4::text, 'hex'), $5::text, $6::text, ($7::text)::jsonb
  FROM current
  WHERE ($3::text)::bigint <= current.version
  ON CONFLICT (stream) DO UPDATE
  SET type_name = EXCLUDED.type_name,
      version = EXCLUDED.version,
      data = EXCLUDED.data,
      tenant = EXCLUDED.tenant, subject = EXCLUDED.subject, encryption = EXCLUDED.encryption
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

// Counts the rows the read can return: a row whose event is gone is never read, so it never counts.
const EVENT_UNPUBLISHED_COUNT: &str = r"/* edax:event_unpublished_count */
SELECT count(*)
FROM {event_outbox} AS o
JOIN {events} AS e ON e.id = o.event_id";

const COMMAND_ENQUEUE: &str = r"/* edax:command_enqueue */
INSERT INTO {command_outbox} (
  {command_columns}
)
SELECT
  {command_values}
FROM {command_input}
ORDER BY r.n";

// A command waits behind the last event its save appended, so the relay never sends a command before
// the event it follows from. Every later command for the same aggregate and tenant waits behind a held
// one too, so the aggregate's commands still leave in order. A command with no aggregate is ordered by
// its own id and holds back nothing else.
const COMMAND_UNPUBLISHED: OutboxRead = OutboxRead {
    tag: "command_unpublished",
    role: Logical::CommandOutbox,
    select: r"SELECT {command_json}::text
FROM {command_outbox} AS c",
    scope: &[
        "NOT EXISTS (SELECT 1 FROM {event_outbox} AS held WHERE held.event_id = c.after_event)",
        "NOT EXISTS (
  SELECT 1
  FROM {command_outbox} AS earlier
  JOIN {event_outbox} AS held ON held.event_id = earlier.after_event
  WHERE earlier.aggregate_id = c.aggregate_id
    AND earlier.tenant IS NOT DISTINCT FROM c.tenant
    AND earlier.position < c.position
)",
    ],
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
    {saga_columns}
  )
  SELECT
    {saga_values}
  FROM written
  CROSS JOIN {saga_input}
  ORDER BY r.n
)";

/// What a saga write returns: the version it wrote, or the instance it found instead, read once.
const SAGA_RESULT: &str = r"SELECT json_build_object(
  'written', (SELECT version FROM written),
  'stored', i.version,
  'completed', i.completed
)::text
FROM (SELECT 1) AS one
LEFT JOIN {saga_instances} AS i ON i.saga = $1::text AND i.id = $2::text";

const SAGA_INSERT: &str = r"/* edax:saga_insert */
WITH written AS (
  INSERT INTO {saga_instances} (saga, id, version, state, folded, completed, completed_at)
  VALUES (
    $1::text,
    $2::text,
    1,
    $3::text,
    $4::text,
    ($6::text)::boolean,
    CASE WHEN ($6::text)::boolean THEN clock_timestamp() END
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
      completed = ($7::text)::boolean,
      completed_at = CASE WHEN ($7::text)::boolean THEN clock_timestamp() END
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
    select: r"SELECT {saga_json}::text
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

/// A save's commands carry the last event it appended, the event they wait behind, from the enqueue's own
/// parameter; only the read's hold looks at it.
const COMMAND_FILLED: &[Filled] = &[Filled {
    column: "after_event",
    value: "$2::text",
    returned: false,
}];

/// A contract column a write fills from the statement itself rather than from its input rows.
struct Filled {
    column: &'static str,
    /// The SQL value the write stores.
    value: &'static str,
    /// Whether a row a read returns carries it.
    returned: bool,
}

/// The SQL fragments of a message row, built from a table's contract columns, so the statements and the
/// contract name the same columns. One alias names the input record a write reads and the row a read
/// returns.
struct RowSql {
    /// The columns edax writes, one per line.
    columns: String,
    /// A row as one JSON object, from the alias.
    json: String,
    /// The column values a write stores, in column order.
    values: String,
    /// The rows of the JSON array parameter `{rows}`, in array order, as `r.n` and the record.
    input: String,
}

impl RowSql {
    /// The fragments of `role`'s table under `alias`, each column of `filled` written from its value.
    fn from_contract(role: Logical, alias: &str, filled: &[Filled]) -> RowSql {
        let columns: Vec<Column> = table_contract(role)
            .columns
            .into_iter()
            .filter(|column| !column.defaulted)
            .collect();
        let fill = |column: &Column| filled.iter().find(|f| f.column == column.name);
        let names: Vec<&str> = columns.iter().map(|column| column.name).collect();
        let fields: Vec<String> = columns
            .iter()
            .filter(|column| fill(column).is_none_or(|f| f.returned))
            .map(|column| format!("'{}', {}", column.name, read(alias, column.name, column.column_type)))
            .collect();
        let values: Vec<String> = columns
            .iter()
            .map(|column| {
                fill(column).map_or_else(|| write(alias, column.name, column.column_type), |f| f.value.to_owned())
            })
            .collect();
        let record: Vec<String> = columns
            .iter()
            .filter(|column| fill(column).is_none())
            .map(|column| format!("{} {}", column.name, wire_type(column.column_type)))
            .collect();
        RowSql {
            columns: names.join(",\n"),
            json: format!("json_build_object(\n  {}\n)", fields.join(",\n  ")),
            values: values.join(",\n"),
            input: format!(
                "jsonb_array_elements(({{rows}}::text)::jsonb) WITH ORDINALITY AS r(value, n)\n\
                 CROSS JOIN LATERAL jsonb_to_record(r.value) AS {alias}(\n  {}\n)",
                record.join(",\n  ")
            ),
        }
    }
}

/// A column of the row `alias` as a read returns it: bytes as hex text.
fn read(alias: &str, name: &str, column_type: ColumnType) -> String {
    match column_type {
        ColumnType::Bytea => format!("encode({alias}.{name}, 'hex')"),
        _ => format!("{alias}.{name}"),
    }
}

/// A field of the record `alias` as its column stores it: hex text as bytes.
fn write(alias: &str, name: &str, column_type: ColumnType) -> String {
    match column_type {
        ColumnType::Bytea => format!("decode({alias}.{name}, 'hex')"),
        _ => format!("{alias}.{name}"),
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

/// What opens a transaction: `READ COMMITTED` whatever the server's default, and the lock, statement and
/// idle timeouts of `settings` for this transaction only (`SET LOCAL`). Several statements without
/// parameters, so a client sends them as one simple query, in one round trip. The values are the checked
/// integers rendered here, never host text; settings [`check_config`] refuses are its error.
pub fn begin(settings: &Settings) -> Result<String, Error> {
    check_config(settings)?;
    Ok(format!(
        "/* edax:begin */\n\
         BEGIN ISOLATION LEVEL READ COMMITTED;\n\
         SET LOCAL lock_timeout = '{}ms';\n\
         SET LOCAL statement_timeout = '{}ms';\n\
         SET LOCAL idle_in_transaction_session_timeout = '{}ms'",
        settings.lock_timeout_ms, settings.statement_timeout_ms, settings.idle_in_transaction_ms
    ))
}

/// Every statement, rendered for the tables `tables` names.
pub fn statements(tables: &Tables) -> Statements {
    // An event's stream is the append's own parameter, and its time the store's clock: no input row has
    // either, and a returned row carries only the time.
    let event = RowSql::from_contract(
        Logical::Events,
        "e",
        &[
            Filled {
                column: "stream",
                value: "$1::text",
                returned: false,
            },
            Filled {
                column: "recorded_at",
                value: "clock.recorded_at",
                returned: true,
            },
        ],
    );
    let command = RowSql::from_contract(Logical::CommandOutbox, "c", COMMAND_FILLED);
    // A saga step's commands carry the saga the write names.
    let saga = RowSql::from_contract(
        Logical::SagaOutbox,
        "c",
        &[Filled {
            column: "saga",
            value: "$1::text",
            returned: false,
        }],
    );
    // Each fragment, in the order they nest: a fragment may hold the placeholders of those after it.
    let fragments: [(&str, &str); 15] = [
        ("saga_sent", SAGA_SENT),
        ("saga_result", SAGA_RESULT),
        ("ledger_fields", LEDGER_FIELDS),
        ("event_json", &event.json),
        ("event_columns", &event.columns),
        ("event_values", &event.values),
        ("event_input", &event.input),
        ("command_json", &command.json),
        ("command_columns", &command.columns),
        ("command_values", &command.values),
        ("command_input", &command.input),
        ("saga_json", &saga.json),
        ("saga_columns", &saga.columns),
        ("saga_values", &saga.values),
        ("saga_input", &saga.input),
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
        commit: COMMIT.to_owned(),
        rollback: ROLLBACK.to_owned(),
        stream_lock: render(
            STREAM_LOCK,
            &[(
                "lock_name",
                &format!("'{}.{}'", tables.schema(), tables.name(Logical::Events)),
            )],
        ),
        append: render(APPEND, &[("rows", "$3")]),
        load: render(LOAD, &[]),
        load_snapshot: render(LOAD_SNAPSHOT, &[]),
        save_snapshot: render(SAVE_SNAPSHOT, &[]),
        event_unpublished: render(&outbox_read(&EVENT_UNPUBLISHED, None), &[]),
        event_unpublished_leased: leased(&EVENT_UNPUBLISHED),
        event_mark_published: render(EVENT_MARK_PUBLISHED, &[]),
        event_unpublished_count: render(EVENT_UNPUBLISHED_COUNT, &[]),
        command_enqueue: render(COMMAND_ENQUEUE, &[("rows", "$1")]),
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
        saga_insert: render(SAGA_INSERT, &[("rows", "$5")]),
        saga_update: render(SAGA_UPDATE, &[("rows", "$6")]),
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
    fn begin_renders_checked_settings() {
        let settings = Settings {
            lock_timeout_ms: 250,
            ..Settings::default()
        };
        let text = begin(&settings).unwrap();
        assert!(text.starts_with("/* edax:begin */\nBEGIN ISOLATION LEVEL READ COMMITTED;"));
        assert!(text.contains("SET LOCAL lock_timeout = '250ms';"));
        assert!(text.contains("SET LOCAL idle_in_transaction_session_timeout = '60000ms'"));
        let refused = Settings {
            lock_timeout_ms: -1,
            ..Settings::default()
        };
        assert_eq!(
            begin(&refused).map_err(|e| e.to_string()),
            Err("bad_handle: lock_timeout_ms must be positive, got -1".to_owned())
        );
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
        let saga = table_contract(Logical::SagaOutbox).columns;
        for column in table_contract(Logical::CommandOutbox).columns {
            let name = column.name;
            let unreturned = COMMAND_FILLED.iter().any(|f| f.column == name && !f.returned);
            let in_enqueue = rendered.command_enqueue.contains(&format!("\n  {name}"));
            assert_eq!(in_enqueue, !column.defaulted, "{name} in command_enqueue");
            let in_saga = rendered.saga_insert.contains(&format!("\n    {name}"));
            let saga_has = saga.iter().any(|c| c.name == name);
            assert_eq!(in_saga, !column.defaulted && saga_has, "{name} in saga_insert");
            assert_eq!(
                rendered.command_unpublished.contains(&format!("'{name}', ")),
                !column.defaulted && !unreturned,
                "{name} in command_unpublished"
            );
        }
        assert!(rendered.command_unpublished.contains("'data', encode(c.data, 'hex')"));
        assert!(rendered.command_enqueue.contains("decode(c.data, 'hex')"));
        assert!(
            !rendered.saga_unpublished.contains("'saga', "),
            "a saga's command row carries no saga"
        );
    }

    /// The snapshot statements are written by hand, so they are held to the contract here.
    #[test]
    fn snapshot_statements_name_every_contract_column() {
        let rendered = statements(&Tables::default());
        let (save, _) = rendered.save_snapshot.split_once("ON CONFLICT").unwrap();
        let columns = table_contract(Logical::Snapshots).columns;
        let (_, inserted) = save.split_once("AS s (").unwrap();
        let mut inserted: Vec<&str> = inserted.split_once(')').unwrap().0.split(", ").collect();
        let mut contract: Vec<&str> = columns.iter().map(|column| column.name).collect();
        inserted.sort_unstable();
        contract.sort_unstable();
        assert_eq!(inserted, contract, "the save writes every contract column");
        for column in &columns {
            let name = column.name;
            assert_eq!(
                rendered
                    .load_snapshot
                    .contains(&format!("'{name}', {}", read("s", name, column.column_type))),
                name != "stream",
                "{name} in the read row"
            );
        }
        let jsonb = columns.iter().filter(|c| c.column_type == ColumnType::Jsonb).count();
        assert_eq!(
            save.matches("::jsonb").count(),
            jsonb,
            "each jsonb column is bound as jsonb"
        );
    }

    #[test]
    fn event_fragments_name_every_contract_column() {
        let rendered = statements(&Tables::default());
        let (input, _) = rendered.append.split_once("CROSS JOIN clock").unwrap();
        let (_, record) = input.split_once("jsonb_to_record").unwrap();
        for column in table_contract(Logical::Events).columns {
            let name = column.name;
            assert!(
                rendered.append.contains(&format!("\n    {name},"))
                    || rendered.append.contains(&format!("\n    {name}\n")),
                "{name} in the insert"
            );
            let filled = matches!(name, "stream" | "recorded_at");
            assert_eq!(
                record.contains(&format!("\n    {name} ")),
                !filled,
                "{name} in the input record"
            );
            assert_eq!(
                rendered.load.contains(&format!("'{name}', ")),
                name != "stream",
                "{name} in a read row"
            );
        }
        assert!(
            rendered.append.contains("$1::text,\n    e.sequence"),
            "the stream is the append's own"
        );
        assert!(rendered.append.contains("clock.recorded_at"));
        assert!(rendered.load.contains("'data', encode(e.data, 'hex')"));
        assert!(rendered.append.contains("decode(e.data, 'hex')"));
    }
}
