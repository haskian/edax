//! The tables a Postgres store reads and writes, stated as a contract the host's own schema must meet,
//! and the check that holds a database to it.
//!
//! edax provisions nothing. The host creates the tables with its own migration tool, under the names it
//! chooses ([`Tables`]); edax states what each table must hold ([`contract`]) and checks the catalog against
//! it before the first statement runs ([`check_catalog`]). [`reference_ddl`] writes one schema that meets the
//! contract: a starting point to copy into a migration, never run by edax.
//!
//! The contract holds what correctness rests on and nothing more:
//!
//! - **Columns.** Each column edax reads or writes exists with an accepted type. One edax writes `NULL` into
//!   is nullable. A column edax never writes (`position`) has a default.
//! - **Unique keys.** Each compare-and-set and each insert-if-absent rests on a unique index over exactly the
//!   key's columns: not partial, not over an expression, not deferrable (a deferred check fires at commit,
//!   and `ON CONFLICT` cannot use one as its arbiter).
//! - **Collation.** Every text key column, and its unique index, collates as `"C"` (or `"POSIX"`, the same
//!   order): byte order is code point order in UTF-8, the order the kernel sorts by, and a deterministic
//!   collation is what makes two keys equal only when their bytes are.
//! - **Extra columns** are allowed when nullable or defaulted, so an `INSERT` that does not name them lands.
//!   Extra indexes, any constraint names, partitioning and storage settings are the host's.
//!
//! The contract is additive across edax releases: [`CONTRACT_VERSION`] names it, and a release that needs
//! more raises it. Constraint names are the host's, so the check reports which unique index guards which
//! key ([`Compatibility`]); the error verdict reads a unique violation by it.

use std::collections::BTreeMap;

use edax_core::Error;
use serde::{Deserialize, Serialize};

/// The version of the table contract this build requires. Raised only by an addition.
pub const CONTRACT_VERSION: u32 = 2;

/// The oldest Postgres major version the statements run on (`server_version_num` / 10000).
pub const MIN_SERVER_VERSION: i64 = 14;

/// The collations a text key column may use: both order by bytes.
pub const KEY_COLLATIONS: &[&str] = &["C", "POSIX"];

edax_core::closed_names! {
    /// A table edax reads and writes, by its role. The host names each one ([`Tables`]).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Logical {
        /// The event log, one row per event.
        Events = "events",
        /// The latest snapshot per stream.
        Snapshots = "snapshots",
        /// One row per unpublished event.
        EventOutbox = "event_outbox",
        /// The commands a save sends, until a relay marks them.
        CommandOutbox = "command_outbox",
        /// Read-model documents.
        Projections = "projections",
        /// The idempotency ledger.
        Ledger = "ledger",
        /// Saga instances.
        SagaInstances = "saga_instances",
        /// Each saga step's commands.
        SagaOutbox = "saga_outbox",
        /// The optional relay lease per outbox.
        RelayLeases = "relay_leases",
    }
}

edax_core::closed_names! {
    /// A unique key the contract requires, by what it guards. A unique violation is read by it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum UniqueKey {
        /// One event per stream and sequence: a violation is a lost append race.
        EventsStreamSequence = "events_stream_sequence",
        /// One event per id, store-wide.
        EventsId = "events_id",
        /// One snapshot per stream.
        SnapshotsStream = "snapshots_stream",
        /// One waiting command per id.
        CommandOutboxId = "command_outbox_id",
        /// One document per projection and id.
        ProjectionsKey = "projections_key",
        /// One record per idempotency key.
        LedgerKey = "ledger_key",
        /// One instance per saga and id.
        SagaInstancesKey = "saga_instances_key",
        /// One waiting command per saga and id.
        SagaOutboxCommand = "saga_outbox_command",
        /// One lease per outbox.
        RelayLeasesOutbox = "relay_leases_outbox",
    }
}

impl Logical {
    /// The role named `name`, or a `bad_handle` error.
    pub fn from_name(name: &str) -> Result<Logical, Error> {
        Logical::by_name(name).ok_or_else(|| Error::bad_handle(format_args!("unknown postgres table role: {name}")))
    }

    /// The name a table of this role takes when the host names none: `edax_` and the role.
    pub fn default_name(self) -> String {
        format!("edax_{}", self.as_str())
    }
}

/// A validated identifier: one or more of `a-z`, `0-9` and `_`, not starting with a digit, at most 63 bytes
/// (the server's limit), so it is quoted without escaping and never case-folded.
fn identifier(what: &str, name: &str) -> Result<String, Error> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if !valid {
        return Err(Error::bad_handle(format_args!(
            "postgres {what} {name:?} must be 1 to 63 of a-z, 0-9 and _, not starting with a digit"
        )));
    }
    Ok(name.to_owned())
}

/// Where a store's tables live: the schema, and a name per role. A role the host does not name takes its
/// default (`edax_events`, ...). The wire form is `{"schema": "public", "names": {"events": "..."}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Tables {
    schema: String,
    names: BTreeMap<Logical, String>,
}

impl Tables {
    /// The tables in `schema`, named by `names` where it names a role. A name outside the identifier rule,
    /// or two roles under one name, is a `bad_handle` error.
    pub fn new(schema: &str, names: BTreeMap<Logical, String>) -> Result<Tables, Error> {
        let schema = identifier("schema", schema)?;
        let mut seen: BTreeMap<String, Logical> = BTreeMap::new();
        let mut resolved = BTreeMap::new();
        for role in Logical::ALL {
            let name = match names.get(role) {
                Some(name) => identifier("table name", name)?,
                None => role.default_name(),
            };
            if let Some(other) = seen.insert(name.clone(), *role) {
                return Err(Error::bad_handle(format_args!(
                    "postgres tables {} and {} are both named {name:?}",
                    other.as_str(),
                    role.as_str()
                )));
            }
            resolved.insert(*role, name);
        }
        Ok(Tables {
            schema,
            names: resolved,
        })
    }

    /// The tables from their JSON wire form, validated as [`Tables::new`] does. Text that is not the wire
    /// form is a `bad_handle` error: a wiring mistake.
    pub fn from_json(json: &str) -> Result<Tables, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: String,
            #[serde(default)]
            names: BTreeMap<String, String>,
        }
        let wire: Wire = serde_json::from_str(json)
            .map_err(|e| Error::bad_handle(format_args!("postgres tables are not the wire form: {e}")))?;
        let names = wire
            .names
            .into_iter()
            .map(|(role, name)| Logical::from_name(&role).map(|role| (role, name)))
            .collect::<Result<_, _>>()?;
        Tables::new(&wire.schema, names)
    }

    /// The tables, every role named, as their JSON wire form.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// The schema.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The name of the table of `role`.
    pub fn name(&self, role: Logical) -> &str {
        self.names.get(&role).map_or("", String::as_str)
    }

    /// The table of `role`, schema-qualified and quoted: `"public"."edax_events"`.
    pub fn qualified(&self, role: Logical) -> String {
        format!("\"{}\".\"{}\"", self.schema, self.name(role))
    }

    /// `template` with every `{<role>}` replaced by that table, qualified.
    pub(crate) fn render(&self, template: &str) -> String {
        Logical::ALL.iter().fold(template.to_owned(), |text, role| {
            text.replace(&format!("{{{}}}", role.as_str()), &self.qualified(*role))
        })
    }
}

impl Default for Tables {
    /// Every table in `public` under its default name.
    fn default() -> Self {
        Tables {
            schema: "public".to_owned(),
            names: Logical::ALL.iter().map(|role| (*role, role.default_name())).collect(),
        }
    }
}

/// A column type the contract names, by the name `format_type` gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    /// `text` or `character varying`.
    Text,
    /// `bigint`.
    Bigint,
    /// `bytea`.
    Bytea,
    /// `jsonb`.
    Jsonb,
    /// `boolean`.
    Boolean,
    /// `timestamp with time zone`.
    Timestamptz,
}

impl ColumnType {
    /// The type names `format_type` gives a column this type accepts.
    pub fn accepts(self) -> &'static [&'static str] {
        match self {
            ColumnType::Text => &["text", "character varying"],
            ColumnType::Bigint => &["bigint"],
            ColumnType::Bytea => &["bytea"],
            ColumnType::Jsonb => &["jsonb"],
            ColumnType::Boolean => &["boolean"],
            ColumnType::Timestamptz => &["timestamp with time zone"],
        }
    }

    fn ddl(self) -> &'static str {
        match self {
            ColumnType::Text => "text",
            ColumnType::Bigint => "bigint",
            ColumnType::Bytea => "bytea",
            ColumnType::Jsonb => "jsonb",
            ColumnType::Boolean => "boolean",
            ColumnType::Timestamptz => "timestamptz",
        }
    }
}

/// What a column must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Its name.
    pub name: &'static str,
    /// Its type.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// edax writes `NULL` into it, so it must be nullable.
    pub nullable: bool,
    /// edax never writes it, so it must have a default (an identity or a sequence).
    pub defaulted: bool,
    /// A text key column: its collation must be one of [`KEY_COLLATIONS`].
    pub key: bool,
}

/// A unique key and the columns it covers.
pub type UniqueColumns = (UniqueKey, Vec<&'static str>);

/// What a table must hold: its columns and its unique keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableContract {
    /// The table's role.
    pub role: Logical,
    /// Every column edax reads or writes.
    pub columns: Vec<Column>,
    /// Every unique key, by its columns.
    pub unique: Vec<UniqueColumns>,
    /// The reference schema's primary key, by its columns: the first unique key, or the `position` of an
    /// outbox that has none. Never checked: the host may key its table as it likes.
    pub primary_key: Vec<&'static str>,
    /// Indexes the reference schema adds for speed, by their columns; never checked.
    pub suggested_indexes: Vec<Vec<&'static str>>,
}

const fn col(name: &'static str, column_type: ColumnType) -> Column {
    Column {
        name,
        column_type,
        nullable: false,
        defaulted: false,
        key: false,
    }
}

const fn key(name: &'static str) -> Column {
    Column {
        key: true,
        ..col(name, ColumnType::Text)
    }
}

const fn nullable(name: &'static str, column_type: ColumnType) -> Column {
    Column {
        nullable: true,
        ..col(name, column_type)
    }
}

const fn position() -> Column {
    Column {
        defaulted: true,
        ..col("position", ColumnType::Bigint)
    }
}

/// The columns of a message row after its key columns, shared by the command and saga outboxes.
fn command_columns() -> Vec<Column> {
    use ColumnType::{Bigint, Bytea, Text};
    vec![
        nullable("idempotency_key", Text),
        nullable("aggregate_id", Text),
        nullable("tenant", Text),
        nullable("subject", Text),
        nullable("encryption", Text),
        col("name", Text),
        col("version", Bigint),
        col("data", Bytea),
        col("correlation_id", Text),
        col("causation_id", Text),
        col("issued_at", Bigint),
        col("extensions", Text),
        col("metadata", Text),
    ]
}

/// What the table of `role` must hold.
pub fn table_contract(role: Logical) -> TableContract {
    use ColumnType::{Bigint, Boolean, Bytea, Jsonb, Text, Timestamptz};
    let (columns, unique, suggested_indexes): (Vec<Column>, Vec<UniqueColumns>, Vec<Vec<&str>>) = match role {
        Logical::Events => (
            vec![
                key("id"),
                key("stream"),
                col("sequence", Bigint),
                col("aggregate_id", Text),
                nullable("tenant", Text),
                nullable("subject", Text),
                nullable("encryption", Text),
                col("name", Text),
                col("version", Bigint),
                col("data", Bytea),
                col("correlation_id", Text),
                col("causation_id", Text),
                nullable("occurred_at", Bigint),
                col("recorded_at", Bigint),
                col("extensions", Text),
                col("metadata", Text),
            ],
            vec![
                (UniqueKey::EventsId, vec!["id"]),
                (UniqueKey::EventsStreamSequence, vec!["stream", "sequence"]),
            ],
            vec![],
        ),
        Logical::Snapshots => (
            vec![
                key("stream"),
                nullable("tenant", Text),
                nullable("subject", Text),
                nullable("encryption", Text),
                col("type_name", Text),
                col("version", Bigint),
                col("data", Bytea),
            ],
            vec![(UniqueKey::SnapshotsStream, vec!["stream"])],
            vec![],
        ),
        Logical::EventOutbox => (vec![position(), key("event_id")], vec![], vec![vec!["event_id"]]),
        Logical::CommandOutbox => (
            [vec![position(), key("id")], command_columns()].concat(),
            vec![(UniqueKey::CommandOutboxId, vec!["id"])],
            vec![vec!["position"]],
        ),
        Logical::Projections => (
            vec![
                key("projection"),
                key("id"),
                col("version", Bigint),
                col("document", Jsonb),
            ],
            vec![(UniqueKey::ProjectionsKey, vec!["projection", "id"])],
            vec![],
        ),
        Logical::Ledger => (
            vec![
                key("key"),
                nullable("owner", Text),
                nullable("settled_by", Text),
                col("record", Text),
                col("claimed_at", Timestamptz),
                nullable("settled_at", Timestamptz),
            ],
            vec![(UniqueKey::LedgerKey, vec!["key"])],
            vec![],
        ),
        Logical::SagaInstances => (
            vec![
                key("saga"),
                key("id"),
                col("version", Bigint),
                nullable("state", Text),
                col("folded", Text),
                col("completed", Boolean),
                nullable("completed_at", Timestamptz),
            ],
            vec![(UniqueKey::SagaInstancesKey, vec!["saga", "id"])],
            vec![],
        ),
        Logical::SagaOutbox => (
            [vec![position(), key("saga"), key("id")], command_columns()].concat(),
            vec![(UniqueKey::SagaOutboxCommand, vec!["saga", "id"])],
            vec![vec!["saga", "position"]],
        ),
        Logical::RelayLeases => (
            vec![key("outbox"), col("owner", Text), col("expires_at", Timestamptz)],
            vec![(UniqueKey::RelayLeasesOutbox, vec!["outbox"])],
            vec![],
        ),
    };
    // An outbox keyed by nothing else is keyed by the order its relay reads it in.
    let primary_key = unique
        .first()
        .map_or_else(|| vec!["position"], |(_, columns)| columns.clone());
    TableContract {
        role,
        columns,
        unique,
        primary_key,
        suggested_indexes,
    }
}

/// The whole contract, one entry per role.
pub fn contract() -> Vec<TableContract> {
    Logical::ALL.iter().map(|role| table_contract(*role)).collect()
}

/// A schema that meets the contract, for the tables `tables` names: a starting point for the host's own
/// migration, never run by edax. Each table is keyed by its contract's `primary_key`. Storage settings,
/// foreign keys and query indexes are the host's to add; `docs/backends/postgres.md` suggests some.
pub fn reference_ddl(tables: &Tables) -> String {
    let mut ddl = format!(
        "-- edax Postgres table contract {CONTRACT_VERSION}: a reference schema. Copy it into your own migration.\n"
    );
    for table in contract() {
        let name = tables.qualified(table.role);
        let mut lines: Vec<String> = table
            .columns
            .iter()
            .map(|column| {
                let mut line = format!("  {} {}", column.name, column.column_type.ddl());
                if column.key {
                    line.push_str(" COLLATE \"C\"");
                }
                if column.defaulted {
                    line.push_str(" GENERATED ALWAYS AS IDENTITY");
                }
                if !column.nullable {
                    line.push_str(" NOT NULL");
                }
                line
            })
            .collect();
        lines.push(format!("  PRIMARY KEY ({})", table.primary_key.join(", ")));
        for (_, columns) in table.unique.iter().filter(|(_, columns)| *columns != table.primary_key) {
            lines.push(format!("  UNIQUE ({})", columns.join(", ")));
        }
        ddl.push_str(&format!("\nCREATE TABLE {name} (\n{}\n);\n", lines.join(",\n")));
        for columns in &table.suggested_indexes {
            let index = format!("{}_{}", tables.name(table.role), columns.join("_"));
            ddl.push_str(&format!(
                "CREATE INDEX \"{}\" ON {name} ({});\n",
                index.chars().take(63).collect::<String>(),
                columns.join(", ")
            ));
        }
    }
    ddl
}

/// A column as the catalog reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct CatalogColumn {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
    not_null: bool,
    default: bool,
    collation: Option<String>,
}

/// A unique index as the catalog reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct CatalogUnique {
    name: String,
    columns: Vec<String>,
    collations: Vec<String>,
    partial: bool,
    expressions: bool,
    deferrable: bool,
    valid: bool,
}

/// A table as the catalog reports it; `kind` is `None` when no such table exists.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct CatalogTable {
    table: Logical,
    kind: Option<String>,
    columns: Vec<CatalogColumn>,
    unique: Vec<CatalogUnique>,
}

/// What the check learnt from a compatible database: which unique index guards which key, by the index's
/// name, the name a unique violation reports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compatibility {
    /// Each unique index the contract relies on, by its name.
    pub unique: BTreeMap<String, UniqueKey>,
}

/// The parameter of the `catalog` statement: the tables of `roles`, by role, schema and name.
pub fn catalog_param(tables: &Tables, roles: &[Logical]) -> String {
    let rows: Vec<serde_json::Value> = roles
        .iter()
        .map(
            |role| serde_json::json!({"logical": role.as_str(), "schema": tables.schema(), "name": tables.name(*role)}),
        )
        .collect();
    serde_json::Value::Array(rows).to_string()
}

/// Hold the tables of `roles`, as the `catalog` statement reported them in `catalog_json`, to the
/// contract. Every mismatch is listed in one `bad_handle` error, a wiring mistake no retry fixes. A
/// compatible database answers which unique index guards which key.
pub fn check_catalog(tables: &Tables, roles: &[Logical], catalog_json: &str) -> Result<Compatibility, Error> {
    let catalog: Vec<CatalogTable> = serde_json::from_str(catalog_json)
        .map_err(|e| Error::bad_handle(format_args!("postgres catalog reply is not the expected layout: {e}")))?;
    let mut problems = Vec::new();
    let mut compatibility = Compatibility::default();
    for role in roles {
        let name = tables.qualified(*role);
        let Some(found) = catalog.iter().find(|t| t.table == *role && t.kind.is_some()) else {
            problems.push(format!("table {name} ({}) does not exist", role.as_str()));
            continue;
        };
        check_table(&name, &table_contract(*role), found, &mut problems, &mut compatibility);
    }
    if !problems.is_empty() {
        return Err(Error::bad_handle(format_args!(
            "postgres tables do not meet edax table contract {CONTRACT_VERSION}: {}",
            problems.join("; ")
        )));
    }
    Ok(compatibility)
}

fn check_table(
    name: &str,
    contract: &TableContract,
    found: &CatalogTable,
    problems: &mut Vec<String>,
    compatibility: &mut Compatibility,
) {
    for column in &contract.columns {
        let Some(have) = found.columns.iter().find(|c| c.name == column.name) else {
            problems.push(format!("{name}.{} is missing", column.name));
            continue;
        };
        let accepts = column.column_type.accepts();
        if !accepts.contains(&have.column_type.as_str()) {
            problems.push(format!(
                "{name}.{} is {}, needs {}",
                column.name,
                have.column_type,
                accepts.join(" or ")
            ));
        }
        if column.nullable && have.not_null {
            problems.push(format!(
                "{name}.{} is NOT NULL, but edax writes NULL into it",
                column.name
            ));
        }
        if column.defaulted && !have.default {
            problems.push(format!(
                "{name}.{} needs a default (an identity or a sequence)",
                column.name
            ));
        }
        if column.key && !have.collation.as_deref().is_some_and(|c| KEY_COLLATIONS.contains(&c)) {
            problems.push(format!(
                "{name}.{} collates as {}, needs \"C\"",
                column.name,
                have.collation.as_deref().unwrap_or("nothing")
            ));
        }
    }
    let known: Vec<&str> = contract.columns.iter().map(|c| c.name).collect();
    for extra in found.columns.iter().filter(|c| !known.contains(&c.name.as_str())) {
        if extra.not_null && !extra.default {
            problems.push(format!(
                "{name}.{} is NOT NULL with no default, so edax's inserts would fail",
                extra.name
            ));
        }
    }
    for (unique_key, columns) in &contract.unique {
        // Every index that guards the key, not just the first: a violation names the one that caught it.
        let guards: Vec<&CatalogUnique> = found
            .unique
            .iter()
            .filter(|index| {
                let mut have: Vec<&str> = index.columns.iter().map(String::as_str).collect();
                let mut want = columns.clone();
                have.sort_unstable();
                want.sort_unstable();
                have == want
                    && !index.partial
                    && !index.expressions
                    && !index.deferrable
                    && index.valid
                    && index
                        .collations
                        .iter()
                        .all(|c| c.is_empty() || KEY_COLLATIONS.contains(&c.as_str()))
            })
            .collect();
        if guards.is_empty() {
            problems.push(format!(
                "{name} has no unique index on exactly ({}): not partial, not deferrable, collating as \"C\"",
                columns.join(", ")
            ));
        }
        for index in guards {
            compatibility.unique.insert(index.name.clone(), *unique_key);
        }
    }
}

/// Refuse a server the statements cannot run on, as a `bad_handle` error: a database whose
/// `server_encoding` is not `UTF8` (byte order is then not code point order), or a server older than
/// [`MIN_SERVER_VERSION`]. The lock timeout must exceed the server deadlock timeout, so a deadlock
/// reaches detection before a lock wait expires. `server_version_num` is `170004` for 17.4.
pub fn check_server(
    server_encoding: &str,
    server_version_num: i64,
    deadlock_timeout_ms: i64,
    settings: &crate::Settings,
) -> Result<(), Error> {
    if server_encoding != "UTF8" {
        return Err(Error::bad_handle(format_args!(
            "postgres server_encoding is {server_encoding}, edax needs UTF8"
        )));
    }
    if server_version_num / 10_000 < MIN_SERVER_VERSION {
        return Err(Error::bad_handle(format_args!(
            "postgres {server_version_num} is older than {MIN_SERVER_VERSION}"
        )));
    }
    if settings.lock_timeout_ms <= deadlock_timeout_ms {
        return Err(Error::bad_handle(format_args!(
            "lock_timeout_ms {} must exceed deadlock_timeout_ms {}, so deadlocks are detected before lock waits expire",
            settings.lock_timeout_ms, deadlock_timeout_ms
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tables_branches() {
        let custom = Tables::new("billing", BTreeMap::from([(Logical::Events, "billing_events".into())])).unwrap();
        assert_eq!(custom.qualified(Logical::Events), "\"billing\".\"billing_events\"");
        assert_eq!(
            custom.name(Logical::Ledger),
            "edax_ledger",
            "unnamed roles take the default"
        );
        assert_eq!(Tables::from_json(&custom.to_json()), Ok(custom));
        assert!(Tables::new("Bad", BTreeMap::new()).is_err());
        assert!(Tables::new("public", BTreeMap::from([(Logical::Ledger, "x-y".into())])).is_err());
        assert!(
            Tables::new("public", BTreeMap::from([(Logical::Ledger, "edax_events".into())])).is_err(),
            "two roles under one name"
        );
        assert!(Tables::from_json(r#"{"schema":"public","names":{"evnts":"x"}}"#).is_err());
        assert!(Tables::from_json(r#"{"schema":"public","extra":1}"#).is_err());
    }

    /// The compatibility map names every index that guards a key, so a violation the server reports on
    /// any of them reads as that key. The refusals are `postgres_contract.json`'s, in `tests/golden.rs`.
    #[test]
    fn check_catalog_records_every_guarding_index() {
        let column = |name: &str, ty: &str, not_null: bool, collation: Option<&str>| json!({"name": name, "type": ty, "not_null": not_null, "default": false, "collation": collation});
        let unique = |name: &str| {
            json!({"name": name, "columns": ["key"], "collations": ["C"],
                                         "partial": false, "expressions": false, "deferrable": false, "valid": true})
        };
        let catalog = json!([{
            "table": "ledger", "kind": "r",
            "columns": [
                column("key", "text", true, Some("C")),
                column("owner", "text", false, Some("default")),
                column("settled_by", "text", false, Some("default")),
                column("record", "text", true, Some("default")),
                column("claimed_at", "timestamp with time zone", true, None),
                column("settled_at", "timestamp with time zone", false, None),
            ],
            "unique": [unique("my_ledger_pkey"), unique("my_ledger_key_again")],
        }]);
        assert_eq!(
            check_catalog(&Tables::default(), &[Logical::Ledger], &catalog.to_string()).map(|c| c.unique),
            Ok(BTreeMap::from([
                ("my_ledger_pkey".to_owned(), UniqueKey::LedgerKey),
                ("my_ledger_key_again".to_owned(), UniqueKey::LedgerKey),
            ]))
        );
    }

    #[test]
    fn reference_ddl_names_every_table() {
        let ddl = reference_ddl(&Tables::default());
        for role in Logical::ALL {
            assert!(
                ddl.contains(&format!("CREATE TABLE \"public\".\"edax_{}\"", role.as_str())),
                "{role:?}"
            );
        }
        assert!(ddl.contains("UNIQUE (stream, sequence)"));
        assert!(!ddl.contains("FUNCTION"), "no database objects but tables and indexes");
    }

    #[test]
    fn server_checks() {
        assert_eq!(
            check_server("UTF8", 170_004, 1_000, &crate::Settings::default()),
            Ok(())
        );
        assert!(check_server("SQL_ASCII", 170_004, 1_000, &crate::Settings::default()).is_err());
        assert!(check_server("UTF8", 130_000, 1_000, &crate::Settings::default()).is_err());
    }
}
