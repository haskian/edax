//! Durable erasure operations, notifications and destination fences. This module provides SQL,
//! never provisions tables, and never starts a transaction.
//!
//! SDK protocol: use one READ COMMITTED transaction; execute each lock call in returned order,
//! then execute `check_revoked` as a separate statement and apply [`check_guard`] before effects.
//! Keep the transaction open through the protected write's commit. Cleanup instead acquires
//! [`revocation_locks`], writes the permanent tombstone, and deletes plaintext in that transaction.
//! A tenant exclusive lock drains every child subject, including subjects with no existing rows.
//! Acquire fences before application row locks. For work touching multiple scopes, pass them all to
//! one [`writer_locks`] call: it returns every tenant lock, then every subject lock, in one order.
//!
//! Separate commands matter: READ COMMITTED obtains a new snapshot per command. A read in the
//! statement that waited for an advisory lock can predate the revocation it waited behind.
//! See PostgreSQL 17 `transaction-iso.html` and `functions-admin.html` (transaction advisory locks).

use std::collections::{BTreeMap, BTreeSet};

use edax_core::{Error, ErrorCode};
use edax_event::erasure::{ErasureOperation, ErasureStage};
use edax_seal::KeyScope;
use serde::{Deserialize, Serialize};

use crate::config::check_bigint;
use crate::rows::{text_param, Call, Param};
use crate::schema::{Tables, KEY_COLLATIONS};

struct ErasureColumn {
    name: &'static str,
    kind: &'static str,
    key: bool,
}
struct ErasureTable {
    suffix: &'static str,
    columns: &'static [ErasureColumn],
    primary: &'static [&'static str],
    unique: &'static [&'static [&'static str]],
}

const TABLES: &[ErasureTable] = &[
    ErasureTable {
        suffix: "operations",
        columns: &[
            ErasureColumn {
                name: "operation_id",
                kind: "text",
                key: true,
            },
            ErasureColumn {
                name: "scope",
                kind: "text",
                key: true,
            },
            ErasureColumn {
                name: "request_id",
                kind: "text",
                key: true,
            },
            ErasureColumn {
                name: "version",
                kind: "bigint",
                key: false,
            },
            ErasureColumn {
                name: "record",
                kind: "text",
                key: false,
            },
        ],
        primary: &["operation_id"],
        unique: &[&["scope", "request_id"]],
    },
    ErasureTable {
        suffix: "notifications",
        columns: &[
            ErasureColumn {
                name: "operation_id",
                kind: "text",
                key: true,
            },
            ErasureColumn {
                name: "notification",
                kind: "text",
                key: false,
            },
        ],
        primary: &["operation_id"],
        unique: &[],
    },
    ErasureTable {
        suffix: "revocations",
        columns: &[ErasureColumn {
            name: "scope",
            kind: "text",
            key: true,
        }],
        primary: &["scope"],
        unique: &[],
    },
];

/// SQL for the erasure tables in one validated schema. Every parameter crosses as text.
#[derive(Debug, Clone, Serialize)]
pub struct Statements {
    /// No parameters. Read the catalog and pass its JSON text to [`check_catalog`] before use.
    pub check_schema: String,
    /// `$1` operation ID, `$2` canonical scope, `$3` request ID, `$4` operation JSON; returns version.
    pub create_operation: String,
    /// `$1` canonical scope, `$2` request ID; returns a versioned operation JSON row.
    pub load_operation: String,
    /// `$1` operation ID; returns a versioned operation JSON row.
    pub get_operation: String,
    /// `$1` limit, `$2` optional exclusive operation-ID cursor; reset after an empty page.
    pub pending_operations: String,
    /// `$1` operation ID, `$2` expected version, `$3` operation JSON, `$4` optional notification JSON.
    pub update_operation: String,
    /// `$1` canonical scope; shared fence held until transaction end.
    pub lock_shared: String,
    /// `$1` canonical scope; exclusive fence held until transaction end.
    pub lock_exclusive: String,
    /// `$1` parent scope, `$2` exact scope; run after lock commands, then call [`check_guard`].
    pub check_revoked: String,
    /// `$1` canonical scope; permanently revoke while holding its exclusive fence.
    pub revoke: String,
    /// `$1` limit; pending control notification JSON rows.
    pub pending_notifications: String,
    /// `$1` operation ID; remove its notification only after confirmed publication.
    pub mark_published: String,
}

fn table(schema: &str, suffix: &str) -> String {
    format!("\"{schema}\".\"edax_erasure_{suffix}\"")
}

/// Reference schema for the adopter's migration. Edax never runs it.
pub fn reference_ddl(schema: &str) -> Result<String, Error> {
    let tables = Tables::new(schema, BTreeMap::new())?;
    let mut ddl = String::new();
    for spec in TABLES {
        let mut columns: Vec<String> = spec
            .columns
            .iter()
            .map(|column| {
                let collation = if column.key { " COLLATE \"C\"" } else { "" };
                let required = if spec.primary.len() == 1 && spec.primary.contains(&column.name) {
                    " PRIMARY KEY"
                } else {
                    " NOT NULL"
                };
                let check = if column.name == "version" {
                    " CHECK (version > 0)"
                } else {
                    ""
                };
                format!("  {} {}{collation}{required}{check}", column.name, column.kind)
            })
            .collect();
        if spec.primary.len() > 1 {
            columns.push(format!("  PRIMARY KEY ({})", spec.primary.join(", ")));
        }
        for unique in spec.unique {
            columns.push(format!("  UNIQUE ({})", unique.join(", ")));
        }
        ddl.push_str(&format!(
            "CREATE TABLE {} (\n{}\n);\n",
            table(tables.schema(), spec.suffix),
            columns.join(",\n")
        ));
    }
    ddl.push_str(&format!(
        "CREATE INDEX edax_erasure_operations_pending ON {} (operation_id) WHERE record::json->>'stage' <> 'complete';\n",
        table(tables.schema(), "operations")
    ));
    Ok(ddl)
}

/// Reuse the shared catalog query, adding primary-key identity for the control tables' stricter contract.
fn catalog_query(tables: &Tables) -> String {
    let wanted: Vec<serde_json::Value> = TABLES
        .iter()
        .map(|spec| {
            serde_json::json!({
                "logical": spec.suffix, "schema": tables.schema(), "name": format!("edax_erasure_{}", spec.suffix)
            })
        })
        .collect();
    // The schema was validated by Tables and all remaining strings are kernel constants: no quoted
    // SQL literal can contain an apostrophe. Embedding this fixed parameter keeps adapters declarative.
    crate::statements::statements(tables)
        .catalog
        .replace(
            "'valid', i.indisvalid,",
            "'valid', i.indisvalid, 'primary', i.indisprimary,",
        )
        .replace("$1::text", &format!("'{}'::text", serde_json::Value::Array(wanted)))
}

#[derive(Deserialize)]
struct CatalogColumn {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    not_null: bool,
    default: bool,
    collation: Option<String>,
}
#[derive(Deserialize)]
struct CatalogIndex {
    columns: Vec<String>,
    collations: Vec<String>,
    partial: bool,
    expressions: bool,
    deferrable: bool,
    valid: bool,
    primary: bool,
}
#[derive(Deserialize)]
struct CatalogTable {
    table: String,
    kind: Option<String>,
    columns: Vec<CatalogColumn>,
    unique: Vec<CatalogIndex>,
}

fn index_matches(index: &CatalogIndex, columns: &[&str], primary: bool) -> bool {
    let mut have: Vec<&str> = index.columns.iter().map(String::as_str).collect();
    let mut wanted = columns.to_vec();
    have.sort_unstable();
    wanted.sort_unstable();
    have == wanted
        && (!primary || index.primary)
        && index.valid
        && !index.partial
        && !index.expressions
        && !index.deferrable
        && index.collations.len() == columns.len()
        && index
            .collations
            .iter()
            .all(|name| KEY_COLLATIONS.contains(&name.as_str()))
}

/// Verify columns, strict nullability, byte collations, and immediate valid primary/unique keys.
/// This is a read-only startup check; it never provisions or repairs a schema.
pub fn check_catalog(schema: &str, catalog_json: &str) -> Result<(), Error> {
    let tables = Tables::new(schema, BTreeMap::new())?;
    let catalog: Vec<CatalogTable> = serde_json::from_str(catalog_json)
        .map_err(|_| Error::bad_handle("postgres erasure catalog reply has an invalid layout"))?;
    let mut problems = Vec::new();
    for spec in TABLES {
        let name = table(tables.schema(), spec.suffix);
        let Some(found) = catalog
            .iter()
            .find(|entry| entry.table == spec.suffix && matches!(entry.kind.as_deref(), Some("r" | "p")))
        else {
            problems.push(format!("table {name} does not exist"));
            continue;
        };
        for required in spec.columns {
            let Some(column) = found.columns.iter().find(|column| column.name == required.name) else {
                problems.push(format!("{name}.{} is missing", required.name));
                continue;
            };
            if column.kind != required.kind {
                problems.push(format!("{name}.{} must have type {}", required.name, required.kind));
            }
            if !column.not_null {
                problems.push(format!("{name}.{} must be NOT NULL", required.name));
            }
            if required.key
                && !column
                    .collation
                    .as_deref()
                    .is_some_and(|name| KEY_COLLATIONS.contains(&name))
            {
                problems.push(format!("{name}.{} must collate as C or POSIX", required.name));
            }
        }
        for extra in found
            .columns
            .iter()
            .filter(|column| !spec.columns.iter().any(|required| required.name == column.name))
        {
            if extra.not_null && !extra.default {
                problems.push(format!(
                    "{name}.{} prevents inserts: NOT NULL without a default",
                    extra.name
                ));
            }
        }
        if !found
            .unique
            .iter()
            .any(|index| index_matches(index, spec.primary, true))
        {
            problems.push(format!(
                "{name} needs an immediate valid primary key on ({}) collating as C",
                spec.primary.join(", ")
            ));
        }
        for unique in spec.unique {
            if !found.unique.iter().any(|index| index_matches(index, unique, false)) {
                problems.push(format!(
                    "{name} needs an immediate valid unique key on ({}) collating as C",
                    unique.join(", ")
                ));
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(Error::bad_handle(format!(
            "postgres erasure tables do not meet the contract: {}",
            problems.join("; ")
        )))
    }
}

/// Render all erasure statements. Table names are fixed within the validated schema.
pub fn statements(schema: &str) -> Result<Statements, Error> {
    let tables = Tables::new(schema, BTreeMap::new())?;
    let operations = table(tables.schema(), "operations");
    let notifications = table(tables.schema(), "notifications");
    let revocations = table(tables.schema(), "revocations");
    let row = "json_build_object('version', version, 'operation', record::json)::text";
    Ok(Statements {
        check_schema: catalog_query(&tables),
        create_operation: format!(
            "INSERT INTO {operations} (operation_id, scope, request_id, version, record) \
             VALUES ($1::text, $2::text, $3::text, 1, $4::text) \
             ON CONFLICT DO NOTHING RETURNING version"
        ),
        load_operation: format!("SELECT {row} FROM {operations} WHERE scope = $1::text AND request_id = $2::text"),
        get_operation: format!("SELECT {row} FROM {operations} WHERE operation_id = $1::text"),
        pending_operations: format!(
            "SELECT {row} FROM {operations} WHERE record::json->>'stage' <> 'complete' AND ($2::text IS NULL OR operation_id > $2::text) \
             ORDER BY operation_id COLLATE \"C\" LIMIT ($1::text)::bigint"
        ),
        update_operation: format!(
            "WITH updated AS (\n\
             UPDATE {operations} SET version = version + 1, record = $3::text \
             WHERE operation_id = $1::text AND version = ($2::text)::bigint RETURNING version\n\
             ), notified AS (\n\
             INSERT INTO {notifications} (operation_id, notification) \
             SELECT $1::text, $4::text FROM updated WHERE $4::text IS NOT NULL \
             ON CONFLICT (operation_id) DO NOTHING\n\
             ) SELECT version FROM updated"
        ),
        lock_shared: "SELECT pg_advisory_xact_lock_shared(hashtextextended('edax.erasure:' || $1::text, 0))".into(),
        lock_exclusive: "SELECT pg_advisory_xact_lock(hashtextextended('edax.erasure:' || $1::text, 0))".into(),
        check_revoked: format!(
            "SELECT json_build_object('isolation', current_setting('transaction_isolation'), \
             'revoked', EXISTS (SELECT 1 FROM {revocations} WHERE scope IN ($1::text, $2::text)))::text"
        ),
        revoke: format!("INSERT INTO {revocations} (scope) VALUES ($1::text) ON CONFLICT DO NOTHING"),
        pending_notifications: format!(
            "SELECT notification FROM {notifications} ORDER BY operation_id COLLATE \"C\" LIMIT ($1::text)::bigint"
        ),
        mark_published: format!("DELETE FROM {notifications} WHERE operation_id = $1::text"),
    })
}

/// Exact canonical scope ID; never uses the tenancy-dependent storage key helper.
pub fn scope_id(scope: &KeyScope) -> Result<String, Error> {
    scope.encode_hex()
}

fn parent_scope(scope: &KeyScope) -> KeyScope {
    KeyScope {
        subject: None,
        ..scope.clone()
    }
}

fn lock(scope_id: String, exclusive: bool) -> Call {
    Call {
        statement: if exclusive { "lock_exclusive" } else { "lock_shared" },
        params: vec![Some(scope_id)],
    }
}

/// Shared fences for one transaction's writes to `scopes`: every tenant fence, then every subject fence,
/// each set in scope-ID order and without repeats. One order for every writer keeps two writers that
/// fence the same scopes from waiting on each other behind a queued cleanup.
pub fn writer_locks(scopes: &[KeyScope]) -> Result<Vec<Call>, Error> {
    let mut tenants = BTreeSet::new();
    let mut subjects = BTreeSet::new();
    for scope in scopes {
        tenants.insert(scope_id(&parent_scope(scope))?);
        if scope.subject.is_some() {
            subjects.insert(scope_id(scope)?);
        }
    }
    Ok(tenants.into_iter().chain(subjects).map(|id| lock(id, false)).collect())
}

/// Tenant cleanup takes the exclusive parent fence; subject cleanup shares the parent and excludes
/// its subject. Hash collisions can delay unrelated work but never permit a conflicting writer.
pub fn revocation_locks(scope: &KeyScope) -> Result<Vec<Call>, Error> {
    let mut locks = vec![lock(scope_id(&parent_scope(scope))?, scope.subject.is_none())];
    if scope.subject.is_some() {
        locks.push(lock(scope_id(scope)?, true));
    }
    Ok(locks)
}

/// Bind the parent and exact scope for the fresh revocation read after lock acquisition.
pub fn guard_params(scope: &KeyScope) -> Result<Vec<Param>, Error> {
    Ok(vec![Some(scope_id(&parent_scope(scope))?), Some(scope_id(scope)?)])
}

/// Refuse revoked writes and isolation levels whose snapshot can predate a waited-for revocation.
pub fn check_guard(row: &str) -> Result<(), Error> {
    #[derive(Deserialize)]
    struct Guard {
        isolation: String,
        revoked: bool,
    }
    let guard: Guard = serde_json::from_str(row).map_err(Error::codec)?;
    if guard.isolation != "read committed" {
        return Err(Error::bad_handle(
            "erasure write fences require READ COMMITTED isolation",
        ));
    }
    if guard.revoked {
        return Err(Error::new(ErrorCode::Rejected, "protected scope has been revoked"));
    }
    Ok(())
}

/// Durable operation and optimistic concurrency version returned by operation reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredOperation {
    /// Positive database revision, independent of the workflow stage.
    pub version: u64,
    /// Validated workflow state.
    pub operation: ErasureOperation,
}

/// Decode one operation row, rejecting invalid database revisions or workflow state.
pub fn decode_operation(row: &str) -> Result<StoredOperation, Error> {
    let value: StoredOperation = serde_json::from_str(row).map_err(Error::codec)?;
    check_revision(value.version)?;
    Ok(value)
}

/// Bind a new persisted operation. If insertion conflicts, read and validate the existing request.
pub fn create_params(operation: &ErasureOperation) -> Result<Vec<Param>, Error> {
    if operation.stage() != ErasureStage::Persisted {
        return Err(Error::codec("new erasure operation must precede revocation"));
    }
    Ok(vec![
        text_param("erasure operation ID", operation.operation_id())?,
        Some(scope_id(operation.scope())?),
        text_param("erasure request ID", operation.request_id())?,
        Some(serde_json::to_string(operation).map_err(Error::codec)?),
    ])
}

/// Bind an idempotent operation request lookup.
pub fn load_params(scope: &KeyScope, request_id: &str) -> Result<Vec<Param>, Error> {
    Ok(vec![
        Some(scope_id(scope)?),
        text_param("erasure request ID", request_id)?,
    ])
}

/// Validate a repeated request against the operation already stored. The stored operation ID wins
/// even if the caller minted a new candidate ID before discovering the existing request.
pub fn check_request(stored: &StoredOperation, requested: &ErasureOperation) -> Result<(), Error> {
    edax_event::erasure::check_request(&stored.operation, requested)
}

/// Bind a single monotone workflow update. A lost compare-and-set requires reloading and reapplying
/// the confirmation, so concurrent acknowledgements cannot overwrite one another.
pub fn update_params(previous: &StoredOperation, next: &ErasureOperation) -> Result<Vec<Param>, Error> {
    check_revision(previous.version)?;
    check_bigint("next erasure revision", previous.version.saturating_add(1))?;
    let notification = edax_event::erasure::notification(&previous.operation, next)?;
    Ok(vec![
        text_param("erasure operation ID", next.operation_id())?,
        Some(previous.version.to_string()),
        Some(serde_json::to_string(next).map_err(Error::codec)?),
        notification,
    ])
}

fn check_revision(version: u64) -> Result<(), Error> {
    if version == 0 {
        return Err(Error::codec("erasure database revision must be positive"));
    }
    check_bigint("erasure revision", version).map(|_| ())
}

/// Interpret an update result. A lost race is transient and never publishes a notification.
pub fn update_result(version: Option<i64>) -> Result<u64, Error> {
    match version {
        Some(version) if version > 0 => u64::try_from(version).map_err(Error::codec),
        Some(_) => Err(Error::codec("erasure database revision must be positive")),
        None => Err(Error::new(ErrorCode::Concurrency, "erasure operation revision changed")),
    }
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    #[test]
    fn catalog_statement_is_read_only_and_needs_no_host_contract_parameters() {
        let sql = statements("erasure_test").unwrap().check_schema;
        assert!(sql.contains("pg_attribute") && sql.contains("pg_index"));
        assert!(sql.contains("'primary', i.indisprimary"));
        assert!(sql.contains("edax_erasure_operations") && sql.contains("edax_erasure_notifications"));
        assert!(sql.contains("edax_erasure_revocations"));
        assert!(!sql.contains("$1") && !sql.contains("CREATE "));
        assert!(statements("unsafe'schema").is_err());
        let missing = check_catalog("erasure_test", "[]").unwrap_err();
        assert_eq!(missing.code, ErrorCode::BadHandle);
        assert!(missing.message.contains("operations") && missing.message.contains("revocations"));
        assert_eq!(check_catalog("public", "{}").unwrap_err().code, ErrorCode::BadHandle);
    }

    fn psql(url: &str, sql: &str) -> String {
        let output = std::process::Command::new("psql")
            .args(["-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", url, "-c", sql])
            .output()
            .expect("psql is required when EDAX_POSTGRES_URL is set");
        assert!(
            output.status.success(),
            "psql failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("UTF-8 catalog")
    }

    struct Schema {
        url: String,
        name: String,
    }
    impl Drop for Schema {
        fn drop(&mut self) {
            let _ = std::process::Command::new("psql")
                .args([
                    "-X",
                    "-q",
                    "-v",
                    "ON_ERROR_STOP=1",
                    &self.url,
                    "-c",
                    &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.name),
                ])
                .output();
        }
    }

    #[test]
    fn live_catalog_accepts_reference_and_rejects_broken_control_contracts() {
        let Ok(url) = std::env::var("EDAX_POSTGRES_URL") else {
            return;
        };
        let schema = Schema {
            url,
            name: format!("edax_erasure_catalog_{}", edax_core::guid_simple()),
        };
        psql(
            &schema.url,
            &format!(
                "CREATE SCHEMA \"{}\"; {}",
                schema.name,
                reference_ddl(&schema.name).unwrap()
            ),
        );
        let query = statements(&schema.name).unwrap().check_schema;
        check_catalog(&schema.name, &psql(&schema.url, &query)).unwrap();
        let operations = table(&schema.name, "operations");
        let notifications = table(&schema.name, "notifications");
        let revocations = table(&schema.name, "revocations");
        let cases = [
            (format!("ALTER TABLE {operations} DROP COLUMN record"), "record is missing"),
            (format!("ALTER TABLE {operations} ALTER COLUMN version TYPE integer"), "version must have type bigint"),
            (format!("ALTER TABLE {operations} ALTER COLUMN record DROP NOT NULL"), "record must be NOT NULL"),
            (format!("ALTER TABLE {revocations} ALTER COLUMN scope TYPE text COLLATE \"default\""), "scope must collate"),
            (format!("ALTER TABLE {notifications} DROP CONSTRAINT edax_erasure_notifications_pkey; ALTER TABLE {notifications} ADD UNIQUE (operation_id)"), "primary key"),
            (format!("ALTER TABLE {operations} DROP CONSTRAINT edax_erasure_operations_scope_request_id_key; CREATE UNIQUE INDEX partial_request ON {operations} (scope, request_id) WHERE version > 0"), "unique key"),
            (format!("ALTER TABLE {operations} DROP CONSTRAINT edax_erasure_operations_pkey; ALTER TABLE {operations} ADD PRIMARY KEY (operation_id) DEFERRABLE"), "primary key"),
            (format!("ALTER TABLE {notifications} ADD COLUMN extra text NOT NULL"), "prevents inserts"),
            (format!("DROP TABLE {revocations}"), "does not exist"),
        ];
        for (change, expected) in cases {
            let catalog = psql(&schema.url, &format!("BEGIN; {change}; {query}; ROLLBACK"));
            let error = check_catalog(&schema.name, &catalog).unwrap_err();
            assert_eq!(error.code, ErrorCode::BadHandle);
            assert!(error.message.contains(expected), "{change}: {error}");
        }
        let compatible = psql(&schema.url, &format!("BEGIN; ALTER TABLE {notifications} ADD COLUMN nullable_extra text; ALTER TABLE {notifications} ADD COLUMN defaulted_extra text NOT NULL DEFAULT ''; {query}; ROLLBACK"));
        check_catalog(&schema.name, &compatible).unwrap();
    }
}
