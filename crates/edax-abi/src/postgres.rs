//! The `edax-postgres` surface: the settings, the schema, the statements, the row layout, the query
//! compiler, the ledger fence and the verdict on a failure.
//!
//! Parameters cross as the JSON text of an array of strings and nulls, which the host binds in order,
//! and rows cross as the JSON text the statements select, so the layout stays the kernel's.

use crate::{names, to_wire, CommandMessage, EventDraft, EventMessage, SnapshotResult};

/// The settings a Postgres store is built with, in milliseconds. See [`edax_postgres::Settings`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresSettings {
    /// How long a call waits for a pooled connection.
    pub acquire_timeout_ms: i64,
    /// How long a statement waits on a row lock.
    pub lock_timeout_ms: i64,
    /// How long one statement may run.
    pub statement_timeout_ms: i64,
    /// How long the client waits for any one call.
    pub op_timeout_ms: i64,
    /// How long a transaction may sit idle between statements.
    pub idle_in_transaction_ms: i64,
    /// How long a committed pending ledger claim lives before another claimant takes it over.
    pub claim_ttl_ms: i64,
}

impl From<PostgresSettings> for edax_postgres::Settings {
    fn from(s: PostgresSettings) -> Self {
        Self {
            acquire_timeout_ms: s.acquire_timeout_ms,
            lock_timeout_ms: s.lock_timeout_ms,
            statement_timeout_ms: s.statement_timeout_ms,
            op_timeout_ms: s.op_timeout_ms,
            idle_in_transaction_ms: s.idle_in_transaction_ms,
            claim_ttl_ms: s.claim_ttl_ms,
        }
    }
}

/// The kernel's default settings. See [`edax_postgres::config::DEFAULT`].
pub fn postgres_default_settings() -> PostgresSettings {
    let d = edax_postgres::config::DEFAULT;
    PostgresSettings {
        acquire_timeout_ms: d.acquire_timeout_ms,
        lock_timeout_ms: d.lock_timeout_ms,
        statement_timeout_ms: d.statement_timeout_ms,
        op_timeout_ms: d.op_timeout_ms,
        idle_in_transaction_ms: d.idle_in_transaction_ms,
        claim_ttl_ms: d.claim_ttl_ms,
    }
}

/// Refuse the settings a store is built with, as a `bad_handle:` error naming the first one refused.
/// See [`edax_postgres::check_config`].
pub fn postgres_check_config(settings: PostgresSettings) -> Result<(), String> {
    edax_postgres::check_config(&settings.into()).map_err(to_wire)
}

/// How many times a claim reads a key again when the row it lost to vanished before the read. A call
/// because the generator binds functions, not constants. See [`edax_postgres::config::CLAIM_READS`].
pub fn postgres_claim_reads() -> u32 {
    edax_postgres::config::CLAIM_READS
}

fn tables(tables_json: &str) -> Result<edax_postgres::Tables, String> {
    edax_postgres::Tables::from_json(tables_json).map_err(to_wire)
}

/// The tables `tables_json` names, every role resolved (an unnamed one takes its default), as their JSON
/// wire form `{"schema": ..., "names": {...}}`. A name outside the identifier rule, two roles under one
/// name, or an unknown role is a `bad_handle:` error. See [`edax_postgres::Tables`].
pub fn postgres_tables(tables_json: &str) -> Result<String, String> {
    Ok(tables(tables_json)?.to_json())
}

/// The table roles a host names. See [`edax_postgres::Logical`].
pub fn postgres_table_roles() -> Vec<String> {
    names(edax_postgres::Logical::NAMES)
}

/// The version of the table contract this build requires. See [`edax_postgres::CONTRACT_VERSION`].
pub fn postgres_contract_version() -> u32 {
    edax_postgres::CONTRACT_VERSION
}

/// A schema that meets the table contract, for the tables `tables_json` names: a starting point for the
/// host's own migration, never run by edax. See [`edax_postgres::reference_ddl`].
pub fn postgres_reference_ddl(tables_json: &str) -> Result<String, String> {
    Ok(edax_postgres::reference_ddl(&tables(tables_json)?))
}

fn roles(roles: &[String]) -> Result<Vec<edax_postgres::Logical>, String> {
    roles
        .iter()
        .map(|role| edax_postgres::Logical::from_name(role).map_err(to_wire))
        .collect()
}

/// The parameter of the `catalog` statement for the tables of `roles`. See
/// [`edax_postgres::catalog_param`].
pub fn postgres_catalog_param(tables_json: &str, roles_named: Vec<String>) -> Result<String, String> {
    Ok(edax_postgres::catalog_param(
        &tables(tables_json)?,
        &roles(&roles_named)?,
    ))
}

/// Hold the tables of `roles` to the contract, from the `catalog` statement's reply. Every mismatch is
/// listed in one `bad_handle:` error. A compatible database answers, as JSON text, which unique key each
/// unique index guards, by the index's name: `{"unique": {"<index>": "<key>"}}`. See
/// [`edax_postgres::check_catalog`].
pub fn postgres_check_catalog(
    tables_json: &str,
    roles_named: Vec<String>,
    catalog_json: &str,
) -> Result<String, String> {
    let compatibility =
        edax_postgres::check_catalog(&tables(tables_json)?, &roles(&roles_named)?, catalog_json).map_err(to_wire)?;
    serde_json::to_string(&compatibility).map_err(|e| to_wire(edax_core::Error::codec(e)))
}

/// Refuse a server the statements cannot run on. See [`edax_postgres::check_server`].
pub fn postgres_check_server(
    server_encoding: &str,
    server_version_num: i64,
    deadlock_timeout_ms: i64,
    settings: PostgresSettings,
) -> Result<(), String> {
    edax_postgres::check_server(
        server_encoding,
        server_version_num,
        deadlock_timeout_ms,
        &settings.into(),
    )
    .map_err(to_wire)
}

/// Every statement rendered for the tables `tables_json` names, as the JSON text of an object from
/// statement name to SQL. See [`edax_postgres::Statements`].
pub fn postgres_statements(tables_json: &str) -> Result<String, String> {
    serde_json::to_string(&edax_postgres::statements(&tables(tables_json)?))
        .map_err(|e| to_wire(edax_core::Error::codec(e)))
}

/// The operations a server error is read against. See [`edax_postgres::Op`].
pub fn postgres_ops() -> Vec<String> {
    names(edax_postgres::Op::NAMES)
}

/// The failures a client reports without a SQLSTATE. See [`edax_postgres::ClientFailure`].
pub fn postgres_client_failures() -> Vec<String> {
    names(edax_postgres::ClientFailure::NAMES)
}

/// The verdict on a server error, in its wire form. `unique` names the key the violated unique index
/// guards, read from the table check's answer; `None` when none. An unknown `op` or key is a `codec:`
/// error. See [`edax_postgres::verdict`].
pub fn postgres_verdict(op: &str, sqlstate: &str, unique: Option<String>, message: &str) -> Result<String, String> {
    let op = edax_postgres::Op::from_name(op).map_err(to_wire)?;
    let unique = unique
        .map(|key| {
            edax_postgres::UniqueKey::by_name(&key)
                .ok_or_else(|| to_wire(edax_core::Error::codec(format_args!("unknown unique key: {key}"))))
        })
        .transpose()?;
    Ok(to_wire(edax_postgres::verdict(op, sqlstate, unique, message)))
}

/// The verdict on a client-side failure of `kind`, in its wire form. See
/// [`edax_postgres::client_verdict`].
pub fn postgres_client_verdict(op: &str, kind: &str, message: &str) -> Result<String, String> {
    let op = edax_postgres::Op::from_name(op).map_err(to_wire)?;
    Ok(to_wire(edax_postgres::client_verdict(op, kind, message)))
}

fn params_json(params: Vec<Option<String>>) -> Result<String, String> {
    serde_json::to_string(&params).map_err(|e| to_wire(edax_core::Error::codec(e)))
}

/// The parameters of `append`. See [`edax_postgres::rows::append_params`].
pub fn postgres_append_params(stream: &str, expected: u64, drafts: Vec<EventDraft>) -> Result<String, String> {
    let drafts: Vec<edax_event::EventDraft> = drafts.into_iter().map(Into::into).collect();
    params_json(edax_postgres::rows::append_params(stream, expected, &drafts).map_err(to_wire)?)
}

/// The stored events `append` returned, or its `concurrency:` error. See
/// [`edax_postgres::rows::append_result`].
pub fn postgres_append_result(expected: u64, drafts: u64, result: &str) -> Result<Vec<EventMessage>, String> {
    let drafts = usize::try_from(drafts).unwrap_or(usize::MAX);
    Ok(edax_postgres::rows::append_result(expected, drafts, result)
        .map_err(to_wire)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// The events a read returned. See [`edax_postgres::rows::decode_events`].
pub fn postgres_decode_events(rows: Vec<String>) -> Result<Vec<EventMessage>, String> {
    Ok(edax_postgres::rows::decode_events(&rows)
        .map_err(to_wire)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// The commands a read returned. See [`edax_postgres::rows::decode_commands`].
pub fn postgres_decode_commands(rows: Vec<String>) -> Result<Vec<CommandMessage>, String> {
    Ok(edax_postgres::rows::decode_commands(&rows)
        .map_err(to_wire)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// The parameter holding `commands`. See [`edax_postgres::rows::commands_param`].
pub fn postgres_commands_param(commands: Vec<CommandMessage>) -> Result<String, String> {
    let commands: Vec<edax_event::CommandMessage> = commands.into_iter().map(Into::into).collect();
    edax_postgres::rows::commands_param(&commands).map_err(to_wire)
}

/// The parameter holding the ids a `mark_published` deletes. See [`edax_postgres::rows::ids_param`].
pub fn postgres_ids_param(ids: Vec<String>) -> Result<String, String> {
    edax_postgres::rows::ids_param(&ids).map_err(to_wire)
}

/// A limit or a load's `after` as `bigint` text. See [`edax_postgres::rows::bound_param`].
pub fn postgres_bound_param(value: u64) -> String {
    edax_postgres::rows::bound_param(value)
}

/// The parameters of `save_snapshot`. See [`edax_postgres::rows::snapshot_params`].
pub fn postgres_snapshot_params(stream: &str, type_name: &str, version: u64, data: Vec<u8>) -> Result<String, String> {
    params_json(edax_postgres::rows::snapshot_params(stream, type_name, version, &data).map_err(to_wire)?)
}

/// The snapshot `load_snapshot` returned. See [`edax_postgres::rows::decode_snapshot`].
pub fn postgres_decode_snapshot(row: &str) -> Result<SnapshotResult, String> {
    let row = edax_postgres::rows::decode_snapshot(row).map_err(to_wire)?;
    Ok(SnapshotResult {
        type_name: row.type_name,
        version: row.version,
        data: row.data,
    })
}

/// What `save_snapshot` returned, read against the snapshot. See
/// [`edax_postgres::rows::snapshot_result`].
pub fn postgres_snapshot_result(stream: &str, version: u64, stream_version: i64) -> Result<(), String> {
    edax_postgres::rows::snapshot_result(stream, version, stream_version).map_err(to_wire)
}

/// The text projection `id`'s document is stored as. See [`edax_postgres::stored_document`].
pub fn postgres_stored_document(id: &str, document_json: &str) -> Result<String, String> {
    edax_postgres::stored_document(id, document_json.as_bytes()).map_err(to_wire)
}

/// What a projection write returned: the new version, or a `concurrency:` error when none was written.
/// See [`edax_postgres::rows::projection_result`].
pub fn postgres_projection_result(id: &str, expected: u64, written: Option<i64>) -> Result<u64, String> {
    edax_postgres::rows::projection_result(id, expected, written).map_err(to_wire)
}

/// A compiled read-model query: the statement and the JSON text of its parameters.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresQuery {
    pub sql: String,
    pub params_json: String,
}

/// The query `spec` compiled for projection `projection` in the projections table `tables_json` names. See
/// [`edax_postgres::compile`].
pub fn postgres_compile_query(tables_json: &str, projection: &str, spec: &str) -> Result<PostgresQuery, String> {
    let compiled = edax_postgres::compile(&tables(tables_json)?, projection, spec.as_bytes()).map_err(to_wire)?;
    Ok(PostgresQuery {
        sql: compiled.sql,
        params_json: params_json(compiled.params)?,
    })
}

/// The page the rows of a compiled `spec` make, as `{"rows": [...], "next": cursor|null}`. See
/// [`edax_postgres::page`].
pub fn postgres_page(spec: &str, rows: Vec<String>) -> Result<String, String> {
    edax_postgres::page(spec.as_bytes(), rows)
        .and_then(|page| page.to_json())
        .map_err(to_wire)
}

/// A fresh claim owner token, also its handle. See [`edax_postgres::new_owner`].
pub fn postgres_new_owner() -> String {
    edax_postgres::new_owner()
}

/// The parameters of `claim_insert`. See [`edax_postgres::ledger::claim_params`].
pub fn postgres_claim_params(key: &str, owner: &str, payload_hash: u64) -> Result<String, String> {
    params_json(edax_postgres::ledger::claim_params(key, owner, payload_hash).map_err(to_wire)?)
}

/// What a claim that lost the insert does with the row it read: `action` is `prior` (`value` the
/// record), `take_over` (`value` the lapsed owner) or `vanished`.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresClaimRead {
    pub action: String,
    pub value: Option<String>,
}

fn ledger_row(row: Option<String>) -> Result<Option<edax_postgres::LedgerRow>, String> {
    row.as_deref()
        .map(edax_postgres::ledger::decode_row)
        .transpose()
        .map_err(to_wire)
}

/// What a claim does with `row`, the JSON row `claim_read` returned, `None` when none. See
/// [`edax_postgres::claim_read`].
pub fn postgres_claim_read(row: Option<String>) -> Result<PostgresClaimRead, String> {
    let row = ledger_row(row)?;
    Ok(match edax_postgres::claim_read(row.as_ref()) {
        edax_postgres::ClaimRead::Prior(record) => PostgresClaimRead {
            action: "prior".to_owned(),
            value: Some(record),
        },
        edax_postgres::ClaimRead::TakeOver(owner) => PostgresClaimRead {
            action: "take_over".to_owned(),
            value: Some(owner),
        },
        edax_postgres::ClaimRead::Vanished => PostgresClaimRead {
            action: "vanished".to_owned(),
            value: None,
        },
    })
}

/// Whether a `complete` writes `record` over `row`, the JSON row `ledger_hold` locked. See
/// [`edax_postgres::complete_write`].
pub fn postgres_complete_write(key: &str, row: Option<String>, handle: &[u8], record: &str) -> Result<bool, String> {
    edax_postgres::complete_write(key, ledger_row(row)?.as_ref(), handle, record).map_err(to_wire)
}

/// Whether a `release` deletes the key. See [`edax_postgres::release_write`].
pub fn postgres_release_write(row: Option<String>, handle: &[u8]) -> Result<bool, String> {
    Ok(edax_postgres::release_write(ledger_row(row)?.as_ref(), handle))
}

/// Whether a `forget` of `key` deletes `row`. See [`edax_postgres::forget_write`].
pub fn postgres_forget_write(key: &str, row: Option<String>) -> Result<bool, String> {
    edax_postgres::forget_write(key, ledger_row(row)?.as_ref()).map_err(to_wire)
}

/// A saga instance as `saga_load` returns it; `state_json` is `None` for a completed one.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresSagaRow {
    pub version: u64,
    pub state_json: Option<String>,
    pub folded: String,
    pub completed: bool,
}

/// The instance row `saga_load` returned. See [`edax_postgres::rows::decode_saga`].
pub fn postgres_decode_saga(row: &str) -> Result<PostgresSagaRow, String> {
    let row = edax_postgres::rows::decode_saga(row).map_err(to_wire)?;
    Ok(PostgresSagaRow {
        version: row.version,
        state_json: row.state,
        folded: row.folded,
        completed: row.completed,
    })
}

/// The parameters of a saga write: `saga_insert` at version 0, else `saga_update`. See
/// [`edax_postgres::rows::saga_params`].
pub fn postgres_saga_params(
    saga: &str,
    id: &str,
    version: u64,
    state_json: Option<String>,
    folded: &str,
    commands: Vec<CommandMessage>,
) -> Result<String, String> {
    let commands: Vec<edax_event::CommandMessage> = commands.into_iter().map(Into::into).collect();
    params_json(
        edax_postgres::rows::saga_params(saga, id, version, state_json.as_deref(), folded, &commands)
            .map_err(to_wire)?,
    )
}

/// What a saga write returned: the new version, or its `concurrency:` error. See
/// [`edax_postgres::rows::saga_result`].
pub fn postgres_saga_result(saga: &str, id: &str, expected: u64, result: &str) -> Result<u64, String> {
    edax_postgres::rows::saga_result(saga, id, expected, result).map_err(to_wire)
}

#[cfg(test)]
mod tests {
    use crate::*;

    /// What the boundary adds: parameters cross as JSON text, rows as the kernel's JSON, failures as wire
    /// strings with the kernel's codes.
    #[test]
    fn postgres_surface_crosses_the_kernel_layout() {
        assert_eq!(postgres_check_config(postgres_default_settings()), Ok(()));
        let public = r#"{"schema":"public"}"#;
        assert!(postgres_statements(public).unwrap().contains("\"append\""));
        assert!(postgres_statements(r#"{"schema":"Bad"}"#)
            .unwrap_err()
            .starts_with("bad_handle:"));
        assert!(postgres_reference_ddl(public).unwrap().contains("CREATE TABLE"));
        assert!(postgres_check_catalog(public, vec!["ledger".into()], "[]")
            .unwrap_err()
            .starts_with("bad_handle:"));
        assert!(
            postgres_verdict("append", "23505", Some("events_stream_sequence".into()), "x")
                .unwrap()
                .starts_with("concurrency:")
        );
        assert!(postgres_verdict("append", "40001", None, "x")
            .unwrap()
            .starts_with("store:"));
        assert!(postgres_verdict("bogus", "40001", None, "x")
            .unwrap_err()
            .starts_with("codec:"));
        assert_eq!(postgres_bound_param(u64::MAX), i64::MAX.to_string());
        let compiled = postgres_compile_query(public, "accounts", "{}").unwrap();
        assert_eq!(compiled.params_json, r#"["accounts"]"#);
        let page = postgres_page(r#"{"limit":1}"#, vec![r#"{"id":"a"}"#.into(), r#"{"id":"b"}"#.into()]).unwrap();
        assert_eq!(page, r#"{"rows":[{"id":"a"}],"next":null}"#);
        let owner = postgres_new_owner();
        let held = format!(r#"{{"owner":"{owner}","settled_by":null,"record":"x","lapsed":false}}"#);
        assert!(postgres_release_write(Some(held), owner.as_bytes()).unwrap());
        assert_eq!(postgres_claim_read(None).unwrap().action, "vanished");
    }
}
