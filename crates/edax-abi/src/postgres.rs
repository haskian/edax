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

impl From<edax_postgres::Settings> for PostgresSettings {
    fn from(s: edax_postgres::Settings) -> Self {
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

/// The kernel's default settings with `overrides_json`, a JSON object of fields by name, applied. An
/// unknown field or a value that is not an integer is a `bad_handle:` error. See
/// [`edax_postgres::Settings::with_overrides`].
pub fn postgres_settings(overrides_json: &str) -> Result<PostgresSettings, String> {
    edax_postgres::Settings::with_overrides(overrides_json)
        .map(Into::into)
        .map_err(to_wire)
}

/// Refuse the settings a store is built with, as a `bad_handle:` error naming the first one refused.
/// See [`edax_postgres::check_config`].
pub fn postgres_check_config(settings: PostgresSettings) -> Result<(), String> {
    edax_postgres::check_config(&settings.into()).map_err(to_wire)
}

/// Refuse a relay lease, as a `bad_handle:` error. See [`edax_postgres::check_lease`].
pub fn postgres_check_lease(owner: &str, ttl_ms: i64) -> Result<(), String> {
    edax_postgres::check_lease(owner, ttl_ms).map_err(to_wire)
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

/// What opens a transaction with `settings`' timeouts, sent as one simple query; settings the kernel refuses
/// are its `bad_handle:` error. See [`edax_postgres::begin`].
pub fn postgres_begin(settings: PostgresSettings) -> Result<String, String> {
    edax_postgres::begin(&settings.into()).map_err(to_wire)
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

/// A statement to run, by its name in the statements, and the JSON text of its parameters.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PostgresCall {
    pub statement: String,
    pub params_json: String,
}

fn call(call: Result<edax_postgres::Call, edax_core::Error>) -> Result<PostgresCall, String> {
    let call = call.map_err(to_wire)?;
    Ok(PostgresCall {
        statement: call.statement.to_owned(),
        params_json: params_json(call.params)?,
    })
}

/// The parameters of `append`. See [`edax_postgres::rows::append_params`].
pub fn postgres_append_params(stream: &str, expected: u64, drafts: Vec<EventDraft>) -> Result<String, String> {
    let drafts: Vec<edax_event::EventDraft> = drafts.into_iter().map(Into::into).collect();
    params_json(edax_postgres::rows::append_params(stream, expected, &drafts).map_err(to_wire)?)
}

/// `drafts`, the batch `append` bound, as the store recorded them, or its `concurrency:` error. See
/// [`edax_postgres::rows::append_result`].
pub fn postgres_append_result(
    expected: u64,
    drafts: Vec<EventDraft>,
    result: &str,
) -> Result<Vec<EventMessage>, String> {
    let drafts: Vec<edax_event::EventDraft> = drafts.into_iter().map(Into::into).collect();
    Ok(edax_postgres::rows::append_result(expected, drafts, result)
        .map_err(to_wire)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// The parameters of `load`. See [`edax_postgres::rows::load_params`].
pub fn postgres_load_params(stream: &str, after: u64) -> Result<String, String> {
    params_json(edax_postgres::rows::load_params(stream, after).map_err(to_wire)?)
}

/// The parameters of `load_snapshot`. See [`edax_postgres::rows::load_snapshot_params`].
pub fn postgres_load_snapshot_params(stream: &str) -> Result<String, String> {
    params_json(edax_postgres::rows::load_snapshot_params(stream).map_err(to_wire)?)
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

/// The parameters of `command_enqueue`. See [`edax_postgres::rows::enqueue_params`].
pub fn postgres_enqueue_params(commands: Vec<CommandMessage>) -> Result<String, String> {
    let commands: Vec<edax_event::CommandMessage> = commands.into_iter().map(Into::into).collect();
    params_json(edax_postgres::rows::enqueue_params(&commands).map_err(to_wire)?)
}

/// The outboxes a relay drains. See [`edax_postgres::Outbox`].
pub fn postgres_outboxes() -> Vec<String> {
    names(edax_postgres::Outbox::NAMES)
}

fn outbox(name: &str) -> Result<edax_postgres::Outbox, String> {
    edax_postgres::Outbox::from_name(name).map_err(to_wire)
}

/// The read of an outbox's oldest rows: with `lease_owner`, the leased read for that relay, whose lease
/// lasts `lease_ttl_ms` (an owner without one is refused). See [`edax_postgres::rows::unpublished`].
pub fn postgres_unpublished(
    outbox_named: &str,
    limit: u64,
    saga: Option<String>,
    lease_owner: Option<String>,
    lease_ttl_ms: Option<i64>,
) -> Result<PostgresCall, String> {
    let lease = lease_owner.as_deref().map(|owner| (owner, lease_ttl_ms.unwrap_or(0)));
    call(edax_postgres::rows::unpublished(
        outbox(outbox_named)?,
        saga.as_deref(),
        limit,
        lease,
    ))
}

/// The delete of an outbox's published rows. See [`edax_postgres::rows::mark_published`].
pub fn postgres_mark_published(
    outbox_named: &str,
    ids: Vec<String>,
    saga: Option<String>,
) -> Result<PostgresCall, String> {
    call(edax_postgres::rows::mark_published(
        outbox(outbox_named)?,
        saga.as_deref(),
        &ids,
    ))
}

/// The count of an outbox's waiting rows. See [`edax_postgres::rows::unpublished_count`].
pub fn postgres_unpublished_count(outbox_named: &str, saga: Option<String>) -> Result<PostgresCall, String> {
    call(edax_postgres::rows::unpublished_count(
        outbox(outbox_named)?,
        saga.as_deref(),
    ))
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

/// The read of a projection row, holding it when `locked`. See [`edax_postgres::rows::projection_get`].
pub fn postgres_projection_get(projection: &str, id: &str, locked: bool) -> Result<PostgresCall, String> {
    call(edax_postgres::rows::projection_get(projection, id, locked))
}

/// The write of a projection document at version `expected`. See [`edax_postgres::rows::projection_save`].
pub fn postgres_projection_save(
    projection: &str,
    id: &str,
    expected: u64,
    document_json: &str,
) -> Result<PostgresCall, String> {
    call(edax_postgres::rows::projection_save(
        projection,
        id,
        expected,
        document_json.as_bytes(),
    ))
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

/// The parameters of `claim_insert`, and of `claim_take_over` before the lapsed owner. See
/// [`edax_postgres::ledger::claim_params`].
pub fn postgres_claim_params(key: &str, owner: &str, payload_hash: u64) -> Result<String, String> {
    params_json(edax_postgres::ledger::claim_params(key, owner, payload_hash).map_err(to_wire)?)
}

/// The parameters of `claim_read`. See [`edax_postgres::ledger::claim_read_params`].
pub fn postgres_claim_read_params(key: &str, claim_ttl_ms: i64) -> Result<String, String> {
    params_json(edax_postgres::ledger::claim_read_params(key, claim_ttl_ms).map_err(to_wire)?)
}

/// The parameters of `ledger_hold` and `ledger_delete`. See [`edax_postgres::ledger::key_params`].
pub fn postgres_ledger_key_params(key: &str) -> Result<String, String> {
    params_json(edax_postgres::ledger::key_params(key).map_err(to_wire)?)
}

/// The parameters of `ledger_settle`. See [`edax_postgres::ledger::settle_params`].
pub fn postgres_settle_params(key: &str, handle: &[u8], record: &str) -> Result<String, String> {
    params_json(edax_postgres::ledger::settle_params(key, handle, record).map_err(to_wire)?)
}

/// What a claim that lost the insert does with the row it read, by name. See
/// [`edax_postgres::ClaimAction`].
pub fn postgres_claim_actions() -> Vec<String> {
    names(edax_postgres::ClaimAction::NAMES)
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

/// What a claim of `key` does with `row`, the JSON row its `attempt`-th `claim_read` returned, `None`
/// when none; past the kernel's last read a row still gone is its `store:` error. See
/// [`edax_postgres::claim_read`].
pub fn postgres_claim_read(key: &str, attempt: u32, row: Option<String>) -> Result<PostgresClaimRead, String> {
    let read = edax_postgres::claim_read(key, ledger_row(row)?.as_ref(), attempt).map_err(to_wire)?;
    let action = read.action().as_str().to_owned();
    let value = match read {
        edax_postgres::ClaimRead::Prior(record) => Some(record),
        edax_postgres::ClaimRead::TakeOver(owner) => Some(owner),
        edax_postgres::ClaimRead::Vanished => None,
    };
    Ok(PostgresClaimRead { action, value })
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

/// The read of a saga instance, holding it when `locked`. See [`edax_postgres::rows::saga_load`].
pub fn postgres_saga_load(saga: &str, id: &str, locked: bool) -> Result<PostgresCall, String> {
    call(edax_postgres::rows::saga_load(saga, id, locked))
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

/// A saga write at `version`: `saga_insert` at 0, else `saga_update`; `state_json` is `None` for a
/// completion. See [`edax_postgres::rows::saga_write`].
pub fn postgres_saga_write(
    saga: &str,
    id: &str,
    version: u64,
    state_json: Option<String>,
    folded: &str,
    commands: Vec<CommandMessage>,
) -> Result<PostgresCall, String> {
    let commands: Vec<edax_event::CommandMessage> = commands.into_iter().map(Into::into).collect();
    call(edax_postgres::rows::saga_write(
        saga,
        id,
        version,
        state_json.as_deref(),
        folded,
        &commands,
    ))
}

/// What a saga write returned: the new version, or its `concurrency:` error. See
/// [`edax_postgres::rows::saga_result`].
pub fn postgres_saga_result(saga: &str, id: &str, expected: u64, result: &str) -> Result<u64, String> {
    edax_postgres::rows::saga_result(saga, id, expected, result).map_err(to_wire)
}

/// SQL and reference DDL for the optional ingestion tables.
pub fn postgres_ingest_statements(config: &str) -> Result<String, String> {
    crate::write_json(&edax_postgres::ingest::statements(config).map_err(crate::to_wire)?)
}

/// Batch recovery classification; record-local isolation requires a confirmed rollback.
pub fn postgres_ingest_recovery(rolled_back: bool, sqlstate: Option<String>) -> String {
    edax_postgres::ingest::recovery(sqlstate.as_deref(), rolled_back).to_owned()
}

/// Statements and host-owned DDL for atomic fold state, receipts, checkpoints and intents.
pub fn postgres_fold_statements(config_json: &str) -> Result<String, String> {
    serde_json::to_string(&edax_postgres::fold::statements(config_json).map_err(to_wire)?)
        .map_err(edax_core::Error::codec)
        .map_err(to_wire)
}

#[cfg(test)]
mod tests {
    use crate::*;

    /// What the boundary adds: parameters cross as JSON text, rows as the kernel's JSON, failures as wire
    /// strings with the kernel's codes.
    #[test]
    fn postgres_surface_crosses_the_kernel_layout() {
        assert_eq!(postgres_check_config(postgres_settings("{}").unwrap()), Ok(()));
        assert!(postgres_settings(r#"{"bogus_ms":1}"#).is_err_and(|e| e.starts_with("bad_handle:")));
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
        let leased = postgres_unpublished(
            "saga",
            u64::MAX,
            Some("ship".into()),
            Some("relay-a".into()),
            Some(1_000),
        )
        .unwrap();
        assert_eq!(leased.statement, "saga_unpublished_leased");
        assert_eq!(
            leased.params_json,
            format!(r#"["ship","{}","relay-a","1000"]"#, i64::MAX)
        );
        assert!(postgres_unpublished("evnt", 1, None, None, None).is_err_and(|e| e.starts_with("bad_handle:")));
        let compiled = postgres_compile_query(public, "accounts", "{}").unwrap();
        assert_eq!(compiled.params_json, r#"["accounts"]"#);
        let page = postgres_page(r#"{"limit":1}"#, vec![r#"{"id":"a"}"#.into(), r#"{"id":"b"}"#.into()]).unwrap();
        assert_eq!(page, r#"{"rows":[{"id":"a"}],"next":null}"#);
        let owner = postgres_new_owner();
        let held = format!(r#"{{"owner":"{owner}","settled_by":null,"record":"x","lapsed":false}}"#);
        assert!(postgres_release_write(Some(held), owner.as_bytes()).unwrap());
        assert_eq!(postgres_claim_read("k", 1, None).unwrap().action, "vanished");
        assert!(postgres_claim_read("k", 3, None).is_err_and(|e| e.starts_with("store:")));
    }
}
