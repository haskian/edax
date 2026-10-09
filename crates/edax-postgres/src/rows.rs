//! How a value becomes the text a statement binds, and how a returned row becomes a value again: the
//! row layout every SDK sharing one database must write and read alike.
//!
//! A message travels as one JSON object per row: `data` as hex (Postgres `bytea` text without line
//! breaks), `extensions` and `metadata` as the JSON text of their sorted map, which the table stores as
//! `text` so it is kept exactly as written. Versions and sequences are `bigint`, so a value past
//! `i64::MAX` is refused ([`crate::check_bigint`]). A string field holding NUL is refused as `codec`
//! before any I/O: Postgres `text` cannot hold it, and a server refusal would abort the caller's
//! transaction.

use edax_core::{Error, ErrorCode};
use edax_event::{
    check_saga_commands, check_snapshot, saga_completed, CommandMessage, EventDraft, EventMessage, Metadata,
};
use serde::{Deserialize, Serialize};

use crate::config::{check_bigint, clamp_bigint};

/// A bound parameter: text, or SQL `NULL`.
pub type Param = Option<String>;

#[derive(Serialize, Deserialize)]
struct EventRow {
    id: String,
    aggregate_id: String,
    tenant: Option<String>,
    sequence: i64,
    name: String,
    version: i64,
    data: String,
    correlation_id: String,
    causation_id: String,
    occurred_at: Option<i64>,
    // The store's own clock writes it; a draft carries none.
    #[serde(default, skip_serializing)]
    recorded_at: i64,
    extensions: String,
    metadata: String,
}

#[derive(Serialize, Deserialize)]
struct CommandRow {
    id: String,
    idempotency_key: Option<String>,
    aggregate_id: Option<String>,
    tenant: Option<String>,
    name: String,
    version: i64,
    data: String,
    correlation_id: String,
    causation_id: String,
    issued_at: i64,
    extensions: String,
    metadata: String,
}

/// The parameters of `append`: the stream, the expected version and the drafts. A batch the kernel's
/// dense-batch rule refuses (`edax_event::check_batch`), a version past `bigint`, or a NUL in a field is
/// a `codec` error, before any I/O.
pub fn append_params(stream: &str, expected: u64, drafts: &[EventDraft]) -> Result<Vec<Param>, Error> {
    let sequences: Vec<Option<u64>> = drafts.iter().map(|d| d.sequence).collect();
    let last = edax_event::check_batch(expected, &sequences)?;
    check_bigint("stream version", last)?;
    let rows = drafts.iter().map(event_row_of_draft).collect::<Result<Vec<_>, _>>()?;
    Ok(vec![
        Some(text("stream", stream)?.to_owned()),
        Some(check_bigint("expected version", expected)?.to_string()),
        Some(json(&rows)?),
    ])
}

/// What `append` returned, `{"stored", "events"}`, read against the call: the stored events, one per
/// draft, or a `concurrency` error naming the stored version when the stream was not at `expected`.
pub fn append_result(expected: u64, drafts: usize, result: &str) -> Result<Vec<EventMessage>, Error> {
    #[derive(Deserialize)]
    struct Appended {
        stored: i64,
        events: Vec<EventRow>,
    }
    let appended: Appended = decode("append result", result)?;
    if u64::try_from(appended.stored) != Ok(expected) {
        return Err(Error::concurrency(expected, appended.stored));
    }
    if appended.events.len() != drafts {
        return Err(Error::store(format_args!(
            "append stored {} events for {drafts} drafts",
            appended.events.len()
        )));
    }
    appended.events.into_iter().map(event_of_row).collect()
}

/// The stored events a read returned, one JSON row each.
pub fn decode_events(rows: &[String]) -> Result<Vec<EventMessage>, Error> {
    rows.iter()
        .map(|row| decode::<EventRow>("event row", row).and_then(event_of_row))
        .collect()
}

/// The commands a read returned, one JSON row each.
pub fn decode_commands(rows: &[String]) -> Result<Vec<CommandMessage>, Error> {
    rows.iter()
        .map(|row| decode::<CommandRow>("command row", row).and_then(command_of_row))
        .collect()
}

/// The parameter holding `commands`, in order, for `command_enqueue` and the saga writes.
pub fn commands_param(commands: &[CommandMessage]) -> Result<String, Error> {
    let rows = commands.iter().map(command_row).collect::<Result<Vec<_>, _>>()?;
    json(&rows)
}

/// The parameter holding the ids a `mark_published` deletes. An id holding NUL names no stored row, so
/// it is left out.
pub fn ids_param(ids: &[String]) -> Result<String, Error> {
    let ids: Vec<&String> = ids.iter().filter(|id| !id.contains('\0')).collect();
    json(&ids)
}

/// A limit, an offset or a load's `after` as `bigint` text: past `i64::MAX` it means "no bound".
pub fn bound_param(value: u64) -> String {
    clamp_bigint(value).to_string()
}

/// A stored snapshot, as `load_snapshot` returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    /// The aggregate type it marshals.
    pub type_name: String,
    /// The stream version it was taken at.
    pub version: u64,
    /// The marshaled aggregate.
    pub data: Vec<u8>,
}

/// The snapshot row `load_snapshot` returned.
pub fn decode_snapshot(row: &str) -> Result<SnapshotRow, Error> {
    #[derive(Deserialize)]
    struct Row {
        type_name: String,
        version: i64,
        data: String,
    }
    let row: Row = decode("snapshot row", row)?;
    Ok(SnapshotRow {
        type_name: row.type_name,
        version: unsigned("snapshot version", row.version)?,
        data: from_hex(&row.data)?,
    })
}

/// The parameters of `save_snapshot`.
pub fn snapshot_params(stream: &str, type_name: &str, version: u64, data: &[u8]) -> Result<Vec<Param>, Error> {
    Ok(vec![
        Some(text("stream", stream)?.to_owned()),
        Some(text("snapshot type", type_name)?.to_owned()),
        Some(check_bigint("snapshot version", version)?.to_string()),
        Some(to_hex(data)),
    ])
}

/// What `save_snapshot` returned, the stream's version, read against the snapshot's: one past the
/// stream is a `codec` error (nothing was stored), as every store refuses it.
pub fn snapshot_result(stream: &str, version: u64, stream_version: i64) -> Result<(), Error> {
    check_snapshot(stream, version, unsigned("stream version", stream_version)?)
}

/// What a projection write returned: the new version, or `None` when the row was not at `expected`, a
/// `concurrency` error.
pub fn projection_result(id: &str, expected: u64, written: Option<i64>) -> Result<u64, Error> {
    match written {
        Some(version) => unsigned("projection version", version),
        None => Err(Error::new(
            ErrorCode::Concurrency,
            format_args!("projection {id} is not at version {expected}"),
        )),
    }
}

/// A saga instance, as `saga_load` returns it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SagaRow {
    /// The instance version.
    pub version: u64,
    /// The state's JSON text, `None` for a completed instance.
    pub state: Option<String>,
    /// The runner's `folded` marks, as written.
    pub folded: String,
    /// Whether the instance completed: a tombstone.
    pub completed: bool,
}

/// The instance row `saga_load` returned.
pub fn decode_saga(row: &str) -> Result<SagaRow, Error> {
    decode("saga row", row)
}

/// The parameters of a saga write: `saga_insert` when `version` is 0, else `saga_update`. `state` is
/// `None` for a completion. The commands must be the kernel's for this write
/// ([`edax_event::check_saga_commands`]), else a `codec` error.
pub fn saga_params(
    saga: &str,
    id: &str,
    version: u64,
    state: Option<&str>,
    folded: &str,
    commands: &[CommandMessage],
) -> Result<Vec<Param>, Error> {
    let next = version
        .checked_add(1)
        .ok_or_else(|| Error::codec(format_args!("saga {saga} instance {id} is at the last version")))?;
    check_bigint("saga version", next)?;
    check_saga_commands(saga, id, next, commands)?;
    let mut params = vec![
        Some(text("saga", saga)?.to_owned()),
        Some(text("saga instance", id)?.to_owned()),
    ];
    if version > 0 {
        params.push(Some(version.to_string()));
    }
    params.push(state.map(str::to_owned));
    params.push(Some(text("folded marks", folded)?.to_owned()));
    params.push(Some(state.is_none().to_string()));
    params.push(Some(commands_param(commands)?));
    Ok(params)
}

/// What a saga write returned, read against the call: the new version, or a `concurrency` error for a
/// completed instance (completion is final) or a version other than `expected`.
pub fn saga_result(saga: &str, id: &str, expected: u64, result: &str) -> Result<u64, Error> {
    #[derive(Deserialize)]
    struct Written {
        written: Option<i64>,
        stored: Option<i64>,
        completed: Option<bool>,
    }
    let written: Written = decode("saga write result", result)?;
    let stored = written.stored.unwrap_or(0);
    match (written.written, written.completed) {
        (Some(version), _) => unsigned("saga version", version),
        (None, Some(true)) => Err(saga_completed(saga, id, unsigned("saga version", stored)?)),
        (None, _) => Err(Error::new(
            ErrorCode::Concurrency,
            format_args!("saga {saga} instance {id} is not at version {expected}"),
        )),
    }
}

fn event_row_of_draft(draft: &EventDraft) -> Result<EventRow, Error> {
    let sequence = draft
        .sequence
        .ok_or_else(|| Error::codec("an appended draft carries no sequence"))?;
    Ok(EventRow {
        id: text("event id", &draft.id)?.to_owned(),
        aggregate_id: text("aggregate id", &draft.aggregate_id)?.to_owned(),
        tenant: draft
            .tenant
            .as_deref()
            .map(|t| text("tenant", t).map(str::to_owned))
            .transpose()?,
        sequence: check_bigint("sequence", sequence)?,
        name: text("event name", &draft.name)?.to_owned(),
        version: i64::from(draft.version),
        data: to_hex(&draft.data),
        correlation_id: text("correlation id", &draft.correlation_id)?.to_owned(),
        causation_id: text("causation id", &draft.causation_id)?.to_owned(),
        occurred_at: draft.occurred_at,
        recorded_at: 0,
        extensions: json(&draft.extensions)?,
        metadata: json(&draft.metadata)?,
    })
}

fn event_of_row(row: EventRow) -> Result<EventMessage, Error> {
    Ok(EventMessage {
        id: row.id,
        aggregate_id: row.aggregate_id,
        tenant: row.tenant,
        sequence: Some(unsigned("sequence", row.sequence)?),
        name: row.name,
        version: payload_version(row.version)?,
        data: from_hex(&row.data)?,
        correlation_id: row.correlation_id,
        causation_id: row.causation_id,
        occurred_at: row.occurred_at,
        recorded_at: row.recorded_at,
        extensions: map("extensions", &row.extensions)?,
        metadata: map("metadata", &row.metadata)?,
    })
}

fn command_row(command: &CommandMessage) -> Result<CommandRow, Error> {
    let optional =
        |what: &str, value: &Option<String>| value.as_deref().map(|v| text(what, v).map(str::to_owned)).transpose();
    Ok(CommandRow {
        id: text("command id", &command.id)?.to_owned(),
        idempotency_key: optional("idempotency key", &command.idempotency_key)?,
        aggregate_id: optional("aggregate id", &command.aggregate_id)?,
        tenant: optional("tenant", &command.tenant)?,
        name: text("command name", &command.name)?.to_owned(),
        version: i64::from(command.version),
        data: to_hex(&command.data),
        correlation_id: text("correlation id", &command.correlation_id)?.to_owned(),
        causation_id: text("causation id", &command.causation_id)?.to_owned(),
        issued_at: command.issued_at,
        extensions: json(&command.extensions)?,
        metadata: json(&command.metadata)?,
    })
}

fn command_of_row(row: CommandRow) -> Result<CommandMessage, Error> {
    Ok(CommandMessage {
        id: row.id,
        idempotency_key: row.idempotency_key,
        aggregate_id: row.aggregate_id,
        tenant: row.tenant,
        name: row.name,
        version: payload_version(row.version)?,
        data: from_hex(&row.data)?,
        correlation_id: row.correlation_id,
        causation_id: row.causation_id,
        issued_at: row.issued_at,
        extensions: map("extensions", &row.extensions)?,
        metadata: map("metadata", &row.metadata)?,
    })
}

/// `value`, refused as `codec` when it holds NUL.
pub(crate) fn text<'a>(what: &str, value: &'a str) -> Result<&'a str, Error> {
    if value.contains('\0') {
        return Err(Error::codec(format_args!(
            "{what} holds a NUL character, which Postgres cannot store"
        )));
    }
    Ok(value)
}

fn json<T: Serialize + ?Sized>(value: &T) -> Result<String, Error> {
    serde_json::to_string(value).map_err(Error::codec)
}

fn decode<'a, T: Deserialize<'a>>(what: &str, row: &'a str) -> Result<T, Error> {
    serde_json::from_str(row).map_err(|e| Error::codec(format_args!("{what} is not the stored layout: {e}")))
}

fn map(what: &str, text: &str) -> Result<Metadata, Error> {
    serde_json::from_str(text).map_err(|e| Error::codec(format_args!("stored {what} are not a string map: {e}")))
}

fn unsigned(what: &str, value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::codec(format_args!("stored {what} {value} is negative")))
}

fn payload_version(value: i64) -> Result<u32, Error> {
    u32::try_from(value).map_err(|_| Error::codec(format_args!("stored payload version {value} is not a u32")))
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(text: &str) -> Result<Vec<u8>, Error> {
    let bad = || Error::codec("stored bytes are not hex");
    if !text.len().is_multiple_of(2) {
        return Err(bad());
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(bad)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn draft(sequence: u64) -> EventDraft {
        EventDraft {
            id: format!("evt-{sequence}"),
            aggregate_id: "invoice-7".into(),
            tenant: Some("acme".into()),
            sequence: Some(sequence),
            name: "InvoiceIssued".into(),
            version: 2,
            data: vec![0, 1, 254, 255],
            correlation_id: "corr-1".into(),
            causation_id: "cause-1".into(),
            occurred_at: Some(1_700_000_000_000_000_000),
            extensions: BTreeMap::from([("traceparent".into(), "00-ab".into())]),
            metadata: BTreeMap::from([("note".into(), "a\u{0}b".into())]),
        }
    }

    #[test]
    fn append_round_trip() {
        let params = append_params("acme:invoice-7", 0, &[draft(1), draft(2)]).unwrap();
        assert_eq!(params[0].as_deref(), Some("acme:invoice-7"));
        assert_eq!(params[1].as_deref(), Some("0"));
        let rows: Vec<serde_json::Value> = serde_json::from_str(params[2].as_deref().unwrap()).unwrap();
        assert_eq!(rows[0]["data"], "0001feff");
        assert_eq!(
            rows[0]["metadata"], r#"{"note":"a\u0000b"}"#,
            "map NUL survives as JSON text"
        );
        let mut stored = rows.clone();
        stored[0]["recorded_at"] = 5.into();
        stored[1]["recorded_at"] = 5.into();
        let result = serde_json::json!({"stored": 0, "events": stored}).to_string();
        let events = append_result(0, 2, &result).unwrap();
        assert_eq!(events[0], draft(1).record(5));
        assert_eq!(events[1], draft(2).record(5));
    }

    #[test]
    fn append_refusals() {
        let err = |r: Result<Vec<Param>, Error>| r.map_err(|e| e.to_string()).err();
        assert!(
            err(append_params("s", 0, &[draft(2)])).unwrap().starts_with("codec:"),
            "gap"
        );
        let mut nul = draft(1);
        nul.id = "e\0".into();
        assert_eq!(
            err(append_params("s", 0, &[nul])).as_deref(),
            Some("codec: event id holds a NUL character, which Postgres cannot store")
        );
        assert!(err(append_params("s", i64::MAX as u64, &[draft(i64::MAX as u64 + 1)])).is_some());
        assert_eq!(
            append_result(3, 1, r#"{"stored":4,"events":[]}"#).map_err(|e| e.to_string()),
            Err("concurrency: expected 3, stored 4".into())
        );
        assert_eq!(
            append_result(4, 0, r#"{"stored":4,"events":[]}"#),
            Ok(vec![]),
            "empty batch checks the version"
        );
    }

    #[test]
    fn snapshot_and_projection_results() {
        assert_eq!(snapshot_result("s", 3, 3), Ok(()));
        assert!(snapshot_result("s", 4, 3).is_err());
        assert_eq!(projection_result("p", 0, Some(1)), Ok(1));
        assert_eq!(
            projection_result("p", 2, None).map_err(|e| e.to_string()),
            Err("concurrency: projection p is not at version 2".into())
        );
        let row = decode_snapshot(r#"{"type_name":"Invoice","version":3,"data":"00ff"}"#).unwrap();
        assert_eq!(row.data, vec![0, 255]);
        assert!(from_hex("0").is_err() && from_hex("zz").is_err());
    }

    #[test]
    fn saga_results() {
        assert_eq!(
            saga_result("s", "i", 1, r#"{"written":2,"stored":1,"completed":false}"#),
            Ok(2)
        );
        assert_eq!(
            saga_result("s", "i", 1, r#"{"written":null,"stored":2,"completed":true}"#).map_err(|e| e.to_string()),
            Err("concurrency: saga s instance i completed at version 2: a completed instance takes no write".into())
        );
        assert_eq!(
            saga_result("s", "i", 0, r#"{"written":null,"stored":1,"completed":false}"#).map_err(|e| e.to_string()),
            Err("concurrency: saga s instance i is not at version 0".into())
        );
        let params = saga_params("ship", "order-1", 0, Some("{}"), "", &[]).unwrap();
        assert_eq!(params.len(), 6, "an insert has no expected version");
        assert_eq!(params[4].as_deref(), Some("false"));
        let done = saga_params("ship", "order-1", 2, None, "", &[]).unwrap();
        assert_eq!(done.len(), 7);
        assert_eq!(done[3], None, "a completion stores no state");
    }
}
