//! How a value becomes the text a statement binds, and how a returned row becomes a value again: the
//! row layout every SDK sharing one database must write and read alike.
//!
//! A message travels as one JSON object per row: `data` as hex (Postgres `bytea` text without line
//! breaks), `extensions`, `metadata` and `encryption` as JSON objects, which the table stores as `jsonb`
//! so a host can query and index them. Versions and sequences are `bigint`, so a value past `i64::MAX`
//! is refused (`config::check_bigint`). Text holding NUL is refused as `codec` before any I/O, by the
//! kernel's rule ([`edax_core::check_text`], through [`edax_event::Message::check`] for a message): a
//! server refusal would abort the caller's transaction.

use edax_core::{Error, ErrorCode};
use edax_event::{
    check_saga_commands, check_snapshot, saga_completed, CommandMessage, EventDraft, EventMessage, Extensions, Message,
    Metadata,
};
use edax_seal::EncryptionInfo;
use serde::{Deserialize, Serialize};

use crate::config::{check_bigint, check_lease, clamp_bigint};
use crate::documents::stored_document;

/// A bound parameter: text, or SQL `NULL`.
pub type Param = Option<String>;

/// A statement to run, by its name in [`crate::Statements`], and the parameters it binds: the answer of a
/// write or a read whose statement turns on its input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Call {
    /// The statement's name, a field of [`crate::Statements`].
    pub statement: &'static str,
    /// The parameters, `$1` first.
    pub params: Vec<Param>,
}

edax_core::closed_names! {
    /// An outbox a relay drains.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Outbox {
        /// The event store's outbox.
        Event = "event",
        /// The command outbox.
        Command = "command",
        /// The saga store's outbox, read one saga at a time.
        Saga = "saga",
    }
}

impl Outbox {
    /// The outbox named `name`, or a `bad_handle` error.
    pub fn from_name(name: &str) -> Result<Outbox, Error> {
        Outbox::by_name(name).ok_or_else(|| Error::bad_handle(format_args!("unknown postgres outbox: {name}")))
    }

    /// The parameters that keep one scope of this outbox's rows: the saga for the saga outbox, which takes
    /// one, and nothing for the others, which take none. A scope that does not fit is a `bad_handle` error.
    fn scope(self, saga: Option<&str>) -> Result<Vec<Param>, Error> {
        match (self, saga) {
            (Outbox::Saga, Some(saga)) => Ok(vec![text_param("saga", saga)?]),
            (Outbox::Saga, None) => Err(Error::bad_handle("the saga outbox is read one saga at a time")),
            (_, None) => Ok(Vec::new()),
            (outbox, Some(_)) => Err(Error::bad_handle(format_args!(
                "the {} outbox has no saga scope",
                outbox.as_str()
            ))),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct EventRow {
    id: String,
    aggregate_id: String,
    tenant: Option<String>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    encryption: Option<EncryptionInfo>,
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
    extensions: Extensions,
    metadata: Metadata,
}

#[derive(Serialize, Deserialize)]
struct CommandRow {
    id: String,
    idempotency_key: Option<String>,
    aggregate_id: Option<String>,
    tenant: Option<String>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    encryption: Option<EncryptionInfo>,
    name: String,
    version: i64,
    data: String,
    correlation_id: String,
    causation_id: String,
    issued_at: i64,
    extensions: Extensions,
    metadata: Metadata,
}

/// The parameters of `append`: the stream, the expected version and the drafts. A batch the kernel's
/// dense-batch rule refuses (`edax_event::check_batch`), a version past `bigint`, or a draft
/// [`EventDraft::check`] refuses is a `codec` error, before any I/O.
pub fn append_params(stream: &str, expected: u64, drafts: &[EventDraft]) -> Result<Vec<Param>, Error> {
    let sequences: Vec<Option<u64>> = drafts.iter().map(|d| d.sequence).collect();
    let last = edax_event::check_batch(expected, &sequences)?;
    check_bigint("stream version", last)?;
    let rows = drafts.iter().map(event_row_of_draft).collect::<Result<Vec<_>, _>>()?;
    Ok(vec![
        text_param("stream", stream)?,
        Some(check_bigint("expected version", expected)?.to_string()),
        Some(json(&rows)?),
    ])
}

/// What `append` returned, `{"stored", "recorded_at"}`, read against the call: `drafts`, the batch it
/// bound, as the store recorded them at its clock reading, or a `concurrency` error naming the stored
/// version when the stream was not at `expected`. A stream at `expected` stores every draft or none: the
/// unique key on `(stream, sequence)` fails the whole statement. The statement returns no event, because
/// every stored field but the clock reading is the draft's own.
pub fn append_result(expected: u64, drafts: Vec<EventDraft>, result: &str) -> Result<Vec<EventMessage>, Error> {
    #[derive(Deserialize)]
    struct Appended {
        stored: i64,
        recorded_at: i64,
    }
    let appended: Appended = decode("append result", result)?;
    if u64::try_from(appended.stored) != Ok(expected) {
        return Err(Error::concurrency(expected, appended.stored));
    }
    Ok(drafts
        .into_iter()
        .map(|draft| draft.record(appended.recorded_at))
        .collect())
}

/// The parameters of `load`: the stream and the sequence to read after. An `after` past `bigint` means
/// "no bound".
pub fn load_params(stream: &str, after: u64) -> Result<Vec<Param>, Error> {
    Ok(vec![text_param("stream", stream)?, bound(after)])
}

/// The parameters of `load_snapshot`: the stream.
pub fn load_snapshot_params(stream: &str) -> Result<Vec<Param>, Error> {
    Ok(vec![text_param("stream", stream)?])
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

/// The parameter holding `commands`, in order, for `command_enqueue` and the saga writes. It binds them as
/// they are: [`enqueue_params`] and [`saga_write`] check them first ([`Message::check`]).
pub fn commands_param(commands: &[CommandMessage]) -> Result<String, Error> {
    let rows: Vec<CommandRow> = commands.iter().map(command_row).collect();
    json(&rows)
}

/// The parameters of `command_enqueue`: the commands. A command [`Message::check`] refuses is a `codec`
/// error, before any I/O.
pub fn enqueue_params(commands: &[CommandMessage]) -> Result<Vec<Param>, Error> {
    commands.iter().try_for_each(Message::check)?;
    Ok(vec![Some(commands_param(commands)?)])
}

/// The read of `outbox`'s oldest `limit` rows, of the saga `saga` for the saga outbox. With `lease`, the
/// relay's owner and the lease's length in milliseconds, the leased read, which reads rows only for the
/// lease's holder. A limit past `bigint` means "no bound"; a lease [`check_lease`] refuses is its error.
pub fn unpublished(outbox: Outbox, saga: Option<&str>, limit: u64, lease: Option<(&str, i64)>) -> Result<Call, Error> {
    let mut params = outbox.scope(saga)?;
    params.push(bound(limit));
    let (unleased, leased) = match outbox {
        Outbox::Event => ("event_unpublished", "event_unpublished_leased"),
        Outbox::Command => ("command_unpublished", "command_unpublished_leased"),
        Outbox::Saga => ("saga_unpublished", "saga_unpublished_leased"),
    };
    let Some((owner, ttl_ms)) = lease else {
        return Ok(Call {
            statement: unleased,
            params,
        });
    };
    check_lease(owner, ttl_ms)?;
    params.extend([Some(owner.to_owned()), Some(ttl_ms.to_string())]);
    Ok(Call {
        statement: leased,
        params,
    })
}

/// The delete of the rows of `ids` from `outbox`, of the saga `saga` for the saga outbox. An id holding NUL
/// names no stored row, so it is left out.
pub fn mark_published(outbox: Outbox, saga: Option<&str>, ids: &[String]) -> Result<Call, Error> {
    let mut params = outbox.scope(saga)?;
    let ids: Vec<&String> = ids.iter().filter(|id| !id.contains('\0')).collect();
    params.push(Some(json(&ids)?));
    let statement = match outbox {
        Outbox::Event => "event_mark_published",
        Outbox::Command => "command_mark_published",
        Outbox::Saga => "saga_mark_published",
    };
    Ok(Call { statement, params })
}

/// The count of `outbox`'s waiting rows, of the saga `saga` for the saga outbox.
pub fn unpublished_count(outbox: Outbox, saga: Option<&str>) -> Result<Call, Error> {
    let statement = match outbox {
        Outbox::Event => "event_unpublished_count",
        Outbox::Command => "command_unpublished_count",
        Outbox::Saga => "saga_unpublished_count",
    };
    Ok(Call {
        statement,
        params: outbox.scope(saga)?,
    })
}

/// A stored snapshot, as `load_snapshot` returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    /// Persisted tenant.
    pub tenant: Option<String>,
    /// Immutable state owner.
    pub subject: Option<String>,
    /// State encryption information.
    pub encryption: Option<EncryptionInfo>,
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
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default)]
        subject: Option<String>,
        #[serde(default)]
        encryption: Option<EncryptionInfo>,
    }
    let row: Row = decode("snapshot row", row)?;
    Ok(SnapshotRow {
        tenant: row.tenant,
        subject: row.subject,
        encryption: row.encryption,
        type_name: row.type_name,
        version: unsigned("snapshot version", row.version)?,
        data: from_hex(&row.data)?,
    })
}

/// The parameters of `save_snapshot`.
pub fn snapshot_params(stream: &str, type_name: &str, version: u64, data: &[u8]) -> Result<Vec<Param>, Error> {
    protected_snapshot_params(stream, type_name, version, data, None, None, None)
}

/// Persist all protected snapshot fields. The application provisions the expanded schema first.
#[allow(clippy::too_many_arguments)]
pub fn protected_snapshot_params(
    stream: &str,
    type_name: &str,
    version: u64,
    data: &[u8],
    tenant: Option<&str>,
    subject: Option<&str>,
    encryption: Option<&EncryptionInfo>,
) -> Result<Vec<Param>, Error> {
    edax_event::sealing::check_protection(subject, encryption, data)?;
    Ok(vec![
        text_param("stream", stream)?,
        text_param("snapshot type", type_name)?,
        Some(check_bigint("snapshot version", version)?.to_string()),
        Some(to_hex(data)),
        optional_text("tenant", tenant)?,
        optional_text("subject", subject)?,
        encryption.map(json).transpose()?,
    ])
}

/// What `save_snapshot` returned, the stream's version, read against the snapshot's: one past the
/// stream is a `codec` error (nothing was stored), as every store refuses it.
pub fn snapshot_result(stream: &str, version: u64, stream_version: i64) -> Result<(), Error> {
    check_snapshot(stream, version, unsigned("stream version", stream_version)?)
}

/// The read of projection `projection`'s row `id`: `projection_get`, or with `locked` the read that holds
/// the row until the transaction ends.
pub fn projection_get(projection: &str, id: &str, locked: bool) -> Result<Call, Error> {
    Ok(Call {
        statement: if locked {
            "projection_get_locked"
        } else {
            "projection_get"
        },
        params: vec![text_param("projection", projection)?, text_param("projection id", id)?],
    })
}

/// The write of `document`, JSON text, as projection `projection`'s row `id` at version `expected`:
/// `projection_insert` at 0, else `projection_update`. The document is bound as [`stored_document`]
/// stores it, and refused as it refuses it.
pub fn projection_save(projection: &str, id: &str, expected: u64, document: &[u8]) -> Result<Call, Error> {
    let mut params = vec![text_param("projection", projection)?, text_param("projection id", id)?];
    let statement = if expected == 0 {
        "projection_insert"
    } else {
        params.push(Some(check_bigint("expected version", expected)?.to_string()));
        "projection_update"
    };
    params.push(Some(stored_document(id, document)?));
    Ok(Call { statement, params })
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

/// The read of saga `saga`'s instance `id`: `saga_load`, or with `locked` the read that holds the row until
/// the transaction ends.
pub fn saga_load(saga: &str, id: &str, locked: bool) -> Result<Call, Error> {
    Ok(Call {
        statement: if locked { "saga_load_locked" } else { "saga_load" },
        params: vec![text_param("saga", saga)?, text_param("saga instance", id)?],
    })
}

/// A saga write at `version`: `saga_insert` at 0, else `saga_update`. `state` is `None` for a completion.
/// The commands must be the kernel's for this write ([`edax_event::check_saga_commands`]), else a `codec`
/// error.
pub fn saga_write(
    saga: &str,
    id: &str,
    version: u64,
    state: Option<&str>,
    folded: &str,
    commands: &[CommandMessage],
) -> Result<Call, Error> {
    let next = edax_core::next_version(&format!("saga {saga} instance {id}"), version)?;
    check_bigint("saga version", next)?;
    check_saga_commands(saga, id, next, commands)?;
    let mut params = vec![text_param("saga", saga)?, text_param("saga instance", id)?];
    let statement = if version == 0 {
        "saga_insert"
    } else {
        params.push(Some(version.to_string()));
        "saga_update"
    };
    params.extend([
        optional_text("saga state", state)?,
        text_param("folded marks", folded)?,
        Some(commands_param(commands)?),
        Some(
            match state {
                None => true,
                Some(json) => edax_event::sealing::decode_protected_state(json)?.is_some_and(|state| state.completed),
            }
            .to_string(),
        ),
    ]);
    Ok(Call { statement, params })
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
    draft.check()?;
    let sequence = draft
        .sequence
        .ok_or_else(|| Error::codec("an appended draft carries no sequence"))?;
    Ok(EventRow {
        id: draft.id.clone(),
        aggregate_id: draft.aggregate_id.clone(),
        tenant: draft.tenant.clone(),
        subject: draft.subject.clone(),
        encryption: draft.encryption.clone(),
        sequence: check_bigint("sequence", sequence)?,
        name: draft.name.clone(),
        version: i64::from(draft.version),
        data: to_hex(&draft.data),
        correlation_id: draft.correlation_id.clone(),
        causation_id: draft.causation_id.clone(),
        occurred_at: draft.occurred_at,
        recorded_at: 0,
        extensions: draft.extensions.clone(),
        metadata: draft.metadata.clone(),
    })
}

fn event_of_row(row: EventRow) -> Result<EventMessage, Error> {
    Ok(EventMessage {
        id: row.id,
        aggregate_id: row.aggregate_id,
        tenant: row.tenant,
        subject: row.subject,
        encryption: row.encryption,
        sequence: Some(unsigned("sequence", row.sequence)?),
        name: row.name,
        version: payload_version(row.version)?,
        data: from_hex(&row.data)?,
        correlation_id: row.correlation_id,
        causation_id: row.causation_id,
        occurred_at: row.occurred_at,
        recorded_at: row.recorded_at,
        extensions: row.extensions,
        metadata: row.metadata,
    })
}

fn command_row(command: &CommandMessage) -> CommandRow {
    CommandRow {
        id: command.id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        aggregate_id: command.aggregate_id.clone(),
        tenant: command.tenant.clone(),
        subject: command.subject.clone(),
        encryption: command.encryption.clone(),
        name: command.name.clone(),
        version: i64::from(command.version),
        data: to_hex(&command.data),
        correlation_id: command.correlation_id.clone(),
        causation_id: command.causation_id.clone(),
        issued_at: command.issued_at,
        extensions: command.extensions.clone(),
        metadata: command.metadata.clone(),
    }
}

fn command_of_row(row: CommandRow) -> Result<CommandMessage, Error> {
    Ok(CommandMessage {
        id: row.id,
        idempotency_key: row.idempotency_key,
        aggregate_id: row.aggregate_id,
        tenant: row.tenant,
        subject: row.subject,
        encryption: row.encryption,
        name: row.name,
        version: payload_version(row.version)?,
        data: from_hex(&row.data)?,
        correlation_id: row.correlation_id,
        causation_id: row.causation_id,
        issued_at: row.issued_at,
        extensions: row.extensions,
        metadata: row.metadata,
    })
}

/// `value` as a bound parameter, refused as [`edax_core::check_text`] refuses it.
pub(crate) fn text_param(what: &str, value: &str) -> Result<Param, Error> {
    edax_core::check_text(what, value)?;
    Ok(Some(value.to_owned()))
}

/// `value`, when there is one, as [`text_param`] binds it.
fn optional_text(what: &str, value: Option<&str>) -> Result<Param, Error> {
    value.map_or(Ok(None), |value| text_param(what, value))
}

/// A limit, an offset or a load's `after` as `bigint` text: past `i64::MAX` it means "no bound".
fn bound(value: u64) -> Param {
    Some(clamp_bigint(value).to_string())
}

fn json<T: Serialize + ?Sized>(value: &T) -> Result<String, Error> {
    serde_json::to_string(value).map_err(Error::codec)
}

fn decode<'a, T: Deserialize<'a>>(what: &str, row: &'a str) -> Result<T, Error> {
    serde_json::from_str(row).map_err(|e| Error::codec(format_args!("{what} is not the stored layout: {e}")))
}

fn unsigned(what: &str, value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::codec(format_args!("stored {what} {value} is negative")))
}

fn payload_version(value: i64) -> Result<u32, Error> {
    u32::try_from(value).map_err(|_| Error::codec(format_args!("stored payload version {value} is not a u32")))
}

fn to_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for nibble in bytes.iter().flat_map(|byte| [byte >> 4, byte & 0x0f]) {
        // A nibble is below 16, so it always has a hex digit.
        text.extend(char::from_digit(u32::from(nibble), 16));
    }
    text
}

fn from_hex(text: &str) -> Result<Vec<u8>, Error> {
    let bad = || Error::codec("stored bytes are not hex");
    let nibble = |digit: u8| {
        char::from(digit)
            .to_digit(16)
            .and_then(|n| u8::try_from(n).ok())
            .ok_or_else(bad)
    };
    if !text.len().is_multiple_of(2) {
        return Err(bad());
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        if let [high, low] = pair {
            bytes.push(nibble(*high)? << 4 | nibble(*low)?);
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn draft(sequence: u64) -> EventDraft {
        EventDraft {
            subject: None,
            encryption: None,
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
            metadata: BTreeMap::from([("note".into(), "é \"quoted\"".into())]),
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
            rows[0]["metadata"],
            serde_json::json!({"note": "é \"quoted\""}),
            "a map travels as a JSON object, for a jsonb column"
        );
        assert_eq!(rows[0]["encryption"], serde_json::Value::Null);
        let events = append_result(0, vec![draft(1), draft(2)], r#"{"stored":0,"recorded_at":5}"#).unwrap();
        assert_eq!(events, vec![draft(1).record(5), draft(2).record(5)]);
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
            Some("codec: event id holds a NUL character")
        );
        let mut nul = draft(1);
        nul.metadata.insert("t\0".into(), "v".into());
        assert_eq!(
            err(append_params("s", 0, &[nul])).as_deref(),
            Some("codec: metadata holds a NUL character"),
            "the kernel refuses NUL in a map key"
        );
        assert!(err(append_params("s", i64::MAX as u64, &[draft(i64::MAX as u64 + 1)])).is_some());
        assert_eq!(
            append_result(3, vec![draft(4)], r#"{"stored":4,"recorded_at":5}"#).map_err(|e| e.to_string()),
            Err("concurrency: expected 3, stored 4".into())
        );
        assert_eq!(
            append_result(4, vec![], r#"{"stored":4,"recorded_at":5}"#),
            Ok(vec![]),
            "empty batch checks the version"
        );
    }

    /// Encryption information is checked before it is bound, at each call that binds it.
    #[test]
    fn encryption_is_checked_where_it_is_bound() {
        let info = EncryptionInfo {
            format_version: 1,
            key_namespace: "billing".into(),
            key_id: "k\0".into(),
            cipher: edax_seal::default_cipher(),
            nonce: vec![0; 24],
        };
        let refused = Some("codec: key id holds a NUL character".to_owned());
        let sealed = vec![0; edax_seal::TAG_LEN];
        let snapshot = protected_snapshot_params("s", "Invoice", 1, &sealed, None, Some("customer-42"), Some(&info));
        assert_eq!(snapshot.err().map(|e| e.to_string()), refused, "snapshot");
        let event = EventDraft {
            subject: Some("customer-42".into()),
            encryption: Some(info),
            data: sealed,
            ..draft(1)
        };
        assert_eq!(
            append_params("s", 0, &[event]).err().map(|e| e.to_string()),
            refused,
            "append"
        );
    }

    #[test]
    fn decode_refuses_a_map_that_is_not_strings() {
        let params = append_params("s", 0, &[draft(1)]).unwrap();
        let mut row: Vec<serde_json::Value> = serde_json::from_str(params[2].as_deref().unwrap()).unwrap();
        row[0]["recorded_at"] = 5.into();
        assert!(decode_events(&[row[0].to_string()]).is_ok());
        row[0]["metadata"] = serde_json::json!({"n": 1});
        let err = decode_events(&[row[0].to_string()]).unwrap_err().to_string();
        assert!(err.starts_with("codec: event row is not the stored layout"), "{err}");
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
        let insert = saga_write("ship", "order-1", 0, Some("{}"), "", &[]).unwrap();
        assert_eq!(insert.statement, "saga_insert");
        assert_eq!(insert.params.len(), 6, "an insert has no expected version");
        let done = saga_write("ship", "order-1", 2, None, "", &[]).unwrap();
        assert_eq!(done.statement, "saga_update");
        assert_eq!(done.params.len(), 7);
        assert_eq!(done.params[3], None, "a completion stores no state");
        assert!(
            saga_write("ship", "order-1", u64::MAX, None, "", &[]).is_err(),
            "no next version"
        );
    }

    #[test]
    fn hex_round_trip() {
        let bytes: Vec<u8> = (0..=255).collect();
        assert_eq!(&to_hex(&bytes)[..8], "00010203");
        assert_eq!(&to_hex(&bytes)[506..], "fdfeff");
        assert_eq!(from_hex(&to_hex(&bytes)), Ok(bytes));
        assert_eq!(from_hex("0A"), Ok(vec![10]), "upper case reads too");
        assert!(from_hex("é1").is_err(), "a multi-byte character is no digit");
    }

    /// Every call names a statement the kernel renders, and keeps one scope per outbox.
    #[test]
    fn calls_name_rendered_statements() {
        let rendered = serde_json::to_value(crate::statements(&crate::Tables::default())).unwrap();
        let lease = Some(("relay-a", 1_000));
        let mut calls = vec![
            projection_get("p", "i", false),
            projection_get("p", "i", true),
            projection_save("p", "i", 0, b"{}"),
            projection_save("p", "i", 3, b"{}"),
            saga_load("s", "i", false),
            saga_load("s", "i", true),
        ];
        for (outbox, saga) in [
            (Outbox::Event, None),
            (Outbox::Command, None),
            (Outbox::Saga, Some("s")),
        ] {
            calls.extend([
                unpublished(outbox, saga, 5, None),
                unpublished(outbox, saga, 5, lease),
                mark_published(outbox, saga, &[]),
                unpublished_count(outbox, saga),
            ]);
        }
        for call in calls {
            let call = call.unwrap();
            assert!(rendered.get(call.statement).is_some(), "{}", call.statement);
        }
        assert!(unpublished(Outbox::Saga, None, 1, None).is_err());
        assert!(unpublished(Outbox::Event, Some("s"), 1, None).is_err());
        assert!(unpublished(Outbox::Event, None, 1, Some(("", 1))).is_err());
        assert_eq!(
            unpublished(Outbox::Saga, Some("s"), u64::MAX, lease).unwrap().params,
            vec![
                Some("s".into()),
                Some(i64::MAX.to_string()),
                Some("relay-a".into()),
                Some("1000".into())
            ]
        );
        assert_eq!(
            mark_published(Outbox::Event, None, &["a".into(), "b\0".into()])
                .unwrap()
                .params,
            vec![Some(r#"["a"]"#.into())]
        );
    }
}
