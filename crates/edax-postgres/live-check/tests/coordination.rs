//! Lease writes and saga conflicts against a real PostgreSQL server.

mod common;

use std::collections::BTreeMap;

use common::{bind, connect, provision};
use edax_event::{CommandMessage, EventDraft};
use edax_postgres::rows::{append_params, commands_param, enqueue_params};
use edax_postgres::{Logical, Statements, Tables};
use postgres::Client;

/// The command a step of saga `ship` sends for `order-7`, as the kernel lays it out.
fn command() -> CommandMessage {
    CommandMessage {
        id: "cmd-invoice-7".into(),
        idempotency_key: Some("req-7".into()),
        aggregate_id: Some("invoice-7".into()),
        tenant: Some("acme".into()),
        subject: None,
        encryption: None,
        name: "SendInvoice".into(),
        version: 1,
        data: b"{}".to_vec(),
        correlation_id: "corr-7".into(),
        causation_id: "invoice-issued-7".into(),
        issued_at: 1_700_000_000_000_000_000,
        extensions: BTreeMap::new(),
        metadata: BTreeMap::from([("origin".into(), "web".into())]),
    }
}

/// The first event of stream `acme:invoice-7`.
fn draft() -> EventDraft {
    EventDraft {
        id: "evt-1".into(),
        aggregate_id: "invoice-7".into(),
        tenant: Some("acme".into()),
        subject: None,
        encryption: None,
        sequence: Some(1),
        name: "InvoiceIssued".into(),
        version: 2,
        data: br#"{"total":"9.50"}"#.to_vec(),
        correlation_id: "corr-1".into(),
        causation_id: "cmd-1".into(),
        occurred_at: Some(1_700_000_000_123_456_000),
        extensions: BTreeMap::from([("traceparent".into(), "00-4bf9-00f0-01".into())]),
        metadata: BTreeMap::from([("origin".into(), "web".into())]),
    }
}

/// One waiting command in the command outbox.
fn enqueue(client: &mut Client, sql: &Statements) {
    let params = enqueue_params(&[command()]).unwrap();
    client.execute(&sql.command_enqueue, &bind(&params)).unwrap();
}

/// Wait until the backend `pid` waits on a lock another session holds.
fn wait_until_blocked(client: &mut Client, pid: i32, who: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let blocked: bool = client
            .query_one("SELECT cardinality(pg_blocking_pids($1)) > 0", &[&pid])
            .unwrap()
            .get(0);
        if blocked {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{who} did not reach the lock");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn relay_lease_writes_only_for_acquisition_renewal_or_takeover() {
    let Some(mut client) = connect() else { return };
    let tables = Tables::from_json(r#"{"schema":"edax_lease_check"}"#).unwrap();
    let sql = provision(&mut client, &tables);
    // A real outbox row proves the fresh holder still reads while every nonholder is excluded.
    enqueue(&mut client, &sql);
    let leases = tables.qualified(Logical::RelayLeases);
    for (name, owner, age, writes, rows) in [
        ("acquire", "worker-a", None, true, 1),
        ("fresh_holder", "worker-a", None, false, 1),
        ("nonholder", "worker-b", None, false, 0),
        ("renew_below_half", "worker-a", Some("20 seconds"), true, 1),
        ("expired_takeover", "worker-b", Some("-1 second"), true, 1),
    ] {
        if let Some(age) = age {
            client
                .execute(
                    &format!("UPDATE {leases} SET expires_at = clock_timestamp() + $1::text::interval"),
                    &[&age],
                )
                .unwrap();
        }
        let before: Option<String> = client
            .query_opt(&format!("SELECT xmin::text FROM {leases}"), &[])
            .unwrap()
            .map(|r| r.get(0));
        let mut tx = client.transaction().unwrap();
        assert_eq!(
            tx.query(&sql.command_unpublished_leased, &[&"10", &owner, &"60000"])
                .unwrap()
                .len(),
            rows,
            "{name}"
        );
        let txid: Option<String> = tx
            .query_one("SELECT pg_current_xact_id_if_assigned()::text", &[])
            .unwrap()
            .get(0);
        assert_eq!(txid.is_some(), writes, "{name}: transaction ID assignment");
        let after: String = tx
            .query_one(&format!("SELECT xmin::text FROM {leases}"), &[])
            .unwrap()
            .get(0);
        assert_eq!(before.as_ref() != Some(&after), writes, "{name}: row version");
        tx.commit().unwrap();
    }
    client.batch_execute("DROP SCHEMA edax_lease_check CASCADE").unwrap();
}

/// Two relays race for one lease: worker-a's leased read sits in an open transaction while worker-b's
/// waits on it. Once worker-a commits, the conflict fence keeps worker-a's lease, and worker-b reads
/// nothing, whether the lease was free or had lapsed.
#[test]
fn racing_relays_leave_the_lease_with_the_first_writer() {
    let Some(mut client) = connect() else { return };
    let tables = Tables::from_json(r#"{"schema":"edax_lease_race_check"}"#).unwrap();
    let sql = provision(&mut client, &tables);
    let leases = tables.qualified(Logical::RelayLeases);
    enqueue(&mut client, &sql);
    for (name, lapsed_holder) in [("free", None), ("lapsed", Some("worker-z"))] {
        client.batch_execute(&format!("DELETE FROM {leases}")).unwrap();
        if let Some(holder) = lapsed_holder {
            client
                .query(&sql.command_unpublished_leased, &[&"10", &holder, &"60000"])
                .unwrap();
            client
                .batch_execute(&format!(
                    "UPDATE {leases} SET expires_at = clock_timestamp() - interval '1 second'"
                ))
                .unwrap();
        }
        let mut first = connect().unwrap();
        let mut tx = first.transaction().unwrap();
        let rows = tx
            .query(&sql.command_unpublished_leased, &[&"10", &"worker-a", &"60000"])
            .unwrap();
        assert_eq!(rows.len(), 1, "{name}: worker-a takes the lease");
        let (sender, receiver) = std::sync::mpsc::channel();
        let read = sql.command_unpublished_leased.clone();
        let second = std::thread::spawn(move || {
            let mut client = connect().unwrap();
            client.batch_execute("SET statement_timeout = '5s'").unwrap();
            let pid: i32 = client.query_one("SELECT pg_backend_pid()", &[]).unwrap().get(0);
            sender.send(pid).unwrap();
            client.query(&read, &[&"10", &"worker-b", &"60000"]).unwrap().len()
        });
        wait_until_blocked(&mut client, receiver.recv().unwrap(), "worker-b");
        tx.commit().unwrap();
        assert_eq!(second.join().unwrap(), 0, "{name}: worker-b reads nothing");
        let owner: String = client
            .query_one(&format!("SELECT owner FROM {leases}"), &[])
            .unwrap()
            .get(0);
        assert_eq!(owner, "worker-a", "{name}: the lease stays worker-a's");
    }
    client
        .batch_execute("DROP SCHEMA edax_lease_race_check CASCADE")
        .unwrap();
}

#[test]
fn racing_saga_write_does_not_report_a_stale_stored_version() {
    let Some(mut client) = connect() else { return };
    let tables = Tables::from_json(r#"{"schema":"edax_saga_race_check"}"#).unwrap();
    let sql = provision(&mut client, &tables);
    client
        .query_one(
            &sql.saga_insert,
            &[&"ship", &"order-7", &r#"{"step":"allocated"}"#, &"", &"[]", &"false"],
        )
        .unwrap();
    let mut winner = connect().unwrap();
    let mut tx = winner.transaction().unwrap();
    tx.query_one(
        &sql.saga_update,
        &[&"ship", &"order-7", &"1", &r#"{"step":"packed"}"#, &"", &"[]", &"false"],
    )
    .unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let update = sql.saga_update.clone();
    let loser = std::thread::spawn(move || {
        let mut client = connect().unwrap();
        client.batch_execute("SET statement_timeout = '5s'").unwrap();
        let pid: i32 = client.query_one("SELECT pg_backend_pid()", &[]).unwrap().get(0);
        sender.send(pid).unwrap();
        client
            .query_one(
                &update,
                &[
                    &"ship",
                    &"order-7",
                    &"1",
                    &r#"{"step":"shipped"}"#,
                    &"",
                    &"[]",
                    &"false",
                ],
            )
            .unwrap()
            .get::<_, String>(0)
    });
    wait_until_blocked(&mut client, receiver.recv().unwrap(), "the loser");
    tx.commit().unwrap();
    let result = loser.join().unwrap();
    let row: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(row["stored"], 1, "the statement snapshot predates the winning commit");
    assert_eq!(
        edax_postgres::rows::saga_result("ship", "order-7", 1, &result)
            .unwrap_err()
            .to_string(),
        "concurrency: saga ship instance order-7 is not at version 1"
    );
    client
        .batch_execute("DROP SCHEMA edax_saga_race_check CASCADE")
        .unwrap();
}

/// A completion stores no state, and that alone marks the instance completed: a write over it finds the
/// tombstone.
#[test]
fn a_saga_completes_when_its_state_is_null() {
    let Some(mut client) = connect() else { return };
    let tables = Tables::from_json(r#"{"schema":"edax_saga_complete_check"}"#).unwrap();
    let sql = provision(&mut client, &tables);
    let write = |client: &mut Client, version: u64, state: Option<&str>| -> String {
        let call = edax_postgres::rows::saga_write("ship", "order-7", version, state, "", &[]).unwrap();
        let text = match call.statement {
            "saga_insert" => &sql.saga_insert,
            _ => &sql.saga_update,
        };
        client.query_one(text, &bind(&call.params)).unwrap().get(0)
    };
    let parsed = |text: String| serde_json::from_str::<serde_json::Value>(&text).unwrap();
    assert_eq!(
        parsed(write(&mut client, 0, Some("{}"))),
        serde_json::json!({"written": 1, "stored": null, "completed": null})
    );
    assert_eq!(
        parsed(write(&mut client, 1, None)),
        serde_json::json!({"written": 2, "stored": 1, "completed": false})
    );
    let refused = write(&mut client, 2, Some("{}"));
    assert_eq!(
        edax_postgres::rows::saga_result("ship", "order-7", 2, &refused)
            .unwrap_err()
            .to_string(),
        "concurrency: saga ship instance order-7 completed at version 2: a completed instance takes no write"
    );
    let completed_at: bool = client
        .query_one(
            &format!(
                "SELECT completed AND completed_at IS NOT NULL AND state IS NULL FROM {}",
                tables.qualified(Logical::SagaInstances)
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(completed_at, "the tombstone");
    client
        .batch_execute("DROP SCHEMA edax_saga_complete_check CASCADE")
        .unwrap();
}

#[test]
fn server_deadlock_timeout_is_checked_in_milliseconds() {
    let Some(mut client) = connect() else { return };
    let sql = edax_postgres::statements(&Tables::default());
    let settings = edax_postgres::Settings::default();
    for (timeout, millis, accepts) in [("1999ms", 1999, true), ("2s", 2000, false), ("3s", 3000, false)] {
        client
            .query_one("SELECT set_config('deadlock_timeout', $1, false)", &[&timeout])
            .unwrap();
        let server = client.query_one(&sql.server, &[]).unwrap();
        assert_eq!(server.get::<_, i64>(2), millis);
        let result = edax_postgres::check_server(server.get(0), server.get(1), server.get(2), &settings);
        if accepts {
            assert_eq!(result, Ok(()));
        } else {
            assert_eq!(result.unwrap_err().to_string(), format!("bad_handle: lock_timeout_ms 2000 must exceed deadlock_timeout_ms {millis}, so deadlocks are detected before lock waits expire"));
        }
    }
}

/// Two stores in one schema whose outboxes have different names share the default `relay_leases`
/// table: a relay draining one outbox must not hold the lease of the other.
#[test]
fn relay_leases_are_per_outbox_table() {
    let Some(mut client) = connect() else { return };
    let schema = "edax_lease_tables_check";
    let first = Tables::from_json(&format!(r#"{{"schema":"{schema}"}}"#)).unwrap();
    let second = Tables::from_json(&format!(
        r#"{{"schema":"{schema}","names":{{"events":"billing_events","event_outbox":"billing_event_outbox","command_outbox":"billing_command_outbox","saga_instances":"billing_saga_instances","saga_outbox":"billing_saga_outbox"}}}}"#
    ))
    .unwrap();
    let first_sql = provision(&mut client, &first);
    for role in [
        Logical::Events,
        Logical::EventOutbox,
        Logical::CommandOutbox,
        Logical::SagaInstances,
        Logical::SagaOutbox,
    ] {
        client
            .batch_execute(&format!(
                "CREATE TABLE {} (LIKE {} INCLUDING ALL)",
                second.qualified(role),
                first.qualified(role)
            ))
            .unwrap();
    }
    let second_sql = edax_postgres::statements(&second);
    let appended = append_params("acme:invoice-7", 0, &[draft()]).unwrap();
    let commands = Some(commands_param(&[command()]).unwrap());
    let saga_insert = [
        Some("ship".to_owned()),
        Some("order-7".to_owned()),
        Some(r#"{"step":"allocated"}"#.to_owned()),
        Some(String::new()),
        commands,
        Some("false".to_owned()),
    ];
    for sql in [&first_sql, &second_sql] {
        client.query_one(&sql.append, &bind(&appended)).unwrap();
        enqueue(&mut client, sql);
        client.query_one(&sql.saga_insert, &bind(&saga_insert)).unwrap();
    }
    for outbox in [Logical::EventOutbox, Logical::CommandOutbox, Logical::SagaOutbox] {
        let name = outbox.as_str();
        assert_eq!(
            leased_read(&mut client, &first_sql, outbox, "relay-a"),
            1,
            "{name}: relay-a takes the first lease"
        );
        assert_eq!(
            leased_read(&mut client, &second_sql, outbox, "relay-b"),
            1,
            "{name}: relay-b takes the second lease"
        );
        assert_eq!(
            leased_read(&mut client, &first_sql, outbox, "relay-b"),
            0,
            "{name}: relay-a still holds the first"
        );
    }
    let held: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {}", first.qualified(Logical::RelayLeases)),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(held, 6, "one lease per outbox table and saga");
    client.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).unwrap();
}

/// The rows relay `owner` reads from `outbox` through its leased read, saga `ship` for the saga outbox.
fn leased_read(client: &mut Client, sql: &Statements, outbox: Logical, owner: &str) -> usize {
    let rows = match outbox {
        Logical::EventOutbox => client.query(&sql.event_unpublished_leased, &[&"10", &owner, &"60000"]),
        Logical::CommandOutbox => client.query(&sql.command_unpublished_leased, &[&"10", &owner, &"60000"]),
        _ => client.query(&sql.saga_unpublished_leased, &[&"ship", &"10", &owner, &"60000"]),
    };
    rows.unwrap().len()
}
