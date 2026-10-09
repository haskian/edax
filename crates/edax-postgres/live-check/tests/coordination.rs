//! Lease writes and saga conflicts against a real PostgreSQL server.
use edax_postgres::{reference_ddl, statements, Logical, Tables};
use postgres::{Client, NoTls};

fn connect() -> Option<Client> {
    let url = std::env::var("EDAX_POSTGRES_URL").ok()?;
    Some(Client::connect(&url, NoTls).unwrap())
}

#[test]
fn relay_lease_writes_only_for_acquisition_renewal_or_takeover() {
    let Some(mut client) = connect() else { return };
    let tables = Tables::from_json(r#"{"schema":"edax_lease_check"}"#).unwrap();
    client
        .batch_execute("DROP SCHEMA IF EXISTS edax_lease_check CASCADE; CREATE SCHEMA edax_lease_check")
        .unwrap();
    client.batch_execute(&reference_ddl(&tables)).unwrap();
    let sql = statements(&tables);
    // A real outbox row proves the fresh holder still reads while every nonholder is excluded.
    client.execute(&sql.command_enqueue, &[&r#"[{"id":"cmd-invoice-7","aggregate_id":"invoice-7","name":"SendInvoice","version":1,"data":"7b7d","correlation_id":"corr-7","causation_id":"invoice-issued-7","issued_at":1700000000000000000,"extensions":"{}","metadata":"{}"}]"#]).unwrap();
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
            tx.query(&sql.command_unpublished_leased, &[&"10", &"commands", &owner, &"60000"])
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

#[test]
fn racing_saga_write_does_not_report_a_stale_stored_version() {
    let Some(mut client) = connect() else { return };
    let tables = Tables::from_json(r#"{"schema":"edax_saga_race_check"}"#).unwrap();
    client
        .batch_execute("DROP SCHEMA IF EXISTS edax_saga_race_check CASCADE; CREATE SCHEMA edax_saga_race_check")
        .unwrap();
    client.batch_execute(&reference_ddl(&tables)).unwrap();
    let sql = statements(&tables);
    client
        .query_one(
            &sql.saga_insert,
            &[&"ship", &"order-7", &r#"{"step":"allocated"}"#, &"", &"false", &"[]"],
        )
        .unwrap();
    let mut winner = connect().unwrap();
    let mut tx = winner.transaction().unwrap();
    tx.query_one(
        &sql.saga_update,
        &[&"ship", &"order-7", &"1", &r#"{"step":"packed"}"#, &"", &"false", &"[]"],
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
                    &"false",
                    &"[]",
                ],
            )
            .unwrap()
            .get::<_, String>(0)
    });
    let pid = receiver.recv().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let blocked: bool = client
            .query_one("SELECT cardinality(pg_blocking_pids($1)) > 0", &[&pid])
            .unwrap()
            .get(0);
        if blocked {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "loser did not reach the row lock");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
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

#[test]
fn server_deadlock_timeout_is_checked_in_milliseconds() {
    let Some(mut client) = connect() else { return };
    let sql = statements(&Tables::default());
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
