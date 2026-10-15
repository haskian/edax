#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeSet;

use edax_event::erasure::ErasureOperation;
use edax_postgres::erasure::{self, StoredOperation};
use edax_seal::KeyScope;

fn scope(subject: Option<&str>) -> KeyScope {
    KeyScope {
        key_namespace: "production-eu".into(),
        tenant: Some("clinic-7".into()),
        subject: subject.map(str::to_owned),
    }
}

fn operation() -> ErasureOperation {
    ErasureOperation::new(
        "erase-42".into(),
        "request-42".into(),
        scope(Some("patient-42")),
        BTreeSet::from(["patient-search".into()]),
    )
    .unwrap()
}

#[test]
fn scope_locks_are_parent_first_and_erasure_conflicts_with_every_writer() {
    let subject = scope(Some("patient-42"));
    let tenant = scope(None);
    let write = erasure::writer_locks(&subject).unwrap();
    let revoke_subject = erasure::revocation_locks(&subject).unwrap();
    let revoke_tenant = erasure::revocation_locks(&tenant).unwrap();
    assert_eq!(write.len(), 2);
    assert_eq!(write[0].statement, "lock_shared");
    assert_eq!(write[1].statement, "lock_shared");
    assert_eq!(revoke_subject[0], write[0]);
    assert_eq!(revoke_subject[1].params, write[1].params);
    assert_eq!(revoke_subject[1].statement, "lock_exclusive");
    assert_eq!(revoke_tenant.len(), 1);
    assert_eq!(revoke_tenant[0].params, write[0].params);
    assert_eq!(revoke_tenant[0].statement, "lock_exclusive");
    let mut another = subject.clone();
    another.tenant = Some("clinic-8".into());
    assert_ne!(erasure::writer_locks(&another).unwrap()[0].params, write[0].params);
    another = subject.clone();
    another.key_namespace = "staging-eu".into();
    assert_ne!(
        erasure::scope_id(&another).unwrap(),
        erasure::scope_id(&subject).unwrap()
    );
    assert_eq!(
        erasure::guard_params(&subject).unwrap(),
        vec![write[0].params[0].clone(), write[1].params[0].clone()]
    );
}

#[test]
fn sql_keeps_lock_and_revocation_reads_separate_and_notification_atomic() {
    let sql = erasure::statements("privacy").unwrap();
    assert!(sql.lock_shared.contains("pg_advisory_xact_lock_shared"));
    assert!(sql.lock_exclusive.contains("pg_advisory_xact_lock("));
    assert!(!sql.check_revoked.contains("advisory"));
    assert!(sql.check_revoked.contains("transaction_isolation"));
    assert!(sql.update_operation.contains("WITH updated AS"));
    assert!(sql
        .update_operation
        .contains("INSERT INTO \"privacy\".\"edax_erasure_notifications\""));
    assert!(sql.update_operation.contains("version = ($2::text)::bigint"));
    assert!(sql.create_operation.contains("ON CONFLICT DO NOTHING"));
    let ddl = erasure::reference_ddl("privacy").unwrap();
    assert!(ddl.contains("UNIQUE (scope, request_id)"));
    assert!(ddl.contains("scope text COLLATE \"C\" PRIMARY KEY"));
    assert!(erasure::statements("bad-schema; DROP TABLE users").is_err());
}

#[test]
fn optimistic_updates_preserve_identity_acknowledgements_and_stage_order() {
    let prior = StoredOperation {
        version: 1,
        operation: operation(),
    };
    let mut revoked = prior.operation.clone();
    revoked.confirm_revocation().unwrap();
    let params = erasure::update_params(&prior, &revoked).unwrap();
    assert_eq!(params[0].as_deref(), Some("erase-42"));
    assert_eq!(params[1].as_deref(), Some("1"));
    assert_eq!(params[3], None);
    let mut notified = revoked.clone();
    notified.confirm_notification().unwrap();
    assert!(erasure::update_params(&prior, &notified).is_err());
    let prior = StoredOperation {
        version: 2,
        operation: revoked,
    };
    let params = erasure::update_params(&prior, &notified).unwrap();
    let notification: serde_json::Value = serde_json::from_str(params[3].as_deref().unwrap()).unwrap();
    assert_eq!(notification["operation_id"], "erase-42");
    assert_eq!(notification["scope"]["subject"], "patient-42");
    let prior = StoredOperation {
        version: 3,
        operation: notified.clone(),
    };
    notified.acknowledge("patient-search").unwrap();
    assert!(erasure::update_params(&prior, &notified).unwrap()[3].is_none());
    let acknowledged = StoredOperation {
        version: 4,
        operation: notified,
    };
    assert!(erasure::update_params(&acknowledged, &prior.operation).is_err());
    assert!(erasure::update_result(None).is_err());
    assert_eq!(erasure::update_result(Some(5)).unwrap(), 5);
    assert!(erasure::create_params(&prior.operation).is_err());
}

#[test]
fn guard_refuses_revocation_and_old_transaction_snapshots() {
    for (isolation, revoked, allowed) in [
        ("read committed", false, true),
        ("read committed", true, false),
        ("repeatable read", false, false),
        ("serializable", false, false),
    ] {
        let row = serde_json::json!({"isolation": isolation, "revoked": revoked}).to_string();
        assert_eq!(erasure::check_guard(&row).is_ok(), allowed, "{isolation}/{revoked}");
    }
}

#[test]
fn repeated_requests_preserve_the_stored_participant_set_and_identity() {
    let stored = StoredOperation {
        version: 1,
        operation: operation(),
    };
    let retry = ErasureOperation::new(
        "new-candidate-id".into(),
        "request-42".into(),
        scope(Some("patient-42")),
        stored.operation.required_participants().clone(),
    )
    .unwrap();
    erasure::check_request(&stored, &retry).unwrap();
    assert_eq!(stored.operation.operation_id(), "erase-42");
    assert!(erasure::update_params(&stored, &retry).is_err());
    let missing = ErasureOperation::new(
        "new-candidate-id".into(),
        "request-42".into(),
        scope(Some("patient-42")),
        BTreeSet::new(),
    )
    .unwrap();
    assert!(erasure::check_request(&stored, &missing).is_err());
    for version in [0, i64::MAX as u64, u64::MAX] {
        let mut invalid = stored.clone();
        invalid.version = version;
        assert!(erasure::update_params(&invalid, &stored.operation).is_err());
    }
    assert!(erasure::update_result(Some(0)).is_err());
    assert!(erasure::update_result(Some(-1)).is_err());
}

// Opt-in live proof uses psql, so the kernel remains independent of a Postgres client crate.
mod live {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Database {
        url: String,
        schema: String,
        sql: erasure::Statements,
    }

    impl Database {
        fn connect() -> Option<Self> {
            let url = std::env::var("EDAX_POSTGRES_URL").ok()?;
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let schema = format!("erasure_{}_{}", std::process::id(), nonce);
            let db = Self {
                sql: erasure::statements(&schema).unwrap(),
                url,
                schema,
            };
            db.run(&format!(
                "CREATE SCHEMA \"{}\"; {}",
                db.schema,
                erasure::reference_ddl(&db.schema).unwrap()
            ));
            Some(db)
        }

        fn run(&self, sql: &str) -> String {
            let output = Command::new("psql")
                .args(["-XAtq", "-v", "ON_ERROR_STOP=1", &self.url, "-c", sql])
                .output()
                .unwrap();
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        }

        fn fails(&self, sql: &str) {
            let output = Command::new("psql")
                .args(["-XAtq", "-v", "ON_ERROR_STOP=1", &self.url, "-c", sql])
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("lock timeout"));
        }

        fn calls(&self, calls: &[edax_postgres::rows::Call]) -> String {
            calls
                .iter()
                .map(|call| {
                    let sql = match call.statement {
                        "lock_shared" => &self.sql.lock_shared,
                        "lock_exclusive" => &self.sql.lock_exclusive,
                        other => panic!("unexpected lock call {other}"),
                    };
                    format!("{};", bind(sql, &call.params))
                })
                .collect()
        }
    }

    impl Drop for Database {
        fn drop(&mut self) {
            let _ = Command::new("psql")
                .args([
                    "-XAtq",
                    &self.url,
                    "-c",
                    &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.schema),
                ])
                .output();
        }
    }

    fn bind(sql: &str, params: &[Option<String>]) -> String {
        let mut result = sql.to_owned();
        for (index, value) in params.iter().enumerate().rev() {
            let literal = value
                .as_ref()
                .map_or("NULL".to_owned(), |value| format!("'{}'", value.replace('\'', "''")));
            result = result.replace(&format!("${}::text", index + 1), &format!("{literal}::text"));
        }
        result
    }

    struct Session {
        child: Child,
        input: ChildStdin,
        output: BufReader<ChildStdout>,
    }

    impl Session {
        fn new(db: &Database) -> Self {
            let mut child = Command::new("psql")
                .args(["-XAtq", "-v", "ON_ERROR_STOP=1", &db.url])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let input = child.stdin.take().unwrap();
            let output = BufReader::new(child.stdout.take().unwrap());
            Self { child, input, output }
        }

        fn run(&mut self, sql: &str) -> String {
            writeln!(self.input, "{sql}; SELECT 'edax-ready';").unwrap();
            self.input.flush().unwrap();
            let mut result = String::new();
            loop {
                let mut line = String::new();
                assert_ne!(self.output.read_line(&mut line).unwrap(), 0);
                if line.trim() == "edax-ready" {
                    break;
                }
                result.push_str(&line);
            }
            result.trim().to_owned()
        }
    }

    impl Drop for Session {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[test]
    fn transaction_fences_drain_writers_and_block_new_subjects_after_tenant_erasure() {
        let Some(db) = Database::connect() else {
            return;
        };
        let patient = scope(Some("patient-42"));
        let mut writer = Session::new(&db);
        writer.run("BEGIN ISOLATION LEVEL READ COMMITTED");
        writer.run(&db.calls(&erasure::writer_locks(&patient).unwrap()));
        let guard = writer.run(&bind(&db.sql.check_revoked, &erasure::guard_params(&patient).unwrap()));
        erasure::check_guard(&guard).unwrap();

        for erased in [patient.clone(), scope(None)] {
            let locks = db.calls(&erasure::revocation_locks(&erased).unwrap());
            db.fails(&format!("BEGIN; SET LOCAL lock_timeout = '100ms'; {locks} COMMIT"));
        }
        let other = db.calls(&erasure::revocation_locks(&scope(Some("patient-99"))).unwrap());
        db.run(&format!("BEGIN; SET LOCAL lock_timeout = '100ms'; {other} COMMIT"));
        writer.run("COMMIT");

        let tenant = scope(None);
        let locks = db.calls(&erasure::revocation_locks(&tenant).unwrap());
        let revoke = bind(&db.sql.revoke, &[Some(erasure::scope_id(&tenant).unwrap())]);
        db.run(&format!("BEGIN; {locks} {revoke}; COMMIT"));
        for subject in ["patient-42", "never-created"] {
            let scope = scope(Some(subject));
            writer.run("BEGIN ISOLATION LEVEL READ COMMITTED");
            writer.run(&db.calls(&erasure::writer_locks(&scope).unwrap()));
            let row = writer.run(&bind(&db.sql.check_revoked, &erasure::guard_params(&scope).unwrap()));
            assert!(erasure::check_guard(&row).is_err());
            writer.run("ROLLBACK");
        }
    }

    #[test]
    fn operation_cas_and_notification_rollback_commit_together() {
        let Some(db) = Database::connect() else {
            return;
        };
        let original = operation();
        let create = bind(&db.sql.create_operation, &erasure::create_params(&original).unwrap());
        assert_eq!(db.run(&create), "1");
        assert_eq!(db.run(&create), "");
        let load = bind(&db.sql.get_operation, &[Some(original.operation_id().into())]);
        let mut stored = erasure::decode_operation(&db.run(&load)).unwrap();
        let mut revoked = stored.operation.clone();
        revoked.confirm_revocation().unwrap();
        let update = bind(
            &db.sql.update_operation,
            &erasure::update_params(&stored, &revoked).unwrap(),
        );
        assert_eq!(db.run(&update), "2");
        stored = erasure::decode_operation(&db.run(&load)).unwrap();
        let mut notified = stored.operation.clone();
        notified.confirm_notification().unwrap();
        let notify = bind(
            &db.sql.update_operation,
            &erasure::update_params(&stored, &notified).unwrap(),
        );
        let pending = bind(&db.sql.pending_notifications, &[Some("10".into())]);
        db.run(&format!("BEGIN; {notify}; ROLLBACK"));
        assert_eq!(erasure::decode_operation(&db.run(&load)).unwrap(), stored);
        assert!(db.run(&pending).is_empty());
        assert_eq!(db.run(&notify), "3");
        assert_eq!(db.run(&notify), "");
        let notifications: Vec<serde_json::Value> = db
            .run(&pending)
            .lines()
            .map(|row| serde_json::from_str(row).unwrap())
            .collect();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0]["operation_id"], original.operation_id());

        stored = erasure::decode_operation(&db.run(&load)).unwrap();
        let mut acknowledged = stored.operation.clone();
        acknowledged.acknowledge("patient-search").unwrap();
        let acknowledge = bind(
            &db.sql.update_operation,
            &erasure::update_params(&stored, &acknowledged).unwrap(),
        );
        assert_eq!(db.run(&acknowledge), "4");
        assert_eq!(db.run(&acknowledge), "");
        assert_eq!(
            erasure::decode_operation(&db.run(&load)).unwrap().operation,
            acknowledged
        );
        db.run(&bind(&db.sql.mark_published, &[Some(original.operation_id().into())]));
        assert!(db.run(&pending).is_empty());
        assert_eq!(db.run(&notify), "");
        assert!(db.run(&pending).is_empty());
    }
}
