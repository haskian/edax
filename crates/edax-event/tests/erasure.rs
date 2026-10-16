#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeSet;

use edax_event::erasure::{ErasureOperation, ErasureStage};
use edax_seal::KeyScope;

fn operation(participants: &[&str]) -> ErasureOperation {
    ErasureOperation::new(
        "erase-01".to_owned(),
        "request-01".to_owned(),
        KeyScope {
            key_namespace: "production-eu".to_owned(),
            tenant: Some("clinic-7".to_owned()),
            subject: Some("patient-42".to_owned()),
        },
        participants.iter().map(|name| (*name).to_owned()).collect(),
    )
    .unwrap()
}

fn restart(operation: &ErasureOperation) -> ErasureOperation {
    serde_json::from_slice(&serde_json::to_vec(operation).unwrap()).unwrap()
}

#[test]
fn every_stage_survives_restart_and_duplicate_confirmations() {
    let mut op = operation(&["patient-search", "backup-retention"]);
    assert_eq!(op.stage(), ErasureStage::Persisted);
    assert_eq!(restart(&op), op);
    op.confirm_revocation().unwrap();
    op = restart(&op);
    op.confirm_revocation().unwrap();
    assert_eq!(op.stage(), ErasureStage::Revoked);
    op.confirm_notification().unwrap();
    op = restart(&op);
    op.confirm_notification().unwrap();
    assert_eq!(op.stage(), ErasureStage::Notified);
    op.acknowledge("patient-search").unwrap();
    op = restart(&op);
    op.acknowledge("patient-search").unwrap();
    assert_eq!(op.acknowledgements().len(), 1);
    assert!(!op.can_destroy());
    op.acknowledge("backup-retention").unwrap();
    assert!(op.can_destroy());
    op.confirm_destruction().unwrap();
    op = restart(&op);
    op.confirm_destruction().unwrap();
    assert_eq!(op.stage(), ErasureStage::DestructionConfirmed);
    op.complete().unwrap();
    op = restart(&op);
    let completed = op.clone();
    op.complete().unwrap();
    op.confirm_revocation().unwrap();
    op.confirm_notification().unwrap();
    op.confirm_destruction().unwrap();
    op.acknowledge("patient-search").unwrap();
    assert_eq!(op, completed);
    assert_eq!(op.stage(), ErasureStage::Complete);
    assert_eq!(op.operation_id(), "erase-01");
    assert_eq!(op.request_id(), "request-01");
    assert_eq!(op.required_participants().len(), 2);
}

#[test]
fn premature_or_unknown_confirmations_never_advance() {
    for stage in [ErasureStage::Persisted, ErasureStage::Revoked, ErasureStage::Notified] {
        let mut op = operation(&["patient-search"]);
        if stage != ErasureStage::Persisted {
            op.confirm_revocation().unwrap();
        }
        if stage == ErasureStage::Notified {
            op.confirm_notification().unwrap();
        }
        let prior = op.clone();
        assert!(op.complete().is_err(), "{stage:?}");
        assert!(op.confirm_destruction().is_err(), "{stage:?}");
        assert!(op.acknowledge("unknown-participant").is_err(), "{stage:?}");
        if stage != ErasureStage::Notified {
            assert!(op.acknowledge("patient-search").is_err(), "{stage:?}");
        }
        if stage == ErasureStage::Persisted {
            assert!(op.confirm_notification().is_err());
        }
        assert_eq!(op, prior, "{stage:?}");
    }
}

#[test]
fn offline_participant_keeps_operation_pending_across_restarts() {
    let mut op = operation(&["patient-search", "offline-archive"]);
    op.confirm_revocation().unwrap();
    op.confirm_notification().unwrap();
    op.acknowledge("patient-search").unwrap();
    let pending = op.clone();
    for _ in 0..3 {
        op = restart(&op);
        assert!(!op.can_destroy());
        assert!(op.confirm_destruction().is_err());
        assert!(op.complete().is_err());
        assert_eq!(op, pending);
    }
}

#[test]
fn tenant_erasure_and_empty_participant_set_still_require_destroy_confirmation() {
    let mut scope = operation(&[]).scope().clone();
    scope.subject = None;
    let mut op = ErasureOperation::new("erase-tenant".into(), "request-tenant".into(), scope, BTreeSet::new()).unwrap();
    assert!(op.scope().subject.is_none());
    assert!(!op.can_destroy());
    op.confirm_revocation().unwrap();
    op.confirm_notification().unwrap();
    assert!(op.can_destroy());
    assert!(op.complete().is_err());
    op.confirm_destruction().unwrap();
    op.complete().unwrap();
    assert_eq!(restart(&op).stage(), ErasureStage::Complete);
}

#[test]
fn malformed_restored_states_and_empty_identifiers_are_rejected() {
    let op = operation(&["patient-search"]);
    let original = serde_json::to_value(&op).unwrap();
    for (field, value) in [
        ("operation_id", serde_json::json!("")),
        ("request_id", serde_json::json!("")),
        ("required_participants", serde_json::json!([""])),
        ("acknowledgements", serde_json::json!(["patient-search"])),
        ("acknowledgements", serde_json::json!(["unknown-participant"])),
        ("stage", serde_json::json!("complete")),
        ("stage", serde_json::json!("destruction_confirmed")),
    ] {
        let mut invalid = original.clone();
        invalid[field] = value;
        assert!(serde_json::from_value::<ErasureOperation>(invalid).is_err(), "{field}");
    }
    assert!(ErasureOperation::new("".into(), "request-01".into(), op.scope().clone(), BTreeSet::new()).is_err());
}
