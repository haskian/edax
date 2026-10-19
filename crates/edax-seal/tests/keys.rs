#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use edax_core::ErrorCode;
use edax_seal::{Cipher, KeyScope, KeyStatus, MemoryKeyProvider, AES_GCM_MAX_INVOCATIONS};
use std::sync::{Arc, Barrier, Mutex};

fn scope(subject: Option<&str>) -> KeyScope {
    KeyScope {
        key_namespace: "orders-production".to_owned(),
        tenant: Some("bookshop".to_owned()),
        subject: subject.map(str::to_owned),
    }
}

#[test]
fn reserve_rotates_at_the_global_budget_and_retains_history() {
    let mut keys = MemoryKeyProvider::with_aes_limit(2).unwrap();
    let owner = scope(Some("customer-42"));
    let first = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    let second = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    assert_eq!(first.key_id, second.key_id);
    assert_eq!(first.key.as_bytes(), second.key.as_bytes());
    let third = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    assert_ne!(first.key_id, third.key_id);
    assert_ne!(first.key.as_bytes(), third.key.as_bytes());
    let old = keys.resolve(&owner, Cipher::Aes256Gcm, &first.key_id).unwrap();
    assert_eq!(old.as_bytes(), first.key.as_bytes());
    let fourth = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    assert_eq!(third.key_id, fourth.key_id);
    assert_eq!(keys.status(&owner).unwrap(), KeyStatus::Active);
    for limit in [0, AES_GCM_MAX_INVOCATIONS + 1] {
        assert_eq!(
            MemoryKeyProvider::with_aes_limit(limit).unwrap_err().code,
            ErrorCode::BadHandle
        );
    }
}

#[test]
fn reads_never_create_and_require_exact_scope_cipher_and_purpose() {
    let mut keys = MemoryKeyProvider::new();
    let owner = scope(Some("customer-42"));
    assert_eq!(keys.status(&owner).unwrap(), KeyStatus::Unknown);
    assert_eq!(
        keys.resolve(&owner, Cipher::Aes256Gcm, "https://attacker.invalid/key")
            .unwrap_err()
            .code,
        ErrorCode::KeyMissing
    );
    assert_eq!(keys.status(&owner).unwrap(), KeyStatus::Unknown);
    let encryption = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    let fingerprint = keys.fingerprint_key(&owner).unwrap();
    let other_cipher = keys.reserve(&owner, Cipher::XChaCha20Poly1305).unwrap();
    for other in [
        scope(Some("customer-43")),
        scope(None),
        KeyScope {
            tenant: None,
            ..owner.clone()
        },
        KeyScope {
            tenant: Some("another-bookshop".to_owned()),
            ..owner.clone()
        },
        KeyScope {
            key_namespace: "orders-staging".to_owned(),
            ..owner.clone()
        },
    ] {
        assert_eq!(
            keys.resolve(&other, Cipher::Aes256Gcm, &encryption.key_id)
                .unwrap_err()
                .code,
            ErrorCode::KeyUnauthorized
        );
    }
    assert_eq!(
        keys.resolve(&owner, Cipher::XChaCha20Poly1305, &encryption.key_id)
            .unwrap_err()
            .code,
        ErrorCode::KeyUnauthorized
    );
    assert_eq!(
        keys.resolve_fingerprint(&owner, &encryption.key_id).unwrap_err().code,
        ErrorCode::KeyUnauthorized
    );
    assert_eq!(
        keys.resolve(&owner, Cipher::Aes256Gcm, &fingerprint.key_id)
            .unwrap_err()
            .code,
        ErrorCode::KeyUnauthorized
    );
    assert_ne!(encryption.key_id, other_cipher.key_id);
    assert_ne!(encryption.key.as_bytes(), other_cipher.key.as_bytes());
    assert_ne!(encryption.key.as_bytes(), fingerprint.key.as_bytes());
    let stable = keys.fingerprint_key(&owner).unwrap();
    assert_eq!(stable.key_id, fingerprint.key_id);
    assert_eq!(stable.key.as_bytes(), fingerprint.key.as_bytes());
    assert_eq!(
        keys.resolve_fingerprint(&owner, &fingerprint.key_id)
            .unwrap()
            .as_bytes(),
        fingerprint.key.as_bytes()
    );
    assert_eq!(format!("{:?}", encryption.key), "KeyMaterial([REDACTED])");
    assert!(!format!("{keys:?}").contains(&hex::encode(encryption.key.as_bytes())));
}

#[test]
fn revocation_fences_reads_and_writes_and_destruction_keeps_every_generation_tombstoned() {
    let mut keys = MemoryKeyProvider::with_aes_limit(1).unwrap();
    let owner = scope(Some("customer-42"));
    let unrelated = scope(Some("customer-43"));
    let old = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    let new = keys.reserve(&owner, Cipher::Aes256Gcm).unwrap();
    let other_cipher = keys.reserve(&owner, Cipher::XChaCha20Poly1305).unwrap();
    let fingerprint = keys.fingerprint_key(&owner).unwrap();
    assert_eq!(keys.revoke(&owner).unwrap(), KeyStatus::Revoking);
    assert_eq!(keys.revoke(&owner).unwrap(), KeyStatus::Revoking);
    assert_eq!(keys.status(&owner).unwrap(), KeyStatus::Revoking);
    assert_eq!(
        keys.reserve(&owner, Cipher::Aes256Gcm).unwrap_err().code,
        ErrorCode::KeyRevoking
    );
    assert_eq!(keys.fingerprint_key(&owner).unwrap_err().code, ErrorCode::KeyRevoking);
    assert_eq!(
        keys.resolve(&owner, Cipher::Aes256Gcm, &old.key_id).unwrap_err().code,
        ErrorCode::KeyRevoking
    );
    keys.reserve(&unrelated, Cipher::Aes256Gcm).unwrap();
    assert_eq!(keys.destroy(&owner).unwrap(), KeyStatus::Destroyed);
    assert_eq!(keys.destroy(&owner).unwrap(), KeyStatus::Destroyed);
    assert_eq!(keys.revoke(&owner).unwrap(), KeyStatus::Destroyed);
    for (cipher, key_id) in [
        (Cipher::Aes256Gcm, &old.key_id),
        (Cipher::Aes256Gcm, &new.key_id),
        (Cipher::XChaCha20Poly1305, &other_cipher.key_id),
    ] {
        assert_eq!(
            keys.resolve(&owner, cipher, key_id).unwrap_err().code,
            ErrorCode::Shredded
        );
    }
    assert_eq!(
        keys.resolve_fingerprint(&owner, &fingerprint.key_id).unwrap_err().code,
        ErrorCode::Shredded
    );
    assert_eq!(
        keys.reserve(&owner, Cipher::Aes256Gcm).unwrap_err().code,
        ErrorCode::Shredded
    );
    assert_eq!(keys.fingerprint_key(&owner).unwrap_err().code, ErrorCode::Shredded);
    assert_eq!(
        keys.resolve(&owner, Cipher::Aes256Gcm, "never-issued")
            .unwrap_err()
            .code,
        ErrorCode::KeyMissing
    );
    assert_eq!(
        keys.resolve(&unrelated, Cipher::Aes256Gcm, &old.key_id)
            .unwrap_err()
            .code,
        ErrorCode::KeyUnauthorized
    );
    assert_eq!(keys.status(&unrelated).unwrap(), KeyStatus::Active);
}

#[test]
fn tenant_tombstones_cover_existing_and_future_subjects_but_not_other_namespaces_or_tenants() {
    let mut keys = MemoryKeyProvider::new();
    let tenant = scope(None);
    let existing = scope(Some("customer-42"));
    let future = scope(Some("customer-43"));
    let previous = keys.reserve(&existing, Cipher::Aes256Gcm).unwrap();
    keys.reserve(&tenant, Cipher::Aes256Gcm).unwrap();
    keys.revoke(&tenant).unwrap();
    for child in [&tenant, &existing, &future] {
        assert_eq!(keys.status(child).unwrap(), KeyStatus::Revoking);
        assert_eq!(
            keys.reserve(child, Cipher::Aes256Gcm).unwrap_err().code,
            ErrorCode::KeyRevoking
        );
    }
    keys.destroy(&existing).unwrap();
    assert_eq!(keys.status(&existing).unwrap(), KeyStatus::Destroyed);
    assert_eq!(keys.status(&future).unwrap(), KeyStatus::Revoking);
    keys.destroy(&tenant).unwrap();
    for child in [&tenant, &existing, &future] {
        assert_eq!(keys.status(child).unwrap(), KeyStatus::Destroyed);
        assert_eq!(keys.revoke(child).unwrap(), KeyStatus::Destroyed);
        assert_eq!(
            keys.reserve(child, Cipher::Aes256Gcm).unwrap_err().code,
            ErrorCode::Shredded
        );
    }
    assert_eq!(
        keys.resolve(&existing, Cipher::Aes256Gcm, &previous.key_id)
            .unwrap_err()
            .code,
        ErrorCode::Shredded
    );
    for independent in [
        KeyScope {
            tenant: None,
            ..existing.clone()
        },
        KeyScope {
            tenant: Some("different-bookshop".to_owned()),
            ..existing.clone()
        },
        KeyScope {
            key_namespace: "orders-staging".to_owned(),
            ..existing.clone()
        },
    ] {
        keys.reserve(&independent, Cipher::Aes256Gcm).unwrap();
        assert_eq!(keys.status(&independent).unwrap(), KeyStatus::Active);
    }
}

#[test]
fn creation_and_revocation_are_serialized_by_the_host_lock() {
    for _ in 0..32 {
        let keys = Arc::new(Mutex::new(MemoryKeyProvider::new()));
        let barrier = Arc::new(Barrier::new(3));
        let owner = scope(Some("customer-42"));
        let writer = {
            let keys = Arc::clone(&keys);
            let barrier = Arc::clone(&barrier);
            let owner = owner.clone();
            std::thread::spawn(move || {
                barrier.wait();
                keys.lock().unwrap().reserve(&owner, Cipher::Aes256Gcm)
            })
        };
        let revoker = {
            let keys = Arc::clone(&keys);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                keys.lock().unwrap().revoke(&scope(None)).unwrap()
            })
        };
        barrier.wait();
        let result = writer.join().unwrap();
        assert_eq!(revoker.join().unwrap(), KeyStatus::Revoking);
        let mut keys = keys.lock().unwrap();
        assert_eq!(keys.status(&owner).unwrap(), KeyStatus::Revoking);
        assert_eq!(
            keys.reserve(&owner, Cipher::Aes256Gcm).unwrap_err().code,
            ErrorCode::KeyRevoking
        );
        match result {
            Ok(reserved) => {
                assert_eq!(
                    keys.resolve(&owner, Cipher::Aes256Gcm, &reserved.key_id)
                        .unwrap_err()
                        .code,
                    ErrorCode::KeyRevoking
                );
            }
            Err(error) => assert_eq!(error.code, ErrorCode::KeyRevoking),
        }
        keys.destroy(&scope(None)).unwrap();
        assert_eq!(keys.status(&owner).unwrap(), KeyStatus::Destroyed);
    }
}

#[test]
fn reserve_many_charges_one_generation_or_rotates() {
    let mut keys = MemoryKeyProvider::with_aes_limit(5).unwrap();
    let owner = scope(Some("customer-42"));
    let batch = keys.reserve_many(&owner, Cipher::Aes256Gcm, 3).unwrap();
    let next = keys.reserve_many(&owner, Cipher::Aes256Gcm, 2).unwrap();
    assert_eq!(batch.key_id, next.key_id, "five uses fit one generation");
    let rotated = keys.reserve_many(&owner, Cipher::Aes256Gcm, 1).unwrap();
    assert_ne!(rotated.key_id, batch.key_id, "an exhausted generation rotates");
    let short = keys.reserve_many(&owner, Cipher::Aes256Gcm, 5).unwrap();
    assert_ne!(
        short.key_id, rotated.key_id,
        "a generation with too few uses left rotates"
    );
    for count in [0, 6] {
        assert_eq!(
            keys.reserve_many(&owner, Cipher::Aes256Gcm, count).unwrap_err().code,
            ErrorCode::BadHandle
        );
    }
    let unlimited = keys.reserve_many(&owner, Cipher::XChaCha20Poly1305, u64::MAX).unwrap();
    assert_eq!(
        keys.resolve(&owner, Cipher::XChaCha20Poly1305, &unlimited.key_id)
            .unwrap()
            .as_bytes(),
        unlimited.key.as_bytes()
    );
}
