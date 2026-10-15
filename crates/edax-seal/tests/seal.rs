#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use edax_seal::{fingerprint, new_key, new_nonce, open, seal, Aad, Cipher, EncryptionInfo, KeyScope, SealedValue};
use std::collections::BTreeMap;

fn info(cipher: Cipher) -> EncryptionInfo {
    EncryptionInfo {
        format_version: 1,
        key_namespace: "production-eu".into(),
        key_id: "generation-1".into(),
        cipher,
        nonce: vec![0; cipher.nonce_len()],
    }
}

#[test]
fn independently_published_aead_vectors() {
    // NIST CAVS 14.0, gcmEncryptExtIV256.rsp, first 128-bit-tag vector.
    // https://github.com/pyca/cryptography/blob/main/vectors/cryptography_vectors/ciphers/AES/GCM/gcmEncryptExtIV256.rsp
    let mut aes = info(Cipher::Aes256Gcm);
    aes.nonce = hex::decode("516c33929df5a3284ff463d7").unwrap();
    let aes_key = hex::decode("b52c505a37d78eda5dd34f20c22540ea1b58963cf8e5bf8ffa85f9f2492505b4").unwrap();
    let aes_expected = hex::decode("bdc1ac884d332457a1d2664f168c76f0").unwrap();
    assert_eq!(seal(&aes, &aes_key, b"", b"").unwrap(), aes_expected);
    assert_eq!(open(&aes, &aes_key, b"", &aes_expected).unwrap(), b"");

    // draft-arciszewski-xchacha-03 Appendix A.1; independent of RustCrypto.
    let mut xchacha = info(Cipher::XChaCha20Poly1305);
    xchacha.nonce = (0x40..=0x57).collect();
    let key: Vec<u8> = (0x80..=0x9f).collect();
    let aad = hex::decode("50515253c0c1c2c3c4c5c6c7").unwrap();
    let plaintext = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
    let expected = hex::decode(concat!(
        "bd6d179d3e83d43b9576579493c0e939572a1700252bfaccbed2902c21396cbb",
        "731c7f1b0b4aa6440bf3a82f4eda7e39ae64c6708c54c216cb96b72e1213",
        "b4522f8c9ba40db5d945b11b69b982c1bb9e3f3fac2bc369488f76b2383565",
        "d3fff921f9664c97637da9768812f615c68b13b52e",
        "c0875924c1c7987947deafd8780acf49"
    ))
    .unwrap();
    assert_eq!(seal(&xchacha, &key, &aad, plaintext).unwrap(), expected);
    assert_eq!(open(&xchacha, &key, &aad, &expected).unwrap(), plaintext);
}

#[test]
fn tampering_and_invalid_inputs_fail_without_plaintext() {
    for cipher in Cipher::ALL {
        let original = info(*cipher);
        let key = [42; 32];
        let ciphertext = seal(&original, &key, b"event-context", b"customer address").unwrap();
        for position in [0, ciphertext.len() - 1] {
            let mut altered = ciphertext.clone();
            altered[position] ^= 1;
            assert!(open(&original, &key, b"event-context", &altered).is_err());
        }
        assert!(open(&original, &[43; 32], b"event-context", &ciphertext).is_err());
        assert!(open(&original, &key, b"command-context", &ciphertext).is_err());
        assert!(open(&original, &key, b"event-context", &[0; 15]).is_err());
        assert!(seal(&original, &[42; 31], b"", b"").is_err());
        assert!(fingerprint(&[42; 31], b"command").is_err());
        let mut altered = original.clone();
        altered.nonce[0] ^= 1;
        assert!(open(&altered, &key, b"event-context", &ciphertext).is_err());
        for field in ["format", "nonce", "namespace", "key_id"] {
            let mut altered = original.clone();
            match field {
                "format" => altered.format_version = 2,
                "nonce" => altered.nonce.clear(),
                "namespace" => altered.key_namespace.clear(),
                "key_id" => altered.key_id.clear(),
                _ => unreachable!(),
            }
            assert!(altered.check().is_err(), "{field}");
            assert!(seal(&altered, &key, b"", b"").is_err(), "{field}");
            assert!(open(&altered, &key, b"", &ciphertext).is_err(), "{field}");
        }
    }
}

#[test]
fn scope_encoding_is_injective_and_validated() {
    let scopes = [
        ("a", None, None),
        ("a", Some("b"), None),
        ("a", None, Some("b")),
        ("a:b", Some("c"), Some("d")),
        ("a", Some("b:c"), Some("d")),
        ("a", Some("b"), Some("c:d")),
        ("é", Some("b"), Some("d")),
    ];
    let encodings: std::collections::BTreeSet<Vec<u8>> = scopes
        .iter()
        .map(|(namespace, tenant, subject)| {
            KeyScope {
                key_namespace: (*namespace).into(),
                tenant: tenant.map(str::to_owned),
                subject: subject.map(str::to_owned),
            }
            .encode()
            .unwrap()
        })
        .collect();
    assert_eq!(encodings.len(), scopes.len());
    for (namespace, tenant, subject) in [("", None, None), ("a", Some(""), None), ("a", None, Some(""))] {
        assert!(KeyScope {
            key_namespace: namespace.into(),
            tenant: tenant.map(str::to_owned),
            subject: subject.map(str::to_owned)
        }
        .encode()
        .is_err());
    }
}

#[test]
fn aad_encodes_option_presence_lengths_maps_and_integers() {
    let mut absent = Aad::new("snapshot");
    absent.optional(None);
    let mut empty = Aad::new("snapshot");
    empty.optional(Some(""));
    assert_ne!(absent.finish(), empty.finish());
    let mut first = Aad::new("event");
    first.str("ab").str("c");
    let mut second = Aad::new("event");
    second.str("a").str("bc");
    assert_ne!(first.finish(), second.finish());
    let mut aad = Aad::new("integer");
    aad.u32(1)
        .u64(u64::MAX)
        .i64(i64::MIN)
        .optional_u64(Some(0))
        .optional_i64(None);
    assert!(aad.finish().ends_with(&[0; 9]));
    let mut left = BTreeMap::new();
    left.insert("z".into(), "2".into());
    left.insert("a".into(), "1".into());
    let right = BTreeMap::from([("a".into(), "1".into()), ("z".into(), "2".into())]);
    let mut first = Aad::new("metadata");
    first.map(&left);
    let mut second = Aad::new("metadata");
    second.map(&right);
    assert_eq!(first.finish(), second.finish());
}

#[test]
fn encryption_info_preserves_wire_types_and_requires_fields() {
    for cipher in Cipher::ALL {
        let original = info(*cipher);
        let json = serde_json::to_value(&original).unwrap();
        assert!(json["nonce"].is_string());
        assert_eq!(json["cipher"], cipher.as_str());
        assert_eq!(
            serde_json::from_value::<EncryptionInfo>(json.clone()).unwrap(),
            original
        );
        let packed = rmp_serde::to_vec_named(&original).unwrap();
        assert_eq!(rmp_serde::from_slice::<EncryptionInfo>(&packed).unwrap(), original);
        for field in ["format_version", "key_namespace", "key_id", "cipher", "nonce"] {
            let mut incomplete = json.clone();
            incomplete.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<EncryptionInfo>(incomplete).is_err(), "{field}");
        }
    }
}

#[test]
fn hmac_sha256_matches_rfc4231_case_2_with_zero_padded_key() {
    // HMAC zero-padding a short key to 32 bytes leaves its 64-byte block unchanged.
    let mut key = [0; 32];
    key[..4].copy_from_slice(b"Jefe");
    assert_eq!(
        hex::encode(fingerprint(&key, b"what do ya want for nothing?").unwrap()),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn random_keys_and_nonces_have_the_required_sizes() {
    assert_eq!(new_key().unwrap().len(), 32);
    for cipher in Cipher::ALL {
        assert_eq!(new_nonce(*cipher).unwrap().len(), cipher.nonce_len());
    }
}

#[test]
fn stored_sealed_values_preserve_bytes_and_validate_framing() {
    let encryption = info(Cipher::default());
    let original = SealedValue {
        data: seal(&encryption, &[7; 32], b"snapshot-context", b"customer address").unwrap(),
        encryption,
    };
    original.check().unwrap();
    let json = serde_json::to_value(&original).unwrap();
    assert!(json["data"].is_string());
    assert_eq!(serde_json::from_value::<SealedValue>(json).unwrap(), original);
    let packed = rmp_serde::to_vec_named(&original).unwrap();
    assert_eq!(rmp_serde::from_slice::<SealedValue>(&packed).unwrap(), original);
    let mut malformed = original;
    malformed.data.truncate(15);
    assert!(malformed.check().is_err());
}
