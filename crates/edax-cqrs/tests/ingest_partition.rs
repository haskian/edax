#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_cqrs::ingest::{self, Contract, Input};
use serde_json::json;
use std::collections::BTreeMap;

fn contract() -> Contract {
    serde_json::from_value(json!({"mode":"append","generation":"pulse-v1","source_incarnation":"sensor-a","mapper_id":"pulse-v1","at_rest":"plaintext","partition":"event_id_uuid7"})).unwrap()
}
fn input(id: &str) -> Input {
    let contract = contract();
    Input {
        source: ingest::record_source_key(&contract.generation, &contract.source_incarnation, "measurement-1").unwrap(),
        key: ingest::row_key(&contract.generation, "append", Some("clinic"), Some("person-7"), id).unwrap(),
        stream: "session-7".into(),
        event_id: id.into(),
        sequence: None,
        content: json!({"heart_rate":72}),
        private: Some(json!({"heart_rate":72})),
        public: None,
        erased: false,
        fingerprint: None,
        raw_fingerprint: ingest::raw_fingerprint(b"heart_rate=72"),
        raw: "heart_rate=72".into(),
        failure: None,
    }
}
#[test]
fn admission_boundaries_and_rejection_precedence() {
    let c = contract();
    let original = input("00000000-0001-7000-8000-000000000001");
    let empty = BTreeMap::new();
    for (floor, outcome) in [
        (0, "accepted"),
        (999, "accepted"),
        (1000, "accepted"),
        (1001, "expired"),
    ] {
        let plan =
            ingest::partition::plan(&c, std::slice::from_ref(&original), &empty, &BTreeMap::new(), floor).unwrap();
        assert_eq!(plan.outcomes, [outcome]);
        assert_eq!(plan.writes.len(), usize::from(outcome == "accepted"));
        assert!(plan.rejections.is_empty());
    }
    let mut malformed = original.clone();
    malformed.content = json!({"heart_rate":"bad\0value"});
    assert_eq!(
        ingest::partition::plan(&c, &[malformed], &empty, &BTreeMap::new(), 1001)
            .unwrap()
            .outcomes,
        ["rejected"]
    );
    let invalid = input("not-a-uuid");
    let mut erased_invalid = invalid.clone();
    erased_invalid.erased = true;
    assert_eq!(
        ingest::partition::plan(&c, &[erased_invalid], &empty, &BTreeMap::new(), 1001)
            .unwrap()
            .outcomes,
        ["rejected"]
    );
    let refused = ingest::partition::plan(&c, &[invalid], &empty, &BTreeMap::new(), 1001).unwrap();
    assert_eq!(refused.outcomes, ["rejected"]);
    let replay =
        ingest::partition::plan(&c, std::slice::from_ref(&original), &empty, &refused.rejections, 1001).unwrap();
    assert_eq!(replay.outcomes, ["rejected"]);
    let mut changed = original;
    changed.raw_fingerprint = ingest::raw_fingerprint(b"changed");
    assert!(ingest::partition::plan(&c, &[changed], &empty, &refused.rejections, 1001).is_err());
}
#[test]
fn manifest_and_coordinates_are_exact() {
    let mut c = contract();
    let original = input("ffffffff-ffff-7000-bfff-ffffffffffff");
    let coords = ingest::partition::coordinates(&c, std::slice::from_ref(&original.key)).unwrap();
    assert_eq!(coords[&original.key], "281474976710655000");
    assert!(ingest::partition::horizon(-1).is_err());
    assert!(ingest::partition::horizon(ingest::partition::MAX_HORIZON + 1).is_err());
    c.mode = "upsert".into();
    assert!(ingest::bind_contract(None, &c).is_err());
    c.mode = "append".into();
    c.partition = Some("mapped".into());
    assert!(ingest::bind_contract(None, &c).is_err());
    c.partition = None;
    assert!(!serde_json::to_string(&c).unwrap().contains("partition"));
}

#[test]
fn partition_golden() {
    let c = contract();
    let mut cases = Vec::new();
    for (id, floor) in [
        ("00000000-0001-7000-8000-000000000001", 1000),
        ("00000000-0001-7000-8000-000000000001", 1001),
        ("ffffffff-ffff-7000-bfff-ffffffffffff", 281_474_976_710_655_001),
        ("not-a-uuid", 1001),
        ("00000000-0001-4000-8000-000000000001", 1001),
    ] {
        let inputs = vec![input(id)];
        let plan = ingest::partition::plan(&c, &inputs, &BTreeMap::new(), &BTreeMap::new(), floor).unwrap();
        cases.push(json!({"contract":c,"inputs":inputs,"floor":floor.to_string(),"want":plan}));
    }
    edax_golden::write_or_check("ingestion_partition.json", cases);
}
