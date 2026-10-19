#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_postgres::ingest;
use serde_json::json;

#[test]
fn partition_sql_golden() {
    let mut cases = Vec::new();
    for config in [
        json!({"partitioned":true}),
        json!({"schema":"pulse","partitioned":true,"names":{"ingest_events":"readings"}}),
    ] {
        let sql = ingest::statements(&config.to_string()).unwrap();
        cases.push(json!({"config":config,"want":sql}));
    }
    edax_golden::write_or_check("postgres_ingest_partition.json", cases);
}
