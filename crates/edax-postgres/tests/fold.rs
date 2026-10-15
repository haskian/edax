#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_postgres::fold;
use serde_json::{json, Value};

#[test]
fn fold_sql_golden() {
    let mut cases = Vec::new();
    for (name, config, valid) in [
        ("default_table", json!({"schema":"public","table":"edax_fold"}), true),
        (
            "custom_schema_table",
            json!({"schema":"billing","table":"account_balances"}),
            true,
        ),
        (
            "schema_injection",
            json!({"schema":"public'; DROP TABLE accounts; --","table":"edax_fold"}),
            false,
        ),
        (
            "table_injection",
            json!({"schema":"public","table":"accounts\""}),
            false,
        ),
        ("uppercase_name", json!({"schema":"Billing","table":"edax_fold"}), false),
        ("missing_table", json!({"schema":"public"}), false),
        (
            "unknown_option",
            json!({"schema":"public","table":"edax_fold","create":true}),
            false,
        ),
    ] {
        let result = fold::statements(&config.to_string());
        assert_eq!(result.is_ok(), valid, "{name}");
        let (want, error) = match result {
            Ok(sql) => (json!(sql), Value::Null),
            Err(error) => (Value::Null, json!(error.to_string())),
        };
        cases.push(json!({"name":name,"op":"sql","config":config,"want":want,"error":error}));
    }
    edax_golden::write_or_check("postgres_fold.json", cases);
}
