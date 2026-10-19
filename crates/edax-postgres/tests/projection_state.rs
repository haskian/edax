#![allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "test assertions")]
use edax_postgres::projection;
use serde_json::{json, Value};

#[test]
fn projection_state_sql_golden() {
    let mut cases = Vec::new();
    for (name, config, valid) in [
        (
            "default_table",
            json!({"schema":"public","table":"edax_projection_state","intents":"edax_projection_intents"}),
            true,
        ),
        (
            "custom_schema_table",
            json!({"schema":"billing","table":"account_balances","intents":"account_balance_intents"}),
            true,
        ),
        (
            "schema_injection",
            json!({"schema":"public'; DROP TABLE accounts; --","table":"edax_projection_state","intents":"edax_projection_intents"}),
            false,
        ),
        (
            "table_injection",
            json!({"schema":"public","table":"accounts\"","intents":"edax_projection_intents"}),
            false,
        ),
        (
            "uppercase_name",
            json!({"schema":"Billing","table":"edax_projection_state","intents":"edax_projection_intents"}),
            false,
        ),
        (
            "event_store_names_allowed",
            json!({"schema":"public","table":"edax_projections","intents":"edax_events"}),
            true,
        ),
        ("missing_table", json!({"schema":"public"}), false),
        (
            "one_table_for_both",
            json!({"schema":"public","table":"edax_projection","intents":"edax_projection"}),
            false,
        ),
        (
            "unknown_option",
            json!({"schema":"public","table":"edax_projection_state","intents":"edax_projection_intents","create":true}),
            false,
        ),
    ] {
        let result = projection::statements(&config.to_string());
        assert_eq!(result.is_ok(), valid, "{name}");
        let (want, error) = match result {
            Ok(sql) => (json!(sql), Value::Null),
            Err(error) => (Value::Null, json!(error.to_string())),
        };
        cases.push(json!({"name":name,"op":"sql","config":config,"want":want,"error":error}));
    }
    edax_golden::write_or_check("postgres_projection_state.json", cases);
}
