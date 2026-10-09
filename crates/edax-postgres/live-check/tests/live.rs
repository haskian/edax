//! The SQL `edax-postgres` emits, run against a real server and held to the kernel's reference applier:
//! every case of `filter.json` and `query.json`, every page of a keyset walk, and a seeded random
//! differential over documents built to hit each rule (types, missing paths, nulls, decimal text,
//! code point order, doubles past 2^53, NUL). The reference is `edax_cqrs::query` over the documents as
//! the table returns them, in row key order, which is the memory store's input order.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "a test"
)]

use std::sync::Mutex;

use edax_cqrs::Page;
use edax_postgres::{
    check_catalog, compile, decimal_sql, page, reference_ddl, statements, stored_document, Logical, Statements, Tables,
};
use postgres::{Client, NoTls};
use serde_json::{json, Value};

static LOCK: Mutex<()> = Mutex::new(());

struct Live {
    client: Client,
    sql: Statements,
    tables: Tables,
}

fn live() -> Option<Live> {
    let url = std::env::var("EDAX_POSTGRES_URL").ok()?;
    let mut client = Client::connect(&url, NoTls).expect("EDAX_POSTGRES_URL connects");
    let tables = Tables::from_json(r#"{"schema":"edax_live_check"}"#).unwrap();
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS \"{0}\" CASCADE; CREATE SCHEMA \"{0}\"",
            tables.schema()
        ))
        .unwrap();
    // What a host's migration tool runs: the reference schema, nothing else.
    client.batch_execute(&reference_ddl(&tables)).unwrap();
    Some(Live {
        client,
        sql: statements(&tables),
        tables,
    })
}

impl Live {
    /// Store `docs` as projection `name`, each under its string `id` or a generated row key, and return
    /// them as the table reads them back, in row key order.
    fn store(&mut self, name: &str, docs: &[Value]) -> Vec<Value> {
        self.client
            .execute(
                &format!(
                    "DELETE FROM {} WHERE projection = $1",
                    self.tables.qualified(Logical::Projections)
                ),
                &[&name],
            )
            .unwrap();
        for (i, doc) in docs.iter().enumerate() {
            let id = doc
                .get("id")
                .and_then(Value::as_str)
                .map_or_else(|| format!("row-{i:04}"), str::to_owned);
            let text = stored_document(&id, doc.to_string().as_bytes()).unwrap();
            let rows = self
                .client
                .execute(
                    &self.sql.projection_insert,
                    &[&Some(name), &Some(id.as_str()), &Some(text.as_str())],
                )
                .unwrap();
            assert_eq!(rows, 1, "row keys are unique");
        }
        let rows = self
            .client
            .query(
                &format!(
                    "SELECT document::text FROM {} WHERE projection = $1 ORDER BY id",
                    self.tables.qualified(Logical::Projections)
                ),
                &[&name],
            )
            .unwrap();
        rows.iter()
            .map(|row| serde_json::from_str(row.get::<_, &str>(0)).unwrap())
            .collect()
    }

    /// Run `spec` over projection `name` through the compiled SQL.
    fn get_many(&mut self, name: &str, spec: &Value) -> Result<Page, String> {
        let spec = spec.to_string();
        let compiled = compile(&self.tables, name, spec.as_bytes()).map_err(|e| e.to_string())?;
        let params: Vec<&(dyn postgres::types::ToSql + Sync)> = compiled
            .params
            .iter()
            .map(|p| p as &(dyn postgres::types::ToSql + Sync))
            .collect();
        let rows = self
            .client
            .query(&compiled.sql, &params)
            .map_err(|e| format!("{e}: {}", compiled.sql))?;
        let rows: Vec<String> = rows.iter().map(|row| row.get::<_, String>(0)).collect();
        page(spec.as_bytes(), rows).map_err(|e| e.to_string())
    }
}

fn reference(spec: &Value, docs: &[Value]) -> Result<Page, String> {
    let query = edax_cqrs::query_spec(spec.to_string().as_bytes()).map_err(|e| e.to_string())?;
    edax_cqrs::query(&query, docs.to_vec()).map_err(|e| e.to_string())
}

/// Walk every page of `spec` with `limit` rows each, through Postgres and through the reference, and
/// require the same pages and cursors.
fn walk(live: &mut Live, name: &str, docs: &[Value], spec: &Value, limit: usize, label: &str) {
    let mut spec = spec.clone();
    spec["limit"] = json!(limit);
    for step in 0..200 {
        let got = live.get_many(name, &spec);
        let want = reference(&spec, docs);
        assert_eq!(got, want, "{label}: page {step} of {spec}");
        match want {
            Ok(Page { next: Some(next), .. }) => spec["after"] = json!(next),
            _ => return,
        }
    }
    panic!("{label}: the walk did not end");
}

fn is_sorted(spec: &Value) -> bool {
    spec.get("sort")
        .and_then(Value::as_array)
        .is_some_and(|s| !s.is_empty())
}

#[test]
fn filter_vectors() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    for case in edax_golden::read("filter.json") {
        let name = case["name"].as_str().unwrap();
        let spec = json!({"filter": case["filter"]});
        if !case["error"].is_null() {
            let got = live.get_many("filter", &spec).err();
            assert!(got.is_some_and(|e| e.starts_with("codec:")), "{name}: refused");
            continue;
        }
        let stored = match stored_document("row", case["document"].to_string().as_bytes()) {
            Ok(_) => live.store("filter", &[case["document"].clone()]),
            Err(_) => continue, // a document no store keeps
        };
        let got = live.get_many("filter", &spec).unwrap();
        let want = reference(&spec, &stored).unwrap();
        assert_eq!(got, want, "{name}");
        if case["document"].get("id").is_none() {
            assert_eq!(
                !got.rows.is_empty(),
                case["expected"].as_bool().unwrap(),
                "{name}: the golden verdict"
            );
        }
    }
}

#[test]
fn query_vectors() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    for case in edax_golden::read("query.json") {
        let name = case["name"].as_str().unwrap();
        let docs = case["documents"].as_array().unwrap();
        let spec = &case["query"];
        if docs
            .iter()
            .any(|d| stored_document("x", d.to_string().as_bytes()).is_err())
        {
            continue;
        }
        let stored = live.store("query", docs);
        assert_eq!(live.get_many("query", spec), reference(spec, &stored), "{name}");
        if case["error"].is_null() && is_sorted(spec) && spec.get("after").is_none() {
            for limit in [1, 2] {
                walk(&mut live, "query", &stored, spec, limit, name);
            }
        }
    }
}

/// A small seeded generator: the same documents and queries on every run.
struct Seeded(u64);

impl Seeded {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[(self.next() % items.len() as u64) as usize]
    }

    fn value(&mut self) -> Value {
        let values = [
            json!(0),
            json!(1),
            json!(-1),
            json!(1.0),
            json!(1.5),
            json!(-0.0),
            json!(9_007_199_254_740_993_u64),
            json!(58_088_200_776_852_271_u64),
            json!(5.808_820_077_685_227e16),
            json!(u64::MAX),
            json!(i64::MIN),
            json!(1e-7),
            json!(0.1),
            json!("a"),
            json!("B"),
            json!("é"),
            json!("\u{ff71}"),
            json!("\u{1f980}"),
            json!(""),
            json!("7"),
            json!("07"),
            json!("9.5"),
            json!("10"),
            json!("9.50"),
            json!("-0"),
            json!("1E+2"),
            json!("n/a"),
            json!(true),
            json!(false),
            json!(null),
            json!([1, "a"]),
            json!([1.0]),
            json!([]),
            json!({"x": 1}),
            json!({"x": 1.0}),
        ];
        self.pick(&values).clone()
    }

    fn doc(&mut self, i: usize) -> Value {
        let mut doc = serde_json::Map::new();
        doc.insert("id".into(), json!(format!("d{:03}", i)));
        for key in ["a", "b", "c"] {
            if !self.next().is_multiple_of(5) {
                doc.insert(key.into(), self.value());
            }
        }
        if self.next().is_multiple_of(3) {
            let mut nested = serde_json::Map::new();
            nested.insert("x".into(), self.value());
            doc.insert("n".into(), Value::Object(nested));
        }
        Value::Object(doc)
    }

    fn path(&mut self) -> &'static str {
        self.pick::<&str>(&["a", "b", "c", "n.x", "n", "missing", "a.x", "id"])
    }

    fn leaf(&mut self) -> Value {
        let path = self.path();
        let value = self.value();
        let decimal_value = self.pick(&["9.5", "10", "007", "-0.0", "1E+2", "0.1"]).to_owned();
        match self.next() % 8 {
            0 if !value.is_null() => {
                json!({"compare": {"path": path, "op": self.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]), "value": value}})
            }
            1 => {
                json!({"compare": {"path": path, "op": self.pick(&["eq", "ne", "gt", "gte", "lt", "lte"]), "value": decimal_value, "collation": "decimal"}})
            }
            2 => json!({"compare": {"path": path, "op": "starts_with", "value": self.pick(&["", "a", "9", "0"])}}),
            3 => {
                let values: Vec<Value> = (0..self.next() % 3)
                    .map(|_| self.value())
                    .filter(|v| !v.is_null())
                    .collect();
                json!({"in": {"path": path, "values": values}})
            }
            4 => json!({"contains": {"path": path, "value": self.value()}}),
            5 => json!({"exists": {"path": path, "exists": self.next().is_multiple_of(2)}}),
            6 => json!({"in": {"path": path, "values": [decimal_value], "collation": "decimal"}}),
            _ => json!({"compare": {"path": path, "op": "eq", "value": "a"}}),
        }
    }

    fn filter(&mut self, depth: u32) -> Value {
        if depth == 0 {
            return self.leaf();
        }
        match self.next() % 5 {
            0 => json!({"and": (0..self.next() % 3).map(|_| self.filter(depth - 1)).collect::<Vec<_>>()}),
            1 => json!({"or": (0..self.next() % 3).map(|_| self.filter(depth - 1)).collect::<Vec<_>>()}),
            2 => json!({"not": self.filter(depth - 1)}),
            _ => self.leaf(),
        }
    }

    fn sort(&mut self) -> Value {
        let keys: Vec<Value> = (0..1 + self.next() % 3)
            .map(|_| {
                let mut key = json!({"path": self.path(), "desc": self.next().is_multiple_of(2)});
                if self.next().is_multiple_of(3) {
                    key["collation"] = json!("decimal");
                }
                key
            })
            .collect();
        Value::Array(keys)
    }
}

#[test]
fn seeded_differential() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    let mut seeded = Seeded(0x9e37_79b9_7f4a_7c15);
    for round in 0..12 {
        let docs: Vec<Value> = (0..40).map(|i| seeded.doc(i)).collect();
        let stored = live.store("random", &docs);
        for case in 0..60 {
            let mut spec = json!({});
            if !seeded.next().is_multiple_of(4) {
                spec["filter"] = seeded.filter(2);
            }
            if !seeded.next().is_multiple_of(4) {
                spec["sort"] = seeded.sort();
            }
            if seeded.next().is_multiple_of(5) {
                spec["offset"] = json!(seeded.next() % 4);
            }
            let label = format!("round {round} case {case}");
            assert_eq!(
                live.get_many("random", &spec),
                reference(&spec, &stored),
                "{label}: {spec}"
            );
            if is_sorted(&spec) && spec.get("offset").is_none() {
                walk(
                    &mut live,
                    "random",
                    &stored,
                    &spec,
                    1 + (seeded.next() % 7) as usize,
                    &label,
                );
            }
        }
    }
}

#[test]
fn decimal_sql_matches_the_kernel() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    let mut texts: Vec<String> = [
        "42",
        "9.5",
        "9.50",
        "100.000",
        "007.25",
        "0.10",
        ".5",
        "5.",
        "+3",
        "-12.30",
        "-0.0",
        "0E-10",
        "0E+999999999",
        "1E+2",
        "1.5e3",
        "1.5E-7",
        "12345E-2",
        "0E+2147483647",
        "1E-2147483648",
        "1E+2147483648",
        "0.001E+1001",
        "1e+0000000002",
        "",
        ".",
        "-",
        "+",
        "1e",
        "e5",
        "1.2.3",
        "１２",
        "1_000",
        "NaN",
        "Infinity",
        " 1",
        "1 ",
        "0x10",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    texts.push(format!("1E+{}", 999));
    texts.push(format!("1E+{}", 1000));
    texts.push(format!("1E-{}", 999));
    texts.push(format!("{}1.{}", "0".repeat(999), "0".repeat(999)));
    texts.push(format!("{}1.{}", "0".repeat(999), "0".repeat(1000)));
    texts.push(format!("1{}E-{}", "0".repeat(999), 999));
    texts.push(format!("1{}E-1000", "0".repeat(1000)));
    let function = decimal_sql("$1::text");
    for text in texts {
        let row = live
            .client
            .query_one(&format!("SELECT ({function})::text"), &[&text])
            .unwrap();
        let got: Option<String> = row.get(0);
        let want = edax_cqrs::canonical_decimal(&text).ok();
        // Postgres renders the value in its own notation; compare values through the kernel's canonical form.
        let got = got.map(|g| edax_cqrs::canonical_decimal(&g).unwrap());
        assert_eq!(got, want, "{text:?}");
    }
}

/// `postgres_query.json`: each case's SQL and parameters, exactly as the vector holds them, return the
/// vector's page from a server holding its documents.
#[test]
fn postgres_query_vectors() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    let live_schema = format!("\"{}\"", live.tables.schema());
    for case in edax_golden::read("postgres_query.json") {
        let name = case["name"].as_str().unwrap();
        if !case["error"].is_null() {
            continue;
        }
        let docs = case["documents"].as_array().unwrap();
        live.store("accounts", docs);
        // The vector is rendered for `public`; this check runs in its own schema.
        let sql = case["sql"]
            .as_str()
            .unwrap()
            .replace("\"public\".", &format!("{live_schema}."));
        let params: Vec<Option<String>> = serde_json::from_value(case["params"].clone()).unwrap();
        let bound: Vec<&(dyn postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p as &(dyn postgres::types::ToSql + Sync))
            .collect();
        let rows: Vec<String> = live
            .client
            .query(&sql, &bound)
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        let got = page(case["query"].to_string().as_bytes(), rows).map_err(|e| e.to_string());
        let want = match case["page_error"].as_str() {
            Some(error) => Err(error.to_owned()),
            None => Ok(serde_json::from_value::<Page>(case["expected_page"].clone()).unwrap()),
        };
        assert_eq!(got, want, "{name}");
    }
}

/// The reference schema meets the contract, read back from a real catalog: the check maps each unique key
/// to the index the server created, and refuses a table altered off the contract with every mismatch named.
#[test]
fn the_reference_schema_meets_the_contract() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    let all = Logical::ALL.to_vec();
    let catalog = |live: &mut Live| -> String {
        let param = edax_postgres::catalog_param(&live.tables, &all);
        live.client.query_one(&live.sql.catalog, &[&param]).unwrap().get(0)
    };
    let found = catalog(&mut live);
    let compatibility = check_catalog(&live.tables, &all, &found).unwrap();
    assert_eq!(
        compatibility.unique.len(),
        9,
        "one index per unique key: {compatibility:?}"
    );
    assert!(compatibility
        .unique
        .values()
        .any(|k| k.as_str() == "events_stream_sequence"));
    let ledger = live.tables.qualified(Logical::Ledger);
    live.client
        .batch_execute(&format!(
            "ALTER TABLE {ledger} ALTER COLUMN record TYPE json USING record::json; \
             ALTER TABLE {ledger} ALTER COLUMN key TYPE text COLLATE \"en_US\"; \
             ALTER TABLE {ledger} ADD COLUMN region text NOT NULL DEFAULT 'eu', ADD COLUMN shard int NOT NULL"
        ))
        .or_else(|_| {
            // A server without the en_US collation: refuse the key column's type instead.
            live.client.batch_execute(&format!(
                "ALTER TABLE {ledger} ALTER COLUMN record TYPE json USING record::json; \
                 ALTER TABLE {ledger} ALTER COLUMN key TYPE varchar COLLATE \"default\"; \
                 ALTER TABLE {ledger} ADD COLUMN region text NOT NULL DEFAULT 'eu', ADD COLUMN shard int NOT NULL"
            ))
        })
        .unwrap();
    let altered = catalog(&mut live);
    let refused = check_catalog(&live.tables, &all, &altered).unwrap_err().to_string();
    for want in [
        "record is json",
        "key collates as",
        "shard is NOT NULL with no default",
        "no unique index on exactly (key)",
    ] {
        assert!(refused.contains(want), "{want}: {refused}");
    }
    assert!(
        !refused.contains("region"),
        "a defaulted extra column is the host's: {refused}"
    );
}

#[test]
fn id_queries_use_primary_key_under_generic_plans() {
    let _guard = LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(mut live) = live() else { return };
    let table = live.tables.qualified(Logical::Projections);
    live.client
        .batch_execute(&format!(
            "INSERT INTO {table} (projection, id, version, document) \
         SELECT 'plans', 'account-' || lpad(n::text, 6, '0'), 1, \
         jsonb_build_object('id', 'account-' || lpad(n::text, 6, '0')) \
         FROM generate_series(1, 20000) AS n; \
         ANALYZE {table}; SET plan_cache_mode = force_generic_plan"
        ))
        .unwrap();
    let asc = json!([{"path": "id"}]);
    let desc = json!([{"path": "id", "desc": true}]);
    let cursor = |sort: &Value| {
        let spec = edax_cqrs::query_spec(json!({"sort": sort}).to_string().as_bytes()).unwrap();
        edax_cqrs::encode_cursor(&json!({"id": "account-010000"}), &spec.sort).unwrap()
    };
    for (name, spec, id_conditions) in [
        ("asc", json!({"sort": asc, "limit": 10}), vec![]),
        ("desc", json!({"sort": desc, "limit": 10}), vec![]),
        (
            "after_asc",
            json!({"sort": asc, "after": cursor(&asc), "limit": 10}),
            vec!["id >"],
        ),
        (
            "after_desc",
            json!({"sort": desc, "after": cursor(&desc), "limit": 10}),
            vec!["id <"],
        ),
        (
            "equal",
            json!({"filter": {"compare": {"path": "id", "op": "eq", "value": "account-010000"}}}),
            vec!["id ="],
        ),
        (
            "prefix",
            json!({"filter": {"compare": {"path": "id", "op": "starts_with", "value": "account-010"}}, "sort": asc, "limit": 10}),
            vec!["id >=", "id <"],
        ),
    ] {
        let compiled = compile(&live.tables, "plans", spec.to_string().as_bytes()).unwrap();
        let types = vec!["text"; compiled.params.len()].join(", ");
        live.client
            .batch_execute(&format!("PREPARE query_plan ({types}) AS {}", compiled.sql))
            .unwrap();
        let args = compiled
            .params
            .iter()
            .map(|p| match p {
                Some(text) => format!("'{}'", text.replace('\'', "''")),
                None => "NULL".to_owned(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        let rows = live
            .client
            .query(&format!("EXPLAIN EXECUTE query_plan ({args})"), &[])
            .unwrap();
        let plan = rows
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(plan.contains("Index Scan") && plan.contains("_pkey"), "{name}: {plan}");
        assert!(!plan.contains("Sort"), "{name}: {plan}");
        let condition = plan
            .lines()
            .find(|line| line.contains("Index Cond:"))
            .expect("index condition");
        assert!(condition.contains("projection = $1"), "{name}: {plan}");
        for wanted in id_conditions {
            assert!(condition.contains(wanted), "{name}: missing {wanted}: {plan}");
        }
        live.client.batch_execute("DEALLOCATE query_plan").unwrap();
    }
}
