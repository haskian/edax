//! A read-model query compiled to one SQL statement over `edax_projections`, and the page its rows make.
//!
//! The SQL reproduces the kernel's reference applier (`edax_cqrs::query`), not `jsonb`'s own semantics:
//!
//! - **Paths.** A segment is an object key, read with `->` and a text operand, which yields `NULL` on an
//!   array or a scalar: the kernel's `resolve`, which never indexes an array. A path is present when it
//!   resolves to anything but JSON `null`. The `id` path is the row key column, which every stored
//!   document carries as its `"id"`.
//! - **Two-valued leaves.** Every leaf is `coalesce(…, false)`, so `NOT` flips a missing or null path to
//!   true and `Ne` stays false on it, as the kernel's table in `docs/kernels/filter.md` says.
//! - **Values.** Equality is `jsonb` `=`: structural, numeric at every depth. Orderings hold within one
//!   type, read by the comparand's type: numbers as `numeric`, strings `COLLATE "C"` (code point order),
//!   bools. The decimal collation reads string values through [`decimal_sql`]. `Contains` tests the
//!   array's elements one by one, never `@>`, whose nested containment is another relation.
//! - **Sort.** Each key expands to its type rank (number < string < bool < array < object < null, a
//!   missing path as null), then the value of each type. Under the decimal collation, decimal text sorts
//!   by value before other text, and only other text has a text column, so `"7"` and `"07"` tie and the
//!   `id` tiebreak decides. The effective sort ends on the `id` column.
//! - **Keyset.** A row is after the cursor when its tuple is greater key by key under each key's
//!   direction: an `OR` over the keys, each with every earlier key tied.
//! - **NUL.** A comparand holding NUL cannot be bound, and no stored document holds one
//!   ([`crate::documents::stored_document`]), so it is folded to the answer the kernel gives every stored
//!   document: equality false, a string ordering against the text before the NUL.
//!
//! Numbers are bound as [`crate::documents::exact_json`] number text, so a double past 2^53 compares at its
//! exact value, as the kernel compares it.

use edax_core::Error;
use edax_cqrs::filter::{Filter, Operator};
use edax_cqrs::{
    canonical_decimal, type_rank, Collation, Page, SortKey, MAX_DECIMAL_DIGITS, MAX_DECIMAL_TEXT, TIEBREAK_PATH,
    TYPE_ORDER,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::clamp_bigint;
use crate::documents::{exact_json, exact_number};
use crate::schema::{Logical, Tables};
use edax_cqrs::holds_nul;

/// A query compiled to SQL: the statement and its parameters, every one bound as text (`None` binds
/// SQL `NULL`). Its rows are one text column, a stored document each; hand them to [`page`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledQuery {
    /// The statement.
    pub sql: String,
    /// The parameters, `$1` first.
    pub params: Vec<Option<String>>,
}

/// Compile the query `spec` (the kernel's JSON query) over the documents of `projection` in the projections table of `tables`.
/// A spec the kernel refuses, or a cursor minted under another sort, is the same `codec` error the
/// kernel's applier gives.
pub fn compile(tables: &Tables, projection: &str, spec: &[u8]) -> Result<CompiledQuery, Error> {
    let query = edax_cqrs::query_spec(spec)?;
    edax_core::check_text("projection", projection)?;
    let mut sql = Sql::new();
    let projection = sql.bind(projection);
    let keys = edax_cqrs::effective_sort(&query.sort);
    let sorted: Vec<Key> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| Key::new(&mut sql, i, key))
        .collect();

    let mut conditions = vec![format!("p.projection = {projection}")];
    if let Some(filter) = &query.filter {
        conditions.push(sql.filter(filter)?);
    }
    if let Some(cursor) = &query.after {
        let tuple = edax_cqrs::decode_cursor(cursor, &query.sort)?;
        conditions.push(keyset(&mut sql, &sorted, &tuple));
    }
    let order = if sorted.is_empty() {
        "p.id".to_owned()
    } else {
        sorted.iter().flat_map(Key::order_by).collect::<Vec<_>>().join(", ")
    };
    let mut text = format!(
        "/* edax:get_many */ SELECT p.document::text FROM {} AS p",
        tables.qualified(Logical::Projections)
    );
    if !sorted.is_empty() {
        let values: Vec<String> = sorted
            .iter()
            .map(|key| format!("{} AS v{}", key.value, key.index))
            .collect();
        text.push_str(&format!(" CROSS JOIN LATERAL (SELECT {}) AS k", values.join(", ")));
        let decimals: Vec<String> = sorted.iter().filter_map(Key::decimal_column).collect();
        if !decimals.is_empty() {
            text.push_str(&format!(" CROSS JOIN LATERAL (SELECT {}) AS kd", decimals.join(", ")));
        }
    }
    text.push_str(&format!(" WHERE {} ORDER BY {order}", conditions.join(" AND ")));
    if query.offset > 0 {
        let offset = sql.bind(&clamp_bigint(query.offset as u64).to_string());
        text.push_str(&format!(" OFFSET ({offset})::bigint"));
    }
    if let Some(limit) = query.limit {
        // One row past the page says whether another page follows.
        let fetch = clamp_bigint((limit as u64).saturating_add(1));
        let fetch = sql.bind(&fetch.to_string());
        text.push_str(&format!(" LIMIT ({fetch})::bigint"));
    }
    Ok(CompiledQuery {
        sql: text,
        params: sql.params,
    })
}

/// The page the rows of a compiled `spec` make: the stored documents the statement returned, at most
/// one past the page. `next` is minted from the page's last row only when that extra row came back,
/// and never for an unsorted query, exactly as the kernel's applier mints it.
pub fn page(spec: &[u8], rows: Vec<String>) -> Result<Page, Error> {
    let query = edax_cqrs::query_spec(spec)?;
    let mut docs: Vec<Value> = rows
        .iter()
        .map(|row| {
            serde_json::from_str(row).map_err(|e| Error::codec(format_args!("stored document is not JSON: {e}")))
        })
        .collect::<Result<_, _>>()?;
    let more = query.limit.is_some_and(|limit| docs.len() > limit);
    if let Some(limit) = query.limit {
        docs.truncate(limit);
    }
    let next = match (docs.last(), more, query.sort.is_empty()) {
        (Some(last), true, false) => Some(edax_cqrs::encode_cursor(last, &query.sort)?),
        _ => None,
    };
    Ok(Page { rows: docs, next })
}

/// The exact value of decimal text, as SQL over the text expression `text`: `numeric`, or `NULL` for text
/// the kernel does not read as decimal text. The kernel's grammar and bounds (`edax_cqrs::canonical_decimal`):
/// at most [`MAX_DECIMAL_TEXT`] bytes, an optional sign, ASCII digits with at most one point and at least
/// one digit, an optional exponent within 32 bits, at most [`MAX_DECIMAL_DIGITS`] significant digits and at
/// most as many digits in plain notation. The text is read as the kernel reads it, with plain string
/// functions: one sign off the front, a split on the one `e` or `E`, then on the one point. The mantissa and
/// exponent are normalized before the cast, so `"0E+999999999"` reads as `0`, as the kernel reads it. Text
/// outside the grammar reaches each cast as `NULL`, never as text the cast refuses: a server folds a bound
/// text into constants and evaluates the casts while it plans, before any condition filters it. Plain SQL,
/// so the contract needs no database object but tables; `text` is evaluated once.
pub fn decimal_sql(text: &str) -> String {
    let (max_text, max_digits) = (MAX_DECIMAL_TEXT, MAX_DECIMAL_DIGITS);
    let (min_exponent, max_exponent) = (i32::MIN, i32::MAX);
    // The most digits a 32-bit exponent has once its leading zeros are trimmed.
    let exponent_digits = max_exponent.to_string().len();
    // Whether the text expression `x` is ASCII digits only.
    let digits = |x: &str| format!("translate({x}, '0123456789', '') = ''");
    // How many times the one-character `c` occurs in `x`.
    let count = |x: &str, c: &str| format!("length({x}) - length(replace({x}, '{c}', ''))");
    let (integer_and_fraction, magnitude) = (digits("p.integer || p.fraction"), digits("p.magnitude"));
    let (exponents, points) = (count("r.rest", "e"), count("r.rest", "."));
    format!(
        r"(
  SELECT CASE
    WHEN c.coefficient = '' THEN 0::numeric
    WHEN greatest(length(c.coefficient) - 1 + c.scale, 0) + 1 + greatest(-c.scale, 0) > {max_digits} THEN NULL
    ELSE (c.sign || c.coefficient || 'e' || c.scale)::numeric
  END
  FROM (
    SELECT
      b.sign,
      rtrim(b.digits, '0') AS coefficient,
      b.exponent - b.fraction + (length(b.digits) - length(rtrim(b.digits, '0'))) AS scale
    FROM (
      SELECT
        p.sign,
        CASE WHEN {integer_and_fraction} THEN ltrim(p.integer || p.fraction, '0') END AS digits,
        length(p.fraction) AS fraction,
        CASE
          WHEN p.exponent IS NULL THEN 0::bigint
          WHEN p.magnitude = '' OR NOT ({magnitude}) THEN NULL
          WHEN length(ltrim(p.magnitude, '0')) > {exponent_digits} THEN NULL
          ELSE (p.exponent_sign || '0' || ltrim(p.magnitude, '0'))::bigint
        END AS exponent
      FROM (
        SELECT
          m.sign,
          m.integer,
          m.fraction,
          m.exponent,
          CASE WHEN left(m.exponent, 1) IN ('+', '-') THEN left(m.exponent, 1) ELSE '' END AS exponent_sign,
          CASE WHEN left(m.exponent, 1) IN ('+', '-') THEN substr(m.exponent, 2) ELSE m.exponent END AS magnitude
        FROM (
          SELECT
            r.sign,
            split_part(split_part(r.rest, 'e', 1), '.', 1) AS integer,
            split_part(split_part(r.rest, 'e', 1), '.', 2) AS fraction,
            CASE WHEN {exponents} = 1 THEN split_part(r.rest, 'e', 2) END AS exponent
          FROM (
            SELECT
              CASE WHEN left(t.x, 1) IN ('+', '-') THEN left(t.x, 1) ELSE '' END AS sign,
              translate(CASE WHEN left(t.x, 1) IN ('+', '-') THEN substr(t.x, 2) ELSE t.x END, 'E', 'e') AS rest
            FROM (SELECT {text} AS x) AS t
            WHERE octet_length(t.x) <= {max_text}
          ) AS r
          WHERE {exponents} <= 1
            AND {points} <= 1
        ) AS m
      ) AS p
      WHERE p.integer || p.fraction <> ''
        AND length(ltrim(p.integer, '0')) + length(rtrim(p.fraction, '0')) <= {max_digits}
    ) AS b
    WHERE b.exponent BETWEEN {min_exponent} AND {max_exponent}
  ) AS c
)"
    )
}

/// The statement being built: the parameters bound so far.
struct Sql {
    params: Vec<Option<String>>,
}

/// What a path names: the row key column, or a value inside the document.
enum Target {
    RowKey,
    Path(Vec<String>),
}

impl Target {
    fn of(path: &str) -> Target {
        if path == TIEBREAK_PATH {
            Target::RowKey
        } else {
            Target::Path(path.split('.').map(str::to_owned).collect())
        }
    }
}

impl Sql {
    fn new() -> Sql {
        Sql { params: Vec::new() }
    }

    /// Bind `text` and return its placeholder, typed `text`.
    fn bind(&mut self, text: &str) -> String {
        self.params.push(Some(text.to_owned()));
        format!("${}::text", self.params.len())
    }

    /// The `jsonb` value `target` resolves to, SQL `NULL` when it resolves to nothing.
    fn value(&mut self, target: &Target) -> String {
        match target {
            Target::RowKey => "to_jsonb(p.id)".to_owned(),
            // A key holding NUL is a key no stored document has.
            Target::Path(segments) if segments.iter().any(|s| s.contains('\0')) => "NULL::jsonb".to_owned(),
            Target::Path(segments) => {
                let mut expr = "p.document".to_owned();
                for segment in segments {
                    let bound = self.bind(segment);
                    expr = format!("{expr} -> {bound}");
                }
                format!("({expr})")
            }
        }
    }

    /// The boolean `filter` compiles to, never `NULL`.
    fn filter(&mut self, filter: &Filter) -> Result<String, Error> {
        Ok(match filter {
            Filter::Compare {
                path,
                op,
                value,
                collation,
            } => self.compare(&Target::of(path), *op, value, *collation)?,
            Filter::In {
                path,
                values,
                collation,
            } => {
                let target = Target::of(path);
                let arms = values
                    .iter()
                    .map(|value| self.equals(&target, value, *collation))
                    .collect::<Result<Vec<_>, _>>()?;
                any(arms)
            }
            Filter::Contains { path, value } => self.contains(&Target::of(path), value),
            Filter::Exists { path, exists } => {
                let present = self.present(&Target::of(path));
                if *exists {
                    present
                } else {
                    format!("(NOT {present})")
                }
            }
            Filter::And(filters) => {
                let arms = filters.iter().map(|f| self.filter(f)).collect::<Result<Vec<_>, _>>()?;
                all(arms)
            }
            Filter::Or(filters) => {
                let arms = filters.iter().map(|f| self.filter(f)).collect::<Result<Vec<_>, _>>()?;
                any(arms)
            }
            Filter::Not(inner) => format!("(NOT {})", self.filter(inner)?),
        })
    }

    /// Whether `target` resolves to a value other than JSON `null`.
    fn present(&mut self, target: &Target) -> String {
        match target {
            Target::RowKey => "true".to_owned(),
            Target::Path(_) => {
                let value = self.value(target);
                format!("coalesce(jsonb_typeof({value}) <> 'null', false)")
            }
        }
    }

    fn compare(
        &mut self,
        target: &Target,
        op: Operator,
        value: &Value,
        collation: Option<Collation>,
    ) -> Result<String, Error> {
        Ok(match op {
            Operator::Eq => self.equals(target, value, collation)?,
            Operator::Ne => {
                let present = self.present(target);
                let equals = self.equals(target, value, collation)?;
                format!("({present} AND NOT {equals})")
            }
            Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte => {
                let order = Order::of(op);
                match collation {
                    None => self.ordered(target, order, value),
                    Some(Collation::Decimal) => {
                        let decimal = self.decimal_of(target);
                        let bound = self.decimal_bound(value)?;
                        format!("coalesce({decimal} {} {bound}, false)", order.sql())
                    }
                    Some(other) => return Err(unknown_collation(other)),
                }
            }
            Operator::StartsWith => match value.as_str() {
                Some(prefix) if prefix.contains('\0') => "false".to_owned(),
                Some(prefix) if matches!(target, Target::RowKey) => {
                    if prefix.is_empty() {
                        return Ok("true".to_owned());
                    }
                    let lower = self.bind(prefix);
                    let mut bounds = vec![format!("p.id >= {lower}")];
                    if let Some(upper) = prefix_successor(prefix) {
                        let upper = self.bind(&upper);
                        bounds.push(format!("p.id < {upper}"));
                    }
                    // The contracted C-collated key index accepts these bounds even in generic plans.
                    all(bounds)
                }
                Some(prefix) => {
                    let text = self.string_text(target);
                    let bound = self.bind(prefix);
                    format!("coalesce(starts_with({text}, {bound}), false)")
                }
                None => "false".to_owned(),
            },
            other => return Err(Error::codec(format_args!("unknown filter operator {other:?}"))),
        })
    }

    /// Whether `target` equals `value` under `collation`.
    fn equals(&mut self, target: &Target, value: &Value, collation: Option<Collation>) -> Result<String, Error> {
        Ok(match collation {
            None => match (target, value) {
                (_, value) if holds_nul(value) => "false".to_owned(),
                (Target::RowKey, Value::String(s)) => {
                    let bound = self.bind(s);
                    format!("p.id = {bound}")
                }
                (Target::RowKey, _) => "false".to_owned(),
                (Target::Path(_), value) => {
                    let resolved = self.value(target);
                    let bound = self.bind(&exact_json(value));
                    format!("coalesce({resolved} = ({bound})::jsonb, false)")
                }
            },
            Some(Collation::Decimal) => {
                let decimal = self.decimal_of(target);
                let bound = self.decimal_bound(value)?;
                format!("coalesce({decimal} = {bound}, false)")
            }
            Some(other) => return Err(unknown_collation(other)),
        })
    }

    /// The decimal value of `target` when it is decimal text, else `NULL`.
    fn decimal_of(&mut self, target: &Target) -> String {
        let text = self.string_text(target);
        decimal_sql(&text)
    }

    /// A decimal comparand, validated by the kernel already, bound at its canonical value.
    fn decimal_bound(&mut self, value: &Value) -> Result<String, Error> {
        let text = value
            .as_str()
            .ok_or_else(|| Error::codec("a decimal comparand must be decimal text"))?;
        let canonical = canonical_decimal(text)?;
        let bound = self.bind(&canonical);
        Ok(format!("({bound})::numeric"))
    }

    /// The text of `target` when it is a string, else `NULL`.
    fn string_text(&mut self, target: &Target) -> String {
        match target {
            Target::RowKey => "p.id".to_owned(),
            Target::Path(_) => {
                let value = self.value(target);
                format!("({})", string_of(&value))
            }
        }
    }

    /// `target` against `value` under a plain ordering: both numbers, both strings or both bools.
    fn ordered(&mut self, target: &Target, order: Order, value: &Value) -> String {
        match value {
            Value::Number(n) => match target {
                Target::RowKey => "false".to_owned(),
                Target::Path(_) => {
                    let resolved = self.value(target);
                    let bound = self.bind(&exact_number(n));
                    format!(
                        "coalesce({} AND ({resolved})::numeric {} ({bound})::numeric, false)",
                        is_type(&resolved, "number"),
                        order.sql()
                    )
                }
            },
            Value::String(s) => {
                let text = self.string_text(target);
                let (order, bound) = string_bound(order, s);
                let bound = self.bind(bound);
                format!(
                    "coalesce({text} COLLATE \"C\" {} {bound} COLLATE \"C\", false)",
                    order.sql()
                )
            }
            Value::Bool(b) => match target {
                Target::RowKey => "false".to_owned(),
                Target::Path(_) => {
                    let resolved = self.value(target);
                    let bound = self.bind(&b.to_string());
                    format!(
                        "coalesce({} AND ({resolved})::boolean {} ({bound})::boolean, false)",
                        is_type(&resolved, "boolean"),
                        order.sql()
                    )
                }
            },
            // An array, an object or null orders against nothing.
            _ => "false".to_owned(),
        }
    }

    fn contains(&mut self, target: &Target, value: &Value) -> String {
        match target {
            Target::RowKey => "false".to_owned(),
            Target::Path(_) if holds_nul(value) => "false".to_owned(),
            Target::Path(_) => {
                let resolved = self.value(target);
                let bound = self.bind(&exact_json(value));
                format!(
                    "coalesce({} AND EXISTS (SELECT 1 FROM jsonb_array_elements({resolved}) AS e(x) WHERE e.x = ({bound})::jsonb), false)",
                    is_type(&resolved, "array")
                )
            }
        }
    }
}

/// A plain ordering operator.
#[derive(Clone, Copy)]
enum Order {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Order {
    fn of(op: Operator) -> Order {
        match op {
            Operator::Gte => Order::Gte,
            Operator::Lt => Order::Lt,
            Operator::Lte => Order::Lte,
            _ => Order::Gt,
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Order::Gt => ">",
            Order::Gte => ">=",
            Order::Lt => "<",
            Order::Lte => "<=",
        }
    }
}

/// The ordering and bound a string comparison runs with. A comparand holding NUL compares as the text
/// before its first NUL: no stored string holds NUL, so a stored string greater than that text is
/// greater than the comparand, and one not greater is less.
fn string_bound(order: Order, s: &str) -> (Order, &str) {
    match s.split_once('\0') {
        None => (order, s),
        Some((before, _)) => match order {
            Order::Gt | Order::Gte => (Order::Gt, before),
            Order::Lt | Order::Lte => (Order::Lte, before),
        },
    }
}

/// The least string after every extension of `prefix` in Unicode scalar (and UTF-8 byte) order.
fn prefix_successor(prefix: &str) -> Option<String> {
    for (index, ch) in prefix.char_indices().rev() {
        let next = match ch {
            '\u{10ffff}' => continue,
            '\u{d7ff}' => '\u{e000}',
            ch => char::from_u32(u32::from(ch) + 1)?,
        };
        let mut upper = prefix.get(..index)?.to_owned();
        upper.push(next);
        return Some(upper);
    }
    None
}

/// Whether the `jsonb` expression `expr` is of the JSON type `json_type`, as `jsonb_typeof` names it.
fn is_type(expr: &str, json_type: &str) -> String {
    format!("jsonb_typeof({expr}) = '{json_type}'")
}

/// The `jsonb` expression `expr` cast to `cast` when it is of the JSON type `json_type`, else `NULL`.
fn typed(expr: &str, json_type: &str, cast: &str) -> String {
    format!("CASE WHEN {} THEN ({expr})::{cast} END", is_type(expr, json_type))
}

/// The text of the `jsonb` expression `expr` when it is a string, else `NULL`.
fn string_of(expr: &str) -> String {
    format!("CASE WHEN {} THEN {expr} #>> '{{}}' END", is_type(expr, "string"))
}

fn all(arms: Vec<String>) -> String {
    if arms.is_empty() {
        "true".to_owned()
    } else {
        format!("({})", arms.join(" AND "))
    }
}

fn any(arms: Vec<String>) -> String {
    if arms.is_empty() {
        "false".to_owned()
    } else {
        format!("({})", arms.join(" OR "))
    }
}

fn unknown_collation(collation: Collation) -> Error {
    Error::codec(format_args!("unknown collation {collation:?}"))
}

/// One key of the effective sort, as the statement reads it: its value (`k.v<index>`), and under the
/// decimal collation its decimal reading (`kd.d<index>`).
struct Key {
    index: usize,
    row_key: bool,
    desc: bool,
    decimal: bool,
    /// The expression the lateral join binds to `k.v<index>`.
    value: String,
}

impl Key {
    fn new(sql: &mut Sql, index: usize, key: &SortKey) -> Key {
        let target = Target::of(&key.path);
        let decimal = key.collation.is_some();
        let row_key = matches!(target, Target::RowKey) && !decimal;
        Key {
            index,
            row_key,
            desc: key.desc,
            decimal,
            value: sql.value(&target),
        }
    }

    fn v(&self) -> String {
        format!("k.v{}", self.index)
    }

    fn d(&self) -> String {
        format!("kd.d{}", self.index)
    }

    fn decimal_column(&self) -> Option<String> {
        self.decimal
            .then(|| format!("{} AS d{}", decimal_sql(&string_of(&self.v())), self.index))
    }

    /// The type rank of the value, in the kernel's type order: a missing path ranks as null, the last.
    fn rank(&self) -> String {
        let last = TYPE_ORDER.len() - 1;
        let arms: Vec<String> = TYPE_ORDER
            .iter()
            .enumerate()
            .map(|(rank, json_type)| {
                if rank == last {
                    format!("ELSE {rank}")
                } else {
                    format!("WHEN '{json_type}' THEN {rank}")
                }
            })
            .collect();
        format!("(CASE jsonb_typeof({}) {} END)", self.v(), arms.join(" "))
    }

    fn number(&self) -> String {
        format!("({})", typed(&self.v(), "number", "numeric"))
    }

    /// The text of a string value: under the decimal collation only of text that is not decimal.
    fn text(&self) -> String {
        if self.row_key {
            return "p.id".to_owned();
        }
        let v = self.v();
        let other = if self.decimal {
            format!(" AND {} IS NULL", self.d())
        } else {
            String::new()
        };
        format!(
            "(CASE WHEN {}{other} THEN {v} #>> '{{}}' END) COLLATE \"C\"",
            is_type(&v, "string")
        )
    }

    /// Whether a string value is other text, not decimal text: decimal text sorts first.
    fn other_text(&self) -> String {
        format!("({} IS NULL)", self.d())
    }

    fn boolean(&self) -> String {
        format!("({})", typed(&self.v(), "boolean", "boolean"))
    }

    /// The `ORDER BY` terms of this key: its rank, then each type's value. Within one rank every other
    /// type's column is `NULL` for every row, so the `NULL` placement never decides.
    fn order_by(&self) -> Vec<String> {
        let dir = if self.desc { " DESC" } else { "" };
        let terms = if self.row_key {
            vec!["p.id".to_owned()]
        } else if self.decimal {
            vec![
                self.rank(),
                self.number(),
                self.other_text(),
                self.d(),
                self.text(),
                self.boolean(),
            ]
        } else {
            vec![self.rank(), self.number(), self.text(), self.boolean()]
        };
        terms.into_iter().map(|term| format!("{term}{dir}")).collect()
    }
}

/// How a row's key compares with a cursor value.
#[derive(Clone, Copy, PartialEq)]
enum Side {
    Before,
    Tied,
    After,
}

/// The condition that a row's key holds `side` of the cursor value `t`, in ascending order.
fn key_side(sql: &mut Sql, key: &Key, t: &Value, side: Side) -> String {
    let rank = type_rank(t);
    if key.row_key {
        // The row key is always a string.
        let string_rank = type_rank(&Value::String(String::new()));
        return match t {
            Value::String(s) => string_side(sql, "p.id", s, side),
            _ => constant(match side {
                Side::Before => string_rank < rank,
                Side::Tied => false,
                Side::After => string_rank > rank,
            }),
        };
    }
    let within = match t {
        Value::Number(n) => {
            let bound = sql.bind(&exact_number(n));
            format!("{} {} ({bound})::numeric", key.number(), side_sql(side))
        }
        Value::String(s) if key.decimal => decimal_side(sql, key, s, side),
        Value::String(s) => string_side(sql, &key.text(), s, side),
        Value::Bool(b) => {
            let bound = sql.bind(&b.to_string());
            format!("{} {} ({bound})::boolean", key.boolean(), side_sql(side))
        }
        // Two arrays, two objects or two nulls tie.
        _ => constant(side == Side::Tied),
    };
    let same = format!("{} = {rank}", key.rank());
    let across = match side {
        Side::Before => format!("{} < {rank} OR ", key.rank()),
        Side::Tied => String::new(),
        Side::After => format!("{} > {rank} OR ", key.rank()),
    };
    format!("coalesce({across}({same} AND {within}), false)")
}

fn side_sql(side: Side) -> &'static str {
    match side {
        Side::Before => "<",
        Side::Tied => "=",
        Side::After => ">",
    }
}

fn constant(holds: bool) -> String {
    if holds { "true" } else { "false" }.to_owned()
}

/// A string column against the string `s`, folding a NUL as [`string_bound`] does.
fn string_side(sql: &mut Sql, text: &str, s: &str, side: Side) -> String {
    match (s.split_once('\0'), side) {
        (None, side) => {
            let bound = sql.bind(s);
            format!("{text} {} {bound} COLLATE \"C\"", side_sql(side))
        }
        (Some(_), Side::Tied) => "false".to_owned(),
        (Some((before, _)), Side::After) => {
            let bound = sql.bind(before);
            format!("{text} > {bound} COLLATE \"C\"")
        }
        (Some((before, _)), Side::Before) => {
            let bound = sql.bind(before);
            format!("{text} <= {bound} COLLATE \"C\"")
        }
    }
}

/// A string under the decimal collation against the cursor string `s`: decimal text by value, before
/// all other text, which orders by code point.
fn decimal_side(sql: &mut Sql, key: &Key, s: &str, side: Side) -> String {
    let other = key.other_text();
    match canonical_decimal(s) {
        Ok(canonical) => {
            let bound = sql.bind(&canonical);
            let value = format!("{} {} ({bound})::numeric", key.d(), side_sql(side));
            match side {
                Side::Before | Side::Tied => format!("(NOT {other} AND {value})"),
                Side::After => format!("({other} OR {value})"),
            }
        }
        Err(_) => {
            let text = string_side(sql, &key.text(), s, side);
            match side {
                Side::Before => format!("(NOT {other} OR {text})"),
                Side::Tied | Side::After => format!("({other} AND {text})"),
            }
        }
    }
}

/// The condition that a row sorts strictly after the cursor `tuple` under `keys`.
fn keyset(sql: &mut Sql, keys: &[Key], tuple: &[Value]) -> String {
    let mut arms = Vec::new();
    for (i, (key, t)) in keys.iter().zip(tuple).enumerate() {
        let mut terms: Vec<String> = keys
            .iter()
            .zip(tuple)
            .take(i)
            .map(|(earlier, t)| key_side(sql, earlier, t, Side::Tied))
            .collect();
        let after = if key.desc { Side::Before } else { Side::After };
        terms.push(key_side(sql, key, t, after));
        arms.push(all(terms));
    }
    any(arms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Tables {
        Tables::default()
    }

    #[test]
    fn compile_shapes() {
        let compiled = compile(
            &schema(),
            "accounts",
            br#"{"filter":{"compare":{"path":"a.b","op":"eq","value":1}}}"#,
        )
        .unwrap();
        assert_eq!(
            compiled.sql,
            "/* edax:get_many */ SELECT p.document::text FROM \"public\".\"edax_projections\" AS p WHERE p.projection = $1::text AND coalesce((p.document -> $2::text -> $3::text) = ($4::text)::jsonb, false) ORDER BY p.id"
        );
        assert_eq!(
            compiled.params,
            vec![
                Some("accounts".into()),
                Some("a".into()),
                Some("b".into()),
                Some("1".into())
            ]
        );
        let sorted = compile(
            &schema(),
            "accounts",
            br#"{"sort":[{"path":"n","desc":true}],"limit":2,"offset":1}"#,
        )
        .unwrap();
        assert!(sorted
            .sql
            .contains("CROSS JOIN LATERAL (SELECT (p.document -> $2::text) AS v0, to_jsonb(p.id) AS v1) AS k"));
        assert!(sorted
            .sql
            .ends_with("p.id OFFSET ($3::text)::bigint LIMIT ($4::text)::bigint"));
        assert_eq!(sorted.params[3], Some("3".into()), "one past the limit");
    }

    #[test]
    fn compile_refuses_what_the_kernel_refuses() {
        for (name, projection, spec, want) in [
            (
                "misspelled_field",
                "a",
                &br#"{"limt":1}"#[..],
                "codec: unknown field `limt`, expected one of `filter`, `sort`, `offset`, `limit`, `after` at line 1 column 7",
            ),
            (
                "cursor_without_sort",
                "a",
                br#"{"after":"x"}"#,
                "codec: an unsorted query has no order to resume from, so it takes no cursor",
            ),
            ("nul_in_projection", "a\0", b"{}", "codec: projection holds a NUL character"),
        ] {
            let got = compile(&schema(), projection, spec).map(|_| ()).map_err(|e| e.to_string());
            assert_eq!(got, Err(want.to_owned()), "{name}");
        }
    }

    #[test]
    fn nul_comparands_fold() {
        let eq = compile(
            &schema(),
            "a",
            br#"{"filter":{"compare":{"path":"s","op":"eq","value":"a\u0000"}}}"#,
        )
        .unwrap();
        assert!(eq.sql.contains("AND false ORDER BY"), "{}", eq.sql);
        let gt = compile(
            &schema(),
            "a",
            br#"{"filter":{"compare":{"path":"s","op":"gte","value":"ab\u0000c"}}}"#,
        )
        .unwrap();
        assert!(gt.sql.contains(" > $"), "{}", gt.sql);
        assert!(gt.params.contains(&Some("ab".into())));
    }

    #[test]
    fn page_branches() {
        let rows = |n: usize| {
            (0..n)
                .map(|i| format!(r#"{{"id":"r{i}","n":{i}}}"#))
                .collect::<Vec<_>>()
        };
        let sorted = br#"{"sort":[{"path":"n"}],"limit":2}"#;
        let full = page(sorted, rows(3)).unwrap();
        assert_eq!(full.rows.len(), 2);
        assert!(full.next.is_some(), "an extra row came back");
        assert_eq!(page(sorted, rows(2)).unwrap().next, None, "no extra row");
        assert_eq!(page(br#"{"limit":2}"#, rows(3)).unwrap().next, None, "unsorted");
        assert_eq!(
            page(br#"{"sort":[{"path":"n"}],"limit":0}"#, rows(1)).unwrap().next,
            None,
            "limit 0"
        );
        assert_eq!(
            page(br#"{"sort":[{"path":"n"}]}"#, rows(3)).unwrap().rows.len(),
            3,
            "no limit"
        );
    }
}
