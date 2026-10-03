# The filter kernel — `edax_cqrs::filter`

A sealed abstract syntax tree (AST) plus one reference evaluator. The bindings build filters and
call `eval_json`. The match decision is made only here, so the null and missing-path rules cannot
drift between languages. The golden vector is `testdata/golden/filter.json`.

## The AST

```mermaid
classDiagram
    class Filter {
        <<enumeration>>
    }
    class Compare {
        +String path
        +Operator op
        +Value value
    }
    class In {
        +String path
        +Vec~Value~ values
    }
    class Contains {
        +String path
        +Value value
    }
    class Exists {
        +String path
        +bool exists
    }
    class And
    class Or
    class Not
    class Operator {
        <<enumeration>>
        Eq
        Ne
        Gt
        Gte
        Lt
        Lte
        StartsWith
    }

    Filter <|-- Compare : leaf
    Filter <|-- In : leaf
    Filter <|-- Contains : leaf
    Filter <|-- Exists : leaf
    Filter <|-- And : branch
    Filter <|-- Or : branch
    Filter <|-- Not : branch
    Compare --> Operator : selects with op
    And --> Filter : holds arms
    Or --> Filter : holds arms
    Not --> Filter : holds one inner
```

Each variant serializes to an externally-tagged, snake_case JSON object. That exact shape is the
wire contract between the software development kits (SDKs), and a unit test pins it.

| Variant | Wire form | Matches when |
|---|---|---|
| `Compare` | `{"compare": {"path", "op", "value"}}` | the path is present, non-null, and the operator holds |
| `In` | `{"in": {"path", "values"}}` | the path is present, non-null, and equals one of `values` |
| `Contains` | `{"contains": {"path", "value"}}` | the path resolves to an **array** that contains `value` |
| `Exists` | `{"exists": {"path", "exists"}}` | presence of the path equals the `exists` flag |
| `And` | `{"and": [...]}` | every arm matches. `And([])` is **true** |
| `Or` | `{"or": [...]}` | any arm matches. `Or([])` is **false** |
| `Not` | `{"not": {...}}` | the inner filter does not match |

`Contains` is the mirror of `In`: `In` tests a scalar path against a given set, `Contains` tests
whether the array at a path includes a given value.

`Operator` is `#[non_exhaustive]`, so a new operator is additive. `Filter` is not. A `match` over
its variants must stay exhaustive. A new variant is then a compile error rather than a wildcard's
silent default.

## Operators

| Operator | Rust constructor | Python constructor | Holds when | Falls to `false` when |
|---|---|---|---|---|
| `Eq` | `eq` | `eq` | values are equal — numeric for numbers at every depth, structural otherwise | — |
| `Ne` | `ne` | `ne` | values are not equal | the path is missing or null |
| `Gt` / `Gte` / `Lt` / `Lte` | `gt` / `gte` / `lt` / `lte` | same names | both sides are numbers, both strings, or both bools, and the ordering holds | any other pairing — mismatched types, array, object |
| `StartsWith` | `starts_with` | `starts_with` | both sides are strings and the document value has the prefix | either side is not a string |

Ordering is defined only *within* a type. Numbers compare numerically, so `1` and `1.0` are equal;
two integers compare exactly, with no `f64` promotion above 2^53. Strings compare by Unicode code
point, which is UTF-8 byte order. `"Z"` sorts before `"a"`, and U+FF71 sorts after U+1F980.
JavaScript, Java and C# order that last pair the other way, by UTF-16 code units. Strings are not
normalized, so the NFC and NFD spellings of `café` are unequal. Bools order `false < true`, the rank
the read side's sort gives them, so a sort and a range filter agree on every orderable pair.

Equality is numeric at every depth, the same rule as at the top level: `eq(p, [1.0])` matches
`[1]`, and `eq(p, {"x": 2.0})` matches `{"x": 2}`. `In` and `Contains` use the same equality.

**Timestamps.** No special operator is needed, but string order is chronological only for one
fixed-width form in one zone. Store timestamps as UTC with a `Z` suffix and a fixed fraction width
(`2024-01-01T00:00:00.000000Z`), or as epoch integers. Two common forms order wrongly, silently:

- A form that omits a zero fraction. msgspec, which the Python SDK encodes with, writes
  `2024-01-01T00:00:00Z` for a whole second and `2024-01-01T00:00:00.500000Z` otherwise, and the
  first sorts *after* the second because `Z` follows `.`.
- A form with an offset. `2024-01-01T00:30:00+01:00` sorts by wall time, after `2024-01-01T00:00:00Z`,
  although that instant is 23:30Z the day before.

Normalize to the fixed UTC form before a value reaches a document or a filter.
`testdata/golden/filter.json` pins both wrong orderings.

## What the kernel refuses

`eval_json`, and every query that carries a filter, reads the filter strictly. Each of these is a
terminal `codec` error, never a filter that silently means something else:

| Input | Why it is refused |
|---|---|
| an unknown field in any node, `{"compare": {..., "vlaue": 1}}` | a typo would otherwise be a no-op |
| a `null` comparand for `Compare` or inside `In` | no present value equals it, so the leaf matches nothing; ask `Exists(path, false)` for a missing or null path |
| a path with an empty segment: `""`, `"a."`, `".a"`, `"a..b"` | a backend translating paths to SQL has no rule for one |
| an integer literal wider than 64 bits | it would be compared as the nearest float, so `eq(x, 2**70)` would match `2**70 + 1` |

`Contains(path, null)` stays valid: it asks whether the array holds a `null` element. A path is
dot-delimited object keys, so a key containing `.` cannot be addressed. A numeric segment is a key,
never an array index: `tags.0` reads `{"tags": {"0": ...}}`, not `{"tags": [...]}`.
`Filter::validate` holds the rule, and `testdata/golden/filter.json` pins each refusal.

## The null and missing-path rule

The semantics are **two-valued**: there is no third "unknown". A path predicate is satisfied only
when the path is present, non-null, and the comparison holds. Everything follows from one helper:

```rust
/// A path is "present" iff it resolves to a non-null value.
fn present<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    match resolve(doc, path) {
        Some(v) if !v.is_null() => Some(v),
        _ => None,
    }
}
```
<sup>`edax_cqrs::filter::eval`</sup>

A leaf that cannot find a value returns `false`. `Not` then negates that `false` to `true`. So `Ne`
and `Not(Eq)` deliberately differ.

| Document | `Eq(p, "x")` | `Ne(p, "x")` | `Not(Eq(p, "x"))` | `Exists(p, true)` | `Exists(p, false)` |
|---|---|---|---|---|---|
| `{"p": "x"}` | true | false | false | true | false |
| `{"p": "y"}` | false | true | true | true | false |
| `{"p": null}` | false | **false** | **true** | false | true |
| `{}` (missing) | false | **false** | **true** | false | true |
| `{"p": {"q": 1}}`, path `p.q.r` | false | **false** | **true** | false | true |

The last row is the same rule one level deeper. `resolve` walks dot-delimited segments, and every
intermediate segment must be an object. A missing key or a non-object segment resolves to nothing.

### Worked example

A read model of accounts, where a closed account carries `closed_at` and an open one leaves it
`null`:

```python
from edax.filter import eq, matches, ne, not_

open_account = {"id": "acct-1", "status": "settled", "closed_at": None}

matches(ne("closed_at", "2024-01-01"), open_account)  # False
matches(not_(eq("closed_at", "2024-01-01")), open_account)  # True
```

Read it this way. "The closing date is something other than 2024-01-01" is not true of an account
with no closing date. "It is not the case that the closing date is 2024-01-01" is true of it. Use
`Not(Eq(...))` for "everything except", and `Ne` for "has a value, and that value differs". Both
SDKs pin this pair — the two `{}`-document cases in `testdata/golden/filter.json`, and
`test_filter.py::test_null_semantics`.

So `and_(eq("status", "settled"), ne("closed_at", X))` drops every open account without warning,
because the second arm is false wherever `closed_at` is null. Write `not_(eq("closed_at",
X))`, or add `exists("closed_at", False)` as an explicit arm.

## One evaluation, step by step

```mermaid
flowchart TB
    S["eval Compare with path, op, value"] --> R["resolve: take the next dot-delimited segment"]
    R --> Q1{"is the current node an object<br/>that has this key?"}
    Q1 -->|no| F["leaf is false"]
    Q1 -->|"yes, segments remain"| R
    Q1 -->|"yes, last segment"| Q2{"is the resolved value null?"}
    Q2 -->|yes| F
    Q2 -->|no| A["apply op to the document value<br/>and the filter value"]
    A --> Q3{"are both sides<br/>the same orderable type?"}
    Q3 -->|no| F
    Q3 -->|yes| Q4{"does the comparison hold?"}
    Q4 -->|no| F
    Q4 -->|yes| T["leaf is true"]
    F --> N["a wrapping Not flips this to true"]
```

`eval` dispatches on the variant and recurses for `And` / `Or` / `Not`. `eval_json` is the boundary
the bindings call. It parses filter and document from JSON bytes and refuses what the section above
lists. It returns the verdict, and reports every failure as a `codec` error.

## The verdict is the kernel's; the dialect is the backend's

A backend is expected to translate a `Filter` into its own query language, so the database does the
work instead of the process. What it may not do is decide the *match*. A backend whose native `!=`
matches a missing column would answer a `Ne` leaf differently from every other service reading the
same model. A translation is correct only when it reproduces this evaluator's answer for every
document, the null and missing rows included. That is why `ProjectionStore.get_many` is optional. A
backend that cannot reproduce the semantics should leave it unimplemented rather than approximate
it.

## The two surfaces, side by side

| | Rust — `edax_cqrs::filter` | Python — `edax.filter` |
|---|---|---|
| Filter type | the typed `Filter` enum | `Filter = dict[str, Any]`, the kernel's canonical form |
| Comparisons | `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `starts_with` | same names |
| Set membership | `is_in(path, vec![...])` | `in_(path, *values)` |
| Array membership | `contains(path, value)` | `contains(path, value)` |
| Presence | `exists(path, bool)` — flag required | `exists(path, present=True)` — flag defaults to `True` |
| Connectives | `and(Vec<Filter>)`, `or(Vec<Filter>)`, `not(Filter)` | `and_(*filters)`, `or_(*filters)`, `not_(filter)` |
| Evaluate | `eval(&Filter, &Value) -> bool` | `matches(filter, document) -> bool` |
| JSON boundary | `eval_json(&[u8], &[u8]) -> Result<bool, Error>` | used internally by `matches` |
| Validation | `Filter::validate()` | through `matches` |
| Path helpers | `resolve`, `value_cmp` (public, shared with the `query` module next door) | not exposed |

```rust
use edax_cqrs::filter::{and, eq, eval, gt};
use serde_json::json;

let doc = json!({"status": "open", "total": 5000});
let f = and(vec![eq("status", json!("open")), gt("total", json!(4200))]);
assert!(eval(&f, &doc));
```
<sup>the `and_all_true` case of the `edax-cqrs` filter suite; the same filter and document are a case in `testdata/golden/filter.json`</sup>

```python
from edax.filter import and_, eq, gte
from edax.query import sort_key

only = and_(gte("balance", 100), eq("status", "active"))
order = [sort_key("balance", desc=True), sort_key("id")]  # richest first, id as the tiebreak

page = await store.get_many(filter=only, sort=order, limit=2)
```
<sup>`examples/py/11_projection_query.py`</sup>

The Python constructors are the only part of filtering the SDK owns: which dict key and which op
name each one emits. A wrong key would pass the golden vectors unnoticed, so a separate table test
pins every constructor's output.

## See also

- [edax-core](./core.md) — hashing, backoff, the clock and the error taxonomy.
- [cqrs.md](./cqrs.md) — the rest of the crate: the query that composes a filter with sorting and
  paging, and the command side's idempotency decision.
- [../sdk/projections.md](../sdk/projections.md) — the host view: read models, filters and cursors.
- [golden-vectors.md](./golden-vectors.md) — how `filter.json` is generated and asserted.
