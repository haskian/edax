"""The filter constructors and :func:`edax.filter.matches`. ``filter.json`` pins the kernel's match
semantics from JSON; the table below pins the dict each constructor emits, which is the only part
the SDK owns — a wrong key or op name would sail past the golden vectors."""

from typing import Any

import edax
import pytest
from conftest import golden
from edax import filter as F

# One realistic read-model row, shared by every case; each case overrides only the path it probes.
ACCOUNT: dict[str, Any] = {
    "id": "acct-1",
    "owner": {"name": "Ada Lovelace", "email": "ada@example.com"},
    "status": "settled",
    "balance_cents": 4200,
    "currency": "EUR",
    "tags": ["vip", "eu"],
    "closed_at": None,
}


def test_golden_filter() -> None:
    """The match decision per row, and the refusals: a ``None`` comparand, an empty path segment, a typo
    or an unknown node is the terminal ``CodecError``, never a filter that silently matches nothing."""
    for c in golden("filter.json"):
        if c["error_code"] is not None:
            with pytest.raises(edax.CodecError) as caught:
                F.matches(c["filter"], c["document"])
            assert caught.value.code == c["error_code"], c["name"]
            continue
        assert F.matches(c["filter"], c["document"]) == c["expected"], c["name"]


# One constructor per row, with the canonical form it must build beside it. Rows run to 156
# columns; reflowed, the table alternates one-line and six-line rows and stops reading as a table.
@pytest.mark.parametrize(
    "branch, built, want_canonical, want_match",
    [
        ("eq", F.eq("status", "settled"), {"compare": {"path": "status", "op": "eq", "value": "settled"}}, True),
        ("ne", F.ne("currency", "USD"), {"compare": {"path": "currency", "op": "ne", "value": "USD"}}, True),
        ("gt_above", F.gt("balance_cents", 4199), {"compare": {"path": "balance_cents", "op": "gt", "value": 4199}}, True),
        # the comparison boundary: gt excludes the equal value, gte includes it
        ("gt_at_boundary", F.gt("balance_cents", 4200), {"compare": {"path": "balance_cents", "op": "gt", "value": 4200}}, False),
        ("gte_at_boundary", F.gte("balance_cents", 4200), {"compare": {"path": "balance_cents", "op": "gte", "value": 4200}}, True),
        ("lt_at_boundary", F.lt("balance_cents", 4200), {"compare": {"path": "balance_cents", "op": "lt", "value": 4200}}, False),
        ("lte_at_boundary", F.lte("balance_cents", 4200), {"compare": {"path": "balance_cents", "op": "lte", "value": 4200}}, True),
        ("starts_with_nested_path", F.starts_with("owner.email", "ada@"), {"compare": {"path": "owner.email", "op": "starts_with", "value": "ada@"}}, True),
        ("in_one_value", F.in_("currency", "EUR"), {"in": {"path": "currency", "values": ["EUR"]}}, True),
        ("in_many_values", F.in_("status", "open", "settled"), {"in": {"path": "status", "values": ["open", "settled"]}}, True),
        ("in_no_values", F.in_("status"), {"in": {"path": "status", "values": []}}, False),
        ("contains", F.contains("tags", "vip"), {"contains": {"path": "tags", "value": "vip"}}, True),
        ("exists_default_true", F.exists("owner.name"), {"exists": {"path": "owner.name", "exists": True}}, True),
        # a present-but-null path is absent to the kernel's two-valued rules
        ("exists_false_on_null", F.exists("closed_at", present=False), {"exists": {"path": "closed_at", "exists": False}}, True),
        (
            "and_",
            F.and_(F.eq("status", "settled"), F.gt("balance_cents", 1000)),
            {"and": [
                {"compare": {"path": "status", "op": "eq", "value": "settled"}},
                {"compare": {"path": "balance_cents", "op": "gt", "value": 1000}},
            ]},
            True,
        ),
        (
            "or_short_circuits_to_the_true_arm",
            F.or_(F.eq("status", "open"), F.eq("currency", "EUR")),
            {"or": [
                {"compare": {"path": "status", "op": "eq", "value": "open"}},
                {"compare": {"path": "currency", "op": "eq", "value": "EUR"}},
            ]},
            True,
        ),
        ("not_", F.not_(F.eq("status", "open")), {"not": {"compare": {"path": "status", "op": "eq", "value": "open"}}}, True),
        # A filter built from an empty list of conditions: the identity of each connective, so code
        # that folds an optional set of predicates degrades to "everything" / "nothing", not an error.
        ("and_no_arms_matches_everything", F.and_(), {"and": []}, True),
        ("or_no_arms_matches_nothing", F.or_(), {"or": []}, False),
    ],
)  # fmt: skip
def test_constructor_canonical_form(branch: str, built: dict, want_canonical: dict, want_match: bool) -> None:
    assert built == want_canonical
    assert F.matches(built, ACCOUNT) is want_match  # the kernel accepts what we built


def test_null_semantics() -> None:
    # ne on a missing path is false; not_(eq) on a missing path is true.
    assert F.matches(F.ne("status", "x"), {}) is False
    assert F.matches(F.not_(F.eq("status", "x")), {}) is True
