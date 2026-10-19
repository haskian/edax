"""Partition admission and SQL contracts cross the generated ABI unchanged."""

import json
from typing import Any

import pytest
from conftest import golden
from edax import _native


@pytest.mark.parametrize("case", golden("ingestion_partition.json"))
def test_admission(case: dict[str, Any]) -> None:
    actual = _native.ingest_target_plan(
        json.dumps(case["contract"]), json.dumps(case["inputs"]), "{}", "{}", int(case["floor"])
    )
    assert json.loads(actual) == case["want"]


@pytest.mark.parametrize("case", golden("postgres_ingest_partition.json"))
def test_sql(case: dict[str, Any]) -> None:
    assert json.loads(_native.postgres_ingest_statements(json.dumps(case["config"]))) == case["want"]
