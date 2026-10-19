"""Keep unsafe integer tokens intact until the native ingestion boundary reads them."""

from __future__ import annotations

import json
from typing import Any

import pytest
from conftest import golden
from edax import _edax


@pytest.mark.parametrize("case", golden("ingestion_numbers.json"), ids=lambda case: case["name"])
def test_ingestion_raw_numbers(case: dict[str, Any]) -> None:
    def replay() -> Any:
        if case["op"] == "plan":
            return json.loads(_edax.ingest_plan(case["mode"], case["inputs_json"], "{}", "{}"))
        if case["op"] == "projection_sealed_input":
            return json.loads(
                _edax.projection_sealed_input(2**63 - 1, json.dumps(case["envelope"]), case["event_json"])
            )
        assert case["op"] == "projection_keys"
        return json.loads(_edax.projection_keys(case["request_json"]))

    if case["error"] is not None:
        with pytest.raises(RuntimeError) as failure:
            replay()
        assert str(failure.value) == case["error"]
    else:
        assert replay() == case["want"]
