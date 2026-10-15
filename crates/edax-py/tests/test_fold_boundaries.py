"""The fold validation and PostgreSQL contracts agree across native SDK boundaries."""

from typing import Any

import pytest
from conftest import golden
from edax import _edax, _json


@pytest.mark.parametrize("fixture", ["fold_boundaries.json", "postgres_fold.json"])
def test_fold_boundary_golden(fixture: str) -> None:
    for case in golden(fixture):

        def run(case: dict[str, Any] = case) -> Any:
            match case["op"]:
                case "keys":
                    return _json.decode(_edax.fold_keys(_json.encode(case["request"])))
                case "source":
                    return _edax.fold_check_source(
                        _json.encode(case["contract"]), _json.encode(case["proof"]), case["cursor"]
                    )
                case "eligible":
                    return _edax.fold_intent_eligible(
                        _json.encode(case["manifest"]), _json.encode(case["intent"])
                    )
                case "sql":
                    return _json.decode(_edax.postgres_fold_statements(_json.encode(case["config"])))
                case _:
                    pytest.fail(f"unknown fold boundary operation: {case['op']}")

        if case["error"] is not None:
            with pytest.raises(RuntimeError) as caught:
                run()
            assert str(caught.value) == case["error"], case["name"]
        else:
            assert run() == case["want"], case["name"]
