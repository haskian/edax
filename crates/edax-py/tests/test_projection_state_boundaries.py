"""The projection state validation and PostgreSQL contracts agree across native SDK boundaries."""

from typing import Any

import pytest
from conftest import golden
from edax import _edax, _json


@pytest.mark.parametrize("fixture", ["projection_state_boundaries.json", "postgres_projection_state.json"])
def test_projection_state_boundary_golden(fixture: str) -> None:
    for case in golden(fixture):

        def run(case: dict[str, Any] = case) -> Any:
            match case["op"]:
                case "keys":
                    return _json.decode(_edax.projection_keys(_json.encode(case["request"])))
                case "lock":
                    return _json.decode(_edax.projection_reads(_json.encode(case["request"])))["lock"]
                case "source":
                    return _edax.projection_check_source(
                        _json.encode(case["contract"]), _json.encode(case["proof"]), case["cursor"]
                    )
                case "eligible":
                    return _edax.projection_intent_eligible(
                        _json.encode(case["manifest"]), _json.encode(case["intent"])
                    )
                case "sealed_input":
                    event = None if case["event"] is None else _json.encode(case["event"])
                    return _json.decode(
                        _edax.projection_sealed_input(case["offset"], _json.encode(case["envelope"]), event)
                    )
                case "state_scope_range":
                    return _json.decode(
                        _edax.projection_state_scope_range(
                            _json.encode(case["contract"]), case["subject"], case["tenant"]
                        )
                    )
                case "sql":
                    return _json.decode(_edax.postgres_projection_statements(_json.encode(case["config"])))
                case _:
                    pytest.fail(f"unknown projection boundary operation: {case['op']}")

        if case["error"] is not None:
            with pytest.raises(RuntimeError) as caught:
                run()
            assert str(caught.value) == case["error"], case["name"]
        else:
            assert run() == case["want"], case["name"]
