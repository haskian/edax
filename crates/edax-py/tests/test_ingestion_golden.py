"""Replay ingestion decisions across the generated Python ABI."""

from __future__ import annotations

import json
from typing import Any

import pytest
from conftest import golden
from edax import _edax


def replay(case: dict[str, Any]) -> Any:
    op = case["op"]
    if op == "names":
        return {
            "ingest_mode": _edax.ingest_mode_names,
            "ingest_outcome": _edax.ingest_outcome_names,
            "batch_action": _edax.batch_action_names,
        }[case["set"]]()
    if op == "plan":
        return json.loads(
            _edax.ingest_plan(
                case["mode"],
                json.dumps(case["inputs"]),
                json.dumps(case["stored"]),
                json.dumps(case["rejected"]),
            )
        )
    if op == "bind":
        _edax.ingest_bind_contract(json.dumps(case["previous"]), json.dumps(case["expected"]))
        return True
    if op == "validate":
        _edax.ingest_validate_inputs(json.dumps(case["contract"]), json.dumps(case["inputs"]))
        return True
    if op == "check_sealed":
        _edax.ingest_check_sealed(json.dumps(case["contract"]), json.dumps(case["inputs"]))
        return True
    if op == "row_key":
        return _edax.ingest_row_key(
            json.dumps(
                {
                    "generation": case["target"],
                    "mode": case["mode"],
                    "mapper_id": "test-mapper",
                    "source_incarnation": "test-source",
                    "at_rest": "sealed",
                }
            ),
            json.dumps({"row_id": case["row_id"], "tenant": case["tenant"], "subject": case["subject"]}),
        )
    if op == "scope_range":
        manifest = json.dumps(
            {
                "generation": case["target"],
                "mode": case["mode"],
                "mapper_id": "test-mapper",
                "source_incarnation": "test-source",
                "at_rest": "sealed",
            }
        )
        return json.loads(_edax.ingest_scope_range(manifest, case["subject"], case["tenant"]))
    if op == "keyed_fingerprint":
        return _edax.ingest_keyed_fingerprint(bytes(case["key"]), json.dumps(case["state"]))
    if op == "source":
        return _edax.ingest_source_key(
            case["target"], case["incarnation"], case["topic"], case["partition"], case["offset"]
        )
    if op == "statements":
        return json.loads(_edax.postgres_ingest_statements(case["config"]))
    if op == "reserve":
        return _edax.batch_reserve(json.dumps(case["config"]))
    if op == "charge":
        return _edax.batch_charge(case["bytes"], json.dumps(case["config"]))
    if op == "recover":
        return json.loads(
            _edax.batch_recover(
                case["attempt"],
                case["waits"],
                case["count"],
                case["max_attempts"],
                case["rolled_back"],
                case["isolable"],
                case["code"],
            )
        )
    config = json.dumps(case["config"])
    if op == "config":
        return json.loads(_edax.batch_config(config))
    if op == "cut":
        return _edax.batch_cut(case["records"], case["bytes"], case["age_ms"], config)
    if op == "admit":
        return _edax.batch_admit(
            case["used_records"], case["used_bytes"], case["records"], case["bytes"], config
        )
    raise AssertionError(f"unknown golden operation {op}")


@pytest.mark.parametrize("filename", ["ingest.json", "event_batch.json", "postgres_ingest.json"])
def test_golden_ingestion(filename: str) -> None:
    for case in golden(filename):
        if case["error"] is not None:
            with pytest.raises(RuntimeError) as failure:
                replay(case)
            assert str(failure.value) == case["error"], case["name"]
        else:
            assert replay(case) == case["want"], case["name"]


def test_kafka_raw_golden() -> None:
    for case in golden("kafka_ingestion_raw.json"):
        if case["op"] == "rejection":
            if case["error"] is not None:
                with pytest.raises(RuntimeError) as failure:
                    _edax.kafka_ingestion_rejection_record(json.dumps(case["raw"]), case["source"])
                assert str(failure.value) == case["error"], case["name"]
                continue
            restored = _edax.kafka_ingestion_rejection_record(json.dumps(case["raw"]), case["source"])
            assert {
                "key": list(restored.key),
                "value": list(restored.value),
                "headers": [{"key": h.key, "value": list(h.value)} for h in restored.headers],
            } == case["want"], case["name"]
            continue
        if case["op"] == "decode":
            with pytest.raises(RuntimeError) as failure:
                _edax.kafka_ingestion_record(json.dumps(case["raw"]))
            assert str(failure.value) == case["error"]
            continue
        record = case["record"]
        raw = _edax.kafka_ingestion_raw(
            _edax.KafkaRecord(
                bytes(record["key"]),
                bytes(record["value"]),
                [_edax.KafkaHeader(h["key"], bytes(h["value"])) for h in record["headers"]],
            ),
            json.dumps({k: case[k] for k in ("topic", "partition", "offset", "timestamp", "timestamp_type")}),
            case["limit"],
        )
        assert json.loads(raw) == case["want"], case["name"]
        decoded = _edax.kafka_ingestion_record(raw)
        assert {
            "key": list(decoded.key),
            "value": list(decoded.value),
            "headers": [{"key": h.key, "value": list(h.value)} for h in decoded.headers],
        } == case["decoded"]
