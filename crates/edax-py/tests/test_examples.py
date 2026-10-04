"""Guards over the shipped examples: what they demonstrate must match what the SDK documents.

An example is the shape an adopter copies, so a mistake in one propagates into services the project
never sees. Most guards load each example as a module without running it, so they cost nothing and
cannot depend on a broker.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

EXAMPLES = sorted((Path(__file__).resolve().parents[3] / "examples" / "py").glob("*.py"))


def load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"example_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered before exec: `dataclasses` resolves a `ClassVar` annotation by looking the field's
    # module up in `sys.modules`, and every example uses postponed annotations, so every annotation
    # reaches it as a string.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # every example guards its `main()` under `__main__`
    return module


def test_the_example_suite_is_not_empty() -> None:
    """A glob that silently matches nothing would make every guard below vacuous."""
    assert EXAMPLES, "no examples found to check"


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_no_example_command_hashes_its_type(path: Path) -> None:
    """``type`` is the command's name: the hash takes it once, as the name, never as a field too.

    Declared as a dataclass *instance* field it enters the payload as well, and a Rust command carrying
    the same business fields does not. A service sharing a ledger across the two languages then gets
    ``PayloadMismatch`` on every genuine cross-language duplicate. ``docs/sdk/commands.md`` states the
    rule; this pins it for every example.
    """
    for name, value in vars(load(path)).items():
        if not (isinstance(value, type) and dataclasses.is_dataclass(value)):
            continue
        if not hasattr(value, "type"):
            continue
        assert "type" not in {f.name for f in dataclasses.fields(value)}, (
            f"{path.name}: {name}.type is an instance field, so it is hashed; declare it ClassVar"
        )


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
async def test_every_example_runs_to_its_ok_marker(path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Every example ends in asserts and a final ``ok:`` line. Run here as well as in CI's examples
    step, so a change that breaks the documented wiring fails the suite a developer runs, not only a
    later CI job. Every example runs on the in-memory backends by default. `16_redis_ledger` and
    `17_kafka` switch to a real server when ``EDAX_REDIS_URL`` or ``EDAX_KAFKA_BOOTSTRAP`` names one, as
    CI's test job does."""
    await load(path).main()
    assert "\nok:" in capsys.readouterr().out, f"{path.name} did not reach its ok: marker"
