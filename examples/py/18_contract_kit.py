"""Step 18: check a store you wrote yourself with the contract test kit, `edax.testing`.

edax ships no durable store: you write one for your database. `edax.testing` holds the clauses the
in-tree stores pass. Subclass a contract, give it a fixture that builds your store, and pytest runs
every clause against it. A clause your store cannot support (here: transactions) is skipped.

This file is both the example and the test module: `main()` runs pytest on this file.

Run:  cd examples/py && uv run python 18_contract_kit.py
"""

from __future__ import annotations

import asyncio
import copy
import subprocess
import sys
from typing import Any

import pytest
from edax import ConcurrencyError, Entity, Tx
from edax.testing import ProjectionStoreContract


class DictProjectionStore:
    """A ProjectionStore over a plain dict: the shape of a store you write for your own database."""

    def __init__(self) -> None:
        self._rows: dict[str, tuple[int, Any]] = {}  # id -> (version, document)

    async def get(self, id: str, tx: Tx | None = None) -> Entity | None:
        if id not in self._rows:
            return None
        version, document = self._rows[id]
        return Entity(id, version, copy.deepcopy(document))

    async def save(self, entity: Entity, tx: Tx | None = None) -> None:
        # Compare-and-set on the version. A database does this in one statement, never as a read
        # followed by a write. A dict in one event loop gets away with it: nothing runs in between.
        stored_version = self._rows[entity.id][0] if entity.id in self._rows else 0
        if stored_version != entity.version:
            raise ConcurrencyError(f"{entity.id}: stored version {stored_version}, not {entity.version}")
        self._rows[entity.id] = (entity.version + 1, copy.deepcopy(entity.document))
        entity.version += 1


class TestDictProjectionStore(ProjectionStoreContract):
    supports_transactions = False  # a dict has no unit of work, so those clauses skip

    @pytest.fixture
    def projection_store(self) -> DictProjectionStore:
        return DictProjectionStore()


async def main() -> None:
    # 1. Run pytest on this very file, in a child process, as CI would run your test suite.
    # The contract clauses are async, hence asyncio_mode=auto. no:cacheprovider writes no cache here.
    options = ["-q", "-p", "no:cacheprovider", "-o", "asyncio_mode=auto"]
    result = subprocess.run(
        [sys.executable, "-m", "pytest", __file__, *options], capture_output=True, text=True
    )

    # 2. Show the summary line: how many clauses passed and how many were skipped.
    summary = result.stdout.strip().splitlines()[-1]
    print(f"pytest: {summary}")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "failed" not in summary and "passed" in summary, summary
    print("\nok: the dict store passed every contract clause it supports")


if __name__ == "__main__":
    asyncio.run(main())
