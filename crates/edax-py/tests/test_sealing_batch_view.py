"""A consumer batch's view of the key provider: one read per scope, no kept failure, a deadline."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from edax.errors import KeyMissing, KeyUnavailable
from edax.sealing import MemoryKeys, Sealing


class CountingKeys(MemoryKeys):
    def __init__(self) -> None:
        super().__init__()
        self.reads = 0
        self.fail_next = False
        self.hang = False

    async def status(self, scope: Any) -> Any:
        self.reads += 1
        if self.hang:
            await asyncio.Event().wait()
        if self.fail_next:
            self.fail_next = False
            raise KeyMissing("provider lost the scope for one call")
        return await super().status(scope)


@pytest.mark.parametrize(
    "case", ["second_read_hits_the_view", "failure_is_not_kept", "hung_provider_is_unavailable"]
)
async def test_batch_view(case: str) -> None:
    keys = CountingKeys()
    view = Sealing(keys, "orders").for_batch(timeout_s=0.02)
    scope = view.scope("acme", "person-42")
    if case == "second_read_hits_the_view":
        assert await view.keys.status(scope) == await view.keys.status(scope)
        assert keys.reads == 1
    elif case == "failure_is_not_kept":
        keys.fail_next = True
        with pytest.raises(KeyMissing):
            await view.keys.status(scope)
        assert await view.keys.status(scope) == "unknown"
        assert keys.reads == 2
    else:
        keys.hang = True
        with pytest.raises(KeyUnavailable):
            await view.keys.status(scope)
