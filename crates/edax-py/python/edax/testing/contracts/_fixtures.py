"""How a clause asks for what a backend may lack: a capability it skips without, and the ``transactor``
fixture, resolved only by the clause that runs, so a backend whose clauses skip need not define it."""

from __future__ import annotations

from typing import Any

import pytest

from ...unit_of_work import Transactor


def requires(capable: bool, reason: str) -> None:
    """Skip the running clause unless the backend is ``capable``.

    ``pytest.skip.Exception(...)`` behaves and reports like ``pytest.skip(...)``. The call form does not
    type-check under pytest 8: a checker reads its ``_WithException`` declaration as taking no arguments."""
    if not capable:
        raise pytest.skip.Exception(reason)


def transactor(request: pytest.FixtureRequest) -> Transactor[Any]:
    """The ``transactor`` fixture: the unit of work a clause joins, races in or rolls back."""
    resolved: Transactor[Any] = request.getfixturevalue("transactor")
    return resolved


class Transactional:
    """A contract with clauses that need a unit of work: the one capability flag that skips them, and the
    one fixture they share."""

    #: There is a unit of work to join, roll back, race in and isolate. Requires a ``transactor`` fixture
    #: when true, unless the contract names another port that opens it; a backend that declares ``False``
    #: needs no such fixture.
    supports_transactions: bool = True

    def _unit_of_work(self, request: pytest.FixtureRequest) -> Transactor[Any]:
        """The ``transactor`` fixture, or a skip for a backend that declares no unit of work."""
        requires(self.supports_transactions, "the backend declares no unit of work")
        return transactor(request)
