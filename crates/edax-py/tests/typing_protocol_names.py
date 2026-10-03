"""Type-check fixture: an implementation may name its parameters after its own domain.

Not a pytest module (pytest collects only ``test_*.py``); `ty` checks this directory, so this file
fails the lint gate if a seam Protocol stops accepting names other than its own. The codec below is
the one `docs/guides/getting-started.md` teaches, whose `dump` parameter is named after the
aggregate — the natural choice, and the first thing a new adopter writes.

Structural matching compares parameter *names* unless the parameter is positional-only, so without
the `/` markers on the Protocols this does not type-check.
"""

from __future__ import annotations

from typing import Any

from edax.aggregate import Aggregate, Event, EventEnvelope
from edax.saga import Completed, Saga
from edax.store import AggregateCodec, EventCodec


class Account(Aggregate):
    def __init__(self, id: str, version: int = 0) -> None:
        super().__init__(id, version)
        self.balance = 0

    def transition(self, event: Any) -> None:
        self.balance += 1


class AccountCodec:
    """`docs/guides/getting-started.md`: `dump` takes `account`, not the Protocol's `agg`."""

    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        account = Account(id, version)
        if data is not None:
            account.balance = int(data)
        return account

    def dump(self, account: Account) -> bytes:
        return str(account.balance).encode()


class DepositCodec:
    """An `EventCodec` naming its parameter after the domain event rather than `event`."""

    def marshal(self, deposited: Any) -> Event:
        return Event("Deposited", 1, b"{}")

    def unmarshal(self, stored: Event) -> Any:
        return stored


class OrderSaga:
    """A `Saga` naming its parameters after the workflow rather than `env` / `state`."""

    def correlation_id(self, message: EventEnvelope) -> str | None:
        return message.aggregate_id

    def handle(self, order: Any, message: EventEnvelope) -> tuple[Any, list[Any]]:
        return Completed, []


# Assigned against the Protocol rather than passed to a constructor: the subject is the seams'
# parameter names, so binding one through `Repository` or `SagaRunner` would couple this fixture to
# constructor signatures it does not check.
aggregate_codec: AggregateCodec[Account] = AccountCodec()
event_codec: EventCodec = DepositCodec()
saga: Saga = OrderSaga()
