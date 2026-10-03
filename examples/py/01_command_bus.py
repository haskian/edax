"""Step 1: send a command to its handler through a CommandBus.

A command is a plain dataclass that asks for one change. Its `type` names the handler. The bus
finds that handler and returns what it returns.

Run:  cd examples/py && uv run python 01_command_bus.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import ClassVar

from edax import CommandBus, HandlerNotFound


@dataclass
class Deposit:
    account_id: str
    amount: int
    # A ClassVar, not a field: it routes the command and stays out of its payload.
    type: ClassVar[str] = "Deposit"


@dataclass
class Withdraw:
    account_id: str
    amount: int
    type: ClassVar[str] = "Withdraw"


async def main() -> None:
    balances: dict[str, int] = {}

    # 1. A handler is an async function that takes one command.
    async def deposit(cmd: Deposit) -> int:
        balances[cmd.account_id] = balances.get(cmd.account_id, 0) + cmd.amount
        return balances[cmd.account_id]

    # 2. Register the handler under the command's type.
    bus = CommandBus()
    bus.register("Deposit", deposit)

    # 3. Dispatch commands. The bus hands back what the handler returned.
    first = await bus.dispatch(Deposit("acct-1", 100))
    print(f"deposit 100 -> balance {first}")
    second = await bus.dispatch(Deposit("acct-1", 50))
    print(f"deposit 50  -> balance {second}")

    # 4. A command with no registered handler raises HandlerNotFound.
    try:
        await bus.dispatch(Withdraw("acct-1", 30))
        raise AssertionError("expected HandlerNotFound")
    except HandlerNotFound as e:
        print(f"withdraw    -> HandlerNotFound: {e}")

    assert (first, second) == (100, 150), (first, second)
    print("\nok: the bus routed each command to its handler by type")


if __name__ == "__main__":
    asyncio.run(main())
