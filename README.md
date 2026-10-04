# edax

> [!WARNING]
> **Early alpha (0.x).** The application programming interface (API) is unstable and a minor release may break it, and nothing is
> published to a package registry yet. Not ready for production. Build from source. What a version
> promises: [stability](docs/stability.md).

**An ergonomic toolkit for building event-driven systems. Command Query Responsibility Segregation
(CQRS) and event sourcing, with a Rust core and language software development kits (SDKs) that
cannot drift apart.**

Event-driven services stay consistent only while every service agrees, bit for bit, on a small set
of decisions. Which partition an aggregate routes to. What bytes a message serializes to. Whether
a redelivered command is a duplicate. How a filtered page is ordered. Get one of those wrong in one
service, or in one language, and the bug surfaces months later as a dropped row or a double-charged
customer. edax puts exactly those decisions in pure Rust **kernels**, and wraps them in an SDK per
language. **Golden vectors** pin the result, so drift fails the build instead of production. The
SDK wires. The kernel decides. Neither layer re-derives the other's job.

## Install

Nothing is published to a registry yet, so install from the Git source. The Python SDK builds the
Rust kernel from source, so a [Rust toolchain](https://rustup.rs) must be on your `PATH`.

**Python ([uv](https://docs.astral.sh/uv/)):** `maturin` builds the native extension automatically.

```sh
uv pip install "git+https://github.com/crtomirmajer/edax#subdirectory=crates/edax-py"
```

Python is the supported SDK. The Rust `edax` crate is a reference model (`publish = false`). It
implements the same orchestration in a second language, against the same kernels and golden
vectors. See [Rust and Python parity](docs/sdk/rust-parity.md).

Working inside a checkout instead? Point `uv` at the local path (`uv pip install ./crates/edax-py`),
or use the pinned [mise](https://mise.jdx.dev) toolchain. See
[getting started](docs/guides/getting-started.md).

## Quickstart

A command bus with idempotency and retry, an event-sourced aggregate, and a transactional outbox.
All of it commits as one unit of work.

<!-- doclint: check -->
```python
import asyncio
from dataclasses import dataclass
from typing import ClassVar

from edax import Aggregate, Relay, Repository, json_codec
from edax.backend.memory import MemoryEventStore, MemoryIdempotencyStore, MemoryTransactor
from edax.command import CommandBus, idempotent, retry


@dataclass
class Deposited:  # a domain event
    amount: int


@dataclass
class Deposit:  # a command: its fields are the payload
    account_id: str
    amount: int
    type: ClassVar[str] = "Deposit"  # the command name, hashed with the payload


class Account(Aggregate):
    def __init__(self, id: str, initial: int = 0) -> None:
        super().__init__(id, initial)
        self.balance = 0

    def transition(self, event: object) -> None:  # fold an event into state
        if isinstance(event, Deposited):
            self.balance += event.amount


class AccountCodec:  # how the aggregate snapshots
    type = "Account"

    def load(self, id: str, version: int, data: bytes | None) -> Account:
        account = Account(id, version)
        if data is not None:
            account.balance = int(data)
        return account

    def dump(self, account: Account) -> bytes:
        return str(account.balance).encode()


async def main() -> None:
    db = MemoryTransactor()  # one database: every store below commits together
    store = MemoryEventStore(db)
    repo = Repository(store, json_codec({"Deposited": Deposited}), AccountCodec())

    async def deposit(cmd: Deposit) -> int:
        account = await repo.load(cmd.account_id)  # snapshot + replay
        account.apply(Deposited(cmd.amount))
        await repo.save(account)  # joins the unit of work the guard opened
        return account.balance

    # The guard opens the unit of work: claim, events, snapshot and outbox commit together.
    # retry sits outside it, so a failed attempt rolls back whole before the next one starts.
    ledger = MemoryIdempotencyStore(db)
    bus = CommandBus(retry(3), idempotent(ledger, scope="accounts", unit_of_work=db))
    bus.register("Deposit", deposit)

    async def submit(cmd: Deposit, request_id: str) -> int:
        # The client's request id is the idempotency key. No transaction around the dispatch:
        # retry refuses to run inside one.
        return await bus.dispatch(cmd, idempotency_key=request_id)

    print(await submit(Deposit("acct-1", 100), "req-A"))  # 100
    print(await submit(Deposit("acct-1", 100), "req-A"))  # 100, replayed, not deposited twice

    published: list[str] = []
    await Relay(store, publish=lambda msg: published.append(msg.id)).drain()
    print(len(published))  # 1, because the replay wrote nothing
```

Dispatch `req-A` twice and the second call replays the first outcome. Reuse the key with a
different command and the guard raises `PayloadMismatch`. The guard claims the key under its
`scope`, so another service's guard on the same ledger never replays this outcome. The kernel decides between replay and
mismatch, so a Rust service that shares the store reaches the same verdict. A retry re-runs a whole
unit of work, never a handler inside one, so `retry` goes outside a guard given `unit_of_work`.

Nineteen runnable examples, one idea each, go on from here in learning order. The
[examples README](examples/py/README.md) lists them, from `01_command_bus.py` to
`19_fault_injection.py`. Each one ends in asserts, so a clean exit is the test. Continuous
integration (CI) runs them all.

## Documentation

Start at [`docs/README.md`](docs/README.md). The four entry pages:

- [Getting started](docs/guides/getting-started.md) — install the toolchain, build the native
  module, run both test suites.
- [Architecture](docs/architecture.md) — the kernel, application binary interface (ABI) and SDK
  layers, and what each one owns.
- [Delivery guarantees](docs/guides/delivery-guarantees.md) — at-least-once, dedup windows, and
  both idempotency guard modes.
- [Backends](docs/backends/README.md) — the seam contracts, and the memory, Redis and Kafka
  implementations. The `edax-kafka` kernel owns a Kafka record, its header, its partition and the
  offset watermark that settles it. The adapter only makes the client calls.

## Layout

| Path | What |
|---|---|
| `crates/edax-core` | shared primitives: hashing, backoff, the clock, id generation, the error taxonomy, a poison-tolerant lock, settlement tokens |
| `crates/edax-event` | event-driven kernel: the event and command messages, codec and media types, lineage, the ordering key, local fan-out, upcast, staleness, dedup, sequence, the retry and dead-letter policy, the saga step |
| `crates/edax-cqrs` | command-and-query kernel: canonical bytes (RFC 8785), payload and command hash, the claim key, the idempotency record, outcome, settle and forget, the filter AST and evaluator, sort and cursor rules |
| `crates/edax` | the Rust reference model (`publish = false`): the SDK orchestration in a second language, held to the same kernels and golden vectors; not a deployable SDK |
| `crates/edax-memory` | in-memory event, projection, idempotency and saga stores and a command outbox behind one transaction boundary, and a partitioned broker log with consumer groups |
| `crates/edax-kafka` | Kafka kernel: the record key and header, broker placement, the media-type route, the dead-letter record, the offset watermark |
| `crates/edax-redis` | Redis kernel: the idempotency ledger's key, value layout and claim handle, its five scripts, and the verdict on a Redis reply |
| `crates/edax-abi` | flat Rust surface over the kernels, and the source the Python binding is generated from |
| `crates/edax-golden` | test-only golden-vector harness: every kernel's `tests/golden.rs` writes and checks through it |
| `crates/edax-py` | Python SDK (`python/edax`); the native extension `src/lib.rs` is generated, not hand-written |
| `examples/py` | nineteen runnable examples in learning order; start at its [README](examples/py/README.md) |
| `docs` | the full documentation set. Start at [`docs/README.md`](docs/README.md) |
| `testdata/golden` | cross-language consistency vectors |

## Status

Early alpha. The API is not yet stable, and nothing is published to a package registry. Build from source; see
[getting started](docs/guides/getting-started.md). The version literal lives in `Cargo.toml` alone,
so this page does not restate it; [stability](docs/stability.md) says what a version promises.

Deliberately out of scope: durable event- and projection-store backends, transports beyond the
Kafka adapter, and upcast transforms. The kernel plans an upcast, and your code rewrites the
payload.

## License

MIT. See [LICENSE](LICENSE).
