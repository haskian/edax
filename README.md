# edax

> [!WARNING]
> Early alpha (0.x). The application programming interface (API) can change in a minor release.
> Build from source. See [stability](docs/stability.md) for the version policy.

edax provides Python and Go software development kits (SDKs) for event-driven systems.
It supports Command Query Responsibility Segregation (CQRS), event sourcing, idempotency,
transactional outboxes, projections, and sagas.

Both SDKs call the same Rust kernels for message encoding, partition placement, sequencing,
idempotency decisions, and query semantics. Shared golden vectors test those results.
The SDKs run handlers and coordinate stores. Applications supply domain rules and durable storage.

## Install

Install from source while registry publishing is disabled. The Python SDK builds a native extension,
so a [Rust toolchain](https://rustup.rs) must be on your `PATH`.

**Python ([uv](https://docs.astral.sh/uv/)):** `maturin` builds the native extension automatically.

```sh
uv pip install "git+https://github.com/haskian/edax#subdirectory=crates/edax-py"
```

**Go:** Clone this repository and build its native library with the pinned toolchain.

```sh
mise trust
mise install
mise run go:build
cd crates/edax-go
mise exec -- go test ./...
```

The Go module is `github.com/haskian/edax/crates/edax-go`. It requires cgo and a C compiler.
See the [Go SDK guide](docs/sdk/go.md) for application setup and examples.

The Rust `edax` crate is a reference model (`publish = false`). It runs against the same kernels
and golden vectors. See [Rust and Python parity](docs/sdk/rust-parity.md).

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

from edax import Aggregate, Relay, Repository, Tenancy, json_codec
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
    bus = CommandBus(retry(3), idempotent(ledger, scope="accounts", tenancy=Tenancy.SILO, unit_of_work=db))
    bus.register(Deposit, deposit)

    async def submit(cmd: Deposit, request_id: str) -> int:
        # The client's request id is the idempotency key. No transaction around the dispatch:
        # retry refuses to run inside one.
        return await bus.dispatch(cmd, idempotency_key=request_id)

    print(await submit(Deposit("acct-1", 100), "req-A"))  # 100
    print(await submit(Deposit("acct-1", 100), "req-A"))  # 100, replayed, not deposited twice

    published: list[str] = []
    await Relay(store, publish=lambda msg: published.append(msg.id)).drain()
    print(len(published))  # 1, because the replay wrote nothing


asyncio.run(main())
```

Dispatch `req-A` twice and the second call replays the first outcome. Reuse the key with a
different command and the guard raises `PayloadMismatch`. The guard claims the key under its
`scope`, so another service's guard on the same ledger never replays this outcome. The kernel decides between replay and
mismatch, so a Rust service that shares the store reaches the same verdict. A retry re-runs a whole
unit of work, never a handler inside one, so `retry` goes outside a guard given `unit_of_work`.

Python and Go each have twenty-one examples in the same learning order. The
[Python examples](examples/py/README.md) and [Go examples](crates/edax-go/examples/README.md)
cover commands, projections, ingestion and folds. Examples 20 and 21 use public Python stores;
their Go counterparts exercise internal kernels because Go runners and database adapters for those
features do not ship. Each example checks its results. Continuous integration
(CI) smoke-tests both sets, including Redis and Kafka against real services.

## Documentation

Start at [`docs/README.md`](docs/README.md). Entry pages:

- [Getting started](docs/guides/getting-started.md) — install the toolchain, build the native
  module, run both test suites.
- [Architecture](docs/architecture.md) — the kernel, application binary interface (ABI) and SDK
  layers, and what each one owns.
- [Delivery guarantees](docs/guides/delivery-guarantees.md) — at-least-once, dedup windows, and
  both idempotency guard modes.
- [Backends](docs/backends/README.md) — the seam contracts, and the memory, Postgres, Redis and Kafka
  implementations. The `edax-kafka` kernel owns a Kafka record, its header, its partition and the
  offset watermark that settles it. The adapter only makes the client calls.
- [Microbatch ingestion](docs/backends/ingestion.md) — Python Kafka append/upsert ingestion, atomic
  Postgres sinks, durable rejection delivery and bounded admission.
- [Ordered folds](docs/backends/folds.md) — delta reducers, fenced database checkpoints, outbox
  delivery, generation cutovers and offline restore.

## Layout

| Path | What |
|---|---|
| `crates/edax-core` | shared primitives: `names`, `hash`, `key` (tenancy, guard key), `backoff`, `clock`, `guid`, `version`, `error` (the taxonomy); and two Rust-only runtime helpers, `lock` and `TokenMint`. [Scope](docs/kernels/core.md) |
| `crates/edax-event` | event-driven kernel: `message` (the messages, their payload, the ordering key), `codec`, `causation`, `partition` (local fan-out), `staleness`, `idempotency`, `sequence`, `upcast`, `retry`, `drain`, `saga`, `batch` (ingestion limits and recovery). [Scope](docs/kernels/event.md) |
| `crates/edax-cqrs` | command-and-query kernel: `canonical_json` (RFC 8785), `payload_hash_json`, `command_hash`, `received_key`, `claim_key`, `complete_fence`, `release_fence`, the idempotency `record`, the host values `Decimal` and `Instant`, `filter`, `query`, `ingest` (append/upsert identity, conflicts and compaction), `fold` (ordered deltas, receipts, checkpoints, fences and effect activation). [Scope](docs/kernels/cqrs.md) |
| `crates/edax` | the Rust reference model (`publish = false`): the SDK orchestration in a second language, held to the same kernels and golden vectors; not a deployable SDK |
| `crates/edax-memory` | in-memory event, projection, idempotency and saga stores and a command outbox behind one transaction boundary, and a partitioned broker log with consumer groups; `ingest` is the atomic ingestion reference sink |
| `crates/edax-kafka` | Kafka kernel: `record` (key, header, route, dead-letter record), `partition` (broker placement), `offsets` (the watermark), `client` (`check_config`, `broker_error`). [Scope](docs/kernels/kafka.md) |
| `crates/edax-redis` | Redis kernel: `ledger` (the idempotency ledger's key, settings, value layout, claim handle, reply outcomes), `scripts` (the five fence scripts, the reply verdict). [Scope](docs/kernels/redis.md) |
| `crates/edax-postgres` | Postgres kernel: `config`, `error` (the SQLSTATE verdict), `schema` (the table contract and its catalog check), `statements`, `rows` (the row layout), `documents` (exact number text), `query` (the query compiled to SQL), `ledger` (the fence), `ingest` (optional ingestion schema, statements and recovery), `fold` (atomic fold storage and effect suppression). [Scope](docs/kernels/postgres.md) |
| `crates/edax-abi` | flat Rust surface over the kernels, and the source both native bindings are generated from |
| `crates/edax-golden` | test-only golden-vector harness: every kernel's `tests/golden.rs` writes and checks through it |
| `crates/edax-py` | Python SDK (`python/edax`); the native extension `src/lib.rs` is generated, not hand-written |
| `crates/edax-abi-ffi` | Alef-generated C interface used by Go; no independent consistency logic |
| `crates/edax-go` | Go SDK, with generated native calls under `internal/native` |
| `examples/py` | twenty-one Python examples in learning order; start at its [README](examples/py/README.md) |
| `crates/edax-go/examples` | the same twenty-one topics in Go; start at its [README](crates/edax-go/examples/README.md) |
| `docs` | the full documentation set. Start at [`docs/README.md`](docs/README.md) |
| `testdata/golden` | cross-language consistency vectors |

## Status

Early alpha. The API is not yet stable. Build from source; see
[getting started](docs/guides/getting-started.md). PyPI publishing is prepared and disabled by default.
See [release setup](docs/releasing.md). The version literal lives in `Cargo.toml` alone,
so this page does not restate it; [stability](docs/stability.md) says what a version promises.

Deliberately out of scope: durable stores beyond the Postgres backend (Python only), transports
beyond the Kafka adapter, and upcast transforms. The kernel plans an upcast, and your code rewrites the
payload.

## License

MIT. See [LICENSE](LICENSE).
