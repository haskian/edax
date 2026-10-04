"""What two SDKs sharing a ledger must agree on, checked from the Python side against the bytes the Rust
kernel and the Rust SDK wrote into ``testdata/golden``: the canonical form a command hash is taken over,
the event guard's key rule, a command's claim key, the ordering key, the extension key rule, the jittered
backoff schedule, and a failed record's code."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import edax
import msgspec
import pytest
from conftest import build_command, build_message, golden
from edax import _edax
from edax._async import maybe_await
from edax.backend.memory import MemoryIdempotencyStore, MemoryLedger
from edax.command import Won, _payload_hash
from hypothesis import given
from hypothesis import strategies as st

# ---- the canonical form -----------------------------------------------------


@pytest.mark.parametrize("case", golden("canonical_json.json"), ids=lambda c: c["name"])
def test_python_reaches_the_rust_canonical_bytes(case: dict[str, Any]) -> None:
    """Each row's canonical text and hash, as the Rust kernel computed them; a refused input gives the
    same `codec:` error, offset included."""
    if case["error"] is None:
        assert _edax.canonical_json(case["input"].encode()) == case["canonical"]
        assert edax.payload_hash_json(case["input"].encode()) == case["hash"]
    else:
        with pytest.raises(RuntimeError) as caught:
            _edax.canonical_json(case["input"].encode())
        assert str(caught.value) == case["error"]
        with pytest.raises(edax.CodecError), edax.errors.mapped():
            edax.payload_hash_json(case["input"].encode())


@dataclass
class Pay:
    request_id: str
    amount: float
    type: ClassVar[str] = "pay"


@pytest.mark.parametrize(
    "branch, left, right",
    [
        # `amount: float` holding an int is legal Python; msgspec writes `2`, a decoded copy writes `2.0`.
        ("int_in_a_float_field_vs_the_float", Pay("r1", 2), Pay("r1", 2.0)),
        (
            "decoded_off_a_queue",
            Pay("r1", 2),
            msgspec.json.decode(b'{"request_id":"r1","amount":2.0}', type=Pay),
        ),
        ("negative_zero", Pay("r1", 0), Pay("r1", -0.0)),
    ],
)
def test_one_command_value_has_one_hash_whatever_the_number_spelling(
    branch: str, left: Any, right: Any
) -> None:
    """Before numbers were canonical by value, `2` and `2.0` hashed apart, so one command dispatched
    from its constructor and again off a queue read as a payload mismatch: terminal, for a duplicate."""
    assert msgspec.json.encode(left) != msgspec.json.encode(right)
    assert _payload_hash(left, left.type) == _payload_hash(right, right.type), branch


def test_a_third_sdks_spelling_hashes_the_same() -> None:
    """A JavaScript or Go producer writes a whole float as `200`; another writes `2e2` or `200.0`."""
    spellings = [
        b'{"amount":200,"request_id":"r1"}',
        b'{"request_id":"r1","amount":2e2}',
        b'{"amount":200.0,"request_id":"r1"}',
    ]
    assert len({edax.payload_hash_json(s) for s in spellings}) == 1


def test_a_python_int_matches_the_rust_float_for_the_golden_command() -> None:
    """The golden row the Rust SDK hashed from an `f64` field holding `1.0`; Python builds the same
    command with the int `1` and must bind the key to the same number."""
    [case] = [
        c for c in golden("command_hash.json") if c["name"] == "whole_number_float_hashes_as_the_integer"
    ]
    command = dict(case["command"], weight_kg=1)
    assert isinstance(command["weight_kg"], int)
    assert _edax.command_hash_json(case["type"], msgspec.json.encode(command)) == case["expected"]


# ---- the event guard's key rule ---------------------------------------------


@pytest.mark.parametrize("case", golden("event_idempotency.json"), ids=lambda c: c["name"])
def test_the_event_guard_claims_the_kernels_key_and_hash(case: dict[str, Any]) -> None:
    env = build_message(
        "acct-1",
        "Deposited",
        3,
        id=case["id"],
        data=bytes.fromhex(case["data_hex"]),
        extensions=case["extensions"],
        metadata=case["metadata"],
        recorded_at=case["recorded_at"],
    )
    rule = _edax.event_idempotency(env)
    assert (rule.key, rule.payload_hash) == (case["key"], case["payload_hash"])


# ---- the command claim key, the ordering key, the extension key rule ------------------------


@pytest.mark.parametrize(
    "case", golden("claim_key.json"), ids=lambda c: f"{c['scope']}|{c['idempotency_key']}"
)
def test_a_command_claims_the_kernels_scoped_key(case: dict[str, Any]) -> None:
    """The ledger key a guarded command claims: the scope's length keeps a scope holding the separator
    from colliding with another scope and key."""
    assert edax.claim_key(case["scope"], case["idempotency_key"]) == case["expected"]


@pytest.mark.parametrize("case", golden("ordering_key.json"), ids=lambda c: f"{c['id']}|{c['aggregate_id']}")
def test_a_message_orders_by_the_kernels_key(case: dict[str, Any]) -> None:
    """The aggregate id when the message names one, an empty one included, else the message's own id. The
    dispatcher picks its worker by it, so a Python and a Rust consumer serialize the same messages."""
    assert _edax.ordering_key(case["id"], case["aggregate_id"]) == case["expected"]
    command = build_command("Deposit", id=case["id"], aggregate_id=case["aggregate_id"])
    dispatcher = edax.KeyOrderedDispatcher(64, lambda msg: None)
    assert dispatcher._worker_index(command) == edax.partition(case["expected"], 64)


@pytest.mark.parametrize(
    "case", golden("command_received_key.json"), ids=lambda c: f"{c['id']}|{c['idempotency_key']}"
)
def test_a_received_command_claims_the_kernels_key(case: dict[str, Any]) -> None:
    """The client's key when the message carries one, an empty one included, else the message's own id.
    A broker redelivers the same message, so the id still deduplicates a command sent with no key."""
    msg = build_command("Deposit", id=case["id"], idempotency_key=case["idempotency_key"])
    assert edax.received_key(msg) == case["expected"]


@pytest.mark.parametrize("case", golden("extensions.json"), ids=lambda c: c["key"] or "empty")
def test_an_extension_key_follows_the_kernels_rule(case: dict[str, Any]) -> None:
    """Checked before a save writes anything, and again when a message encodes, so no SDK mints a message
    another SDK refuses to read."""
    extensions = {case["key"]: "v"}
    message = build_message("acct-1", "Deposited", 1, extensions=extensions)
    if case["valid"]:
        assert _edax.check_extensions(extensions) is None
        assert edax.KernelSerializer("json").encode(message)
        return
    with pytest.raises(edax.CodecError) as refused, edax.errors.mapped():
        _edax.check_extensions(extensions)
    assert refused.value.code == case["error_code"]
    with pytest.raises(edax.CodecError):
        edax.KernelSerializer("msgpack").encode(message)


# ---- the backoff schedule ----------------------------------------------------

_U64 = st.integers(min_value=0, max_value=2**64 - 1)


@given(base=_U64, cap=_U64, attempt=_U64, entropy=_U64)
def test_a_jittered_wait_stays_within_its_ceiling(base: int, cap: int, attempt: int, entropy: int) -> None:
    """Full jitter: never longer than `min(base * 2**attempt, cap)`, and never past the cap."""
    policy = edax.Backoff(base, cap)
    ceiling = min(base * 2 ** min(attempt, 64), cap)
    waited = policy.delay_millis(attempt, entropy)
    assert 0 <= waited <= ceiling <= cap


@given(entropy=_U64)
def test_the_pacing_waits_what_the_kernel_computes(entropy: int) -> None:
    pacing = edax.Pacing(entropy=lambda: entropy)
    for attempt in (0, 3, 9):
        assert pacing.delay(attempt) == edax.DEFAULT_BACKOFF.delay_millis(attempt, entropy) / 1000


async def test_the_pacing_sleeps_its_own_delay() -> None:
    slept: list[float] = []

    async def record(seconds: float) -> None:
        slept.append(seconds)

    pacing = edax.Pacing(backoff=edax.Backoff(100, 1_000), sleep=record, entropy=lambda: 150)
    for attempt in (0, 1, 4):
        await pacing.wait(attempt)
    assert slept == [0.049, 0.15, 0.15], "150 % 101, 150 % 201, 150 % 1001"


def test_the_default_pacing_draws_fresh_entropy() -> None:
    pacing = edax.Pacing()
    assert pacing.backoff == edax.DEFAULT_BACKOFF == edax.Backoff(100, 30_000)
    assert len({pacing.entropy() for _ in range(32)}) > 1, "the draw is not constant"


@pytest.mark.parametrize(
    "branch, base, cap",
    [
        ("negative_base", -1, 100),
        ("negative_cap", 100, -1),
        ("base_past_u64", 2**64, 100),
        ("cap_past_u64", 1, 2**64),
    ],
)
def test_a_policy_outside_the_kernels_range_is_refused_at_construction(
    branch: str, base: int, cap: int
) -> None:
    with pytest.raises(ValueError, match=r"must be within 0 \.\. 2"):
        edax.Backoff(base, cap)


# ---- one ledger, two SDKs ----------------------------------------------------


@dataclass
class _Rejected:
    """The golden row the Rust SDK wrote for a `Terminal` host rejection."""

    stored: str
    payload_hash: int


def _rust_rejection() -> _Rejected:
    [case] = [c for c in golden("idempotency_record.json") if c["name"] == "cached_rejection_written_by_rust"]
    return _Rejected(case["stored"], case["payload_hash"])


async def test_a_failure_rust_cached_replays_in_python_with_its_code() -> None:
    """Shared-ledger fault: the Rust SDK cached a host rejection; a Python consumer of the same event
    reads the record off the same ledger. The handler never runs, the replay is terminal, the original
    code survives, and the dispatcher dead-letters the event on its first attempt."""
    rust = _rust_rejection()
    ledger = MemoryIdempotencyStore()
    won = await ledger.claim("evt-1", rust.payload_hash)
    assert isinstance(won, edax.Won)
    await ledger.complete("evt-1", won.handle, edax.command.decode_record(rust.stored))
    runs = 0

    async def handler(env: edax.EventMessage) -> None:
        nonlocal runs
        runs += 1

    router = edax.EventRouter(edax.idempotent_by_id(ledger))
    router.register("Deposited", handler)
    dead: list[BaseException] = []

    async def on_dead(delivery: edax.Delivery, error: BaseException) -> None:
        dead.append(error)

    dispatcher = edax.KeyOrderedDispatcher(1, router.dispatch, on_dead=on_dead, max_attempts=5)
    dispatcher.start()
    # The event guard hashes the raw body: `cmd-a` is the body the Rust row's hash was taken over.
    env = build_message("acct-1", "Deposited", 1, id="evt-1", data=b"cmd-a")
    settled: list[str] = []
    await dispatcher.submit(edax.Delivery(env, lambda: settled.append("ack"), lambda: settled.append("nack")))
    await dispatcher._queues[0].join()
    await dispatcher.stop()

    assert runs == 0, "the cached outcome is replayed, never re-run"
    assert settled == ["ack"] and len(dead) == 1, "dead-lettered on the first attempt"
    [error] = dead
    assert isinstance(error, edax.CachedFailure) and error.terminal
    assert error.original_code == "rejected"
    assert str(error) == "rejected: insufficient funds: balance 300, withdrawal 500"


async def test_a_failure_python_caches_is_byte_for_byte_what_rust_writes() -> None:
    """The reverse direction: Python caches a `Rejected` under the same hash and the ledger holds the
    exact bytes the Rust SDK wrote for its `Terminal` rejection."""
    rust = _rust_rejection()
    ledger = MemoryIdempotencyStore()

    async def handler(env: edax.EventMessage) -> None:
        raise edax.Rejected("insufficient funds: balance 300, withdrawal 500")

    guarded = edax.idempotent_by_id(ledger)(handler)
    env = build_message("acct-1", "Deposited", 1, id="evt-1", data=b"cmd-a")
    with pytest.raises(edax.Rejected):
        await maybe_await(guarded(env))
    prior = await ledger.claim("evt-1", rust.payload_hash)
    assert not isinstance(prior, Won)
    assert edax.command.encode_record(prior) == rust.stored


async def test_a_record_rust_tore_mid_write_replays_in_python_as_the_kernels_cached_failure() -> None:
    """Shared-ledger fault: the Rust SDK's record write was cut off, and the torn bytes hold the key. A
    Python consumer of the same event reads them as Rust does: the kernel's terminal codec failure, with
    the replay text the golden row pins. The handler never runs and the dispatcher dead-letters the event
    on its first attempt, instead of redelivering it until the record's retention ends."""
    [case] = [c for c in golden("idempotency_record.json") if c["name"] == "torn_write_from_rust"]
    ledger = MemoryLedger()
    ledger.put_raw("evt-1", case["stored"])
    runs = 0

    async def handler(env: edax.EventMessage) -> None:
        nonlocal runs
        runs += 1

    router = edax.EventRouter(edax.idempotent_by_id(ledger))
    router.register("Deposited", handler)
    dead: list[BaseException] = []

    async def on_dead(delivery: edax.Delivery, error: BaseException) -> None:
        dead.append(error)

    dispatcher = edax.KeyOrderedDispatcher(1, router.dispatch, on_dead=on_dead, max_attempts=5)
    dispatcher.start()
    env = build_message("acct-1", "Deposited", 1, id="evt-1", data=b"cmd-a")
    settled: list[str] = []
    await dispatcher.submit(edax.Delivery(env, lambda: settled.append("ack"), lambda: settled.append("nack")))
    await dispatcher._queues[0].join()
    await dispatcher.stop()

    assert runs == 0, "the unreadable outcome is replayed, never re-run"
    assert settled == ["ack"] and len(dead) == 1, "dead-lettered on the first attempt"
    [error] = dead
    assert isinstance(error, edax.CachedFailure) and error.terminal and error.original_code == "codec"
    assert f"{error.code}: {error}" == case["replay"], "the replay text both SDKs show"
