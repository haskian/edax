from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from conftest import RecordingLedger
from edax._observer import wrap
from edax.backend.memory import MemoryIdempotencyStore, MemoryLedger, MemoryTransactor
from edax.errors import (
    BadHandle,
    CachedFailure,
    CodecError,
    InProgress,
    KeyUnavailable,
    PayloadMismatch,
    Shredded,
    StoreError,
)
from edax.idempotency import (
    Claimed,
    Done,
    Failed,
    Guard,
    Pending,
    Record,
    Won,
    encode_record,
    joins_transaction,
)
from edax.message import CommandMessage
from edax.protected_ledger import ProtectedLedger
from edax.sealing import Cipher, MemoryKeys, Sealing
from edax.unit_of_work import current_tx, transaction

CLAIM = "billing:register-customer:request-42"
PAYLOAD = b'{"customer_id":"customer-42","email":"ada@example.test","invoice_id":"invoice-12"}'
RESULT = {"customer_id": "customer-42", "delivery_email": "private@example.test", "invoice_id": "invoice-12"}


def message(**changes: Any) -> CommandMessage:
    fields: dict[str, Any] = dict(
        id="command-42",
        name="RegisterCustomer",
        version=1,
        data=PAYLOAD,
        tenant="bookshop",
        subject="customer-42",
        encryption=None,
        aggregate_id="invoice-12",
        idempotency_key="request-42",
        correlation_id="flow-42",
        causation_id="event-41",
        issued_at=1_712_340_000_000_000_000,
        metadata={},
        extensions={},
    )
    return CommandMessage(**(fields | changes))


async def prepared(
    store: Any,
    sealing: Sealing,
    *,
    command: CommandMessage | None = None,
    payload: bytes = PAYLOAD,
) -> ProtectedLedger[Any]:
    return await ProtectedLedger(
        store, sealing, command if command is not None else message(), payload
    ).prepare()


def replay_guard(store: ProtectedLedger[Any]) -> Guard[Any]:
    return Guard(store=store, name="RegisterCustomer", key=CLAIM, payload_hash=0, observe=wrap(None))


async def complete(store: ProtectedLedger[Any], result: Any = RESULT) -> Won:
    won = await store.claim(CLAIM, 0)
    assert isinstance(won, Won)
    await store.complete(CLAIM, won.handle, Record.done(0, result))
    return won


@pytest.mark.parametrize("result", [RESULT, None], ids=["private-result", "null-result"])
@pytest.mark.parametrize("cipher", ["aes-256-gcm", "xchacha20-poly1305"])
async def test_stored_outcome_has_no_plaintext_hash_or_result_and_replays(
    result: Any, cipher: Cipher
) -> None:
    raw = MemoryLedger()
    keys = MemoryKeys()
    sealing = Sealing(keys, "orders-production", cipher=cipher)
    first = await prepared(raw, sealing)
    await complete(first, result)
    stored = await raw.claim(first.storage_key(CLAIM), 0)
    assert isinstance(stored, Record) and isinstance(stored.state, Done)
    assert stored.payload_hash == 0
    assert isinstance(stored.state.result, str)
    wire = encode_record(stored)
    assert "ada@example.test" not in wire and "private@example.test" not in wire
    protected = json.loads(stored.state.result)
    assert len(protected["fingerprint"]["value"]) == 32
    assert protected["fingerprint"]["algorithm"] == "hmac-sha256"
    assert protected["state"]["kind"] == "done"
    # A different transport instance and a different JSON key order are the same command intent.
    other_cipher: Cipher = "aes-256-gcm" if cipher == "xchacha20-poly1305" else "xchacha20-poly1305"
    replay = await prepared(
        raw,
        Sealing(keys, "orders-production", cipher=other_cipher),
        command=message(id="command-redelivery"),
        payload=b'{"invoice_id":"invoice-12","email":"ada@example.test","customer_id":"customer-42"}',
    )
    prior = await replay.claim(CLAIM, 0)
    assert isinstance(prior, Record)
    assert replay_guard(replay).replay(prior) == result


async def test_contender_is_pending_until_full_fingerprint_can_be_compared() -> None:
    raw = MemoryLedger()
    sealing = Sealing(MemoryKeys(), "orders-production")
    winner = await prepared(raw, sealing)
    changed = await prepared(
        raw, sealing, payload=PAYLOAD.replace(b"ada@example.test", b"grace@example.test")
    )
    won = await winner.claim(CLAIM, 0)
    assert isinstance(won, Won)
    pending = await changed.claim(CLAIM, 0)
    assert isinstance(pending, Record) and isinstance(pending.state, Pending)
    with pytest.raises(InProgress):
        replay_guard(changed).replay(pending)
    await winner.complete(CLAIM, won.handle, Record.done(0, RESULT))
    with pytest.raises(PayloadMismatch):
        await changed.claim(CLAIM, 0)


@pytest.mark.parametrize("reason", [None, "not_found"])
async def test_failure_cache_contains_only_the_terminal_code(reason: str | None) -> None:
    raw = MemoryLedger()
    sealing = Sealing(MemoryKeys(), "orders-production")
    ledger = await prepared(raw, sealing)
    won = await ledger.claim(CLAIM, 0)
    assert isinstance(won, Won)
    await ledger.complete(
        CLAIM, won.handle, Record.failed(0, "rejected email ada@example.test", "rejected", reason)
    )
    stored = await raw.claim(ledger.storage_key(CLAIM), 0)
    assert isinstance(stored, Record) and isinstance(stored.state, Done)
    assert "ada@example.test" not in encode_record(stored)
    assert json.loads(stored.state.result)["state"] == {"kind": "failed", "code": "rejected"}
    replay = await prepared(raw, sealing)
    prior = await replay.claim(CLAIM, 0)
    assert isinstance(prior, Record) and isinstance(prior.state, Failed)
    with pytest.raises(CachedFailure) as error:
        replay_guard(replay).replay(prior)
    assert error.value.original_code == "rejected"
    assert error.value.original_reason is None
    assert "ada@example.test" not in str(error.value)


@pytest.mark.parametrize(
    "record",
    [
        Record.done(0, RESULT),
        Record.failed(0, "private@example.test", "rejected"),
        Record.done(0, "invalid"),
        Record.done(17, "invalid"),
    ],
    ids=["legacy-success", "legacy-failure", "malformed-protected-json", "nonconstant-hash"],
)
async def test_legacy_and_malformed_records_fail_closed(record: Record) -> None:
    raw = MemoryLedger()
    ledger = await prepared(raw, Sealing(MemoryKeys(), "orders-production"))
    raw.put_raw(CLAIM, encode_record(record))
    with pytest.raises(CodecError):
        await ledger.claim(CLAIM, 0)


async def test_scopes_do_not_alias_and_swapped_records_are_rejected() -> None:
    raw = MemoryLedger()
    keys = MemoryKeys()
    original = await prepared(raw, Sealing(keys, "orders-production"))
    await complete(original)
    stored = await raw.claim(original.storage_key(CLAIM), 0)
    assert isinstance(stored, Record)
    for namespace, tenant, subject in [
        ("orders-staging", "bookshop", "customer-42"),
        ("orders-production", "another-bookshop", "customer-42"),
        ("orders-production", "bookshop", "customer-43"),
    ]:
        other = await prepared(raw, Sealing(keys, namespace), command=message(tenant=tenant, subject=subject))
        assert other.storage_key(CLAIM) == original.storage_key(CLAIM)
        raw.put_raw(other.storage_key(CLAIM), encode_record(stored))
        with pytest.raises(CodecError):
            await other.claim(CLAIM, 0)


async def test_shredding_and_provider_failure_never_return_cached_plaintext() -> None:
    class UnavailableKeys(MemoryKeys):
        unavailable = False
        destroy_after_resolve = False

        async def resolve(self, scope: Any, cipher: Any, key_id: str) -> bytes:
            if self.unavailable:
                raise KeyUnavailable("provider is unavailable")
            secret = await super().resolve(scope, cipher, key_id)
            if self.destroy_after_resolve:
                await self.destroy(scope)
            return secret

    raw = MemoryLedger()
    keys = UnavailableKeys()
    sealing = Sealing(keys, "orders-production")
    ledger = await prepared(raw, sealing)
    await complete(ledger)
    replay = await prepared(raw, sealing)
    keys.unavailable = True
    with pytest.raises(KeyUnavailable):
        await replay.claim(CLAIM, 0)
    keys.unavailable = False
    assert (await replay.claim(CLAIM, 0)) == Record.done(0, RESULT)
    keys.destroy_after_resolve = True
    with pytest.raises(Shredded):
        await replay.claim(CLAIM, 0)
    assert await keys.status(ledger.scope) == "destroyed"
    with pytest.raises(Shredded):
        await prepared(raw, sealing)


@pytest.mark.parametrize("tamper", ["ciphertext", "short-fingerprint"])
async def test_tampered_protected_results_never_replay(tamper: str) -> None:
    raw = MemoryLedger()
    sealing = Sealing(MemoryKeys(), "orders-production")
    ledger = await prepared(raw, sealing)
    await complete(ledger)
    stored = await raw.claim(ledger.storage_key(CLAIM), 0)
    assert isinstance(stored, Record) and isinstance(stored.state, Done)
    protected = json.loads(stored.state.result)
    if tamper == "ciphertext":
        ciphertext = bytearray(base64.b64decode(protected["state"]["result"]["data"]))
        ciphertext[0] ^= 1
        protected["state"]["result"]["data"] = base64.b64encode(ciphertext).decode()
    else:
        protected["fingerprint"]["value"] = protected["fingerprint"]["value"][:8]
    raw.put_raw(ledger.storage_key(CLAIM), encode_record(Record.done(0, json.dumps(protected))))
    with pytest.raises(CodecError):
        await (await prepared(raw, sealing)).claim(CLAIM, 0)


async def test_prepared_settlement_survives_an_ambiguous_complete_response() -> None:
    class LostReplyLedger(RecordingLedger):
        async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
            await super().complete(key, handle, record, tx)
            if len(self.records) == 1:
                raise StoreError("completed response lost")

    raw = LostReplyLedger()
    ledger = await prepared(raw, Sealing(MemoryKeys(), "orders-production"))
    won = await ledger.claim(CLAIM, 0)
    assert isinstance(won, Won)
    with pytest.raises(StoreError):
        await ledger.complete(CLAIM, won.handle, Record.done(0, RESULT))
    await ledger.complete(CLAIM, won.handle, Record.done(0, RESULT))
    assert len(raw.records) == 2 and raw.records[0] == raw.records[1]


async def test_release_forget_and_stale_owner_use_the_underlying_fence() -> None:
    raw = MemoryLedger()
    sealing = Sealing(MemoryKeys(), "orders-production")
    old = await prepared(raw, sealing)
    old_claim = await old.claim(CLAIM, 0)
    assert isinstance(old_claim, Won)
    with pytest.raises(InProgress):
        await old.forget(CLAIM)
    assert raw.expire(old.storage_key(CLAIM))
    new = await prepared(raw, sealing)
    new_claim = await new.claim(CLAIM, 0)
    assert isinstance(new_claim, Won)
    with pytest.raises(StoreError):
        await old.complete(CLAIM, old_claim.handle, Record.done(0, RESULT))
    await old.release(CLAIM, old_claim.handle)
    still_pending = await raw.claim(new.storage_key(CLAIM), 0)
    assert isinstance(still_pending, Record) and isinstance(still_pending.state, Pending)
    await new.complete(CLAIM, new_claim.handle, Record.done(0, RESULT))
    await new.forget(CLAIM)
    again = await prepared(raw, sealing)
    won = await again.claim(CLAIM, 0)
    assert isinstance(won, Won)
    await again.release(CLAIM, won.handle)
    assert isinstance(await again.claim(CLAIM, 0), Won)


async def test_transaction_identity_and_rollback_are_preserved() -> None:
    transactor = MemoryTransactor()
    raw = MemoryIdempotencyStore(transactor)
    sealing = Sealing(MemoryKeys(), "orders-production")
    ledger = await prepared(raw, sealing)
    assert joins_transaction(ledger)
    assert ledger.transactor is transactor
    with pytest.raises(RuntimeError, match="abort"):
        async with transaction(transactor) as tx:
            assert current_tx(ledger) is tx
            won = await ledger.claim(CLAIM, 0, tx)
            assert isinstance(won, Won)
            await ledger.complete(CLAIM, won.handle, Record.done(0, RESULT), tx)
            raise RuntimeError("abort")
    retried = await prepared(raw, sealing)
    assert isinstance(await retried.claim(CLAIM, 0), Won)
    standalone = await prepared(MemoryLedger(), sealing)
    assert not joins_transaction(standalone)


async def test_unprepared_wrong_hash_or_unknown_owner_never_settles() -> None:
    raw = MemoryLedger()
    ledger = ProtectedLedger(raw, Sealing(MemoryKeys(), "orders-production"), message(), PAYLOAD)
    with pytest.raises(BadHandle, match="prepared"):
        await ledger.claim(CLAIM, 0)
    await ledger.prepare()
    await ledger.prepare()
    with pytest.raises(BadHandle, match="constant"):
        await ledger.claim(CLAIM, 42)
    with pytest.raises(BadHandle, match="owner"):
        await ledger.complete(CLAIM, b"missing", Record.done(0, RESULT))
    won = await ledger.claim(CLAIM, 0)
    assert isinstance(won, Won)
    with pytest.raises(BadHandle, match="constant"):
        await ledger.complete(CLAIM, won.handle, Record.done(42, RESULT))
    with pytest.raises(BadHandle, match="completed"):
        await ledger.complete(CLAIM, won.handle, Record.pending(0))


async def test_legacy_empty_owner_handles_get_an_opaque_claim_identity() -> None:
    class EmptyHandleLedger(MemoryLedger):
        handle: bytes = b""

        async def claim(self, key: str, payload_hash: int, tx: Any = None) -> Claimed:
            result = await super().claim(key, payload_hash, tx)
            if isinstance(result, Won):
                self.handle = result.handle
                return Won()
            return result

        async def complete(self, key: str, handle: bytes, record: Record, tx: Any = None) -> None:
            assert handle == b""
            await super().complete(key, self.handle, record, tx)

    raw = EmptyHandleLedger()
    ledger = await prepared(raw, Sealing(MemoryKeys(), "orders-production"))
    await complete(ledger)
    stored = await raw.claim(ledger.storage_key(CLAIM), 0)
    assert isinstance(stored, Record) and isinstance(stored.state, Done)
    assert json.loads(stored.state.result)["owner"]


async def test_pre_migration_pending_claim_is_not_bypassed() -> None:
    raw = MemoryLedger()
    old = await raw.claim(CLAIM, 17)
    assert isinstance(old, Won)
    ledger = await prepared(raw, Sealing(MemoryKeys(), "orders-production"))
    with pytest.raises(CodecError):
        await ledger.claim(CLAIM, 0)
    await raw.complete(CLAIM, old.handle, Record.done(17, RESULT))
    with pytest.raises(CodecError):
        await ledger.claim(CLAIM, 0)
