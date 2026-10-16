"""Protected command outcomes over the existing atomic idempotency-store seam.

The underlying ledger sees only a constant hash and an opaque protected record. Scope composition,
full keyed fingerprint checks, settlement, and result authentication belong to the CQRS kernel.
Each adapter belongs to one dispatch; the ordinary guard still owns transaction ordering and release.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic

from . import _json, _names, _native
from .errors import BadHandle, CodecError
from .idempotency import (
    Claimed,
    Done,
    Failed,
    IdempotencyStore,
    Pending,
    Record,
    Won,
    encode_record,
    joins_transaction,
)
from .message import CommandMessage
from .sealing import Sealing
from .unit_of_work import TxT

__all__ = ["ProtectedLedger"]

_names.check(("pending", "done", "failed"), _native.protected_replay_actions(), "protected replay actions")


@dataclass
class _Claim:
    pending: str
    owner: str
    settled: str | None = None


class ProtectedLedger(Generic[TxT]):
    """Per-dispatch adapter. Call :meth:`prepare` before giving it to an ordinary ``Guard``.

    Configure that guard with ``payload_hash=0``. Pending contenders remain pending even when their
    payloads differ; the protected record compares full fingerprints once the owner settles. Hold the
    sealing provider's write guard through the dispatch's final storage commit.
    """

    def __init__(
        self,
        store: IdempotencyStore[TxT],
        sealing: Sealing,
        message: CommandMessage,
        canonical_payload: bytes,
    ) -> None:
        self.store = store
        self.sealing = sealing
        self.scope = _native.protected_scope(sealing.key_namespace, message.tenant, message.subject)
        self._name, self._version = message.name, message.version
        self._canonical_payload = canonical_payload
        self._fingerprint: str | None = None
        self._claims: dict[tuple[str, bytes], _Claim] = {}

    @property
    def joins_transaction(self) -> bool:
        return joins_transaction(self.store)

    @property
    def transactor(self) -> object:
        return getattr(self.store, "transactor", self.store)

    async def prepare(self) -> ProtectedLedger[TxT]:
        """Obtain the dedicated fingerprint key once, without retaining its secret bytes."""
        if self._fingerprint is None:
            reserved = await self.sealing.keys.fingerprint(self.scope)
            self._fingerprint = _native.protected_fingerprint(
                self._name, self._version, self._canonical_payload, reserved.key_id, bytes(reserved.key)
            )
            self._canonical_payload = b""
        await self.sealing.check_active(self.scope.tenant, self.scope.subject)
        return self

    def _prepared(self) -> str:
        if self._fingerprint is None:
            raise BadHandle("protected ledger must be prepared before claiming")
        return self._fingerprint

    def storage_key(self, key: str) -> str:
        """The unchanged ordinary claim identity; the record authenticates its security scope."""
        return _native.protected_claim_key(self.scope, key)

    async def claim(self, key: str, payload_hash: int, tx: TxT | None = None) -> Claimed:
        fingerprint = self._prepared()
        if payload_hash != 0:
            raise BadHandle("protected ledger requires the constant guard hash")
        await self.sealing.check_active(self.scope.tenant, self.scope.subject)
        stored_key = self.storage_key(key)
        claimed = await self.store.claim(stored_key, 0, tx=tx)
        if isinstance(claimed, Won):
            owner = claimed.handle.hex() if claimed.handle else _native.guid()
            self._claims[stored_key, claimed.handle] = _Claim(
                _native.protected_pending(self.scope, stored_key, owner, fingerprint), owner
            )
            return claimed
        if claimed.payload_hash != 0:
            raise CodecError("invalid protected ledger envelope")
        if isinstance(claimed.state, Pending):
            return Record.pending(0)
        if not isinstance(claimed.state, Done) or not isinstance(claimed.state.result, str):
            raise CodecError("invalid protected ledger envelope")
        protected = claimed.state.result
        replay = _native.protected_replay(protected, self.scope, stored_key, fingerprint)
        action = _names.named(replay.action, ("pending", "done", "failed"), "protected replay action")
        if action == "pending":
            return Record.pending(0)
        if action == "failed":
            await self.sealing.check_active(self.scope.tenant, self.scope.subject)
            return Record.failed(
                0, "protected command failed", _names.given(replay.code, "protected failure code")
            )
        info = _names.given(replay.encryption, "protected result encryption")
        secret = await self.sealing.resolve(info, self.scope.tenant, self.scope.subject)
        plaintext = bytes(_native.protected_open(protected, secret))
        result = _json.decode(plaintext)
        # Provider I/O or decoding may span erasure. Recheck before returning an opened result.
        await self.sealing.check_active(self.scope.tenant, self.scope.subject)
        return Record.done(0, result)

    async def complete(self, key: str, handle: bytes, record: Record, tx: TxT | None = None) -> None:
        if record.payload_hash != 0:
            raise BadHandle("protected ledger requires the constant guard hash")
        stored_key = self.storage_key(key)
        claim = self._claims.get((stored_key, handle))
        if claim is None:
            raise BadHandle("protected settlement has no local claim owner")
        await self.sealing.check_active(self.scope.tenant, self.scope.subject)
        if claim.settled is None:
            if isinstance(record.state, Done):
                # Validate ordinary result limits before hiding the result inside a protected string.
                encode_record(record)
                info, secret = await self.sealing.reserve(self.scope.tenant, self.scope.subject)
                claim.settled = _native.protected_finish(
                    claim.pending, claim.owner, info, secret, _json.encode_bytes(record.state.result)
                )
            elif isinstance(record.state, Failed):
                claim.settled = _native.protected_failure(
                    claim.pending,
                    claim.owner,
                    _names.given(record.state.error_code, "protected failure code"),
                )
            else:
                raise BadHandle("protected settlement requires a completed outcome")
        await self.store.complete(stored_key, handle, Record.done(0, claim.settled), tx=tx)

    async def release(self, key: str, handle: bytes, tx: TxT | None = None) -> None:
        stored_key = self.storage_key(key)
        await self.store.release(stored_key, handle, tx=tx)
        self._claims.pop((stored_key, handle), None)

    async def forget(self, key: str, tx: TxT | None = None) -> None:
        await self.store.forget(self.storage_key(key), tx=tx)
