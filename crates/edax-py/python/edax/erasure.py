"""Durable erasure orchestration over kernel-owned workflow decisions.

Participants must make cleanup idempotent and install destination write fences before acknowledging.
Retries can repeat external effects after a crash. A notification is durably enqueued before cleanup;
its relay uses the authorized control path independently of subject and tenant payload keys.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, TypedDict, cast, get_args

from . import _names, _native
from ._wiring import wiring_u64
from .errors import BadHandle, ConcurrencyError
from .sealing import KeyProvider, KeyScope

__all__ = [
    "Cleanup",
    "ErasureCoordinator",
    "ErasureNotification",
    "ErasureOperation",
    "ErasureStage",
    "ErasureStore",
    "MemoryErasureStore",
    "ScopeCleanup",
    "StoredErasure",
]

ErasureStage = Literal["persisted", "revoked", "notified", "destruction_confirmed", "complete"]
ErasureAction = Literal["revoke", "notify", "acknowledge", "destroy", "complete"]
Cleanup = Callable[[KeyScope], Awaitable[None]]
_names.check(get_args(ErasureStage), _native.erasure_stages(), "erasure stages")
_names.check(get_args(ErasureAction), _native.erasure_actions(), "erasure actions")


class _ScopeWire(TypedDict):
    key_namespace: str
    tenant: str | None
    subject: str | None


class _OperationWire(TypedDict):
    operation_id: str
    request_id: str
    scope: _ScopeWire
    required_participants: list[str]
    acknowledgements: list[str]
    stage: str


@dataclass(frozen=True, slots=True)
class ErasureOperation:
    """Immutable, validated kernel record. Its participant set never changes after creation."""

    record: str = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "record", _native.erasure_validate(self.record))

    @property
    def _wire(self) -> _OperationWire:
        return cast(_OperationWire, json.loads(self.record))

    @property
    def operation_id(self) -> str:
        return self._wire["operation_id"]

    @property
    def request_id(self) -> str:
        return self._wire["request_id"]

    @property
    def scope(self) -> KeyScope:
        return KeyScope(**self._wire["scope"])

    @property
    def required_participants(self) -> frozenset[str]:
        return frozenset(self._wire["required_participants"])

    @property
    def acknowledgements(self) -> frozenset[str]:
        return frozenset(self._wire["acknowledgements"])

    @property
    def stage(self) -> ErasureStage:
        return _names.named(self._wire["stage"], get_args(ErasureStage), "erasure stage")


@dataclass(frozen=True, slots=True)
class StoredErasure:
    """An operation with the revision used for compare-and-set updates."""

    version: int
    operation: ErasureOperation

    def to_json(self) -> str:
        return json.dumps({"version": self.version, "operation": json.loads(self.operation.record)})


@dataclass(frozen=True, slots=True)
class ErasureNotification:
    """A durable control message. Publish its record unchanged and acknowledge only after success."""

    record: str

    @property
    def operation_id(self) -> str:
        return cast(str, json.loads(self.record)["operation_id"])


class ErasureStore(Protocol):
    """Durable workflow port. Creation binds one scope/request to its frozen participants.

    ``advance`` atomically checks the revision, applies the kernel transition, and enqueues a stable
    notification when entering ``notified``. Recovery resumes ``pending`` rows. Publication is at least
    once: consumers deduplicate on the operation ID. Implementations must persist these rules.
    """

    async def create(self, candidate: ErasureOperation) -> StoredErasure: ...
    async def get(self, scope: KeyScope, request_id: str) -> StoredErasure | None: ...
    async def get_by_id(self, operation_id: str) -> StoredErasure | None: ...
    async def advance(
        self, previous: StoredErasure, action: ErasureAction, participant: str | None = None
    ) -> StoredErasure: ...
    async def pending(self, limit: int = 100, *, after: str | None = None) -> list[StoredErasure]: ...
    async def revoked(self, limit: int = 100, *, after: str | None = None) -> list[StoredErasure]: ...
    async def unpublished(self, limit: int = 100) -> list[ErasureNotification]: ...
    async def mark_published(self, operation_id: str) -> None: ...


class MemoryErasureStore:
    """Process-local reference. Restart loses operations and notifications; it is not durable storage."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._operations: dict[str, StoredErasure] = {}
        self._requests: dict[tuple[bytes, str], str] = {}
        self._notifications: dict[str, ErasureNotification] = {}

    @staticmethod
    def _key(scope: KeyScope, request_id: str) -> tuple[bytes, str]:
        return bytes(_native.encode_key_scope(scope)), request_id

    async def create(self, candidate: ErasureOperation) -> StoredErasure:
        # Re-validate that this is an initial operation through the kernel's creation path.
        expected = _native.erasure_create(
            candidate.scope,
            candidate.operation_id,
            candidate.request_id,
            sorted(candidate.required_participants),
        )
        if candidate.record != _native.erasure_validate(expected):
            raise BadHandle("an erasure store creates only initial operations")
        key = self._key(candidate.scope, candidate.request_id)
        async with self._lock:
            existing_id = self._requests.get(key)
            if existing_id is not None:
                existing = self._operations[existing_id]
                _native.erasure_check_request(existing.operation.record, candidate.record)
                return existing
            if candidate.operation_id in self._operations:
                raise BadHandle("erasure operation ID is already in use")
            stored = StoredErasure(1, candidate)
            self._requests[key] = candidate.operation_id
            self._operations[candidate.operation_id] = stored
            return stored

    async def get(self, scope: KeyScope, request_id: str) -> StoredErasure | None:
        key = self._key(scope, request_id)
        async with self._lock:
            operation_id = self._requests.get(key)
            return None if operation_id is None else self._operations[operation_id]

    async def get_by_id(self, operation_id: str) -> StoredErasure | None:
        async with self._lock:
            return self._operations.get(operation_id)

    async def advance(
        self, previous: StoredErasure, action: ErasureAction, participant: str | None = None
    ) -> StoredErasure:
        async with self._lock:
            current = self._operations.get(previous.operation.operation_id)
            if current != previous:
                raise ConcurrencyError("erasure operation revision changed")
            next_operation = ErasureOperation(
                _native.erasure_apply(current.operation.record, action, participant)
            )
            notification = _native.erasure_notification(current.operation.record, next_operation.record)
            stored = StoredErasure(current.version + 1, next_operation)
            self._operations[next_operation.operation_id] = stored
            if notification is not None:
                self._notifications[next_operation.operation_id] = ErasureNotification(notification)
            return stored

    async def _page(
        self, limit: int, after: str | None, keep: Callable[[StoredErasure], bool]
    ) -> list[StoredErasure]:
        limit = wiring_u64(limit, "limit")
        async with self._lock:
            return [
                row
                for key, row in sorted(self._operations.items())
                if keep(row) and (after is None or key > after)
            ][:limit]

    async def pending(self, limit: int = 100, *, after: str | None = None) -> list[StoredErasure]:
        return await self._page(limit, after, lambda row: row.operation.stage != "complete")

    async def revoked(self, limit: int = 100, *, after: str | None = None) -> list[StoredErasure]:
        return await self._page(limit, after, lambda row: _native.erasure_scope_revoked(row.operation.record))

    async def unpublished(self, limit: int = 100) -> list[ErasureNotification]:
        limit = wiring_u64(limit, "limit")
        async with self._lock:
            return [row for _, row in sorted(self._notifications.items())][:limit]

    async def mark_published(self, operation_id: str) -> None:
        async with self._lock:
            self._notifications.pop(operation_id, None)


class ScopeCleanup:
    """An erasure participant that deletes a scope's plaintext rows from one store: what a sink or store's
    ``cleanup()`` returns for a target kept ``at_rest="plaintext"``. Register it with the
    :class:`ErasureCoordinator`; edax does not check that you did.

    :meth:`resweep` deletes again for every scope the erasure store has revoked and returns how many
    rows it found. A count above zero means a participant was missing, or a restore brought rows back:
    run it on a schedule or as a deploy check, and alert on it. It covers a restore only when the
    erasure store outlives the restored database. The coordinator can report ``complete`` before the
    next sweep runs."""

    def __init__(self, delete: Callable[[KeyScope], Awaitable[int]]) -> None:
        self._delete = delete

    async def __call__(self, scope: KeyScope) -> None:
        await self._delete(scope)

    async def resweep(self, store: ErasureStore, *, limit: int = 100) -> int:
        deleted = 0
        after: str | None = None
        while page := await store.revoked(limit, after=after):
            for stored in page:
                deleted += await self._delete(stored.operation.scope)
            after = page[-1].operation.operation_id
        return deleted


class _StaleOperation(Exception):
    """Only a workflow-store CAS conflict, never a provider or participant exception."""


class ErasureCoordinator:
    """Resume durable erasure effects, advancing only after their confirmations.

    Cleanup callbacks are idempotent participants, not arbitrary delete functions: each must install
    its write fence, drain work, delete plaintext and satisfy its backup/restore obligations. A missing
    callback remains pending for an explicit acknowledgement or a later worker. Provider and callback
    failures propagate, preserving the last durable stage for retry. No exception text is persisted.
    """

    def __init__(
        self,
        keys: KeyProvider,
        store: ErasureStore,
        participants: Mapping[str, Cleanup | None],
        *,
        required_participants: Sequence[str] | None = None,
    ) -> None:
        self._keys = keys
        self._store = store
        self._participants = dict(participants)
        self._required = tuple(participants if required_participants is None else required_participants)
        _native.erasure_validate_participants(list(self._required), list(participants))

    async def erase(self, scope: KeyScope, request_id: str) -> ErasureOperation:
        candidate = ErasureOperation(
            _native.erasure_create(scope, _native.guid(), request_id, list(self._required))
        )
        stored = await self._store.create(candidate)
        return await self.resume(stored.operation.scope, stored.operation.request_id)

    async def status(self, scope: KeyScope, request_id: str) -> ErasureOperation | None:
        stored = await self._store.get(scope, request_id)
        return None if stored is None else stored.operation

    async def acknowledge(self, scope: KeyScope, request_id: str, participant: str) -> ErasureOperation:
        """Explicitly confirm a registered participant's finished fencing and cleanup."""
        while True:
            stored = await self._require(scope, request_id)
            try:
                result = await self._store.advance(stored, "acknowledge", participant)
                return result.operation
            except ConcurrencyError:
                continue

    async def _require(self, scope: KeyScope, request_id: str) -> StoredErasure:
        stored = await self._store.get(scope, request_id)
        if stored is None:
            raise BadHandle("erasure operation does not exist")
        return stored

    async def _advance(
        self, stored: StoredErasure, action: ErasureAction, participant: str | None = None
    ) -> StoredErasure:
        try:
            return await self._store.advance(stored, action, participant)
        except ConcurrencyError as error:
            raise _StaleOperation from error

    async def _cleanup(self, stored: StoredErasure) -> StoredErasure:
        operation = stored.operation
        failure: Exception | None = None
        for participant in sorted(operation.required_participants - operation.acknowledgements):
            cleanup = self._participants.get(participant)
            if cleanup is None:
                continue
            try:
                await cleanup(operation.scope)
            except Exception as error:  # noqa: BLE001 — retain and re-raise after other cleanup attempts.
                if failure is None:
                    failure = error
                continue
            stored = await self._advance(stored, "acknowledge", participant)
        if failure is not None:
            raise failure
        return stored

    async def resume(self, scope: KeyScope, request_id: str) -> ErasureOperation:
        """Resume an existing request using its frozen participants, including ones currently offline."""
        while True:
            stored = await self._require(scope, request_id)
            operation = stored.operation
            stage = operation.stage
            try:
                match stage:
                    case "persisted":
                        status = await self._keys.revoke(operation.scope)
                        if not _native.erasure_provider_confirmed("revoke", status):
                            return operation
                        await self._advance(stored, "revoke")
                    case "revoked":
                        await self._advance(stored, "notify")
                    case "notified":
                        stored = await self._cleanup(stored)
                        operation = stored.operation
                        if not _native.erasure_can_destroy(operation.record):
                            return operation
                        status = await self._keys.destroy(operation.scope)
                        if not _native.erasure_provider_confirmed("destroy", status):
                            return operation
                        await self._advance(stored, "destroy")
                    case "destruction_confirmed":
                        await self._advance(stored, "complete")
                    case "complete":
                        return operation
                    case _:
                        _names.unreachable(stage)
            except _StaleOperation:
                # The external effect may already have succeeded. Reload and repeat it idempotently.
                continue
