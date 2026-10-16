"""Authenticated payloads and an async key-provider port; shared decisions remain in the kernels.

Providers must resolve exact scope, purpose, and generation before reporting destruction. They must
fence revocation atomically with reservations. The reference provider is process-local and loses keys
and tombstones on restart. Neither adapter nor :class:`Sealing` caches key material.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Literal, Protocol, cast, get_args, runtime_checkable

from . import _names, _native
from ._owned import ThreadBound
from ._values import give_value_semantics
from .errors import BadHandle, CodecError, Shredded
from .message import CommandMessage, EventDraft, EventMessage

__all__ = [
    "Cipher",
    "EncryptionInfo",
    "KeyProvider",
    "KeyReservation",
    "KeyScope",
    "KeyStatus",
    "MemoryKeys",
    "Opened",
    "SealedValue",
    "Sealing",
    "TransactionFence",
    "WriteFence",
]

Cipher = Literal["aes-256-gcm", "xchacha20-poly1305"]
KeyStatus = Literal["unknown", "active", "revoking", "destroyed"]
#: How :meth:`Sealing.write_guard` fences a write: ``"guard"`` holds the provider's ``guard`` from before
#: the transaction through its commit; ``"transaction"`` fences inside each transaction that opens.
WriteFence = Literal["guard", "transaction"]
EncryptionInfo = _native.EncryptionInfo
KeyScope = _native.KeyScope
SealedValue = _native.SealedValue


@dataclass(frozen=True)
class KeyReservation:
    """One operation's secret bytes; diagnostics expose only its public generation ID."""

    key_id: str
    key: bytes = field(repr=False)


_names.check(get_args(Cipher), _native.encryption_ciphers(), "encryption ciphers")
_names.check(get_args(KeyStatus), _native.key_statuses(), "key statuses")
give_value_semantics(KeyScope)
give_value_semantics(SealedValue)
# A reservation contains a secret. Do not give it the field-printing repr of public value records.


class KeyProvider(Protocol):
    """Async I/O port. A missing key is an error, never evidence of destruction.

    ``reserve`` charges an invocation atomically and rotates exhausted generations. ``resolve`` never
    creates material and checks exact ownership and purpose before returning ``Shredded``. Revoking a
    tenant scope fences every current and future subject. Returned keys belong to one operation only.
    Production providers must retain permanent tombstones across restarts and backup restoration.
    """

    async def reserve(self, scope: KeyScope, cipher: Cipher) -> KeyReservation: ...
    async def resolve(self, scope: KeyScope, cipher: Cipher, key_id: str) -> bytes: ...
    async def fingerprint(self, scope: KeyScope) -> KeyReservation: ...
    async def resolve_fingerprint(self, scope: KeyScope, key_id: str) -> bytes: ...
    async def revoke(self, scope: KeyScope) -> KeyStatus: ...
    async def destroy(self, scope: KeyScope) -> KeyStatus: ...
    async def status(self, scope: KeyScope) -> KeyStatus: ...

    def guard(self, scope: KeyScope) -> AbstractAsyncContextManager[None]:
        """Fence storage writes through transaction commit against revocation.

        A production provider must coordinate this guard with the configured storage transaction.
        Merely locking its remote key service does not fence independent application storage.
        """
        ...


@runtime_checkable
class TransactionFence(KeyProvider, Protocol):
    """A provider that fences writes inside the transaction that makes them, for
    ``Sealing(write_fence="transaction")``.

    ``transactor`` names the database where ``revoke`` drains writers: edax fences inside a transaction
    only when its transactor's :func:`~edax.unit_of_work.database_of` is this one's, and holds ``guard``
    around transactions on any other database.
    """

    transactor: object

    async def fence(self, tx: Any, scopes: Sequence[KeyScope]) -> None:
        """Admit writes to ``scopes`` inside ``tx``, a READ COMMITTED transaction of ``transactor``, until it
        commits or rolls back. Raise :class:`~edax.errors.KeyRevoking` or :class:`~edax.errors.Shredded` for
        a fenced scope, as ``guard`` does. Idempotent within one transaction.

        A Postgres provider takes the kernel's lock calls for all ``scopes`` at once
        (``postgres_erasure_writer_locks``), then reads the status in a separate statement: a snapshot
        taken before the lock wait can miss a revoke that committed during it.
        """
        ...


_write_guards: ContextVar[dict[int, object] | None] = ContextVar("edax_write_guards", default=None)
_managed_write_guards: ContextVar[bool] = ContextVar("edax_managed_write_guards", default=False)


@contextmanager
def sealing_context() -> Iterator[None]:
    """Share guard lineage with structured child tasks for one transaction lifetime."""
    existing = _write_guards.get()
    lineage = existing if _managed_write_guards.get() and existing is not None else dict(existing or {})
    token = _write_guards.set(lineage)
    managed_token = _managed_write_guards.set(True)
    try:
        yield
    finally:
        _managed_write_guards.reset(managed_token)
        _write_guards.reset(token)


def detach_sealing_context() -> None:
    """Background workers must not inherit an application's write admission."""
    _write_guards.set(None)
    _managed_write_guards.set(False)
    _pending_fences.set(())
    _authenticated_event.set(None)
    _opened_event.set(None)


class MemoryKeys:
    """Process-local reference provider for development and tests, bound to its creating thread.

    The native mutex owns atomic creation, usage budgets, generations, and permanent revocation.
    These async methods do not suspend inside native operations. Rebuilding this provider loses all
    keys and tombstones; it cannot recover previously stored ciphertext.
    """

    def __init__(self) -> None:
        self._native = ThreadBound(_native.memory_keys(), "MemoryKeys")
        self._guard = asyncio.Lock()
        self._guard_owner: object | None = None
        self._guard_depth = 0

    async def reserve(self, scope: KeyScope, cipher: Cipher) -> KeyReservation:
        native = self._native.get()
        reserved = native.reserve(scope, cipher)
        return KeyReservation(reserved.key_id, bytes(native.resolve(scope, cipher, reserved.key_id)))

    async def resolve(self, scope: KeyScope, cipher: Cipher, key_id: str) -> bytes:
        return bytes(self._native.get().resolve(scope, cipher, key_id))

    async def fingerprint(self, scope: KeyScope) -> KeyReservation:
        native = self._native.get()
        reserved = native.fingerprint(scope)
        return KeyReservation(reserved.key_id, bytes(native.resolve_fingerprint(scope, reserved.key_id)))

    async def resolve_fingerprint(self, scope: KeyScope, key_id: str) -> bytes:
        return bytes(self._native.get().resolve_fingerprint(scope, key_id))

    async def revoke(self, scope: KeyScope) -> KeyStatus:
        native = self._native.get()
        self._check_not_writing()
        async with self._guard:
            return cast(KeyStatus, native.revoke(scope))

    async def destroy(self, scope: KeyScope) -> KeyStatus:
        native = self._native.get()
        self._check_not_writing()
        async with self._guard:
            return cast(KeyStatus, native.destroy(scope))

    async def status(self, scope: KeyScope) -> KeyStatus:
        return cast(KeyStatus, self._native.get().status(scope))

    def _check_not_writing(self) -> None:
        if self._guard_owner is not None and self._guard_owner is (_write_guards.get() or {}).get(id(self)):
            raise BadHandle("cannot revoke or destroy keys inside this task's active write guard")

    @asynccontextmanager
    async def guard(self, scope: KeyScope) -> AsyncIterator[None]:
        """Drain guarded writes before revocation. Keep this guard outside the commit context.

        The single process-local lock also fences a tenant revocation against every subject writer.
        It coordinates only writers using this same provider instance and this guard.
        """
        native = self._native.get()
        lineage = _write_guards.get()
        if lineage is None:
            lineage = {}
        pending = lineage.get(id(self))
        while isinstance(pending, asyncio.Future):
            # Sibling participants share one acquisition. Queueing both on the provider lock
            # would leave the second waiting for a commit that awaits that very participant.
            await asyncio.shield(pending)
            pending = lineage.get(id(self))
        if self._guard_owner is not None and self._guard_owner is lineage.get(id(self)):
            _native.check_key_write_status(native.status(scope))
            self._guard_depth += 1
        else:
            if not _managed_write_guards.get():
                # A completed guard's inherited mapping does not admit future independent tasks.
                lineage = dict(lineage)
            _write_guards.set(lineage)
            acquiring: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            lineage[id(self)] = acquiring
            try:
                await self._guard.acquire()
                try:
                    _native.check_key_write_status(native.status(scope))
                except BaseException:
                    self._guard.release()
                    raise
                self._guard_owner = object()
                lineage[id(self)] = self._guard_owner
                self._guard_depth = 1
            finally:
                if lineage.get(id(self)) is acquiring:
                    del lineage[id(self)]
                acquiring.set_result(None)
        try:
            yield
        finally:
            self._guard_depth -= 1
            if self._guard_depth == 0:
                self._guard_owner = None
                self._guard.release()


class PendingFence(AbstractAsyncContextManager[None]):
    """A write guard under ``write_fence="transaction"``. Entering it admits nothing: while it is entered,
    every edax transaction that opens fences its scope (:mod:`edax._units`), so a handler that writes
    nothing pays no fence. Enter it outside the transaction, as any write guard."""

    def __init__(self, keys: TransactionFence, scope: KeyScope) -> None:
        self.keys = keys
        self.scope = scope

    async def __aenter__(self) -> None:
        pending = _pending_fences.get()
        # An equal fence already pending covers this one: guards nest, so it outlives this block.
        if not any(other.keys is self.keys and other.scope == self.scope for other in pending):
            _pending_fences.set((*pending, self))

    async def __aexit__(
        self, kind: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        _pending_fences.set(tuple(pending for pending in _pending_fences.get() if pending is not self))


_pending_fences: ContextVar[tuple[PendingFence, ...]] = ContextVar("edax_pending_fences", default=())


def pending_fences() -> tuple[PendingFence, ...]:
    """The fences a transaction opening now in this task must take."""
    return _pending_fences.get()


@dataclass(frozen=True, slots=True)
class Opened:
    """Authenticated bytes returned by the consumer opening stage, before handler execution."""

    data: bytes = field(repr=False)


class Sealing:
    """Wire one authoritative provider into a deployment namespace.

    ``allow_plaintext`` explicitly permits legacy unencrypted reads. Writers always seal. Preserve
    returned sealed envelopes across ambiguous storage retries instead of sealing the same ID again.
    A destroyed scope raises on direct reads. Only the consumer-specific methods return ``None`` for
    provider-confirmed destruction; exceptions from handlers never pass through those methods.

    ``write_fence="transaction"`` needs a :class:`TransactionFence` provider. It fences only writes made
    in edax transactions: inside each one on the provider's database, with ``guard`` around each one on
    another. Writes through connections edax does not manage, and side effects outside storage, are not
    fenced; each such destination needs its own write fence before its cleanup acknowledges.
    """

    def __init__(
        self,
        keys: KeyProvider,
        key_namespace: str,
        *,
        cipher: Cipher | None = None,
        allow_plaintext: bool = False,
        write_fence: WriteFence = "guard",
    ) -> None:
        if write_fence not in get_args(WriteFence):
            raise BadHandle(f"unknown write fence {write_fence!r}: use one of {get_args(WriteFence)}")
        if write_fence == "transaction" and not isinstance(keys, TransactionFence):
            raise BadHandle("write_fence='transaction' needs a provider with transactor and fence()")
        self._fencing = cast(TransactionFence, keys) if write_fence == "transaction" else None
        self.keys = keys
        self.key_namespace = key_namespace
        selected = _native.default_cipher() if cipher is None else cipher
        self.cipher = cast(
            Cipher, _names.named(selected, tuple(_native.encryption_ciphers()), "encryption cipher")
        )
        self.allow_plaintext = allow_plaintext
        self.scope(None, None)

    def scope(self, tenant: str | None, subject: str | None) -> KeyScope:
        scope = KeyScope(key_namespace=self.key_namespace, tenant=tenant, subject=subject)
        _native.check_key_scope(scope)
        return scope

    def write_guard(self, tenant: str | None, subject: str | None) -> AbstractAsyncContextManager[None]:
        """Hold the provider's storage fence outside the transaction context, through commit; under
        ``write_fence="transaction"``, fence each transaction that opens while it is entered."""
        scope = self.scope(tenant, subject)
        if self._fencing is not None:
            return PendingFence(self._fencing, scope)
        return self.keys.guard(scope)

    @property
    def write_fence(self) -> WriteFence:
        return "guard" if self._fencing is None else "transaction"

    def skip_destroyed(self, tenant: str | None, subject: str | None) -> AbstractAsyncContextManager[None]:
        """A consumer's handler boundary. Under ``write_fence="transaction"`` a fence refuses inside the
        handler, so its :class:`~edax.errors.Shredded` is swallowed when the provider confirms the scope
        destroyed: the provider is trusted, the exception is not, since a handler may raise it for another
        scope. In the default mode the guard admitted the scope before the handler ran, so a handler's
        ``Shredded`` always propagates."""
        return nullcontext() if self._fencing is None else self._skip_destroyed(self.scope(tenant, subject))

    @asynccontextmanager
    async def _skip_destroyed(self, scope: KeyScope) -> AsyncIterator[None]:
        try:
            yield
        except Shredded:
            if not _native.erasure_provider_confirmed("destroy", await self.keys.status(scope)):
                raise

    async def check_active(self, tenant: str | None, subject: str | None) -> None:
        """Check revocation before returning a cached result or other previously decoded data."""
        _native.check_key_status(await self.keys.status(self.scope(tenant, subject)))

    async def reserve(self, tenant: str | None, subject: str | None) -> tuple[EncryptionInfo, bytes]:
        scope = _native.protected_scope(self.key_namespace, tenant, subject)
        reserved = await self.keys.reserve(scope, self.cipher)
        info = _native.fresh_encryption_info(self.key_namespace, reserved.key_id, self.cipher)
        # The kernel checks subject requirements and the configured namespace before encryption.
        _native.encryption_scope(self.key_namespace, info, tenant, subject)
        return info, bytes(reserved.key)

    async def resolve(self, info: EncryptionInfo, tenant: str | None, subject: str | None) -> bytes:
        scope = _native.encryption_scope(self.key_namespace, info, tenant, subject)
        # Ownership must resolve before status: an unrelated destroyed key ID must never cause a skip.
        key = await self.keys.resolve(scope, cast(Cipher, info.cipher), info.key_id)
        _native.check_key_status(await self.keys.status(scope))
        return key

    async def check_legacy(self, tenant: str | None, subject: str | None) -> None:
        """Legacy compatibility never permits restoring data into a revoked scope."""
        _native.check_plaintext_policy(self.allow_plaintext)
        scope = KeyScope(key_namespace=self.key_namespace, tenant=tenant, subject=subject)
        _native.check_key_scope(scope)
        _native.check_key_write_status(await self.keys.status(scope))

    async def seal_event(self, draft: EventDraft) -> EventDraft:
        info, key = await self.reserve(draft.tenant, draft.subject)
        return _native.seal_event(draft, info, key)

    async def open_event(self, message: EventMessage) -> bytes:
        if message.encryption is None:
            await self.check_legacy(message.tenant, message.subject)
            return bytes(message.data)
        key = await self.resolve(message.encryption, message.tenant, message.subject)
        return bytes(_native.open_event(message, key))

    async def open_event_for_consumer(self, message: EventMessage) -> Opened | None:
        if message.encryption is None:
            try:
                await self.check_legacy(message.tenant, message.subject)
            except Shredded:
                return None
            return Opened(bytes(message.data))
        try:
            key = await self.resolve(message.encryption, message.tenant, message.subject)
        except Shredded:
            return None
        return Opened(bytes(_native.open_event(message, key)))

    async def seal_command(self, message: CommandMessage) -> CommandMessage:
        info, key = await self.reserve(message.tenant, message.subject)
        return _native.seal_command(message, info, key)

    async def open_command(self, message: CommandMessage) -> bytes:
        if message.encryption is None:
            await self.check_legacy(message.tenant, message.subject)
            return bytes(message.data)
        key = await self.resolve(message.encryption, message.tenant, message.subject)
        return bytes(_native.open_command(message, key))

    async def open_command_for_consumer(self, message: CommandMessage) -> Opened | None:
        if message.encryption is None:
            try:
                await self.check_legacy(message.tenant, message.subject)
            except Shredded:
                return None
            return Opened(bytes(message.data))
        try:
            key = await self.resolve(message.encryption, message.tenant, message.subject)
        except Shredded:
            return None
        return Opened(bytes(_native.open_command(message, key)))

    async def seal_state(
        self,
        kind: Literal["snapshot", "saga"],
        identity: str,
        type_name: str,
        version: int,
        data: bytes,
        tenant: str | None,
        subject: str | None,
        marks: str = "",
        completed: bool = False,
    ) -> SealedValue:
        info, key = await self.reserve(tenant, subject)
        context = _native.state_encryption_context(
            kind, identity, type_name, version, marks, completed, info, tenant, subject
        )
        return SealedValue(encryption=info, data=_native.seal_bytes(info, key, context, data))

    async def open_state(
        self,
        kind: Literal["snapshot", "saga"],
        identity: str,
        type_name: str,
        version: int,
        value: SealedValue,
        tenant: str | None,
        subject: str | None,
        marks: str = "",
        completed: bool = False,
    ) -> bytes:
        info = value.encryption
        context = _native.state_encryption_context(
            kind, identity, type_name, version, marks, completed, info, tenant, subject
        )
        key = await self.resolve(info, tenant, subject)
        return bytes(_native.open_bytes(info, key, context, bytes(value.data)))


# An opened view never replaces the ciphertext used by guards or nested saga runners.

_opened_event: ContextVar[tuple[EventMessage, EventMessage] | None] = ContextVar(
    "edax_opened_event", default=None
)


@contextmanager
def opened_event(raw: EventMessage, view: EventMessage) -> Iterator[None]:
    """Bind the exact internal view to its authenticated raw envelope for nested SDK consumers."""
    token = _opened_event.set((raw, view))
    try:
        yield
    finally:
        _opened_event.reset(token)


def original_event(view: EventMessage) -> EventMessage:
    """Return the raw envelope only for the exact view currently passed by the SDK."""
    pair = _opened_event.get()
    return pair[0] if pair is not None and pair[1] is view else view


_authenticated_event: ContextVar[tuple[Sealing, bytes, bytes] | None] = ContextVar(
    "edax_authenticated_event", default=None
)


@contextmanager
def authenticated_event(sealing: Sealing, raw: EventMessage, data: bytes) -> Iterator[None]:
    token = _authenticated_event.set((sealing, bytes(_native.encode_event("json", raw)), data))
    try:
        yield
    finally:
        _authenticated_event.reset(token)


def cached_event(raw: EventMessage, sealing: Sealing | None = None) -> bytes | None:
    cached = _authenticated_event.get()
    if cached is None or (sealing is not None and cached[0] is not sealing):
        return None
    if bytes(_native.encode_event("json", raw)) != cached[1]:
        raise CodecError("middleware changed an authenticated event envelope")
    return cached[2]
