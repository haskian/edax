"""Structured error taxonomy over the kernel's ``ErrorCode``. The native layer reports failures as strings
with a stable code prefix (``"codec: ..."``); :func:`from_runtime` maps the code to an exception class.
``terminal`` marks an error that fails the same way on retry. The retry middleware, the idempotency guard and
the dispatcher decide retry-vs-dead-letter on it, and the guard persists on it: a terminal failure is cached
under the idempotency key, a transient one releases the claim. The verdict is the kernel's, pinned by
``testdata/golden/error_taxonomy.json``, so every SDK sharing a store agrees. These classes cover the whole
code set, one per code, checked both ways at import. See ``docs/sdk/errors.md``."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import ClassVar

from . import _edax


class EdaxError(Exception):
    """Base for every edax error. ``code`` is the kernel's wire name for the kind, ``None`` here.
    ``terminal`` is the kernel's verdict for that code, and the kernel's default for a failure with no
    code."""

    code: ClassVar[str | None] = None
    terminal: ClassVar[bool] = False

    def __init_subclass__(cls, code: str | None = None, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if code is None:
            return  # a host subclass inherits its parent's code and verdict
        cls.code = code
        # Raises for a code the kernel does not know, so the import fails on an unpinned verdict.
        cls.terminal = _edax.error_code_is_terminal(code)
        _BY_CODE[code] = cls


#: Every kernel code, mapped to its exception class, checked against the kernel's code list right after.
_BY_CODE: dict[str, type[EdaxError]] = {}


class ConcurrencyError(EdaxError, code="concurrency"):
    """The expected version did not match: a concurrent writer got there first. Reload and retry."""


class StoreError(EdaxError, code="store"):
    """The backing store failed for a reason other than a concurrency conflict."""


class CodecError(EdaxError, code="codec"):
    """A value could not be marshaled or unmarshaled. Deterministic."""


class PayloadMismatch(EdaxError, code="payload_mismatch"):
    """An idempotency key was reused with a different command payload."""


class InProgress(EdaxError, code="in_progress"):
    """The idempotency key is claimed by a dispatch that has not completed. Retry so it can replay."""


class CachedFailure(EdaxError, code="cached_failure"):
    """Replay of a terminal failure cached under this idempotency key, so a redelivery dead-letters rather
    than retries. The message is the original failure's wire form (``"codec: bad payload"``), and
    ``original_code`` its taxonomy code (``None`` for a record stored without one); the original exception
    type does not survive."""

    def __init__(self, message: str = "", original_code: str | None = None) -> None:
        super().__init__(message)
        self.original_code = original_code


class HandlerNotFound(EdaxError, code="handler_not_found"):
    """No handler is registered for the command type. A redelivery finds the same registry: dead-letter."""


class AlreadyRegistered(EdaxError, code="already_registered"):
    """A name is already taken: a handler for a command type, or a serializer for a media type. A wiring
    mistake at startup."""


class BadHandle(EdaxError, code="bad_handle"):
    """A wiring mistake: a transaction handle that names no open transaction (never begun, already settled,
    or from another store), a store setting the store refuses, or a command the server refuses. Build every
    store that must commit together over one transactor. Terminal: it always fails alike."""


class Rejected(EdaxError, code="rejected"):
    """A host's own business rejection ("insufficient funds"): the same command is rejected the same way on
    every retry, so the guard caches it and a dispatcher dead-letters it. Raise it, or subclass it, from a
    handler. A host exception that declares ``terminal = True`` itself is cached under this code too."""


def _check_every_kernel_code_has_a_class() -> None:
    """Fail the import if the kernel knows a code this module does not. Without it a new kernel code would
    reach :func:`from_runtime`'s unknown-code default and be classified transient with no signal."""
    unmapped = sorted(set(_edax.error_codes()) - set(_BY_CODE))
    if unmapped:
        raise ImportError(f"kernel error codes with no edax.errors class: {unmapped}")


_check_every_kernel_code_has_a_class()


#: The kernel's verdict for a failure no code classifies — a host's own exception, a wire string with an
#: unknown code. Read from the kernel, never chosen here, so every SDK sharing a store agrees on it.
UNCLASSIFIED_IS_TERMINAL: bool = _edax.error_verdict("")


def _check_the_unclassified_class_carries_the_kernels_default() -> None:
    """:func:`from_runtime` maps an unknown code to :class:`StoreError`, so that class's verdict must be the
    kernel's default for an unclassified failure. Fail the import if the kernel ever changes it."""
    if StoreError.terminal != UNCLASSIFIED_IS_TERMINAL:
        raise ImportError(
            "edax.errors maps an unknown code to StoreError, whose verdict is not the kernel's default"
        )


_check_the_unclassified_class_carries_the_kernels_default()


def from_runtime(e: Exception) -> EdaxError:
    """Map a native ``"<code>: <message>"`` error to the matching :class:`EdaxError`. An unknown or absent
    code becomes a :class:`StoreError` carrying the whole message, code included: the kernel's verdict for
    an unclassified failure is transient (a wrongly transient error is redelivered loudly and the work
    survives, while a wrongly terminal one is dead-lettered and cached under its key, poisoning it). See
    ``docs/sdk/errors.md``."""
    return from_wire(str(e))


def from_wire(wire: str) -> EdaxError:
    """Map a kernel error's wire form ``"<code>: <message>"`` to the matching :class:`EdaxError`, with the
    same unknown-code default as :func:`from_runtime`."""
    # An unknown code comes back with an empty code and the whole string as the message.
    split = _edax.split_error(wire)
    cls = _BY_CODE.get(split.code)
    return cls(split.message) if cls else StoreError(split.message)


def is_terminal(e: BaseException) -> bool:
    """Whether ``e`` fails the same way on retry: its ``terminal`` attribute, or the kernel's default for a
    failure with no classification (transient)."""
    return bool(getattr(e, "terminal", UNCLASSIFIED_IS_TERMINAL))


def code_of(e: BaseException) -> str | None:
    """The taxonomy code a won idempotency claim caches failure ``e`` under, or ``None`` when the claim is
    released instead. ``e`` carries an edax error's own code, ``rejected`` for any other exception that
    declares itself terminal (a host's business rejection, whatever its own ``code`` attribute holds), and
    no code otherwise; the kernel's settle table (``idempotency_settle``) decides from that code, so every
    SDK sharing a ledger caches the same failures. Pinned by ``testdata/golden/idempotency_settle.json``."""
    if isinstance(e, EdaxError) and e.code in _BY_CODE:
        carried: str | None = e.code
    elif is_terminal(e):
        carried = Rejected.code
    else:
        carried = None
    return _edax.idempotency_settle(carried)


#: What a native call raises when an argument does not convert: a negative or too-wide integer, a value of the
#: wrong type, a ``str`` holding a lone surrogate. Each fails the same way on every retry.
_ARGUMENT_ERRORS = (OverflowError, TypeError, UnicodeError)


@contextmanager
def mapped() -> Iterator[None]:
    """Re-raise a failed native call as its typed :class:`EdaxError`: a ``RuntimeError`` by its kernel code,
    and an argument the binding could not convert as :class:`CodecError`. Without it a ``codec:``-tagged
    failure, or a value no retry can convert, reads as transient and is redelivered forever instead of
    dead-lettered. Wrap the native call only: a ``TypeError`` from host code inside would be retyped too."""
    try:
        yield
    except RuntimeError as e:
        raise from_runtime(e) from e
    except _ARGUMENT_ERRORS as e:
        raise CodecError(f"argument could not cross into the kernel: {e}") from e
