"""Structured error taxonomy over the kernel's ``ErrorCode``. The native layer reports failures as strings
with a stable code prefix (``"codec: ..."``); :func:`from_wire` maps the code to an exception class.
:func:`is_terminal` says whether an error fails the same way on retry, from the code it carries. The retry
middleware, the idempotency guard and the dispatcher decide retry-vs-dead-letter on that one verdict, and the
guard persists on it: a terminal failure is cached
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
    ``terminal`` is the kernel's verdict for that code, read once at class definition; a subclass cannot
    restate it."""

    code: ClassVar[str | None] = None
    terminal: ClassVar[bool] = False

    def __init_subclass__(cls, code: str | None = None, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if "terminal" in cls.__dict__:
            # Every consumer asks the kernel by code, so a restated verdict would be silently ignored.
            raise TypeError(
                f"{cls.__name__} restates `terminal`: an edax error's verdict is its code's. "
                "Subclass Rejected for a terminal failure, or raise your own exception for a transient one."
            )
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
    """Fail the import if the kernel knows a code this module does not, so :func:`from_wire` finds a class
    for every code the kernel names."""
    unmapped = sorted(set(_edax.error_codes()) - set(_BY_CODE))
    if unmapped:
        raise ImportError(f"kernel error codes with no edax.errors class: {unmapped}")


_check_every_kernel_code_has_a_class()


#: The kernel's verdict for a failure no code classifies — a host's own exception. Read from the kernel,
#: never chosen here, so every SDK sharing a store agrees on it.
UNCLASSIFIED_IS_TERMINAL: bool = _edax.error_verdict("")


def from_wire(wire: str) -> EdaxError:
    """Map a kernel error's wire form ``"<code>: <message>"`` to the matching :class:`EdaxError`. The
    kernel reads an unknown or absent code as a :class:`StoreError` carrying the whole string, code
    included: a wrongly transient error is redelivered loudly and the work survives, while a wrongly
    terminal one is dead-lettered and cached under its key, poisoning it. See ``docs/sdk/errors.md``."""
    split = _edax.split_error(wire)
    return _BY_CODE[split.code](split.message)


def _carried_code(e: BaseException) -> str | None:
    """The taxonomy code failure ``e`` carries: an edax error's own code, ``rejected`` for any other exception
    that declares itself terminal (a host's business rejection, whatever its own ``code`` attribute holds),
    and ``None`` otherwise. Every kernel decision about a failure starts from this code, as the Rust SDK's
    ``code_of`` does."""
    if isinstance(e, EdaxError):
        return e.code if e.code in _BY_CODE else None
    return Rejected.code if bool(getattr(e, "terminal", False)) else None


def is_terminal(e: BaseException) -> bool:
    """Whether ``e`` fails the same way on retry: the kernel's verdict for the code ``e`` carries, and the
    kernel's default (transient) for a failure no code classifies. Every consumer of a failure branches on
    this one function or on the same code, as the Rust SDK's ``command::is_terminal`` does."""
    code = _carried_code(e)
    return UNCLASSIFIED_IS_TERMINAL if code is None else _edax.error_code_is_terminal(code)


def code_of(e: BaseException) -> str | None:
    """The taxonomy code a won idempotency claim caches failure ``e`` under, or ``None`` when the claim is
    released instead. The kernel's settle table (``idempotency_settle``) decides from the code ``e`` carries
    (its own, ``rejected`` for a host exception that declares itself terminal, else none), so every SDK
    sharing a ledger caches the same failures. Pinned by ``testdata/golden/idempotency_settle.json``."""
    return _edax.idempotency_settle(_carried_code(e))


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
        raise from_wire(str(e)) from e
    except _ARGUMENT_ERRORS as e:
        raise CodecError(f"argument could not cross into the kernel: {e}") from e
