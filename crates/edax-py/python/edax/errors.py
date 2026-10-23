"""Structured error taxonomy over the kernel's ``ErrorCode``. The native layer reports failures as strings
with a stable code prefix (``"codec: ..."``); :func:`from_wire` maps the code to an exception class.
:func:`is_terminal` says whether an error fails the same way on retry, from the code it carries, and
:func:`is_fatal` whether it is a wiring fault that fails every message alike. The retry
middleware, the idempotency guard and the dispatcher decide retry-vs-dead-letter on that one verdict, and the
guard persists on it: a terminal failure is cached
under the idempotency key, a transient one releases the claim. The verdict is the kernel's, pinned by
``testdata/golden/error_taxonomy.json``, so every SDK sharing a store agrees. These classes cover the whole
code set, one per code, checked both ways at import. See ``docs/sdk/errors.md``."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, ClassVar, Final, Literal, cast, get_args

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
        if code in _BY_CODE:
            # A second class for a code would replace the kernel's class in `from_wire` process-wide, and one
            # with its own `__init__` signature would turn every kernel failure of that code into a transient
            # `TypeError`.
            raise TypeError(
                f"{cls.__name__} re-registers code {code!r}, which {_BY_CODE[code].__name__} owns; "
                "subclass it without code="
            )
        cls.code = code
        # Raises for a code the kernel does not know, so the import fails on an unpinned verdict.
        cls.terminal = _edax.error_verdict(code)
        _BY_CODE[code] = cls


#: Every kernel code, mapped to its exception class, checked against the kernel's code list right after.
_BY_CODE: dict[str, type[EdaxError]] = {}


class ConcurrencyError(EdaxError, code="concurrency"):
    """The expected version did not match: a concurrent writer got there first. Reload and retry."""


class StoreError(EdaxError, code="store"):
    """The backing store failed for a reason other than a concurrency conflict."""


class TransactionFailure(StoreError):
    """An owned transaction's cause and recovery evidence.

    A confirmed rollback proves no write committed only when ``commit_started`` is false.
    Once commit starts, losing its reply leaves the outcome unknown even if rollback succeeds.
    Runners classify ``cause`` using the kernel's error taxonomy.
    """

    def __init__(self, cause: Exception, *, rolled_back: bool, commit_started: bool) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.rolled_back = rolled_back
        self.commit_started = commit_started


class CodecError(EdaxError, code="codec"):
    """A value could not be marshaled or unmarshaled. Deterministic."""


class PayloadMismatch(EdaxError, code="payload_mismatch"):
    """An idempotency key was reused with a different command payload."""


class InProgress(EdaxError, code="in_progress"):
    """The idempotency key is claimed by a dispatch that has not completed. Retry so it can replay."""


class CachedFailure(EdaxError, code="cached_failure"):
    """Replay of a terminal failure cached under this idempotency key, so a redelivery dead-letters rather
    than retries. The message is the original failure's wire form (``"codec: bad payload"``), and
    ``original_code`` its taxonomy code (``None`` for a record stored without one), and ``original_reason``
    why a :class:`Rejected` refused, when its class named one; the original exception type does not
    survive. :func:`reason_of` reads the reason off this and a live rejection alike."""

    def __init__(
        self, message: str = "", original_code: str | None = None, original_reason: str | None = None
    ) -> None:
        super().__init__(message)
        self.original_code = original_code
        self.original_reason = original_reason


class HandlerNotFound(EdaxError, code="handler_not_found"):
    """No handler is registered for the command type. A redelivery finds the same registry: dead-letter."""


class AlreadyRegistered(EdaxError, code="already_registered"):
    """A name is already taken: a handler for a command type, or a serializer for a media type. A wiring
    mistake at startup."""


class BadHandle(EdaxError, code="bad_handle"):
    """A wiring mistake: a transaction handle that names no open transaction (never begun, already settled,
    or from another store), a store setting the store refuses, a command the server refuses, or a native
    handle used on a thread other than the one that built it. Build every store that must commit together
    over one transactor. Terminal: it always fails alike."""


class Rejected(EdaxError, code="rejected"):
    """A host's own business rejection ("insufficient funds"): the same command is rejected the same way on
    every retry, so the guard caches it and a dispatcher dead-letters it. Raise it, or subclass it, from a
    handler. A host exception that declares ``terminal = True`` itself is cached under this code too.

    A subclass may name why it refuses, ``class NotFound(Rejected, reason="not_found")``, so a replay of
    the cached failure tells it from another rejection: the record keeps the reason beside the code, and
    :func:`reason_of` reads it off the live error and the replayed :class:`CachedFailure` alike. The kernel
    checks the reason when the class is defined: 1 to 64 bytes of ``[a-z0-9._-]``, starting with
    ``[a-z0-9]``, else :class:`CodecError`. Only the class keyword names one; a ``reason`` attribute in the
    class body raises :class:`TypeError`, so a mixed-in library's own ``reason`` never reaches the record."""

    _edax_reason: ClassVar[str | None] = None

    def __init_subclass__(cls, reason: str | None = None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "reason" in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} defines `reason` in its body: name it as a class keyword, "
                f'class {cls.__name__}(Rejected, reason="not_found")'
            )
        if reason is None:
            return  # a subclass inherits its parent's reason
        try:
            _edax.check_rejection_reason(reason)
        except RuntimeError as e:
            raise from_wire(str(e)) from e
        except TypeError as e:
            raise CodecError(f"a rejection reason is text: {e}") from e
        cls._edax_reason = reason


class Shredded(EdaxError, code="shredded"):
    """The key provider confirms permanent destruction of the requested scope."""


class KeyUnavailable(EdaxError, code="key_unavailable"):
    """Provider outage; processing must be deferred without poison-message attempts."""


class KeyRevoking(EdaxError, code="key_revoking"):
    """Scope access is revoked and destruction is pending."""


class KeyMissing(EdaxError, code="key_missing"):
    """A required generation is missing; this is not proof of destruction."""


class KeyUnauthorized(EdaxError, code="key_unauthorized"):
    """Provider credentials or scope configuration do not authorize access."""


class OwnerMismatch(CodecError, code="owner_mismatch"):
    """A sealed stream, snapshot or saga state belongs to another tenant or subject than the call names.
    Deterministic. A :class:`CodecError` subclass, so a host that catches that keeps working. Catch this
    around a load to answer another owner's stream as not found; from a save it means the caller changed
    a committed owner."""


def _check_every_kernel_code_has_a_class() -> None:
    """Fail the import if the kernel knows a code this module does not, so :func:`from_wire` finds a class
    for every code the kernel names."""
    unmapped = sorted(set(_edax.error_codes()) - set(_BY_CODE))
    if unmapped:
        raise ImportError(f"kernel error codes with no edax.errors class: {unmapped}")


_check_every_kernel_code_has_a_class()


#: The kernel's verdict for a failure no code classifies — a host's own exception. Read from the kernel,
#: never chosen here, so every SDK sharing a store agrees on it.
UNCLASSIFIED_IS_TERMINAL: bool = _edax.error_verdict(None)


def from_wire(wire: str) -> EdaxError:
    """Map a kernel error's wire form ``"<code>: <message>"`` to the matching :class:`EdaxError`. The
    kernel reads an unknown or absent code as a :class:`StoreError` carrying the whole string, code
    included: a wrongly transient error is redelivered loudly and the work survives, while a wrongly
    terminal one is dead-lettered and cached under its key, poisoning it. See ``docs/sdk/errors.md``."""
    split = _edax.split_error(wire)
    return _BY_CODE[split.code](split.message)


def code_of(e: BaseException) -> str | None:
    """The taxonomy code failure ``e`` carries: an edax error's own code, ``rejected`` for any other exception
    that declares itself terminal (a host's business rejection, whatever its own ``code`` attribute holds),
    and ``None`` otherwise. A ``BaseException`` that is not an ``Exception`` (a cancel from elsewhere, a
    native panic) carries none: nothing says it repeats. Every kernel decision about a failure starts from
    this code, as the Rust SDK's ``code_of`` does."""
    if not isinstance(e, Exception):
        return None
    if isinstance(e, EdaxError):
        return e.code if e.code in _BY_CODE else None
    return Rejected.code if bool(getattr(e, "terminal", False)) else None


def reason_of(e: BaseException) -> str | None:
    """Why rejection ``e`` refused: the reason its :class:`Rejected` class names, or the one a replayed
    :class:`CachedFailure` carries from the record; ``None`` for any other failure. One read for the live
    rejection and its replay, so a host maps both to one answer (a 404 for ``not_found``)."""
    if isinstance(e, Rejected):
        return type(e)._edax_reason
    if isinstance(e, CachedFailure):
        return e.original_reason
    return None


def is_terminal(e: BaseException) -> bool:
    """Whether ``e`` fails the same way on retry: the kernel's verdict for the code ``e`` carries, and the
    kernel's default (transient) for a failure no code classifies. Every consumer of a failure branches on
    this one function or on the same code, as the Rust SDK does with ``code_of`` then
    ``edax_core::is_terminal``. The verdict is the kernel's, read once per code at import."""
    code = code_of(e)
    return UNCLASSIFIED_IS_TERMINAL if code is None else _BY_CODE[code].terminal


#: The kinds of failure the kernel reads off an error code: a wiring fault that fails every message alike, a
#: failure that repeats, one that may pass, and contention for an idempotency claim. The SDK checks the list
#: against the kernel's at import.
RetryFailure = Literal["fatal", "terminal", "transient", "contended"]


def _kernel_failure_kind(code: str | None) -> RetryFailure:
    kind = _edax.retry_failure(code)
    for known in get_args(RetryFailure):
        if known == kind:
            return cast(RetryFailure, known)
    raise ImportError(f"the kernel reads code {code!r} as failure kind {kind!r}, which edax does not name")


#: The kernel's failure kind for every code, and for no code, read once at import.
_FAILURE_KIND: Final[dict[str | None, RetryFailure]] = {
    code: _kernel_failure_kind(code) for code in [*_BY_CODE, None]
}


def failure_kind(e: BaseException) -> RetryFailure:
    """The kernel's kind of failure ``e`` is, read off the code it carries: ``fatal`` for a wiring fault,
    ``terminal`` for one that repeats, ``contended`` for a held idempotency claim, else ``transient``."""
    return _FAILURE_KIND[code_of(e)]


def is_fatal(e: BaseException) -> bool:
    """Whether ``e`` is a wiring fault (a refused credential, topic or setting, the ``bad_handle`` code) that
    fails every message alike: the kernel's failure kind for the code ``e`` carries. A
    :class:`~edax.outbox.Relay` and a :class:`~edax.dispatcher.KeyOrderedDispatcher` stop on one rather than
    dead-letter every message after it, so a host's own loop around them stops on it too. Every fatal
    failure is terminal."""
    return failure_kind(e) == "fatal"


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
