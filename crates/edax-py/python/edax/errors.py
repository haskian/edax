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
    ``terminal`` is the kernel's verdict for that code, defaulting to transient for anything with no code."""

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
    than retries. Only the message survives, not the original exception type."""


class HandlerNotFound(EdaxError, code="handler_not_found"):
    """No handler is registered for the command type. A redelivery finds the same registry: dead-letter."""


class AlreadyRegistered(EdaxError, code="already_registered"):
    """A handler is already registered for this command type: a wiring mistake at startup."""


class BadHandle(EdaxError, code="bad_handle"):
    """A transaction handle names no open transaction: never begun, already settled, or from another store.
    Build every store that must commit together over one transactor. Terminal: it always fails alike."""


def _check_every_kernel_code_has_a_class() -> None:
    """Fail the import if the kernel knows a code this module does not. Without it a new kernel code would
    reach :func:`from_runtime`'s unknown-code default and be classified transient with no signal."""
    unmapped = sorted(set(_edax.error_codes()) - set(_BY_CODE))
    if unmapped:
        raise ImportError(f"kernel error codes with no edax.errors class: {unmapped}")


_check_every_kernel_code_has_a_class()


def from_runtime(e: Exception) -> EdaxError:
    """Map a native ``"<code>: <message>"`` error to the matching :class:`EdaxError`. An unknown or absent
    code becomes a :class:`StoreError` carrying the whole message, code included. Transient is the safe
    default: a wrongly transient error is redelivered loudly and the work survives, while a wrongly terminal
    one is dead-lettered and cached under its key, poisoning it. See ``docs/sdk/errors.md``."""
    # An unknown code comes back with an empty code and the whole string as the message: transient below.
    split = _edax.split_error(str(e))
    cls = _BY_CODE.get(split.code)
    return cls(split.message) if cls else StoreError(split.message)


def is_terminal(e: BaseException) -> bool:
    """Whether ``e`` fails the same way on retry. A host error has no ``terminal`` attribute: transient."""
    return bool(getattr(e, "terminal", False))


@contextmanager
def mapped() -> Iterator[None]:
    """Re-raise a native ``RuntimeError`` as its typed :class:`EdaxError`. Without it a ``codec:``-tagged
    failure reads as transient and is redelivered forever instead of dead-lettered."""
    try:
        yield
    except RuntimeError as e:
        raise from_runtime(e) from e
