"""The closed sets of names the kernel decides with: a saga step's write and admit, an idempotency record's
state, a lost claim's verdict, a tenancy mode. The kernel owns each list. The SDK spells each as a
``Literal``, so a type checker sees every branch, and checks that ``Literal`` against the kernel's list at
import. A name the kernel adds then fails the import instead of falling into a neighbouring branch."""

from __future__ import annotations

from collections.abc import Iterable
from typing import NoReturn, TypeVar

from . import errors

Name = TypeVar("Name", bound=str)
Field = TypeVar("Field")


def check(names: Iterable[str], kernel: Iterable[str], what: str) -> None:
    """Fail the import unless ``names``, the SDK's ``Literal``, is exactly the kernel's list of ``what``."""
    ours, theirs = sorted(names), sorted(kernel)
    if ours != theirs:
        raise ImportError(f"{what}: edax names {ours}, the kernel names {theirs}")


def named(value: str, names: tuple[Name, ...], what: str) -> Name:
    """``value`` as one of ``names``, typed as their ``Literal``. A name outside them raises the terminal
    :class:`~edax.errors.CodecError`: a native module ahead of this SDK, which the import check rules out
    unless the module was swapped after it."""
    for name in names:
        if name == value:
            return name
    raise errors.CodecError(f"unknown {what} {value!r}")


def given(value: Field | None, what: str) -> Field:
    """``value``, a field the kernel sets for the decision it just named, typed without its ``None``. ``None``
    raises the terminal :class:`~edax.errors.CodecError`, as :func:`named` does: a native module out of step
    with this SDK."""
    if value is None:
        raise errors.CodecError(f"the kernel's answer carries no {what}")
    return value


def unreachable(value: NoReturn) -> NoReturn:
    """The branch after every name of a ``Literal`` matched: a type checker proves it dead, so a name added
    to the ``Literal`` and left unhandled is a type error. ``typing.assert_never`` needs Python 3.11."""
    raise AssertionError(f"unhandled name {value!r}")
