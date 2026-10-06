"""The native kernel, with every failure mapped to :mod:`edax.errors` once, here, rather than at each call
site. A raw ``_edax`` call raises a bare ``RuntimeError`` that :func:`~edax.errors.is_terminal` reads as
transient, so a payload that can never hash would be redelivered forever. The SDK reaches the kernel through
this module: each function it hands out runs under :func:`~edax.errors.mapped`, and :func:`handle` wraps a
native object so each of its method calls does too. Type checkers see ``_edax``'s own stub, so every
signature stays typed. A native class passes through unwrapped: its constructor is not a kernel decision,
and wrapping it would break ``isinstance``.

The wiring checks in :mod:`edax._wiring` call ``_edax`` straight, since they map a refused argument to
:class:`~edax.errors.BadHandle` rather than :class:`~edax.errors.CodecError`. So does :mod:`edax.errors`
itself, which this module imports."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from . import _edax, errors

T = TypeVar("T")


def _mapping(native: Callable[..., Any]) -> Callable[..., Any]:
    def call(*args: Any, **kwargs: Any) -> Any:
        with errors.mapped():
            return native(*args, **kwargs)

    return call


class _Mapped:
    """A native object whose every method call runs under :func:`~edax.errors.mapped`."""

    __slots__ = ("_native",)

    def __init__(self, native: object) -> None:
        self._native = native

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._native, name)
        return _mapping(attribute) if callable(attribute) else attribute


def handle(native: T) -> T:
    """``native``, a handle ``_edax`` built, with each method call's failure mapped as this module maps a
    function's. Typed as the handle itself, so the stub still checks every call."""
    return cast(T, _Mapped(native))


if TYPE_CHECKING:
    from ._edax import *  # noqa: F403
else:

    def __getattr__(name: str) -> Any:
        value = getattr(_edax, name)
        if isinstance(value, type) or not callable(value):
            return value
        mapped = globals()[name] = _mapping(value)  # built once: the next lookup skips this hook
        return mapped
