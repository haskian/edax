"""Value semantics for the native message classes, which the binding emits with none: the field list of
each class, read off its constructor, equality and hashing by value, a repr, sorted dictionaries and
copy/pickle reducers. :mod:`edax.message` applies them once at import; the other modules copy a message with
:func:`replace`."""

from __future__ import annotations

import copyreg
import inspect
from typing import Any, TypeVar


def _sorted_dict(getter: Any) -> property:
    """A property returning the native getter's dictionary sorted by key, so its order is the same in every
    process: by code point, which is the order of the kernel's UTF-8 keys on the wire."""

    def read(self: Any) -> dict[str, str]:
        return dict(sorted(getter.__get__(self, type(self)).items()))

    return property(read, doc=getter.__doc__)


def _value(fields: tuple[str, ...], obj: Any) -> tuple[Any, ...]:
    """``obj`` as the tuple of its fields: what equality and the hash compare."""
    out: list[Any] = []
    for name in fields:
        v = getattr(obj, name)
        if isinstance(v, dict):
            v = tuple(v.items())  # already sorted by the property above
        out.append(v)
    return tuple(out)


def rebuild(cls: type, fields: dict[str, Any]) -> Any:
    """Build ``cls`` from its fields by keyword. The binding orders a constructor's optional parameters
    last, so a positional rebuild would hand a field to the wrong parameter."""
    return cls(**fields)


# Each value class's fields, by class: what `replace` copies and what the pickle reducer rebuilds from.
FIELDS: dict[type, tuple[str, ...]] = {}

M = TypeVar("M")


def fields_of(obj: Any) -> dict[str, Any]:
    """Every field of a value class instance, by name."""
    return {name: getattr(obj, name) for name in FIELDS[type(obj)]}


def replace(msg: M, **changes: Any) -> M:
    """``msg`` with ``changes`` applied and every other field kept. A name that is not a field of the class
    raises :class:`TypeError`, as its constructor does."""
    rebuilt: M = rebuild(type(msg), fields_of(msg) | changes)
    return rebuilt


def give_value_semantics(cls: Any) -> None:
    """Give ``cls`` value semantics over its fields, read off its constructor's parameters, so a field the
    binding adds joins equality, the hash and every copy with no second edit here."""
    fields = FIELDS[cls] = tuple(inspect.signature(cls).parameters)
    for name in ("extensions", "metadata"):
        if name in fields:
            setattr(cls, name, _sorted_dict(cls.__dict__[name]))

    def eq(self: Any, other: object) -> object:  # a bool, or NotImplemented for another type
        if type(other) is not type(self):
            return NotImplemented
        same: bool = _value(fields, self) == _value(fields, other)
        return same

    def hash_(self: Any) -> int:
        return hash(_value(fields, self))

    def repr_(self: Any) -> str:
        shown = ", ".join(f"{name}={getattr(self, name)!r}" for name in fields)
        return f"{cls.__name__}({shown})"

    cls.__eq__ = eq
    cls.__hash__ = hash_
    cls.__repr__ = repr_
    cls.__module__ = "edax"
    copyreg.dispatch_table[cls] = lambda v: (rebuild, (cls, fields_of(v)))
