"""Versioned-event upcasting: register single-step transforms, apply them in kernel-planned order. The
transforms are domain code; the kernel decides which steps run, in what order, and rejects a gap or a
payload from a newer writer, so every service raises a stored payload to the target version the same way."""

from __future__ import annotations

from collections.abc import Callable

from . import _native
from ._wiring import wiring_u32
from .errors import AlreadyRegistered

__all__ = [
    "UpcastStep",
    "Upcaster",
    "target",
    "upcast",
]

#: One step of an :class:`Upcaster`: the payload at one version in, the payload at the next out. The Rust
#: SDK's ``UpcastStep``.
UpcastStep = Callable[[bytes], bytes]


class Upcaster:
    """Registry of ``version -> version+1`` transforms for one event type. The type's schema version is
    one past its highest step: to bump a type whose shape did not change, register an identity step."""

    def __init__(self) -> None:
        self._steps: dict[int, UpcastStep] = {}

    def step(self, version: int, fn: UpcastStep) -> Upcaster:
        """Register the transform that upgrades ``version`` to ``version + 1``. Chainable. A second
        transform for one version raises :class:`~edax.errors.AlreadyRegistered`: keeping either one
        silently would run a migration nobody chose. A ``version`` that is not a schema version (negative,
        too large, not an ``int``) raises the terminal :class:`~edax.errors.BadHandle`."""
        wiring_u32(version, "upcast step version")
        if version in self._steps:
            raise AlreadyRegistered(f"upcast step {version} is registered twice")
        self._steps[version] = fn
        return self

    def to(self, data: bytes, *, from_: int, to: int) -> bytes:
        """Raise ``data`` from version ``from_`` to ``to``, applying steps in kernel-planned order. Raises
        :class:`~edax.errors.CodecError` on a missing step or when ``from_ > to``; both are deterministic,
        so a redelivery must dead-letter rather than retry."""
        return upcast(self, data, from_=from_, to=to)


def target(upcaster: Upcaster | None) -> int:
    """The schema version an event name with ``upcaster`` writes and raises older payloads to: one past
    its highest step, or the first version (1) when it has none. A hole in the steps raises
    :class:`~edax.errors.CodecError`, since a payload stored at the missing version could never be
    raised. Pinned by ``testdata/golden/upcast.json``."""
    return _native.upcast_target(None if upcaster is None else list(upcaster._steps))


def upcast(upcaster: Upcaster | None, data: bytes, *, from_: int, to: int) -> bytes:
    """Raise ``data`` from ``from_`` to ``to`` through ``upcaster``, or as it is when the event name has no
    upcaster. The kernel is asked either way, so a payload from a newer writer (``from_ > to``) raises
    :class:`~edax.errors.CodecError` instead of decoding silently under the older schema. Pinned by
    ``testdata/golden/upcast.json``."""
    steps = None if upcaster is None else upcaster._steps
    plan = _native.upcast_plan(from_, to, None if steps is None else list(steps))
    for v in plan:
        # A plan names only steps it was given, so `steps` is set whenever the plan is not empty.
        data = (steps or {})[v](data)
    return data
