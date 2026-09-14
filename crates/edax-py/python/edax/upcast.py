"""Versioned-event upcasting: register single-step transforms, apply them in kernel-planned order. The
transforms are domain code; the kernel decides which steps run, in what order, and rejects a gap, so every
service raises a stored payload to the target version the same way."""

from __future__ import annotations

from typing import Callable

from . import _edax, errors

Upcast = Callable[[bytes], bytes]


class Upcaster:
    """Registry of ``version -> version+1`` transforms for one event type."""

    def __init__(self) -> None:
        self._steps: dict[int, Upcast] = {}

    def step(self, version: int, fn: Upcast) -> Upcaster:
        """Register the transform that upgrades ``version`` to ``version + 1``. Chainable."""
        self._steps[version] = fn
        return self

    def to(self, data: bytes, *, from_: int, to: int) -> bytes:
        """Raise ``data`` from version ``from_`` to ``to``, applying steps in kernel-planned order. Raises
        :class:`~edax.errors.CodecError` on a missing step or when ``from_ > to``; both are deterministic,
        so a redelivery must dead-letter rather than retry."""
        with errors.mapped():
            plan = _edax.upcast_plan(from_, to, list(self._steps))
        for v in plan:
            data = self._steps[v](data)
        return data
