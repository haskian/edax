"""How a retry waits: the kernel's jittered backoff policy, the sleep that waits it out, and the randomness
behind the jitter. :func:`~edax.command.retry` and :class:`~edax.dispatcher.KeyOrderedDispatcher` take a
:class:`Pacing`, so a host can tune the schedule and a test can wait in no wall time. The Rust SDK's
``edax::Pacing`` is the same three parts."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from . import _native
from ._wiring import U64_MAX, wiring_u64

__all__ = [
    "DEFAULT_BACKOFF",
    "Backoff",
    "Pacing",
    "Sleep",
]

#: Blocks the calling task for a number of seconds.
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class Backoff:
    """A backoff policy, in milliseconds: retry ``attempt`` (0-based) waits a random duration in
    ``0 ..= min(base_millis * 2**attempt, cap_millis)``. The schedule is the kernel's, pinned by
    ``testdata/golden/backoff.json``; the default is :data:`DEFAULT_BACKOFF`. A duration that is not an
    ``int`` the kernel can carry (negative, or past ``2**64 - 1``) raises the terminal
    :class:`~edax.errors.BadHandle`."""

    base_millis: int
    cap_millis: int

    def __post_init__(self) -> None:
        # A wiring mistake, so it fails at construction rather than on the first retry.
        wiring_u64(self.base_millis, "Backoff.base_millis")
        wiring_u64(self.cap_millis, "Backoff.cap_millis")

    def delay_millis(self, attempt: int, entropy: int) -> int:
        """The kernel's wait before retry ``attempt`` for the random draw ``entropy``."""
        return _native.backoff_millis(self.base_millis, self.cap_millis, min(attempt, U64_MAX), entropy)


_DEFAULT = _native.default_backoff()
#: The kernel's default policy: 100 ms, doubling, capped at 30 s.
DEFAULT_BACKOFF = Backoff(_DEFAULT.base_millis, _DEFAULT.cap_millis)


def _random_u64() -> int:
    """A fresh draw for one wait's jitter. Jitter needs spread, not cryptography."""
    return random.getrandbits(64)


@dataclass(frozen=True)
class Pacing:
    """The schedule a retry waits on, the sleep that waits it out, and the random draw behind each wait's
    jitter. The default is the kernel's policy, ``asyncio.sleep`` and a fresh draw per wait. Without the
    jitter, every consumer that hit one downed dependency would retry in lockstep."""

    backoff: Backoff = DEFAULT_BACKOFF
    sleep: Sleep = field(default=asyncio.sleep)
    entropy: Callable[[], int] = field(default=_random_u64)

    def delay(self, attempt: int) -> float:
        """The wait before retry ``attempt`` (0-based), in seconds, from a fresh draw."""
        return self.backoff.delay_millis(attempt, self.entropy()) / 1000

    async def wait(self, attempt: int) -> None:
        """Wait out :meth:`delay` on this pacing's sleep."""
        await self.sleep(self.delay(attempt))
