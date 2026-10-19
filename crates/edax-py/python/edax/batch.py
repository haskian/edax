"""Microbatch limits shared by every Kafka runner: the event kernel's batch policy
(``edax_event::batch``), validated by the kernel when built."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from . import _native, errors
from ._json import decode, encode

__all__ = ["BatchSettings", "recover"]

_DEFAULTS: dict[str, int] = decode(_native.batch_config("{}"))


@dataclass(frozen=True)
class BatchSettings:
    """Validated runner limits. Defaults and constraints come from the event kernel.

    Retained bytes measure serialized data; broker prefetch, Python object overhead, and memory
    privately allocated by a mapper are outside that bound.
    """

    max_records: int = _DEFAULTS["max_records"]
    max_bytes: int = _DEFAULTS["max_bytes"]
    flush_ms: int = _DEFAULTS["flush_ms"]
    concurrency: int = _DEFAULTS["concurrency"]
    retained_records: int = _DEFAULTS["retained_records"]
    retained_bytes: int = _DEFAULTS["retained_bytes"]
    max_record_bytes: int = _DEFAULTS["max_record_bytes"]
    max_mapped_bytes: int = _DEFAULTS["max_mapped_bytes"]
    max_attempts: int = _DEFAULTS["max_attempts"]

    def __post_init__(self) -> None:
        _native.batch_config(self.json)

    @property
    def json(self) -> str:
        """The native policy configuration shared by admission, batching, and recovery."""
        return encode(self)


def recover(
    error: BaseException, settings: BatchSettings, *, attempt: int, count: int, isolable: bool, waits: int = 0
) -> dict[str, Any]:
    """The kernel's recovery after ``error`` failed an attempt of ``count`` originals: an ``action`` of
    ``stop``, ``retry``, ``wait``, ``split`` or ``reject``. Only a :class:`~edax.errors.TransactionFailure`
    that rolled back before its commit started proves nothing was written; any other failure may have."""
    cause, rolled_back = error, False
    if isinstance(error, errors.TransactionFailure):
        cause, rolled_back = error.cause, error.rolled_back and not error.commit_started
    return cast(
        dict[str, Any],
        decode(
            _native.batch_recover(
                attempt, waits, count, settings.max_attempts, rolled_back, isolable, errors.code_of(cause)
            )
        ),
    )
