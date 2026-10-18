"""Microbatch limits shared by every Kafka runner: the event kernel's batch policy
(``edax_event::batch``), validated by the kernel when built."""

from __future__ import annotations

from dataclasses import dataclass

from . import _native
from ._json import decode, encode

__all__ = ["BatchSettings"]

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
