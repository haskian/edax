"""The one guard every host telemetry hook runs behind, so a broken exporter never fails the work it
measures."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("edax.observe")


def safely(call: Callable[..., object], *args: Any) -> None:
    """Call a host telemetry hook; log and swallow whatever it raises. The one guard behind every
    observer call and the :func:`~edax.observe.timing` recorder, so a broken exporter never fails the work it
    measures. A hook is synchronous: an ``async def`` one returns a coroutine nobody awaits, so its metric
    would be dropped with only a ``RuntimeWarning``. That coroutine is closed and logged as an error."""
    try:
        result: object = call(*args)
    except Exception:
        logger.exception("edax observe: %s raised; the failure is ignored", _name(call))
        return
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        logger.error(
            "edax observe: %s returned an awaitable, which is never awaited: observer hooks are synchronous; "
            "hand the value to your own task or queue instead",
            _name(call),
        )


def _name(call: Callable[..., object]) -> str:
    """The hook's name for a log line. Read only on a failure: a ``repr`` runs host code."""
    return getattr(call, "__qualname__", None) or repr(call)
