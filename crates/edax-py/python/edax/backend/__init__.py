"""Concrete backends for the SDK seams. :mod:`edax.backend.memory` ships in the base package for dev and
tests; :mod:`edax.backend.redis` and :mod:`edax.backend.kafka` install via extras and wrap a caller-owned
client. Every seam's contract is the ``Protocol`` docstring in the module that defines it."""
