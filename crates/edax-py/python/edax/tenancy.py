"""How a deployment separates its tenants, and the keys that follow from it.

The mode is chosen once, where a service is built, and every seam of that service then keys the same
way. The rule itself lives in the kernel (``testdata/golden/tenancy.json`` pins it), so a Python
service and a service in another binding spell one tenant's keys identically.

Which model a mode belongs to is a deployment decision, and edax takes none of it: see
``docs/guides/multi-tenancy.md``.
"""

from __future__ import annotations

from enum import Enum
from typing import Final

from . import _names, _native

__all__ = ["TENANT_MAX", "Tenancy", "check_tenant", "tenant_key"]

#: The longest tenant id the kernel's key rule allows, in bytes, read from the kernel.
TENANT_MAX: Final[int] = _native.tenant_max()


class Tenancy(str, Enum):
    """The three ways a deployment separates tenants. The value is the name the kernel reads, so a
    mode crosses a configuration file or an environment variable as itself."""

    #: One deployment per tenant, sharing nothing. A tenant is optional and no key carries one.
    SILO = "silo"
    #: One deployment for every tenant, sharing stores and topics. A tenant is required and every
    #: key carries it, because a bare key would read another tenant's rows.
    POOL = "pool"
    #: Shared compute, a store per tenant. A tenant is required, so a message can be attributed and
    #: routed to its own store, and keys stay bare inside it.
    BRIDGE = "bridge"

    @property
    def requires_tenant(self) -> bool:
        """Whether this mode requires every message and every keyed call to name a tenant."""
        return bool(_native.tenancy_rules(self.value).requires_tenant)

    @property
    def composes_keys(self) -> bool:
        """Whether this mode composes the tenant into the keys a seam is called with."""
        return bool(_native.tenancy_rules(self.value).composes_keys)


_names.check((mode.value for mode in Tenancy), _native.tenancy_modes(), "tenancy modes")


def check_tenant(tenant: str) -> None:
    """Raise :class:`~edax.errors.CodecError` if ``tenant`` breaks the kernel's rule: 1 to
    :data:`TENANT_MAX` bytes. The charset is open, because :func:`tenant_key` is injective over any
    bytes, so a separator inside a tenant id spells no other tenant's key."""
    _native.check_tenant(tenant)


def tenant_key(mode: Tenancy, tenant: str | None, key: str) -> str:
    """The key a seam is called with under ``mode``: ``key`` itself, or ``key`` composed with its
    ``tenant``. The arguments run as in the Rust SDK's ``tenant_key(mode, tenant, key)``.

    A mode that requires a tenant and a call that names none raises
    :class:`~edax.errors.CodecError`, so a pool can never read an uncomposed key by omission.
    """
    return _native.tenant_key(mode.value, key, tenant)
