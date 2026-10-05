//! Scoped keys: one key namespaced by a scope, so two scopes never spell the same key, and the
//! [`Tenancy`] rule that decides when a tenant is one of those scopes.

use crate::Error;

/// The longest tenant id, in bytes: enough for a readable name or a UUID, short enough that a
/// composed key stays inside the key-length limit of any store a host is likely to use.
pub const TENANT_MAX: usize = 64;

/// `key` under `scope`: `"{len(scope)}:{scope}:{key}"`.
///
/// The scope's length prefix keeps the result injective: no scope and key pair spells another's,
/// even when either holds a `:`. The length is in bytes, not characters, so every binding counts
/// it the same way.
pub fn scoped(scope: &str, key: &str) -> String {
    format!("{}:{scope}:{key}", scope.len())
}

/// How a deployment separates its tenants. The choice is made once, where a service is built, and
/// every seam of that service then keys the same way.
///
/// Exhaustive on purpose: a host must decide about every mode, so a new one is a compile error in
/// every SDK rather than a wildcard's silent default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tenancy {
    /// One deployment per tenant, sharing nothing. A tenant id is optional, and no key carries one:
    /// the infrastructure already separates what a key would.
    Silo,
    /// One deployment for every tenant, sharing stores and topics. A tenant id is required and every
    /// key carries it, because a bare key would read another tenant's rows.
    Pool,
    /// Shared compute, a store per tenant. A tenant id is required, so a message can be attributed and
    /// routed to its own store, and keys stay bare inside it.
    Bridge,
}

impl Tenancy {
    /// The name a binding selects this mode by: `"silo"`, `"pool"` or `"bridge"`.
    pub fn name(self) -> &'static str {
        match self {
            Tenancy::Silo => "silo",
            Tenancy::Pool => "pool",
            Tenancy::Bridge => "bridge",
        }
    }

    /// The inverse of [`name`](Tenancy::name). Anything else is a `codec` error.
    pub fn from_name(name: &str) -> Result<Tenancy, Error> {
        match name {
            "silo" => Ok(Tenancy::Silo),
            "pool" => Ok(Tenancy::Pool),
            "bridge" => Ok(Tenancy::Bridge),
            other => Err(Error::codec(format_args!("unknown tenancy mode: {other}"))),
        }
    }

    /// Whether this mode requires every message and every keyed call to name a tenant.
    pub fn requires_tenant(self) -> bool {
        matches!(self, Tenancy::Pool | Tenancy::Bridge)
    }

    /// Whether this mode composes the tenant into the keys a seam is called with.
    pub fn composes_keys(self) -> bool {
        matches!(self, Tenancy::Pool)
    }
}

/// Check a tenant id a message carries or a caller passes.
///
/// The charset is not restricted: [`scoped`] is injective over any bytes, so a separator inside a
/// tenant id cannot spell another tenant's key. What is restricted is the empty string, which would
/// read as "no tenant" at one seam and as a tenant named `""` at another, and the length, which
/// bounds every composed key.
pub fn check_tenant(tenant: &str) -> Result<(), Error> {
    if tenant.is_empty() {
        return Err(Error::codec("tenant id is empty: pass none, or pass a name"));
    }
    if tenant.len() > TENANT_MAX {
        return Err(Error::codec(format_args!(
            "tenant id is {} bytes, over the {TENANT_MAX} the key rule allows",
            tenant.len()
        )));
    }
    Ok(())
}

/// Check a tenant id against `mode`: present and valid where the mode requires one, absent or valid
/// where it does not.
pub fn check_tenancy(mode: Tenancy, tenant: Option<&str>) -> Result<(), Error> {
    match tenant {
        Some(tenant) => check_tenant(tenant),
        None if mode.requires_tenant() => {
            Err(Error::codec(format_args!("tenancy {} requires a tenant, and none was given", mode.name())))
        }
        None => Ok(()),
    }
}

/// The key a seam is called with under `mode`: `key` itself, or `key` scoped by its tenant.
///
/// This is the one place the composition happens, so a Python service and a Rust service reading one
/// store spell a tenant's keys identically. It is the rule for every key a seam takes: an aggregate
/// stream and its snapshot, an idempotency claim, a saga correlation, a read-model id.
pub fn tenant_key(mode: Tenancy, tenant: Option<&str>, key: &str) -> Result<String, Error> {
    check_tenancy(mode, tenant)?;
    match tenant {
        Some(tenant) if mode.composes_keys() => Ok(scoped(tenant, key)),
        _ => Ok(key.to_owned()),
    }
}

/// The ledger key an idempotency guard of `kind` (`cmd` or `evt`) claims for `key`:
/// `"edax:{kind}:{len(scope)}:{scope}:{key}"`, then composed with the tenant under `mode` by
/// [`tenant_key`].
///
/// The one rule behind the command guard's `claim_key` and the event guard's `event_idempotency`, so
/// the two never compose in different orders. The kind keeps command claims apart from event claims,
/// and the scope names the guard, so two guards on one ledger never replay each other's outcome. The
/// tenant goes outermost, as it does on every key [`tenant_key`] composes, so all of one tenant's rows
/// in any store share one prefix.
pub fn guard_key(mode: Tenancy, tenant: Option<&str>, kind: &str, scope: &str, key: &str) -> Result<String, Error> {
    tenant_key(mode, tenant, &format!("edax:{kind}:{}", scoped(scope, key)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_is_injective_over_scope_and_key() {
        assert_eq!(scoped("billing", "req-1"), "7:billing:req-1");
        assert_eq!(scoped("é", "k"), "2:é:k", "the length counts bytes");
        assert_ne!(scoped("a:b", "c"), scoped("a", "b:c"));
        assert_ne!(scoped("", "x"), scoped("x", ""));
    }

    #[test]
    fn a_mode_round_trips_through_its_name() {
        for mode in [Tenancy::Silo, Tenancy::Pool, Tenancy::Bridge] {
            assert_eq!(Tenancy::from_name(mode.name()), Ok(mode), "{mode:?}");
        }
        assert!(Tenancy::from_name("shard").is_err(), "an unknown mode is a codec error");
    }

    #[test]
    fn check_tenant_rejects_the_empty_string_and_an_over_long_id() {
        assert!(check_tenant("acme").is_ok());
        assert!(check_tenant(&"a".repeat(TENANT_MAX)).is_ok(), "the limit itself is allowed");
        assert!(check_tenant("").is_err(), "empty would read as no tenant at the next seam");
        assert!(check_tenant(&"a".repeat(TENANT_MAX + 1)).is_err());
        assert!(check_tenant("a:b").is_ok(), "the charset is open: `scoped` is injective anyway");
    }

    #[test]
    fn a_mode_decides_whether_a_tenant_is_required() {
        assert!(check_tenancy(Tenancy::Silo, None).is_ok(), "a silo needs no tenant");
        assert!(check_tenancy(Tenancy::Pool, None).is_err(), "a pool cannot key without one");
        assert!(check_tenancy(Tenancy::Bridge, None).is_err(), "a bridge cannot route without one");
        assert!(check_tenancy(Tenancy::Silo, Some("")).is_err(), "a named tenant is still checked");
    }

    #[test]
    fn only_a_pool_composes_the_key() {
        assert_eq!(tenant_key(Tenancy::Pool, Some("acme"), "order-1"), Ok("4:acme:order-1".to_owned()));
        assert_eq!(tenant_key(Tenancy::Bridge, Some("acme"), "order-1"), Ok("order-1".to_owned()));
        assert_eq!(tenant_key(Tenancy::Silo, None, "order-1"), Ok("order-1".to_owned()));
        assert!(tenant_key(Tenancy::Pool, None, "order-1").is_err(), "a pool refuses a bare key");
    }

    #[test]
    fn a_guard_key_scopes_the_key_then_composes_the_tenant_outermost() {
        assert_eq!(guard_key(Tenancy::Silo, None, "cmd", "api", "req-1"), Ok("edax:cmd:3:api:req-1".to_owned()));
        assert_eq!(guard_key(Tenancy::Pool, Some("acme"), "evt", "api", "e-1"), Ok("4:acme:edax:evt:3:api:e-1".to_owned()));
        assert_eq!(guard_key(Tenancy::Bridge, Some("acme"), "cmd", "api", "req-1"), Ok("edax:cmd:3:api:req-1".to_owned()));
        assert!(guard_key(Tenancy::Pool, None, "cmd", "api", "req-1").is_err(), "a pool refuses a bare claim");
    }

    #[test]
    fn two_tenants_never_share_a_composed_key() {
        let acme = tenant_key(Tenancy::Pool, Some("acme"), "order-1");
        let globex = tenant_key(Tenancy::Pool, Some("globex"), "order-1");
        assert_ne!(acme, globex, "the same aggregate id under two tenants is two keys");
        assert_ne!(
            tenant_key(Tenancy::Pool, Some("a:b"), "c"),
            tenant_key(Tenancy::Pool, Some("a"), "b:c"),
            "a separator inside a tenant id spells no other tenant's key"
        );
    }
}
