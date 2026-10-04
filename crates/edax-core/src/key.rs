//! Scoped keys: one key namespaced by a scope, so two scopes never spell the same key.

/// `key` under `scope`: `"{len(scope)}:{scope}:{key}"`.
///
/// The scope's length prefix keeps the result injective: no scope and key pair spells another's,
/// even when either holds a `:`. The length is in bytes, not characters, so every binding counts
/// it the same way.
pub fn scoped(scope: &str, key: &str) -> String {
    format!("{}:{scope}:{key}", scope.len())
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
}
