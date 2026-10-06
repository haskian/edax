//! Settlement tokens: the placeholder value, and the mint that hands out live ones.
//!
//! A delivery settles by token and never by its coordinates: a token names exactly one delivery for
//! the state machine's whole life, so a settlement that arrived late — for a delivery already
//! settled, for a copy a redelivery replaced, or for a partition since revoked — settles nothing
//! instead of discarding a live booking. A transaction token works the same way.

use std::sync::atomic::{AtomicI64, Ordering};

/// The token value no mint ever issues, so a host's "no delivery" or "no transaction" placeholder
/// cannot alias a live one.
pub const NO_TOKEN: i64 = 0;

/// The source of tokens: one per delivery or transaction, starting one past [`NO_TOKEN`] and
/// stepping by one. Atomic, so one mint serves a per-instance state machine and a process-wide
/// `static` alike.
pub struct TokenMint(AtomicI64);

impl Default for TokenMint {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenMint {
    /// A mint that has issued nothing.
    pub const fn new() -> Self {
        Self(AtomicI64::new(NO_TOKEN + 1))
    }

    /// Issue the next token. Not `next`: this is no iterator, and clippy reads the name as one.
    pub fn issue(&self) -> i64 {
        // `fetch_add` wraps rather than saturating: saturation would hand the same token out
        // forever. A wrap needs 2^63 issues, and only matters if a token that old were still live.
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_step_from_one_past_the_placeholder() {
        let mint = TokenMint::new();
        let issued: Vec<i64> = (0..4).map(|_| mint.issue()).collect();
        assert_eq!(issued, [1, 2, 3, 4]);
        assert!(issued.iter().all(|&token| token != NO_TOKEN));
    }

    #[test]
    fn the_counter_wraps_rather_than_repeating_a_token() {
        let mint = TokenMint(AtomicI64::new(i64::MAX));
        assert_eq!(mint.issue(), i64::MAX);
        assert_eq!(
            mint.issue(),
            i64::MIN,
            "the step wraps instead of saturating on i64::MAX"
        );
    }

    #[test]
    fn a_static_mint_hands_each_thread_a_distinct_token() {
        static MINT: TokenMint = TokenMint::new();
        let mut issued: Vec<i64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8).map(|_| scope.spawn(|| MINT.issue())).collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap_or(NO_TOKEN))
                .collect()
        });
        issued.sort_unstable();
        assert_eq!(
            issued,
            (1..=8).collect::<Vec<i64>>(),
            "no token is issued twice under contention"
        );
    }
}
