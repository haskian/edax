//! Settlement tokens: the placeholder value, and the mint that hands out live ones.
//!
//! A delivery settles by token and never by its coordinates: a token names exactly one delivery for
//! the state machine's whole life, so a settlement that arrived late — for a delivery already
//! settled, for a copy a redelivery replaced, or for a partition since revoked — settles nothing
//! instead of discarding a live booking.

/// The token value no delivery ever carries, so a host's "no delivery" placeholder cannot alias a
/// live one.
pub const NO_DELIVERY: i64 = 0;

/// The source of settlement tokens: one per delivery, starting one past [`NO_DELIVERY`] and
/// stepping by one.
pub struct TokenMint(i64);

impl Default for TokenMint {
    fn default() -> Self {
        Self(NO_DELIVERY + 1)
    }
}

impl TokenMint {
    /// A mint that has issued nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Issue the next token. Not `next`: this is no iterator, and clippy reads the name as one.
    pub fn issue(&mut self) -> i64 {
        let token = self.0;
        // Wrapping, not saturating: saturation would hand the same token out forever. A wrap needs
        // 2^63 deliveries, and only matters if a token that old were still unsettled.
        self.0 = token.wrapping_add(1);
        token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_step_from_one_past_the_placeholder() {
        let mut mint = TokenMint::new();
        let issued: Vec<i64> = (0..4).map(|_| mint.issue()).collect();
        assert_eq!(issued, [1, 2, 3, 4]);
        assert!(issued.iter().all(|&token| token != NO_DELIVERY));
    }

    #[test]
    fn the_counter_wraps_rather_than_repeating_a_token() {
        let mut mint = TokenMint(i64::MAX);
        assert_eq!(mint.issue(), i64::MAX);
        assert_eq!(mint.issue(), i64::MIN, "the step wraps instead of saturating on i64::MAX");
    }
}
