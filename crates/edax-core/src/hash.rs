//! FNV-1a hashing. Fully specified and dependency-free, so every binding computes the same hash.
//! Used by the event kernel's partitioner (32-bit) and by [`payload_hash`] (64-bit), the hash every
//! idempotency guard binds its key to.

const FNV32_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV32_PRIME: u32 = 0x0100_0193;
const FNV64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 32-bit.
pub fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = FNV32_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV32_PRIME);
    }
    hash
}

/// FNV-1a 64-bit.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = FNV64_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV64_PRIME);
    }
    hash
}

/// The payload hash an idempotency record binds its key to: FNV-1a 64-bit over `bytes`. One function for
/// both guards, so a command's and an event's hash cannot drift apart. A command guard hashes its
/// canonical JSON (`edax_cqrs::command_hash`), an event guard the raw `data` bytes
/// (`edax_event::event_idempotency`). Pinned by `testdata/golden/payload_hash.json`.
pub fn payload_hash(bytes: &[u8]) -> u64 {
    fnv1a64(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv32_known_vectors() {
        assert_eq!(fnv1a32(b""), 0x811c_9dc5);
        assert_eq!(fnv1a32(b"a"), 0xe40c_292c);
        assert_eq!(fnv1a32(b"foobar"), 0xbf9c_f968);
    }

    #[test]
    fn fnv64_known_vectors() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    }
}
