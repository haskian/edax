//! Broker placement: the partition a keyed record lands on.
//!
//! Every producer on a topic must place one key on one partition, or two producers split an
//! aggregate across partitions and its order is lost. The producers on an edax topic are not all
//! edax: a Java service, Kafka Connect and Kafka Streams place a keyed record with Java's default
//! partitioner, and so does aiokafka, while librdkafka (`rdkafka`, `confluent-kafka`) defaults to
//! CRC32. So the kernel fixes the Java rule, and every edax shell passes the partition explicitly
//! instead of leaving it to its client library:
//!
//! `partition = (murmur2(key) & 0x7fffffff) % partitions`
//!
//! `murmur2` is `org.apache.kafka.common.utils.Utils.murmur2`, and the mask is `Utils.toPositive`;
//! `BuiltInPartitioner.partitionForKey` composes them. `testdata/golden/kafka_partition.json` pins
//! the function, including Kafka's own `UtilsTest.testMurmur2` vectors.

use edax_core::Error;

/// The partition a record keyed `key` lands on in a topic of `partitions` partitions, as Java's
/// default partitioner places it. The key is the record's key bytes after every middleware ran, the
/// bytes the broker stores. A topic has at least one partition, so `0` is a `codec` error rather
/// than a division by zero.
pub fn partition_for(key: &[u8], partitions: u32) -> Result<u32, Error> {
    if partitions == 0 {
        return Err(Error::codec("a topic has at least one partition, got 0"));
    }
    Ok((murmur2(key) & 0x7fff_ffff) % partitions)
}

/// Kafka's `Utils.murmur2`: MurmurHash2, 32-bit, seed `0x9747b28c`, little-endian blocks. Java's
/// `int` arithmetic wraps, so every step here wraps too, and `>>>` is a shift of the unsigned value.
fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    // Java takes the length as an `int`; a key is never near 4 GiB, so the truncation is moot.
    #[allow(clippy::cast_possible_truncation, reason = "Java's length is a 32-bit int")]
    let mut h = SEED ^ data.len() as u32;
    let mut blocks = data.chunks_exact(4);
    for block in &mut blocks {
        if let &[a, b, c, d] = block {
            let mut k = u32::from_le_bytes([a, b, c, d]);
            k = k.wrapping_mul(M);
            k ^= k >> R;
            k = k.wrapping_mul(M);
            h = h.wrapping_mul(M);
            h ^= k;
        }
    }
    // Java's switch falls through: three trailing bytes also take the two- and one-byte steps.
    let tail = blocks.remainder();
    if let Some(&c) = tail.get(2) {
        h ^= u32::from(c) << 16;
    }
    if let Some(&b) = tail.get(1) {
        h ^= u32::from(b) << 8;
    }
    if let Some(&a) = tail.first() {
        h ^= u32::from(a);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kafka's own vectors, verbatim from `UtilsTest.testMurmur2`
    /// (`clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java`), as Java `int`s. They
    /// cover every tail length: 2, 6, 24, 26, 48 and 3 bytes.
    #[test]
    fn murmur2_matches_kafkas_utils_test() {
        let cases: [(&[u8], i32); 6] = [
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58_897_971),
            (b"abc", 479_470_107),
        ];
        for (key, java) in cases {
            assert_eq!(murmur2(key).cast_signed(), java, "{}", String::from_utf8_lossy(key));
        }
    }

    #[test]
    fn partition_for_branches() {
        // A negative Java hash: the mask, not `abs`, makes it positive. `abs` would give 973932308.
        let masked = (-973_932_308i32).cast_unsigned() & 0x7fff_ffff;
        type Case = (&'static str, &'static [u8], u32, Result<u32, Error>);
        let cases: [Case; 5] = [
            ("negative_hash_is_masked_not_negated", b"21", u32::MAX, Ok(masked)),
            ("one_partition_takes_every_key", b"21", 1, Ok(0)),
            ("a_positive_hash", b"abc", 12, Ok(479_470_107 % 12)),
            ("the_empty_key_is_a_key", b"", 7, Ok((murmur2(b"") & 0x7fff_ffff) % 7)),
            (
                "no_partitions_is_refused",
                b"21",
                0,
                Err(Error::codec("a topic has at least one partition, got 0")),
            ),
        ];
        for (name, key, partitions, want) in cases {
            assert_eq!(partition_for(key, partitions), want, "{name}");
        }
    }
}
