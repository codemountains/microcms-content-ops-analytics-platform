use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

const CROCKFORD_BASE32: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ULID_ENTROPY_BYTES: usize = 10;
const ULID_TIMESTAMP_MAX: u64 = (1_u64 << 48) - 1;

pub(super) fn deterministic_content_ulid(
    timestamp: DateTime<Utc>,
    seed: u64,
    namespace: &str,
    api: &str,
    index: u32,
) -> String {
    let timestamp_millis =
        u64::try_from(timestamp.timestamp_millis()).expect("debug seed timestamp must be positive");
    assert!(
        timestamp_millis <= ULID_TIMESTAMP_MAX,
        "debug seed timestamp must fit ULID timestamp"
    );

    let mut hasher = Sha256::new();
    hasher.update(seed.to_be_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update([0]);
    hasher.update(api.as_bytes());
    hasher.update([0]);
    hasher.update(index.to_be_bytes());
    let digest = hasher.finalize();

    let mut entropy = 0_u128;
    for byte in digest.iter().take(ULID_ENTROPY_BYTES) {
        entropy = (entropy << 8) | u128::from(*byte);
    }

    encode_ulid((u128::from(timestamp_millis) << 80) | entropy)
}

fn encode_ulid(value: u128) -> String {
    let mut encoded = String::with_capacity(26);
    for offset in (0..26).rev() {
        let index = ((value >> (offset * 5)) & 0x1f) as usize;
        encoded.push(CROCKFORD_BASE32[index] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn deterministic_content_ulid_encodes_crockford_base32() {
        let timestamp = Utc.with_ymd_and_hms(2026, 6, 29, 0, 0, 0).unwrap();

        let ulid = deterministic_content_ulid(timestamp, 42, "test", "blogs", 7);

        assert_eq!(ulid.len(), 26);
        assert_eq!(ulid, "01KW8ASZ00NVA8JB1AA7DAW871");
        assert!(ulid.bytes().all(|byte| matches!(
            byte,
            b'0'..=b'9'
                | b'A'..=b'H'
                | b'J'..=b'K'
                | b'M'..=b'N'
                | b'P'..=b'T'
                | b'V'..=b'Z'
        )));
        assert_eq!(
            ulid,
            deterministic_content_ulid(timestamp, 42, "test", "blogs", 7)
        );
    }
}
