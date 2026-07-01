use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn verify_signature(body: &[u8], secret: &[u8], signature: &str) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts arbitrary key length");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let signature = signature.trim();
    let signature = signature
        .strip_prefix("sha256=")
        .or_else(|| signature.strip_prefix("SHA256="))
        .unwrap_or(signature);

    if let Ok(expected) = hex::decode(signature) {
        return constant_time_eq(&digest, &expected);
    }

    if let Ok(expected) = BASE64_STANDARD.decode(signature) {
        return constant_time_eq(&digest, &expected);
    }

    false
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_body() -> &'static [u8] {
        br#"{
          "service": "example-service",
          "api": "blogs",
          "id": "content-id",
          "type": "edit",
          "contents": {
            "old": {"status": "DRAFT", "updatedAt": "2026-06-28T12:00:00Z"},
            "new": {"status": "PUBLISH", "updatedAt": "2026-06-29T12:00:00Z"}
          }
        }"#
    }

    #[test]
    fn verifies_hex_and_base64_hmac_signature() {
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(sample_body());
        let digest = mac.finalize().into_bytes();
        let signature = hex::encode(digest);
        let base64_signature = BASE64_STANDARD.encode(digest);

        assert!(verify_signature(sample_body(), b"secret", &signature));
        assert!(verify_signature(
            sample_body(),
            b"secret",
            &base64_signature
        ));
        assert!(verify_signature(
            sample_body(),
            b"secret",
            &format!("sha256={signature}")
        ));
        assert!(!verify_signature(sample_body(), b"wrong", &signature));
    }
}
