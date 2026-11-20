// governed-by: ADR-0039 U5 (KTD6)
#![forbid(unsafe_code)]

//! Standard Webhooks v1 signing and endpoint verification (U5).
//!
//! HMAC-SHA256 over `{webhook-id}.{webhook-timestamp}.{raw body}` with the
//! decoded `whsec_<base64>` secret, base64-encoded as `v1,<sig>`. Endpoint
//! verification uses the derived key `HMAC(secret, "wev-v1")`. No new crate:
//! HMAC and base64 are implemented here over the existing `sha2` dependency.
//!
//! The secret is NEVER logged: Debug/Display of `Secret` is redacted.

use sha2::{Digest, Sha256};

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// A `whsec_` shared secret. Debug/Display are redacted so a log can never
/// carry the raw key (KTD5 step 8).
#[derive(Clone)]
pub struct Secret {
    key: Vec<u8>,
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret([REDACTED])")
    }
}

impl Secret {
    pub fn from_whsec(s: &str) -> Result<Self, String> {
        let raw = s.strip_prefix("whsec_").unwrap_or(s);
        if raw.is_empty() {
            return Err("empty webhook secret".into());
        }
        let key = b64_decode(raw).ok_or_else(|| "webhook secret is not base64".to_string())?;
        if key.is_empty() {
            return Err("empty decoded webhook secret".into());
        }
        Ok(Self { key })
    }

    /// Generate a fresh `whsec_<base64(32 random bytes)>` (returned once).
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).expect("webhook secret rng");
        Self { key: key.to_vec() }
    }

    pub fn whsec(&self) -> String {
        format!("whsec_{}", b64_encode(&self.key))
    }
}

fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes.get(i + 1).copied();
        let b2 = bytes.get(i + 2).copied();
        out.push(B64[(b0 >> 2) as usize] as char);
        out.push(B64[(((b0 & 0x03) << 4) | (b1.unwrap_or(0) >> 4)) as usize] as char);
        if let Some(b1) = b1 {
            out.push(B64[(((b1 & 0x0f) << 2) | (b2.unwrap_or(0) >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if let Some(b2) = b2 {
            out.push(B64[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    out
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let clean: Vec<u8> = s.bytes().filter(|b| *b != b'=').collect();
    if clean.is_empty() || clean.len() % 4 == 1 {
        return None;
    }
    let val = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for b in clean {
        acc = (acc << 6) | val(b)? as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1u32 << bits) - 1;
        }
    }
    Some(out)
}

/// HMAC-SHA256 over `msg` with `key` (RFC 2104, 64-byte blocks).
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Vec::with_capacity(BLOCK + msg.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(msg);
    let ih = Sha256::digest(&inner);
    let mut outer = Vec::with_capacity(BLOCK + 32);
    outer.extend_from_slice(&opad);
    outer.extend_from_slice(&ih);
    let oh = Sha256::digest(&outer);
    let mut out = [0u8; 32];
    out.copy_from_slice(&oh);
    out
}

/// The exact bytes a v1 signature covers: `{id}.{timestamp}.{body}`.
pub fn signed_content(id: &str, timestamp: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!("{id}.{timestamp}.").into_bytes();
    out.extend_from_slice(body);
    out
}

/// The `webhook-signature` header value for one delivery.
pub fn sign(secret: &Secret, id: &str, timestamp: u64, body: &[u8]) -> String {
    let ts = timestamp.to_string();
    let mac = hmac_sha256(&secret.key, &signed_content(id, &ts, body));
    format!("v1,{}", b64_encode(&mac))
}

/// The `webhook-challenge-response` for endpoint verification: the derived
/// key is `HMAC(secret, "wev-v1")` and the challenge is signed with it.
pub fn endpoint_verification_response(secret: &Secret, challenge: &str) -> String {
    let key = hmac_sha256(&secret.key, b"wev-v1");
    let mac = hmac_sha256(&key, challenge.as_bytes());
    format!("v1,{}", b64_encode(&mac))
}

/// Verify a delivery's `webhook-signature` header, failing closed. All
/// candidates are compared in constant time over fixed 32-byte digests.
pub fn verify(
    secret: &Secret,
    id: &str,
    timestamp: &str,
    body: &[u8],
    signature_header: &str,
    now_secs: u64,
    tolerance_secs: u64,
) -> bool {
    let Ok(ts) = timestamp.parse::<u64>() else {
        return false;
    };
    if now_secs.abs_diff(ts) > tolerance_secs {
        return false;
    }
    let expected = hmac_sha256(&secret.key, &signed_content(id, timestamp, body));
    let mut ok = false;
    for candidate in signature_header.split_whitespace() {
        let Some(sig) = candidate.strip_prefix("v1,") else {
            continue;
        };
        let Some(got) = b64_decode(sig) else {
            continue;
        };
        // Constant-time compare over the expected length; a wrong length is a
        // mismatch but still walks all 32 bytes.
        let mut diff = (got.len() ^ 32) as u8;
        for i in 0..32 {
            let b = got.get(i).copied().unwrap_or(0);
            diff |= b ^ expected[i];
        }
        if diff == 0 {
            ok = true;
        }
    }
    ok
}

#[cfg(test)]
mod tests {
    use super::*;

    // Published Standard Webhooks v1 test vector (shared by the six reference
    // implementations), plus the endpoint-verification vector.
    const SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    const MSG_ID: &str = "msg_p5jXN8AQM9LWM0D4loKWxJek";
    const TS: &str = "1614265330";
    const BODY: &[u8] = br#"{"test": 2432232314}"#;
    const SIG: &str = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

    #[test]
    fn signature_matches_the_published_standard_webhooks_vector() {
        let s = Secret::from_whsec(SECRET).expect("secret");
        assert_eq!(sign(&s, MSG_ID, TS.parse().unwrap(), BODY), SIG);
    }

    #[test]
    fn verification_accepts_a_real_signature_and_rejects_tampering() {
        let s = Secret::from_whsec(SECRET).expect("secret");
        assert!(verify(&s, MSG_ID, TS, BODY, SIG, 1_614_265_330, 300));
        assert!(!verify(
            &s,
            MSG_ID,
            TS,
            br#"{"test":0}"#,
            SIG,
            1_614_265_330,
            300
        ));
        assert!(!verify(&s, MSG_ID, TS, BODY, SIG, 1_614_265_931, 300));
        assert!(!verify(
            &s,
            MSG_ID,
            "not-a-time",
            BODY,
            SIG,
            1_614_265_330,
            300
        ));
    }

    #[test]
    fn endpoint_verification_matches_the_published_vector() {
        let s = Secret::from_whsec(SECRET).expect("secret");
        assert_eq!(
            endpoint_verification_response(&s, "challenge_123"),
            "v1,kB/p+Are8EXsK+elLOyTQWWzz8D6msGsHc2httWz348="
        );
    }

    #[test]
    fn a_generated_secret_round_trips_and_is_never_debugged_in_plain() {
        let s = Secret::generate();
        let whsec = s.whsec();
        assert!(whsec.starts_with("whsec_"));
        assert_eq!(Secret::from_whsec(&whsec).unwrap().whsec(), whsec);
        assert!(!format!("{s:?}").contains(&whsec[6..]));
    }
}
