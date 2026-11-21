// governed-by: ADR-0039
//! bd-6zo01 G8: endpoint verification cache and receiver-published well-known
//! consent, both over the same SSRF-hardened egress as deliveries. Pure here:
//! the HTTP fetch lives in toron-mcp, so a wrong consent rule is a unit failure.

use std::collections::HashMap;

/// A verification result is cached per (principal, url), not per subscription,
/// so varying `arguments` cannot multiply verification POSTs at a victim.
#[derive(Debug, Default)]
pub struct VerificationCache {
    entries: HashMap<String, i64>,
    ttl_us: i64,
}

impl VerificationCache {
    pub fn with_ttl(ttl_us: i64) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_us,
        }
    }

    fn key(principal: &str, url: &str) -> String {
        format!("{principal}\0{url}")
    }

    pub fn is_verified(&self, principal: &str, url: &str, now_us: i64) -> bool {
        self.entries
            .get(&Self::key(principal, url))
            .is_some_and(|until| *until > now_us)
    }

    pub fn mark(&mut self, principal: &str, url: &str, now_us: i64) {
        self.entries.insert(
            Self::key(principal, url),
            now_us.saturating_add(self.ttl_us),
        );
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// True when a well-known receiver document consents to deliveries at `url`.
/// The document is `{"receivers":["/hooks/", ...]}`; a match is a path-prefix
/// match on the callback URL. Unparseable documents never grant consent.
pub fn well_known_covers(document: &str, url: &str) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(document) else {
        return false;
    };
    let Some(receivers) = v.get("receivers").and_then(|r| r.as_array()) else {
        return false;
    };
    let path = url
        .split_once("://")
        .and_then(|(_, rest)| rest.find('/').map(|at| &rest[at..]))
        .unwrap_or("/");
    receivers
        .iter()
        .filter_map(|r| r.as_str())
        .any(|prefix| !prefix.is_empty() && prefix.starts_with('/') && path.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consent_is_a_path_prefix_match_and_fails_closed() {
        let doc = r#"{"receivers":["/hooks/"]}"#;
        assert!(well_known_covers(
            doc,
            "https://proxy.example/hooks/client123"
        ));
        assert!(!well_known_covers(doc, "https://proxy.example/other"));
        assert!(!well_known_covers(
            "not json",
            "https://proxy.example/hooks/x"
        ));
        assert!(!well_known_covers(
            r#"{"receivers":[]}"#,
            "https://proxy.example/hooks/x"
        ));
    }

    #[test]
    fn verification_is_cached_per_principal_and_url_and_expires() {
        let mut cache = VerificationCache::with_ttl(1_000);
        assert!(!cache.is_verified("P", "https://x/h", 10));
        cache.mark("P", "https://x/h", 10);
        assert!(cache.is_verified("P", "https://x/h", 999));
        assert!(!cache.is_verified("P", "https://x/h", 1_010), "expired");
        assert!(
            !cache.is_verified("Q", "https://x/h", 999),
            "other principal"
        );
        assert!(!cache.is_verified("P", "https://y/h", 999), "other url");
        assert_eq!(cache.len(), 1);
    }
}
