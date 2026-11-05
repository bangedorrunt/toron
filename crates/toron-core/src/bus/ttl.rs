// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! NIP-40 envelope TTL (ADR-0003 rev6): transient envelopes vs permanent Git.
//!
//! Gift-wrapped mail / bus envelopes carry a NIP-40 `expiration` tag (default
//! 90 days, configurable). The bus omits expired envelopes from replay and
//! prunes them (bounded bus growth — the parity-superiority row vs AM-Rust's
//! unbounded single store). The Git archive holds unwrapped canonical content
//! **permanently** (no TTL) — that separation is the archive layer's concern.

use crate::nostr::{Tag, Timestamp};

/// Default envelope TTL: 90 days (ADR-0003 rev6: "gift-wrap envelopes TTL 90
/// days, configurable").
pub const DEFAULT_ENVELOPE_TTL_SECS: u64 = 90 * 24 * 60 * 60;

/// Envelope TTL configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeTtl {
    /// Seconds the envelope lives on the bus before expiry. `None` = no
    /// expiration (permanent envelope — not the default).
    pub secs: Option<u64>,
}

impl Default for EnvelopeTtl {
    fn default() -> Self {
        Self {
            secs: Some(DEFAULT_ENVELOPE_TTL_SECS),
        }
    }
}

impl EnvelopeTtl {
    /// Local-only bus: no egress, envelopes may live longer if configured.
    pub const fn permanent() -> Self {
        Self { secs: None }
    }

    /// The NIP-40 expiration timestamp for a publish at `now`.
    pub fn expiration_at(&self, now: Timestamp) -> Option<Timestamp> {
        self.secs
            .map(|s| Timestamp::from(now.as_secs().saturating_add(s)))
    }

    /// The NIP-40 `expiration` tag to add during event construction.
    ///
    /// Returns `None` when the TTL is permanent. Callers add this to the tag
    /// list at build time — mutating a signed `Event` post-hoc would leave its
    /// canonical `id` stale (forgery-adjacent).
    pub fn expiration_tag(&self, created_at: Timestamp) -> Option<Tag> {
        self.expiration_at(created_at).map(Tag::expiration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::{Keys, Kind};

    fn keys() -> Keys {
        Keys::generate()
    }

    #[test]
    fn test_default_ttl_is_90_days() {
        assert_eq!(DEFAULT_ENVELOPE_TTL_SECS, 90 * 24 * 60 * 60);
        let ttl = EnvelopeTtl::default();
        assert_eq!(ttl.secs, Some(DEFAULT_ENVELOPE_TTL_SECS));
    }

    #[test]
    fn test_expiration_at_computes_ttl() {
        let ttl = EnvelopeTtl::default();
        let exp = ttl
            .expiration_at(Timestamp::from(1_700_000_000_u64))
            .expect("ttl set");
        assert_eq!(exp.as_secs(), 1_700_000_000 + DEFAULT_ENVELOPE_TTL_SECS);
    }

    #[test]
    fn test_permanent_has_no_expiration() {
        let ttl = EnvelopeTtl::permanent();
        assert_eq!(ttl.expiration_at(Timestamp::now()), None);
        assert!(ttl.expiration_tag(Timestamp::now()).is_none());
    }

    #[test]
    fn test_expiration_tag_included_in_signed_event() {
        let k = keys();
        let ttl = EnvelopeTtl::default();
        let ts = Timestamp::from(1_700_000_000_u64);
        let mut tags: Vec<Tag> = Vec::new();
        if let Some(tag) = ttl.expiration_tag(ts) {
            tags.push(tag);
        }
        let ev = build_event(&k, Kind::GiftWrap, "wrapped", tags, ts).expect("build");
        let exp = ev.tags.expiration().expect("expiration tag present");
        assert_eq!(exp.as_secs(), 1_700_000_000 + DEFAULT_ENVELOPE_TTL_SECS);
        assert!(crate::nostr::event::verify_event(&ev).is_ok());
    }
}
