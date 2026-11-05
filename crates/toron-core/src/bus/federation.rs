// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Federation policy (ADR-0003 A4 / NEW-D3).
//!
//! Phase-1 federated privacy is NIP-17-only: only gift-wrapped DMs cross
//! external relays, and their confidentiality holds cryptographically. toron's
//! default is **local-only — zero network egress** (ADR-0003 Decision 5:
//! federation default list empty, opt-in).
//!
//! Public-relay deployment is operator-enabled and NEVER silent. It requires
//! hardening because random gift-wrap keys defeat pubkey-reputation filtering:
//! **NIP-42 AUTH** (membership gate) + **NIP-13 POW** (raise spam cost) +
//! **allowlist** (trusted relay URLs / authors). Risk +12 (public-relay NIP-17
//! spam) is documented in ADR-0003; this module encodes the gate.

/// Federation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FederationMode {
    /// Local-first: zero relays configured, no network egress. Default.
    #[default]
    LocalOnly,
    /// Operator-enabled public relay mode (requires AUTH + POW + allowlist).
    Public,
}

/// NIP-13 proof-of-work difficulty for outbound publishing (public mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PowDifficulty(pub u8);

impl Default for PowDifficulty {
    fn default() -> Self {
        Self(20)
    }
}

/// Federation policy configuration.
#[derive(Debug, Clone, Default)]
pub struct FederationPolicy {
    /// Mode. Defaults to [`FederationMode::LocalOnly`].
    pub mode: FederationMode,
    /// Trusted relay URLs (public mode). Empty in local-only mode.
    pub relays: Vec<String>,
    /// NIP-42 AUTH required (public mode MUST enable to pass doctor).
    pub auth_required: bool,
    /// NIP-13 POW difficulty (public mode).
    pub pow: PowDifficulty,
    /// Allowlisted author pubkeys (hex). Empty = accept all AUTH'd members.
    pub allowlist: Vec<String>,
}

/// Validation result from the `doctor` check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FederationIssue {
    /// Public mode without NIP-42 AUTH is a hard misconfiguration.
    PublicWithoutAuth,
    /// Public mode without any relay configured.
    PublicWithoutRelays,
    /// Local-only mode but relays are configured (dead config).
    LocalWithRelays,
    /// Public mode with POW below the floor.
    PowTooLow(u8),
}

impl FederationPolicy {
    /// Doctor check: returns all configuration issues.
    ///
    /// Local-only mode passes iff no relays are configured. Public mode
    /// requires NIP-42 AUTH + at least one relay + POW >= floor.
    pub fn doctor(&self) -> Vec<FederationIssue> {
        let mut issues = Vec::new();
        match self.mode {
            FederationMode::LocalOnly => {
                if !self.relays.is_empty() {
                    issues.push(FederationIssue::LocalWithRelays);
                }
            }
            FederationMode::Public => {
                if !self.auth_required {
                    issues.push(FederationIssue::PublicWithoutAuth);
                }
                if self.relays.is_empty() {
                    issues.push(FederationIssue::PublicWithoutRelays);
                }
                const POW_FLOOR: u8 = 16;
                if self.pow.0 < POW_FLOOR {
                    issues.push(FederationIssue::PowTooLow(self.pow.0));
                }
            }
        }
        issues
    }

    /// True when the policy permits outbound network egress.
    pub fn egress_enabled(&self) -> bool {
        self.mode == FederationMode::Public && !self.relays.is_empty()
    }

    /// Author is allowed to publish to external relays.
    ///
    /// Empty allowlist = any AUTH'd member (NIP-42 gate is the membership
    /// check); non-empty = explicit allowlist.
    pub fn author_allowed(&self, pubkey_hex: &str) -> bool {
        self.allowlist.is_empty() || self.allowlist.iter().any(|a| a == pubkey_hex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local() -> FederationPolicy {
        FederationPolicy::default()
    }

    fn public_ok() -> FederationPolicy {
        FederationPolicy {
            mode: FederationMode::Public,
            relays: vec!["wss://relay.example.com".into()],
            auth_required: true,
            pow: PowDifficulty(20),
            allowlist: vec![],
        }
    }

    #[test]
    fn test_default_is_local_only_zero_egress() {
        let p = local();
        assert_eq!(p.mode, FederationMode::LocalOnly);
        assert!(p.relays.is_empty());
        assert!(
            !p.egress_enabled(),
            "default must make zero network connections"
        );
        assert!(p.doctor().is_empty(), "local-only default is healthy");
    }

    #[test]
    fn test_public_requires_auth() {
        let p = FederationPolicy {
            mode: FederationMode::Public,
            relays: vec!["wss://relay.example.com".into()],
            auth_required: false,
            ..Default::default()
        };
        assert!(p.doctor().contains(&FederationIssue::PublicWithoutAuth));
        assert!(p.egress_enabled(), "egress is on even if doctor fails");
    }

    #[test]
    fn test_public_requires_relays() {
        let p = FederationPolicy {
            mode: FederationMode::Public,
            relays: vec![],
            auth_required: true,
            ..Default::default()
        };
        assert!(p.doctor().contains(&FederationIssue::PublicWithoutRelays));
    }

    #[test]
    fn test_public_requires_pow_floor() {
        let p = FederationPolicy {
            mode: FederationMode::Public,
            relays: vec!["wss://relay.example.com".into()],
            auth_required: true,
            pow: PowDifficulty(4),
            ..Default::default()
        };
        assert!(p.doctor().contains(&FederationIssue::PowTooLow(4)));
    }

    #[test]
    fn test_healthy_public_passes_doctor() {
        let p = public_ok();
        assert!(p.doctor().is_empty());
        assert!(p.egress_enabled());
    }

    #[test]
    fn test_local_with_relays_is_dead_config() {
        let p = FederationPolicy {
            mode: FederationMode::LocalOnly,
            relays: vec!["wss://relay.example.com".into()],
            ..Default::default()
        };
        assert!(p.doctor().contains(&FederationIssue::LocalWithRelays));
        assert!(
            !p.egress_enabled(),
            "local mode never egresses even with relays listed"
        );
    }

    #[test]
    fn test_allowlist_gates_authors() {
        let keys = crate::keys::generate();
        let hex = keys.public_key().to_hex();

        let open = public_ok();
        assert!(open.author_allowed(&hex));

        let other = crate::keys::generate().public_key().to_hex();
        let strict = FederationPolicy {
            allowlist: vec![other.clone()],
            ..public_ok()
        };
        assert!(strict.author_allowed(&other));
        assert!(!strict.author_allowed(&hex));
    }

    #[test]
    fn test_default_pow_is_20() {
        assert_eq!(PowDifficulty::default().0, 20);
    }
}
