// governed-by: ADR-0018
//! Canonical agent-name grammar (ADR-0018 D1/D3).
//!
//! The canonical routable identity per pane is `flywheel-<project-slug>-
//! <role><seq>` — seq 1 is suffix-less (`flywheel-toron-oc`), seq ≥ 2 appends
//! digits (`flywheel-toron-oc2`, `-oc3`). This mirrors flywheel's
//! `agent_name_n` exactly, so both sides of the boundary build and parse the
//! SAME names. Names are project-scoped: a name embeds its project slug.
//!
//! Pure module — no IO. The DB-backed helpers (identity mode, agent-name
//! aliases) live next to their owners (infra.rs, identity.rs).

/// Per-project routing policy (ADR-0018 D3).
///
/// - [`IdentityMode::Swarm`]: whois + send-path accept canonical names or
///   npubs ONLY; non-canonical names fail with actionable errors; register/
///   create reject non-canonical names.
/// - [`IdentityMode::Open`] (default): any registered name resolves (legacy
///   AM-Rust parity; the demo/fixture projects).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityMode {
    Open,
    Swarm,
}

impl IdentityMode {
    /// Parse a persisted mode string; anything unrecognized is `Open`
    /// (fail-soft — a corrupt row never hard-blocks a project).
    pub fn parse(s: &str) -> Self {
        match s {
            "swarm" => IdentityMode::Swarm,
            _ => IdentityMode::Open,
        }
    }

    /// The persisted form.
    pub fn as_str(self) -> &'static str {
        match self {
            IdentityMode::Open => "open",
            IdentityMode::Swarm => "swarm",
        }
    }
}

/// Parse a canonical agent name into `(role, seq)`.
///
/// Grammar: `flywheel-<project-slug>-<role><seq>` where `seq` is 1-based and
/// omitted when 1 (`flywheel-toron-oc`), digit-suffixed when ≥ 2
/// (`flywheel-toron-oc2`). `None` when the name is not canonical FOR THIS
/// PROJECT — including names embedding a different project slug
/// (`flywheel-other-oc` in project `toron` is not canonical).
pub fn parse_canonical_name(name: &str, project_slug: &str) -> Option<(String, u32)> {
    let prefix = format!("flywheel-{project_slug}-");
    let rest = name.strip_prefix(&prefix)?;
    let digit_at = rest
        .find(|c: char| c.is_ascii_digit())
        .unwrap_or(rest.len());
    let (role, digits) = rest.split_at(digit_at);
    if role.is_empty() || !role.chars().all(|c| c.is_ascii_lowercase()) {
        return None;
    }
    if digits.is_empty() {
        return Some((role.to_string(), 1));
    }
    let seq: u32 = digits.parse().ok()?;
    if seq < 2 {
        return None; // seq 1 is suffix-less; `flywheel-toron-oc1` never exists
    }
    Some((role.to_string(), seq))
}

/// Build the canonical name for `(role, seq)` in `project_slug` (mirrors
/// flywheel `agent_name_n`: seq 1 → bare, seq ≥ 2 → digit-suffixed).
pub fn canonical_name(project_slug: &str, role: &str, seq: u32) -> String {
    if seq <= 1 {
        format!("flywheel-{project_slug}-{role}")
    } else {
        format!("flywheel-{project_slug}-{role}{seq}")
    }
}

/// `true` when `name` is a canonical agent name for `project_slug`.
pub fn is_canonical_name(name: &str, project_slug: &str) -> bool {
    parse_canonical_name(name, project_slug).is_some()
}

/// `true` when `s` is a 64-hex npub routing address (ADR-0018 D3).
pub fn is_npub(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// bd-pin-public-plane-s-easy.8: `true` when `s` looks like a host OS
/// username (`$USER`) — a single lowercase-alphanumeric token: lowercase
/// start, then lowercase letters and digits only. NO separators (`-`, `_`,
/// `.`): dashed names are slug/agent-shaped (`lane-1-bootstrap`,
/// `flywheel-toron-coder`) and must keep the generic grammar error.
///
/// A username is NOT a mailbox identity: it is never a pin shape (needs an
/// uppercase pair), never a canonical `flywheel-<slug>-<role><seq>` name,
/// and never an npub. Detected so the reject path can name the class and
/// the recovery (registered pin / `list_agents`) instead of the generic
/// grammar error — callers who type their own OS username think it is their
/// mailbox name.
///
/// Pure shape check: a folded legacy alias is checked BEFORE this in
/// `resolve_agent_name` (an explicit fold is authoritative — bd-6wfv), so a
/// username-shaped alias still resolves.
pub fn is_username_class(s: &str) -> bool {
    let b = s.as_bytes();
    let Some(&first) = b.first() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    b.iter()
        .skip(1)
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// `true` when `name` has the adjective+noun pin shape (ADR-0018 D1a,
/// captain amendment 2026-08-15: the routable mail identity is a REGISTERED
/// adj+noun pin — `DustySparrow`, `RoseDune`, `CoralFinch`).
///
/// Shape: exactly two capitalized words concatenated, no separators, no
/// digits, no underscores (`[A-Z][a-z]+[A-Z][a-z]+`). This mirrors the
/// legacy flywheel pin generator's whitelist pair space (adjective + noun).
/// Shape is necessary, not sufficient: a pin is only ROUTABLE once
/// registered in the project (registration is the gate, the shape is the
/// grammar — D1a #3: unregistered free strings remain rejected).
pub fn is_adjnoun_pin(name: &str) -> bool {
    let b = name.as_bytes();
    if b.len() < 4 || !b[0].is_ascii_uppercase() {
        return false;
    }
    let mut words = 1usize;
    let mut saw_lower = false;
    for (_, &c) in b.iter().enumerate().skip(1) {
        if c.is_ascii_uppercase() {
            if !saw_lower {
                return false; // double upper: `DUSTYsparrow`
            }
            words += 1;
            saw_lower = false;
        } else if c.is_ascii_lowercase() {
            saw_lower = true;
        } else {
            return false; // digit / separator / underscore
        }
    }
    words == 2 && saw_lower
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_names_across_seq_bounds() {
        assert_eq!(
            parse_canonical_name("flywheel-toron-oc", "toron"),
            Some(("oc".to_string(), 1))
        );
        assert_eq!(
            parse_canonical_name("flywheel-toron-oc2", "toron"),
            Some(("oc".to_string(), 2))
        );
        assert_eq!(
            parse_canonical_name("flywheel-toron-oc10", "toron"),
            Some(("oc".to_string(), 10))
        );
        assert_eq!(
            parse_canonical_name("flywheel-toron-coder", "toron"),
            Some(("coder".to_string(), 1))
        );
    }

    #[test]
    fn rejects_non_canonical_shapes() {
        // Wrong project slug embedded in the name.
        assert_eq!(parse_canonical_name("flywheel-other-oc", "toron"), None);
        // Seq 1 with a digit suffix never exists (builder omits it).
        assert_eq!(parse_canonical_name("flywheel-toron-oc1", "toron"), None);
        // Empty role.
        assert_eq!(parse_canonical_name("flywheel-toron-", "toron"), None);
        // Uppercase role.
        assert_eq!(parse_canonical_name("flywheel-toron-OC", "toron"), None);
        // Not a flywheel name at all.
        assert_eq!(parse_canonical_name("lane-1-bootstrap", "toron"), None);
        assert_eq!(parse_canonical_name("DustySparrow", "toron"), None);
        assert_eq!(parse_canonical_name("assistant", "toron"), None);
    }

    #[test]
    fn builder_round_trips_parser() {
        for (role, seq) in [("oc", 1), ("oc", 2), ("oc", 3), ("coder", 1), ("pi", 7)] {
            let name = canonical_name("toron", role, seq);
            assert_eq!(
                parse_canonical_name(&name, "toron"),
                Some((role.to_string(), seq)),
                "round trip {name}"
            );
        }
    }

    #[test]
    fn is_canonical_name_matches_parse() {
        assert!(is_canonical_name("flywheel-toron-oc2", "toron"));
        assert!(!is_canonical_name("flywheel-toron-oc2", "demo"));
        assert!(!is_canonical_name("BlueLake", "toron"));
        assert!(!is_canonical_name("flywheel-toron-oc1", "toron"));
    }

    #[test]
    fn npub_detection_is_strict_hex_64() {
        let npub = "d9ac60b20f7ce9bbcf1255b9f8e966a05ab6dc17f944865ac554d44a88f8f874";
        assert!(is_npub(npub));
        assert!(!is_npub("short"));
        assert!(!is_npub(&format!("{npub}ff")));
        assert!(!is_npub(
            "zzac60b20f7ce9bbcf1255b9f8e966a05ab6dc17f944865ac554d44a88f8f874"
        ));
        assert!(!is_npub("flywheel-toron-oc2"));
    }

    #[test]
    fn adjnoun_pin_shape_matches_legacy_pins() {
        for pin in [
            "DustySparrow",
            "RoseDune",
            "BeigeFern",
            "CoralFinch",
            "MistyDesert",
            "BlueLynx",
            "PinkGorge",
            "CalmPeak",
            "CyanCove",
            "ScarletOwl",
            "SunnyTiger",
            "FrostyKite",
            "BlackMouse",
            "WindyCrane",
            "OliveLeopard",
            "FoggyTern",
            "SandyBay",
            "StormyHollow",
            "KiteLoft",
        ] {
            assert!(is_adjnoun_pin(pin), "{pin} must be a valid pin shape");
        }
    }

    #[test]
    fn adjnoun_pin_rejects_non_shapes() {
        for bad in [
            "flywheel-toron-oc",
            "flywheel-toron-oc2",
            "assistant",
            "lane-1-bootstrap",
            "lane4catalog",
            "dusky",
            "DUSTYsparrow",
            "Dusty",
            "Dusty Sparrow",
            "Dusty_Sparrow",
            "DustySparrow2",
            "DustySparrowX",
            "AB",
            "",
            "aB",
        ] {
            assert!(!is_adjnoun_pin(bad), "{bad:?} must NOT be a pin shape");
        }
    }

    #[test]
    fn identity_mode_parse_round_trip() {
        assert_eq!(IdentityMode::parse("swarm"), IdentityMode::Swarm);
        assert_eq!(IdentityMode::parse("open"), IdentityMode::Open);
        assert_eq!(IdentityMode::parse("garbage"), IdentityMode::Open);
        assert_eq!(IdentityMode::Swarm.as_str(), "swarm");
        assert_eq!(IdentityMode::Open.as_str(), "open");
    }

    #[test]
    fn username_class_matches_os_user_names() {
        for name in ["bangedorrunt", "root", "docker", "ubuntu", "wwwdata"] {
            assert!(is_username_class(name), "{name} must be username-class");
        }
        // NO separators: dashed/underscored names are slug/agent-shaped
        // (lane-1-bootstrap, my_user) and keep the generic grammar error.
        // Uppercase (pins), digit-start, empty, and non-alnum are never
        // username-class.
        for name in [
            "BlueRidge",
            "DustySparrow",
            "flywheel-toron-oc",
            "lane-1-bootstrap",
            "my_user",
            "",
            "X",
            "1bangedorrunt",
            "aBc",
            "abc.def",
            "abc/def",
            "abc def",
        ] {
            assert!(
                !is_username_class(name),
                "{name:?} must NOT be username-class"
            );
        }
    }
}
