// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Identity ops — `create_agent_identity` vs `register_agent` (G1/G2).
//!
//! Pure (no I/O): constructs + signs the addressable kind-30013 agent profile
//! event whose `d` tag is `project-slug:name`. The profile is the name→npub
//! source of truth for project-scoped identity (G2). Key storage (ncryptsec)
//! and bus publishing are the caller's concern (mailbox layer).

use crate::nostr::{Event, Keys, PublicKey, Tag, Timestamp};
use ::nostr::error::Error as BuilderError;

use crate::bus::scoping::with_project_scoping;
use crate::nostr::event::build_event;
use crate::nostr::kinds::{AGENT_PROFILE, toron_kind};

/// Versioned persona / system-prompt pack (ADR-0003 "persona packs extend the
/// agent profile", Buzz `buzz-persona` analogue). Reproducible, swappable
/// agent behavior.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PersonaPack {
    /// Semver-style persona version (e.g. `"1.0.0"`).
    pub version: String,
    /// Persona name (e.g. `"researcher"`).
    pub name: String,
    /// The system prompt that defines this persona.
    pub system_prompt: String,
}

/// Agent profile metadata carried in the kind-30013 `content` (JSON).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentProfile {
    /// Display name (project-scoped).
    pub name: String,
    /// Agent program (pi / opencode / grok).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<String>,
    /// Model id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Short task description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_description: Option<String>,
    /// Versioned persona / system-prompt pack (Phase 2.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persona: Option<PersonaPack>,
}

impl AgentProfile {
    /// New profile with the required name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            program: None,
            model: None,
            task_description: None,
            persona: None,
        }
    }
}

/// Identity op result: the fresh-or-existing keys + the signed profile event.
#[derive(Debug)]
pub struct IdentityResult {
    /// The agent's keys (fresh for create, existing-or-fresh for register).
    pub keys: Keys,
    /// The signed kind-30013 profile event.
    pub event: Event,
}

/// Build the addressable `d` tag value `project-slug:name` (G2).
pub fn profile_d_tag(project_slug: &str, name: &str) -> String {
    format!("{project_slug}:{name}")
}

/// Build + sign the kind-30013 agent profile event (with dual-emit scoping).
///
/// The event's author pubkey IS the agent's npub. Project scoping tags
/// (`#t` + NIP-32 labels) are applied since kind-30013 is not metadata-hiding.
pub fn build_profile_event(
    keys: &Keys,
    project_slug: &str,
    profile: &AgentProfile,
    created_at: Timestamp,
) -> Result<Event, BuilderError> {
    build_profile_event_with_roles(keys, project_slug, profile, &[], created_at)
}

/// Build a profile event that also carries `role=` tags (bd-lmcy).
/// Roles are signed, replaceable via the same `d` tag — the latest event per
/// `d` wins (NIP-01). Each role becomes `["role", "<name>"]`.
pub fn build_profile_event_with_roles(
    keys: &Keys,
    project_slug: &str,
    profile: &AgentProfile,
    roles: &[String],
    created_at: Timestamp,
) -> Result<Event, BuilderError> {
    let content = serde_json::to_string(profile).expect("profile serializes");
    let mut base_tags =
        vec![Tag::parse(["d", &profile_d_tag(project_slug, &profile.name)]).expect("d tag")];
    for role in roles {
        if let Ok(t) = Tag::parse(["role", role]) {
            base_tags.push(t);
        }
    }
    let tags = with_project_scoping(toron_kind(AGENT_PROFILE), base_tags, Some(project_slug));
    build_event(keys, toron_kind(AGENT_PROFILE), content, tags, created_at)
}

/// Extract `role=` tags from a kind-30013 profile event (bd-lmcy).
pub fn profile_roles(event: &Event) -> Vec<String> {
    event
        .tags
        .iter()
        .filter_map(|t| {
            let s = t.as_slice();
            if s.first().map(String::as_str) == Some("role") && s.len() > 1 {
                Some(s[1].clone())
            } else {
                None
            }
        })
        .collect()
}

/// `create_agent_identity`: ALWAYS generate a fresh keypair + publish the
/// kind-30013 profile (ADR-0003 G1).
pub fn create_agent_identity(
    project_slug: &str,
    profile: &AgentProfile,
    created_at: Timestamp,
) -> Result<IdentityResult, BuilderError> {
    let keys = crate::keys::generate();
    let event = build_profile_event(&keys, project_slug, profile, created_at)?;
    Ok(IdentityResult { keys, event })
}

/// `register_agent`: upsert the profile on the EXISTING agent key, or create a
/// fresh keypair if absent (ADR-0003 G1).
pub fn register_agent(
    project_slug: &str,
    profile: &AgentProfile,
    existing: Option<&Keys>,
    created_at: Timestamp,
) -> Result<IdentityResult, BuilderError> {
    let keys = existing.cloned().unwrap_or_else(crate::keys::generate);
    let event = build_profile_event(&keys, project_slug, profile, created_at)?;
    Ok(IdentityResult { keys, event })
}

/// Extract the agent name from a kind-30013 `d` tag (`project-slug:name`).
pub fn profile_name_from_d_tag(d: &str) -> Option<&str> {
    d.split_once(':').map(|(_slug, name)| name)
}

/// Extract the agent's npub (the event author) from a kind-30013 profile.
pub const fn profile_npub(event: &Event) -> PublicKey {
    event.pubkey
}

/// Resolve a name to an npub from a project's kind-30013 profile events (G2).
///
/// Names are **project-scoped**: only profiles whose `d` tag is exactly
/// `<project_slug>:<name>` match. Unknown names return `None` (caller fails
/// fast, AM-Rust parity).
///
/// bd-0pcd: kind-30013 is a NIP-01 parameterized REPLACEABLE event. After a
/// key deletion + re-register, the archive holds TWO profiles under one d
/// tag — first-match resolution served the DEAD npub to whois and gift-wrap
/// (inbox starved while send reported success). The LATEST event per d tag
/// wins, regardless of replay order.
pub fn resolve_name<'a>(
    project_slug: &str,
    profiles: impl IntoIterator<Item = &'a Event>,
    name: &str,
) -> Option<PublicKey> {
    let want = profile_d_tag(project_slug, name);
    profiles
        .into_iter()
        .filter(|ev| ev.kind.as_u16() == AGENT_PROFILE)
        .filter(|ev| {
            ev.tags
                .iter()
                .find_map(|t| (t.kind() == "d").then(|| t.content()).flatten())
                == Some(want.as_str())
        })
        .max_by(|a, b| crate::bus::addressable::nip01_replaceable_cmp(a, b))
        .map(profile_npub)
}

/// Parse the profile content from a kind-30013 event.
pub fn parse_profile(event: &Event) -> Option<AgentProfile> {
    serde_json::from_str(&event.content).ok()
}

/// The kind-30013 profile event as JSON (wire-identical, relay-acceptable).
pub fn profile_json(event: &Event) -> String {
    event.as_json()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::scoping::is_metadata_hiding_kind;
    use crate::nostr::kinds::AGENT_PROFILE;

    const TS: u64 = 1_700_000_000;

    fn ts() -> Timestamp {
        Timestamp::from(TS)
    }

    fn sample_profile() -> AgentProfile {
        AgentProfile {
            name: "BlueLake".into(),
            program: Some("opencode".into()),
            model: Some("opencode".into()),
            task_description: Some("mailbox agent".into()),
            persona: None,
        }
    }

    #[test]
    fn test_d_tag_format() {
        assert_eq!(profile_d_tag("demo", "BlueLake"), "demo:BlueLake");
        assert_eq!(profile_name_from_d_tag("demo:BlueLake"), Some("BlueLake"));
        assert_eq!(profile_name_from_d_tag("no-colon"), None);
    }

    #[test]
    fn test_create_agent_identity_always_different_npub() {
        let p = sample_profile();
        let a = create_agent_identity("demo", &p, ts()).expect("create a");
        let b = create_agent_identity("demo", &p, ts()).expect("create b");
        assert_ne!(
            a.keys.public_key(),
            b.keys.public_key(),
            "create_agent_identity must always mint a fresh keypair"
        );
        assert_ne!(a.event.pubkey, b.event.pubkey);
        assert_eq!(a.event.kind.as_u16(), AGENT_PROFILE);
    }

    #[test]
    fn test_register_agent_same_name_same_npub() {
        let p = sample_profile();
        let existing = crate::keys::generate();
        let a = register_agent("demo", &p, Some(&existing), ts()).expect("register a");
        let b = register_agent("demo", &p, Some(&existing), ts()).expect("register b");
        assert_eq!(a.keys.public_key(), existing.public_key());
        assert_eq!(a.event.pubkey, b.event.pubkey, "upsert keeps the same npub");
    }

    #[test]
    fn test_register_agent_creates_when_absent() {
        let p = sample_profile();
        let a = register_agent("demo", &p, None, ts()).expect("register absent");
        let b = register_agent("demo", &p, None, ts()).expect("register absent 2");
        assert_ne!(
            a.keys.public_key(),
            b.keys.public_key(),
            "absent → fresh keys"
        );
    }

    #[test]
    fn test_profile_event_has_d_tag_and_scoping() {
        let p = sample_profile();
        let r = create_agent_identity("demo", &p, ts()).expect("create");
        let ev = &r.event;

        // d-tag = project-slug:name
        assert!(
            ev.tags
                .iter()
                .any(|t| { t.as_slice() == ["d".to_string(), "demo:BlueLake".to_string(),] })
        );

        // Dual-emit scoping: #t + NIP-32 label (kind-30013 is not hiding).
        assert!(
            ev.tags
                .iter()
                .any(|t| t.as_slice() == ["t".to_string(), "demo".to_string()])
        );
        assert!(ev
            .tags
            .iter()
            .any(|t| t.as_slice() == ["l".to_string(), "demo".to_string(), "toron".to_string()]));
        assert!(
            ev.tags
                .iter()
                .any(|t| t.as_slice() == ["L".to_string(), "toron".to_string()])
        );

        assert!(!is_metadata_hiding_kind(ev.kind));
    }

    #[test]
    fn test_profile_roundtrip_content() {
        let p = sample_profile();
        let r = create_agent_identity("demo", &p, ts()).expect("create");
        let parsed = parse_profile(&r.event).expect("parse");
        assert_eq!(parsed, p);
    }

    /// ADR-0005 P2.2 protocol guard (Review Log round 6 — the audit fix):
    /// harness/model live in 30013 CONTENT (`program`/`model`), NEVER as `t`
    /// tags. The ONLY `t` tag on a profile event is the project slug (project
    /// scoping, scoping.rs). Overloading `t` with harness/model/role would
    /// collide with project filters (false matches). This test is the
    /// forever-guard.
    #[test]
    fn profile_no_tag_overload() {
        let p = AgentProfile {
            name: "BlueLake".into(),
            program: Some("pi".into()),
            model: Some("claude-sonnet-4".into()),
            task_description: Some("profile catalog spawn".into()),
            persona: None,
        };
        let r = create_agent_identity("demo", &p, ts()).expect("create");
        let ev = &r.event;

        // 1. harness/model present in CONTENT (the content-map target).
        let parsed = parse_profile(ev).expect("parse profile content");
        assert_eq!(parsed.program.as_deref(), Some("pi"));
        assert_eq!(parsed.model.as_deref(), Some("claude-sonnet-4"));

        // 2. The ONLY `t` tag is the project slug. NO `t` tag carries
        // harness/model/role values (pi, claude-sonnet-4, oracle, ...).
        let t_tags: Vec<Vec<String>> = ev
            .tags
            .iter()
            .filter(|t| t.kind() == "t")
            .map(|t| t.as_slice().iter().map(ToString::to_string).collect())
            .collect();
        assert_eq!(
            t_tags,
            vec![vec!["t".to_string(), "demo".to_string()]],
            "t tag must be project-slug-only; harness/model must NOT overload it"
        );

        // 3. Nothing resembling harness/model/role leaks into ANY tag value.
        for tag in ev.tags.iter() {
            for part in tag.as_slice() {
                let s = part.as_str();
                assert!(
                    ![
                        "pi",
                        "claude-sonnet-4",
                        "oracle",
                        "planner",
                        "reviewer",
                        "coder",
                        "scout",
                        "housekeeper"
                    ]
                    .contains(&s),
                    "tag value {s:?} overloads profile fields onto the bus"
                );
            }
        }
    }

    /// Persona pack fields round-trip through the kind-30013 profile.
    #[test]
    fn test_persona_pack_roundtrip() {
        let mut p = sample_profile();
        p.persona = Some(PersonaPack {
            version: "1.0.0".into(),
            name: "researcher".into(),
            system_prompt: "You are a rigorous researcher. Verify before claiming.".into(),
        });
        let r = create_agent_identity("demo", &p, ts()).expect("create");
        let parsed = parse_profile(&r.event).expect("parse");
        assert_eq!(parsed, p);
        let persona = parsed.persona.expect("persona present");
        assert_eq!(persona.version, "1.0.0");
        assert_eq!(persona.name, "researcher");
        assert!(persona.system_prompt.contains("rigorous researcher"));
        // The signed profile still verifies.
        assert!(crate::nostr::event::verify_event(&r.event).is_ok());
    }

    /// Backward-compatible parse: a profile without persona fields loads fine.
    #[test]
    fn test_persona_optional_absent() {
        let p = sample_profile(); // persona: None
        assert!(p.persona.is_none());
        let r = create_agent_identity("demo", &p, ts()).expect("create");
        let parsed = parse_profile(&r.event).expect("parse");
        assert!(parsed.persona.is_none());
    }

    #[test]
    fn test_resolve_name_to_npub() {
        let p = sample_profile();
        let blue = create_agent_identity("demo", &p, ts()).expect("create blue");
        let red = create_agent_identity("demo", &sample_profile_with_name("RoseDune"), ts())
            .expect("create red");

        let profiles = [&blue.event, &red.event];
        assert_eq!(
            resolve_name("demo", profiles.iter().copied(), "BlueLake"),
            Some(blue.event.pubkey)
        );
        assert_eq!(
            resolve_name("demo", profiles.iter().copied(), "RoseDune"),
            Some(red.event.pubkey)
        );
        assert_eq!(
            resolve_name("demo", profiles.iter().copied(), "Ghost"),
            None
        );
        // Wrong project d-tag does not match.
        let other = create_agent_identity("other", &p, ts()).expect("create other");
        assert_eq!(
            resolve_name("demo", [&other.event].into_iter(), "BlueLake"),
            None,
            "project-scoped name must not leak across projects"
        );
        assert_eq!(
            resolve_name("other", [&other.event].into_iter(), "BlueLake"),
            Some(other.event.pubkey),
            "name resolves inside its own project"
        );
    }

    #[test]
    fn test_profile_json_wire_identical() {
        let p = sample_profile();
        let r = create_agent_identity("demo", &p, ts()).expect("create");
        let json = profile_json(&r.event);
        let parsed = Event::from_json(json).expect("parse");
        assert_eq!(parsed, r.event);
    }

    /// bd-0pcd: kind-30013 is a NIP-01 parameterized REPLACEABLE event —
    /// after a key rotation both the old and the new profile sit in the
    /// archive under the same d tag, and first-match resolution serves the
    /// DEAD npub to whois + gift-wrap (inbox starves while send reports
    /// success). The resolver must pick the LATEST event per d tag,
    /// regardless of replay order.
    #[test]
    fn resolve_name_prefers_latest_replaceable_event() {
        let old = create_agent_identity("demo", &sample_profile(), ts()).expect("old");
        let new = create_agent_identity("demo", &sample_profile(), Timestamp::from(TS + 60))
            .expect("new");

        // Replay order A: oldest first (rumor-time order).
        assert_eq!(
            resolve_name("demo", [&old.event, &new.event], "BlueLake"),
            Some(new.event.pubkey),
            "latest profile wins over the stale one"
        );
        // Replay order B: newest first.
        assert_eq!(
            resolve_name("demo", [&new.event, &old.event], "BlueLake"),
            Some(new.event.pubkey),
            "order-independent: latest wins either way"
        );
    }

    /// NIP-01: equal `created_at` on two 30013s under one d-tag keeps the
    /// lowest event id, not iterator order.
    #[test]
    fn resolve_name_equal_created_at_keeps_lowest_id() {
        let a = create_agent_identity("demo", &sample_profile(), ts()).expect("a");
        let b = create_agent_identity("demo", &sample_profile(), ts()).expect("b");
        let (low, high) = if a.event.id.to_hex() < b.event.id.to_hex() {
            (a, b)
        } else {
            (b, a)
        };
        assert_eq!(
            resolve_name("demo", [&high.event, &low.event], "BlueLake"),
            Some(low.event.pubkey),
            "lowest id wins on a created_at tie"
        );
        assert_eq!(
            resolve_name("demo", [&low.event, &high.event], "BlueLake"),
            Some(low.event.pubkey),
            "order-independent lowest id"
        );
    }

    fn sample_profile_with_name(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: None,
            model: None,
            task_description: None,
            persona: None,
        }
    }
}
