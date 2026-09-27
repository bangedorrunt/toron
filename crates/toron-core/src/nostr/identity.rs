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

/// bd-pinrule: an agent pin is CamelCase Adjective+Noun, and that is a
/// NAMESPACE rule, not a style rule.
///
/// WHY IT MATTERS, measured on this host. Identity is per-PROJECT: the same pane
/// holds a DIFFERENT key in each project it is registered in. `whois flywheel
/// Buffy` and `whois toron.dev Buffy` return two different npubs — and `Buffy` is
/// a single ordinary word, the kind of name two unrelated agents pick
/// independently. A name that is distinctive AND structured (`IvoryRidge`) is
/// what keeps two agents from colliding; a name like `Buffy` or `Agent1` cannot.
///
/// This is enforced at the REGISTRATION door, not at resolution: refusing to
/// read a legacy name would orphan every agent already registered, and the
/// misroute this prevents is a send-time problem, so a warn-at-create plus a
/// warn-at-send is where it belongs.
///
/// Project SLUGS are deliberately NOT covered — `demo`, `forge` and
/// `parity-product` are legitimate kebab-case project names, and conflating the
/// two vocabularies is how a naming rule gets ignored.
pub fn validate_agent_pin(name: &str) -> Result<(), String> {
    let reject = |why: &str| {
        Err(format!(
            "agent pin {name:?} is not CamelCase Adjective+Noun ({why}). Pins are a \
             NAMESPACE, not a label: identity is per-project, so the same agent \
             holds a different key in every project, and a name two agents can \
             pick independently WILL collide. Use e.g. IvoryRidge, EmberOtter, \
             TealMarlin. Project slugs are unaffected and stay kebab-case."
        ))
    };
    if name.is_empty() {
        return reject("empty");
    }
    if name.len() < 3 {
        return reject("too short to be two words");
    }
    // Alphabetic only, NOT alphanumeric: a digit makes the first "word" not a
    // word (`Ivory2Ridge`), and `Agent1` is exactly the kind of name two agents
    // pick independently. Each word must be Upper then lowercase(s).
    if !name.chars().all(|c| c.is_ascii_alphabetic()) {
        return reject("contains a non-alphabetic character — each word is Upper then lowercase");
    }
    // Exactly two Capitalised words. An uppercase begins a word when it is at
    // index 0 OR follows a lowercase — my first version only counted the
    // "follows a lowercase" case, so it scored EmberOtter as ONE word and
    // refused the pin I use myself. The fence caught it, which is the third
    // time this session that has paid for itself.
    let mut words = 0usize;
    let mut prev_lower = false;
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i == 0 || prev_lower {
                words += 1;
            }
            prev_lower = false;
        } else {
            prev_lower = true;
        }
    }
    if !name.ends_with(char::is_alphabetic) {
        return reject("ends mid-word");
    }
    // Each word is Upper then lower(s): NO two capitals may be adjacent, or the
    // first "word" is not a capitalised word but a typo. Without this,
    // `IVoryRidge` scores as exactly two words and is accepted.
    let mut prev_upper = false;
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            if prev_upper {
                return reject("two capitals in a row — each word is Upper then lowercase");
            }
            prev_upper = true;
        } else {
            prev_upper = false;
        }
    }
    if words != 2 {
        return reject(if words == 1 {
            "one word only — needs Adjective AND Noun"
        } else {
            "more than two words"
        });
    }
    Ok(())
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

/// Every project a NAME is registered under, with the key that project binds.
///
/// bd-xroute: `resolve_name` is correctly project-scoped, which means a name
/// can be a DIFFERENT identity in every project. Proven live on 2026-09-28:
/// `whois flywheel Buffy` -> `6778eb30...` and `whois toron.dev Buffy` ->
/// `71998e0b...` are the SAME PANE (w1W:p1) under two per-project keys. So
/// `mail send --to Buffy` from the flywheel project encrypts to a key the
/// target never reads, and the send reports `ok` — the message is invisible, not
/// merely undelivered. Address the same person by npub and it arrives.
///
/// This is the detector for that. It is deliberately a PURE scan over the same
/// events `resolve_name` reads, so a caller can warn before sending rather than
/// after the message is unreadable.
#[must_use]
pub fn name_across_projects<'a>(
    profiles: impl IntoIterator<Item = &'a Event>,
    name: &str,
) -> Vec<(String, PublicKey)> {
    let mut out: Vec<(String, PublicKey)> = Vec::new();
    for ev in profiles {
        if ev.kind.as_u16() != AGENT_PROFILE {
            continue;
        }
        // The d tag is "{project}:{name}" — `profile_d_tag` /
        // `profile_name_from_d_tag` are the ONLY authority on that shape, so
        // this reuses them instead of re-splitting. My first version split on
        // '/', which never matches, so it would have returned an empty list and
        // the warning would have been silently dead. A detector that cannot fire
        // is worse than none: it reads as coverage.
        let Some(d) = ev
            .tags
            .iter()
            .find_map(|t| (t.kind() == "d").then(|| t.content()).flatten())
        else {
            continue;
        };
        let Some((project, d_name)) = d.split_once(':') else {
            continue;
        };
        if d_name != name {
            continue;
        }
        let npub = profile_npub(ev);
        let project = project.to_string();
        if !out.iter().any(|(p, n)| *p == project && *n == npub) {
            out.push((project, npub));
        }
    }
    out
}

/// bd-xroute: the warning text, pure so a fence can read it. The same reasoning
/// as the toron-cli contradiction message: reaching this needs the live event
/// set, and the interesting property — that it NAMES the alternatives — is
/// checkable without any of that.
pub fn ambiguous_name_warning(
    name: &str,
    send_project: &str,
    binds: &[(String, PublicKey)],
) -> Option<String> {
    // The rule is deliberately symmetric. The sender CANNOT know which of the
    // bound identities the target actually reads under, so a name bound
    // anywhere other than the sending project is worth a warning in BOTH
    // directions. An earlier version tried to be clever — "warn unless the
    // send project is the bound one" — and its own fence proved it wrong: from
    // toron.dev, "Buffy" resolves to the toron.dev key, yet the person at that
    // pane may be the flywheel Buffy and never see the message. Guessing which
    // identity is "really" them is exactly the inference this warning exists to
    // replace with a fact.
    let others: Vec<&(String, PublicKey)> =
        binds.iter().filter(|(p, _)| p != send_project).collect();
    if others.is_empty() {
        return None;
    }
    let mut keys: Vec<String> = others
        .iter()
        .map(|(p, n)| format!("{p}={}", n.to_hex()))
        .collect();
    keys.sort();
    Some(format!(
        "WARNING: {name:?} is bound in {} as a DIFFERENT key from the one this send uses. \
         Mail is encrypted per project, so if the recipient reads under {}'s identity this \
         message will be UNREADABLE to them — it is not lost in transit, it is invisible. \
         Cross-project delivery must address the npub directly. Other bindings: {}",
        keys.len(),
        keys.join(", "),
        keys.join(", ")
    ))
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

#[cfg(test)]
mod bd_xroute_tests {
    use super::*;

    /// Builds the profile event through the PRODUCTION builder
    /// (`build_profile_event_with_roles`), not a hand-rolled struct.
    ///
    /// bd-xroute: my first two attempts hand-built the `Event` and both were
    /// wrong — one hit `non-exhaustive` and one invented a d-tag shape the wire
    /// never carries. A test fixture that does not use the real builder is a
    /// fixture that tests a fiction, and here the fiction was silently EMPTY
    /// (the wrong split matched nothing, so the detector "passed" by never
    /// firing). The real builder is also shorter.
    fn profile_event(project: &str, name: &str, keys: &crate::nostr::Keys) -> Event {
        let profile = AgentProfile {
            name: name.to_string(),
            program: Some("opencode".into()),
            model: None,
            task_description: None,
            persona: None,
        };
        build_profile_event_with_roles(
            keys,
            project,
            &profile,
            &[],
            ::nostr::types::Timestamp::from(1_700_000_000u64),
        )
        .expect("build the profile event")
    }

    fn keys() -> crate::nostr::Keys {
        crate::nostr::Keys::generate()
    }

    /// bd-xroute: the detector finds the same NAME under two projects with two
    /// DIFFERENT keys, which is the live Buffy case.
    #[test]
    fn a_name_bound_in_two_projects_is_reported_with_both_keys() {
        let events = vec![
            profile_event("flywheel", "Buffy", &keys()),
            profile_event("toron.dev", "Buffy", &keys()),
        ];
        // The npub comes from the EVENT, which is the only authority the
        // detector itself uses — deriving it from the builder's key struct
        // would couple the fence to a field name this crate does not own.
        let (npub_a, npub_b) = (profile_npub(&events[0]), profile_npub(&events[1]));
        assert_ne!(npub_a, npub_b, "the fixture needs two distinct identities");
        let binds = name_across_projects(&events, "Buffy");
        assert_eq!(binds.len(), 2, "both bindings must be found: {binds:?}");
        assert!(binds.contains(&("flywheel".to_string(), npub_a)));
        assert!(binds.contains(&("toron.dev".to_string(), npub_b)));
    }

    /// And the warning NAMES the other binding, because a warning that says
    /// "ambiguous" without saying where else cannot be acted on.
    #[test]
    fn the_warning_names_the_other_project_and_key() {
        let npub_a = profile_npub(&profile_event("flywheel", "Buffy", &keys()));
        let npub_b = profile_npub(&profile_event("toron.dev", "Buffy", &keys()));
        let binds = vec![
            ("flywheel".to_string(), npub_a),
            ("toron.dev".to_string(), npub_b),
        ];
        let w = ambiguous_name_warning("Buffy", "flywheel", &binds)
            .expect("sending from flywheel to a name bound in toron.dev is ambiguous");
        assert!(w.contains("Buffy"), "names the recipient: {w}");
        assert!(w.contains("toron.dev"), "names the other project: {w}");
        assert!(
            w.contains(&npub_b.to_hex()),
            "names the other KEY, which is what the operator must address: {w}"
        );
        assert!(w.contains("npub"), "says what to do instead: {w}");
    }

    /// A name bound in ONE project is not ambiguous, and the warning must stay
    /// silent — otherwise every ordinary send prints noise nobody reads.
    #[test]
    fn a_single_project_binding_warns_about_nothing() {
        let npub_a = profile_npub(&profile_event("flywheel", "PeachRobin", &keys()));
        let binds = vec![("flywheel".to_string(), npub_a)];
        assert_eq!(
            ambiguous_name_warning("PeachRobin", "flywheel", &binds),
            None,
            "an ordinary single-project recipient must not produce a warning"
        );
    }

    /// The warning fires in BOTH directions, and that is the point.
    ///
    /// I first wrote the rule asymmetrically — "warn unless the sending project
    /// is the bound one" — on the reasoning that a caller bound to toron.dev IS
    /// that Buffy. The fixture disproved it: the same PANE (w1W:p1) holds a key
    /// in each project, so "the caller is that Buffy" is not knowable from the
    /// send site. Only the operator can resolve it, and the warning is what
    /// tells them.
    #[test]
    fn the_warning_is_symmetric_because_the_sender_cannot_who_the_target_is() {
        let npub_a = profile_npub(&profile_event("flywheel", "Buffy", &keys()));
        let npub_b = profile_npub(&profile_event("toron.dev", "Buffy", &keys()));
        let binds = vec![
            ("flywheel".to_string(), npub_a),
            ("toron.dev".to_string(), npub_b),
        ];
        for from in ["flywheel", "toron.dev"] {
            let w = ambiguous_name_warning("Buffy", from, &binds).unwrap_or_else(|| {
                panic!("sending from {from} must warn: the name is bound elsewhere")
            });
            assert!(w.contains("Buffy"), "{from}: names the recipient: {w}");
            assert!(
                w.contains("UNREADABLE"),
                "{from}: must say the consequence is invisibility, not loss: {w}"
            );
        }
    }
}

#[cfg(test)]
mod bd_pinrule_tests {
    use super::validate_agent_pin;

    /// The live set. Every one of these is a real pin in use, which is the
    /// point: the rule has to accept the names people actually chose.
    #[test]
    fn the_pins_in_live_use_are_accepted() {
        for pin in [
            "EmberOtter",
            "PeachRobin",
            "RoseCoyote",
            "MaroonDove",
            "AmberQuill",
            "IvoryRidge",
            "TealMarlin",
            "BlueLake",
            "ScarletOwl",
        ] {
            assert!(
                validate_agent_pin(pin).is_ok(),
                "{pin} is a pin in live use and must be accepted: {:?}",
                validate_agent_pin(pin)
            );
        }
    }

    /// `Buffy` is the live counter-example: a single ordinary word, registered
    /// in two projects, and it resolved to TWO different npubs — the misroute
    /// bd-xroute warns about. So the refusal has to name that shape.
    #[test]
    fn a_single_word_is_refused_and_the_reason_is_actionable() {
        let e = validate_agent_pin("Buffy").expect_err("one word is not AdjNoun");
        assert!(e.contains("Adjective"), "names the shape: {e}");
        assert!(
            e.contains("IvoryRidge") || e.contains("EmberOtter"),
            "offers a conforming example, or the operator cannot act: {e}"
        );
    }

    #[test]
    fn the_shapes_that_collide_are_all_refused() {
        for bad in [
            "Agent1",          // word + digit
            "agent",           // lowercase
            "ivoryridge",      // lowercase joined
            "IVoryRidge",      // two capitals in a row
            "AB",              // shouty
            "EmberOTTER",      // shouty tail
            "Ivory-Ridge",     // kebab
            "Ivory_Ridge",     // snake
            "Blue",            // adjective only
            "Ridge",           // noun only
            "IvoryRidgeCreek", // three words
            "",                // empty
            "Ab",              // too short
            "Ivory2Ridge",     // digit inside
        ] {
            assert!(
                validate_agent_pin(bad).is_err(),
                "{bad:?} must be refused: a name two agents can pick independently WILL collide"
            );
        }
    }

    /// Project slugs are a DIFFERENT vocabulary and must be unaffected —
    /// otherwise the rule gets ignored as obviously wrong.
    #[test]
    fn the_refusal_does_not_claim_to_cover_project_slugs() {
        let e = validate_agent_pin("demo").expect_err("not a pin shape");
        assert!(
            e.contains("Project slugs"),
            "must scope itself to pins so `demo`/`forge` stay legal project names: {e}"
        );
    }
}
