// governed-by: ADR-0027
#![forbid(unsafe_code)]

//! Typed vocabulary for `SendMessage.extra_tags` (ADR-0027 E2/E3).
//!
//! Tags ride rumor kind 14 and job 43001 builder; NEVER the sealed 1059
//! envelope. Vocabulary: `corr`, `wake(standing|tasked)`,
//! `priority(queue|interrupt)`, `audience(human|agents)`, `result_path`.
//! Parsed at the boundary into typed structs; unknown wake/priority/audience
//! values are hard errors (type-system-discipline).

use crate::mailbox::error::{MailError, MailErrorCode};
use crate::nostr::Tag;

/// Wake assignment kind (E3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Wake {
    Standing,
    Tasked,
}

impl Wake {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standing => "standing",
            Self::Tasked => "tasked",
        }
    }
    pub fn parse(s: &str) -> Result<Self, MailError> {
        match s {
            "standing" => Ok(Self::Standing),
            "tasked" => Ok(Self::Tasked),
            _ => Err(MailError::new(
                MailErrorCode::Malformed,
                format!("unknown wake value {s:?} — expected standing|tasked"),
            )),
        }
    }
}

impl std::fmt::Display for Wake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Wake {
    type Err = MailError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Priority (queue = default tasked, interrupt = operator/user barge-in).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Priority {
    Queue,
    Interrupt,
}

impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Interrupt => "interrupt",
        }
    }
    pub fn parse(s: &str) -> Result<Self, MailError> {
        match s {
            "queue" => Ok(Self::Queue),
            "interrupt" => Ok(Self::Interrupt),
            _ => Err(MailError::new(
                MailErrorCode::Malformed,
                format!("unknown priority value {s:?} — expected queue|interrupt"),
            )),
        }
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Priority {
    type Err = MailError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Audience of the assignment (E3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Audience {
    Human,
    Agents,
}

impl Audience {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Human => "human",
            Self::Agents => "agents",
        }
    }
    pub fn parse(s: &str) -> Result<Self, MailError> {
        match s {
            "human" => Ok(Self::Human),
            "agents" => Ok(Self::Agents),
            _ => Err(MailError::new(
                MailErrorCode::Malformed,
                format!("unknown audience value {s:?} — expected human|agents"),
            )),
        }
    }
}

impl std::fmt::Display for Audience {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Audience {
    type Err = MailError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Typed projection of the extra_tags vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SendTags {
    pub corr: Option<String>,
    pub wake: Option<Wake>,
    pub priority: Option<Priority>,
    pub audience: Option<Audience>,
    pub result_path: Option<String>,
}

impl SendTags {
    /// Build from explicit fields (CLI/MCP boundary). Validates values are
    /// known (typed) — callers pass typed enums already, so this is infallible
    /// except for corr/result_path emptiness.
    pub fn from_fields(
        corr: Option<String>,
        wake: Option<Wake>,
        priority: Option<Priority>,
        audience: Option<Audience>,
        result_path: Option<String>,
    ) -> Result<Self, MailError> {
        if let Some(c) = &corr
            && c.trim().is_empty()
        {
            return Err(MailError::new(
                MailErrorCode::Malformed,
                "corr must be non-empty when present",
            ));
        }
        if let Some(r) = &result_path
            && r.trim().is_empty()
        {
            return Err(MailError::new(
                MailErrorCode::Malformed,
                "result_path must be non-empty when present",
            ));
        }
        Ok(Self {
            corr,
            wake,
            priority,
            audience,
            result_path,
        })
    }

    /// Parse the raw `extra_tags` vector into typed `SendTags`. Rejects unknown
    /// wake/priority/audience loudly; ignores non-vocabulary tags (e.g.
    /// workflow-emitted). Duplicate vocabulary keys: last wins (deterministic).
    pub fn parse(extra_tags: &[Tag]) -> Result<Self, MailError> {
        let mut out = Self::default();
        for tag in extra_tags {
            let parts = tag.as_slice();
            if parts.is_empty() {
                continue;
            }
            match parts[0].as_str() {
                "corr" => {
                    let v = parts.get(1).cloned().unwrap_or_default();
                    if v.trim().is_empty() {
                        return Err(MailError::new(
                            MailErrorCode::Malformed,
                            "corr tag value must be non-empty",
                        ));
                    }
                    out.corr = Some(v);
                }
                "wake" => {
                    let v = parts.get(1).map(|s| s.as_str()).unwrap_or("");
                    out.wake = Some(Wake::parse(v)?);
                }
                "priority" => {
                    let v = parts.get(1).map(|s| s.as_str()).unwrap_or("");
                    out.priority = Some(Priority::parse(v)?);
                }
                "audience" => {
                    let v = parts.get(1).map(|s| s.as_str()).unwrap_or("");
                    out.audience = Some(Audience::parse(v)?);
                }
                "result_path" => {
                    let v = parts.get(1).cloned().unwrap_or_default();
                    if v.trim().is_empty() {
                        return Err(MailError::new(
                            MailErrorCode::Malformed,
                            "result_path tag value must be non-empty",
                        ));
                    }
                    out.result_path = Some(v);
                }
                _ => {
                    // Non-vocabulary tags (workflow-emitted, etc.) are ignored
                    // here — they ride the same extra_tags vehicle.
                }
            }
        }
        Ok(out)
    }

    /// Validate E3 invariants that require recipient context:
    /// - audience=human fan-out to >1 recipient is a hard error
    /// - interrupt priority is operator/user only (requires overseer)
    /// - interrupt never onto human pin
    pub fn validate_for_send(
        &self,
        recipient_count: usize,
        overseer: bool,
    ) -> Result<(), MailError> {
        if let Some(Audience::Human) = self.audience
            && recipient_count > 1
        {
            return Err(MailError::new(
                MailErrorCode::Malformed,
                format!(
                    "audience=human fan-out to {} recipients is a hard error (E3): human audience addresses one orchestrator pin",
                    recipient_count
                ),
            ));
        }
        if let Some(Priority::Interrupt) = self.priority {
            if !overseer {
                return Err(MailError::new(
                    MailErrorCode::PolicyDenied,
                    "priority=interrupt is operator/user only (requires overseer)",
                ));
            }
            if let Some(Audience::Human) = self.audience {
                return Err(MailError::new(
                    MailErrorCode::Malformed,
                    "priority=interrupt never onto human pin (E3)",
                ));
            }
        }
        Ok(())
    }

    /// Serialize to `Vec<Tag>` for rumor / job builder. Only present fields
    /// emit. Vocabulary tags are NEVER placed on the sealed 1059 envelope —
    /// caller must only extend the rumor (kind 14) and job 43001 tags.
    pub fn to_tags(&self) -> Vec<Tag> {
        let mut out = Vec::new();
        if let Some(c) = &self.corr
            && let Ok(t) = Tag::parse(["corr", c])
        {
            out.push(t);
        }
        if let Some(w) = self.wake
            && let Ok(t) = Tag::parse(["wake", w.as_str()])
        {
            out.push(t);
        }
        if let Some(p) = self.priority
            && let Ok(t) = Tag::parse(["priority", p.as_str()])
        {
            out.push(t);
        }
        if let Some(a) = self.audience
            && let Ok(t) = Tag::parse(["audience", a.as_str()])
        {
            out.push(t);
        }
        if let Some(r) = &self.result_path
            && let Ok(t) = Tag::parse(["result_path", r])
        {
            out.push(t);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(k: &str, v: &str) -> Tag {
        Tag::parse([k, v]).expect("tag")
    }

    #[test]
    fn send_tags_parse_roundtrip() {
        let tags = vec![
            tag("corr", "FM-GS-012"),
            tag("wake", "tasked"),
            tag("priority", "queue"),
            tag("audience", "agents"),
            tag("result_path", "/tmp/result.json"),
        ];
        let parsed = SendTags::parse(&tags).expect("parse");
        assert_eq!(parsed.corr.as_deref(), Some("FM-GS-012"));
        assert_eq!(parsed.wake, Some(Wake::Tasked));
        assert_eq!(parsed.priority, Some(Priority::Queue));
        assert_eq!(parsed.audience, Some(Audience::Agents));
        assert_eq!(parsed.result_path.as_deref(), Some("/tmp/result.json"));
        let rebuilt = parsed.to_tags();
        let reparsed = SendTags::parse(&rebuilt).expect("reparse");
        assert_eq!(reparsed, parsed);
    }

    #[test]
    fn send_tags_parse_rejects_unknown_wake() {
        let tags = vec![tag("wake", "bogus")];
        let err = SendTags::parse(&tags).expect_err("unknown wake must error");
        assert!(err.message.contains("unknown wake"), "{err}");
        assert_eq!(err.code, MailErrorCode::Malformed);
    }

    #[test]
    fn send_tags_parse_rejects_unknown_priority() {
        let tags = vec![tag("priority", "fast")];
        let err = SendTags::parse(&tags).expect_err("unknown priority must error");
        assert!(err.message.contains("unknown priority"), "{err}");
    }

    #[test]
    fn send_tags_parse_rejects_unknown_audience() {
        let tags = vec![tag("audience", "everyone")];
        let err = SendTags::parse(&tags).expect_err("unknown audience must error");
        assert!(err.message.contains("unknown audience"), "{err}");
    }

    #[test]
    fn send_tags_validate_human_fanout_hard_error() {
        let tags = SendTags {
            audience: Some(Audience::Human),
            ..Default::default()
        };
        let err = tags
            .validate_for_send(2, false)
            .expect_err("human fan-out >1 must error");
        assert!(err.message.contains("fan-out"), "{err}");
    }

    #[test]
    fn send_tags_validate_interrupt_requires_overseer() {
        let tags = SendTags {
            priority: Some(Priority::Interrupt),
            ..Default::default()
        };
        let err = tags
            .validate_for_send(1, false)
            .expect_err("interrupt without overseer must error");
        assert!(err.message.contains("operator"), "{err}");
    }

    #[test]
    fn send_tags_validate_interrupt_never_human() {
        let tags = SendTags {
            priority: Some(Priority::Interrupt),
            audience: Some(Audience::Human),
            ..Default::default()
        };
        let err = tags
            .validate_for_send(1, true)
            .expect_err("interrupt onto human must error");
        assert!(err.message.contains("never onto human"), "{err}");
    }

    #[test]
    fn send_tags_ignores_non_vocabulary() {
        let tags = vec![
            tag("toron:workflow-emitted", "1"),
            tag("corr", "X"),
            Tag::parse(["p", "abcd"]).unwrap(),
        ];
        let parsed = SendTags::parse(&tags).expect("parse");
        assert_eq!(parsed.corr.as_deref(), Some("X"));
        assert_eq!(parsed.wake, None);
    }

    #[test]
    fn send_tags_duplicate_last_wins() {
        let tags = vec![tag("wake", "standing"), tag("wake", "tasked")];
        let parsed = SendTags::parse(&tags).expect("parse");
        assert_eq!(parsed.wake, Some(Wake::Tasked));
    }

    // ---- mailbox-bound send_tags integration (E2/E3) ----

    #[tokio::test]
    async fn send_tags_mail_rumor_carries_and_not_envelope() {
        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index::schema;
        use crate::mailbox::{Mailbox, SendMessage};
        use crate::nostr::identity::AgentProfile;
        use std::sync::Arc;

        let db_path = std::env::temp_dir().join(format!(
            "toron-send-tags-rumor-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-send-tags-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_agent_passphrase("x");
        let profile = |n: &str| AgentProfile {
            name: n.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        };
        let _sender = mailbox
            .register_agent("demo", &profile("SenderOne"))
            .await
            .expect("register sender");
        let _recv = mailbox
            .register_agent("demo", &profile("RecvOne"))
            .await
            .expect("register recv");
        let recv_keys = mailbox
            .load_agent_key(std::path::Path::new(&_recv.key_path))
            .expect("key");
        // Build typed tags
        let tags = SendTags {
            corr: Some("FM-GS-012".into()),
            wake: Some(Wake::Tasked),
            priority: Some(Priority::Queue),
            audience: Some(Audience::Agents),
            result_path: Some("/tmp/out.json".into()),
        };
        let extra = tags.to_tags();
        let msg = SendMessage {
            project_slug: "demo".into(),
            sender_name: "SenderOne".into(),
            to: vec!["RecvOne".into()],
            subject: "work".into(),
            body_md: "do it".into(),
            ..Default::default()
        };
        let mut msg = msg;
        msg.extra_tags = extra.clone();
        mailbox.send_message(&msg).await.expect("send with tags");
        // Rumor (kind 14) carries tags: verify via unwrap
        let filter = crate::bus::filter::ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(recv_keys.public_key())
            .build();
        let wraps = crate::mailbox::collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("replay");
        assert_eq!(wraps.len(), 1, "one wrap");
        let gift = crate::nostr::gift_wrap::unwrap(&recv_keys, &wraps[0]).expect("unwrap");
        let rumor_tags = &gift.rumor.tags;
        assert!(
            rumor_tags
                .iter()
                .any(|t| t.as_slice() == ["corr", "FM-GS-012"]),
            "rumor has corr"
        );
        assert!(
            rumor_tags
                .iter()
                .any(|t| t.as_slice() == ["wake", "tasked"])
        );
        assert!(
            rumor_tags
                .iter()
                .any(|t| t.as_slice() == ["priority", "queue"])
        );
        assert!(
            rumor_tags
                .iter()
                .any(|t| t.as_slice() == ["audience", "agents"])
        );
        assert!(
            rumor_tags
                .iter()
                .any(|t| t.as_slice() == ["result_path", "/tmp/out.json"])
        );
        // Envelope (kind 1059) MUST NOT carry vocabulary — p-gated, no corr/wake
        assert!(
            !wraps[0]
                .tags
                .iter()
                .any(|t| t.as_slice().first().map(|s| s.as_str()) == Some("corr")),
            "envelope must not have corr"
        );
        assert!(
            !wraps[0]
                .tags
                .iter()
                .any(|t| t.as_slice().first().map(|s| s.as_str()) == Some("wake")),
            "envelope must not have wake"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn send_tags_mail_human_fanout_rejected() {
        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index::schema;
        use crate::mailbox::{Mailbox, SendMessage};
        use crate::nostr::identity::AgentProfile;
        use std::sync::Arc;

        let db_path = std::env::temp_dir().join(format!(
            "toron-send-tags-fanout-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-send-tags-fanout-data-{}",
            uuid::Uuid::new_v4()
        ));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_agent_passphrase("x");
        let p = |n: &str| AgentProfile {
            name: n.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        };
        mailbox
            .register_agent("demo", &p("SenderOne"))
            .await
            .expect("sender");
        mailbox
            .register_agent("demo", &p("RecvOne"))
            .await
            .expect("recv1");
        mailbox
            .register_agent("demo", &p("RecvTwo"))
            .await
            .expect("recv2");
        let tags = SendTags {
            audience: Some(Audience::Human),
            ..Default::default()
        }
        .to_tags();
        let msg = SendMessage {
            project_slug: "demo".into(),
            sender_name: "SenderOne".into(),
            to: vec!["RecvOne".into(), "RecvTwo".into()],
            subject: "hi".into(),
            body_md: "body".into(),
            extra_tags: tags,
            ..Default::default()
        };
        let err = mailbox
            .send_message(&msg)
            .await
            .expect_err("human fan-out must error");
        assert!(
            err.to_string().contains("fan-out") || err.to_string().contains("human"),
            "{err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn send_tags_mail_interrupt_requires_overseer() {
        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index::schema;
        use crate::mailbox::{Mailbox, SendMessage};
        use crate::nostr::identity::AgentProfile;
        use std::sync::Arc;
        let db_path = std::env::temp_dir().join(format!(
            "toron-send-tags-interrupt-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-send-tags-interrupt-data-{}",
            uuid::Uuid::new_v4()
        ));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_agent_passphrase("x");
        let p = |n: &str| AgentProfile {
            name: n.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        };
        mailbox
            .register_agent("demo", &p("SenderOne"))
            .await
            .expect("sender");
        mailbox
            .register_agent("demo", &p("RecvOne"))
            .await
            .expect("recv");
        let tags = SendTags {
            priority: Some(Priority::Interrupt),
            ..Default::default()
        }
        .to_tags();
        let msg = SendMessage {
            project_slug: "demo".into(),
            sender_name: "SenderOne".into(),
            to: vec!["RecvOne".into()],
            subject: "hi".into(),
            body_md: "body".into(),
            extra_tags: tags,
            overseer: false,
            ..Default::default()
        };
        let err = mailbox
            .send_message(&msg)
            .await
            .expect_err("interrupt without overseer must error");
        assert!(
            err.to_string().contains("operator") || err.to_string().contains("interrupt"),
            "{err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn send_tags_mail_unknown_wake_rejected() {
        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index::schema;
        use crate::mailbox::{Mailbox, SendMessage};
        use crate::nostr::identity::AgentProfile;
        use std::sync::Arc;
        let db_path = std::env::temp_dir().join(format!(
            "toron-send-tags-unknown-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-send-tags-unknown-data-{}",
            uuid::Uuid::new_v4()
        ));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_agent_passphrase("x");
        let p = |n: &str| AgentProfile {
            name: n.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        };
        mailbox
            .register_agent("demo", &p("SenderOne"))
            .await
            .expect("sender");
        mailbox
            .register_agent("demo", &p("RecvOne"))
            .await
            .expect("recv");
        let bad = crate::nostr::Tag::parse(["wake", "bogus"]).expect("tag");
        let msg = SendMessage {
            project_slug: "demo".into(),
            sender_name: "SenderOne".into(),
            to: vec!["RecvOne".into()],
            subject: "hi".into(),
            body_md: "body".into(),
            extra_tags: vec![bad],
            ..Default::default()
        };
        let err = mailbox
            .send_message(&msg)
            .await
            .expect_err("unknown wake must error");
        assert!(err.to_string().contains("unknown wake"), "{err}");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
