// governed-by: ADR-0003, ADR-0020
//! `#seq` board/channel facade over Toron's existing bus + SQLite identity
//! store (`D4/R2`). Room-class traffic posts plaintext signed `kind 9` events
//! directly to the bus (zero NIP-17 gift-wrap), while secret-class bodies are
//! refused so callers route them through sealed NIP-17 (`toron mail send`).

use std::time::Duration;

use nostr::prelude::{Event, Tag, Timestamp};
use serde::{Deserialize, Serialize};

use crate::bus::{Filter, SubscriptionEvent};
use crate::nostr::event::build_event;
use crate::nostr::kinds::toron_kind;

use super::Mailbox;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BoardRefusal {
    #[error(
        "secret-class traffic ({reason}) is refused on board_post; use sealed NIP-17 mail (`toron mail send`)"
    )]
    SecretClassRequiresSealedMail { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardMessage {
    pub seq: u64,
    pub event_id: String,
    pub project: String,
    pub channel: String,
    pub author: String,
    pub body: String,
    pub mentions: Vec<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardReadResult {
    pub project: String,
    pub channel: String,
    pub head_seq: u64,
    pub messages: Vec<BoardMessage>,
}

/// Refuse secret/private/BCC-class traffic on the plaintext `#seq` board.
pub fn classify_board_body(body: &str, sensitivity: Option<&str>) -> Result<(), BoardRefusal> {
    if let Some(level) = sensitivity {
        let lower = level.trim().to_ascii_lowercase();
        if matches!(lower.as_str(), "secret" | "private" | "bcc") {
            return Err(BoardRefusal::SecretClassRequiresSealedMail {
                reason: "sensitivity label",
            });
        }
    }
    const SECRET_MARKERS: &[&str] = &["nsec1", "-----BEGIN ", "Bearer exp_", "sk-ant-", "sk-live-"];
    if SECRET_MARKERS.iter().any(|m| body.contains(m)) {
        return Err(BoardRefusal::SecretClassRequiresSealedMail {
            reason: "secret material in body",
        });
    }
    Ok(())
}

fn event_tag<'a>(ev: &'a Event, name: &str) -> Option<&'a str> {
    ev.tags
        .iter()
        .find(|t| t.as_slice().first().map(String::as_str) == Some(name))
        .and_then(|t| t.as_slice().get(1).map(String::as_str))
}

fn event_mentions(ev: &Event) -> Vec<String> {
    ev.tags
        .iter()
        .filter(|t| t.as_slice().first().map(String::as_str) == Some("mention"))
        .filter_map(|t| t.as_slice().get(1).cloned())
        .collect()
}

fn matches_board_event(ev: &Event, project: &str, channel: &str) -> bool {
    event_tag(ev, "toron-kind") == Some("board-post")
        && event_tag(ev, "t") == Some(project)
        && event_tag(ev, "h") == Some(channel)
}

impl Mailbox {
    /// Post a room-class message to `#seq` board (`kind 9`, no NIP-17 gift-wrap).
    pub async fn board_post(
        &self,
        project: &str,
        channel: &str,
        author: &str,
        body: &str,
        mentions: &[String],
        sensitivity: Option<&str>,
    ) -> anyhow::Result<BoardMessage> {
        classify_board_body(body, sensitivity)?;
        let canonical = self.canonical_project(project).await;
        let author_keys = self.agent_keys_for_resolved(&canonical, author).await?;
        let mut tags = vec![
            Tag::parse(["t", &canonical]).expect("t tag"),
            Tag::parse(["h", channel]).expect("h tag"),
            Tag::parse(["toron-kind", "board-post"]).expect("toron-kind"),
            Tag::parse(["from", author]).expect("from tag"),
        ];
        for mention in mentions {
            tags.push(Tag::parse(["mention", mention]).expect("mention tag"));
            if let Ok(keys) = self.agent_keys_for_resolved(&canonical, mention).await {
                tags.push(Tag::parse(["p", &keys.public_key().to_hex()]).expect("p tag"));
            }
        }
        let ev = build_event(
            &author_keys,
            toron_kind(9),
            body.to_string(),
            tags,
            Timestamp::now(),
        )
        .map_err(|e| anyhow::anyhow!("build board_post event: {e}"))?;
        let event_id = ev.id.to_hex();
        self.bus
            .publish(ev)
            .await
            .map_err(|e| anyhow::anyhow!("publish board_post: {e}"))?;
        let read = self.board_read(&canonical, channel, 0, usize::MAX).await?;
        read.messages
            .into_iter()
            .find(|m| m.event_id == event_id)
            .ok_or_else(|| anyhow::anyhow!("posted board message {event_id} not visible on replay"))
    }

    /// Read room-class `#seq` board messages for `(project, channel)` after `since_seq`.
    pub async fn board_read(
        &self,
        project: &str,
        channel: &str,
        since_seq: u64,
        limit: usize,
    ) -> anyhow::Result<BoardReadResult> {
        let canonical = self.canonical_project(project).await;
        let filter = Filter::new().kinds([toron_kind(9)]);
        let mut sub = self
            .bus
            .subscribe_trusted(filter)
            .await
            .map_err(|e| anyhow::anyhow!("board_read subscribe: {e}"))?;
        let mut events = Vec::new();
        while let Some(SubscriptionEvent::Event(ev)) = sub.next().await {
            if matches_board_event(&ev, &canonical, channel) {
                events.push(ev);
            }
        }
        let head_seq = events.len() as u64;
        let messages = events
            .into_iter()
            .enumerate()
            .map(|(idx, ev)| {
                let author = event_tag(&ev, "from").unwrap_or("unknown").to_string();
                let mentions = event_mentions(&ev);
                let created_at = ev.created_at.as_secs() as i64;
                BoardMessage {
                    seq: (idx as u64) + 1,
                    event_id: ev.id.to_hex(),
                    project: canonical.clone(),
                    channel: channel.to_string(),
                    author,
                    body: ev.content,
                    mentions,
                    created_at,
                }
            })
            .filter(|m| m.seq > since_seq)
            .take(limit)
            .collect();
        Ok(BoardReadResult {
            project: canonical,
            channel: channel.to_string(),
            head_seq,
            messages,
        })
    }

    /// Wait up to `timeout_ms` for `#seq > since_seq` on `(project, channel)`.
    pub async fn board_wait(
        &self,
        project: &str,
        channel: &str,
        since_seq: u64,
        mentions_only: bool,
        recipient: Option<&str>,
        timeout_ms: u64,
    ) -> anyhow::Result<BoardReadResult> {
        let canonical = self.canonical_project(project).await;
        let filter_mentions = |mut res: BoardReadResult| -> BoardReadResult {
            if mentions_only && let Some(pin) = recipient {
                res.messages.retain(|m| m.mentions.iter().any(|x| x == pin));
            }
            res
        };

        let initial = filter_mentions(
            self.board_read(&canonical, channel, since_seq, usize::MAX)
                .await?,
        );
        if !initial.messages.is_empty() || timeout_ms == 0 {
            return Ok(initial);
        }

        let mut sub = self
            .bus
            .subscribe_trusted(Filter::new().kinds([toron_kind(9)]))
            .await
            .map_err(|e| anyhow::anyhow!("board_wait subscribe: {e}"))?;
        let deadline = Duration::from_millis(timeout_ms.min(60_000));
        let _ = tokio::time::timeout(deadline, async {
            while let Some(item) = sub.next().await {
                if let SubscriptionEvent::Event(ev) = item
                    && matches_board_event(&ev, &canonical, channel)
                {
                    let candidate = filter_mentions(
                        self.board_read(&canonical, channel, since_seq, usize::MAX)
                            .await
                            .unwrap_or(BoardReadResult {
                                project: canonical.clone(),
                                channel: channel.to_string(),
                                head_seq: since_seq,
                                messages: Vec::new(),
                            }),
                    );
                    if !candidate.messages.is_empty() {
                        return Some(candidate);
                    }
                }
            }
            None
        })
        .await;

        Ok(filter_mentions(
            self.board_read(&canonical, channel, since_seq, usize::MAX)
                .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use nostr::prelude::Kind;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn test_mailbox(dir: &TempDir) -> Mailbox {
        let db_path = dir.path().join("test.sqlite");
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let mb = Mailbox::new(
            Arc::new(bus),
            dir.path().to_path_buf(),
            Some(crate::keys::generate()),
        )
        .with_index_pool(pool);
        mb.create_agent_identity(
            "proj",
            &crate::nostr::identity::AgentProfile::new("AmberFalcon"),
        )
        .await
        .expect("create AmberFalcon");
        mb.create_agent_identity(
            "proj",
            &crate::nostr::identity::AgentProfile::new("DustyHollow"),
        )
        .await
        .expect("create DustyHollow");
        mb
    }

    #[test]
    fn board_post_refuses_secret_class_and_routes_to_sealed_mail() {
        assert_eq!(
            classify_board_body("normal room update", Some("secret")),
            Err(BoardRefusal::SecretClassRequiresSealedMail {
                reason: "sensitivity label"
            })
        );
        assert_eq!(
            classify_board_body("here is nsec1abc123", None),
            Err(BoardRefusal::SecretClassRequiresSealedMail {
                reason: "secret material in body"
            })
        );
        assert!(classify_board_body("ready for review", None).is_ok());
    }

    #[tokio::test]
    async fn board_post_and_read_assign_monotonic_seq_without_gift_wrap() {
        let dir = TempDir::new().unwrap();
        let mb = test_mailbox(&dir).await;
        let m1 = mb
            .board_post("proj", "main", "AmberFalcon", "first", &[], None)
            .await
            .unwrap();
        let m2 = mb
            .board_post(
                "proj",
                "main",
                "DustyHollow",
                "second",
                &["AmberFalcon".into()],
                None,
            )
            .await
            .unwrap();
        let m3 = mb
            .board_post("proj", "main", "AmberFalcon", "third", &[], None)
            .await
            .unwrap();
        assert_eq!((m1.seq, m2.seq, m3.seq), (1, 2, 3));

        let tail = mb.board_read("proj", "main", 1, 10).await.unwrap();
        assert_eq!(tail.head_seq, 3);
        assert_eq!(tail.messages.len(), 2);
        assert_eq!(tail.messages[0].seq, 2);
        assert_eq!(tail.messages[1].seq, 3);

        let wraps = crate::mailbox::collect_replay(&*mb.bus, Filter::new().kinds([Kind::GiftWrap]))
            .await
            .unwrap();
        assert!(wraps.is_empty(), "board_post must not emit kind-1059 wraps");
    }

    #[tokio::test]
    async fn board_wait_returns_immediately_when_unread_exists_and_times_out_bounded() {
        let dir = TempDir::new().unwrap();
        let mb = test_mailbox(&dir).await;
        mb.board_post(
            "proj",
            "main",
            "DustyHollow",
            "ping @AmberFalcon",
            &["AmberFalcon".into()],
            None,
        )
        .await
        .unwrap();
        let hit = mb
            .board_wait("proj", "main", 0, true, Some("AmberFalcon"), 50)
            .await
            .unwrap();
        assert_eq!(hit.messages.len(), 1);
        assert_eq!(hit.messages[0].seq, 1);

        let empty = mb
            .board_wait("proj", "main", 1, false, None, 20)
            .await
            .unwrap();
        assert!(empty.messages.is_empty());
    }
}
