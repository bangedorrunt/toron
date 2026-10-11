// governed-by: ADR-0041
//! Comms inbox on the mailbox (ADR-0041): the shared durable root's
//! [`InboxStore`] plus name -> npub resolution for the CLI verbs.
//!
//! The verbs are CLI-only (no new MCP tool; the frozen surface stays
//! frozen): each `toron next|ack|done|inbox append` opens the same durable
//! root the daemon uses, the pattern `ack-sla` and `reserve yield` already
//! follow. SQLite (WAL, `BEGIN IMMEDIATE`) serialises the writers.

use super::Mailbox;
use crate::envelope::is_npub_hex;
use crate::inbox::{InboxError, InboxStore, enabled};

impl Mailbox {
    /// The comms inbox store over the index pool. Refuses while the
    /// `TORON_COMMS_INBOX` flag is off.
    pub fn inbox_store(&self) -> anyhow::Result<InboxStore> {
        if !enabled() {
            return Err(InboxError::Disabled.into());
        }
        let pool = self
            .index_pool()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("comms inbox requires the index pool (no --index)"))?;
        Ok(InboxStore::new(pool))
    }

    /// Resolve `--as` to a hex npub: 64 lowercase hex passes through, any
    /// other value is an agent name looked up in `project`'s roster
    /// (ADR-0037 W1: one name, one key).
    pub async fn comms_npub(&self, project: &str, who: &str) -> anyhow::Result<String> {
        if is_npub_hex(who) {
            return Ok(who.to_string());
        }
        let w = self.whois(project, who).await?;
        Ok(w.npub)
    }
}
