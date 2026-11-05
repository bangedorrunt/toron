// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Structured reservation yields (bd-reserve-yield-taqf).
//!
//! Measured friction: the loop resumed the same in-progress bead every pass
//! while a live peer held the bead's lane, and every resumed pane answered
//! with a stand-down comment commit — dozens of commits for one fact. A yield
//! records that stand-down ONCE, as durable state the loop reads back.
//!
//! Table `reservation_yields(project_slug, bead_id, holder, yielder, note,
//! created_ts_us)`, unique on `(project_slug, bead_id, holder)` — two yields
//! for the same pair collapse to one marker (the idempotence the bead asks
//! for), with the note refreshed.
//!
//! **Suppression is derived, never stored.** A bead is suppressed while ANY
//! yield row for it names a holder who currently holds a live reservation in
//! the project. That makes "the marker clears when the holder releases" true
//! by construction: the live board is the other half of the rule, so a
//! released holder stops suppressing without any delete path to drift out of
//! sync. The boundary is deliberately project-wide: the yield names a bead and
//! a holder, not a path, so the holder reading live anywhere in the project is
//! the whole signal.
//!
//! [`Mailbox::reservation_yield_board`] is the ONE join — the MCP read tool
//! and `toron reserve yields` both print its rows, so the two surfaces cannot
//! disagree about who is suppressed.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::Mailbox;

/// One stand-down marker: "bead `bead_id` is being worked by `holder`".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct YieldMarker {
    pub bead_id: String,
    pub holder: String,
    pub yielder: String,
    pub note: String,
    pub created_ts_us: i64,
}

/// A marker with its holder's liveness resolved against the live lease board.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct YieldView {
    pub bead_id: String,
    pub holder: String,
    pub yielder: String,
    pub note: String,
    pub created_ts_us: i64,
    /// The holder holds a live lease in the project right now.
    pub holder_live: bool,
}

impl Mailbox {
    /// Record one yield marker. Idempotent per `(project, bead, holder)`: a
    /// repeat refreshes `yielder`/`note`/`created_ts_us` instead of adding a
    /// row, so "I yield bead X to H" said twice is one fact.
    pub async fn record_reservation_yield(
        &self,
        project_slug: &str,
        yielder: &str,
        bead_id: &str,
        holder: &str,
        note: &str,
    ) -> anyhow::Result<()> {
        let pool = self.index_pool_required()?;
        let project_slug = self.canonical_project(project_slug).await;
        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        sqlx::query(
            "INSERT INTO reservation_yields \
             (project_slug, bead_id, holder, yielder, note, created_ts_us) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(project_slug, bead_id, holder) DO UPDATE SET \
               yielder=excluded.yielder, note=excluded.note, created_ts_us=excluded.created_ts_us",
        )
        .bind(project_slug)
        .bind(bead_id)
        .bind(holder)
        .bind(yielder)
        .bind(note)
        .bind(now_us)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Every marker recorded in the project, oldest first.
    pub async fn list_reservation_yields(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<YieldMarker>> {
        let pool = self.index_pool_required()?;
        let project_slug = self.canonical_project(project_slug).await;
        let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
            "SELECT bead_id, holder, yielder, note, created_ts_us \
             FROM reservation_yields WHERE project_slug = ? \
             ORDER BY created_ts_us, bead_id",
        )
        .bind(project_slug)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(bead_id, holder, yielder, note, created_ts_us)| YieldMarker {
                    bead_id,
                    holder,
                    yielder,
                    note,
                    created_ts_us,
                },
            )
            .collect())
    }

    /// The ONE join of markers against the live lease board: every marker
    /// with `holder_live` resolved, plus the beads a resume must currently
    /// skip. No caller re-derives either half.
    pub async fn reservation_yield_board(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<(Vec<YieldView>, Vec<String>)> {
        let markers = self.list_reservation_yields(project_slug).await?;
        let live: BTreeSet<String> = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .map(|r| r.agent_name)
            .filter(|n| !n.is_empty())
            .collect();
        let suppressed = suppressed_beads(&markers, &live);
        let views = markers
            .iter()
            .map(|m| YieldView {
                bead_id: m.bead_id.clone(),
                holder: m.holder.clone(),
                yielder: m.yielder.clone(),
                note: m.note.clone(),
                created_ts_us: m.created_ts_us,
                holder_live: live.contains(&m.holder),
            })
            .collect();
        Ok((views, suppressed))
    }

    /// Convenience for the loop plane: the beads a resume must skip right now.
    pub async fn suppressed_resume_beads(&self, project_slug: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.reservation_yield_board(project_slug).await?.1)
    }
}

/// The pure rule behind the board join: a bead is suppressed while a yield
/// row for it names a live holder. Deduped and sorted (`BTreeSet`), so the
/// shipped read and its test cannot disagree.
pub fn suppressed_beads(yields: &[YieldMarker], live_holders: &BTreeSet<String>) -> Vec<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    for y in yields {
        if !y.holder.is_empty() && live_holders.contains(&y.holder) {
            out.insert(y.bead_id.clone());
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(bead: &str, holder: &str) -> YieldMarker {
        YieldMarker {
            bead_id: bead.to_string(),
            holder: holder.to_string(),
            yielder: "Yielder".to_string(),
            note: String::new(),
            created_ts_us: 0,
        }
    }

    /// The pure join: a live holder suppresses, a released holder does not, an
    /// unstamped holder never does, and duplicate pairs collapse.
    #[test]
    fn suppression_follows_the_live_holder() {
        let live: BTreeSet<String> = ["Holder".to_string()].into_iter().collect();
        let rows = vec![
            marker("bead-a", "Holder"),
            marker("bead-b", "Gone"),
            marker("bead-a", "Holder"),
            marker("bead-c", ""),
        ];
        assert_eq!(suppressed_beads(&rows, &live), vec!["bead-a".to_string()]);
        assert!(
            suppressed_beads(&rows, &BTreeSet::new()).is_empty(),
            "no live holders → nothing suppressed"
        );
    }
}
