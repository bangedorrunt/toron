// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Scenario registry (ADR register). Skeleton modules per phase; each lands
//! its assertions with the tools it depends on.

/// SC-1..SC-8 + SC-14a — v1 gate (feature `p1`).
pub mod p1 {
    /// SC-1 agent onboarding → boots with `--features p1`.
    pub async fn sc1_onboarding() -> anyhow::Result<()> {
        Ok(())
    }
}

/// SC-9..SC-13 — Phase-2 gate (feature `p2`).
pub mod p2 {
    /// SC-9 @mention → job claim → result.
    pub async fn sc9_mention_job() -> anyhow::Result<()> {
        Ok(())
    }
}
