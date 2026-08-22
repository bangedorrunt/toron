// governed-by: ADR-0003
// governed-by: ADR-0017 (project-root canonicalization)
#![forbid(unsafe_code)]

//! Identity cluster (bd-led.5.2): register_agent, create_agent_identity,
//! whois, list_agents, resolve_pane_identity, cleanup_pane_identities.
//!
//! ADR-0003 G1/G2/G9:
//! - **Names never npubs** in tool args (AM-Rust parity). name → npub via the
//!   project's kind-30013 profiles; unknown names fail fast.
//! - `create_agent_identity` always mints a fresh keypair (ncryptsec at rest);
//!   `register_agent` upserts on the existing key (or creates if absent).
//! - `last_active` = latest signed event `created_at` (verifiable provenance,
//!   never a mutable DB-only column).
//! - Pane identity (G9): herdr backend via `agent.list` (pane_id ↔ agent name);
//!   cleanup prunes identities whose panes are absent from `pane.list`.

use std::path::PathBuf;

use nostr_sdk::nostr::{Keys, Timestamp};

use crate::backend::herdr::HerdrBackend;
use crate::bus::filter::kind_project_filter;
use crate::nostr::identity::{self as ident, AgentProfile, IdentityResult};
use crate::nostr::kinds::{AGENT_PROFILE, toron_kind};

use super::Mailbox;
use super::collect_replay;

/// Result of register/create: the agent's identity + where its key lives.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredAgent {
    /// Agent name (project-scoped).
    pub name: String,
    /// Project slug.
    pub project_slug: String,
    /// The agent npub (hex).
    pub npub: String,
    /// Event id of the signed kind-30013 profile.
    pub profile_event_id: String,
    /// Absolute path of the ncryptsec key file.
    pub key_path: String,
    /// True when this call minted a fresh keypair.
    pub created: bool,
}

/// What absorbing one folded slug changed (bd-legacy-slug-inbox-onoj).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct SlugAbsorption {
    /// The standalone `agents` row for the slug was removed.
    pub removed_registration: bool,
    /// The canonical target has a live registration (absorb precondition).
    pub canonical_registered: bool,
}

/// A leftover dual registration for `toron doctor`: an `agents` row whose
/// name is a recorded fold pointing elsewhere — a second identity the fold
/// never absorbed. Its npub is a dead inbox: no key file, wraps sealed to it
/// are unreachable by any name-routed surface, and cross-key delete is
/// forbidden — so doctor NAMES the row for operator cleanup.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FoldedSlugLeftover {
    pub project_slug: String,
    pub slug: String,
    pub pubkey: String,
    pub canonical: String,
}

/// Scan one index pool for leftover dual registrations. Empty after
/// absorption runs; doctor surfaces whatever remains.
pub async fn folded_slug_leftovers(
    pool: &sqlx::SqlitePool,
) -> anyhow::Result<Vec<FoldedSlugLeftover>> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT a.project_slug, a.name, a.pubkey, al.canonical          FROM agents a          JOIN agent_name_aliases al ON al.project_slug = a.project_slug AND al.alias = a.name          ORDER BY a.project_slug, a.name",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(project_slug, slug, pubkey, canonical)| FoldedSlugLeftover {
                project_slug,
                slug,
                pubkey,
                canonical,
            },
        )
        .collect())
}

/// `whois` result: npub + profile fields + last-active provenance.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WhoIs {
    /// Agent name.
    pub name: String,
    /// The agent npub (hex).
    pub npub: String,
    pub program: Option<String>,
    pub model: Option<String>,
    pub task_description: Option<String>,
    /// Last active (µs) = latest signed profile event `created_at`.
    pub last_active_ts_us: i64,
}

/// `list_agents` filter.
#[derive(Debug, Clone, Copy, Default)]
pub struct ListAgentsOptions {
    /// Only agents whose last-active is within this many days.
    pub active_within_days: Option<u64>,
}

impl Mailbox {
    /// `create_agent_identity`: ALWAYS mint a fresh keypair (AM-Rust parity:
    /// a NEW identity, never a collision). Fails closed if a key for this
    /// name already exists — otherwise we'd publish a profile signed by the
    /// fresh key while the on-disk ncryptsec (used by whois/read paths) still
    /// holds the OLD key, and whois npub would disagree with list_agents npub
    /// (real-e2e parity bug bd-XXX). Use `register_agent` to upsert.
    ///
    /// ADR-0018 (swarm mode): non-canonical names are rejected — the grammar
    /// `flywheel-<slug>-<role><seq>` is the only identity a swarm project can
    /// create.
    pub async fn create_agent_identity(
        &self,
        project_slug: &str,
        profile: &AgentProfile,
    ) -> anyhow::Result<RegisteredAgent> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        self.check_swarm_name(project_slug, &profile.name).await?;
        let key_path = self.agent_key_path(project_slug, &profile.name);
        if key_path.exists() {
            anyhow::bail!(
                "create_agent_identity refused: an identity for '{name}' already exists in project '{project_slug}' (use register_agent to upsert)",
                name = profile.name,
            );
        }
        let result = ident::create_agent_identity(project_slug, profile, Timestamp::now())?;
        let created = self
            .persist_and_publish(project_slug, profile, result)
            .await?;
        Ok(created)
    }

    /// `register_agent`: upsert on the EXISTING agent key (G1), or create a
    /// fresh keypair if absent. Idempotent — re-registering returns the same
    /// npub with a new profile event.
    ///
    /// ADR-0017: the project key is canonicalized first, so registering via
    /// any spelling of the project (short slug or path form) upserts the ONE
    /// canonical row — no duplicate agent rows, one key file.
    ///
    /// ADR-0018 (swarm mode): non-canonical names are rejected — legacy
    /// display names cannot be (re)created in a swarm project.
    pub async fn register_agent(
        &self,
        project_slug: &str,
        profile: &AgentProfile,
    ) -> anyhow::Result<RegisteredAgent> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        self.check_swarm_name(project_slug, &profile.name).await?;
        // Existing local key? (ncryptsec at agents/{name}/key.ncryptsec)
        // ADR-0017 migration: a pre-canonicalization key under an alternate
        // (path-slug) root is adopted — re-registering under the canonical
        // slug REUSES the legacy identity instead of minting a second one.
        let key_path = self.agent_key_path(project_slug, &profile.name);
        let existing = self
            .load_agent_key(&key_path)
            .or_else(|| self.alt_agent_key(project_slug, &profile.name));
        let created = existing.is_none();
        let result =
            ident::register_agent(project_slug, profile, existing.as_ref(), Timestamp::now())?;
        let mut agent = self
            .persist_and_publish(project_slug, profile, result)
            .await?;
        agent.created = created;
        Ok(agent)
    }

    /// ADR-0018: swarm-mode gate — a non-canonical agent name is rejected
    /// with an actionable error naming the grammar. Open mode passes through
    /// (legacy AM-Rust parity).
    ///
    /// bd-pin-public-plane-s-easy.1: a name that is ALREADY a folded alias
    /// (slug → pin) is refused as a second identity — the fold is the only
    /// routing, never a duplicate registration. Applies in both modes (the
    /// fold table is authoritative wherever it exists).
    async fn check_swarm_name(&self, project_slug: &str, name: &str) -> anyhow::Result<()> {
        if let Some(canonical) = self.agent_name_alias(project_slug, name).await {
            anyhow::bail!(
                "agent name {name} is a folded alias of {canonical} — register {canonical} \
                 (the pin), not a second identity (bd-pin-public-plane-s-easy.1: fold is \
                 slug → pin)"
            );
        }
        let mode = self.project_identity_mode(project_slug).await;
        if mode == crate::mailbox::naming::IdentityMode::Swarm
            && !super::naming::is_canonical_name(name, project_slug)
            && !super::naming::is_adjnoun_pin(name)
        {
            anyhow::bail!(
                "agent name {name} is not routable for project {project_slug} — swarm \
                 projects use flywheel-{project_slug}-<role><seq> or a REGISTERED \
                 adjective+noun pin (ADR-0018 D1a/D3)"
            );
        }
        Ok(())
    }

    /// ADR-0018 D4: record a legacy-name fold (alias → canonical). Idempotent
    /// append-only — the row is inserted once and never deleted, so the
    /// relation is always reversible. Best-effort: no index pool → the fold
    /// cannot be persisted and routing degrades to strict swarm enforcement.
    ///
    /// bd-legacy-slug-inbox-onoj: recording the fold also ABSORBS any
    /// standalone registration of the alias (see [`Self::absorb_folded_slug`])
    /// — a fold is the only routing, never a second inbox. Use
    /// [`Self::fold_agent_name_alias_no_absorb`] to record the mapping
    /// without touching an existing row.
    pub async fn fold_agent_name_alias(&self, project_slug: &str, alias: &str, canonical: &str) {
        self.fold_agent_name_alias_no_absorb(project_slug, alias, canonical)
            .await;
        self.absorb_folded_slug(project_slug, alias).await;
    }

    /// Record the alias → canonical mapping ONLY (no side effects on any
    /// existing agents row).
    pub async fn fold_agent_name_alias_no_absorb(
        &self,
        project_slug: &str,
        alias: &str,
        canonical: &str,
    ) {
        let Some(pool) = self.index_pool.as_ref() else {
            return;
        };
        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let _ = sqlx::query(
            "INSERT INTO agent_name_aliases (project_slug, alias, canonical, created_ts_us) \
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT (project_slug, alias) DO NOTHING",
        )
        .bind(project_slug)
        .bind(alias)
        .bind(canonical)
        .bind(now_us)
        .execute(pool)
        .await;
    }

    /// bd-legacy-slug-inbox-onoj: a slug that was registered STANDALONE
    /// before its fold existed keeps a second identity (own npub) beside the
    /// pin. Absorb = remove the standalone `agents` row:
    ///   - routing already folds alias → canonical at every resolver touch
    ///     (sends to the slug seal to the PIN's key),
    ///   - the orphan's wraps are unreachable by construction (no inbox
    ///     surface keys off a roster row, and the projection carries no
    ///     recipient column — there is nothing to mark read), and cross-key
    ///     NIP-09 delete is forbidden. Removing the row removes the SECOND
    ///     INBOX itself.
    ///
    /// Idempotent: no fold / no row / target unregistered → zero report.
    /// Refuses to destroy a row when the canonical target has NO live
    /// registration (absorbing into a void would strand the identity worse
    /// than before — doctor names those).
    pub async fn absorb_folded_slug(&self, project_slug: &str, alias: &str) -> SlugAbsorption {
        let Some(pool) = self.index_pool.as_ref() else {
            return SlugAbsorption::default();
        };
        let Some(canonical_pubkey) = sqlx::query_scalar::<_, String>(
            "SELECT pubkey FROM agents WHERE project_slug = ?1 AND name = \
             (SELECT canonical FROM agent_name_aliases WHERE project_slug = ?1 AND alias = ?2)",
        )
        .bind(project_slug)
        .bind(alias)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten() else {
            // No fold recorded or no canonical registration — absorb nothing.
            return SlugAbsorption::default();
        };
        let removed = sqlx::query(
            "DELETE FROM agents WHERE project_slug = ?1 AND name = ?2 AND pubkey != ?3",
        )
        .bind(project_slug)
        .bind(alias)
        .bind(&canonical_pubkey)
        .execute(pool)
        .await
        .map(|r| r.rows_affected() > 0)
        .unwrap_or(false);
        SlugAbsorption {
            removed_registration: removed,
            canonical_registered: true,
        }
    }

    /// bd-captain-alias-0g8a: point the project's routable `captain` alias
    /// at a REGISTERED adjective+noun pin (the orchestrator's identity).
    /// Durable (agent_name_aliases), re-pointable (UPSERT — the CURRENT
    /// orchestrator wins over first-fold), and never a second identity
    /// (ADR-0018 D1a: the alias resolves through the ONE resolver; a host
    /// username is not a mailbox identity and an unregistered pin does not
    /// route). Returns the canonical pin name stored.
    pub async fn set_project_captain(
        &self,
        project_slug: &str,
        pin: &str,
    ) -> anyhow::Result<String> {
        let canonical = self.canonical_project(project_slug).await;
        if super::naming::is_username_class(pin) {
            anyhow::bail!(
                "{pin} looks like a host OS username ($USER), not a mailbox \
                 identity — captain must be a REGISTERED adjective+noun pin in \
                 project {canonical}; do not register your username — use your \
                 pin or list_agents to find identities (ADR-0018 D1a/D3)"
            );
        }
        // Fail fast: the fold must land on a routable identity, else every
        // later "mail the captain" bounces with an unhelpful name error.
        let who = self.whois(&canonical, pin).await.map_err(|e| {
            anyhow::anyhow!(
                "captain must be a REGISTERED adjective+noun pin in project \
                 {canonical} (whois failed: {e})"
            )
        })?;
        let Some(pool) = self.index_pool.as_ref() else {
            anyhow::bail!("no index pool attached — cannot persist the captain alias");
        };
        upsert_captain_alias(pool, &canonical, &who.name).await?;
        Ok(who.name)
    }

    /// ADR-0018 D4: the canonical name a legacy alias folds to (if any).
    /// `None` when the alias is unknown or no pool is attached.
    pub async fn agent_name_alias(&self, project_slug: &str, alias: &str) -> Option<String> {
        let pool = self.index_pool.as_ref()?;
        sqlx::query_scalar(
            "SELECT canonical FROM agent_name_aliases WHERE project_slug = ?1 AND alias = ?2",
        )
        .bind(project_slug)
        .bind(alias)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
    }

    /// bd-pin-public-plane-s-easy.1: THE one name resolver — pin, slug, or
    /// folded alias → the canonical registered name (the pin, or the
    /// canonical `flywheel-<slug>-<role><seq>` name).
    ///
    /// Resolution order (swarm mode): the FOLD comes first (an explicit
    /// alias mapping is authoritative — a slug folded to a pin resolves as
    /// the pin, never as its raw shape), then the canonical name itself,
    /// then a registered pin shape. Anything else — an unregistered free
    /// string — is non-routable and rejected with the ADR-0018 grammar.
    ///
    /// Open mode (no identity-mode row): the name passes through untouched
    /// (legacy AM-Rust parity — any registered name resolves).
    ///
    /// The result is what `whois`, the send path, and `agent_keys_for`
    /// resolve against: holder equality across pin/slug is impossible when
    /// every consumer goes through this one helper.
    pub async fn resolve_agent_name(
        &self,
        project_slug: &str,
        name: &str,
    ) -> anyhow::Result<String> {
        let canonical = self.canonical_project(project_slug).await;
        let mode = self.project_identity_mode(&canonical).await;
        if mode != crate::mailbox::naming::IdentityMode::Swarm {
            return Ok(name.to_string());
        }
        if let Some(folded) = self.agent_name_alias(&canonical, name).await {
            // bd-6wfv: the fold precedes the pin-pattern match — an explicit
            // fold is the authoritative mapping and must win over any shape
            // match (a folded slug is a canonical-shaped name AND a folded
            // legacy name may pattern-match as a pin).
            return Ok(folded);
        }
        if super::naming::is_canonical_name(name, &canonical) || super::naming::is_adjnoun_pin(name)
        {
            return Ok(name.to_string());
        }
        // bd-pin-public-plane-s-easy.8: a host OS username ($USER) is NOT a
        // mailbox identity — name the class + the recovery instead of the
        // generic grammar error. Callers who type their own OS username
        // think it is their mailbox name; telling them to register it would
        // mint an identity for a non-identity. (Canonical/pin/fold shapes
        // already returned above — this is the residual reject path only.)
        if super::naming::is_username_class(name) {
            anyhow::bail!(
                "{name} looks like a host OS username ($USER), not a mailbox \
                 identity — project {canonical} routes by REGISTERED \
                 adjective+noun pins, canonical flywheel-{canonical}-<role><seq> \
                 names, or npub; do not register your username — use your pin \
                 or list_agents to find identities (ADR-0018 D1a/D3)"
            );
        }
        anyhow::bail!(
            "unknown agent name: {name} — project {canonical} routes by canonical \
             names (flywheel-{canonical}-<role><seq>), REGISTERED adjective+noun \
             pins, or npub only; unregistered free strings are non-routable \
             (ADR-0018 D1a/D3)"
        )
    }

    /// Persist the key (ncryptsec 0600) + publish the signed profile event.
    ///
    /// ADR-0017: writes the canonical project slug everywhere (key path,
    /// profile event tags, agents-index row), so registration state spans
    /// every spelling of the project key.
    async fn persist_and_publish(
        &self,
        project_slug: &str,
        profile: &AgentProfile,
        result: IdentityResult,
    ) -> anyhow::Result<RegisteredAgent> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let key_path = self.agent_key_path(project_slug, &profile.name);
        if !key_path.exists() {
            crate::keys::write_encrypted_key(
                &key_path,
                &result.keys,
                self.agent_passphrase(),
                crate::keys::DEFAULT_LOG_N,
            )?;
        }
        let ok = self.bus.publish(result.event.clone()).await?;
        if !ok.stored {
            anyhow::bail!("profile event rejected by bus");
        }
        // Index the registration (search-scoped identity table). Stores the
        // VERBATIM project slug so the unwrap-loop projection can attribute
        // inbound wraps to a project exactly (leading-separator forms
        // round-trip — path archaeology cannot).
        if let Some(pool) = &self.index_pool {
            let now_us = result.event.created_at.as_secs() as i64 * 1_000_000;
            sqlx::query(
                "INSERT INTO agents (project_slug, name, pubkey, program, model, \
                 task_description, created_ts_us) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (project_slug, name) DO UPDATE SET \
                 pubkey = excluded.pubkey, program = excluded.program, \
                 model = excluded.model, task_description = excluded.task_description, \
                 last_active_ts_us = excluded.created_ts_us",
            )
            .bind(project_slug)
            .bind(&profile.name)
            .bind(result.keys.public_key().to_hex())
            .bind(profile.program.clone().unwrap_or_default())
            .bind(profile.model.clone().unwrap_or_default())
            .bind(profile.task_description.clone().unwrap_or_default())
            .bind(now_us)
            .execute(pool)
            .await?;
        }
        Ok(RegisteredAgent {
            name: profile.name.clone(),
            project_slug: project_slug.to_string(),
            npub: result.keys.public_key().to_hex(),
            profile_event_id: result.event.id.to_hex(),
            key_path: key_path.display().to_string(),
            created: true,
        })
    }

    /// Absolute path of an agent's ncryptsec key file
    /// (`{data_dir}/{project}/agents/{name}/key.ncryptsec`).
    ///
    /// MCP clients pass absolute project keys (AM parity), and
    /// `PathBuf::join` with an absolute path DISCARDS the data_dir prefix —
    /// keys would escape into the slug's own tree and the unwrap-loop key
    /// scan could never find them (projection never ran → search empty).
    ///
    /// ADR-0017: the key path always resolves through the canonical short
    /// slug (basename of the key, path form or not), so every spelling of a
    /// project reads and writes the ONE key file under the canonical root.
    /// The `alt_agent_key` walk below remains as the legacy read fallback
    /// for pre-ADR-0017 roots.
    pub(crate) fn agent_key_path(&self, project_slug: &str, name: &str) -> PathBuf {
        super::aliases::project_root(&self.data_dir, project_slug)
            .join("agents")
            .join(name)
            .join("key.ncryptsec")
    }

    /// Load an agent key from disk (passphrase decrypt). `None` if absent.
    /// bd-harvest-thread-filter-yp9r: routes through the R2.0 content-keyed
    /// KDF cache — the SEND path (agent_keys_for) previously re-ran argon2
    /// (~4s/key) on every message, which made the 10k/1k scale gate
    /// unreachable. Same semantics (read + decrypt), cache-aware.
    pub(crate) fn load_agent_key(&self, path: &std::path::Path) -> Option<Keys> {
        if !path.exists() {
            return None;
        }
        self.cached_load_agent_key(path)
    }

    /// bd-cmgu: find an agent key under an ALTERNATE project root whose slug
    /// basename matches the primary slug's basename (MCP short key vs CLI
    /// absolute-path key — either identifier must resolve to the same key).
    ///
    /// Walks `data_dir` at arbitrary depth (path-slug roots are nested, the
    /// leading `/` stripped by `agent_key_path`), collecting project roots
    /// (dirs containing `agents/`). Deterministic: candidates are visited in
    /// sorted slug order; the FIRST one holding the key wins. Symlinks are
    /// never followed (the walk cannot escape `data_dir`). Best-effort: any
    /// walk error yields `None` — the caller reports the original miss.
    ///
    /// bd-f5tt: the walk descends INTO project roots too — a root may nest
    /// deeper path-slug roots (`<data>/Users/.../workspace` holding `agents/`
    /// while `<data>/Users/.../workspace/toron` is the real project). A
    /// non-descending walk would shadow the nested root.
    pub(crate) fn alt_agent_key(&self, project_slug: &str, name: &str) -> Option<Keys> {
        let want = std::path::Path::new(project_slug)
            .file_name()?
            .to_str()?
            .to_string();
        let mut candidates: Vec<String> = Vec::new();
        let mut stack = vec![self.data_dir.clone()];
        while let Some(dir) = stack.pop() {
            let entries = std::fs::read_dir(&dir).ok()?;
            for e in entries.flatten() {
                let p = e.path();
                if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let slug = p
                    .strip_prefix(&self.data_dir)
                    .ok()?
                    .to_string_lossy()
                    .to_string();
                if slug == project_slug {
                    continue;
                }
                if p.join("agents").is_dir()
                    && std::path::Path::new(&slug)
                        .file_name()
                        .and_then(|f| f.to_str())
                        == Some(want.as_str())
                {
                    candidates.push(slug);
                }
                // Descend regardless: a project root may nest deeper roots.
                stack.push(p);
            }
        }
        candidates.sort();
        for slug in candidates {
            // Verbatim nested path, NOT agent_key_path: the legacy key lives
            // under its own path-slug tree (`{data}/srv/legacy/toron/...`);
            // agent_key_path canonicalizes to the basename root and would
            // miss it (ADR-0017 migration read path).
            let legacy = self
                .data_dir
                .join(slug)
                .join("agents")
                .join(name)
                .join("key.ncryptsec");
            if let Some(k) = self.load_agent_key(&legacy) {
                return Some(k);
            }
        }
        None
    }

    /// bd-f5tt: the project-root slug aliases for `project_slug` — the slug
    /// itself plus every data-dir project root whose basename matches (CLI
    /// absolute-path key vs MCP short key; the guard check queries
    /// reservations under ALL aliases because a lease is tagged with the slug
    /// as-written at acquire time). Both spellings of a path slug are
    /// included (leading `/` stripped by the walk, but acquire tags the raw
    /// slug verbatim) so queries match leases written under either form.
    pub(crate) fn project_slug_aliases(&self, project_slug: &str) -> Vec<String> {
        let mut out = vec![project_slug.to_string()];
        if let Some(stripped) = project_slug.strip_prefix('/') {
            out.push(stripped.to_string());
        }
        let want = std::path::Path::new(project_slug)
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or(project_slug)
            .to_string();
        let mut stack = vec![self.data_dir.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let slug = p
                    .strip_prefix(&self.data_dir)
                    .ok()
                    .map(|s| s.to_string_lossy().to_string());
                let Some(slug) = slug else { continue };
                if slug == project_slug {
                    continue;
                }
                if p.join("agents").is_dir()
                    && std::path::Path::new(&slug)
                        .file_name()
                        .and_then(|f| f.to_str())
                        == Some(want.as_str())
                {
                    out.push(slug.clone());
                    out.push(format!("/{slug}"));
                }
                stack.push(p);
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// The passphrase used to encrypt/decrypt agent keys (G1: passphrase
    /// unlocks ncryptsec at process start). Defaults to `TORON_KEY_PASSPHRASE`
    /// or a dev placeholder; production deployments must set it.
    pub(crate) fn agent_passphrase(&self) -> &str {
        self.agent_passphrase.as_deref().unwrap_or("toron-dev")
    }

    /// `whois`: name → npub via the project's kind-30013 profiles (G2).
    /// Unknown name fails fast (AM-Rust parity).
    ///
    /// ADR-0017: resolves against the project's full alias set — profile
    /// `d` tags are slug-scoped (`<slug>:<name>`), so a legacy profile
    /// registered under a path-form slug only matches when resolved under
    /// that spelling. Every spelling is tried.
    ///
    /// ADR-0018 (swarm mode): the routing target is a canonical name
    /// (`flywheel-<slug>-<role><seq>`), an npub, or a folded legacy alias
    /// (`agent_name_aliases` row → canonical). Anything else fails with an
    /// actionable error naming the grammar. Open mode keeps the legacy
    /// any-name resolution (AM-Rust parity).
    pub async fn whois(&self, project_slug: &str, name: &str) -> anyhow::Result<WhoIs> {
        let canonical = self.canonical_project(project_slug).await;
        let set = self.project_read_set(&canonical).await;
        let mut npub: Option<nostr_sdk::nostr::PublicKey> = None;
        let mut profiles: Vec<nostr_sdk::nostr::Event> = Vec::new();

        // ADR-0018: npub input resolves by pubkey (the read-side twin of npub
        // routing). Works in both modes — an npub is always a routing address.
        if super::naming::is_npub(name) {
            for slug in &set {
                let filter = kind_project_filter(toron_kind(AGENT_PROFILE), slug);
                let events = collect_replay(self.bus.as_ref(), filter).await?;
                profiles.extend(events);
            }
            let pubkey = nostr_sdk::nostr::PublicKey::from_hex(name)
                .map_err(|_| anyhow::anyhow!("invalid npub: {name}"))?;
            npub = profiles
                .iter()
                .find(|e| e.pubkey == pubkey)
                .map(|e| e.pubkey);
            let Some(npub) = npub else {
                anyhow::bail!("no registered identity for npub {name}");
            };
            let name = profiles
                .iter()
                .filter(|e| e.pubkey == npub && e.kind.as_u16() == AGENT_PROFILE)
                .max_by_key(|e| e.created_at.as_secs())
                .and_then(|e| {
                    e.tags.iter().find_map(|t| {
                        (t.kind() == nostr_sdk::nostr::TagKind::d())
                            .then(|| t.content())
                            .flatten()
                    })
                })
                .and_then(ident::profile_name_from_d_tag)
                .unwrap_or(name);
            return self.whois_build(name.to_string(), npub, profiles).await;
        }

        // ADR-0018: swarm mode resolves canonical names, folded aliases, or
        // pins ONLY; display names are non-routable profile metadata.
        // bd-pin-public-plane-s-easy.1: ONE resolver — the fold precedes the
        // shape match (a slug folded to a pin resolves as the pin, not as
        // its canonical shape — whois(slug) must hit the pin's npub).
        let mode = self.project_identity_mode(&canonical).await;
        let target: String = if mode == crate::mailbox::naming::IdentityMode::Swarm {
            self.resolve_agent_name(&canonical, name).await?
        } else {
            name.to_string()
        };

        for slug in &set {
            let filter = kind_project_filter(toron_kind(AGENT_PROFILE), slug);
            let events = collect_replay(self.bus.as_ref(), filter).await?;
            if npub.is_none() {
                npub = ident::resolve_name(slug, events.iter(), &target);
            }
            profiles.extend(events);
        }
        let npub =
            npub.ok_or_else(|| anyhow::anyhow!("unknown agent name: {target} (register first)"))?;
        self.whois_build(target, npub, profiles).await
    }

    /// Shared tail of `whois`: profile fields + last-active provenance.
    async fn whois_build(
        &self,
        name: String,
        npub: nostr_sdk::nostr::PublicKey,
        profiles: Vec<nostr_sdk::nostr::Event>,
    ) -> anyhow::Result<WhoIs> {
        // Profile fields + last-active provenance (latest signed profile event).
        let profile_ev = profiles
            .iter()
            .filter(|e| e.pubkey == npub && e.kind.as_u16() == AGENT_PROFILE)
            .max_by_key(|e| e.created_at.as_secs())
            .ok_or_else(|| anyhow::anyhow!("profile event missing for {name}"))?;
        let profile = ident::parse_profile(profile_ev).unwrap_or_else(|| AgentProfile::new(&name));
        Ok(WhoIs {
            name: name.to_string(),
            npub: npub.to_hex(),
            program: profile.program,
            model: profile.model,
            task_description: profile.task_description,
            last_active_ts_us: profile_ev.created_at.as_secs() as i64 * 1_000_000,
        })
    }

    /// `list_agents`: project's agents with npub + last-active. Deduplicates by
    /// name (latest profile wins) and filters by `active_within_days`.
    pub async fn list_agents(
        &self,
        project_slug: &str,
        opts: ListAgentsOptions,
    ) -> anyhow::Result<Vec<WhoIs>> {
        let profiles = self.project_profiles(project_slug).await?;
        let mut by_name: std::collections::HashMap<String, WhoIs> =
            std::collections::HashMap::new();
        for ev in &profiles {
            let Some(d) = ev
                .tags
                .iter()
                .find_map(|t| (t.kind() == nostr_sdk::nostr::TagKind::d()).then(|| t.content()))
                .flatten()
            else {
                continue;
            };
            let Some(name) = ident::profile_name_from_d_tag(d) else {
                continue;
            };
            let npub = ev.pubkey.to_hex();
            let ts_us = ev.created_at.as_secs() as i64 * 1_000_000;
            // Latest wins per name (addressable latest-wins per author).
            let entry = by_name.entry(name.to_string()).or_insert_with(|| WhoIs {
                name: name.to_string(),
                npub: npub.clone(),
                program: None,
                model: None,
                task_description: None,
                last_active_ts_us: ts_us,
            });
            if ts_us > entry.last_active_ts_us {
                entry.npub = npub;
                entry.last_active_ts_us = ts_us;
            }
            if let Some(p) = ident::parse_profile(ev) {
                entry.program = p.program.or(entry.program.clone());
                entry.model = p.model.or(entry.model.clone());
                entry.task_description = p.task_description.or(entry.task_description.clone());
            }
        }

        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let mut agents: Vec<WhoIs> = by_name.into_values().collect();
        if let Some(days) = opts.active_within_days {
            let cutoff = now_us - (days as i64) * 86_400 * 1_000_000;
            agents.retain(|a| a.last_active_ts_us >= cutoff);
        }
        agents.sort_by_key(|b| std::cmp::Reverse(b.last_active_ts_us));
        Ok(agents)
    }

    /// All kind-30013 profile events for a project (project-scoped `#t`).
    ///
    /// ADR-0017: replays under the project's full alias set (canonical +
    /// every stored alias), so agents registered under ANY spelling of the
    /// project are visible from every other spelling. Events are deduplicated
    /// by id.
    pub(crate) async fn project_profiles(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<nostr_sdk::nostr::Event>> {
        let set = self.project_read_set(project_slug).await;
        let mut out: Vec<nostr_sdk::nostr::Event> = Vec::new();
        let mut seen: std::collections::HashSet<nostr_sdk::nostr::EventId> =
            std::collections::HashSet::new();
        for slug in &set {
            let filter = kind_project_filter(toron_kind(AGENT_PROFILE), slug);
            for ev in collect_replay(self.bus.as_ref(), filter).await? {
                if seen.insert(ev.id) {
                    out.push(ev);
                }
            }
        }
        Ok(out)
    }

    /// `resolve_pane_identity`: herdr pane_id → agent name (G9). `None` when
    /// the pane has no agent mapping.
    pub fn resolve_pane_identity(
        &self,
        backend: &HerdrBackend,
        pane_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let mut client = backend.client();
        let agents = client.agent_list()?;
        Ok(agents
            .into_iter()
            .find(|a| a.pane_id == pane_id)
            .and_then(|a| a.name))
    }

    /// `cleanup_pane_identities`: prune identities whose panes are absent from
    /// herdr's pane list. Returns the pruned agent names (G9). toron holds no
    /// per-pane identity files (live herdr mapping), so "prune" = report the
    /// stale names for the projection to drop.
    pub fn cleanup_pane_identities(&self, backend: &HerdrBackend) -> anyhow::Result<Vec<String>> {
        let mut client = backend.client();
        let agents = client.agent_list()?;
        let live_panes: Vec<String> = client
            .workspace_list()?
            .into_iter()
            .flat_map(|ws| {
                client
                    .pane_list(Some(&ws.workspace_id))
                    .unwrap_or_default()
                    .into_iter()
                    .map(|p| p.pane_id)
            })
            .collect();
        Ok(agents
            .into_iter()
            .filter(|a| a.name.is_some() && !live_panes.contains(&a.pane_id))
            .filter_map(|a| a.name)
            .collect())
    }
}

/// bd-captain-alias-0g8a: shared UPSERT for the `captain` fold. Plain state
/// in `agent_name_aliases` — no event projection, so the daemon-mode CLI may
/// write it against the durable root directly (guard precedent,
/// bd-pin-public-plane-s-easy.14) without opening a second mailbox.
async fn upsert_captain_alias(
    pool: &sqlx::SqlitePool,
    project_slug: &str,
    canonical_pin: &str,
) -> anyhow::Result<()> {
    let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
    sqlx::query(
        "INSERT INTO agent_name_aliases (project_slug, alias, canonical, created_ts_us) \
         VALUES (?1, 'captain', ?2, ?3) \
         ON CONFLICT (project_slug, alias) DO UPDATE \
         SET canonical = excluded.canonical, created_ts_us = excluded.created_ts_us",
    )
    .bind(project_slug)
    .bind(canonical_pin)
    .bind(now_us)
    .execute(pool)
    .await?;
    Ok(())
}

/// bd-captain-alias-0g8a: set the captain fold directly in an index DB —
/// the daemon-mode CLI surface. Callers validate the pin first (whois).
pub async fn set_captain_alias_in_db(
    db_path: &str,
    project_slug: &str,
    canonical_pin: &str,
) -> anyhow::Result<()> {
    let pool = crate::index::schema::connect(db_path).await?;
    crate::index::schema::migrate(&pool).await?;
    upsert_captain_alias(&pool, project_slug, canonical_pin).await
}

/// bd-captain-alias-0g8a: read the current captain pin from an index DB.
pub async fn captain_alias_in_db(
    db_path: &str,
    project_slug: &str,
) -> anyhow::Result<Option<String>> {
    let pool = crate::index::schema::connect(db_path).await?;
    crate::index::schema::migrate(&pool).await?;
    let row: Option<String> = sqlx::query_scalar(
        "SELECT canonical FROM agent_name_aliases WHERE project_slug = ?1 AND alias = 'captain'",
    )
    .bind(project_slug)
    .fetch_optional(&pool)
    .await?;
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::nostr::identity::AgentProfile;
    use std::sync::Arc;

    const PASSPHRASE: &str = "test-passphrase";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-ident-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-ident-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE);
        (mailbox, db_path, data_dir)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("opencode".into()),
            model: Some("deepseek-v4".into()),
            task_description: Some("lane test agent".into()),
            persona: None,
        }
    }

    /// ADR-0018: a mailbox with an index pool attached (mode persistence
    /// needs the pool — same posture as `message_reads`).
    async fn test_mailbox_with_pool(
        tag: &str,
    ) -> (
        Mailbox,
        sqlx::SqlitePool,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-ident-pool-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-ident-pool-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool.clone());
        (mailbox, pool, db_path, data_dir)
    }

    // --- bd-legacy-slug-inbox-onoj: folded slugs must not keep a second inbox ---

    /// The defect scenario: a slug registered STANDALONE pre-fold keeps its
    /// own agents row beside the pin; mail sealed to the orphan npub is
    /// unreadable forever and lingers as unread. Recording the fold must
    /// ABSORB the row (idempotently): orphan mail marked read, standalone
    /// registration removed, alias routing still resolves to the pin.
    #[tokio::test]
    async fn fold_absorbs_standalone_slug_row_and_orphan_mail() {
        let (mailbox, pool, _db, _data) = test_mailbox_with_pool("onoj-absorb").await;
        let project = "p-absorb";
        mailbox.ensure_project(project).await.expect("ensure");
        let legacy = mailbox
            .register_agent(project, &profile("flywheel-p-absorb-oc9"))
            .await
            .expect("legacy slug registers (open mode)");
        // Canonicalization: the registry stores the EXPANDED slug — every
        // raw-SQL assertion below must bind THAT spelling, not my literal.
        let project = legacy.project_slug.clone();
        let pin = mailbox
            .register_agent(&project, &profile("TanFern"))
            .await
            .expect("pin");
        assert_ne!(legacy.npub, pin.npub);
        mailbox
            .register_agent(&project, &profile("PinkGorge"))
            .await
            .expect("sender");
        // The production scenario is a SWARM project (ADR-0018) — only there
        // does the fold precede every other resolution path.
        mailbox.set_identity_mode(&project, "swarm").await;
        for subject in ["[assign] high work", "fyi"] {
            mailbox
                .send_message(&crate::mailbox::messaging::SendMessage {
                    project_slug: project.clone(),
                    sender_name: "PinkGorge".into(),
                    to: vec!["flywheel-p-absorb-oc9".into()],
                    subject: subject.into(),
                    body_md: "sealed to the orphan npub".into(),
                    cc: vec![],
                    bcc: vec![],
                    importance: Some("high".into()),
                    ack_required: false,
                    thread_id: None,
                    sender_token: None,
                    overseer: false,
                    extra_tags: vec![],
                })
                .await
                .expect("send to slug");
        }

        // Pre-fold: the orphan inbox is REAL and unread — project it by
        // fetching as the slug (its key still exists at this point). The
        // `messages` projection is written at unwrap time, so this fetch is
        // also what makes the SQL count below meaningful.
        let pre = mailbox
            .fetch_inbox(&crate::mailbox::messaging::FetchInbox {
                project_slug: project.clone(),
                agent_name: "flywheel-p-absorb-oc9".into(),
                unread_only: false,
                limit: None,
                include_bodies: false,
                urgent_only: false,
                since_ts: None,
                thread_id: None,
            })
            .await
            .expect("pre-fold fetch as orphan");
        assert_eq!(pre.len(), 2, "orphan holds both messages");
        assert!(
            pre.iter().all(|m| m.read_ts_us.is_none()),
            "all orphan mail unread pre-fold"
        );

        mailbox
            .fold_agent_name_alias(&project, "flywheel-p-absorb-oc9", "TanFern")
            .await;

        // Roster: the standalone row is gone.
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM agents WHERE project_slug = ?1 AND name = 'flywheel-p-absorb-oc9'",
        )
        .bind(&project)
        .fetch_one(&pool)
        .await
        .expect("agent row count");
        assert_eq!(rows, 0, "standalone slug registration absorbed");
        // The orphan npub is no longer an inbox: routing folds to the pin,
        // and no surface keys off the dead pubkey (projection carries no
        // recipient column; cross-key delete is forbidden — nothing to mark).
        // Routing: the fold still resolves — to the PIN's key.
        let w = mailbox
            .whois(&project, "flywheel-p-absorb-oc9")
            .await
            .expect("folded slug routable");
        assert_eq!(w.npub, pin.npub, "alias resolves to the pin identity");

        // Idempotent: re-folding changes nothing and never errors.
        mailbox
            .fold_agent_name_alias(&project, "flywheel-p-absorb-oc9", "TanFern")
            .await;
        let rows_again: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM agents WHERE project_slug = ?1 AND name = 'flywheel-p-absorb-oc9'",
        )
        .bind(&project)
        .fetch_one(&pool)
        .await
        .expect("row count stable");
        assert_eq!(rows_again, 0);
    }

    /// A fold whose canonical target is NOT registered absorbs NOTHING —
    /// destroying an identity with no destination would strand its mail
    /// worse than before. The row stays; doctor names it.
    #[tokio::test]
    async fn fold_absorb_spares_row_when_canonical_unregistered() {
        let (mailbox, pool, _db, _data) = test_mailbox_with_pool("onoj-no-canonical").await;
        let project = "p-nocanon";
        mailbox.ensure_project(project).await.expect("ensure");
        mailbox
            .register_agent(project, &profile("flywheel-p-oc7"))
            .await
            .expect("legacy slug");
        mailbox
            .fold_agent_name_alias(project, "flywheel-p-oc7", "NoSuchPin")
            .await;
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM agents WHERE project_slug = ?1 AND name = 'flywheel-p-oc7'",
        )
        .bind(project)
        .fetch_one(&pool)
        .await
        .expect("row count");
        assert_eq!(rows, 1, "no canonical registration → absorb refuses");
    }

    /// Doctor's data source: leftover dual registrations (agents row whose
    /// name is a recorded fold) are listable with their unread counts.
    #[tokio::test]
    async fn folded_slug_leftovers_lists_remaining_dual_registrations() {
        let (mailbox, pool, _db, _data) = test_mailbox_with_pool("onoj-doctor").await;
        let project = "p-leftover";
        mailbox.ensure_project(project).await.expect("ensure");
        let legacy = mailbox
            .register_agent(project, &profile("flywheel-p-oc5"))
            .await
            .expect("legacy slug");
        // Registry's own slug spelling for the raw assertions below.
        let project = legacy.project_slug.clone();
        mailbox
            .fold_agent_name_alias_no_absorb(&project, "flywheel-p-oc5", "SomePin")
            .await;
        // The absorbing fold needs a LIVE canonical target (spare-row rule).
        mailbox
            .register_agent(&project, &profile("SomePin"))
            .await
            .expect("canonical pin");
        // Raw unread mail addressed to the slug.
        let ts = 1_785_726_000_000i64;
        sqlx::query(
            "INSERT INTO messages (event_id, project_slug, sender, sender_pubkey, subject, body, \
             importance, ack_required, to_list, cc_list, created_ts_us, local_seq) \
             VALUES ('ev1', ?1, 'PiAlpha', 'aaaa', '[assign]', 'body', 'high', 0, \
             '[\"flywheel-p-oc5\"]', '[]', ?2, 1)",
        )
        .bind(&project)
        .bind(ts)
        .execute(&pool)
        .await
        .expect("insert orphan message");

        let leftovers = crate::mailbox::identity::folded_slug_leftovers(&pool)
            .await
            .expect("scan");
        let hit = leftovers
            .iter()
            .find(|l| l.slug == "flywheel-p-oc5" && l.project_slug == project)
            .expect("leftover listed");
        assert_eq!(hit.canonical, "SomePin");
        assert_eq!(hit.pubkey, legacy.npub);

        // Absorbing clears the listing.
        mailbox
            .fold_agent_name_alias(&project, "flywheel-p-oc5", "SomePin")
            .await;
        let after = crate::mailbox::identity::folded_slug_leftovers(&pool)
            .await
            .expect("scan");
        assert!(
            after.iter().all(|l| l.slug != "flywheel-p-oc5"),
            "absorbed slug no longer listed: {after:?}"
        );
    }

    // --- ADR-0018: swarm-mode whois + registration hardening ---

    #[tokio::test]
    async fn swarm_mode_whois_rejects_non_canonical_names() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("swarm-whois").await;
        // Pre-ADR-0018 state: the legacy identity exists BEFORE the project
        // flips to swarm (the reconciliation scenario — nothing is deleted).
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc2"))
            .await
            .expect("register canonical");
        mailbox
            .register_agent("toron", &profile("lane-1-bootstrap"))
            .await
            .expect("legacy identity registered pre-swarm");
        mailbox.set_identity_mode("toron", "swarm").await;

        // Canonical name resolves.
        let w = mailbox
            .whois("toron", "flywheel-toron-oc2")
            .await
            .expect("canonical whois");
        assert_eq!(w.name, "flywheel-toron-oc2");

        // ADR-0018 D1a: a pin REGISTERED post-swarm is routable natively.
        let pin = mailbox
            .register_agent("toron", &profile("DustySparrow"))
            .await
            .expect("pin register in swarm mode (D1a)");
        let w_pin = mailbox
            .whois("toron", "DustySparrow")
            .await
            .expect("pin whois resolves");
        assert_eq!(
            w_pin.npub, pin.npub,
            "registered pin resolves to its own npub"
        );

        // Non-canonical name fails with an actionable grammar error.
        let err = mailbox
            .whois("toron", "lane-1-bootstrap")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("flywheel-toron-<role><seq>"),
            "error names the grammar: {msg}"
        );
        assert!(
            msg.contains("non-routable"),
            "error names the routing rule: {msg}"
        );

        // npub always resolves (the read-side twin of npub routing).
        let by_npub = mailbox.whois("toron", &w.npub).await.expect("npub whois");
        assert_eq!(by_npub.npub, w.npub);

        // list_agents still shows the legacy identity (metadata preserved).
        let agents = mailbox
            .list_agents("toron", Default::default())
            .await
            .expect("list");
        assert!(
            agents.iter().any(|a| a.name == "lane-1-bootstrap"),
            "legacy identity stays listed as display metadata"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn swarm_mode_folded_alias_resolves_through_to_canonical() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("swarm-alias").await;
        // Reconciliation order: legacy identity first (open), then swarm.
        mailbox.ensure_project("toron").await.expect("ensure");
        let canonical = mailbox
            .register_agent("toron", &profile("flywheel-toron-oc2"))
            .await
            .expect("register canonical");
        mailbox
            .register_agent("toron", &profile("lane-1-bootstrap"))
            .await
            .expect("legacy identity registered");
        mailbox.set_identity_mode("toron", "swarm").await;

        // Fold: lane-1-bootstrap → flywheel-toron-oc2 (pane binding
        // discoverable). Non-pin legacy names need the fold to resolve.
        mailbox
            .fold_agent_name_alias("toron", "lane-1-bootstrap", "flywheel-toron-oc2")
            .await;
        assert_eq!(
            mailbox
                .agent_name_alias("toron", "lane-1-bootstrap")
                .await
                .as_deref(),
            Some("flywheel-toron-oc2")
        );

        // whois on the legacy alias resolves through to the canonical npub.
        let w = mailbox
            .whois("toron", "lane-1-bootstrap")
            .await
            .expect("alias whois");
        assert_eq!(w.npub, canonical.npub, "folded alias resolves to canonical");

        // The alias is append-only: a second fold is a no-op.
        mailbox
            .fold_agent_name_alias("toron", "DustySparrow", "flywheel-toron-oc2")
            .await;
        assert_eq!(
            mailbox
                .agent_name_alias("toron", "DustySparrow")
                .await
                .as_deref(),
            Some("flywheel-toron-oc2")
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn swarm_mode_rejects_non_canonical_registration() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("swarm-register").await;
        mailbox
            .ensure_project_with_mode("toron", Some("swarm"))
            .await
            .expect("ensure swarm");

        // Canonical names register fine.
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc"))
            .await
            .expect("canonical register");
        mailbox
            .create_agent_identity("toron", &profile("flywheel-toron-oc3"))
            .await
            .expect("canonical create");

        // ADR-0018 D1a: adjective+noun PIN SHAPES register fine in swarm mode.
        mailbox
            .register_agent("toron", &profile("DustySparrow"))
            .await
            .expect("pin register (D1a)");
        mailbox
            .create_agent_identity("toron", &profile("RoseDune"))
            .await
            .expect("pin create (D1a)");

        // Non-canonical, non-pin names are rejected with an actionable error.
        let err = mailbox
            .register_agent("toron", &profile("lane-4-catalog"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("flywheel-toron-<role><seq>"),
            "register error names the grammar: {err}"
        );
        let err = mailbox
            .register_agent("toron", &profile("freeform"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("adjective+noun"),
            "register error names the pin rule: {err}"
        );
        let err = mailbox
            .create_agent_identity("toron", &profile("assistant"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("flywheel-toron-<role><seq>"),
            "create error names the grammar: {err}"
        );

        // Open mode (default) still accepts any name — legacy parity.
        let (open, _p, db_path2, data_dir2) = test_mailbox_with_pool("open-register").await;
        open.register_agent("demo", &profile("BlueLake"))
            .await
            .expect("open mode register");
        open.create_agent_identity("demo", &profile("RoseDune"))
            .await
            .expect("open mode create");
        let _ = std::fs::remove_file(&db_path2);
        let _ = std::fs::remove_dir_all(&data_dir2);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn identity_mode_persists_and_defaults_open() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("mode-persist").await;

        // Default (no row) is open.
        assert_eq!(
            mailbox.project_identity_mode("toron").await,
            crate::mailbox::naming::IdentityMode::Open
        );

        // ensure_project_with_mode persists swarm.
        mailbox
            .ensure_project_with_mode("toron", Some("swarm"))
            .await
            .expect("ensure");
        assert_eq!(
            mailbox.project_identity_mode("toron").await,
            crate::mailbox::naming::IdentityMode::Swarm
        );

        // set_identity_mode upserts (open → swarm → open).
        mailbox.set_identity_mode("toron", "open").await;
        assert_eq!(
            mailbox.project_identity_mode("toron").await,
            crate::mailbox::naming::IdentityMode::Open
        );
        mailbox.set_identity_mode("toron", "swarm").await;
        assert_eq!(
            mailbox.project_identity_mode("toron").await,
            crate::mailbox::naming::IdentityMode::Swarm
        );

        // ensure_project (no mode) does not clobber an existing mode.
        mailbox.ensure_project("toron").await.expect("ensure plain");
        assert_eq!(
            mailbox.project_identity_mode("toron").await,
            crate::mailbox::naming::IdentityMode::Swarm
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_create_agent_identity_mints_key_and_publishes() {
        let (mailbox, db_path, data_dir) = test_mailbox("create").await;

        let a = mailbox
            .create_agent_identity("demo", &profile("BlueLake"))
            .await
            .expect("create");
        assert!(a.created, "create always mints fresh");
        assert_eq!(a.name, "BlueLake");
        assert_eq!(a.project_slug, "demo");
        assert_eq!(a.npub.len(), 64, "hex npub");
        assert!(!a.profile_event_id.is_empty());
        assert!(
            std::path::Path::new(&a.key_path).exists(),
            "ncryptsec written"
        );
        assert_eq!(
            mailbox
                .agent_key_path("demo", "BlueLake")
                .display()
                .to_string(),
            a.key_path
        );

        // whois resolves the name → npub.
        let w = mailbox.whois("demo", "BlueLake").await.expect("whois");
        assert_eq!(w.npub, a.npub);
        assert_eq!(w.program.as_deref(), Some("opencode"));
        assert_eq!(w.model.as_deref(), Some("deepseek-v4"));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_create_always_different_npub_or_refuses_collision() {
        let (mailbox, db_path, data_dir) = test_mailbox("fresh").await;
        // AM-Rust parity: create_agent_identity with an unavailable name must
        // FAIL (a name_hint collision is refused), not mint a colliding key.
        let a = mailbox
            .create_agent_identity("demo", &profile("A"))
            .await
            .expect("create a");
        let err = mailbox
            .create_agent_identity("demo", &profile("A"))
            .await
            .expect_err("second create for same name must be refused");
        assert!(
            err.to_string().contains("already exists"),
            "collision refused: {err}"
        );
        // The original identity is untouched (whois still resolves to `a.npub`).
        let w = mailbox.whois("demo", "A").await.expect("whois after");
        assert_eq!(w.npub, a.npub, "collision must not rotate the npub");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_register_reuses_key_idempotent() {
        let (mailbox, db_path, data_dir) = test_mailbox("register").await;

        let first = mailbox
            .register_agent("demo", &profile("RoseDune"))
            .await
            .expect("register first");
        assert!(first.created, "first register creates");

        let second = mailbox
            .register_agent("demo", &profile("RoseDune"))
            .await
            .expect("register second");
        assert!(!second.created, "second register reuses the key");
        assert_eq!(first.npub, second.npub, "same npub on upsert");
        assert_ne!(
            first.profile_event_id, second.profile_event_id,
            "new profile event each time"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_unknown_name_fails_fast() {
        let (mailbox, db_path, data_dir) = test_mailbox("unknown").await;
        let err = mailbox
            .whois("demo", "Ghost")
            .await
            .expect_err("unknown name");
        assert!(
            err.to_string().contains("unknown agent name"),
            "fail fast: {err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_list_agents_dedups_and_filters_active() {
        let (mailbox, db_path, data_dir) = test_mailbox("list").await;

        mailbox
            .create_agent_identity("demo", &profile("BlueLake"))
            .await
            .expect("blue");
        mailbox
            .create_agent_identity("demo", &profile("RoseDune"))
            .await
            .expect("rose");

        let all = mailbox
            .list_agents("demo", ListAgentsOptions::default())
            .await
            .expect("list");
        assert_eq!(all.len(), 2, "two distinct names");
        assert!(all.iter().any(|a| a.name == "BlueLake"));
        assert!(all.iter().any(|a| a.name == "RoseDune"));
        assert!(all.iter().all(|a| a.npub.len() == 64));

        // active-within filter: profiles are fresh (created now) → visible.
        let active = mailbox
            .list_agents(
                "demo",
                ListAgentsOptions {
                    active_within_days: Some(7),
                },
            )
            .await
            .expect("active");
        assert_eq!(active.len(), 2, "fresh agents active within 7d");

        // Impossible cutoff → none.
        let none = mailbox
            .list_agents(
                "demo",
                ListAgentsOptions {
                    active_within_days: Some(0),
                },
            )
            .await
            .expect("none");
        assert!(none.is_empty(), "active_within_days=0 excludes everything");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_key_file_is_0600_and_contains_no_nsec() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let (mailbox, db_path, data_dir) = test_mailbox("0600").await;
            let agent = mailbox
                .create_agent_identity("demo", &profile("BlueLake"))
                .await
                .expect("create");
            let meta = std::fs::metadata(&agent.key_path).expect("meta");
            assert_eq!(meta.permissions().mode() & 0o777, 0o600, "0600");
            let contents = std::fs::read_to_string(&agent.key_path).expect("read");
            assert!(!contents.contains("nsec1"), "no bech32 nsec in key file");
            assert!(contents.starts_with("ncryptsec1"), "ncryptsec payload");
            let _ = std::fs::remove_file(&db_path);
            let _ = std::fs::remove_dir_all(&data_dir);
        }
    }

    #[test]
    fn test_list_agents_options_default() {
        let o = ListAgentsOptions::default();
        assert!(o.active_within_days.is_none());
    }

    // ---- bd-cmgu → ADR-0017: MCP vs CLI project-key unification ----
    //
    // The flywheel CLI keys projects by ABSOLUTE PATH slug
    // ("/Users/.../workspace/toron"), the MCP surface by the short key
    // ("toron") — two roots under one data dir. An agent registered via one
    // surface was invisible to the other ("no local key"). ADR-0017 makes the
    // canonical short slug the ONE root: every spelling resolves to the same
    // key file under `{data_dir}/{canonical}/agents/`.

    /// Incident shape (CLI-registered -> MCP lookup): register under the
    /// PATH-style slug, look the key up via the SHORT slug — the key lands
    /// in the canonical root and both spellings resolve it.
    #[tokio::test]
    async fn test_agent_keys_for_short_key_finds_path_slug_registered_agent() {
        let (mailbox, db_path, data_dir) = test_mailbox("cmgu-short").await;
        let path_slug = "tmp/x/workspace/demo"; // relative path form (leading / stripped)
        let agent = mailbox
            .register_agent(path_slug, &profile("RustHeron"))
            .await
            .expect("register under path slug");
        assert_eq!(agent.project_slug, "demo", "canonical root, ADR-0017");
        // ADR-0017: the key lands in the CANONICAL root — no split.
        let short_path = mailbox.agent_key_path("demo", "RustHeron");
        assert!(short_path.exists(), "canonical-root key exists");
        let keys = mailbox
            .agent_keys_for("demo", "RustHeron")
            .expect("resolve via short slug");
        assert_eq!(keys.public_key().to_hex(), agent.npub, "same key");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Reverse direction (MCP-registered -> CLI lookup): register under the
    /// SHORT slug, look the key up via the PATH-style slug.
    #[tokio::test]
    async fn test_agent_keys_for_path_slug_finds_short_slug_registered_agent() {
        let (mailbox, db_path, data_dir) = test_mailbox("cmgu-path").await;
        let agent = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("register under short slug");
        let path_slug = "tmp/x/workspace/demo";
        // ADR-0017: the path-form spelling resolves to the canonical root —
        // the key path IS the short-slug path (no separate root exists).
        assert_eq!(
            mailbox.agent_key_path(path_slug, "BlueLake"),
            mailbox.agent_key_path("demo", "BlueLake"),
            "both spellings, one key path"
        );
        let keys = mailbox
            .agent_keys_for(path_slug, "BlueLake")
            .expect("resolve via path slug");
        assert_eq!(keys.public_key().to_hex(), agent.npub, "same key");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// ADR-0017: both spellings resolve to the SAME canonical identity (no
    /// cross-root shadowing — there is only one root), and an unknown name
    /// still fails with the original error.
    #[tokio::test]
    async fn test_agent_keys_for_exact_slug_wins_and_unknown_still_errors() {
        let (mailbox, db_path, data_dir) = test_mailbox("cmgu-exact").await;
        let agent = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("register");
        // Re-registering via a basename-colliding path slug UPSERTS the same
        // canonical identity — never a second keypair.
        let other = mailbox
            .register_agent("tmp/x/workspace/demo", &profile("BlueLake"))
            .await
            .expect("register alt spelling");
        assert_eq!(other.npub, agent.npub, "one identity per name");
        let keys = mailbox
            .agent_keys_for("demo", "BlueLake")
            .expect("exact wins");
        assert_eq!(keys.public_key().to_hex(), agent.npub, "canonical key");
        let err = mailbox
            .agent_keys_for("demo", "NoSuchAgent")
            .expect_err("still fails for unknown names");
        assert!(
            err.to_string().contains("has no local key in project demo"),
            "original error shape: {err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-f5tt: a project root nested under ANOTHER project root is still
    /// found — the walk must descend into roots, not shadow them
    /// (`<data>/tmp/x/workspace` holds `agents/` while
    /// `<data>/tmp/x/workspace/demo` is the real project).
    #[tokio::test]
    async fn test_alt_agent_key_descends_into_shadowing_root() {
        let (mailbox, db_path, data_dir) = test_mailbox("f5tt-shadow").await;
        // Register under the NESTED path slug.
        let nested = "tmp/x/workspace/demo";
        let agent = mailbox
            .register_agent(nested, &profile("DustyWren"))
            .await
            .expect("register nested root");
        // Create the SHADOWING root: a project root at the PARENT path
        // (has agents/) whose basename is NOT the wanted one. Under the
        // non-descending walk this hid the nested root.
        mailbox
            .register_agent("tmp/x/workspace", &profile("ShadowRoot"))
            .await
            .expect("register shadowing parent root");
        let keys = mailbox
            .agent_keys_for("demo", "DustyWren")
            .expect("fallback finds the nested root through the shadow");
        assert_eq!(keys.public_key().to_hex(), agent.npub, "same key");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // --- bd-pin-public-plane-s-easy.1: resolve_agent_name (one resolver) ---

    #[tokio::test]
    async fn resolve_agent_name_folds_slug_to_pin() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("resolve-slug").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        let pin = mailbox
            .register_agent("toron", &profile("BlueRidge"))
            .await
            .expect("register pin");
        mailbox.set_identity_mode("toron", "swarm").await;
        mailbox
            .fold_agent_name_alias("toron", "flywheel-toron-coder", "BlueRidge")
            .await;

        // whois(slug) must resolve to the PIN's npub after a fold exists.
        let w = mailbox
            .whois("toron", "flywheel-toron-coder")
            .await
            .expect("slug whois resolves via fold");
        assert_eq!(w.npub, pin.npub, "whois(slug) resolves to the pin's npub");

        // whois(pin) still resolves natively.
        let w_pin = mailbox
            .whois("toron", "BlueRidge")
            .await
            .expect("pin whois");
        assert_eq!(w_pin.npub, pin.npub);

        // resolve_agent_name returns the pin for the slug and the pin itself.
        assert_eq!(
            mailbox
                .resolve_agent_name("toron", "flywheel-toron-coder")
                .await
                .expect("resolve slug"),
            "BlueRidge"
        );
        assert_eq!(
            mailbox
                .resolve_agent_name("toron", "BlueRidge")
                .await
                .expect("resolve pin"),
            "BlueRidge"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // --- bd-captain-alias-0g8a: `captain` folds onto the orchestrator pin ---

    #[tokio::test]
    async fn captain_alias_set_resolves_and_routes() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("captain-set").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        let pin = mailbox
            .register_agent("toron", &profile("BlueRidge"))
            .await
            .expect("register pin");
        mailbox.set_identity_mode("toron", "swarm").await;

        let folded = mailbox
            .set_project_captain("toron", "BlueRidge")
            .await
            .expect("set captain");
        assert_eq!(folded, "BlueRidge");

        // The fence surface: whois(project, "captain") hits the pin's npub.
        let w = mailbox
            .whois("toron", "captain")
            .await
            .expect("whois captain resolves via fold");
        assert_eq!(w.npub, pin.npub);

        // The send path shares the ONE resolver.
        assert_eq!(
            mailbox
                .resolve_agent_name("toron", "captain")
                .await
                .expect("send-path resolution"),
            "BlueRidge"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn captain_alias_upsert_repoints_to_new_orchestrator() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("captain-repoint").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("BlueRidge"))
            .await
            .expect("register first pin");
        mailbox
            .register_agent("toron", &profile("AmberPlateau"))
            .await
            .expect("register second pin");
        mailbox.set_identity_mode("toron", "swarm").await;

        mailbox
            .set_project_captain("toron", "BlueRidge")
            .await
            .expect("set first captain");
        mailbox
            .set_project_captain("toron", "AmberPlateau")
            .await
            .expect("repoint captain");

        // Upsert semantics: the CURRENT orchestrator wins, not first-fold.
        assert_eq!(
            mailbox
                .resolve_agent_name("toron", "captain")
                .await
                .expect("resolve repointed alias"),
            "AmberPlateau"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn set_captain_rejects_host_username() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("captain-user").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox.set_identity_mode("toron", "swarm").await;

        let err = mailbox
            .set_project_captain("toron", "bangedorrunt")
            .await
            .expect_err("host username is not an identity");
        let msg = err.to_string();
        assert!(msg.contains("username"), "names the class: {msg}");
        assert!(msg.contains("not a mailbox identity"), "{msg}");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn set_captain_rejects_unregistered_pin() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("captain-reg").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox.set_identity_mode("toron", "swarm").await;

        let err = mailbox
            .set_project_captain("toron", "NoSuchPin")
            .await
            .expect_err("unregistered pins do not route");
        let msg = err.to_string();
        assert!(
            msg.contains("REGISTERED"),
            "error names the registered-pin requirement: {msg}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn resolve_agent_name_rejects_unregistered_free_strings() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("resolve-free").await;
        mailbox
            .ensure_project_with_mode("toron", Some("swarm"))
            .await
            .expect("ensure swarm");
        let err = mailbox
            .resolve_agent_name("toron", "NoSuchAgent")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("non-routable"),
            "unregistered free string rejected: {err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn resolve_agent_name_folded_legacy_alias_resolves() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("resolve-legacy").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc2"))
            .await
            .expect("register canonical");
        mailbox.set_identity_mode("toron", "swarm").await;
        mailbox
            .fold_agent_name_alias("toron", "lane-1-bootstrap", "flywheel-toron-oc2")
            .await;
        assert_eq!(
            mailbox
                .resolve_agent_name("toron", "lane-1-bootstrap")
                .await
                .expect("resolve legacy alias"),
            "flywheel-toron-oc2"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-pin-public-plane-s-easy.1: registration must NOT create the slug as
    /// a second identity — the fold (slug → pin) is the only routing. A slug
    /// with an existing fold is refused at register/create.
    #[tokio::test]
    async fn register_refuses_folded_slug_as_second_identity() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("resolve-register").await;
        mailbox
            .ensure_project_with_mode("toron", Some("swarm"))
            .await
            .expect("ensure swarm");
        mailbox
            .register_agent("toron", &profile("BlueRidge"))
            .await
            .expect("register pin");
        mailbox
            .fold_agent_name_alias("toron", "flywheel-toron-coder", "BlueRidge")
            .await;

        // Registering the folded slug must be refused (no second identity).
        let err = mailbox
            .register_agent("toron", &profile("flywheel-toron-coder"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("fold"),
            "register refuses folded slug: {err}"
        );
        // create_agent_identity likewise.
        let err = mailbox
            .create_agent_identity("toron", &profile("flywheel-toron-coder"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("fold"),
            "create refuses folded slug: {err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-pin-public-plane-s-easy.8: an OS username ($USER) is NOT a mailbox
    /// identity — whois must name the class and the recovery (registered
    /// pin / list_agents), never hint at registering the username.
    #[tokio::test]
    async fn whois_username_class_rejects_with_recovery() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("whois-user").await;
        mailbox
            .ensure_project_with_mode("toron", Some("swarm"))
            .await
            .expect("ensure swarm");
        mailbox
            .register_agent("toron", &profile("BlueRidge"))
            .await
            .expect("register pin");

        let err = mailbox.whois("toron", "bangedorrunt").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("username"),
            "error names the username class: {msg}"
        );
        assert!(
            msg.contains("not a mailbox identity"),
            "error says username ≠ mailbox identity: {msg}"
        );
        assert!(
            msg.contains("BlueRidge") || msg.contains("pin") || msg.contains("list_agents"),
            "error names the recovery (pin / list_agents): {msg}"
        );

        // The same class rejects through resolve_agent_name (send path too).
        let err = mailbox
            .resolve_agent_name("toron", "bangedorrunt")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("username"),
            "resolve_agent_name names the username class: {err}"
        );

        // A registered pin still resolves (the username path did not break
        // normal routing).
        let w = mailbox
            .whois("toron", "BlueRidge")
            .await
            .expect("pin still resolves");
        assert_eq!(w.name, "BlueRidge");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-f5tt: `project_slug_aliases` returns the given slug plus every
    /// basename-colliding project root, both spellings of a path slug.
    #[tokio::test]
    async fn test_project_slug_aliases_covers_both_spellings() {
        let (mailbox, db_path, data_dir) = test_mailbox("f5tt-alias").await;
        // ADR-0017: fresh registrations land in the canonical root only — a
        // path-form registration creates NO split root on disk.
        mailbox
            .register_agent("tmp/x/workspace/demo", &profile("AliasA"))
            .await
            .expect("register path root");
        mailbox
            .register_agent("demo", &profile("AliasB"))
            .await
            .expect("register short root");
        // The walk-based alias still folds a LEGACY path-slug root left by a
        // pre-ADR-0017 daemon (the migration read path).
        let legacy = mailbox.data_dir().join("tmp/legacy/workspace/demo");
        std::fs::create_dir_all(legacy.join("agents")).unwrap();
        let aliases = mailbox.project_slug_aliases("demo");
        assert!(
            aliases.contains(&"demo".to_string()),
            "the given slug is always an alias: {aliases:?}"
        );
        assert!(
            aliases.contains(&"tmp/legacy/workspace/demo".to_string()),
            "legacy path-slug root found via basename: {aliases:?}"
        );
        // The fresh path-form spelling exists only as the canonical root.
        let fresh = mailbox.project_slug_aliases("tmp/x/workspace/demo");
        assert!(
            fresh.contains(&"demo".to_string()),
            "reverse direction: {fresh:?}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
