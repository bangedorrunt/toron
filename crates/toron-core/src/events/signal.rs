// governed-by: ADR-0039
//! bd-2ghz8: count-only signal links for hooks. A hook holds no read token —
//! it holds one short-lived secret that can learn only counts for ONE
//! participant. The secret is shown once at mint and stored only as its
//! SHA-256 hash, exactly like an events token. It never appears in a URL or a
//! journal; it travels in `Authorization: Bearer`.
//!
//! The kernel is pure over an injected store and clock: mint/rotate/revoke and
//! resolve are unit-tested with a fake store and a fixed clock.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A signal link lives for 24 hours, then refuses even though its hash remains
/// on disk until the next rotate/revoke sweep.
pub const SIGNAL_TTL_US: i64 = 24 * 60 * 60 * 1_000_000;
pub const SIGNAL_SECRET_BYTES: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalLink {
    pub hash: String,
    /// The participant (pin or operator) this link may see counts for.
    pub participant: String,
    pub created_us: i64,
    pub expires_us: i64,
    pub revoked: bool,
}

/// The count-only view a hook may read. No ids, no bodies, no hints.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalCounts {
    pub pending: u64,
    pub urgent: u64,
    pub waking: u64,
    pub cursor: u64,
    pub expires_at: i64,
}

pub trait SignalStore: Send + Sync {
    fn get(&self, hash: &str) -> anyhow::Result<Option<SignalLink>>;
    /// Revoke every live link for `participant` and insert `link` in ONE
    /// critical section, so a re-create never leaves two live secrets.
    fn rotate(&self, participant: &str, link: &SignalLink) -> anyhow::Result<()>;
    fn revoke(&self, participant: &str) -> anyhow::Result<usize>;
}

/// Hash a plaintext secret exactly as it is stored.
pub fn hash_secret(plaintext: &str) -> String {
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(plaintext.as_bytes()))
}

fn new_secret() -> anyhow::Result<String> {
    let mut bytes = [0u8; SIGNAL_SECRET_BYTES];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("secret generation failed: {e}"))?;
    Ok(hex::encode(bytes))
}

/// Mint a fresh link for `participant`, rotating any previous one. Returns the
/// plaintext secret ONCE; only its hash is persisted.
pub fn mint_signal(
    participant: &str,
    now_us: i64,
    store: &dyn SignalStore,
) -> anyhow::Result<String> {
    let plaintext = new_secret()?;
    let link = SignalLink {
        hash: hash_secret(&plaintext),
        participant: participant.to_string(),
        created_us: now_us,
        expires_us: now_us.saturating_add(SIGNAL_TTL_US),
        revoked: false,
    };
    store.rotate(participant, &link)?;
    Ok(plaintext)
}

/// Resolve a Bearer secret to its participant. Expired and revoked links refuse.
pub fn resolve_signal(
    authorization: Option<&str>,
    now_us: i64,
    store: &dyn SignalStore,
) -> Option<String> {
    let raw = authorization?.strip_prefix("Bearer ")?;
    let link = store.get(&hash_secret(raw.trim())).ok()??;
    if link.revoked || link.expires_us <= now_us {
        return None;
    }
    Some(link.participant)
}

/// Revoke every live link for a participant. Returns how many were revoked.
pub fn revoke_signal(participant: &str, store: &dyn SignalStore) -> anyhow::Result<usize> {
    store.revoke(participant)
}

/// JSON file at 0600 under the durable root, one locked read-modify-write per
/// mutation. Modelled on `principal::FileTokenStore`.
pub struct FileSignalStore {
    path: PathBuf,
}

impl FileSignalStore {
    pub fn at_root(root: &std::path::Path) -> Self {
        Self {
            path: root.join("signal_links.json"),
        }
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

fn read(path: &std::path::Path) -> anyhow::Result<Vec<SignalLink>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn write(path: &std::path::Path, rows: &[SignalLink]) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    use std::io::Write as _;
    file.write_all(&serde_json::to_vec_pretty(rows)?)?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    Ok(())
}

struct LockGuard {
    _file: std::fs::File,
}

fn exclusive_lock(path: &std::path::Path) -> anyhow::Result<LockGuard> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let lock = path.with_extension("lock");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock)?;
    file.lock()?;
    Ok(LockGuard { _file: file })
}

impl SignalStore for FileSignalStore {
    fn get(&self, hash: &str) -> anyhow::Result<Option<SignalLink>> {
        Ok(read(&self.path)?.into_iter().find(|r| r.hash == hash))
    }

    fn rotate(&self, participant: &str, link: &SignalLink) -> anyhow::Result<()> {
        let _lock = exclusive_lock(&self.path)?;
        let mut rows = read(&self.path)?;
        for row in rows.iter_mut().filter(|r| r.participant == participant) {
            row.revoked = true;
        }
        rows.push(link.clone());
        write(&self.path, &rows)
    }

    fn revoke(&self, participant: &str) -> anyhow::Result<usize> {
        let _lock = exclusive_lock(&self.path)?;
        let mut rows = read(&self.path)?;
        let mut n = 0;
        for row in rows
            .iter_mut()
            .filter(|r| r.participant == participant && !r.revoked)
        {
            row.revoked = true;
            n += 1;
        }
        if n > 0 {
            write(&self.path, &rows)?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct Fake {
        rows: Mutex<Vec<SignalLink>>,
    }

    impl SignalStore for Fake {
        fn get(&self, hash: &str) -> anyhow::Result<Option<SignalLink>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.hash == hash)
                .cloned())
        }
        fn rotate(&self, participant: &str, link: &SignalLink) -> anyhow::Result<()> {
            let mut rows = self.rows.lock().unwrap();
            for row in rows.iter_mut().filter(|r| r.participant == participant) {
                row.revoked = true;
            }
            rows.push(link.clone());
            Ok(())
        }
        fn revoke(&self, participant: &str) -> anyhow::Result<usize> {
            let mut rows = self.rows.lock().unwrap();
            let mut n = 0;
            for row in rows
                .iter_mut()
                .filter(|r| r.participant == participant && !r.revoked)
            {
                row.revoked = true;
                n += 1;
            }
            Ok(n)
        }
    }

    #[test]
    fn a_minted_secret_is_shown_once_and_stored_only_hashed() {
        let store = Fake::default();
        let secret = mint_signal("SlateHarbor", 1_000, &store).unwrap();
        assert_eq!(secret.len(), SIGNAL_SECRET_BYTES * 2);
        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(rows[0].hash, secret, "the plaintext is never stored");
        assert_eq!(rows[0].hash, hash_secret(&secret));
        assert_eq!(rows[0].expires_us, 1_000 + SIGNAL_TTL_US);
        assert!(!rows[0].revoked);
    }

    #[test]
    fn resolve_accepts_only_the_bearer_secret_that_is_live() {
        let store = Fake::default();
        let secret = mint_signal("SlateHarbor", 1_000, &store).unwrap();
        assert_eq!(
            resolve_signal(Some(&format!("Bearer {secret}")), 1_001, &store).as_deref(),
            Some("SlateHarbor")
        );
        assert_eq!(resolve_signal(None, 1_001, &store), None);
        assert_eq!(resolve_signal(Some("Bearer wrong"), 1_001, &store), None);
        assert_eq!(
            resolve_signal(Some(&secret), 1_001, &store),
            None,
            "no scheme"
        );
    }

    #[test]
    fn expiry_and_revoke_refuse() {
        let store = Fake::default();
        let secret = mint_signal("SlateHarbor", 1_000, &store).unwrap();
        assert_eq!(
            resolve_signal(
                Some(&format!("Bearer {secret}")),
                1_000 + SIGNAL_TTL_US,
                &store
            ),
            None,
            "the link refuses exactly at expiry"
        );
        let secret2 = mint_signal("BoldCliff", 2_000, &store).unwrap();
        assert_eq!(revoke_signal("BoldCliff", &store).unwrap(), 1);
        assert_eq!(
            resolve_signal(Some(&format!("Bearer {secret2}")), 2_001, &store),
            None
        );
    }

    #[test]
    fn recreate_rotates_the_secret_and_revokes_the_old_one() {
        let store = Fake::default();
        let old = mint_signal("SlateHarbor", 1_000, &store).unwrap();
        let fresh = mint_signal("SlateHarbor", 1_100, &store).unwrap();
        assert_ne!(old, fresh);
        assert_eq!(
            resolve_signal(Some(&format!("Bearer {old}")), 1_101, &store),
            None,
            "the old secret is dead after rotation"
        );
        assert_eq!(
            resolve_signal(Some(&format!("Bearer {fresh}")), 1_101, &store).as_deref(),
            Some("SlateHarbor")
        );
    }

    #[test]
    fn hashes_are_distinct_per_secret() {
        assert_ne!(hash_secret("a"), hash_secret("b"));
        assert_eq!(hash_secret("a"), hash_secret("a"));
    }
}
