// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! NIP-49 `ncryptsec` key store (G1).
//!
//! Generate keypairs, encrypt to `ncryptsec` with a passphrase, write 0600
//! files, and load+decrypt into process memory. The nsec NEVER leaves this
//! module unencrypted: callers receive a [`Keys`] handle whose `Debug` is
//! redacted (public key only) — never `{:#?}` a `SecretKey` (it leaks).

use std::fs;
use std::path::Path;

use crate::nostr::SecretKey;
use ::nostr::nips::nip19::{FromBech32, ToBech32};
use ::nostr::nips::nip49::{EncryptedSecretKey, KeySecurity};

/// Default argon2 log-n rounds for production (slow, ~1s).
pub const DEFAULT_LOG_N: u8 = 16;
/// Fast log-n for tests / CI (faster, ~ms).
pub const TEST_LOG_N: u8 = 10;

/// Error returned by the key store.
#[derive(Debug, thiserror::Error)]
pub enum KeyStoreError {
    #[error("nostr key failure: {0}")]
    Nostr(#[from] ::nostr::error::Error),
    #[error("io failure: {0}")]
    Io(#[from] std::io::Error),
}

/// Generate a fresh keypair.
pub fn generate() -> Keys {
    Keys::generate()
}

/// Encrypt a keypair's secret key to `ncryptsec` (bech32) with a passphrase.
pub fn encrypt(keys: &Keys, password: &str, log_n: u8) -> Result<String, KeyStoreError> {
    let esk = EncryptedSecretKey::new(keys.secret_key(), password, log_n, KeySecurity::Medium)?;
    Ok(esk.to_bech32()?)
}

/// Decrypt an `ncryptsec` (bech32) payload into a [`Keys`] handle.
pub fn decrypt(cryptsec: &str, password: &str) -> Result<Keys, KeyStoreError> {
    let esk = EncryptedSecretKey::from_bech32(cryptsec)?;
    let secret: SecretKey = esk.decrypt(password)?;
    Ok(Keys::new(secret))
}

/// Write an encrypted key to `path` with mode 0600 (POSIX).
///
/// Creates parent directories as needed. On non-POSIX platforms the mode call
/// is a no-op (best effort).
pub fn write_encrypted_key(
    path: &Path,
    keys: &Keys,
    password: &str,
    log_n: u8,
) -> Result<(), KeyStoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let cryptsec = encrypt(keys, password, log_n)?;
    fs::write(path, cryptsec.as_bytes())?;
    set_0600(path)?;
    Ok(())
}

/// Load and decrypt an encrypted key file into process memory.
pub fn load_encrypted_key(path: &Path, password: &str) -> Result<Keys, KeyStoreError> {
    let cryptsec = fs::read_to_string(path)?;
    decrypt(&cryptsec, password)
}

/// Best-effort chmod 0600. On non-POSIX this is a no-op (returning Ok).
#[cfg(unix)]
fn set_0600(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_0600(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

/// The signing keypair type used by the mailbox operator + agents.
/// Re-exported so downstream crates (toron-mcp) can name it without a direct
/// nostr-sdk dependency.
pub use crate::nostr::Keys;
pub use ::nostr::event::SignEvent;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(name: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("toron-keys-{}-{}-{name}", std::process::id(), n))
    }

    #[test]
    fn test_generate_produces_valid_keypair() {
        let keys = generate();
        assert_eq!(keys.secret_key().secret_bytes().len(), 32);
        assert_eq!(keys.public_key().to_hex().len(), 64);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let keys = generate();
        let cryptsec = encrypt(&keys, "hunter2", TEST_LOG_N).expect("encrypt");
        assert!(cryptsec.starts_with("ncryptsec1"));
        let loaded = decrypt(&cryptsec, "hunter2").expect("decrypt");
        assert_eq!(loaded.public_key(), keys.public_key());
    }

    #[test]
    fn test_wrong_passphrase_fails() {
        let keys = generate();
        let cryptsec = encrypt(&keys, "right-password", TEST_LOG_N).expect("encrypt");
        assert!(decrypt(&cryptsec, "wrong-password").is_err());
    }

    #[test]
    fn test_write_load_file_roundtrip() {
        let keys = generate();
        let path = temp_path("roundtrip.ncryptsec");
        write_encrypted_key(&path, &keys, "pass", TEST_LOG_N).expect("write");
        let loaded = load_encrypted_key(&path, "pass").expect("load");
        assert_eq!(loaded.public_key(), keys.public_key());
        let _ = fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn test_file_mode_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let keys = generate();
        let path = temp_path("mode.ncryptsec");
        write_encrypted_key(&path, &keys, "pass", TEST_LOG_N).expect("write");
        let mode = fs::metadata(&path).expect("meta").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "file mode must be 0600, got {mode:o}");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_nsec_not_in_serialized_events_or_cryptsec() {
        let keys = generate();
        // The cryptsec payload must not contain the hex or bech32 nsec.
        let cryptsec = encrypt(&keys, "pass", TEST_LOG_N).expect("encrypt");
        let nsec_bech32 = keys.secret_key().to_bech32().expect("nsec bech32");
        assert!(!cryptsec.contains(&nsec_bech32));
        assert!(!cryptsec.contains(&keys.secret_key().to_secret_hex()));

        // A signed event must not contain the nsec either.
        use crate::nostr::event::build_event;
        use crate::nostr::{Kind, Tag, Timestamp};
        let ev = build_event(
            &keys,
            Kind::TextNote,
            "hello",
            Vec::<Tag>::new(),
            Timestamp::now(),
        )
        .expect("build");
        let json = ev.as_json();
        assert!(!json.contains(&nsec_bech32));
        assert!(!json.contains(&keys.secret_key().to_secret_hex()));
    }

    #[test]
    fn test_keys_debug_does_not_leak_nsec() {
        let keys = generate();
        let debug = format!("{keys:?}");
        // `Keys` Debug prints only the public key; the nsec must not appear.
        assert!(!debug.contains("sec"));
        assert!(!debug.contains(&keys.secret_key().to_secret_hex()));
        assert!(debug.contains(&keys.public_key().to_hex()));
    }

    #[test]
    fn test_operator_key_roundtrip_via_env_path_semantics() {
        // TORON_OPERATOR_NSEC is a PATH, not a secret (CLI design).
        let keys = generate();
        let path = temp_path("operator.ncryptsec");
        write_encrypted_key(&path, &keys, "op-pass", TEST_LOG_N).expect("write");
        let loaded = load_encrypted_key(&path, "op-pass").expect("load");
        assert_eq!(loaded.public_key(), keys.public_key());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_decrypt_corrupt_payload_fails() {
        assert!(decrypt("ncryptsec1corrupt", "pass").is_err());
    }
}
