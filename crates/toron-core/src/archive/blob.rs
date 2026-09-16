// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Content-addressed blob store for attachments (ADR G4).
//!
//! * Blobs live at `blobs/sha256/<hash>` inside the project archive.
//! * A kind-15 file message's `content` is a `toron-blob://` URL; tags carry
//!   `["x", sha256]`, `["file-type", mime]`, optional `["decryption-key", key]`.
//! * Small text/JSON payloads embed inline in the rumor content; large
//!   payloads are never inlined (relay size caps). Blossom/NIP-96 federation
//!   is Phase-2 (out of scope here).

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Default inline cap for text/JSON payloads (bytes). Above this, a payload
/// must go to the blob store instead of the rumor content.
pub const INLINE_CAP_BYTES: usize = 8 * 1024;

/// Scheme prefix for content-addressed attachment references.
pub const BLOB_SCHEME: &str = "toron-blob://";

/// sha256 hex of a byte slice.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Canonical blob path under a project archive: `blobs/sha256/<hash>`.
pub fn blob_path(project_root: &Path, hash: &str) -> PathBuf {
    project_root.join("blobs").join("sha256").join(hash)
}

/// A kind-15 file-message reference (ADR G4 wire shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRef {
    pub url: String,
    pub sha256: String,
    pub file_type: Option<String>,
    pub decryption_key: Option<String>,
}

impl FileRef {
    /// `toron-blob://<hash>` reference.
    pub fn blob_url(hash: &str) -> String {
        format!("{BLOB_SCHEME}{hash}")
    }

    pub fn new(hash: &str, file_type: Option<String>, decryption_key: Option<String>) -> Self {
        Self {
            url: Self::blob_url(hash),
            sha256: hash.to_string(),
            file_type,
            decryption_key,
        }
    }

    /// The kind-15 tag set: `x`=sha256, `file-type`, optional
    /// `decryption-key` (NIP-17 file-message shape).
    pub fn tags(&self) -> Vec<crate::nostr::Tag> {
        let mut tags = vec![
            crate::nostr::Tag::parse(["x", &self.sha256]).expect("x tag"),
            crate::nostr::Tag::parse(["file-type", &self.file_type.clone().unwrap_or_default()])
                .expect("file-type tag"),
        ];
        if let Some(k) = &self.decryption_key {
            tags.push(crate::nostr::Tag::parse(["decryption-key", k]).expect("decryption-key tag"));
        }
        tags
    }
}

/// Content-addressed blob store on the archive.
pub struct BlobStore {
    project_root: PathBuf,
}

impl BlobStore {
    pub const fn new(project_root: PathBuf) -> Self {
        Self { project_root }
    }

    /// Store a blob; returns its sha256. Idempotent by content addressing.
    pub fn put(&self, data: &[u8]) -> std::io::Result<String> {
        self.put_with_type(data, None)
    }

    /// Store a blob with an optional MIME-type hint (ADR-0006 / BUD-02
    /// descriptor `type`). Idempotent by content addressing. When a `type` is
    /// given and not already recorded, it is persisted as a `<hash>.type`
    /// sidecar so a later `get_type` is accurate.
    pub fn put_with_type(&self, data: &[u8], mime: Option<&str>) -> std::io::Result<String> {
        let hash = sha256_hex(data);
        let path = blob_path(&self.project_root, &hash);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, data)?;
        }
        if let Some(mime) = mime {
            let sidecar = self.type_sidecar_path(&hash);
            if !sidecar.exists() {
                std::fs::write(sidecar, mime)?;
            }
        }
        Ok(hash)
    }

    /// Fetch a blob by sha256. Err if absent.
    pub fn get(&self, hash: &str) -> std::io::Result<Vec<u8>> {
        let path = blob_path(&self.project_root, hash);
        std::fs::read(path)
    }

    /// True if a blob with `hash` exists on disk.
    pub fn exists(&self, hash: &str) -> bool {
        blob_path(&self.project_root, hash).is_file()
    }

    /// The recorded MIME-type hint for `hash` (from the `<hash>.type`
    /// sidecar), if any. The caller decides the `application/octet-stream`
    /// default (BUD-01/BUD-02).
    pub fn get_type(&self, hash: &str) -> Option<String> {
        std::fs::read_to_string(self.type_sidecar_path(hash))
            .ok()
            .map(|s| s.trim().to_string())
    }

    fn type_sidecar_path(&self, hash: &str) -> PathBuf {
        let mut p = blob_path(&self.project_root, hash);
        let mut name = p.file_name().unwrap_or_default().to_os_string();
        name.push(".type");
        p.set_file_name(name);
        p
    }

    pub fn path(&self, hash: &str) -> PathBuf {
        blob_path(&self.project_root, hash)
    }

    /// True if `data` is small enough (text/JSON) to inline in a rumor
    /// instead of storing as a blob (ADR G4: small payloads embed inline).
    pub const fn should_inline(&self, data: &[u8]) -> bool {
        data.len() <= INLINE_CAP_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("toron-blob-{tag}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn sha256_hex_known_vector() {
        // Known sha256 of "hello".
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn put_get_roundtrip_by_hash() {
        let root = temp_root("rt");
        let store = BlobStore::new(root.clone());

        let data = b"attachment bytes";
        let hash = store.put(data).expect("put");
        assert_eq!(hash, sha256_hex(data));
        assert_eq!(store.get(&hash).expect("get"), data.to_vec());
        assert!(blob_path(&root, &hash).ends_with(format!("blobs/sha256/{hash}").as_str()));

        // Idempotent: second put does not duplicate.
        let hash2 = store.put(data).expect("put again");
        assert_eq!(hash, hash2);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn get_missing_blob_errors() {
        let root = temp_root("missing");
        let store = BlobStore::new(root.clone());
        assert!(store.get("0".repeat(64).as_str()).is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn inline_policy_small_vs_large() {
        let root = temp_root("cap");
        let store = BlobStore::new(root.clone());
        let small = vec![b'a'; INLINE_CAP_BYTES];
        let large = vec![b'a'; INLINE_CAP_BYTES + 1];
        assert!(store.should_inline(&small), "at-cap inlines");
        assert!(!store.should_inline(&large), "over-cap must not inline");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn large_payload_not_inlined_into_event_content() {
        // Large payloads go to blob storage; the event content is just the
        // toron-blob:// URL, never the payload itself.
        let root = temp_root("large");
        let store = BlobStore::new(root.clone());

        let payload = vec![b'x'; INLINE_CAP_BYTES + 10];
        let hash = store.put(&payload).expect("put large");

        let file_ref = FileRef::new(&hash, Some("application/octet-stream".into()), None);
        assert_eq!(file_ref.url, format!("{BLOB_SCHEME}{hash}"));
        assert!(file_ref.url.len() < 128, "content is just a short URL");

        // Tag set carries the hash + file type.
        let tags = file_ref.tags();
        assert!(
            tags.iter()
                .any(|t| t.as_slice().first().map(String::as_str) == Some("x")
                    && t.as_slice().get(1).map(String::as_str) == Some(&hash))
        );

        // Recipient fetches by hash and recovers the exact bytes.
        assert_eq!(store.get(&hash).expect("fetch").len(), payload.len());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn file_ref_tags_include_decryption_key_when_present() {
        let f = FileRef::new("deadbeef", Some("text/plain".into()), Some("k-42".into()));
        let tags = f.tags();
        assert!(tags.iter().any(|t| t.as_slice().first().map(String::as_str)
            == Some("decryption-key")
            && t.as_slice().get(1).map(String::as_str) == Some("k-42")));
    }

    /// ADR-0006: MIME-type hint persists as a sidecar and `exists` reflects
    /// content-addressed presence.
    #[test]
    fn put_with_type_records_sidecar_and_exists() {
        let root = temp_root("mime");
        let store = BlobStore::new(root.clone());

        let hash = store
            .put_with_type(b"<svg>hi</svg>", Some("image/svg+xml"))
            .expect("put with type");
        assert_eq!(store.get_type(&hash).as_deref(), Some("image/svg+xml"));
        assert!(store.exists(&hash));
        assert!(!store.exists("0".repeat(64).as_str()));

        // Default put keeps no sidecar.
        let plain = store.put(b"plain").expect("put");
        assert_eq!(store.get_type(&plain), None);

        // Sidecar is not overwritten once recorded (first type wins).
        store
            .put_with_type(b"<svg>hi</svg>", Some("text/plain"))
            .expect("put again");
        assert_eq!(
            store.get_type(&hash).as_deref(),
            Some("image/svg+xml"),
            "first recorded type wins"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
