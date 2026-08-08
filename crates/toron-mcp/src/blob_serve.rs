// governed-by: ADR-0006 (bd-esh.5)
#![forbid(unsafe_code)]

//! Blossom / NIP-B7 federated blob serving (ADR-0006).
//!
//! Serves the local G4 content-addressed [`BlobStore`] over the Blossom HTTP
//! contract (BUD-01 retrieval + BUD-02 upload), so an external Nostr client or
//! peer instance can fetch a blob by its sha256 and toron users can upload
//! reachable attachments.
//!
//! The router mounts at `/.blossom` on the daemon's axum tree (never collides
//! with the `/mcp` MCP transport):
//! - `GET  /.blossom/<sha256>(.ext)` → blob bytes (BUD-01).
//! - `HEAD /.blossom/<sha256>(.ext)` → same headers, no body (BUD-01).
//! - `PUT  /.blossom/upload`        → store bytes, return a Blob Descriptor
//!   (BUD-02). Idempotent by content addressing.
//! - `OPTIONS` preflight → CORS per BUD-01.
//!
//! Every response carries `Access-Control-Allow-Origin: *`; errors carry a
//! human-readable `X-Reason` header (BUD-01).
//!
//! governed-by: ADR-0006, BUD-01, BUD-02

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use serde::Serialize;
use serde_json::json;
use toron_core::archive::blob::{BLOB_SCHEME, BlobStore, sha256_hex};

/// The HTTP mount path for the Blossom surface (distinct from `/mcp`).
pub const MOUNT_PATH: &str = "/.blossom";

/// Default MIME type when no hint is recorded (BUD-01/BUD-02 default).
pub const DEFAULT_MIME: &str = "application/octet-stream";

/// A Blossom `Blob Descriptor` (BUD-02 response body).
#[derive(Debug, Clone, Serialize)]
pub struct BlobDescriptor {
    pub url: String,
    pub sha256: String,
    pub size: usize,
    #[serde(rename = "type")]
    pub mime: String,
    pub uploaded: u64,
}

/// Extract the sha256 from a Blossom (or any) URL: the **last** 64-char hex
/// string (BUD-03 client rule). Returns `None` when no 64-hex segment exists.
pub fn extract_sha256(url: &str) -> Option<String> {
    // Scan the URL for 64-hex substrings; keep the last occurrence.
    let hex = "0123456789abcdef";
    let bytes: Vec<char> = url.to_lowercase().chars().collect();
    let mut last: Option<String> = None;
    let mut i = 0;
    while i < bytes.len() {
        if hex.contains(bytes[i]) {
            let start = i;
            while i < bytes.len() && hex.contains(bytes[i]) {
                i += 1;
            }
            let seg: String = bytes[start..i].iter().collect();
            if seg.len() == 64 {
                last = Some(seg);
            }
        } else {
            i += 1;
        }
    }
    last
}

/// Resolve a `toron-blob://<hash>` reference (or any Blossom URL) to an HTTP
/// URL under a public base + mount (ADR-0006 integration point).
///
/// ```
/// let url = toron_mcp::blob_serve::http_url(
///     "https://cdn.example.com",
///     "toron-blob://b1674191a88ec5cdd733e4240a81803105dc412d6c6708d53ab94fc248f4f553",
///     Some("application/pdf"),
/// );
/// assert!(url.unwrap().starts_with("https://cdn.example.com/.blossom/"));
/// ```
pub fn http_url(base: &str, reference: &str, mime: Option<&str>) -> Option<String> {
    let hash = reference
        .strip_prefix(BLOB_SCHEME)
        .map(str::to_string)
        .or_else(|| extract_sha256(reference))?;
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let ext = mime_to_ext(mime);
    let base = base.trim_end_matches('/');
    Some(format!("{base}{MOUNT_PATH}/{hash}{ext}"))
}

/// Map a MIME type to a URL file extension (BUD-02: descriptor `url` MUST
/// include an extension for embeddability). Unknown → empty (no extension).
pub fn mime_to_ext(mime: Option<&str>) -> String {
    match mime {
        Some("application/pdf") => ".pdf".into(),
        Some("image/png") => ".png".into(),
        Some("image/jpeg") | Some("image/jpg") => ".jpg".into(),
        Some("image/gif") => ".gif".into(),
        Some("image/webp") => ".webp".into(),
        Some("image/svg+xml") => ".svg".into(),
        Some("video/mp4") => ".mp4".into(),
        Some("audio/mpeg") => ".mp3".into(),
        Some("text/plain") => ".txt".into(),
        Some("text/markdown") => ".md".into(),
        _ => String::new(),
    }
}

/// The Blossom serving state: the shared blob store.
#[derive(Clone)]
struct BlossomState {
    store: Arc<BlobStore>,
}

/// Build the Blossom router mounted at [`MOUNT_PATH`].
pub fn blossom_router(store: BlobStore) -> Router {
    let state = BlossomState {
        store: Arc::new(store),
    };
    let inner = Router::new()
        .route("/upload", put(upload))
        .route("/:sha256", get(get_blob).head(head_blob))
        .with_state(state);
    Router::new().nest(MOUNT_PATH, inner)
}

/// GET `/.blossom/<sha256>(.ext)` — return the blob bytes (BUD-01).
async fn get_blob(
    State(state): State<BlossomState>,
    AxumPath(sha256): AxumPath<String>,
) -> Response {
    serve_blob(&state, &sha256, false).await
}

/// HEAD `/.blossom/<sha256>(.ext)` — same headers, no body (BUD-01).
async fn head_blob(
    State(state): State<BlossomState>,
    AxumPath(sha256): AxumPath<String>,
) -> Response {
    serve_blob(&state, &sha256, true).await
}

/// Shared BUD-01 GET/HEAD logic.
async fn serve_blob(state: &BlossomState, sha256: &str, head_only: bool) -> Response {
    // The path may carry an optional file extension (`.pdf`, `.png`); the hash
    // is the first 64-hex prefix. Reject malformed paths with 400.
    let hash: String = sha256
        .split('.')
        .next()
        .map(str::to_string)
        .unwrap_or_default();
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return (StatusCode::BAD_REQUEST, cors_headers(), "malformed sha256").into_response();
    }
    let mime = state
        .store
        .get_type(&hash)
        .unwrap_or_else(|| DEFAULT_MIME.to_string());
    match state.store.get(&hash) {
        Ok(bytes) => {
            let mut resp = if head_only {
                StatusCode::OK.into_response()
            } else {
                (StatusCode::OK, bytes).into_response()
            };
            let headers = resp.headers_mut();
            apply_cors(headers);
            headers.insert(
                header::CONTENT_TYPE,
                mime.parse().unwrap_or_else(|_| DEFAULT_MIME.parse().unwrap()),
            );
            headers.insert(header::CONTENT_LENGTH, state.store.get(&hash).unwrap_or_default().len().to_string().parse().unwrap());
            resp
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            cors_headers(),
            json!({ "error": "blob not found" }).to_string(),
        )
            .into_response(),
    }
}

/// PUT `/.blossom/upload` — store the raw body, return a Blob Descriptor
/// (BUD-02). Idempotent: `201` newly stored, `200` already present.
async fn upload(
    State(state): State<BlossomState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string());
    // Optional X-SHA-256: reject on mismatch (BUD-02 409).
    if let Some(x_sha) = headers.get("x-sha-256").and_then(|v| v.to_str().ok()) {
        let actual = sha256_hex(&body);
        if !x_sha.eq_ignore_ascii_case(&actual) {
            return (
                StatusCode::CONFLICT,
                cors_headers(),
                json!({ "error": "X-SHA-256 does not match request body" }).to_string(),
            )
                .into_response();
        }
    }
    let sha256 = sha256_hex(&body);
    let existed = state.store.exists(&sha256);
    let put_res = state.store.put_with_type(&body, mime.as_deref());
    let uploaded = state
        .store
        .get_type(&sha256)
        .unwrap_or_else(|| DEFAULT_MIME.to_string());
    match put_res {
        Ok(_) => {
            let status = if existed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            let descriptor = BlobDescriptor {
                url: format!(
                    "/{MOUNT_PATH}/{sha256}{}",
                    mime_to_ext(Some(&uploaded))
                ),
                sha256: sha256.clone(),
                size: body.len(),
                mime: uploaded,
                uploaded: now_unix(),
            };
            (
                status,
                cors_headers(),
                serde_json::to_string(&descriptor).unwrap_or_else(|_| "{}".into()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            cors_headers(),
            json!({ "error": format!("store failed: {e}") }).to_string(),
        )
            .into_response(),
    }
}

/// Unix-seconds timestamp (descriptor `uploaded` field).
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// CORS headers per BUD-01 (all responses).
fn cors_headers() -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    h
}

fn apply_cors(headers: &mut HeaderMap) {
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn temp_root(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("toron-blossom-{tag}-{}", uuid::Uuid::new_v4()))
    }

    fn test_router(tag: &str) -> (Router, std::path::PathBuf) {
        let root = temp_root(tag);
        let store = BlobStore::new(root.clone());
        (blossom_router(store), root)
    }

    /// BUD-01: GET returns the exact bytes with Content-Type + CORS.
    #[tokio::test]
    async fn get_returns_blob_bytes() {
        let (router, root) = test_router("get");
        let store = BlobStore::new(root.clone());
        let hash = store
            .put_with_type(b"hello blossom", Some("text/plain"))
            .expect("put");

        let resp = router
            .oneshot(
                Request::builder()
                    .uri(format!("/.blossom/{hash}.txt"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "*"
        );
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "text/plain");
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), b"hello blossom");
        let _ = std::fs::remove_dir_all(root);
    }

    /// BUD-01: missing blob → 404.
    #[tokio::test]
    async fn get_missing_returns_404() {
        let (router, root) = test_router("missing");
        let resp = router
            .oneshot(
                Request::builder()
                    .uri(format!("/.blossom/{}", "0".repeat(64)))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let _ = std::fs::remove_dir_all(root);
    }

    /// BUD-01: malformed hash → 400.
    #[tokio::test]
    async fn get_malformed_returns_400() {
        let (router, root) = test_router("malformed");
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/.blossom/not-a-hash")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let _ = std::fs::remove_dir_all(root);
    }

    /// BUD-01: HEAD returns headers, no body.
    #[tokio::test]
    async fn head_returns_headers_no_body() {
        let (router, root) = test_router("head");
        let store = BlobStore::new(root.clone());
        let hash = store.put_with_type(b"xx", Some("application/pdf")).expect("put");

        let resp = router
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/.blossom/{hash}.pdf"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "application/pdf");
        let _ = std::fs::remove_dir_all(root);
    }

    /// BUD-02: PUT stores and returns a Blob Descriptor (201), idempotent
    /// second PUT → 200.
    #[tokio::test]
    async fn put_stores_and_returns_descriptor() {
        let (router, root) = test_router("put");
        let body = Body::from("some payload");
        let req = Request::builder()
            .method("PUT")
            .uri("/.blossom/upload")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body)
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let descriptor: serde_json::Value =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        let hash = descriptor["sha256"].as_str().unwrap().to_string();
        assert_eq!(descriptor["type"], "text/plain");
        assert!(descriptor["url"].as_str().unwrap().contains(&hash));

        // Second PUT (same bytes) → 200 OK, same hash.
        let req2 = Request::builder()
            .method("PUT")
            .uri("/.blossom/upload")
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("some payload"))
            .unwrap();
        let resp2 = router.oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let d2: serde_json::Value =
            serde_json::from_slice(&resp2.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(d2["sha256"], hash);
        let _ = std::fs::remove_dir_all(root);
    }

    /// BUD-02: mismatched X-SHA-256 → 409.
    #[tokio::test]
    async fn put_x_sha256_mismatch_returns_409() {
        let (router, root) = test_router("409");
        let req = Request::builder()
            .method("PUT")
            .uri("/.blossom/upload")
            .header("x-sha-256", "f".repeat(64))
            .body(Body::from("some payload"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let _ = std::fs::remove_dir_all(root);
    }

    /// BUD-03: hash extraction takes the LAST 64-hex string in a URL.
    #[test]
    fn extract_sha256_takes_last_hex() {
        let hash = "b1674191a88ec5cdd733e4240a81803105dc412d6c6708d53ab94fc248f4f553";
        assert_eq!(
            extract_sha256(&format!("https://cdn.example.com/{hash}.pdf")),
            Some(hash.to_string())
        );
        assert_eq!(
            extract_sha256(&format!(
                "https://cdn.example.com/user/ec4425ff5e9446080d2f70440188e3ca5d6da8713db7bdeef73d0ed54d9093f0/media/{hash}.pdf"
            )),
            Some(hash.to_string())
        );
        assert_eq!(extract_sha256("https://example.com/no-hash-here"), None);
    }

    /// ADR-0006: `toron-blob://` resolves to an HTTP URL under the mount.
    #[test]
    fn toron_blob_url_resolves_to_http() {
        let hash = "b1674191a88ec5cdd733e4240a81803105dc412d6c6708d53ab94fc248f4f553";
        let url = http_url(
            "https://cdn.example.com/",
            &format!("{BLOB_SCHEME}{hash}"),
            Some("application/pdf"),
        )
        .expect("resolves");
        assert_eq!(
            url,
            format!("https://cdn.example.com/.blossom/{hash}.pdf")
        );
        // Extension mapping.
        assert_eq!(mime_to_ext(Some("image/png")), ".png");
        assert_eq!(mime_to_ext(Some("text/plain")), ".txt");
        assert_eq!(mime_to_ext(None), "");
    }
}
