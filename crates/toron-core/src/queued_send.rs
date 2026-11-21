// governed-by: ADR-0040
//! Durable local queue for sends attempted while the daemon is down (bd-ys3jp).
//!
//! A send is journaled under its corr BEFORE the transport runs. A failed
//! transport leaves the row in place; the next toron command re-sends it with
//! the SAME corr, and the idempotent-send claim makes that replay a duplicate
//! rather than a second delivery. Removing a row is a rewrite through a temp
//! file, so a crash never leaves a half-deleted queue.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const QUEUE_FILE: &str = "queued-sends.jsonl";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedSend {
    /// Stable correlation id; never re-minted on a flush.
    pub corr: String,
    pub project: String,
    pub sender: String,
    pub to: Vec<String>,
    #[serde(default)]
    pub cc: Vec<String>,
    #[serde(default)]
    pub bcc: Vec<String>,
    pub subject: String,
    pub body: String,
    #[serde(default)]
    pub importance: Option<String>,
    #[serde(default)]
    pub thread: Option<String>,
    #[serde(default)]
    pub wake: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default)]
    pub result_path: Option<String>,
    #[serde(default)]
    pub verdict: Option<String>,
    pub created_at_ms: u64,
}

pub fn mint_corr() -> String {
    format!("q-{}", uuid::Uuid::new_v4())
}

pub fn queue_path() -> PathBuf {
    crate::config::state_dir().join(QUEUE_FILE)
}

fn from_line(line: &str) -> io::Result<QueuedSend> {
    serde_json::from_str(line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_all(path: &Path) -> io::Result<Vec<QueuedSend>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    raw.lines()
        .filter(|line| !line.trim().is_empty())
        .map(from_line)
        .collect()
}

pub fn pending_at(path: &Path) -> io::Result<Vec<QueuedSend>> {
    read_all(path)
}

pub fn pending() -> io::Result<Vec<QueuedSend>> {
    pending_at(&queue_path())
}

/// Append one send under `corr`. A second enqueue with the same corr is a
/// no-op, so a retried CLI invocation cannot double the queue.
pub fn enqueue_at(path: &Path, send: &QueuedSend) -> io::Result<bool> {
    let rows = read_all(path)?;
    if rows.iter().any(|row| row.corr == send.corr) {
        return Ok(false);
    }
    let line =
        serde_json::to_string(send).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{line}")?;
    file.sync_all()?;
    Ok(true)
}

pub fn enqueue(send: &QueuedSend) -> io::Result<bool> {
    enqueue_at(&queue_path(), send)
}

/// Drop the row with this corr. False means it was already gone.
pub fn remove_at(path: &Path, corr: &str) -> io::Result<bool> {
    let rows = read_all(path)?;
    let kept: Vec<&QueuedSend> = rows.iter().filter(|row| row.corr != corr).collect();
    if kept.len() == rows.len() {
        return Ok(false);
    }
    let mut body = String::new();
    for row in kept {
        body.push_str(
            &serde_json::to_string(row)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        );
        body.push('\n');
    }
    let tmp = path.with_extension("tmp");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&tmp, body)?;
    std::fs::rename(tmp, path)?;
    Ok(true)
}

pub fn remove(corr: &str) -> io::Result<bool> {
    remove_at(&queue_path(), corr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(corr: &str) -> QueuedSend {
        QueuedSend {
            corr: corr.to_string(),
            project: "p".into(),
            sender: "Alpha".into(),
            to: vec!["Beta".into()],
            cc: vec![],
            bcc: vec![],
            subject: "s".into(),
            body: "b".into(),
            importance: None,
            thread: None,
            wake: Some("tasked".into()),
            priority: None,
            audience: None,
            result_path: None,
            verdict: None,
            created_at_ms: 1,
        }
    }

    #[test]
    fn a_queued_send_survives_reload_and_keeps_its_corr() {
        let path = std::env::temp_dir().join(format!(
            "toron-queued-send-{}-{}",
            std::process::id(),
            line!()
        ));
        let _ = std::fs::remove_file(&path);
        assert!(enqueue_at(&path, &entry("c-1")).unwrap());
        assert!(
            !enqueue_at(&path, &entry("c-1")).unwrap(),
            "same corr dedupes"
        );
        assert!(enqueue_at(&path, &entry("c-2")).unwrap());
        assert_eq!(pending_at(&path).unwrap().len(), 2);
        assert!(remove_at(&path, "c-1").unwrap());
        let left = pending_at(&path).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].corr, "c-2");
        assert_eq!(left[0].wake.as_deref(), Some("tasked"));
        std::fs::remove_file(&path).ok();
    }
}
