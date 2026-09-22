// governed-by: ADR-0034 (G7)
//! JSONL generation guard: every appended row is exactly one JSON value
//! plus a trailing newline — no embedded `\n`, no partial writes
//! surfacing as silent corruption. Writers go through `append_row`; existing
//! files are audited with `lint_file` (first bad 1-based row index).

use std::io::Write;
use std::path::Path;

/// Encode one row: exactly one line of JSON (no trailing newline yet).
fn encode_row(value: &serde_json::Value) -> anyhow::Result<String> {
    let line = serde_json::to_string(value)?;
    if line.contains('\n') {
        anyhow::bail!("JSONL row contains an embedded newline");
    }
    Ok(line)
}

/// Append one JSON value as a single `\n`-terminated row.
pub fn append_row(path: &Path, value: &serde_json::Value) -> anyhow::Result<()> {
    append_raw_row(path, &encode_row(value)?)
}

/// Append a pre-serialized row after validating it: must parse as exactly
/// one JSON value with no embedded newline. A violation fails with the
/// 1-based row index the append WOULD have written — and writes nothing.
pub fn append_raw_row(path: &Path, line: &str) -> anyhow::Result<()> {
    if line.contains('\n') {
        let existing = count_rows(path);
        anyhow::bail!("JSONL row {} has an embedded newline", existing + 1);
    }
    let parsed: serde_json::Value = serde_json::from_str(line)
        .map_err(|e| anyhow::anyhow!("JSONL row is not valid JSON: {e}"))?;
    let _ = parsed;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{line}")?;
    file.flush()?;
    Ok(())
}

/// Read every row as JSON (strict: any bad row errors).
pub fn read_rows(path: &Path) -> anyhow::Result<Vec<serde_json::Value>> {
    let raw = std::fs::read_to_string(path)?;
    raw.lines()
        .enumerate()
        .map(|(i, line)| {
            serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("JSONL row {} invalid: {e}", i + 1))
        })
        .collect()
}

/// First bad 1-based row index, or `None` when clean (absent file = clean).
/// Readers keep accepting today's files; this audits them.
pub fn lint_file(path: &Path) -> anyhow::Result<Option<usize>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    for (i, line) in raw.lines().enumerate() {
        if serde_json::from_str::<serde_json::Value>(line).is_err() {
            return Ok(Some(i + 1));
        }
    }
    Ok(None)
}

fn count_rows(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|raw| raw.lines().count())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("toron-jsonl-{tag}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn append_then_read_round_trips_byte_stable() {
        let p = tmp("roundtrip");
        let rows = vec![json!({"a": 1}), json!({"b": "x\ny"}), json!([1, 2])];
        for r in &rows {
            append_row(&p, r).unwrap();
        }
        let back = read_rows(&p).unwrap();
        assert_eq!(back, rows);
        // Byte-stable: one JSON value per line, trailing newline each.
        let raw = std::fs::read_to_string(&p).unwrap();
        assert_eq!(raw.lines().count(), 3);
        assert!(raw.ends_with('\n'));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn raw_row_with_embedded_newline_refused_with_index() {
        let p = tmp("badrow");
        append_row(&p, &json!({"ok": true})).unwrap();
        let err = append_raw_row(&p, "{\"broken\": true}\n{\"split\": 1}")
            .expect_err("embedded newline must refuse");
        assert!(
            err.to_string().contains("row 2"),
            "names the row index: {err:#}"
        );
        // The refused row was NOT appended.
        assert_eq!(read_rows(&p).unwrap().len(), 1);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn lint_reports_first_bad_row() {
        let p = tmp("lint");
        std::fs::write(&p, "{\"a\":1}\nnot json\n{\"b\":2}\n").unwrap();
        assert_eq!(lint_file(&p).unwrap(), Some(2));
        assert_eq!(
            lint_file(&tmp("missing")).unwrap(),
            None,
            "absent file is clean"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn non_object_values_are_rows_too() {
        // JSONL rows are JSON values, not necessarily objects.
        let p = tmp("scalar");
        append_row(&p, &json!("just a string")).unwrap();
        append_row(&p, &json!(42)).unwrap();
        assert_eq!(read_rows(&p).unwrap().len(), 2);
        let _ = std::fs::remove_file(&p);
    }
}
