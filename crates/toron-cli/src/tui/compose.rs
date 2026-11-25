// governed-by: ADR-0031
//! Pure `#seq` board-post compose helpers (refuses secret-class bodies).

use toron_core::mailbox::board::classify_board_body;

/// Extract `@PinName` mentions from a board post body.
pub fn extract_mentions(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in body.split_whitespace() {
        if let Some(rest) = token.strip_prefix('@') {
            let clean =
                rest.trim_end_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_');
            if !clean.is_empty() && !out.iter().any(|m| m == clean) {
                out.push(clean.to_string());
            }
        }
    }
    out
}

/// Validate a board-post body before publishing.
pub fn validate_board_draft(body: &str) -> anyhow::Result<()> {
    if body.trim().is_empty() {
        anyhow::bail!("post body cannot be empty");
    }
    classify_board_body(body, None).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
