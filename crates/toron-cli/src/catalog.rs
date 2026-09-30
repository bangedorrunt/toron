// governed-by: ADR-0003
// Catalog contract: toron.dev ADR-0002 D3 — `toron catalog --json` emits the
// MCP surface catalog; `cargo test catalog_freshness` fails red on drift.

//! `toron catalog` — emit the machine-readable surface catalog consumed by
//! toron.dev (ADR-0002 D3): the 38 MCP tools + 25 resources from toron-mcp,
//! plus the CLI `Command` enum introspected from clap.

use std::path::Path;

use clap::{ArgAction, CommandFactory};
use serde_json::{Value, json};

use crate::cli::Cli;

/// The catalog (toron.dev catalog contract, schema v1): 38 tools, 25
/// resources, CLI commands. `generated_at` is caller-supplied so the
/// freshness test can reproduce the committed artifact byte-for-byte.
pub fn build_catalog(generated_at: &str) -> anyhow::Result<Value> {
    Ok(json!({
        "schema_version": 1,
        "generated_at": generated_at,
        "toron_version": env!("CARGO_PKG_VERSION"),
        "tools": toron_mcp::catalog::tool_catalog()?,
        "resources": toron_mcp::catalog::resource_catalog(),
        "cli_commands": cli_commands(),
    }))
}

/// Emit the catalog for the current surface at the current UTC time.
pub fn catalog_now() -> anyhow::Result<Value> {
    build_catalog(&utc_now_rfc3339())
}

/// Write the catalog to `path` (pretty JSON + trailing newline).
pub fn write_catalog(path: &Path, catalog: &Value) -> anyhow::Result<()> {
    let mut buf = serde_json::to_string_pretty(catalog)?;
    buf.push('\n');
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, buf)?;
    Ok(())
}

/// RFC 3339 UTC now (no chrono dep): `YYYY-MM-DDTHH:MM:SSZ`.
fn utc_now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Top-level CLI commands, introspected from clap (name, args, description).
fn cli_commands() -> Value {
    Value::Array(
        Cli::command()
            .get_subcommands()
            .map(|sub| {
                let name = sub.get_name();
                json!({
                    "name": name,
                    "args": sub_args(sub),
                    "description": sub
                        .get_about()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                })
            })
            .collect(),
    )
}

/// Arg summary for one subcommand: nested subcommand names for group
/// commands, otherwise the long-flag / positional signature of its args.
fn sub_args(sub: &clap::Command) -> String {
    let nested: Vec<String> = sub
        .get_subcommands()
        .map(|s| s.get_name().to_string())
        .collect();
    if !nested.is_empty() {
        return nested.join(", ");
    }
    sub.get_arguments()
        .filter(|a| !a.is_global_set() && a.get_id() != "help" && !a.is_hide_set())
        .map(|a| {
            if let Some(long) = a.get_long() {
                if matches!(a.get_action(), ArgAction::Set | ArgAction::Append)
                    && let Some(first) = a.get_value_names().and_then(|v| v.first())
                {
                    return format!("--{long} <{first}>");
                }
                format!("--{long}")
            } else {
                format!("<{}>", a.get_id())
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The committed catalog path (ADR-0002 D3): repo `docs/catalog/toron-mcp.json`.
    fn catalog_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/catalog/toron-mcp.json")
    }

    /// AGENTS.md is the first thing every agent reads, so its tool counts are
    /// part of the surface — and prose does not regenerate when the catalog
    /// does. Three drifts shipped through green CI before this existed: a stale
    /// total (40 for 41), a stale additive count, and a count that said three
    /// while naming two tools. Ground truth is the constants, never the prose.
    ///
    /// Scoped to AGENTS.md on purpose: ADRs carry DATED historical counts
    /// ("Live 2026-09-04: 40") which were true when written, and a gate that
    /// reddened on recorded history would be switched off within a week.
    #[test]
    fn agents_doc_counts_match_the_catalog() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let agents = std::fs::read_to_string(repo.join("AGENTS.md")).expect("AGENTS.md");
        let parity = toron_mcp::catalog::PARITY_TOOL_COUNT;
        let additive = toron_mcp::catalog::ADDITIVE_TOOLS;
        let total = parity + additive.len();

        // Every "38 AM-Rust parity + N toron additive" claim in the file.
        let mut claims = 0usize;
        for (idx, _) in agents.match_indices("AM-Rust parity + ") {
            let rest = &agents[idx + "AM-Rust parity + ".len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            let n: usize = digits
                .parse()
                .unwrap_or_else(|_| panic!("unparsable additive count after `AM-Rust parity + `"));
            assert_eq!(
                n,
                additive.len(),
                "AGENTS.md claims {n} additive tools; catalog::ADDITIVE_TOOLS has {}",
                additive.len()
            );
            claims += 1;
        }
        assert!(
            claims >= 2,
            "expected the header and CONVENTIONS to state the count (found {claims})"
        );

        // Every total: "N MCP tools" / "N tool names". A phrase whose run of
        // digits is empty ("no new MCP tools") is not a claim.
        for marker in [" MCP tools", " tool names"] {
            for (idx, _) in agents.match_indices(marker) {
                let digits: String = agents[..idx]
                    .chars()
                    .rev()
                    .take_while(char::is_ascii_digit)
                    .collect();
                if digits.is_empty() {
                    continue;
                }
                let n: usize = digits
                    .chars()
                    .rev()
                    .collect::<String>()
                    .parse()
                    .expect("digits");
                assert_eq!(
                    n,
                    total,
                    "AGENTS.md says {n}{marker}; the catalog serves {total} ({parity} parity + {})",
                    additive.len()
                );
            }
        }

        // The header must NAME every additive tool: a count that says three
        // while listing two is the drift that started this.
        let after = agents
            .split("toron additive:")
            .nth(1)
            .expect("the header names the additive tools");
        let listed: std::collections::BTreeSet<&str> = after
            .split(')')
            .next()
            .unwrap_or("")
            .split('`')
            .skip(1)
            .step_by(2)
            .collect();
        let expected: std::collections::BTreeSet<&str> = additive.iter().copied().collect();
        assert_eq!(
            listed, expected,
            "AGENTS.md's additive list and catalog::ADDITIVE_TOOLS disagree"
        );
    }

    /// README is the stranger-facing doc, and it advertised a dead env var
    /// (`TORON_OPERATOR_NSEC`) as the operator-key override for months while the
    /// only variable anything reads is `TORON_OPERATOR_KEY`. ADRs may quote the
    /// rejected name as history; the README may not offer it as an option.
    #[test]
    fn readme_offers_only_the_live_operator_key_env() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let readme = std::fs::read_to_string(repo.join("README.md")).expect("README.md");
        assert!(
            readme.contains("TORON_OPERATOR_KEY"),
            "README must name the variable the code actually reads"
        );
        assert!(
            !readme.contains("TORON_OPERATOR_NSEC"),
            "README advertises a dead env var — the only override is TORON_OPERATOR_KEY"
        );
    }

    /// ADR-0002 D3 freshness gate: regenerating the committed catalog with
    /// the committed timestamp must reproduce it byte-for-byte. RED when a
    /// surface change (tool/arg/schema/resource/CLI command) lands without a
    /// catalog regen — the change is unmergeable until regenerated.
    #[test]
    fn catalog_freshness() {
        let path = catalog_path();
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "committed catalog missing at {} ({e}); regenerate and commit it:\n  \
                 cargo run -p toron-cli --bin toron -- catalog --json \
                 --output docs/catalog/toron-mcp.json",
                path.display()
            )
        });
        let committed_json: Value =
            serde_json::from_str(&committed).expect("committed catalog parses");
        let generated_at = committed_json["generated_at"]
            .as_str()
            .expect("committed catalog carries generated_at");
        let fresh = build_catalog(generated_at).expect("catalog builds from the live surface");
        let mut fresh_str =
            serde_json::to_string_pretty(&fresh).expect("catalog serializes deterministically");
        fresh_str.push('\n');
        assert_eq!(
            fresh_str, committed,
            "catalog drift: regenerate via `toron catalog --json \
             --output docs/catalog/toron-mcp.json` and commit the result"
        );

        // Structural floor (toron ADR-0007 C3 / ADR-0002 D3): exactly 38
        // parity tools, the additive excluded, 25 resources, group order
        // matches TOOL_GROUPS.
        let tools = fresh["tools"].as_array().expect("tools array");
        assert_eq!(
            tools.len(),
            toron_mcp::catalog::PARITY_TOOL_COUNT,
            "toron ADR-0007 C3: exactly 38 parity tools in the catalog"
        );
        assert!(
            !tools
                .iter()
                .filter_map(|t| t["name"].as_str())
                .any(|n| n == toron_mcp::catalog::ADDITIVE_TOOL),
            "additive {} excluded from the 38-tool catalog",
            toron_mcp::catalog::ADDITIVE_TOOL
        );
        let groups: Vec<&str> = tools.iter().filter_map(|t| t["group"].as_str()).collect();
        let expected: Vec<&str> = toron_mcp::catalog::TOOL_GROUPS
            .iter()
            .flat_map(|(g, names)| names.iter().map(move |_| *g))
            .collect();
        assert_eq!(groups, expected, "tool order follows TOOL_GROUPS");
        let resources = fresh["resources"].as_array().expect("resources array");
        assert_eq!(resources.len(), 25, "AM-Rust parity requires 25 resources");
    }

    #[test]
    fn utc_now_is_rfc3339() {
        let now = utc_now_rfc3339();
        assert_eq!(now.len(), 20, "{now}");
        assert!(now.ends_with('Z'), "{now}");
        assert!(now.chars().nth(10) == Some('T'), "{now}");
        assert!(now[..4].parse::<u32>().unwrap() >= 2026, "{now}");
    }
}
