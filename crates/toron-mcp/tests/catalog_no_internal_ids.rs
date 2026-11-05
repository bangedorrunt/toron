// governed-by: ADR-0003
//! No internal ledger identifiers in the published surface (bd-catalog-extractor-public-bead-ids-x35i).
//!
//! toron.dev projects `docs/catalog/toron-mcp.json` byte-for-byte into its
//! public tool pages (toron.dev ADR-0002 D4). So any string in a tool description, a
//! field description, or a resource description is not an internal note — it
//! is something a stranger reads, and it is something that ages: a bead id in
//! a published string is a live identifier pointing at one repository's
//! private work ledger, in another repository's public documentation.
//!
//! Ten of them shipped that way before this check existed. Three were public
//! from the first catalog commit. A removal alone is not a fix, because the
//! next citation goes straight back in; this test is the fix.
//!
//! ## What it does NOT ban
//!
//! Ordinary `//` comments and `///` doc comments on code that is never
//! serialised are the RIGHT place to cite a bead, and this repository is full
//! of them. Only the emitted surface is checked. The citation does not go
//! away when it leaves the schema — it moves to a `// governed-by: ADR-XXXX`
//! line or to the commit, which is where a durable pointer belongs anyway.

use serde_json::Value;
use std::collections::BTreeSet;

use toron_mcp::catalog::resource_catalog;
use toron_mcp::tools::ToolServer;

/// A bead id as the ledgers mint them: `bd-` plus a lowercase, dash-separated
/// slug. Matches the short form (`bd-z9q2`) and the descriptive form
/// (`bd-some-descriptive-slug-w3k7`) alike.
fn bead_ids(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"bd-") && (i == 0 || !is_id_byte(bytes[i - 1])) {
            let start = i;
            i += 3;
            while i < bytes.len() && is_id_byte(bytes[i]) {
                i += 1;
            }
            let slug = &text[start..i];
            if slug
                .chars()
                .nth(3)
                .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            {
                out.push(slug.to_string());
            }
        } else {
            i += 1;
        }
    }
    out
}

const fn is_id_byte(b: u8) -> bool {
    b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'
}

/// Every string in a catalog tree, paired with the path that reached it.
///
/// Walking the whole tree rather than a hand-picked list of fields is the
/// point: a `$defs` entry three levels down is exactly where one of the ten
/// hid, and a list of known locations is a list that goes stale.
fn strings(node: &Value, path: &str, out: &mut Vec<(String, String)>) {
    match node {
        Value::String(s) => out.push((path.to_string(), s.clone())),
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                strings(v, &format!("{path}[{i}]"), out);
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                strings(v, &format!("{path}.{k}"), out);
            }
        }
        _ => {}
    }
}

#[test]
fn no_tool_or_resource_description_carries_a_bead_id() {
    let mut found: BTreeSet<String> = BTreeSet::new();

    for t in ToolServer::tool_router().list_all() {
        let name = t.name.to_string();
        let mut parts: Vec<(String, String)> = vec![(
            format!("tools.{name}.description"),
            t.description.clone().unwrap_or_default().to_string(),
        )];
        strings(
            &Value::Object((*t.input_schema).clone()),
            &format!("tools.{name}.input_schema"),
            &mut parts,
        );
        if let Some(out) = t.output_schema.as_ref() {
            strings(
                &Value::Object((**out).clone()),
                &format!("tools.{name}.output_schema"),
                &mut parts,
            );
        }
        for (path, text) in parts {
            for id in bead_ids(&text) {
                found.insert(format!("{path}: {id}"));
            }
        }
    }

    let mut parts: Vec<(String, String)> = Vec::new();
    strings(&Value::Array(resource_catalog()), "resources", &mut parts);
    for (path, text) in parts {
        for id in bead_ids(&text) {
            found.insert(format!("{path}: {id}"));
        }
    }

    assert!(
        found.is_empty(),
        "the published surface carries {} live bead id(s):\n  {}\n\
         toron.dev projects these strings verbatim, so an id from this \
         repository's private ledger is public documentation for another \
         project. Cite an ADR, or move the citation to the commit.",
        found.len(),
        found.into_iter().collect::<Vec<_>>().join("\n  ")
    );
}
#[test]
fn the_bead_id_matcher_actually_bites() {
    assert_eq!(bead_ids("(bd-z9q2)"), vec!["bd-z9q2"]);
    assert_eq!(bead_ids("bd-4t7w.2 G1"), vec!["bd-4t7w"]);
    assert_eq!(
        bead_ids("see bd-some-descriptive-slug-w3k7 for detail"),
        vec!["bd-some-descriptive-slug-w3k7"]
    );
    assert_eq!(
        bead_ids("the bd- prefix marks a bead"),
        Vec::<String>::new(),
        "a bare prefix is prose, not an identifier"
    );
    assert_eq!(
        bead_ids("abbd-z9q2"),
        Vec::<String>::new(),
        "a token that merely ends in bd- is not a bead id"
    );
    assert_eq!(
        bead_ids("ADR-0027 E2 assignment tags"),
        Vec::<String>::new()
    );
}
