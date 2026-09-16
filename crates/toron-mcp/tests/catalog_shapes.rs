// governed-by: ADR-0031
//! Frozen method-shapes fixture (ADR-0031 S5, bead bd-ucjx).
//!
//! `catalog_freshness` guards the 38-parity CLI catalog, but nothing froze
//! the MCP wire shapes: an additive tool could silently reshape a parity
//! tool and stay green. This test freezes all 40 tool shapes
//! (name/description/input/output schema) + 25 resource shapes in
//! `fixtures/tool-method-shapes-v1.json`. Any drift without regen fails red.
//!
//! Regen (explicit, reviewed): `TORON_REGEN_SHAPES=1 mbx test -p toron-mcp catalog_shapes`

use serde_json::{Value, json};
use std::collections::BTreeMap;

use toron_mcp::catalog::{ADDITIVE_TOOLS, resource_catalog, tool_catalog};
use toron_mcp::tools::ToolServer;

const FIXTURE: &str = include_str!("fixtures/tool-method-shapes-v1.json");
const TOOL_COUNT: usize = 40;
const RESOURCE_COUNT: usize = 25;

/// Live shapes: all router tools (38 parity + additives) + resources.
fn live_shapes() -> Value {
    let tools = ToolServer::tool_router().list_all();
    let mut map: BTreeMap<String, Value> = BTreeMap::new();
    for t in &tools {
        let input = Value::Object((*t.input_schema).clone());
        let output = t
            .output_schema
            .as_ref()
            .map(|s| Value::Object((**s).clone()));
        map.insert(
            t.name.to_string(),
            json!({
                "description": t.description.clone().unwrap_or_default().to_string(),
                "input_schema": input,
                "output_schema": output,
            }),
        );
    }
    // Parity catalog must stay exactly the frozen surface (cross-checks the
    // catalog_freshness gate from the shapes side).
    let parity = tool_catalog().expect("parity catalog builds");
    assert_eq!(parity.len(), 38, "parity catalog is 38 tools");
    for entry in &parity {
        let name = entry["name"].as_str().expect("entry name");
        assert!(
            map.contains_key(name),
            "parity tool `{name}` missing from router surface"
        );
    }
    for additive in ADDITIVE_TOOLS {
        assert!(
            map.contains_key(*additive),
            "additive tool `{additive}` missing from router surface"
        );
    }
    json!({ "tools": map, "resources": resource_catalog() })
}

/// Human-readable drift lines between live and frozen shapes.
fn find_drift(live: &Value, frozen: &Value) -> Vec<String> {
    let mut drift = Vec::new();
    let live_tools = live["tools"].as_object().cloned().unwrap_or_default();
    let frozen_tools = frozen["tools"].as_object().cloned().unwrap_or_default();
    for (name, shape) in &live_tools {
        match frozen_tools.get(name) {
            None => drift.push(format!("new tool `{name}` (needs ADR + regen)")),
            Some(old) if old != shape => drift.push(format!("shape drift on `{name}`")),
            _ => {}
        }
    }
    for name in frozen_tools.keys() {
        if !live_tools.contains_key(name) {
            drift.push(format!("tool `{name}` removed"));
        }
    }
    if live["resources"] != frozen["resources"] {
        drift.push("resource shapes drifted".to_string());
    }
    drift
}

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tool-method-shapes-v1.json")
}

#[test]
fn catalog_shapes_frozen() {
    let live = live_shapes();
    assert_eq!(
        live["tools"].as_object().map(serde_json::Map::len),
        Some(TOOL_COUNT),
        "frozen surface is 40 tools (38 parity + 2 additive)"
    );
    assert_eq!(
        live["resources"].as_array().map(Vec::len),
        Some(RESOURCE_COUNT),
        "frozen surface is 25 resources"
    );
    let frozen: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
    if std::env::var("TORON_REGEN_SHAPES").as_deref() == Ok("1") {
        std::fs::write(
            fixture_path(),
            serde_json::to_string_pretty(&live).expect("serializes") + "\n",
        )
        .expect("regen writes fixture");
        return;
    }
    let drift = find_drift(&live, &frozen);
    assert!(
        drift.is_empty(),
        "method-shape drift (regen ONLY with an ADR + review: TORON_REGEN_SHAPES=1):\n{}",
        drift.join("\n")
    );
}

#[test]
fn catalog_shapes_tampered_shape_fails() {
    // Bite-proof: a one-word description change must be reported as drift.
    let live = live_shapes();
    let frozen: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
    assert!(
        find_drift(&live, &frozen).is_empty(),
        "pristine live matches"
    );
    let mut tampered = live;
    let first = tampered["tools"]
        .as_object()
        .and_then(|o| o.keys().next().cloned())
        .expect("tools nonempty");
    tampered["tools"][&first]["description"] = json!("tampered description");
    let drift = find_drift(&tampered, &frozen);
    assert_eq!(drift.len(), 1, "exactly one drift line, got: {drift:?}");
    assert!(drift[0].contains(&first), "drift names the tool");
}
