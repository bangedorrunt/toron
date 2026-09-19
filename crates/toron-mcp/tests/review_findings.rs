// governed-by: ADR-0033
//! Review-finding pins (bd-7qcw): annotations + no-op disclosures live on the
//! wire surface, not just in prose.

use toron_mcp::tools::ToolServer;

#[test]
fn ask_judgment_annotations_bd_7qcw() {
    let router = ToolServer::tool_router();
    let tool = router
        .list_all()
        .into_iter()
        .find(|t| t.name.as_ref() == "ask_judgment")
        .expect("ask_judgment on the surface");
    let ann = tool.annotations.as_ref().expect("annotations set");
    assert_eq!(ann.destructive_hint, Some(true), "journals a row");
    assert_eq!(ann.idempotent_hint, Some(true), "replay on effect_key");
    let desc = tool.description.clone().unwrap_or_default();
    assert!(
        desc.contains("system-one") && desc.contains("Offline"),
        "open-world note names both transports: {desc}"
    );
}

#[test]
fn fetch_inbox_noop_disclosure_bd_7qcw() {
    let router = ToolServer::tool_router();
    let tool = router
        .list_all()
        .into_iter()
        .find(|t| t.name.as_ref() == "fetch_inbox")
        .expect("fetch_inbox on the surface");
    let desc = tool.description.clone().unwrap_or_default();
    assert!(desc.contains("no-op"), "tool description discloses: {desc}");
    let schema = rmcp::schemars::schema_for!(toron_mcp::tools::mail::FetchInboxArgs);
    let props = serde_json::to_value(&schema)
        .expect("schema serializes")
        .pointer("/properties")
        .cloned()
        .expect("properties");
    for field in ["unread_only", "ack_overdue_only"] {
        let fdesc = props
            .get(field)
            .and_then(|f| f.get("description"))
            .and_then(|d| d.as_str())
            .unwrap_or("");
        assert!(
            fdesc.contains("no-op"),
            "{field} arg discloses no-op, got: {fdesc}"
        );
    }
}
