// governed-by: ADR-0033
//! Judgment tool (bd-ncpt, 3rd additive — NOT part of the AM-Rust parity 38).
//!
//! `ask_judgment` runs flywheel's judgment math out-of-process
//! (`flywheel judgment-ask`, contract agreed with crew A in test-channel),
//! journals one row per `effect_key`, and returns typed answers + the
//! journaled effect. A recorded key replays the stored row without
//! re-invoking (idempotent by effect_key). Tick/DecisionReady feed stays
//! flywheel-side: toron returns + journals, it does not publish.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ToJson, ToolOutput, ToolServer};
use toron_core::mailbox::judgment::JudgmentOutcome;

#[tool_router(router = judgment_router, vis = "pub(crate)")]
impl ToolServer {
    /// `ask_judgment` (additive, bd-ncpt): effect_key + pack_id + state in,
    /// typed answers + journaled JudgmentEffect out. Journals (destructive);
    /// idempotent on effect_key (replay, no re-invoke). REQUIRED:
    /// project_key, effect_key, pack_id, state. Transport offline|system-one
    /// (default offline — pure, no network). timeout_seconds default 30, max
    /// 300. Tick/DecisionReady feed is flywheel-side; this tool returns and
    /// journals the effect. Needs `flywheel` on PATH (or FLYWHEEL_BIN).
    #[tool(
        name = "ask_judgment",
        description = "Ask flywheel judgment math (effect_key + pack_id + state in, typed answers + journaled JudgmentEffect out; idempotent replay on effect_key).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::JudgmentOutcomeOut>(),
        annotations(title = "Ask judgment", destructive_hint = true)
    )]
    async fn ask_judgment(&self, Parameters(args): Parameters<AskJudgmentArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .ask_judgment(
                    &args.project_key,
                    &args.effect_key,
                    &args.pack_id,
                    &args.state,
                    args.transport.as_deref().unwrap_or("offline"),
                    super::nonneg_secs(args.timeout_seconds),
                )
                .await,
        )
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct AskJudgmentArgs {
    pub project_key: String,
    /// Idempotency key: a recorded key replays the stored row, no re-invoke.
    pub effect_key: String,
    /// Flywheel resolves the question pack (unknown pack = typed error).
    pub pack_id: String,
    /// State fields (object); capped at 64KiB serialized.
    pub state: Value,
    /// offline (pure, default) or system-one (live model).
    pub transport: Option<String>,
    /// Child budget, default 30, max 300.
    pub timeout_seconds: Option<i64>,
}

impl ToJson for JudgmentOutcome {
    fn to_json(&self) -> Value {
        let r = &self.record;
        serde_json::json!({
            "effect_key": r.effect_key,
            "pack_id": r.pack_id,
            "pack_version": r.pack_version,
            "model_id": r.model_id,
            "confidence": r.confidence,
            "usage": r.usage,
            "fallback_flag": r.fallback_flag,
            "escalated": r.escalated,
            "note": r.note,
            "answers": r.answers,
            "replayed": self.replayed,
        })
    }
}
