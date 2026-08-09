// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! Product-bus tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: product bus (5) tools.
//! `#[tool_router(router = product_router)]` emits
//! `pub(crate) fn product_router()`; `tools/mod.rs` merges it into the single
//! 38-tool surface via `ToolRouter`'s `Add`. Frozen names/args/returns:
//! ZERO change (ADR-0007 C3).

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};


use super::{ToolOutput, ToolServer};

#[tool_router(router = product_router, vis = "pub(crate)")]
impl ToolServer {
    // ------------------------------------------------------------------
    // PRODUCT BUS (5)
    // ------------------------------------------------------------------

    /// `ensure_product`: idempotent product manifest (kind-30011).
    #[tool(
        name = "ensure_product",
        description = "Idempotent product manifest (kind-30011) for a product key.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ProductInfoOut>(),
        annotations(title = "Ensure product", destructive_hint = true)
    )]
    async fn ensure_product(&self, Parameters(args): Parameters<EnsureProductArgs>) -> ToolOutput {
        let key = args
            .product_key
            .clone()
            .or_else(|| args.name.clone())
            .unwrap_or_else(|| "product".to_string());
        let name = args.name.clone().unwrap_or_else(|| key.clone());
        ToolOutput::result(self.mailbox.ensure_product(&key, &name).await)
    }

    /// `products_link`: link a project into a product (idempotent).
    #[tool(
        name = "products_link",
        description = "Link a project into a product (idempotent; kind-30011 a-tag).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ProductInfoOut>(),
        annotations(title = "Products link", destructive_hint = true)
    )]
    async fn products_link(&self, Parameters(args): Parameters<ProductsLinkArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .products_link(&args.product_key, &args.project_key)
                .await,
        )
    }

    /// `search_messages_product`: fan search across member projects.
    #[tool(
        name = "search_messages_product",
        description = "Fan an FTS5 search across a product's member projects.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<Vec<crate::responses::ProjectSearchMessageOut>>(),
        annotations(title = "Search messages (product)", read_only_hint = true)
    )]
    async fn search_messages_product(
        &self,
        Parameters(args): Parameters<SearchMessagesProductArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .search_messages_product(
                    &args.product_key,
                    &args.query,
                    args.limit.map(|l| l as usize),
                    args.include_body_md.unwrap_or(false),
                )
                .await,
        )
    }

    /// `fetch_inbox_product`: fan inbox fetch across member projects.
    #[tool(
        name = "fetch_inbox_product",
        description = "Fan an inbox fetch across a product's member projects.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<Vec<crate::responses::ProjectInboxMessageOut>>(),
        annotations(title = "Fetch inbox (product)", read_only_hint = true)
    )]
    async fn fetch_inbox_product(
        &self,
        Parameters(args): Parameters<FetchInboxProductArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .fetch_inbox_product(&args.product_key, &args.agent_name)
                .await,
        )
    }

    /// `summarize_thread_product`: summarize across member projects.
    #[tool(
        name = "summarize_thread_product",
        description = "Summarize a thread across a product's member projects.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ThreadSummaryOut>(),
        annotations(title = "Summarize thread (product)", read_only_hint = true)
    )]
    async fn summarize_thread_product(
        &self,
        Parameters(args): Parameters<SummarizeThreadProductArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .summarize_thread_product(
                    &args.product_key,
                    &args.thread_id,
                    args.include_examples.unwrap_or(false),
                )
                .await,
        )
    }
}

// ---------------------------------------------------------------------------
// WireArgs — AM-Rust arg parity (field names + Option-ness), one per tool.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct EnsureProductArgs {
    pub product_key: Option<String>,
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ProductsLinkArgs {
    pub product_key: String,
    pub project_key: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SearchMessagesProductArgs {
    pub product_key: String,
    pub query: String,
    pub limit: Option<i32>,
    pub cursor: Option<String>,
    pub project: Option<String>,
    pub sender: Option<String>,
    pub importance: Option<String>,
    pub thread_id: Option<String>,
    pub ranking: Option<String>,
    pub include_body_md: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct FetchInboxProductArgs {
    pub product_key: String,
    pub agent_name: String,
    pub limit: Option<i32>,
    pub urgent_only: Option<bool>,
    pub include_bodies: Option<bool>,
    pub since_ts: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SummarizeThreadProductArgs {
    pub product_key: String,
    pub thread_id: String,
    pub include_examples: Option<bool>,
    pub llm_mode: Option<bool>,
    pub llm_model: Option<String>,
    pub per_thread_limit: Option<i32>,
}
