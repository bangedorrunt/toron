// governed-by: ADR-0003
// governed-by: ADR-0017 (project-root canonicalization)
// governed-by: ADR-0036 (serve-side wake sidecar)
#![forbid(unsafe_code)]

use std::sync::Arc;

use anyhow::Context;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ErrorData, Implementation, InitializeResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
    ResourceContents, ResourceTemplate, ResourceUpdatedNotification,
    ResourceUpdatedNotificationParam, ServerCapabilities, ServerConfig, SubscriptionFilter, Tool,
};
use rmcp::service::{RequestContext, RoleServer, SubscriptionContext};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, ServiceExt};
use toron_core::bus::Bus;
use toron_core::bus::in_process::InProcessBusConfig;
use toron_core::bus::presence::{DEFAULT_PRESENCE_TTL, PresenceBroker, spool_path};
use toron_core::mailbox::{Mailbox, UnwrappedMail};

use crate::resources::{
    concrete_resources, notification_resource_uri, read_resource_value, resource_templates,
    resource_uri_supported,
};
use crate::tools::ToolServer;
use crate::workflow_host::WorkflowHost;

/// MCP server for toron.
///
/// Holds an `Arc<dyn Bus>` (toron-core) that backs data resources and the
/// SEP-2575 subscription push, plus the [`ToolServer`] exposing the 38
/// AM-Rust parity tools (bd-led.7.2).
#[derive(Clone, Default)]
pub struct ToronServer {
    bus: Option<Arc<dyn Bus>>,
    tools: Option<ToolServer>,
    presence: Option<PresenceBroker>,
    events: Option<Arc<dyn toron_core::events::store::SubscriptionStore>>,
    /// bd-2ghz8: hashed signal links for count-only hooks.
    signal_links: Option<Arc<dyn toron_core::events::signal::SignalStore>>,
}

impl std::fmt::Debug for ToronServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToronServer")
            .field("bus", &self.bus.is_some())
            .field("tools", &self.tools.is_some())
            .field("presence", &self.presence.is_some())
            .field("events", &self.events.is_some())
            .finish()
    }
}

fn presence_ttl() -> std::time::Duration {
    std::env::var("TORON_PRESENCE_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(DEFAULT_PRESENCE_TTL)
}

fn start_presence(mailbox: &Mailbox) -> PresenceBroker {
    let root = toron_core::config::mailbox_root(mailbox.data_dir());
    PresenceBroker::start_with_spool(
        mailbox.bus().clone(),
        presence_ttl(),
        Some(spool_path(&root)),
    )
}

impl ToronServer {
    /// Create a new toron MCP server.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the in-process bus used to back data resources + subscriptions.
    /// Also wires a mailbox over the same bus so the 38 tools are served.
    pub fn with_bus(mut self, bus: Arc<dyn Bus>) -> Self {
        let data_dir = std::env::temp_dir().join("toron-mcp-demo");
        let mailbox = Mailbox::new(bus.clone(), data_dir, None);
        let mailbox = match toron_plane::LivePanePlane::connect_default() {
            Ok(live) => mailbox.with_plane(std::sync::Arc::new(live)),
            Err(_) => mailbox,
        };
        self.presence = Some(start_presence(&mailbox));
        self.bus = Some(bus);
        self.tools = Some(ToolServer::new(Arc::new(mailbox)));
        self
    }

    /// Attach a fully-wired mailbox (bus + index pool) backing the tool
    /// surface. The bus is shared so resource/subscription features work too.
    pub fn with_mailbox(mut self, mailbox: Mailbox) -> Self {
        self.presence = Some(start_presence(&mailbox));
        self.bus = Some(mailbox.bus().clone());
        if let Some(pool) = mailbox.index_pool() {
            self.events = Some(Arc::new(
                toron_core::events::store::SqliteSubscriptionStore::new(pool.clone()),
            ));
        }
        self.tools = Some(ToolServer::new(Arc::new(mailbox)));
        self
    }

    /// The live presence broker for this host. The broker folds kind-20001
    /// off the bus together with the cross-process spool, so a heartbeat
    /// written by the CLI is visible to the daemon (and vice versa).
    pub const fn presence(&self) -> Option<&PresenceBroker> {
        self.presence.as_ref()
    }

    /// The attached bus, if any.
    pub fn bus(&self) -> Option<&Arc<dyn Bus>> {
        self.bus.as_ref()
    }

    /// The attached tool surface, if any.
    pub const fn tools(&self) -> Option<&ToolServer> {
        self.tools.as_ref()
    }

    /// Server capabilities advertised via `initialize`/`discover`.
    ///
    /// Logging is deliberately NOT advertised: SEP-2577 deprecates it and
    /// the server never emits log notifications.
    pub fn capabilities() -> ServerCapabilities {
        let mut caps = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .enable_resources_subscribe()
            .enable_tool_list_changed()
            .build();
        let events = serde_json::json!({"listChanged": false});
        caps.extensions = Some(std::collections::BTreeMap::from([(
            crate::events::EVENTS_EXTENSION.to_string(),
            events.as_object().expect("an object").clone(),
        )]));
        caps
    }
}

impl ServerHandler for ToronServer {
    /// ADR-0039 U1: every real client speaks 2026-07-28. An older client fails
    /// version negotiation cleanly instead of getting a 400 on every request.
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_info(&self) -> ServerConfig {
        InitializeResult::new(Self::capabilities())
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
            .with_server_info(Implementation::new("toron", env!("CARGO_PKG_VERSION")))
    }

    /// ADR-0039 U1: `events/*` are protocol methods, never tools.
    fn on_custom_request(
        &self,
        request: rmcp::model::CustomRequest,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::CustomResult, rmcp::ErrorData>>
    + rmcp::service::MaybeSendFuture
    + '_ {
        let method = request.method.clone();
        let params = request.params.clone();
        let events = self.events.clone();
        let signal_links = self.signal_links.clone();
        let authorization = request
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.headers.get(axum::http::header::AUTHORIZATION))
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        async move {
            if method.starts_with("signal/") {
                let links = signal_links.unwrap_or_else(|| {
                    Arc::new(toron_core::events::signal::FileSignalStore::at_root(
                        &toron_core::config::ToronPaths::resolve().root,
                    )) as Arc<dyn toron_core::events::signal::SignalStore>
                });
                let principal = crate::events::authorize(authorization.as_deref())?;
                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                return crate::events::signal_http::route_signal(
                    &method,
                    principal.name(),
                    links.as_ref(),
                    now_us,
                )
                .unwrap_or_else(|| {
                    Err(rmcp::ErrorData::new(
                        rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                        method,
                        None,
                    ))
                });
            }
            if !method.starts_with("events/") {
                return Err(rmcp::ErrorData::new(
                    rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                    method,
                    None,
                ));
            }
            let principal = crate::events::authorize(authorization.as_deref())?;
            let now_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            crate::events::route_events(
                &method,
                params.as_ref(),
                &principal,
                events.as_deref(),
                now_us,
            )
            .await
            .unwrap_or_else(|| {
                Err(rmcp::ErrorData::new(
                    rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                    method,
                    None,
                ))
            })
        }
    }

    /// The 38 AM-Rust parity tools (bd-led.7.2), delegated to `ToolServer`.
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        match &self.tools {
            Some(tools) => tools.list_tools(request, context).await,
            None => Ok(ListToolsResult::default()),
        }
    }

    /// Tool dispatch, delegated to `ToolServer`.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        match &self.tools {
            Some(tools) => tools.call_tool(request, context).await,
            None => Err(ErrorData::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >()),
        }
    }

    /// Tool definition lookup, delegated to `ToolServer`.
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools.as_ref().and_then(|tools| tools.get_tool(name))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let resources: Vec<Resource> = concrete_resources()
            .map(|(name, uri, desc)| {
                Resource::new(uri.to_string(), name.to_string())
                    .with_title(name.to_string())
                    .with_description(desc.to_string())
                    .with_mime_type("application/json")
            })
            .collect();
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let templates: Vec<ResourceTemplate> = resource_templates()
            .map(|(name, uri, desc)| {
                ResourceTemplate::new(uri.to_string(), name.to_string())
                    .with_title(name.to_string())
                    .with_description(desc.to_string())
                    .with_mime_type("application/json")
            })
            .collect();
        Ok(ListResourceTemplatesResult::with_all_items(templates))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let uri = request.uri.as_str();
        let value = if let Some(mb) = self.tools.as_ref().map(super::tools::ToolServer::mailbox) {
            crate::resources::project_resource_value(mb, uri)
                .await
                .or_else(|| read_resource_value(uri))
        } else {
            read_resource_value(uri)
        };
        match value {
            Some(value) => {
                let text = serde_json::to_string_pretty(&value).map_err(|e| {
                    ErrorData::internal_error(format!("serialize resource: {e}"), None)
                })?;
                let contents = ResourceContents::text(text, uri).with_mime_type("application/json");
                Ok(ReadResourceResponse::Complete(ReadResourceResult::new(
                    vec![contents],
                )))
            }
            None => Err(ErrorData::invalid_params(
                format!("resource not found: {uri}"),
                None,
            )),
        }
    }

    /// Accept subscriptions for resource updates + tool_list_changed.
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        let mut accepted = SubscriptionFilter::builder().tools_list_changed();
        if let Some(uris) = &requested.resource_subscriptions {
            let keep: Vec<String> = uris
                .iter()
                .filter(|u| resource_uri_supported(u))
                .cloned()
                .collect();
            if !keep.is_empty() {
                accepted = accepted.resource_subscriptions(keep);
            }
        }
        Some(accepted.build())
    }

    /// Wire the bus to the subscription sink: push matching events <50ms.
    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        let Some(bus) = self.bus() else {
            context.cancelled().await;
            return Ok(());
        };
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .map_err(|e| ErrorData::internal_error(format!("bus subscribe: {e}"), None))?;
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                ev = sub.next() => {
                    let Some(ev) = ev else { return Ok(()) };
                    if let toron_core::bus::SubscriptionEvent::Event(event) = ev {
                        let uri = notification_resource_uri(&event);
                        let notification =
                            ResourceUpdatedNotification::new(ResourceUpdatedNotificationParam::new(uri));
                        if context.sink().send(notification.into()).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Build the mailbox for MCP serve / robot CLI (ADR-0007 R2).
///
/// **Production (default):** durable root from [`toron_core::config::ToronPaths`]
/// (`TORON_DATA_DIR` → `TORON_MCP_DATA_DIR` → `$XDG_DATA_HOME/toron`).
/// Index SQLite lives at `{root}/index.sqlite`; project roots under
/// `{root}/projects/<slug>/`; operator key `{root}/operator/operator.ncryptsec`,
/// mode 0600 when present (ADR-0007 D3).
///
/// **Demo / test isolation:** `TORON_DEMO=1` → unique temp dir per call
/// (bounds KDF scan); `TORON_MCP_DATA_DIR` → a caller-provided shared dir
/// (cli_robot cross-process tests set it so both processes see one root).
///
/// R2.0 KDF cache still bounds warm rebuilds inside a process.
pub async fn demo_mailbox() -> anyhow::Result<Mailbox> {
    build_mailbox(None).await
}

/// [`demo_mailbox`] plus the ADR-0007 R3 post-unwrap trigger channel.
///
/// The serve paths use this variant: the `tx` half is attached via
/// [`Mailbox::with_trigger_tx`] so every successfully-projected gift-wrap
/// notifies the channel; the `rx` half feeds
/// [`WorkflowHost::spawn_mail_trigger_loop`] (F1 `message_posted`).
pub async fn demo_mailbox_with_trigger()
-> anyhow::Result<(Mailbox, tokio::sync::mpsc::UnboundedReceiver<UnwrappedMail>)> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mailbox = build_mailbox(Some(tx)).await?;
    Ok((mailbox, rx))
}

/// ADR-0036 Spec 2 (bd-ewsi): the serving process IS the wake sidecar —
/// one held `subscribe_trusted` fans bus events over `wake.sock` at the
/// path [`toron_core::config::state_dir`] derives for EVERYONE (toron-cli
/// slice B, doorbell `discover_from`), so producer and consumers agree by
/// construction instead of by review.
///
/// Best-effort per Spec 2.5: a live peer on the socket (exotic layout —
/// two serves sharing one state dir) or a bind failure degrades LOUD
/// (`tracing::warn`) and serve continues; consumers already degrade loud
/// on an absent socket. The returned handle must be HELD for the serve's
/// lifetime — dropping it removes the socket file (RAII).
/// U3 producer: one trusted bus subscription for every event subscription,
/// plus the 60s expiry sweep. The returned handle must be held for the serve's
/// lifetime; dropping it aborts the dispatcher task.
async fn start_events_dispatcher(
    mailbox: &Mailbox,
) -> Option<toron_core::events::dispatcher::DispatcherHandle> {
    let pool = mailbox.index_pool()?.clone();
    let store: std::sync::Arc<dyn toron_core::events::store::SubscriptionStore> =
        std::sync::Arc::new(toron_core::events::store::SqliteSubscriptionStore::new(
            pool,
        ));
    let dispatcher = std::sync::Arc::new(toron_core::events::dispatcher::EventDispatcher::new(
        mailbox.bus().clone(),
        store,
    ));
    match dispatcher.start().await {
        Ok(handle) => {
            tracing::info!("events dispatcher started (one trusted bus subscription)");
            Some(handle)
        }
        Err(e) => {
            tracing::warn!("events dispatcher not started: {e}");
            None
        }
    }
}

/// bd-uak1x: the webhook delivery half of the daemon. Built ONCE at startup
/// over the real effect journal; pending retries are re-folded from the journal
/// plus the subscription store at boot, and timers ride the daemon clock. The
/// egress port is injectable so a test can boot this exact production function
/// with a fake HTTP endpoint. The returned handle must be held for the serve's
/// lifetime; dropping it aborts the delivery task.
pub async fn start_webhook_dispatcher<P>(
    mailbox: &Mailbox,
    port: P,
    policy: toron_core::bus::egress::EgressPolicy,
) -> Option<tokio::task::JoinHandle<()>>
where
    P: crate::events::webhook_delivery::EgressPort + 'static,
{
    use crate::events::webhook_delivery::{PostSpecOwned, WebhookDispatcher};

    let pool = mailbox.index_pool()?.clone();
    let store: std::sync::Arc<dyn toron_core::events::store::SubscriptionStore> =
        std::sync::Arc::new(toron_core::events::store::SqliteSubscriptionStore::new(
            pool,
        ));
    let journal_path = toron_core::config::state_dir().join("webhook-journal.jsonl");
    let rows: Vec<String> = std::fs::read_to_string(&journal_path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect();
    // The subscription store keeps only args_hash (never the URL), so the
    // subscribe route writes one spec row per webhook at creation; boot
    // re-reads it to reconstruct pending deliveries.
    let mut specs: Vec<(String, PostSpecOwned)> = Vec::new();
    let spec_path = toron_core::config::state_dir().join("webhook-specs.jsonl");
    if let Ok(body) = std::fs::read_to_string(&spec_path) {
        for line in body.lines() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let (Some(id), Some(url), Some(whsec)) = (
                v.get("id").and_then(|x| x.as_str()),
                v.get("url").and_then(|x| x.as_str()),
                v.get("secret").and_then(|x| x.as_str()),
            ) else {
                continue;
            };
            specs.push((
                format!("webhook:{id}"),
                PostSpecOwned {
                    url: url.to_string(),
                    body: b"{}".to_vec(),
                    secret_whsec: whsec.to_string(),
                    webhook_id: format!("msg_{id}"),
                },
            ));
        }
    }
    let specs_by_id: std::collections::BTreeMap<String, PostSpecOwned> =
        specs.iter().cloned().collect();
    let mut dispatcher = WebhookDispatcher::new(port, policy);
    dispatcher.refold(&rows, specs);

    // ONE trusted bus subscription for the whole daemon (bd-kz33f pattern) —
    // the live-event half of webhook delivery. Facts are matched with the SAME
    // catalogue classifier the stream path uses; no second matcher here.
    let started = toron_core::nostr::Timestamp::now();
    let mut bus_sub = match mailbox
        .bus()
        .subscribe_trusted(toron_core::bus::Filter::new().since(started))
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("webhook dispatcher bus subscription failed: {e}");
            return None;
        }
    };

    let handle = tokio::spawn(async move {
        let tick_ms = std::env::var("TORON_WEBHOOK_TICK_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(5_000);
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
        let mut seen_lag = bus_sub.lag_count();
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    if let Err(e) = dispatcher.on_clock(now_ms, now_ms / 1000).await {
                        tracing::warn!("webhook delivery clock tick failed: {e}");
                    }
                    let _ = std::fs::write(
                        &journal_path,
                        dispatcher.journal_rows().join("\n") + "\n",
                    );
                }
                ev = bus_sub.next() => {
                    let lag = bus_sub.lag_count();
                    if lag > seen_lag {
                        seen_lag = lag;
                        // A bus lag is a gap: one signed delivery per webhook
                        // subscription, never silence. Same helper as the test.
                        match crate::events::webhook_delivery::deliver_gaps(
                            &mut dispatcher, &specs_by_id, lag,
                        ).await {
                            Ok(n) => tracing::info!("webhook gap lag={lag}: {n} deliveries"),
                            Err(e) => tracing::warn!("webhook gap delivery failed: {e}"),
                        }
                        let _ = std::fs::write(
                            &journal_path,
                            dispatcher.journal_rows().join("\n") + "\n",
                        );
                        continue;
                    }
                    let Some(toron_core::bus::SubscriptionEvent::Event(event)) = ev else {
                        continue;
                    };
                    let fact = toron_core::events::dispatcher::fact_from_event(&event);
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_micros() as i64)
                        .unwrap_or(0);
                    let Ok(active) = store.list_active().await else { continue };
                    for sub in active.into_iter().filter(|s| s.delivery == "webhook") {
                        let subscriber = if sub.principal == "operator" {
                            fact.subject_pin.clone().unwrap_or_default()
                        } else { sub.principal.clone() };
                        let Some(name) = toron_core::events::catalogue::classify(&fact, &subscriber)
                        else { continue };
                        let Some(spec) = specs_by_id.get(&format!("webhook:{}", sub.id)) else {
                            continue;
                        };
                        let Ok(secret) = toron_core::events::webhook::Secret::from_whsec(&spec.secret_whsec) else { continue };
                        let hint = serde_json::json!({
                            "event_id": fact.id, "name": name.as_str(),
                            "timestamp": now_us,
                        }).to_string().into_bytes();
                        let event_id = event.id.to_hex();
                        if let Err(e) = dispatcher.on_event(
                            &sub.id, &event_id, &spec.url, hint, &secret,
                            (now_us / 1_000_000) as u64,
                        ).await {
                            tracing::warn!("webhook delivery failed for {}: {e}", sub.id);
                        }
                    }
                    let _ = std::fs::write(
                        &journal_path,
                        dispatcher.journal_rows().join("\n") + "\n",
                    );
                }
            }
        }
    });
    tracing::info!(
        "webhook dispatcher started ({} pending retries re-folded)",
        0
    );
    Some(handle)
}

async fn start_wake_sidecar(mailbox: &Mailbox) -> Option<toron_core::bus::wake::WakeHandle> {
    use toron_core::bus::wake::{SystemClock, WakeConfig, WakeServer};
    let sock = match std::env::var("TORON_WAKE_SOCK") {
        Ok(p) if !p.is_empty() => std::path::PathBuf::from(p),
        _ => toron_core::config::state_dir().join("wake.sock"),
    };
    let server = WakeServer::new(
        {
            let mut config = WakeConfig::new(&sock);
            if let Some(cap) = std::env::var("TORON_WAKE_MAX_SUBSCRIBERS")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                && cap > 0
            {
                config.max_subscribers = cap;
            }
            config
        },
        mailbox.bus().clone(),
        Arc::new(SystemClock),
    );
    match server.start().await {
        Ok(handle) => {
            tracing::info!(sock = %sock.display(), "wake sidecar listening");
            Some(handle)
        }
        Err(e) => {
            tracing::warn!(
                sock = %sock.display(),
                error = %e,
                "wake sidecar not serving — consumers degrade loud (ADR-0036 Spec 2.5)"
            );
            None
        }
    }
}

/// bd-6wfv: build the mailbox from EXPLICIT paths instead of process-global
/// env vars. The e2e parity suite boots one daemon per cluster test in
/// PARALLEL threads — each `LiveDaemon::boot` used to set
/// `TORON_MCP_DATA_DIR` / `TORON_OPERATOR_KEY` / `TORON_KEY_PASSPHRASE`, and
/// the env set/read race made daemons boot against each other's data dirs
/// (intermittent "no local key" / "unknown agent" flakes). Explicit
/// arguments have no race: every daemon uses exactly its own dir + key.
pub async fn build_mailbox_with_opts(
    trigger_tx: Option<tokio::sync::mpsc::UnboundedSender<UnwrappedMail>>,
    data_dir: std::path::PathBuf,
    operator_key_path: std::path::PathBuf,
    operator_passphrase: &str,
) -> anyhow::Result<Mailbox> {
    std::fs::create_dir_all(&data_dir)?;
    let db = data_dir.join("index.sqlite");
    let operator = toron_core::keys::load_encrypted_key(&operator_key_path, operator_passphrase)
        .map(Some)
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to load operator key from {}: {e}",
                operator_key_path.display()
            )
        })?;
    let pool = toron_core::index::schema::connect(db.to_str().unwrap())
        .await
        .with_context(|| format!("connect {}", db.display()))?;
    toron_core::index::schema::migrate(&pool)
        .await
        .context("migrate")?;
    toron_core::mailbox::aliases::reconcile_aliases(&pool, &data_dir)
        .await
        .context("reconcile_aliases")?;
    let bus = toron_core::bus::InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .context("bus event store ddl/backfill")?;
    let mut mailbox =
        toron_core::mailbox::Mailbox::new(std::sync::Arc::new(bus), data_dir, operator)
            .with_index_pool(pool);
    if let Ok(live) = toron_plane::LivePanePlane::connect_default() {
        mailbox = mailbox.with_plane(std::sync::Arc::new(live));
    }
    if let Some(tx) = trigger_tx {
        mailbox = mailbox.with_trigger_tx(tx);
        mailbox.spawn_unwrap_loop();
    }
    Ok(mailbox)
}

async fn build_mailbox(
    trigger_tx: Option<tokio::sync::mpsc::UnboundedSender<UnwrappedMail>>,
) -> anyhow::Result<Mailbox> {
    use toron_core::config::ToronPaths;

    let (data_dir, db, operator) = if ToronPaths::demo_mode() {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stamp = format!("{}-{}", std::process::id(), seq);
        let data_dir = std::env::temp_dir().join(format!("toron-mcp-demo-{stamp}"));
        std::fs::create_dir_all(&data_dir)?;
        let db = std::env::temp_dir().join(format!("toron-mcp-{stamp}.sqlite"));
        (data_dir, db, operator_keys()?)
    } else if let Some(dir) = std::env::var_os("TORON_MCP_DATA_DIR") {
        let d = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&d)?;
        let db = d.join("index.sqlite");
        (d, db, operator_keys()?)
    } else {
        let paths = ToronPaths::resolve();
        paths.ensure_layout()?;
        let _ = paths.ensure_operator_key_mode();
        let operator = match std::env::var_os("TORON_OPERATOR_KEY") {
            Some(_) => operator_keys()?,
            None => Some(paths.ensure_operator_key(&toron_core::config::operator_passphrase())?),
        };
        (paths.projects_dir(), paths.index_path(), operator)
    };

    let pool = toron_core::index::schema::connect(db.to_str().unwrap())
        .await
        .with_context(|| format!("connect {}", db.display()))?;
    toron_core::index::schema::migrate(&pool)
        .await
        .context("migrate")?;
    toron_core::mailbox::aliases::reconcile_aliases(&pool, &data_dir)
        .await
        .context("reconcile_aliases")?;
    let bus = toron_core::bus::InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .context("bus event store ddl/backfill")?;
    let mut mailbox = Mailbox::new(Arc::new(bus), data_dir, operator).with_index_pool(pool);
    if let Ok(live) = toron_plane::LivePanePlane::connect_default() {
        mailbox = mailbox.with_plane(std::sync::Arc::new(live));
    }
    if let Some(tx) = trigger_tx {
        mailbox = mailbox.with_trigger_tx(tx);
        mailbox.spawn_unwrap_loop();
    }
    Ok(mailbox)
}

/// ADR-0007 R3 serve wire: construct the workflow host over the serve mailbox
/// and spawn BOTH trigger loops (F1 + P11.1):
/// - the post-unwrap `message_posted` loop (gift-wrapped inbound mail fires
///   `message_posted` once; workflow-emitted rumors are stamped + skipped);
/// - the raw-bus reactive loop (reservation_acquired / reservation_released /
///   reaction_added / message_posted-over-raw-kinds) via the trusted ingress
///   subscription (A3 gate bypass — P11.1 bd-foh8.1).
///
/// Embedders register workflows via [`WorkflowHost::add_workflow`]; the serve
/// binary arms the loops so triggers are LIVE the moment a workflow exists.
/// Shutdown signal (bd-slkn): tokio ctrl_c resolves on SIGINT, SIGTERM and
/// SIGHUP on unix — exactly what daemon stop sends. Best-effort by design:
/// a missed signal keeps today's SIGTERM-death behavior, never worse.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn spawn_mail_trigger_host(
    mailbox: Mailbox,
    trigger_rx: tokio::sync::mpsc::UnboundedReceiver<UnwrappedMail>,
) -> Vec<WorkflowHost> {
    match WorkflowHost::new(Arc::new(mailbox), Vec::new()) {
        Ok(host) => {
            host.spawn_mail_trigger_loop(trigger_rx);
            host.spawn_trigger_loop();
            host.spawn_resolution_loop();
            host.spawn_durable_sweep(std::time::Duration::from_secs(30));
            vec![host]
        }
        Err(e) => {
            tracing::warn!("workflow host unavailable; message_posted triggers disabled: {e:#}");
            Vec::new()
        }
    }
}

/// Load the operator key for the MCP server from env, if configured.
///
/// Honors `TORON_OPERATOR_KEY` (a **path**, never a secret on the
/// command line — CLI design) + `TORON_KEY_PASSPHRASE` (the SAME env var
/// `toron key generate` uses via `key_password()` in toron-cli/src/operator.rs,
/// default "toron") — they MUST agree, or a freshly-generated operator key
/// will not decrypt at serve time (aead::Error on passphrase mismatch).
/// Without an operator the parity-privileged ops (`ensure_project`, `macro_start_session`,
/// product ops, force-release) fail with "no operator key configured", which
/// the real-e2e parity test (bd-e2e / opencode agent probe) surfaced as a
/// blocking defect. daemon hosts MUST set these so the full 38-tool surface
/// is exercisable.*/
pub(crate) fn operator_keys() -> anyhow::Result<Option<toron_core::keys::Keys>> {
    let Some(key_os) = std::env::var_os("TORON_OPERATOR_KEY") else {
        return Ok(None);
    };
    let path = std::path::PathBuf::from(key_os);
    if !path.is_file() {
        anyhow::bail!(
            "TORON_OPERATOR_KEY={} is not a file (operator ncryptsec)",
            path.display()
        );
    }
    let pass = std::env::var("TORON_KEY_PASSPHRASE").unwrap_or_else(|_| "toron".to_string());
    toron_core::keys::load_encrypted_key(&path, &pass)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("failed to load operator key from {}: {e}", path.display()))
}

/// Serve the toron MCP server over stdio (transport `transport-io`).
///
/// Blocks until the stdio connection closes.
pub async fn serve_stdio() -> anyhow::Result<()> {
    let (mailbox, trigger_rx) = demo_mailbox_with_trigger().await?;
    let _lock = crate::serve_lock::acquire(&toron_core::config::mailbox_root(mailbox.data_dir()))?;
    let _wake = start_wake_sidecar(&mailbox).await;
    let _events = start_events_dispatcher(&mailbox).await;
    // bd-uak1x: the webhook delivery half runs on the production path.
    let webhook_policy = toron_core::bus::egress::EgressPolicy::default();
    let _webhooks = match crate::events::webhook_delivery::UreqEgress::new(webhook_policy.clone()) {
        Ok(egress) => start_webhook_dispatcher(&mailbox, egress, webhook_policy).await,
        Err(e) => {
            tracing::warn!("webhook dispatcher not started: {e}");
            None
        }
    };
    let hosts = spawn_mail_trigger_host(mailbox.clone(), trigger_rx);
    let server = ToronServer::new().with_mailbox(mailbox);
    let running = server.serve(rmcp::transport::stdio()).await?;
    let res: anyhow::Result<()> = tokio::select! {
        r = running.waiting() => r.map(|_| ()).map_err(anyhow::Error::from),
        _ = shutdown_signal() => Ok(()),
    };
    WorkflowHost::persist_hosts_cursors(&hosts).await;
    res?;
    Ok(())
}

/// Build the axum router with the Streamable HTTP transport mounted at `/mcp`.
///
/// SEP-2567 stateless mode is enabled (`legacy_session_mode(false)`) so the
/// daemon/federation path is horizontally scalable with no session affinity.
/// The served handler carries the 38 AM-Rust parity tools (bd-led.7.2).
pub async fn router() -> anyhow::Result<axum::Router> {
    let (mailbox, trigger_rx) = demo_mailbox_with_trigger().await?;
    router_with_mailbox(mailbox, trigger_rx, &[])
        .await
        .map(|(router, _hosts)| router)
}

/// bd-6wfv: [`router`] against an EXPLICIT data dir + operator key — no
/// process-global env reads, so parallel e2e daemons cannot cross-contaminate
/// (each cluster test boots its own daemon with its own paths). Production
/// and the cli_robot cross-process tests keep using [`router`].
pub async fn router_with_isolated(
    data_dir: std::path::PathBuf,
    operator_key_path: std::path::PathBuf,
    operator_passphrase: &str,
) -> anyhow::Result<axum::Router> {
    let (tx, trigger_rx) = tokio::sync::mpsc::unbounded_channel();
    let mailbox =
        build_mailbox_with_opts(Some(tx), data_dir, operator_key_path, operator_passphrase).await?;
    router_with_mailbox(mailbox, trigger_rx, &[])
        .await
        .map(|(router, _hosts)| router)
}

/// Extra `Host` values the MCP-over-HTTP transport accepts, beyond the
/// loopback defaults (`localhost`, `127.0.0.1`, `::1`).
///
/// Sources, merged and de-duplicated: `extra` (the caller — `toron serve`
/// passes `mcp_allowed_hosts` from config.toml), then `TORON_MCP_ALLOWED_HOSTS`
/// (comma-separated env, highest precedence). Each hostname is also admitted
/// with an explicit `:443` authority since TLS-terminating proxies differ on
/// whether they forward the port.
pub fn resolve_allowed_hosts(extra: &[String]) -> Vec<String> {
    let mut hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    let mut push = |h: &str| {
        let h = h.trim();
        if h.is_empty() {
            return;
        }
        if !hosts.iter().any(|e| e == h) {
            hosts.push(h.to_string());
        }
        if !h.contains(':') {
            let with_port = format!("{h}:443");
            if !hosts.iter().any(|e| e == &with_port) {
                hosts.push(with_port);
            }
        }
    };
    for h in extra {
        push(h);
    }
    if let Ok(env) = std::env::var("TORON_MCP_ALLOWED_HOSTS") {
        for h in env.split(',') {
            push(h);
        }
    }
    hosts
}

async fn router_with_mailbox(
    mailbox: toron_core::mailbox::Mailbox,
    trigger_rx: tokio::sync::mpsc::UnboundedReceiver<toron_core::mailbox::unwrap::UnwrappedMail>,
    extra_hosts: &[String],
) -> anyhow::Result<(axum::Router, Vec<WorkflowHost>)> {
    let hosts = spawn_mail_trigger_host(mailbox.clone(), trigger_rx);
    let index_pool = mailbox.index_pool().cloned().expect("mailbox index pool");
    let server = ToronServer::new().with_mailbox(mailbox);
    let service: StreamableHttpService<ToronServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(server.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_json_response(true)
                .with_legacy_session_mode(false)
                .with_stateless_protocol_metadata_required(true)
                .with_allowed_hosts(resolve_allowed_hosts(extra_hosts)),
        );
    let blob_root = std::env::temp_dir().join("toron-mcp-demo").join("blobs");
    let blossom =
        crate::blob_serve::blossom_router(toron_core::archive::blob::BlobStore::new(blob_root));
    // ADR-0039 D7: `events/stream` is a JSON-RPC POST to /mcp answered with
    // SSE. A layer in front of rmcp recognizes that one method; all other
    // requests pass through unchanged. It shares the store/cursor with poll.
    let stream_state = crate::events_stream::StreamDeps {
        store: std::sync::Arc::new(toron_core::events::store::SqliteSubscriptionStore::new(
            index_pool.clone(),
        )),
        tokens: std::sync::Arc::new(toron_core::events::principal::FileTokenStore::at_root(
            &toron_core::config::ToronPaths::resolve().root,
        )),
    };
    let mcp_router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::events_stream::MCP_BODY_LIMIT,
        ))
        .layer(axum::middleware::from_fn_with_state(
            stream_state,
            crate::events_stream::stream_intercept,
        ));
    let signal_state = crate::events::signal_http::SignalDeps {
        links: Arc::new(toron_core::events::signal::FileSignalStore::at_root(
            &toron_core::config::ToronPaths::resolve().root,
        )),
        events: Arc::new(toron_core::events::store::SqliteSubscriptionStore::new(
            index_pool,
        )),
    };
    let signal_router = axum::Router::new()
        .route(
            "/signal",
            axum::routing::get(crate::events::signal_http::signal_get),
        )
        .with_state(signal_state);
    let router = axum::Router::new()
        .merge(mcp_router)
        .merge(blossom)
        .merge(signal_router);
    Ok((router, hosts))
}

/// Serve the toron MCP server over Streamable HTTP on `bind_addr`
/// (e.g. `127.0.0.1:8000`). `extra_hosts` admits Tailnet/proxy `Host`
/// values (`mcp_allowed_hosts` from config.toml); loopback is always allowed.
pub async fn serve_http(bind_addr: &str, extra_hosts: &[String]) -> anyhow::Result<()> {
    let (mailbox, trigger_rx) = demo_mailbox_with_trigger().await?;
    let _lock = crate::serve_lock::acquire(&toron_core::config::mailbox_root(mailbox.data_dir()))?;
    let listener = match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(listener) => listener,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            match classify_port_peer(bind_addr).await {
                PortPeer::HttpPeer => {
                    tracing::info!(
                        %bind_addr,
                        "toron MCP HTTP already served by another instance; exiting cleanly"
                    );
                    return Ok(());
                }
                PortPeer::NonHttp => return Err(e.into()),
            }
        }
        Err(e) => return Err(e.into()),
    };
    tracing::info!(%bind_addr, "toron MCP Streamable HTTP listening at /mcp");
    let _wake = start_wake_sidecar(&mailbox).await;
    let _events = start_events_dispatcher(&mailbox).await;
    // bd-uak1x: the webhook delivery half runs on the production path.
    let webhook_policy = toron_core::bus::egress::EgressPolicy::default();
    let _webhooks = match crate::events::webhook_delivery::UreqEgress::new(webhook_policy.clone()) {
        Ok(egress) => start_webhook_dispatcher(&mailbox, egress, webhook_policy).await,
        Err(e) => {
            tracing::warn!("webhook dispatcher not started: {e}");
            None
        }
    };
    let (router, hosts) = router_with_mailbox(mailbox, trigger_rx, extra_hosts).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    WorkflowHost::persist_hosts_cursors(&hosts).await;
    Ok(())
}

/// What holds a port our bind could not take.
#[derive(Debug, PartialEq, Eq)]
enum PortPeer {
    /// An HTTP/1.x endpoint answered — treat as a healthy toron serve peer.
    HttpPeer,
    /// Nothing answered HTTP (dead port, or a non-HTTP squatter).
    NonHttp,
}

/// Probe the current holder of `bind_addr`: connect, send a minimal HTTP
/// request, and check for an HTTP/1.x response line. Any HTTP response
/// counts (axum answers 404/405 on probes just like a healthy peer).
async fn classify_port_peer(bind_addr: &str) -> PortPeer {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let connect = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio::net::TcpStream::connect(bind_addr),
    )
    .await;
    let Ok(Ok(mut conn)) = connect else {
        return PortPeer::NonHttp;
    };
    let req = format!("GET /mcp HTTP/1.1\r\nHost: {bind_addr}\r\nConnection: close\r\n\r\n");
    if conn.write_all(req.as_bytes()).await.is_err() {
        return PortPeer::NonHttp;
    }
    let mut buf = [0u8; 16];
    let read = tokio::time::timeout(std::time::Duration::from_secs(2), conn.read(&mut buf)).await;
    match read {
        Ok(Ok(n)) if buf[..n].starts_with(b"HTTP/") => PortPeer::HttpPeer,
        _ => PortPeer::NonHttp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::RESOURCES;

    /// bd-serve-double-spawn-oqe6: bind-conflict classification — a healthy
    /// HTTP peer reads as HttpPeer; junk (silent) holders and dead ports
    /// read as NonHttp so serve still fails loudly on real squatters.
    #[tokio::test]
    async fn port_peer_classifies_http_junk_and_dead() {
        use tokio::io::AsyncWriteExt;

        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let responder = tokio::spawn(async move {
            let (mut c, _) = l.accept().await.unwrap();
            c.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        assert_eq!(classify_port_peer(&addr).await, PortPeer::HttpPeer);
        responder.await.unwrap();

        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let junk = tokio::spawn(async move {
            let (_c, _) = l.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        });
        assert_eq!(classify_port_peer(&addr).await, PortPeer::NonHttp);
        junk.abort();

        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        drop(l);
        assert_eq!(classify_port_peer(&addr).await, PortPeer::NonHttp);
    }

    #[test]
    fn capabilities_advertise_tools_and_resources() {
        let caps = ToronServer::capabilities();
        assert!(caps.tools.is_some(), "tools capability must be enabled");
        assert!(
            caps.resources.is_some(),
            "resources capability must be enabled"
        );
        let ext = caps
            .extensions
            .as_ref()
            .expect("ADR-0039 events extension advertised");
        let value = ext
            .get(crate::events::EVENTS_EXTENSION)
            .expect("events row");
        assert_eq!(value["listChanged"], false, "SEP-3415 capability value");
        assert_eq!(value.len(), 1, "no version key");
    }

    #[test]
    fn only_2026_07_28_is_supported() {
        use rmcp::ServerHandler;
        let versions = ToronServer::new().supported_protocol_versions();
        assert_eq!(versions.as_ref(), &[ProtocolVersion::V_2026_07_28]);
    }

    #[test]
    fn resource_surface_splits_literals_from_templates() {
        assert_eq!(RESOURCES.len(), 25);
        assert_eq!(concrete_resources().count(), 7);
        assert_eq!(resource_templates().count(), 18);
    }

    /// ADR-0007 R3/F1 serve wire e2e: with the trigger channel attached
    /// (`with_trigger_tx`) and `spawn_mail_trigger_loop` running, a
    /// GIFT-WRAPPED `send_message` fires the `message_posted` workflow exactly
    /// ONCE, and the workflow's own stamped reply does not re-trigger
    /// (no self-loop).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serve_wire_gift_wrap_fires_message_posted_once() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::nostr::identity::AgentProfile;
        use toron_workflow::schema::parse_workflow;

        let db = std::env::temp_dir().join(format!("serve-wire-{}.sqlite", uuid::Uuid::new_v4()));
        let data_dir =
            std::env::temp_dir().join(format!("serve-wire-data-{}", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<UnwrappedMail>();
        let mailbox = Arc::new(
            Mailbox::new(bus.clone(), data_dir.clone(), None)
                .with_agent_passphrase("serve-wire-pass")
                .with_trigger_tx(tx)
                .with_index_pool(pool.clone()),
        );
        mailbox
            .register_agent("demo", &AgentProfile::new("demo-user"))
            .await
            .expect("register sender");
        mailbox
            .register_agent("demo", &AgentProfile::new("BlueLake"))
            .await
            .expect("register recipient");
        mailbox
            .register_agent("demo", &AgentProfile::new("workflow-demo"))
            .await
            .expect("register workflow sender");
        mailbox.spawn_unwrap_loop();

        let wf = parse_workflow(
            r##"
name: serve-wire-triage
trigger:
  type: message_posted
guard: "trigger_kind == \"14\""
steps:
  - name: ack
    action: send_dm
    to: "BlueLake"
    body: "workflow ack"
    project: "demo"
"##,
        )
        .expect("parse workflow");
        let host = WorkflowHost::new(mailbox.clone(), vec![wf]).expect("host");
        let host_handle = host.spawn_mail_trigger_loop(rx);

        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new().kinds([toron_core::nostr::Kind::GiftWrap]))
            .await
            .expect("subscribe");
        let mut wraps = 0u32;
        let mut ack_body_seen = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
        mailbox
            .send_message(&toron_core::mailbox::SendMessage {
                project_slug: "demo".into(),
                sender_name: "demo-user".into(),
                to: vec!["BlueLake".into()],
                subject: "trigger me".into(),
                body_md: "hello via serve wire".into(),
                ..Default::default()
            })
            .await
            .expect("send");
        while wraps < 2 && tokio::time::Instant::now() < deadline {
            let ev = match tokio::time::timeout_at(deadline, sub.next()).await {
                Ok(Some(toron_core::bus::SubscriptionEvent::Event(e))) => e,
                _ => continue,
            };
            if ev.kind.as_u16() == 1059 {
                wraps += 1;
                if wraps == 2 {
                    let key_path = data_dir
                        .join("demo")
                        .join("agents")
                        .join("BlueLake")
                        .join("key.ncryptsec");
                    let recipient =
                        toron_core::keys::load_encrypted_key(&key_path, "serve-wire-pass")
                            .expect("disk key");
                    if let Ok(unwrapped) = toron_core::nostr::gift_wrap::unwrap(&recipient, &ev) {
                        ack_body_seen = unwrapped.rumor.content.contains("workflow ack");
                        let stamped = unwrapped.rumor.tags.iter().any(|t| {
                            t.as_slice().first().map(String::as_str) == Some("t")
                                && t.as_slice().get(1).map(String::as_str)
                                    == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
                        });
                        assert!(
                            stamped,
                            "workflow reply must carry the workflow-emitted stamp"
                        );
                    }
                }
            }
        }
        assert_eq!(wraps, 2, "user send + one workflow ack (ran exactly once)");
        assert!(ack_body_seen, "workflow reply delivered with expected body");

        let grace = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
        loop {
            let ev = match tokio::time::timeout_at(grace, sub.next()).await {
                Ok(Some(toron_core::bus::SubscriptionEvent::Event(e))) => e,
                _ => break,
            };
            assert_ne!(
                ev.kind.as_u16(),
                1059,
                "no self-loop: workflow reply must not re-trigger message_posted"
            );
        }
        assert_eq!(
            wraps, 2,
            "no self-loop: exactly user send + workflow ack observed"
        );

        let inbox = mailbox
            .fetch_inbox(&toron_core::mailbox::FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                ..Default::default()
            })
            .await
            .expect("fetch inbox");
        assert_eq!(inbox.len(), 2, "user mail + workflow ack both projected");

        host_handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// bd-cli-kdf (2026-08-17): ONE-SHOT mailbox construction (robot CLI /
    /// bench / tui — no trigger channel) must perform ZERO agent-key KDF
    /// loads, even when the bus store holds gift-wraps. The pre-fix builders
    /// spawned the unwrap loop unconditionally; its bus subscription REPLAYS
    /// stored wraps, the first wrap rebuilt the agent keyring, and a
    /// 489-key production data dir burned 35-200s of scrypt per
    /// `toron mail inbox` / `reserve conflicts` invocation. The serve
    /// topology (trigger channel attached) still builds the keyring — it is
    /// the long-lived projector of record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_shot_mailbox_construction_skips_unwrap_keyring_kdf() {
        use toron_core::mailbox::FetchInbox;
        use toron_core::nostr::identity::AgentProfile;

        let data_dir =
            std::env::temp_dir().join(format!("oneshot-kdf-data-{}", uuid::Uuid::new_v4()));
        let key_path =
            std::env::temp_dir().join(format!("oneshot-kdf-key-{}", uuid::Uuid::new_v4()));
        let op = toron_core::keys::generate();
        toron_core::keys::write_encrypted_key(&key_path, &op, "p", toron_core::keys::TEST_LOG_N)
            .expect("operator key");

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<UnwrappedMail>();
        let serve = build_mailbox_with_opts(Some(tx), data_dir.clone(), key_path.clone(), "p")
            .await
            .expect("serve mailbox");
        serve
            .register_agent("demo", &AgentProfile::new("QuietFern"))
            .await
            .expect("register sender");
        serve
            .register_agent("demo", &AgentProfile::new("LuckyPine"))
            .await
            .expect("register recipient");
        serve
            .send_message(&toron_core::mailbox::SendMessage {
                project_slug: "demo".into(),
                sender_name: "QuietFern".into(),
                to: vec!["LuckyPine".into()],
                subject: "kdf probe".into(),
                body_md: "one wrapped mail".into(),
                ..Default::default()
            })
            .await
            .expect("send");
        for _ in 0..80 {
            if serve.kdf_load_count() > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        let robot = build_mailbox_with_opts(None, data_dir.clone(), key_path.clone(), "p")
            .await
            .expect("robot mailbox");
        for _ in 0..50 {
            if robot.kdf_load_count() > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        assert_eq!(
            robot.kdf_load_count(),
            0,
            "one-shot construction must not replay stored wraps into a KDF rebuild"
        );
        let inbox = robot
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "LuckyPine".into(),
                ..Default::default()
            })
            .await
            .expect("fetch inbox");
        assert_eq!(inbox.len(), 1, "serve-side projection is readable");

        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&key_path);
    }
}
