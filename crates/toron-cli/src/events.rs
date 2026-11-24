// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! CLI-only `toron events token mint|list|revoke` (bd-6bgrd).
//! Minting requires the ADR-0038 operator key; no key material is ever printed.

use anyhow::{Context, bail};
use serde_json::json;

use crate::cli::{EventsCommand, TokenCommand, TokenMintArgs, TokenRevokeArgs};
use toron_core::config;
use toron_core::events::principal::{self, FileTokenStore, Principal, Scope, TokenStore};

/// Resolve and load the operator key to prove mint authority (ADR-0038). The
/// key is never printed, logged or put in a response.
fn require_operator_key() -> anyhow::Result<()> {
    let path = config::operator_key_path();
    if !path.is_file() {
        bail!(
            "operator key missing at {} — minting events tokens requires the ADR-0038 operator key",
            path.display()
        );
    }
    let passphrase = config::operator_passphrase();
    toron_core::keys::load_encrypted_key(&path, &passphrase)
        .map_err(|e| anyhow::anyhow!("operator key {}: {e}", path.display()))?;
    Ok(())
}

fn store() -> FileTokenStore {
    FileTokenStore::at_root(&config::ToronPaths::resolve().root)
}

fn now_us() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

fn mint(args: &TokenMintArgs) -> anyhow::Result<()> {
    require_operator_key()?;
    let (principal, scope) = if args.operator {
        if args.pin.is_some() || args.project.is_some() {
            bail!("--operator cannot be combined with --pin/--project");
        }
        (Principal::Operator, Scope::operator())
    } else {
        let pin = args
            .pin
            .as_deref()
            .context("--pin is required (or use --operator)")?;
        let project = args
            .project
            .as_deref()
            .context("--project is required for a pin token")?;
        let mut scope = Scope::for_pin(pin, project);
        if let Some(secs) = args.ttl_secs {
            if secs <= 0 || secs > principal::MAX_PIN_TTL_US / 1_000_000 {
                bail!("--ttl-secs must be between 1 and 86400 for a pin token");
            }
            scope.ttl_us = Some(secs * 1_000_000);
        }
        (
            Principal::Pin {
                pin: pin.to_string(),
                project: project.to_string(),
            },
            scope,
        )
    };
    let plaintext = principal::mint(principal.clone(), scope.clone(), now_us(), &store())?;
    // Plaintext shown ONCE here. Never written to the store or logged.
    println!(
        "{}",
        json!({
            "token": plaintext,
            "principal": principal,
            "scope": scope,
        })
    );
    Ok(())
}

fn list() -> anyhow::Result<()> {
    let rows = store().list()?;
    let safe: Vec<_> = rows
        .iter()
        .map(|r| {
            json!({
                "hash": r.hash,
                "principal": r.principal,
                "scope": r.scope,
                "created_us": r.created_us,
                "revoked": r.revoked,
            })
        })
        .collect();
    println!("{}", json!({ "tokens": safe }));
    Ok(())
}

fn revoke(args: &TokenRevokeArgs) -> anyhow::Result<()> {
    require_operator_key()?;
    store().revoke(&args.hash)?;
    println!("{}", json!({ "revoked": args.hash }));
    Ok(())
}

pub fn run_events(cmd: &EventsCommand) -> anyhow::Result<()> {
    match cmd {
        EventsCommand::Token(TokenCommand::Mint(args)) => mint(args),
        EventsCommand::Token(TokenCommand::List) => list(),
        EventsCommand::Token(TokenCommand::Revoke(args)) => revoke(args),
    }
}
