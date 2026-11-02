// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! Context-free effect-leaf policy (bd-leaf-seams-9rwi).
//!
//! The linear durable host dispatches effect leaves with no Resonate
//! [`Context`] to read dependencies from, so the policy the workflow engine
//! applies to a HostSink leaf lives here as plain data: [`LeafSeams`] carries
//! the two runners, the verify sink, and the journal mode; [`dispatch_leaf`] is
//! the ONE implementation of what happens with them.
//! `control_flow::dispatch_leaf_or_compat` is the context-bearing adapter —
//! [`LeafSeams::from_context`] plus this function — so the engine path and the
//! linear host path cannot drift into two policies.
//!
//! The policy is unchanged from the in-place version it replaced:
//! - the async runner wins and is awaited inline, never thread-hop bridged
//!   (bd-ckkf, ADR-0035 Spec 2.2);
//! - `verify` is the one leaf the engine executes itself, so the sink runs
//!   FIRST and its result rides [`crate::engine::RunContext::provider_result`]
//!   — the runner settles AROUND an effect that already happened instead of
//!   re-performing it (ADR-0024 Spec 2);
//! - with NO runner, [`EffectJournalMode`] decides: `Enforced` refuses
//!   fail-closed, `Recording` marks the value with
//!   [`EFFECT_JOURNAL_BYPASSED`] and performs nothing (ADR-0032 Spec 6).

use crate::schema::Action;

use super::{
    ActionInvocation, AsyncEffectRunner, EFFECT_JOURNAL_BYPASSED, EffectJournalMode, EffectRunner,
    VerifySink, invoke_compat,
};

/// The four host-owned seams the effect-leaf policy depends on, lifted out of
/// the Resonate dependency map so a host with no `Context` can supply them.
///
/// `verify_sink` is concrete rather than `Option`: [`VerifySink`] already holds
/// an optional inner closure, and `Default` (nothing installed) makes `project`
/// answer `{ok:false, error:"verify sink missing"}` instead of performing.
#[derive(Clone, Default)]
pub struct LeafSeams {
    /// Awaited inline. Preferred over [`Self::sync_runner`] whenever present.
    pub async_runner: Option<AsyncEffectRunner>,
    /// The legacy blocking surface, used when no async runner is injected.
    pub sync_runner: Option<EffectRunner>,
    /// The one sink the engine executes itself (`verify`, ADR-0024 Spec 2).
    pub verify_sink: VerifySink,
    /// What to do when neither runner is present (ADR-0032 Spec 6).
    pub journal_mode: EffectJournalMode,
}

impl LeafSeams {}

/// Dispatch one HostSink leaf against an explicit seam set.
///
/// `sink` is the name [`crate::engine::partition`] assigned the action; it is
/// used verbatim in the refusal message, so callers pass their own partition
/// result rather than re-deriving it here.
///
/// Returns the leaf's value, never an `Option`: a refusal is a value, so no
/// caller can mistake "refused" for "not my concern" and re-dispatch it.
pub async fn dispatch_leaf(
    seams: &LeafSeams,
    invocation: &ActionInvocation,
    sink: &'static str,
) -> serde_json::Value {
    // Only `verify` needs a second, carrier-bearing invocation, and only when a
    // runner will observe it: the clone is avoided on the common path, and the
    // no-runner branch below projects the sink itself exactly once.
    let with_verify_carrier = || match &invocation.action {
        Action::Verify { command } => {
            let mut journaled = invocation.clone();
            journaled.run.provider_result = Some(seams.verify_sink.project(command));
            Some(journaled)
        }
        _ => None,
    };

    if let Some(afx) = seams.async_runner.clone() {
        let carried = with_verify_carrier();
        return runner_outcome(afx(carried.as_ref().unwrap_or(invocation)).await);
    }
    if let Some(fx) = seams.sync_runner.clone() {
        let carried = with_verify_carrier();
        return runner_outcome(fx(carried.as_ref().unwrap_or(invocation)));
    }
    without_journal_seam(seams, invocation, sink)
}

/// Runner outcomes are values throughout (`{ok:false,…}` failures, never a bare
/// `Err`), so the engine's failure predicate keeps working.
fn runner_outcome(outcome: anyhow::Result<serde_json::Value>) -> serde_json::Value {
    match outcome {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
    }
}

/// No runner was injected: ADR-0032 Spec 6 says an unjournaled effect is never
/// performed. `Enforced` refuses; `Recording` records the gap visibly so a dry
/// arm reads as unperformed, never as success.
fn without_journal_seam(
    seams: &LeafSeams,
    invocation: &ActionInvocation,
    sink: &'static str,
) -> serde_json::Value {
    if seams.journal_mode == EffectJournalMode::Enforced {
        eprintln!(
            "toron-workflow: effectful '{sink}' action '{}' REFUSED — no EffectRunner is \
             injected (EffectJournalMode::Enforced): an external effect is never performed \
             without its journal (ADR-0032 Spec 6, bd-engine-run-path-journaling-fwlx)",
            invocation.action.type_name()
        );
        return serde_json::json!({
            "ok": false,
            "error": format!(
                "effectful '{sink}' action '{}' refused: no EffectRunner injected \
                 (EffectJournalMode::Enforced, ADR-0032 Spec 6) — build the engine with a \
                 journaling runner or drop to Recording for dry arms",
                invocation.action.type_name()
            ),
            "kind": "effect_journal_unavailable",
        });
    }
    eprintln!(
        "toron-workflow: effectful '{sink}' action '{}' wrote NO effect-journal row \
         ({EFFECT_JOURNAL_BYPASSED}) — ADR-0032 Spec 6 forbids a silent bypass; inject an \
         EffectRunner covering this sink",
        invocation.action.type_name()
    );
    let mut value = match &invocation.action {
        // `Recording` records instead of performs (bd-ctj7): the verify sink
        // still projects (it already executed), every other HostSink leaf
        // reports its missing sink — nothing performs here, ever.
        Action::Verify { command } => seams.verify_sink.project(command),
        _ => invoke_compat(&invocation.action, |_| None),
    };
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "effect_journal".into(),
            serde_json::json!(EFFECT_JOURNAL_BYPASSED),
        );
    }
    value
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::engine::control_flow::invocation_for_leaf;
    use crate::engine::{VerifyRequest, VerifyResult};

    fn send_dm() -> ActionInvocation {
        invocation_for_leaf(
            Action::SendDm {
                to: "pi".into(),
                body: "hi".into(),
                project: None,
            },
            "run-1",
            "digest-1",
            "notify",
            1,
            1,
            None,
        )
    }

    fn verify(command: &str) -> ActionInvocation {
        invocation_for_leaf(
            Action::Verify {
                command: command.into(),
            },
            "run-1",
            "digest-1",
            "check",
            1,
            1,
            None,
        )
    }

    /// Seam-only proof: a HostSink leaf dispatches through the async runner with
    /// no `Context` anywhere in this test.
    #[tokio::test]
    async fn async_runner_dispatches_host_sink() {
        let seen = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&seen);
        let seams = LeafSeams {
            async_runner: Some(Arc::new(move |inv: &ActionInvocation| {
                let flag = Arc::clone(&flag);
                let type_name = inv.action.type_name();
                Box::pin(async move {
                    flag.store(type_name == "send_dm", Ordering::SeqCst);
                    Ok(serde_json::json!({ "ok": true, "via": "async" }))
                })
            })),
            ..LeafSeams::default()
        };

        let value = dispatch_leaf(&seams, &send_dm(), "mailbox").await;

        assert_eq!(value["via"], serde_json::json!("async"), "{value}");
        assert!(
            seen.load(Ordering::SeqCst),
            "runner saw the rendered action"
        );
    }

    /// The carrier rule the extraction must not lose: `verify` runs through the
    /// embedding's sink FIRST and the runner observes the result, so the
    /// journal settles around an effect that already happened (ADR-0024 Spec 2).
    #[tokio::test]
    async fn async_runner_settles_around_the_verify_sink_result() {
        let seams = LeafSeams {
            async_runner: Some(Arc::new(|inv: &ActionInvocation| {
                let carried = inv.run.provider_result.clone();
                Box::pin(async move { Ok(serde_json::json!({ "ok": true, "carried": carried })) })
            })),
            verify_sink: VerifySink(Arc::new(std::sync::Mutex::new(Some(Arc::new(
                |req: &VerifyRequest| {
                    Ok(VerifyResult {
                        ok: true,
                        exit: 0,
                        stdout_path: Some(format!("target/{}.log", req.command)),
                        stderr_path: None,
                        error: None,
                    })
                },
            ))))),
            ..LeafSeams::default()
        };

        let value = dispatch_leaf(&seams, &verify("cargo test"), "verify").await;

        assert_eq!(
            value["carried"],
            serde_json::json!({
                "ok": true,
                "exit": 0,
                "stdout_path": "target/cargo test.log",
            }),
            "the runner carries the sink's already-executed result: {value}"
        );
    }

    /// `Enforced` with no runner is a refusal VALUE, never a silent pass.
    #[tokio::test]
    async fn enforced_refuses_a_host_sink_with_no_runner() {
        let seams = LeafSeams {
            journal_mode: EffectJournalMode::Enforced,
            ..LeafSeams::default()
        };

        let value = dispatch_leaf(&seams, &send_dm(), "mailbox").await;

        assert_eq!(value["ok"], serde_json::json!(false), "{value}");
        assert_eq!(
            value["kind"],
            serde_json::json!("effect_journal_unavailable"),
            "{value}"
        );
        assert!(
            value["error"]
                .as_str()
                .is_some_and(|e| e.contains("no EffectRunner injected")),
            "the refusal names the missing seam: {value}"
        );
        assert!(
            value.get("effect_journal").is_none(),
            "a refused effect never looks journaled: {value}"
        );
    }

    /// `Recording` with no runner marks the bypass and performs nothing.
    #[tokio::test]
    async fn recording_marks_the_bypass_marker() {
        let seams = LeafSeams::default();

        let value = dispatch_leaf(&seams, &send_dm(), "mailbox").await;

        assert_eq!(
            value["effect_journal"],
            serde_json::json!(EFFECT_JOURNAL_BYPASSED),
            "{value}"
        );
        assert_eq!(
            value["ok"],
            serde_json::json!(false),
            "Recording reports the missing sink, it never performs: {value}"
        );
    }
}
