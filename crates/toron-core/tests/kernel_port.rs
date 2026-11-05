// governed-by: ADR-0040
//! The comms kernel port (ADR-0040): the upstream laws the redesign relies on
//! are reachable and green THROUGH `toron_core::kernel`. The crates come from
//! the sibling elm-effect-rs workspace (no vendored copy), so there is no
//! manifest or rev to pin here.

use toron_core::index::workflow_effects as toron_fx;
use toron_core::kernel::{self, DispatchPlan, EffectState, EffectView, MemLog, ProviderCapability};

#[test]
fn stale_epoch_is_fenced_through_the_kernel_port() {
    kernel::durable_log_laws::stale_epoch_is_fenced(&MemLog::new()).unwrap();
}

#[test]
fn every_upstream_durable_log_law_holds_through_the_kernel_port() {
    let log = MemLog::new();
    for (name, outcome) in kernel::durable_log_laws::all(&log) {
        assert!(outcome.is_ok(), "{name}: {outcome:?}");
    }
}

#[test]
fn kernel_effect_table_is_toron_golden_table() {
    let names = [
        "prepared",
        "in_flight",
        "accepted",
        "settled",
        "failed",
        "unknown",
        "reconcile_pending",
        "cancelled",
        "parked",
    ];
    for from in names {
        for to in names {
            let ours = toron_fx::can_transition(
                toron_fx::EffectState::parse(from).unwrap(),
                toron_fx::EffectState::parse(to).unwrap(),
            );
            let theirs = kernel::can_transition(from.parse().unwrap(), to.parse().unwrap());
            assert_eq!(ours, theirs, "{from} -> {to}");
        }
    }
}

#[test]
fn mail_append_capability_reconciles_and_never_refuses_blind() {
    let cap = ProviderCapability {
        supports_idempotency_key: true,
        supports_reconcile_query: true,
    };
    for state in [
        EffectState::InFlight,
        EffectState::Accepted,
        EffectState::Unknown,
    ] {
        let view = EffectView {
            state,
            attempt: 1,
            lease_live: false,
        };
        assert!(
            matches!(
                kernel::plan_attempt(Some(&view), &cap),
                DispatchPlan::ReconcileFirst { .. }
            ),
            "{state}"
        );
    }
    let none = ProviderCapability {
        supports_idempotency_key: false,
        supports_reconcile_query: false,
    };
    let view = EffectView {
        state: EffectState::Unknown,
        attempt: 1,
        lease_live: false,
    };
    assert!(matches!(
        kernel::plan_attempt(Some(&view), &none),
        DispatchPlan::RefuseBlindRetry { .. }
    ));
}

#[test]
fn parity_laws_hold_through_kernel_port() {
    let make_engine = || {
        kernel::OurEngine::with_journal(kernel::MemStorage::new(), kernel::InMemoryStore::new(64))
    };
    for (name, outcome) in kernel::heartbeat_laws(&mut make_engine()) {
        assert!(outcome.is_ok(), "heartbeat law {name}: {outcome:?}");
    }
    for (name, outcome) in kernel::retry_laws(&mut make_engine()) {
        assert!(outcome.is_ok(), "retry law {name}: {outcome:?}");
    }
    for (name, outcome) in kernel::signal_laws(&mut make_engine()) {
        assert!(outcome.is_ok(), "signal law {name}: {outcome:?}");
    }
    for (name, outcome) in kernel::cancel_tree_laws(&mut make_engine()) {
        assert!(outcome.is_ok(), "cancel tree law {name}: {outcome:?}");
    }
    for (name, outcome) in kernel::lineage_laws(&mut make_engine()) {
        assert!(outcome.is_ok(), "lineage law {name}: {outcome:?}");
    }
    for (name, outcome) in kernel::guard_laws(&mut make_engine()) {
        assert!(outcome.is_ok(), "guard law {name}: {outcome:?}");
    }
}
