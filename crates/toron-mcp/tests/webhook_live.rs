//! Gated live egress-guard check (bd-uak1x round 2). Never touches
//! :18080/:18980. Ignored by default; needs FLYWHEEL_LIVE_E2E=1.
//!
//! A full signed-delivery smoke against a local HTTPS receiver needs a cert
//! the pinned rustls client trusts AND a loopback allowlist carve-out, which
//! would weaken the production guard. This live leg intentionally verifies the
//! guard's DEFAULT refusal of loopback/link-local with the real client type,
//! plus the allowlisted tailnet path; the signed POST itself is covered by the
//! interpreter tests with a fake HTTP port and the published signing vectors.

use toron_core::bus::egress::{self, EgressPolicy};
use toron_mcp::events::webhook_delivery::ReqwestEgress;

fn enabled() -> bool {
    std::env::var("FLYWHEEL_LIVE_E2E").ok().as_deref() == Some("1")
}

#[test]
#[ignore = "needs FLYWHEEL_LIVE_E2E=1"]
fn the_real_egress_guard_refuses_loopback_and_admits_the_tailnet() {
    if !enabled() {
        return;
    }
    let policy = EgressPolicy::default();
    // The real client type builds with the pinned egress policy.
    let _egress = ReqwestEgress::new(policy.clone()).expect("client builds");

    // A local receiver URL is NOT allowlisted by default — this is the SSRF
    // guard doing its job, not a test setup failure.
    assert!(
        egress::validate_target("https://127.0.0.1:18443/hook", &policy).is_err(),
        "loopback must be refused by default"
    );
    assert!(
        egress::validate_target("https://169.254.169.254/hook", &policy).is_err(),
        "link-local metadata must be refused by default"
    );
    let target = egress::validate_target("https://foo.example-tailnet.ts.net/hook", &policy)
        .expect("tailnet host is allowlisted");
    assert_eq!(target.port, 443);
}
