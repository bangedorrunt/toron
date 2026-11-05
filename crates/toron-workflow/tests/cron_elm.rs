// governed-by: ADR-0005
//! Host-driven cron scheduler: one latest tick per window, idempotent replay,
//! pause/resume, and a persistent checkpoint.

use toron_core::kernel::{CronPolicy, StartOutcome};
use toron_workflow::cron_elm::ElmCron;

/// A real Unix instant on a 15-minute boundary. `0` is the manager's
/// never-fired checkpoint, so the tests stay off the epoch itself.
const T0: u64 = 1_700_000_000_000 - (1_700_000_000_000 % (15 * 60_000));

fn minutes(n: u64) -> u64 {
    n * 60_000
}

#[test]
fn one_latest_tick_per_window_and_idempotent_replay() {
    let mut cron = ElmCron::new();
    cron.add("digest", "*/15 * * * *", 0).expect("valid cron");

    let fires = cron.tick(T0).expect("tick");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].tick_ms, T0);
    assert_eq!(fires[0].key, format!("cron:digest:{T0}"));
    assert!(matches!(fires[0].outcome, StartOutcome::Started { .. }));

    assert!(cron.tick(T0).expect("second tick").is_empty());

    let fires = cron.tick(T0 + minutes(15)).expect("catch-up");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].tick_ms, T0 + minutes(15));
}

#[test]
fn reset_checkpoint_reattaches_to_the_same_engine_key() {
    let mut cron = ElmCron::new();
    cron.add("digest", "*/15 * * * *", 0).expect("valid cron");
    let first = cron.tick(T0).expect("first");
    assert!(matches!(first[0].outcome, StartOutcome::Started { .. }));

    assert!(cron.set_last_fired_ms("digest", 0));
    let replay = cron.tick(T0).expect("replay");
    assert_eq!(replay.len(), 1);
    assert!(
        matches!(replay[0].outcome, StartOutcome::Already { .. }),
        "the firing key is stable: {replay:?}"
    );
}

#[test]
fn pause_resume_replace_and_remove() {
    let mut cron = ElmCron::new();
    cron.add("daily", "0 8 * * *", 0).expect("valid cron");
    assert!(cron.set_enabled("daily", false));
    assert!(
        cron.tick(T0 + minutes(1_000))
            .expect("paused tick")
            .is_empty()
    );

    assert!(cron.set_enabled("daily", true));
    let fires = cron.tick(T0 + minutes(1_000)).expect("resumed tick");
    assert_eq!(fires.len(), 1);
    assert_eq!(
        fires[0].tick_ms % 86_400_000,
        8 * 3_600_000,
        "fires at 08:00 UTC"
    );

    assert!(cron.set_enabled("daily", false));
    cron.add(
        "daily",
        "0 9 * * *",
        cron.last_fired_ms("daily").expect("checkpoint"),
    )
    .expect("replace");
    assert!(
        cron.tick(T0 + minutes(2_000))
            .expect("paused again")
            .is_empty()
    );
    assert!(cron.remove("daily"));
    assert!(!cron.remove("daily"));
}

#[test]
fn invalid_cron_is_a_typed_error() {
    let mut cron = ElmCron::new();
    assert!(cron.add("bad", "not a cron", 0).is_err());
    assert!(cron.last_fired_ms("bad").is_none());
}

#[test]
fn skip_policy_fires_fresh_and_skips_stale() {
    let mut cron = ElmCron::new();
    cron.add_with_policy(
        "digest",
        "*/15 * * * *",
        T0,
        CronPolicy::Skip { grace_ms: 60_000 },
    )
    .expect("valid cron");
    let fires = cron.tick(T0 + minutes(15)).expect("fresh tick");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].tick_ms, T0 + minutes(15));
    let mut stale = ElmCron::new();
    stale
        .add_with_policy(
            "digest",
            "*/15 * * * *",
            T0,
            CronPolicy::Skip { grace_ms: 60_000 },
        )
        .expect("valid cron");
    assert!(
        stale.tick(T0 + minutes(20)).expect("stale tick").is_empty(),
        "a tick 5min past grace must skip"
    );
    assert_eq!(
        stale.last_fired_ms("digest"),
        Some(T0 + minutes(20)),
        "skipped ticks advance the checkpoint"
    );
    let fires = stale.tick(T0 + minutes(30)).expect("next tick");
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].tick_ms, T0 + minutes(30));
}
