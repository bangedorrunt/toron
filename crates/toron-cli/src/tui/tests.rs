// governed-by: ADR-0031
//! Conformance, fake-clock probe-cadence, diagnostic, and layout tests for the Swarm Board.

use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;

use super::host::fake::{FakeHost, Reply};
use super::model::{
    BOARD_SCHEMA, BoardPost, Diagnostic, DiagnosticSeverity, FlywheelPanel, Inspector, LayoutMode,
    LeaseRow, MIN_H, MIN_W, Mode, Model, RosterRow, SealedInboxCounts, SettingsState,
    ToronHealthSignals, diagnose_toron, layout_mode,
};
use super::msg::{Effect, Msg, Op};
use super::{bounded, keymap, update, view};

fn settings() -> SettingsState {
    SettingsState {
        project: "flywheel".into(),
        agent: Some("AmberFalcon".into()),
        npub: Some("npub1test".into()),
        ..Default::default()
    }
}

fn model() -> Model {
    Model::seed(settings())
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn press(m: &Model, code: KeyCode) -> Option<Msg> {
    keymap::translate(m, key(code))
}

#[test]
fn same_msg_log_same_model_and_effects() {
    let log = vec![
        Msg::Init,
        Msg::Tick {
            now_unix: 1_700_000_001,
        },
        Msg::BoardLoaded(vec![BoardPost {
            seq: 1,
            event_id: "e1".into(),
            author: "AmberFalcon".into(),
            body: "first post".into(),
            mentions: vec![],
            created_at: 1_700_000_000,
        }]),
        Msg::SelectInspector(Inspector::Diagnostics),
        Msg::OpenFilter,
        Msg::FilterChar('a'),
        Msg::CommitFilter,
    ];
    let (m1, e1) = update::replay(&model(), &log);
    let (m2, e2) = update::replay(&model(), &log);
    assert_eq!(format!("{m1:?}"), format!("{m2:?}"));
    assert_eq!(format!("{e1:?}"), format!("{e2:?}"));
    assert!(!e1.is_empty());
}

#[test]
fn fake_clock_300_ticks_bounds_toron_and_flywheel_probes() {
    let mut m = model();
    let (next, init_fx) = update::tick(&m, Msg::Init);
    m = next;
    assert!(init_fx.contains(&Effect::ProbeToronHealth));
    assert!(init_fx.contains(&Effect::ProbeFlywheelSnapshot));

    // Complete initial probes at t=1000.
    (m, _) = update::tick(
        &m,
        Msg::ToronHealthLoaded {
            signals: ToronHealthSignals::default(),
            now_unix: 1000,
        },
    );
    (m, _) = update::tick(
        &m,
        Msg::FlywheelSnapshotLoaded {
            panel: None,
            now_unix: 1000,
        },
    );

    let mut toron_probes = 0usize;
    let mut flywheel_probes = 0usize;
    let mut board_polls = 0usize;

    // 300 1s ticks with active user heartbeat every 60s so idle pause does not trigger.
    for s in 1..=300u64 {
        let now = 1000 + s;
        if s % 60 == 0 {
            (m, _) = update::tick(&m, Msg::ToggleRelativeTime);
        }
        let (next, fx) = update::tick(&m, Msg::Tick { now_unix: now });
        m = next;
        for e in fx {
            match e {
                Effect::ProbeToronHealth => {
                    toron_probes += 1;
                    (m, _) = update::tick(
                        &m,
                        Msg::ToronHealthLoaded {
                            signals: ToronHealthSignals::default(),
                            now_unix: now,
                        },
                    );
                }
                Effect::ProbeFlywheelSnapshot => {
                    flywheel_probes += 1;
                    (m, _) = update::tick(
                        &m,
                        Msg::FlywheelSnapshotLoaded {
                            panel: None,
                            now_unix: now,
                        },
                    );
                }
                Effect::LoadBoard | Effect::LoadRoster | Effect::LoadLeases => board_polls += 1,
                _ => {}
            }
        }
    }

    assert_eq!(
        board_polls, 0,
        "posts/roster/leases must be event-driven, never polled"
    );
    assert_eq!(toron_probes, 30, "300s / 10s = 30 toron health probes");
    assert_eq!(
        flywheel_probes, 10,
        "300s / 30s = 10 flywheel snapshot probes"
    );

    // Unfocused: zero probes emitted even after 60s.
    (m, _) = update::tick(&m, Msg::FocusChanged(false));
    let (_, unfocused_fx) = update::tick(&m, Msg::Tick { now_unix: 1400 });
    assert!(
        unfocused_fx.is_empty(),
        "unfocused terminal must pause probes"
    );

    // Refocused, then idle >= 300s: zero probes emitted until keypress.
    (m, _) = update::tick(&m, Msg::FocusChanged(true));
    let (idle_m, idle_fx) = update::tick(&m, Msg::Tick { now_unix: 1700 });
    assert!(idle_fx.is_empty(), "5m idle must pause probes");
    let (resumed_m, _) = update::tick(&idle_m, Msg::ToggleFollow);
    let (_, resume_fx) = update::tick(&resumed_m, Msg::Tick { now_unix: 1701 });
    assert!(
        resume_fx.contains(&Effect::ProbeToronHealth),
        "keypress after idle must resume probes"
    );
}

#[test]
fn diagnose_toron_rules_cover_wake_and_queued_sends() {
    let healthy = ToronHealthSignals {
        daemon_reachable: true,
        daemon_latency_ms: Some(12),
        wake_liveness_ok: true,
        wake_never_stamped: false,
        wake_publish_failures: 0,
        stuck_queued_sends: 0,
    };
    assert!(diagnose_toron(&healthy).is_empty());

    let deaf = ToronHealthSignals {
        wake_liveness_ok: false,
        ..healthy.clone()
    };
    let deaf_diags = diagnose_toron(&deaf);
    assert_eq!(deaf_diags.len(), 1);
    assert_eq!(deaf_diags[0].id, "WAKE_DEAF");
    assert_eq!(deaf_diags[0].severity, DiagnosticSeverity::P0);

    let never_stamped = ToronHealthSignals {
        wake_never_stamped: true,
        wake_liveness_ok: false,
        ..healthy.clone()
    };
    assert_eq!(diagnose_toron(&never_stamped)[0].id, "WAKE_NEVER_STAMPED");

    let pub_fail = ToronHealthSignals {
        wake_publish_failures: 2,
        ..healthy.clone()
    };
    assert_eq!(diagnose_toron(&pub_fail)[0].id, "WAKE_PUBLISH_FAILURES");

    let queued = ToronHealthSignals {
        stuck_queued_sends: 3,
        ..healthy
    };
    assert_eq!(diagnose_toron(&queued)[0].id, "STUCK_QUEUED_SEND");
}

#[test]
fn projection_json_matches_screen_and_never_contains_sealed_bodies() {
    let mut m = model();
    m.sealed_inbox = SealedInboxCounts {
        total: 4,
        unread: 2,
    };
    m.flywheel = Some(FlywheelPanel {
        project: "flywheel".into(),
        dispatcher: "up".into(),
        degraded: vec![],
        pin_count: 2,
        diagnostics: vec![Diagnostic {
            id: "REX_VERSION_SKEW".into(),
            severity: DiagnosticSeverity::P0,
            source: "flywheel".into(),
            summary: "skew".into(),
            evidence: "missing".into(),
            remedy_cmd: "rex version".into(),
        }],
    });
    let proj = m.projection();
    assert_eq!(proj.schema, BOARD_SCHEMA);
    assert_eq!(proj.sealed_inbox.total, 4);
    assert_eq!(proj.sealed_inbox.unread, 2);
    assert!(m.has_blocking_diagnostics());
    let json = serde_json::to_string(&proj).expect("serialize projection");
    assert!(!json.contains("body_md"));
}

#[test]
fn terminal_quit_state_emits_no_further_effects() {
    let (quit_m, fx) = update::tick(&model(), Msg::Quit);
    assert!(quit_m.should_quit);
    assert_eq!(fx, vec![Effect::Quit]);

    let (after, fx2) = update::tick(&quit_m, Msg::Refresh);
    assert!(after.should_quit);
    assert!(
        fx2.is_empty(),
        "terminal state must emit zero further effects"
    );
}

#[test]
fn layout_renders_wide_compact_phone_and_too_small_without_panic() {
    let mut m = model();
    m.now_unix = 1_700_000_120;
    m.roster = vec![
        RosterRow {
            name: "AmberFalcon".into(),
            npub: "npub1a".into(),
            program: Some("gemini".into()),
            status: "working".into(),
            online: true,
            age_secs: Some(30),
            lease_count: 1,
            ..Default::default()
        },
        RosterRow {
            name: "CobaltCoast".into(),
            npub: "npub1b".into(),
            retired: true,
            status: "done".into(),
            ..Default::default()
        },
    ];
    m.posts = vec![BoardPost {
        seq: 17,
        event_id: "ev17".into(),
        author: "AmberFalcon".into(),
        body: "Swarm board online\n@DustyHollow ready".into(),
        mentions: vec!["DustyHollow".into()],
        created_at: 1_700_000_060,
    }];
    m.leases = vec![LeaseRow {
        id: "l1".into(),
        holder: "AmberFalcon".into(),
        path: "src/tui/**".into(),
        exclusive: false,
        expires_at: 1_700_000_300,
    }];

    for (w, h, expect_mode) in [
        (120, 36, LayoutMode::Wide),
        (80, 24, LayoutMode::Compact),
        (45, 24, LayoutMode::Compact),
        (MIN_W - 1, MIN_H - 1, LayoutMode::TooSmall),
    ] {
        assert_eq!(layout_mode(w, h), expect_mode);
        let backend = TestBackend::new(w, h);
        let mut terminal = Terminal::new(backend).expect("test backend");
        terminal.draw(|f| view::render(f, &m)).expect("render");
    }

    assert_eq!(
        press(&m, KeyCode::Char('d')),
        Some(Msg::SelectInspector(Inspector::Diagnostics))
    );
    let (help_m, _) = update::tick(&m, Msg::ToggleHelp);
    assert_eq!(help_m.mode, Mode::Help);
}

#[tokio::test]
async fn interpreter_bounds_hang_and_surfaces_typed_failure() {
    let host = FakeHost::with_reply(Reply::Hang);
    let msg = bounded(&host, Effect::ProbeToronHealth, Duration::from_millis(20)).await;
    match msg {
        Msg::Failed { op, message } => {
            assert_eq!(op, Op::Health);
            assert!(message.contains("20ms"));
        }
        other => panic!("expected typed failure on timeout, got {other:?}"),
    }
}
