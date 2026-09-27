// governed-by: ADR-0031
//! Conformance tests for the Elm core (ADR-0031 §9, AGENTS.md effect law).
//!
//! Three properties, in order of what breaks most quietly:
//!   1. **Determinism** — one message log, the same model and the same effects.
//!   2. **The fold decides** — keys and payloads move the model, nothing else.
//!   3. **The interpreter bounds the world** — success, typed failure, hang and
//!      cancellation all end in a message, never in a stalled frame.

use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::host::fake::{FakeHost, Reply};
use super::model::{
    AgentRow, ChannelRow, Folder, LayoutMode, MailRow, Mode, Model, Pane, PostRow, SettingsState,
    Tab, layout_mode,
};
use super::msg::{Effect, Msg, Op};
use super::{bounded, keymap, update};
use toron_core::nostr::presence::PresenceState;

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

fn settings() -> SettingsState {
    SettingsState {
        project: "demo".into(),
        agent: Some("TestAgent".into()),
        npub: Some("npub1test".into()),
        ..Default::default()
    }
}

fn model() -> Model {
    Model::seed(Tab::Mail, Folder::Inbox, settings())
}

fn mail(id: &str, thread: &str, sender: &str, subject: &str, at: u64) -> MailRow {
    MailRow {
        id: id.into(),
        thread_id: thread.into(),
        sender: sender.into(),
        subject: subject.into(),
        importance: "normal".into(),
        ack_required: false,
        acked: false,
        read: true,
        state_known: true,
        at,
        body: None,
    }
}

/// A search hit: the store's projection carries no corr and no read stamp, so
/// the row says its state is unknown rather than inventing an unread dot.
fn search_hit(id: &str, thread: &str, sender: &str, subject: &str, at: u64) -> MailRow {
    MailRow {
        read: false,
        state_known: false,
        ..mail(id, thread, sender, subject, at)
    }
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn press(model: &Model, code: KeyCode) -> Option<Msg> {
    keymap::translate(model, key(code))
}

/// Fold one message into a model.
fn step(model: &Model, msg: Msg) -> (Model, Vec<Effect>) {
    update::tick(model, msg)
}

// ---------------------------------------------------------------------------
// 1. determinism
// ---------------------------------------------------------------------------

/// The same message log produces the same model and the same effect list,
/// byte for byte — the ADR-0031 §9 proof, asserted on the real fold.
#[test]
fn same_msg_log_same_model_and_effects() {
    let log = scripted_log();
    let (model_a, effects_a) = update::replay(&model(), &log);
    let (model_b, effects_b) = update::replay(&model(), &log);
    assert_eq!(
        format!("{model_a:?}"),
        format!("{model_b:?}"),
        "model diverged on replay"
    );
    assert_eq!(
        format!("{effects_a:?}"),
        format!("{effects_b:?}"),
        "effect list diverged on replay"
    );
    // And the log actually did something (a pinned test beats a weak one).
    assert!(!effects_a.is_empty(), "the scripted log must dispatch work");
    assert_eq!(model_a.tab, Tab::Agents, "the log's last tab switch landed");
}

/// A representative log: startup, a tick, a tab walk, a keypress in insert
/// mode, a payload, a failure — the shapes the runtime actually sends.
fn scripted_log() -> Vec<Msg> {
    vec![
        Msg::Init,
        Msg::Tick {
            now_unix: 1_700_000_100,
        },
        Msg::InboxLoaded(vec![
            mail("m1", "t1", "bee", "landed", 1_700_000_000),
            MailRow {
                ack_required: true,
                read: false,
                importance: "urgent".into(),
                ..mail("m2", "t1", "cee", "please ack", 1_700_000_050)
            },
        ]),
        Msg::Tick {
            now_unix: 1_700_000_200,
        },
        Msg::CycleTab(1),
        Msg::CycleTab(1),
        Msg::OpenInsert,
        Msg::InputChar('c'),
        Msg::InputChar('i'),
        Msg::CancelInsert,
        Msg::AgentsLoaded(vec![AgentRow {
            name: "bee".into(),
            npub: "npub1bee".into(),
            online: true,
            state: PresenceState::Working,
            age_secs: Some(3),
            ..Default::default()
        }]),
        Msg::Failed {
            op: Op::Channels,
            message: "store busy".into(),
        },
        Msg::Inflight(2),
        Msg::Tick {
            now_unix: 1_700_000_300,
        },
    ]
}

// ---------------------------------------------------------------------------
// 2. the fold decides
// ---------------------------------------------------------------------------

#[test]
fn startup_dispatches_presence_and_the_active_tab() {
    let (_, effects) = step(&model(), Msg::Init);
    assert_eq!(
        effects,
        vec![Effect::RefreshAgents, Effect::RefreshInbox],
        "startup must load the roster (presence) and the open tab"
    );
}

#[test]
fn tick_refreshes_the_active_tab_on_cadence() {
    let m = model();
    // In-flight work gates the tick: no second wave while one is out.
    let (busy, effects) = step(
        &Model {
            inflight: 1,
            ..m.clone()
        },
        Msg::Tick { now_unix: 10 },
    );
    assert!(effects.is_empty(), "a busy tick dispatches nothing");
    assert_eq!(busy.tick, 1, "the spinner still advances");

    let (_, effects) = step(&m, Msg::Tick { now_unix: 10 });
    assert_eq!(effects, vec![Effect::RefreshInbox]);

    // Channels ride the slower cadence: the first tick is quiet, the third is not.
    let channels = Model {
        tab: Tab::Channels,
        ..m.clone()
    };
    let (t1, e1) = step(&channels, Msg::Tick { now_unix: 10 });
    assert!(e1.is_empty(), "tick 1 is off-cadence for channels");
    let (t2, _) = step(&t1, Msg::Tick { now_unix: 11 });
    let (_, e3) = step(&t2, Msg::Tick { now_unix: 12 });
    assert!(
        e3.contains(&Effect::RefreshChannels),
        "tick 3 refreshes channels, got {e3:?}"
    );

    // Presence is the Agents tab's content: every tick.
    let agents = Model {
        tab: Tab::Agents,
        ..m
    };
    let (_, effects) = step(&agents, Msg::Tick { now_unix: 10 });
    assert_eq!(effects, vec![Effect::RefreshAgents]);
}

#[test]
fn opening_a_message_fetches_its_thread() {
    let loaded = Model {
        mail: super::model::MailState {
            messages: vec![
                mail("m1", "t1", "bee", "one", 10),
                mail("m2", "t2", "cee", "two", 20),
            ],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    // Rows are newest first: m2 then m1.
    assert_eq!(loaded.mail_rows()[0].key, "m2");
    let (opened, effects) = step(&loaded, Msg::OpenSelection);
    assert_eq!(
        effects,
        vec![Effect::RefreshThread {
            thread_id: "t2".into()
        }]
    );
    assert_eq!(opened.pane, Pane::Reader);

    // The reader owns j/k while it has focus: that scrolls the conversation
    // instead of moving the list cursor underneath it.
    let (scrolled, _) = step(&opened, Msg::MoveCursor(1));
    assert_eq!(scrolled.mail.selected, 0, "the list cursor did not move");
    assert_eq!(scrolled.mail.scroll, 1, "the reader scrolled");

    // h / Esc hands the keys back to the list, where a second row opens its
    // own thread.
    let (back, _) = step(&scrolled, Msg::CloseReader);
    assert_eq!(back.pane, Pane::List);
    let (down, _) = step(&back, Msg::MoveCursor(1));
    assert_eq!(down.mail.selected, 1);
    let (_, effects) = step(&down, Msg::OpenSelection);
    assert_eq!(
        effects,
        vec![Effect::RefreshThread {
            thread_id: "t1".into()
        }]
    );
}

#[test]
fn folder_switch_resets_cursor_reader_and_search() {
    let with_search = Model {
        mail: super::model::MailState {
            messages: vec![mail("m1", "t1", "bee", "one", 10)],
            selected: 1,
            search: Some(vec![mail("m9", "t9", "zed", "hit", 99)]),
            ..Default::default()
        },
        input: "hit".into(),
        ..model()
    };
    // The sidebar cursor *is* the folder: j/k walks it, and switching drops
    // the cursor, the open conversation and any search results.
    let sidebar = Model {
        pane: Pane::Sidebar,
        ..with_search
    };
    let (stepped, effects) = step(&sidebar, Msg::MoveCursor(1));
    assert_eq!(stepped.mail.folder, Folder::Unread);
    assert_eq!(stepped.mail.selected, 0);
    assert!(
        stepped.mail.search.is_none(),
        "search results drop on switch"
    );
    assert!(effects.is_empty(), "a folder with no rows fetches nothing");
    let (back, _) = step(&stepped, Msg::MoveCursor(-1));
    assert_eq!(back.mail.folder, Folder::Inbox);
}

#[test]
fn needs_ack_and_unread_folders_filter_the_index() {
    let m = Model {
        mail: super::model::MailState {
            messages: vec![
                MailRow {
                    ack_required: true,
                    ..mail("m1", "t1", "bee", "ack me", 10)
                },
                MailRow {
                    read: false,
                    ..mail("m2", "t2", "cee", "new", 20)
                },
                mail("m3", "t3", "dee", "quiet", 30),
            ],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let counts = m.folder_counts();
    let of = |f: Folder| {
        counts
            .iter()
            .find(|(x, _)| *x == f)
            .map(|(_, n)| *n)
            .unwrap()
    };
    assert_eq!(of(Folder::Inbox), 3);
    assert_eq!(of(Folder::Unread), 1);
    assert_eq!(of(Folder::NeedsAck), 1);
    assert_eq!(of(Folder::Threads), 3);

    let (ack, _) = step(
        &Model {
            pane: Pane::Sidebar,
            ..m.clone()
        },
        Msg::MoveCursor(3),
    );
    assert_eq!(ack.mail.folder, Folder::NeedsAck);
    assert_eq!(ack.mail_rows().len(), 1);
    assert_eq!(ack.mail_rows()[0].key, "m1");
}

#[test]
fn threads_group_by_thread_id_and_count_participants() {
    let m = Model {
        mail: super::model::MailState {
            messages: vec![
                mail("m1", "t1", "bee", "first", 10),
                mail("m2", "t1", "cee", "re: first", 30),
                mail("m3", "t2", "dee", "other", 20),
            ],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let (threads, _) = step(
        &Model {
            pane: Pane::Sidebar,
            ..m
        },
        Msg::MoveCursor(2),
    );
    assert_eq!(threads.mail.folder, Folder::Threads);
    let rows = threads.mail_rows();
    assert_eq!(rows.len(), 2, "two threads, not three messages");
    assert_eq!(rows[0].key, "t1", "newest thread first");
    assert_eq!(rows[0].meta, "2 msgs · +1");
    assert!(rows[0].thread);
    assert_eq!(rows[1].key, "t2");
}

#[test]
fn local_filter_narrows_rows_and_esc_clears_search() {
    let m = Model {
        mail: super::model::MailState {
            messages: vec![
                mail("m1", "t1", "bee", "alpha report", 10),
                mail("m2", "t2", "cee", "beta report", 20),
            ],
            loaded: true,
            search: Some(vec![mail("m3", "t3", "dee", "from search", 30)]),
            ..Default::default()
        },
        input: "alpha".into(),
        ..model()
    };
    // Search results replace the folder list, and the local box must NOT
    // re-filter them: the store matched fields the local predicate cannot see
    // (the body), so a second pass hides hits while the title still counts
    // them.
    assert_eq!(m.mail_rows().len(), 1, "hits survive the search text");
    assert_eq!(m.mail_rows()[0].key, "m3");
    let (cleared, _) = step(&m, Msg::CancelInsert);
    assert!(cleared.mail.search.is_none());
    assert!(cleared.input.is_empty());
    let (again, _) = step(&cleared, Msg::OpenInsert);
    assert_eq!(again.mode, Mode::Insert);

    // Filtering the folder list is local and pure.
    let filtered = Model {
        input: "beta".into(),
        ..cleared
    };
    let rows = filtered.mail_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, "m2");
}

#[test]
fn submit_input_searches_the_index_only_on_mail() {
    let typed = Model {
        mode: Mode::Insert,
        input: "  corr-a1  ".into(),
        ..model()
    };
    let (after, effects) = step(&typed, Msg::SubmitInput);
    assert_eq!(after.mode, Mode::Normal);
    assert_eq!(
        effects,
        vec![Effect::Search {
            query: "corr-a1".into()
        }],
        "the query is trimmed and sent once"
    );

    let channels = Model {
        tab: Tab::Channels,
        mode: Mode::Insert,
        input: "report".into(),
        ..model()
    };
    let (_, effects) = step(&channels, Msg::SubmitInput);
    assert!(effects.is_empty(), "channel filtering stays local");

    let blank = Model {
        mode: Mode::Insert,
        input: "   ".into(),
        ..model()
    };
    let (_, effects) = step(&blank, Msg::SubmitInput);
    assert!(effects.is_empty(), "an empty query is never sent");
}

#[test]
fn ack_and_read_target_the_selected_message() {
    let m = Model {
        mail: super::model::MailState {
            messages: vec![mail("m1", "t1", "bee", "one", 10)],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let (_, effects) = step(&m, Msg::AckSelected);
    assert_eq!(
        effects,
        vec![Effect::AckMessage {
            message_id: "m1".into()
        }]
    );
    let (_, effects) = step(&m, Msg::ReadSelected);
    assert_eq!(
        effects,
        vec![Effect::ReadMessage {
            message_id: "m1".into()
        }]
    );

    // A thread row acks its newest message.
    let threaded = Model {
        mail: super::model::MailState {
            messages: vec![
                mail("m1", "t1", "bee", "one", 10),
                mail("m2", "t1", "cee", "two", 20),
            ],
            loaded: true,
            folder: Folder::Threads,
            ..Default::default()
        },
        ..model()
    };
    let (_, effects) = step(&threaded, Msg::AckSelected);
    assert_eq!(
        effects,
        vec![Effect::AckMessage {
            message_id: "m2".into()
        }],
        "the newest message in the thread is the one acknowledged"
    );

    // Nothing selected is a status-line error, never a panic or a silent no-op.
    let empty = Model {
        mail: super::model::MailState::default(),
        ..model()
    };
    let (errored, effects) = step(&empty, Msg::AckSelected);
    assert!(effects.is_empty());
    assert_eq!(errored.status.kind, super::model::StatusKind::Error);
}

#[test]
fn tunables_move_and_clamp() {
    let (faster, _) = step(&model(), Msg::StepRefresh(-250));
    assert_eq!(faster.settings.refresh_ms, 750);
    let (floor, _) = step(&faster, Msg::StepRefresh(-10_000));
    assert_eq!(
        floor.settings.refresh_ms,
        SettingsState::MIN_REFRESH_MS,
        "the cadence is bounded below"
    );
    let (ceiling, _) = step(&floor, Msg::StepRefresh(999_999));
    assert_eq!(ceiling.settings.refresh_ms, SettingsState::MAX_REFRESH_MS);

    let (window, _) = step(&model(), Msg::StepWindow(1));
    assert_eq!(window.dashboard.window_hours, 48);
    let (wrapped, _) = step(&window, Msg::StepWindow(1));
    assert_eq!(wrapped.dashboard.window_hours, 6);

    let (dense, _) = step(&model(), Msg::ToggleDense);
    assert!(dense.settings.dense);
    let (clock, _) = step(&dense, Msg::ToggleRelativeTime);
    assert!(!clock.settings.relative_time);
}

#[test]
fn closed_channels_toggle_refetches() {
    let (shown, effects) = step(&model(), Msg::ToggleClosedChannels);
    assert!(shown.settings.show_closed_channels);
    assert_eq!(effects, vec![Effect::RefreshChannels]);
}

#[test]
fn failed_payloads_keep_the_last_good_data() {
    let loaded = Model {
        mail: super::model::MailState {
            messages: vec![mail("m1", "t1", "bee", "one", 10)],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let (after, effects) = step(
        &loaded,
        Msg::Failed {
            op: Op::Inbox,
            message: "index locked".into(),
        },
    );
    assert!(effects.is_empty());
    assert_eq!(after.mail.messages.len(), 1, "the last good page stays up");
    assert_eq!(after.status.kind, super::model::StatusKind::Error);
    assert!(after.status.text.contains("inbox: index locked"));
}

#[test]
fn stale_thread_payload_never_clobbers_the_reader() {
    let opened = Model {
        mail: super::model::MailState {
            open: Some(super::model::OpenThread {
                thread_id: "t-current".into(),
                subject: "current".into(),
                messages: Vec::new(),
            }),
            ..Default::default()
        },
        ..model()
    };
    let (after, _) = step(
        &opened,
        Msg::ThreadLoaded {
            thread_id: "t-stale".into(),
            rows: vec![mail("m9", "t-stale", "zed", "old", 1)],
        },
    );
    assert_eq!(
        after.mail.open.as_ref().map(|o| o.thread_id.as_str()),
        Some("t-current"),
        "a fetch from a different thread is dropped"
    );
}

#[test]
fn quit_sets_the_flag_and_the_effect() {
    let (quitting, effects) = step(&model(), Msg::Quit);
    assert!(quitting.should_quit);
    assert_eq!(effects, vec![Effect::Quit]);
}

#[test]
fn channels_post_payload_only_applies_to_the_selected_room() {
    let with_room = Model {
        tab: Tab::Channels,
        channels: super::model::ChannelsState {
            channels: vec![ChannelRow {
                id: "ch1".into(),
                name: "general".into(),
                ..Default::default()
            }],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let (applied, _) = step(
        &with_room,
        Msg::ChannelPostsLoaded {
            channel_id: "ch1".into(),
            rows: vec![PostRow {
                event_id: "e1".into(),
                author: "npub1bee".into(),
                content: "hello".into(),
                at: 5,
                kind: 9,
                ..Default::default()
            }],
        },
    );
    assert_eq!(applied.channels.posts.len(), 1);
    let (dropped, _) = step(
        &with_room,
        Msg::ChannelPostsLoaded {
            channel_id: "ch-other".into(),
            rows: Vec::new(),
        },
    );
    assert!(dropped.channels.posts.is_empty());
}

#[test]
fn model_derivations_feed_the_dashboard() {
    let m = Model {
        now_unix: 10_000,
        mail: super::model::MailState {
            messages: vec![
                mail("m1", "t1", "bee", "one", 10_000 - 60),
                mail("m2", "t1", "cee", "two", 10_000 - 2 * 3600),
            ],
            loaded: true,
            ..Default::default()
        },
        channels: super::model::ChannelsState {
            channels: vec![
                ChannelRow {
                    name: "loud".into(),
                    posts: 9,
                    ..Default::default()
                },
                ChannelRow {
                    name: "quiet".into(),
                    posts: 1,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        agents: super::model::AgentsState {
            rows: vec![
                AgentRow {
                    name: "bee".into(),
                    npub: "n1".into(),
                    online: true,
                    state: PresenceState::Working,
                    ..Default::default()
                },
                AgentRow {
                    name: "cee".into(),
                    npub: "n2".into(),
                    online: false,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        ..model()
    };
    assert_eq!(m.thread_count(), 1);
    assert_eq!(m.agents.online(), 1);
    assert_eq!(m.arrivals().len(), 24, "one bucket per hour of the window");
    assert_eq!(m.arrivals().iter().sum::<u64>(), 2);
    assert_eq!(m.channel_bars()[0], ("#loud".to_string(), 9));
    // Ties on count break by label, so the chart order is deterministic.
    assert_eq!(
        m.agent_state_counts(),
        vec![("offline".to_string(), 1), ("working".to_string(), 1)]
    );
    // The roster is the only npub→pin map the views need.
    assert_eq!(m.agent_name_for("n1"), Some("bee"));
    assert_eq!(m.agent_name_for("nope"), None);
}

// ---------------------------------------------------------------------------
// 3. responsive + keymap
// ---------------------------------------------------------------------------

#[test]
fn layout_breakpoints_are_pure() {
    // 40x12 is the floor: a phone-width PTY is single-pane, not "too small".
    assert_eq!(layout_mode(39, 24), LayoutMode::TooSmall);
    assert_eq!(layout_mode(40, 11), LayoutMode::TooSmall);
    assert_eq!(layout_mode(40, 12), LayoutMode::Stacked);
    assert_eq!(layout_mode(45, 25), LayoutMode::Stacked);
    assert_eq!(layout_mode(79, 40), LayoutMode::Stacked);
    assert_eq!(layout_mode(80, 24), LayoutMode::Dual);
    assert_eq!(layout_mode(107, 24), LayoutMode::Dual);
    assert_eq!(layout_mode(108, 24), LayoutMode::Tri);
    assert_eq!(layout_mode(200, 60), LayoutMode::Tri);
}

#[test]
fn pane_focus_is_clamped_never_wrapped() {
    let m = model();
    let (left, _) = step(
        &Model {
            pane: Pane::List,
            ..m.clone()
        },
        Msg::FocusPane(-1),
    );
    assert_eq!(left.pane, Pane::Sidebar);
    let (still, _) = step(&left, Msg::FocusPane(-1));
    assert_eq!(still.pane, Pane::Sidebar, "no wrap into the reader");
    let (right, _) = step(&still, Msg::FocusPane(1));
    assert_eq!(right.pane, Pane::List);
    let (reader, _) = step(&right, Msg::FocusPane(1));
    assert_eq!(reader.pane, Pane::Reader);
}

#[test]
fn keymap_normal_mode_table() {
    let m = model();
    assert_eq!(press(&m, KeyCode::Char('j')), Some(Msg::MoveCursor(1)));
    assert_eq!(press(&m, KeyCode::Char('k')), Some(Msg::MoveCursor(-1)));
    assert_eq!(press(&m, KeyCode::Char('g')), Some(Msg::GotoCursor(true)));
    assert_eq!(press(&m, KeyCode::Char('G')), Some(Msg::GotoCursor(false)));
    assert_eq!(
        press(&m, KeyCode::Char('3')),
        Some(Msg::SelectTab(Tab::Agents))
    );
    assert_eq!(press(&m, KeyCode::Tab), Some(Msg::CycleTab(1)));
    assert_eq!(press(&m, KeyCode::BackTab), Some(Msg::CycleTab(-1)));
    assert_eq!(press(&m, KeyCode::Char('/')), Some(Msg::OpenInsert));
    assert_eq!(press(&m, KeyCode::Char('?')), Some(Msg::ToggleHelp));
    assert_eq!(press(&m, KeyCode::Char('q')), Some(Msg::Quit));
    assert_eq!(press(&m, KeyCode::Char('r')), Some(Msg::Refresh));
    assert_eq!(press(&m, KeyCode::Char('a')), Some(Msg::AckSelected));
    assert_eq!(press(&m, KeyCode::Char('m')), Some(Msg::ReadSelected));
    assert_eq!(press(&m, KeyCode::Char('d')), Some(Msg::ToggleDense));
    assert_eq!(press(&m, KeyCode::Char('t')), Some(Msg::ToggleRelativeTime));
    assert_eq!(
        press(&m, KeyCode::Char('[')),
        Some(Msg::StepRefresh(-super::keymap::REFRESH_STEP_MS))
    );
    assert_eq!(press(&m, KeyCode::Enter), Some(Msg::OpenSelection));
    // 'c' (closed channels) is a Channels-tab key, not a global one.
    assert_eq!(press(&m, KeyCode::Char('c')), None);
    let channels = Model {
        tab: Tab::Channels,
        ..m.clone()
    };
    assert_eq!(
        press(&channels, KeyCode::Char('c')),
        Some(Msg::ToggleClosedChannels)
    );
    // l drills in from the list, h backs out of the reader.
    assert_eq!(press(&m, KeyCode::Char('l')), Some(Msg::OpenSelection));
    let reader = Model {
        pane: Pane::Reader,
        ..m.clone()
    };
    assert_eq!(press(&reader, KeyCode::Char('h')), Some(Msg::CloseReader));
    assert_eq!(press(&reader, KeyCode::Esc), Some(Msg::CloseReader));
    // Esc from the list quits (there is nothing above the list).
    assert_eq!(press(&m, KeyCode::Esc), Some(Msg::Quit));
}

#[test]
fn keymap_insert_and_help_modes_trap_focus() {
    let typing = Model {
        mode: Mode::Insert,
        ..model()
    };
    assert_eq!(
        press(&typing, KeyCode::Char('q')),
        Some(Msg::InputChar('q'))
    );
    assert_eq!(
        press(&typing, KeyCode::Char('1')),
        Some(Msg::InputChar('1'))
    );
    assert_eq!(
        press(&typing, KeyCode::Backspace),
        Some(Msg::InputBackspace)
    );
    assert_eq!(press(&typing, KeyCode::Enter), Some(Msg::SubmitInput));
    assert_eq!(press(&typing, KeyCode::Esc), Some(Msg::CancelInsert));
    // Ctrl-C is still the escape hatch, even mid-typing? No: typing owns the
    // keys, and the fold's CancelInsert is one Escape away.
    assert_eq!(press(&typing, KeyCode::Tab), None);

    let help = Model {
        mode: Mode::Help,
        ..model()
    };
    assert_eq!(
        press(&help, KeyCode::Char('j')),
        None,
        "help is a focus trap"
    );
    assert_eq!(press(&help, KeyCode::Char('q')), Some(Msg::ToggleHelp));
    assert_eq!(press(&help, KeyCode::Esc), Some(Msg::ToggleHelp));
    assert_eq!(press(&help, KeyCode::Enter), Some(Msg::ToggleHelp));
}

#[test]
fn keymap_quit_is_ctrl_c_anywhere_in_normal_mode() {
    let m = model();
    let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
    assert_eq!(keymap::translate(&m, ctrl_c), Some(Msg::Quit));
}

#[test]
fn help_overlay_opens_and_closes() {
    let (open, _) = step(&model(), Msg::ToggleHelp);
    assert_eq!(open.mode, Mode::Help);
    let (closed, _) = step(&open, Msg::ToggleHelp);
    assert_eq!(closed.mode, Mode::Normal);
}

#[test]
fn reader_scroll_is_clamped_at_the_top() {
    let reader = Model {
        pane: Pane::Reader,
        ..model()
    };
    let (down, _) = step(&reader, Msg::MoveCursor(5));
    assert_eq!(down.mail.scroll, 5);
    let (up, _) = step(&down, Msg::MoveCursor(-100));
    assert_eq!(up.mail.scroll, 0, "scrolling stops at the top");
    let (bottom, _) = step(&reader, Msg::GotoCursor(false));
    assert!(bottom.mail.scroll > 1_000, "G jumps far; the view clamps");
    let (top, _) = step(&bottom, Msg::GotoCursor(true));
    assert_eq!(top.mail.scroll, 0);
}

// ---------------------------------------------------------------------------
// 4. the interpreter bounds the world
// ---------------------------------------------------------------------------

/// Success: the fake records the attempt before it answers, and the answer is
/// the message the fold expects.
#[tokio::test]
async fn interpreter_success_records_the_attempt_before_answering() {
    let fake = FakeHost::new();
    let msg = bounded(&fake, Effect::RefreshInbox, Duration::from_millis(200)).await;
    assert!(matches!(msg, Msg::InboxLoaded(_)));
    assert_eq!(fake.seen(), vec![Effect::RefreshInbox]);
}

/// Typed failure: the host's error becomes `Msg::Failed` with the op label —
/// never a panic, never a silent drop.
#[tokio::test]
async fn interpreter_typed_failure_names_the_operation() {
    let fake = FakeHost::with_reply(Reply::Fail("index locked".into()));
    let msg = bounded(&fake, Effect::RefreshAgents, Duration::from_millis(200)).await;
    match msg {
        Msg::Failed { op, message } => {
            assert_eq!(op, Op::Agents);
            assert_eq!(message, "index locked");
        }
        other => panic!("expected a typed failure, got {other:?}"),
    }
}

/// Hang: a provider that never answers is cut off by the ceiling and reported
/// as a timeout for the right operation.
#[tokio::test]
async fn interpreter_hang_times_out_loudly() {
    let fake = FakeHost::with_reply(Reply::Hang);
    let started = std::time::Instant::now();
    let msg = bounded(&fake, Effect::RefreshChannels, Duration::from_millis(30)).await;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the ceiling fired instead of waiting for the store"
    );
    match msg {
        Msg::Failed { op, message } => {
            assert_eq!(op, Op::Channels);
            assert!(message.contains("no answer"), "got {message}");
        }
        other => panic!("expected a timeout failure, got {other:?}"),
    }
    assert_eq!(
        fake.seen(),
        vec![Effect::RefreshChannels],
        "a hung attempt is still journaled"
    );
}

/// Cancellation: leaving the screen aborts the in-flight effect, and the
/// aborted task produces no message the loop could act on.
#[tokio::test]
async fn interpreter_cancellation_leaves_no_message() {
    let fake = FakeHost::with_reply(Reply::Hang);
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let handle = tokio::spawn({
        let fake = fake.clone();
        async move {
            let msg = bounded(&fake, Effect::RefreshInbox, Duration::from_secs(60)).await;
            let _ = tx.send(msg).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.abort();
    let outcome = handle.await;
    assert!(outcome.is_err() && outcome.unwrap_err().is_cancelled());
    assert!(
        rx.try_recv().is_err(),
        "an aborted effect must not post a result"
    );
}

// ---------------------------------------------------------------------------
// 5. the reader vs. the mailbox's thread rule
// ---------------------------------------------------------------------------

/// The reader narrows the body-carrying page to one thread — and a top-level
/// message (no thread tag) opens as its own thread instead of an empty pane.
/// This is the case an exact `thread_id` query cannot serve: the mailbox
/// filter compares the row's thread field, and an untagged row has none.
#[test]
fn reader_opens_untagged_and_tagged_threads() {
    let untagged = MailRow {
        thread_id: String::new(),
        ..mail("m1", "m1", "bee", "solo", 10)
    };
    let tagged_a = mail("m2", "t1", "cee", "first", 20);
    let tagged_b = mail("m3", "t1", "dee", "reply", 30);

    let page = Model {
        mail: super::model::MailState {
            messages: vec![untagged.clone(), tagged_a.clone(), tagged_b.clone()],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };

    // Untagged: the page's row matches by its own id, and the reader opens it.
    let (solo, _) = step(
        &page,
        Msg::ThreadLoaded {
            thread_id: "m1".into(),
            rows: vec![untagged.clone(), tagged_a.clone(), tagged_b.clone()],
        },
    );
    let open = solo.mail.open.expect("the untagged message opens");
    assert_eq!(open.messages.len(), 1);
    assert_eq!(open.messages[0].id, "m1");
    assert_eq!(open.subject, "solo");

    // Tagged: only the thread's own messages, oldest first.
    let (thread, _) = step(
        &page,
        Msg::ThreadLoaded {
            thread_id: "t1".into(),
            rows: vec![untagged, tagged_b, tagged_a],
        },
    );
    let open = thread.mail.open.expect("the tagged thread opens");
    assert_eq!(open.messages.len(), 2, "the untagged row is not in it");
    assert_eq!(open.messages[0].id, "m2", "oldest first");
    assert_eq!(open.messages[1].id, "m3");
    assert_eq!(open.subject, "reply", "the newest subject wins the title");

    // A page that no longer holds the thread falls back to what is already on
    // screen instead of opening an empty reader.
    let (fallback, _) = step(
        &page,
        Msg::ThreadLoaded {
            thread_id: "t1".into(),
            rows: vec![mail("m9", "t9", "zed", "other", 99)],
        },
    );
    let open = fallback.mail.open.expect("the fallback still opens");
    assert_eq!(
        open.messages.len(),
        2,
        "the on-screen rows filled the reader"
    );
    assert_eq!(open.messages[0].id, "m2");
}

/// A slow-but-finite provider still answers (the ceiling is a bound, not a
/// deadline that turns slow into broken).
#[tokio::test]
async fn interpreter_slow_provider_still_lands() {
    let fake = FakeHost::with_reply(Reply::After(Duration::from_millis(10)));
    let msg = bounded(&fake, Effect::RefreshAgents, Duration::from_millis(500)).await;
    assert!(matches!(msg, Msg::AgentsLoaded(_)));
}

// ---------------------------------------------------------------------------
// 6. the responsive ladder, rendered (not just selected)
// ---------------------------------------------------------------------------

/// Every mode by name. The match is exhaustive on purpose: a variant nothing
/// can produce is dead code, and this is where that used to hide.
fn mode_name(mode: LayoutMode) -> &'static str {
    match mode {
        LayoutMode::TooSmall => "too-small",
        LayoutMode::Stacked => "single",
        LayoutMode::Dual => "dual",
        LayoutMode::Tri => "tri",
    }
}

/// Every declared mode is reachable by some frame size.
#[test]
fn the_ladder_reaches_every_mode_it_declares() {
    let mut seen = std::collections::BTreeSet::new();
    for width in 20..=240u16 {
        for height in 6..=40u16 {
            seen.insert(mode_name(layout_mode(width, height)));
        }
    }
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        vec!["dual", "single", "too-small", "tri"],
        "every declared mode is reachable (an uninhabited one is dead code)"
    );
}

/// `split` tiles the frame edge to edge — no gap, no overlap — and the sidebar
/// exists exactly when the tab asked for it.
#[test]
fn split_tiles_the_frame_and_honours_the_sidebar_flag() {
    let area = ratatui::layout::Rect::new(0, 0, 80, 24);

    let dual = super::view::split(area, LayoutMode::Dual, true);
    let sidebar = dual
        .sidebar
        .expect("dual keeps the folder axis: a phone-width fix, not a resize");
    assert_eq!(sidebar.x, area.x);
    assert_eq!(sidebar.right(), dual.list.x, "no gap after the sidebar");
    assert_eq!(dual.list.right(), dual.reader.x);
    assert_eq!(dual.reader.right(), area.right());
    assert!(dual.list.width >= 20 && dual.reader.width >= 20);

    let tri = super::view::split(area, LayoutMode::Tri, true);
    let sidebar = tri.sidebar.expect("tri keeps the sidebar");
    assert_eq!(sidebar.right(), tri.list.x);
    assert_eq!(tri.list.right(), tri.reader.x);
    assert_eq!(tri.reader.right(), area.right());

    // A tab with no sidebar of its own (Agents) gets the full width back.
    let wide = super::view::split(area, LayoutMode::Tri, false);
    assert!(wide.sidebar.is_none());
    assert_eq!(wide.list.x, area.x);
    assert_eq!(wide.reader.right(), area.right());

    // One pane at a time: the list and the reader are the same rect.
    let single = super::view::split(area, LayoutMode::Stacked, true);
    assert!(single.sidebar.is_none());
    assert_eq!(single.list, area);
    assert_eq!(single.reader, area);
}

/// Render a frame to text with the test backend: no terminal, no TTY, and the
/// only witness the responsive claims ever had.
fn screen(model: &Model, width: u16, height: u16) -> String {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = ratatui::Terminal::new(backend).expect("test backend");
    terminal
        .draw(|f| super::view::render(f, model))
        .expect("draw");
    let buffer = terminal.backend().buffer().clone();
    let mut out = String::new();
    for y in 0..buffer.area().height {
        for x in 0..buffer.area().width {
            match buffer.cell((x, y)) {
                Some(cell) => out.push_str(cell.symbol()),
                None => out.push(' '),
            }
        }
        out.push('\n');
    }
    out
}

/// A phone-shaped frame (45x24, what an iPhone SSH client reports) draws ONE
/// pane — the focused one — and `h` still reaches the others. Dual keeps the
/// folder axis beside the list; below the floor it says so.
#[test]
fn phone_width_draws_one_pane_and_dual_keeps_the_folder_axis() {
    let m = Model {
        mail: super::model::MailState {
            messages: vec![mail("m1", "t1", "bee", "alpha report", 10)],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };

    let phone = screen(&m, 45, 24);
    assert!(phone.contains("Inbox"), "the list pane is drawn:\n{phone}");
    assert!(
        !phone.contains("folders"),
        "the panes behind it are not:\n{phone}"
    );
    let folders = screen(
        &Model {
            pane: Pane::Sidebar,
            ..m.clone()
        },
        45,
        24,
    );
    assert!(folders.contains("folders"), "h stays reachable:\n{folders}");

    let dual = screen(&m, 90, 30);
    assert!(dual.contains("folders"), "dual keeps the folders:\n{dual}");
    assert!(dual.contains("reader"), "and the reader column:\n{dual}");

    let tiny = screen(&m, 36, 24);
    assert!(tiny.contains("too small"), "the gate still speaks:\n{tiny}");
}

/// At 45 cols the row is one line and the subject survives (a 10-col sender
/// pad plus a stamp would leave the subject nothing).
#[test]
fn phone_rows_keep_the_subject_readable() {
    let m = Model {
        mail: super::model::MailState {
            messages: vec![mail(
                "m1",
                "t1",
                "bee",
                "a subject long enough to be cut off",
                10,
            )],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let phone = screen(&m, 45, 24);
    let row = phone
        .lines()
        .find(|l| l.contains("bee"))
        .expect("the message row is on screen");
    assert!(row.contains("a subject"), "the subject survives: {row:?}");
}

/// The sidebar cursor is the folder's index, clamped against the FOLDER count
/// — not the message count, which highlighted the wrong row whenever the index
/// held fewer messages than there are folders.
#[test]
fn folder_highlight_tracks_the_folder_not_the_message_count() {
    let m = Model {
        mail: super::model::MailState {
            folder: Folder::Important,
            messages: vec![
                mail("m1", "t1", "bee", "one", 10),
                mail("m2", "t2", "cee", "two", 20),
            ],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    assert_eq!(
        super::view::mail::folder_state(&m).selected(),
        Some(Folder::ALL.len() - 1),
        "Important is the last folder whatever the index holds"
    );

    // An empty index still highlights the folder the operator chose.
    let empty = Model {
        mail: super::model::MailState {
            folder: Folder::Unread,
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    assert_eq!(super::view::mail::folder_state(&empty).selected(), Some(1));
}

/// A search hit carries no receipt and no read stamp, so it must not claim
/// either — the unread dot and the ack badge would both be invented.
#[test]
fn unknown_state_rows_do_not_claim_unread_or_acked() {
    let m = Model {
        mail: super::model::MailState {
            search: Some(vec![MailRow {
                ack_required: true,
                ..search_hit("m9", "t9", "zed", "hit", 99)
            }]),
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let rows = m.mail_rows();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].unread, "a hit is not claimed unread");
    assert!(
        !rows[0].ack_pending,
        "nor ack-owing: the hit has no receipt"
    );
}

/// Typing a query and pressing Enter: the store's hits replace the list, and
/// the local box must not narrow them again.
#[test]
fn server_hits_survive_a_local_pass_that_cannot_see_the_body() {
    let typed = Model {
        mode: Mode::Insert,
        input: "deadline".into(),
        ..model()
    };
    let (asking, effects) = step(&typed, Msg::SubmitInput);
    assert_eq!(
        effects,
        vec![Effect::Search {
            query: "deadline".into()
        }]
    );
    let (hits, _) = step(
        &asking,
        Msg::SearchLoaded(vec![search_hit("m5", "t5", "dee", "re: plan", 30)]),
    );
    assert_eq!(
        hits.mail_rows().len(),
        1,
        "the hit matched in the body the local pass never sees"
    );
}

/// Enter on a tab with no reader must not install a pane that tab cannot show,
/// or Esc/h would quietly mean something else for the rest of the session.
#[test]
fn open_selection_on_a_readerless_tab_keeps_focus_and_says_so() {
    let dashboard = Model {
        tab: Tab::Dashboard,
        ..model()
    };
    let (after, effects) = step(&dashboard, Msg::OpenSelection);
    assert_eq!(after.pane, Pane::List, "no illegal Reader focus");
    assert!(effects.is_empty(), "nothing to fetch");
    assert!(
        after.status.text.contains("nothing to open"),
        "got {:?}",
        after.status.text
    );
}

/// Acking from the reader clears the badge there too: the list agrees on the
/// next inbox fetch, the reader would never.
#[test]
fn ack_from_the_reader_clears_the_badge() {
    let open = MailRow {
        ack_required: true,
        acked: false,
        read: false,
        ..mail("m1", "t1", "bee", "ack me", 10)
    };
    let m = Model {
        pane: Pane::Reader,
        mail: super::model::MailState {
            messages: vec![open.clone()],
            loaded: true,
            open: Some(super::model::OpenThread {
                thread_id: "t1".into(),
                subject: "ack me".into(),
                messages: vec![open],
            }),
            ..Default::default()
        },
        ..model()
    };
    assert!(
        m.mail.open.as_ref().expect("open").messages[0].ack_pending(),
        "the fixture starts owing an ack"
    );
    let (after, _) = step(
        &m,
        Msg::ReceiptWritten {
            corr: "m1".into(),
            verb: "ack",
        },
    );
    assert!(
        !after.mail.open.as_ref().expect("open").messages[0].ack_pending(),
        "the reader agrees with the list"
    );
    assert!(!after.mail.messages[0].ack_pending());
}

/// The Agents tab has no sidebar, so it must not offer one as a focus target.
#[test]
fn agents_tab_has_no_sidebar_pane() {
    assert_eq!(Pane::order(Tab::Agents), &[Pane::List, Pane::Reader]);
    let m = Model {
        tab: Tab::Agents,
        ..model()
    };
    let (left, _) = step(&m, Msg::FocusPane(-1));
    assert_eq!(left.pane, Pane::List, "h never lands on an invisible pane");
    let (right, _) = step(&left, Msg::FocusPane(1));
    assert_eq!(right.pane, Pane::Reader);
}

/// The filter matches the task an agent is running, not just its name (the doc
/// said so long before the code did).
#[test]
fn agent_filter_matches_the_task_too() {
    let m = Model {
        tab: Tab::Agents,
        input: "review".into(),
        agents: super::model::AgentsState {
            rows: vec![
                AgentRow {
                    name: "aye".into(),
                    task: Some("review the patch".into()),
                    ..Default::default()
                },
                AgentRow {
                    name: "bee".into(),
                    task: Some("write the docs".into()),
                    ..Default::default()
                },
            ],
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let rows = m.agents.visible_rows(&m.input);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "aye");
}

/// The agent detail card scrolls, and a different agent opens at the top of
/// ITS card (a long task line used to just clip with j/k doing nothing).
#[test]
fn agent_detail_scrolls_and_resets_on_a_new_agent() {
    let three = |selected: usize, scroll: usize| super::model::AgentsState {
        rows: vec![
            AgentRow {
                name: "aye".into(),
                ..Default::default()
            },
            AgentRow {
                name: "bee".into(),
                ..Default::default()
            },
            AgentRow {
                name: "cee".into(),
                ..Default::default()
            },
        ],
        selected,
        scroll,
        loaded: true,
    };
    let card = |selected: usize, scroll: usize, pane: Pane| Model {
        tab: Tab::Agents,
        pane,
        agents: three(selected, scroll),
        ..model()
    };

    let (scrolled, _) = step(&card(0, 0, Pane::Reader), Msg::MoveCursor(3));
    assert_eq!(scrolled.agents.scroll, 3, "j scrolls the card");
    let (top, _) = step(&scrolled, Msg::MoveCursor(-10));
    assert_eq!(top.agents.scroll, 0, "and stops at the top");

    // Scrolled card + a different agent selected = a fresh card.
    let (moved, _) = step(
        &Model {
            pane: Pane::List,
            ..scrolled.clone()
        },
        Msg::MoveCursor(1),
    );
    assert_eq!(moved.agents.selected, 1, "the list cursor moved");
    assert_eq!(
        moved.agents.scroll, 0,
        "a new agent opens at the top of its card"
    );
}

/// The dashboard refreshes everything it draws. Otherwise its chart and cards
/// are a snapshot from the moment the operator arrived while the header claims
/// to be live.
#[test]
fn dashboard_tick_refreshes_what_it_draws() {
    let mut m = Model {
        tab: Tab::Dashboard,
        ..model()
    };
    let mut seen: Vec<Effect> = Vec::new();
    for i in 1..=super::update::SLOW_CADENCE {
        let (next, effects) = step(
            &m,
            Msg::Tick {
                now_unix: 1_000 + i,
            },
        );
        m = next;
        seen.extend(effects);
    }
    for want in [
        Effect::RefreshReceipts,
        Effect::RefreshHealth,
        Effect::RefreshAgents,
        Effect::RefreshChannels,
        Effect::RefreshInbox,
    ] {
        assert!(
            seen.contains(&want),
            "the dashboard tick must refresh {want:?}, saw {seen:?}"
        );
    }
}

/// A task that panics inside the host never answers, so a mount count that is
/// only ever incremented would sit above zero forever and refuse every later
/// refresh — a frozen console with no typed failure. The count is derived from
/// live handles instead.
#[tokio::test]
async fn a_panicked_effect_leaves_the_refresh_gate_open() {
    let mut handles: Vec<tokio::task::JoinHandle<()>> = vec![
        tokio::spawn(async { panic!("host exploded") }),
        tokio::spawn(async { std::future::pending::<()>().await }),
    ];
    while !handles[0].is_finished() {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        super::live_handles(&mut handles),
        1,
        "only the hung task is still owed an answer"
    );
    // Cancellation is cooperative, so the handle only reports finished once
    // the runtime has polled it — which is exactly what the derived count
    // waits for (an accumulator would have to guess).
    handles[0].abort();
    while !handles[0].is_finished() {
        tokio::task::yield_now().await;
    }
    assert_eq!(super::live_handles(&mut handles), 0);
}
