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

use super::compose::{ComposeState, Importance};
use super::host::composed_send;
use super::host::fake::{FakeHost, Reply};
use super::model::{
    AgentRow, ChannelRow, Folder, LayoutMode, MailRow, Mode, Model, Pane, PostRow, SettingsState,
    StatusKind, Tab, layout_mode,
};
use super::msg::{Effect, Msg, Op};
use super::{bounded, compose, keymap, update};
use toron_core::nostr::presence::PresenceState;

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
    assert_eq!(loaded.mail_rows()[0].key, "m2");
    let (opened, effects) = step(&loaded, Msg::OpenSelection);
    assert_eq!(
        effects,
        vec![Effect::RefreshThread {
            thread_id: "t2".into()
        }]
    );
    assert_eq!(opened.pane, Pane::Reader);

    let (scrolled, _) = step(&opened, Msg::MoveCursor(1));
    assert_eq!(scrolled.mail.selected, 0, "the list cursor did not move");
    assert_eq!(scrolled.mail.scroll, 1, "the reader scrolled");

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
            ..m
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
    assert_eq!(m.mail_rows().len(), 1, "hits survive the search text");
    assert_eq!(m.mail_rows()[0].key, "m3");
    let (cleared, _) = step(&m, Msg::CancelInsert);
    assert!(cleared.mail.search.is_none());
    assert!(cleared.input.is_empty());
    let (again, _) = step(&cleared, Msg::OpenInsert);
    assert_eq!(again.mode, Mode::Insert);

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
    assert_eq!(
        m.agent_state_counts(),
        vec![("offline".to_string(), 1), ("working".to_string(), 1)]
    );
    assert_eq!(m.agent_name_for("n1"), Some("bee"));
    assert_eq!(m.agent_name_for("nope"), None);
}

#[test]
fn layout_breakpoints_are_pure() {
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
            ..m
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
    assert_eq!(press(&m, KeyCode::Char('c')), Some(Msg::OpenCompose));
    assert_eq!(press(&m, KeyCode::Char('C')), None);
    let channels = Model {
        tab: Tab::Channels,
        ..m.clone()
    };
    assert_eq!(press(&channels, KeyCode::Char('c')), Some(Msg::OpenCompose));
    assert_eq!(
        press(&channels, KeyCode::Char('C')),
        Some(Msg::ToggleClosedChannels)
    );
    assert_eq!(press(&m, KeyCode::Char('l')), Some(Msg::OpenSelection));
    let reader = Model {
        pane: Pane::Reader,
        ..m.clone()
    };
    assert_eq!(press(&reader, KeyCode::Char('h')), Some(Msg::CloseReader));
    assert_eq!(press(&reader, KeyCode::Esc), Some(Msg::CloseReader));
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

/// A fetch that FAILED must stop saying "loading": the pane has to name the
/// error and the command that fixes it, or a fresh console with an
/// unregistered pin waits forever on a load that already happened.
#[test]
fn a_failed_fetch_stops_saying_loading_and_names_the_fix() {
    let (failed, _) = step(
        &model(),
        Msg::Failed {
            op: Op::Inbox,
            message: "agent PtyAgent has no local key in this project".into(),
        },
    );
    assert!(
        failed.mail.loaded,
        "the fetch settled, so it is not loading"
    );
    assert_eq!(
        failed.mail.error.as_deref(),
        Some("agent PtyAgent has no local key in this project")
    );

    let (recovered, _) = step(
        &failed,
        Msg::InboxLoaded(vec![mail("m1", "t1", "bee", "hi", 10)]),
    );
    assert_eq!(recovered.mail.error, None);
    assert!(recovered.mail.loaded);

    assert!(!failed.channels.loaded, "another pane's fetch is still out");
}

/// The failure reaches the screen as the error plus the identity command.
#[test]
fn a_failed_inbox_renders_the_error_and_the_command() {
    let (failed, _) = step(
        &model(),
        Msg::Failed {
            op: Op::Inbox,
            message: "agent PtyAgent has no local key in this project".into(),
        },
    );
    let drawn = screen(&failed, 120, 30);
    assert!(
        drawn.contains("no local key"),
        "the error is on screen:\n{drawn}"
    );
    assert!(
        drawn.contains("toron agent bootstrap"),
        "and so is the command that fixes it:\n{drawn}"
    );
    assert!(
        !drawn.contains("loading the index"),
        "a settled failure is not a spinner:\n{drawn}"
    );
}

/// An empty pane names the exact commands, with THIS project and pin already
/// in them — and an unknown pin stays a placeholder, because the console never
/// invents a name.
#[test]
fn empty_panes_name_the_exact_commands() {
    let on = model().onboarding();
    assert!(
        on.mail.starts_with("toron mail send ")
            && on.mail.contains("--project demo --as TestAgent"),
        "mail hint must be a runnable send: {}",
        on.mail
    );
    assert!(
        on.channel.starts_with("toron channel create ")
            && on.channel.contains("--project demo --as TestAgent"),
        "channel hint must be a runnable create: {}",
        on.channel
    );
    assert!(
        on.agent.starts_with("toron agent bootstrap ")
            && on.agent.contains("--project demo --as TestAgent"),
        "roster hint must be a runnable bootstrap: {}",
        on.agent
    );

    let anonymous = SettingsState {
        agent: None,
        ..settings()
    };
    let on = Model {
        settings: anonymous,
        ..model()
    }
    .onboarding();
    assert!(
        on.channel.contains("--as <pin>"),
        "no pin means a placeholder, not a guess: {}",
        on.channel
    );
}

/// Every hint is a REAL invocation: parsed by the same clap surface the binary
/// uses, so a renamed flag or a new required argument fails here instead of
/// rotting into a dead command in a pane nobody exercises.
#[test]
fn onboarding_commands_parse_as_real_cli_invocations() {
    use clap::Parser;
    let on = model().onboarding();
    for command in [on.mail, on.channel, on.agent] {
        let filled = command
            .replace("<pin>", "OtherAgent")
            .replace("<subject>", "hello")
            .replace("<body>", "body")
            .replace("<id>", "general")
            .replace("<name>", "general");
        let parts: Vec<&str> = filled.split_whitespace().collect();
        assert_eq!(parts.first(), Some(&"toron"), "got {filled}");
        crate::cli::Cli::try_parse_from(&parts).unwrap_or_else(|e| {
            panic!("`{filled}` is not a real CLI invocation: {e}");
        });
    }
}

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

    let wide = super::view::split(area, LayoutMode::Tri, false);
    assert!(wide.sidebar.is_none());
    assert_eq!(wide.list.x, area.x);
    assert_eq!(wide.reader.right(), area.right());

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

/// The empty Mail pane actually draws the commands: the strings are only worth
/// having if they reach the screen.
#[test]
fn empty_mail_pane_renders_the_commands() {
    let empty = Model {
        mail: super::model::MailState {
            loaded: true,
            ..Default::default()
        },
        ..model()
    };
    let drawn = screen(&empty, 120, 30);
    assert!(
        drawn.contains("nothing in this folder yet"),
        "the pane explains itself:\n{drawn}"
    );
    assert!(
        drawn.contains("toron mail send"),
        "the send command is on screen:\n{drawn}"
    );
    assert!(
        drawn.contains("toron channel create"),
        "and so is the room command:\n{drawn}"
    );
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

    let (moved, _) = step(
        &Model {
            pane: Pane::List,
            ..scrolled
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
    handles[0].abort();
    while !handles[0].is_finished() {
        tokio::task::yield_now().await;
    }
    assert_eq!(super::live_handles(&mut handles), 0);
}

/// Wrapping and caret placement come from ONE function, so the property that
/// matters is structural: for every caret offset the caret lands inside the
/// box, and every drawn line fits it. A second wrap (letting the renderer
/// re-wrap a line the core already wrapped) is what puts the caret on a row
/// the text is not on, and this is where that would show.
#[test]
fn every_caret_offset_lands_inside_the_wrapped_box() {
    let samples = [
        "",
        "one line",
        "a very long line that will certainly need to wrap several times over",
        "para one\n\npara three",
        "trailing newline\n",
        "a\nbb\nccc\ndddd\neeeee",
        "supercalifragilisticexpialidocious_and_then_some_more_to_force_a_hard_break",
        "  leading spaces stay put  ",
        "unicode ✓ works, é accents, and 日本語 も",
    ];
    for text in samples {
        for width in 1..=40usize {
            let lines = compose::wrap(text, width);
            assert!(
                !lines.is_empty(),
                "wrap({text:?}, {width}) must produce at least one line"
            );
            for line in &lines {
                assert!(
                    line.text.chars().count() <= width,
                    "wrap({text:?}, {width}) drew a {}-col line: {:?}",
                    line.text.chars().count(),
                    line.text
                );
            }
            for pair in lines.windows(2) {
                let newline = usize::from(text.as_bytes().get(pair[0].end) == Some(&b'\n'));
                assert_eq!(
                    pair[1].start,
                    pair[0].end + newline,
                    "wrap({text:?}, {width}) left a gap at {:?}",
                    pair[0].text
                );
            }
            for at in char_boundaries(text) {
                let (row, col) = compose::caret_pos(text, width, at);
                assert!(
                    row < lines.len(),
                    "caret {at} in {text:?} at {width} cols landed on row {row} of {}",
                    lines.len()
                );
                assert!(
                    col <= width,
                    "caret {at} in {text:?} at {width} cols landed at col {col}"
                );
            }
        }
    }
}

fn char_boundaries(text: &str) -> Vec<usize> {
    let mut out: Vec<usize> = text.char_indices().map(|(i, _)| i).collect();
    out.push(text.len());
    out
}

/// A caret on a line boundary belongs to the START of the next line, the way a
/// terminal puts it, and a caret at the end of the text stays on the last line
/// instead of inventing a row.
#[test]
fn caret_row_follows_the_newline_it_sits_on() {
    let text = "ab\ncd";
    assert_eq!(compose::caret_pos(text, 20, 0), (0, 0));
    assert_eq!(
        compose::caret_pos(text, 20, 2),
        (0, 2),
        "before the newline"
    );
    assert_eq!(
        compose::caret_pos(text, 20, 3),
        (1, 0),
        "the newline is consumed"
    );
    assert_eq!(compose::caret_pos(text, 20, 5), (1, 2), "end of the text");

    let wrapped = "aaaa bbbb";
    assert_eq!(compose::caret_pos(wrapped, 5, 5), (1, 0));
    assert_eq!(compose::caret_pos(wrapped, 5, 9), (1, 4));

    assert_eq!(compose::wrap("anything at all", 0).len(), 1);
    assert_eq!(compose::caret_pos("anything", 0, 4), (0, 4));
}

/// The form walks its stops, cycles the toggles, and keeps the caret inside the
/// text of whichever stop is focused.
#[test]
fn compose_stops_cycle_and_the_caret_follows_the_focus() {
    use crate::tui::compose::{CaretMove, ComposeState, Field};

    let mut c = ComposeState::new();
    assert_eq!(
        c.field,
        Field::To,
        "a blank composer starts at the recipient"
    );
    assert_eq!(c.wake.as_str(), "tasked", "a peer message defaults to work");

    c.to = "DustySparrow".into();
    c.set_field(Field::Body);
    assert_eq!(c.caret, 0, "an empty body parks the caret at 0");
    c.body = "hello".into();
    c.set_field(Field::Body);
    assert_eq!(c.caret, 5, "focus parks the caret at the end of the text");

    c.set_field(Field::To);
    let mut seen = Vec::new();
    for _ in 0..Field::ALL.len() {
        seen.push(c.field);
        c.step_field(1);
    }
    assert_eq!(seen, Field::ALL.to_vec());
    assert_eq!(c.field, Field::To, "the walk wrapped");

    c.set_field(Field::Wake);
    c.cycle(1);
    assert_eq!(c.wake.as_str(), "standing");
    c.set_field(Field::Importance);
    c.cycle(1);
    assert_eq!(c.importance.as_str(), "high");
    c.cycle(1);
    assert_eq!(c.importance.as_str(), "urgent");
    c.cycle(1);
    assert_eq!(c.importance.as_str(), "normal", "the cycle wraps");
    c.set_field(Field::Ack);
    c.cycle(1);
    assert!(c.ack);
    c.set_field(Field::To);
    let before = c.to.clone();
    c.cycle(1);
    assert_eq!(c.to, before);

    c.set_field(Field::Body);
    c.body = "one\ntwenty\nxy".into();
    c.set_field(Field::Body);
    assert_eq!(c.caret, 13, "the end of the text");
    c.move_caret(CaretMove::Up);
    assert_eq!(c.caret, 6, "column 2 of `twenty`");
    assert_eq!(&c.body[c.caret..], "enty\nxy");
    c.move_caret(CaretMove::Up);
    assert_eq!(c.caret, 2, "column 2 of `one`");
    c.move_caret(CaretMove::Up);
    assert_eq!(c.caret, 0, "up from the first line goes to the top");
    c.move_caret(CaretMove::Down);
    assert_eq!(c.caret, 4, "down returns to the same column");
    c.caret = 9;
    c.move_caret(CaretMove::Up);
    assert_eq!(c.caret, 3, "column 5 clamped to the 3-char line");
    c.caret = 6;
    c.move_caret(CaretMove::Home);
    assert_eq!(c.caret, 4);
    c.move_caret(CaretMove::End);
    assert_eq!(
        c.caret, 10,
        "the newline after `twenty`, not the end of the text"
    );
}

/// A reply is pre-addressed and in-thread, and a send refuses an incomplete
/// form with a reason rather than wrapping a message with an empty subject.
#[test]
fn compose_reply_prefills_and_an_incomplete_form_refuses() {
    use crate::tui::compose::{ComposeState, Field};

    let reply = ComposeState::reply("t-9".into(), "DustySparrow".into(), "status".into());
    assert_eq!(reply.to, "DustySparrow");
    assert_eq!(
        reply.subject, "status",
        "the subject is copied, never rewritten"
    );
    assert_eq!(reply.thread.as_deref(), Some("t-9"));
    assert_eq!(reply.field, Field::Body, "a reply starts in the body");

    let mut c = ComposeState::new();
    assert!(c.validate().is_err(), "an empty form cannot be sent");
    c.to = "captain".into();
    c.subject = " ".into();
    assert!(
        c.validate().unwrap_err().contains("subject"),
        "the reason has to name what is missing"
    );
    c.subject = "blocked".into();
    c.body = "need a call".into();
    assert!(c.validate().is_ok(), "aliases are legal recipients");
}

/// Editing is char-safe: backspace and delete never split a multi-byte char,
/// and a caret handed a non-boundary offset is snapped instead of panicking.
#[test]
fn compose_editing_never_splits_a_character() {
    use crate::tui::compose::{CaretMove, ComposeState, Field};

    let mut c = ComposeState::new();
    c.set_field(Field::Subject);
    c.insert_char('é');
    c.insert_char('✓');
    assert_eq!(c.subject, "é✓");
    assert_eq!(c.caret, c.subject.len());
    c.backspace();
    assert_eq!(c.subject, "é");
    c.backspace();
    assert_eq!(c.subject, "");
    c.backspace();
    assert_eq!(c.subject, "", "backspace at the start is a no-op");

    c.subject = "ab".into();
    c.caret = 0;
    c.delete_forward();
    assert_eq!(c.subject, "b");
    c.caret = 99;
    c.delete_forward();
    assert_eq!(c.subject, "b", "a caret past the end cannot panic");

    c.subject = "é".into();
    c.caret = 1;
    c.move_caret(CaretMove::Left);
    assert_eq!(c.caret, 0);
    c.caret = 1;
    c.move_caret(CaretMove::Right);
    assert_eq!(c.caret, 2, "snapped to the boundary then stepped");
}

/// A model whose roster is loaded, so the console can address a live pin and
/// can tell a promptable recipient from a digest-bound one.
fn model_with_roster() -> Model {
    let (m, _) = step(&model(), Msg::SelectTab(Tab::Agents));
    let (m, _) = step(
        &m,
        Msg::AgentsLoaded(vec![
            AgentRow {
                name: "DustySparrow".into(),
                npub: "aa".into(),
                online: true,
                state: PresenceState::Working,
                pane_id: Some("w1:p2".into()),
                ..Default::default()
            },
            AgentRow {
                name: "QuietLark".into(),
                npub: "bb".into(),
                online: false,
                ..Default::default()
            },
        ]),
    );
    m
}

/// `c` opens the form, and what it prefills comes from what is on screen: the
/// roster row under the cursor on the Agents tab, the open conversation
/// elsewhere. One key, one meaning, decided in the fold.
#[test]
fn compose_opens_on_the_selected_agent_or_the_open_conversation() {
    use crate::tui::compose::Field;

    let roster = model_with_roster();
    let (c, _) = step(
        &roster,
        press(&roster, KeyCode::Char('c')).expect("c opens the form"),
    );
    assert_eq!(c.mode, Mode::Compose);
    let form = c.compose.clone().expect("a form is open");
    assert_eq!(form.to, "DustySparrow", "the selected agent is addressed");
    assert_eq!(form.thread, None, "a fresh message, not a reply");
    assert_eq!(form.field, Field::To);
    assert_eq!(form.wake.as_str(), "tasked");

    let (again, _) = step(&c, Msg::OpenCompose);
    assert_eq!(again.compose, c.compose, "the draft survived");

    let (open, _) = step(
        &model(),
        Msg::ThreadLoaded {
            thread_id: "t1".into(),
            rows: vec![mail("m1", "t1", "bee", "status", 10)],
        },
    );
    let (r, _) = step(&open, press(&open, KeyCode::Char('c')).expect("c replies"));
    let form = r.compose.expect("a reply form");
    assert_eq!(form.thread.as_deref(), Some("t1"));
    assert_eq!(form.to, "bee");
    assert_eq!(form.subject, "status");
    assert_eq!(form.field, Field::Body, "a reply starts in the body");
}

/// `Enter` means advance / newline / toggle / send depending only on the
/// focused stop, and the send stop produces the mailbox's send — refused with
/// a reason when the form is incomplete, so no partial message leaves the
/// console.
#[test]
fn enter_advances_the_form_and_only_a_complete_form_sends() {
    use crate::tui::compose::Field;

    let mut m = model_with_roster();
    let (next, _) = step(&m, Msg::OpenCompose);
    m = next;
    let (next, _) = step(&m, Msg::ComposeEnter);
    m = next;
    assert_eq!(
        m.compose.as_ref().expect("form").field,
        Field::Subject,
        "Enter on a text stop advances"
    );
    let (next, _) = step(&m, Msg::ComposeFocus(1));
    m = next;
    let (next, effects) = step(&m, Msg::ComposeEnter);
    m = next;
    assert!(effects.is_empty(), "a newline sends nothing");
    assert_eq!(m.compose.as_ref().expect("form").body, "\n");

    let (next, _) = step(&m, Msg::ComposeFocus(4));
    m = next;
    assert_eq!(m.compose.as_ref().expect("form").field, Field::Send);
    let (refused, effects) = step(&m, Msg::ComposeEnter);
    assert!(effects.is_empty(), "an incomplete form sends nothing");
    let draft = refused.compose.as_ref().expect("the draft is still there");
    assert_eq!(
        draft.to, "DustySparrow",
        "the roster prefilled the recipient, so the refusal is about what is left"
    );
    assert!(
        draft
            .error
            .as_deref()
            .expect("the reason is on the form")
            .contains("subject"),
        "the reason names the field that is missing: {:?}",
        draft.error
    );

    let mut ready = refused;
    {
        let c = ready.compose.as_mut().expect("form");
        c.to = "DustySparrow".into();
        c.subject = "ring check".into();
        c.body = "one\ntwo".into();
        c.field = Field::Send;
        c.wake = toron_core::mailbox::send_tags::Wake::Standing;
        c.importance = crate::tui::compose::Importance::High;
        c.ack = true;
        c.thread = Some("t9".into());
    }
    let (sent, effects) = step(&ready, Msg::ComposeEnter);
    match &effects[..] {
        [Effect::SendMail(c)] => {
            assert_eq!(c.to, vec!["DustySparrow"], "comma-separated in, a list out");
            assert_eq!(c.subject, "ring check");
            assert_eq!(c.body, "one\ntwo", "the body's newline survives");
            assert_eq!(c.thread.as_deref(), Some("t9"));
            assert_eq!(c.wake, "standing");
            assert_eq!(c.importance, "high");
            assert!(c.ack);
        }
        other => panic!("expected exactly one SendMail, got {other:?}"),
    }
    assert_eq!(
        sent.mode,
        Mode::Compose,
        "the form stays open until the send lands"
    );
}

/// A refused send keeps the draft (retyping a message is the one failure nobody
/// forgives), and a landed one says how the recipient wakes — a digest is not
/// a notification, so the two are never conflated.
#[test]
fn a_refused_send_keeps_the_draft_and_a_landed_one_says_how_it_wakes() {
    use crate::tui::compose::Field;

    let mut m = model_with_roster();
    let (next, _) = step(&m, Msg::OpenCompose);
    m = next;
    {
        let c = m.compose.as_mut().expect("form");
        c.to = "DustySparrow".into();
        c.subject = "s".into();
        c.body = "b".into();
        c.field = Field::Send;
    }
    let (failed, _) = step(
        &m,
        Msg::Failed {
            op: Op::Send,
            message: "store refused".into(),
        },
    );
    let form = failed
        .compose
        .as_ref()
        .expect("the draft survives a refused send");
    assert_eq!(form.body, "b", "the typing is not thrown away");
    assert_eq!(form.error.as_deref(), Some("store refused"));
    assert!(failed.status.text.contains("store refused"));
    assert_eq!(failed.status.kind, StatusKind::Error);

    let (typed, _) = step(&failed, Msg::ComposeInsert('!'));
    assert_eq!(
        typed.compose.expect("form").error,
        None,
        "a changed form has no stale refusal on it"
    );

    let (quiet, _) = step(
        &m,
        Msg::MailSent {
            to: "QuietLark".into(),
            message_id: "9f2c8d1e4a6b".into(),
            thread: None,
        },
    );
    assert!(quiet.compose.is_none(), "the form closed");
    assert_eq!(quiet.mode, Mode::Normal);
    assert!(
        quiet.status.text.contains("no live pane"),
        "a digest is not a prompt: {}",
        quiet.status.text
    );
    assert!(quiet.status.text.contains("QuietLark"));

    let (live, _) = step(
        &m,
        Msg::MailSent {
            to: "DustySparrow".into(),
            message_id: "9f2c8d1e4a6b".into(),
            thread: Some("t1".into()),
        },
    );
    assert!(
        !live.status.text.contains("no live pane"),
        "{}",
        live.status.text
    );
    assert!(live.status.text.contains("thread t1"));
    assert_eq!(live.status.kind, StatusKind::Ok);
}

/// The compose windows: a scrolling single line and a scrolling body, both
/// derived from the same wrap the drawing uses, so the caret is always on a
/// drawn row and inside the box.
#[test]
fn compose_windows_keep_the_caret_visible() {
    let (visible, col) = compose::single_line_window("abcdefghij", 4, 10);
    assert_eq!(visible, "hij");
    assert_eq!(col, 3, "the caret sits in the last column, after the text");
    let (visible, col) = compose::single_line_window("abcdefghij", 4, 1);
    assert_eq!(visible, "abcd", "no scroll while the start is visible");
    assert_eq!(col, 1);
    assert_eq!(compose::single_line_window("x", 0, 1), (String::new(), 0));

    let body = "r1\nr2\nr3\nr4\nr5";
    let v = compose::body_view(body, 20, 2, body.len());
    assert_eq!(v.lines, vec!["r4", "r5"], "the box scrolled to the caret");
    assert_eq!(v.caret, (1, 2));
    assert_eq!(
        compose::body_view(body, 20, 9, 0).lines,
        vec!["r1", "r2", "r3", "r4", "r5"],
        "a box with room shows everything"
    );

    for width in 1..=12usize {
        for height in 1..=4usize {
            for at in char_boundaries(body) {
                let v = compose::body_view(body, width, height, at);
                assert!(
                    v.caret.0 < height,
                    "caret row {} escaped a {height}-row box at {at}",
                    v.caret.0
                );
                assert!(
                    v.caret.1 <= width,
                    "caret col {} escaped a {width}-col box at {at}",
                    v.caret.1
                );
                assert!(v.lines.len() <= height);
            }
        }
    }
}

/// Every stop is drawn at every size the console supports: a stop tab can
/// reach but nothing renders is the same defect as a focusable pane nothing
/// draws, and the phone-width frame is where that would show.
#[test]
fn the_form_draws_every_stop_even_at_phone_width() {
    use crate::tui::compose::Field;

    let mut m = model_with_roster();
    let (next, _) = step(&m, Msg::OpenCompose);
    m = next;
    {
        let c = m.compose.as_mut().expect("form");
        c.to = "DustySparrow".into();
        c.subject = "ring check".into();
        c.body = "one\ntwo".into();
        c.field = Field::Body;
        c.caret = 1;
    }
    for (w, h) in [(120u16, 30u16), (40, 12)] {
        let drawn = screen(&m, w, h);
        for needle in [
            "compose",
            "To:",
            "Subject:",
            "Body",
            "tasked",
            "Importance",
            "Ack",
            "send",
        ] {
            assert!(
                drawn.contains(needle),
                "at {w}x{h} the form is missing {needle:?}:\n{drawn}"
            );
        }
    }
    let drawn = screen(&m, 120, 30);
    assert!(
        drawn.contains("ring check"),
        "the subject is drawn:\n{drawn}"
    );

    let (refused, _) = step(
        &m,
        Msg::Failed {
            op: Op::Send,
            message: "store refused".into(),
        },
    );
    assert!(screen(&refused, 120, 30).contains("store refused"));
}

/// The help overlay and the footer both have to own the new key, or the console
/// has an action nobody can find.
#[test]
fn the_console_documents_compose() {
    let help = keymap::help_lines().join("\n");
    assert!(help.contains("c  compose"), "help names the key:\n{help}");
    assert!(help.contains("send stop"), "help explains sending:\n{help}");
    let roster = model_with_roster();
    let line = super::view::hint_line_for_test(&roster, false);
    assert!(
        line.contains("[c] message"),
        "the Agents footer names it: {line}"
    );
    let form = {
        let (m, _) = step(&roster, Msg::OpenCompose);
        m
    };
    let line = super::view::hint_line_for_test(&form, false);
    assert!(
        line.contains("[Esc] cancel"),
        "the form's own footer replaces the tab hints: {line}"
    );
}

/// One message log, the same model and the same effects — including the
/// compose log, which is the newest way to produce an effect.
#[test]
fn the_compose_log_is_deterministic() {
    let log = vec![
        Msg::OpenCompose,
        Msg::ComposeInsert('h'),
        Msg::ComposeInsert('i'),
        Msg::ComposeFocus(1),
        Msg::ComposeInsert('s'),
        Msg::ComposeFocus(1),
        Msg::ComposeEnter,
        Msg::ComposeMove(crate::tui::compose::CaretMove::Left),
    ];
    let replay = |log: &[Msg]| {
        let mut m = model_with_roster();
        let mut effects = Vec::new();
        for msg in log {
            let (next, out) = step(&m, msg.clone());
            m = next;
            effects.extend(out);
        }
        (m, effects)
    };
    let (a, ea) = replay(&log);
    let (b, eb) = replay(&log);
    assert_eq!(a, b, "the same log folds to the same model");
    assert_eq!(ea, eb, "and to the same effects");
    assert!(a.compose.is_some());
}

/// The console does not wrap the message itself. `composed_send` builds the same
/// shape the `mail` verb does, with the ADR-0027 assignment vocabulary on the
/// rumor, and hands it to `MailSurface::send_message`.
///
/// The RING is the core's contract and is witnessed there, not copied here:
/// `wake_mail_delivered_is_metadata_only` (toron-core, `mailbox/messaging.rs`)
/// asserts the kind-9 `mail-delivered` event names from/thread/importance and
/// carries neither subject nor body. What this test witnesses is the seam the
/// console adds: the wake and importance the FORM chose survive the shared
/// encoder and re-parse as the typed values the mailbox acts on. A renamed
/// variant or a second tag spelling fails here instead of on the wire.
#[test]
fn a_composed_send_carries_the_assignment_vocabulary_the_mailbox_parses() {
    use toron_core::mailbox::send_tags::{SendTags, Wake};

    let mut form = ComposeState::new();
    form.to = "DustySparrow, QuietLark".into();
    form.subject = "ring check".into();
    form.body = "line one\nline two".into();
    form.wake = Wake::Standing;
    form.importance = Importance::Urgent;
    form.ack = true;
    form.thread = Some("t9".into());

    let msg = composed_send("demo", "TestAgent", &form.to_send()).expect("encode the send");
    assert_eq!(
        msg.to,
        vec!["DustySparrow", "QuietLark"],
        "a comma-separated field reaches the mailbox as a list"
    );
    assert_eq!(msg.project_slug, "demo");
    assert_eq!(msg.sender_name, "TestAgent");
    assert_eq!(msg.subject, "ring check");
    assert_eq!(msg.body_md, "line one\nline two", "the newline survives");
    assert_eq!(msg.importance.as_deref(), Some("urgent"));
    assert!(msg.ack_required);
    assert_eq!(msg.thread_id.as_deref(), Some("t9"));
    assert!(!msg.overseer, "the console never signs as the operator");

    let parsed = SendTags::parse(&msg.extra_tags).expect("the tags re-parse");
    assert_eq!(
        parsed.wake,
        Some(Wake::Standing),
        "the form's choice survives the encoder: {:?}",
        msg.extra_tags
    );

    let flat: String = msg
        .extra_tags
        .iter()
        .map(|t| t.as_slice().join("="))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !flat.contains("ring check"),
        "no subject on the rumor: {flat}"
    );
    assert!(!flat.contains("line two"), "no body on the rumor: {flat}");
}
