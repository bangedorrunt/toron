// governed-by: ADR-0003
// governed-by: ADR-0031
//! `toron tui` (`dashboard` / `ui`) — Swarm Board console and `--json`/`--check` AX door.

pub mod charts;
pub mod compose;
pub mod host;
pub mod keymap;
pub mod model;
pub mod msg;
pub mod update;
pub mod view;

#[cfg(test)]
mod tests;

use std::io::IsTerminal;
use std::time::Duration;

use crossterm::event::{DisableFocusChange, EnableFocusChange, Event};
use crossterm::execute;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use toron_core::bus::{Filter, SubscriptionEvent};
use toron_core::nostr::kinds::toron_kind;

use crate::cli::TuiArgs;

pub use host::{LiveHost, TuiHost};
pub use model::Model;
pub use msg::{Effect, Msg};

/// Run the Swarm Board console, or emit `--json` / `--check` in non-TTY pipes.
#[tokio::main]
pub async fn run_tui(args: &TuiArgs) -> anyhow::Result<()> {
    let ax_mode = args.json || args.check;
    if !ax_mode && (!std::io::stdout().is_terminal() || !std::io::stdin().is_terminal()) {
        anyhow::bail!("toron tui requires a TTY (pass --json or --check for non-TTY output)");
    }

    let host = LiveHost::connect(args).await?;
    let model = Model::seed(host.settings());

    if ax_mode {
        let final_model = run_oneshot(&host, model).await;
        if args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&final_model.projection())?
            );
        }
        if args.check && final_model.has_blocking_diagnostics() {
            let ids: Vec<String> = final_model
                .diagnostics()
                .into_iter()
                .filter(|d| d.severity.is_blocking())
                .map(|d| format!("{}: {}", d.severity.label(), d.id))
                .collect();
            anyhow::bail!("blocking diagnostics detected: {}", ids.join(", "));
        }
        return Ok(());
    }

    let _ = execute!(std::io::stdout(), EnableFocusChange);
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, host, model).await;
    ratatui::restore();
    let _ = execute!(std::io::stdout(), DisableFocusChange);
    result
}

async fn run_oneshot<H: TuiHost>(host: &H, model: Model) -> Model {
    let (mut current, effects) = update::tick(&model, Msg::Init);
    let mut handles = Vec::new();
    for effect in effects {
        if effect == Effect::Quit {
            continue;
        }
        let h = host.clone();
        handles.push(tokio::spawn(async move {
            bounded(&h, effect, host::effect_timeout()).await
        }));
    }
    for handle in handles {
        if let Ok(message) = handle.await {
            let (next, _) = update::tick(&current, message);
            current = next;
        }
    }
    current
}

async fn run_loop(
    terminal: &mut ratatui::DefaultTerminal,
    host: LiveHost,
    model: Model,
) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<Msg>(128);
    let (event_tx, mut event_rx) = mpsc::channel::<Event>(128);
    spawn_input(event_tx);
    spawn_bus_watcher(&host, tx.clone());

    let mut model = model;
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    let (next, effects) = update::tick(&model, Msg::Init);
    model = next;
    dispatch(&host, &tx, &mut handles, effects);
    note_inflight(&mut model, live_handles(&mut handles));

    while !model.should_quit {
        terminal.draw(|f| view::render(f, &model))?;

        let sleep = tokio::time::sleep(Duration::from_millis(model.settings.refresh_ms));
        tokio::pin!(sleep);
        tokio::select! {
            maybe = rx.recv() => {
                let Some(message) = maybe else { break };
                let (next, effects) = update::tick(&model, message);
                model = next;
                dispatch(&host, &tx, &mut handles, effects);
                note_inflight(&mut model, live_handles(&mut handles));
            }
            maybe = event_rx.recv() => {
                let Some(event) = maybe else { break };
                let maybe_msg = match event {
                    Event::Key(key) => keymap::translate(&model, key),
                    Event::FocusGained => Some(Msg::FocusChanged(true)),
                    Event::FocusLost => Some(Msg::FocusChanged(false)),
                    _ => None,
                };
                if let Some(message) = maybe_msg {
                    let (next, effects) = update::tick(&model, message);
                    model = next;
                    dispatch(&host, &tx, &mut handles, effects);
                    note_inflight(&mut model, live_handles(&mut handles));
                }
            }
            _ = &mut sleep => {
                let (next, effects) = update::tick(&model, Msg::Tick { now_unix: host.now_unix() });
                model = next;
                dispatch(&host, &tx, &mut handles, effects);
                note_inflight(&mut model, live_handles(&mut handles));
            }
        }
    }
    for handle in handles {
        handle.abort();
    }
    Ok(())
}

fn spawn_bus_watcher(host: &LiveHost, tx: mpsc::Sender<Msg>) {
    let mailbox = host.mailbox().clone();
    tokio::spawn(async move {
        let filter = Filter::new().kinds([toron_kind(9)]);
        let Ok(mut sub) = mailbox.bus().subscribe_trusted(filter).await else {
            return;
        };
        while let Some(ev) = sub.next().await {
            if matches!(ev, SubscriptionEvent::Event(_)) && tx.send(Msg::BusChanged).await.is_err()
            {
                break;
            }
        }
    });
}

fn dispatch<H: TuiHost>(
    host: &H,
    tx: &mpsc::Sender<Msg>,
    handles: &mut Vec<JoinHandle<()>>,
    effects: Vec<Effect>,
) {
    for effect in effects {
        if effect == Effect::Quit {
            continue;
        }
        let host = host.clone();
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            let message = bounded(&host, effect, host::effect_timeout()).await;
            let _ = tx.send(message).await;
        }));
    }
}

pub(crate) fn live_handles(handles: &mut Vec<JoinHandle<()>>) -> usize {
    handles.retain(|h| !h.is_finished());
    handles.len()
}

fn note_inflight(model: &mut Model, inflight: usize) {
    let (next, _) = update::tick(model, Msg::Inflight(inflight));
    *model = next;
}

pub(crate) async fn bounded<H: TuiHost>(host: &H, effect: Effect, timeout: Duration) -> Msg {
    let op = host::effect_op(&effect);
    match tokio::time::timeout(timeout, host.run(effect)).await {
        Ok(message) => message,
        Err(_) => Msg::Failed {
            op,
            message: format!(
                "no answer after {}ms — store not responding (TORON_TUI_TIMEOUT_MS raises this)",
                timeout.as_millis()
            ),
        },
    }
}

fn spawn_input(tx: mpsc::Sender<Event>) {
    std::thread::spawn(move || {
        loop {
            match crossterm::event::poll(Duration::from_millis(200)) {
                Ok(true) => {
                    let Ok(event) = crossterm::event::read() else {
                        break;
                    };
                    if tx.blocking_send(event).is_err() {
                        break;
                    }
                }
                Ok(false) => {}
                Err(_) => break,
            }
        }
    });
}
