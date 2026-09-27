// governed-by: ADR-0003
// governed-by: ADR-0031
//! `toron tui` — the operator console: mail, channels, agents, dashboard,
//! settings in one Gmail-shaped frame.
//!
//! Shape (ADR-0031 §7, "Elm-everywhere"): `model` is pure data, `update::tick`
//! is the only reducer, `msg::Effect` values are the only way work leaves it,
//! and this module is the interpreter that runs them and feeds the results
//! back. No I/O in the fold; no decision in this file.
//!
//! ```text
//!   keyboard ──keymap──▶ Msg ─┐
//!                             ├─▶ tick(&Model, Msg) ─▶ (Model, Vec<Effect>)
//!   host results ────────────▶┘                              │
//!                                                            ▼
//!                                        dispatch: bounded host.run(effect) ─▶ Msg
//! ```

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

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::cli::TuiArgs;

pub use host::{LiveHost, TuiHost};
pub use model::{Folder, Model, Tab};
pub use msg::{Effect, Msg};

/// Run the console. Refuses a non-TTY outright (a raw-mode dashboard on a
/// pipe would hang a script), then resolves identity + the one store before
/// taking the screen.
#[tokio::main]
pub async fn run_tui(args: &TuiArgs) -> anyhow::Result<()> {
    // Flags first: a typo must be reported as a typo whether or not the caller
    // has a TTY, and before any store is opened.
    let tab = Tab::parse(&args.tab).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown --tab {:?} (mail|channels|agents|dashboard|settings)",
            args.tab
        )
    })?;
    let folder = Folder::parse(&args.folder).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown --folder {:?} (inbox|unread|threads|ack|important)",
            args.folder
        )
    })?;
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        anyhow::bail!("toron tui requires a TTY (refusing to run on a pipe)");
    }

    // Resolve before taking the screen: a bad project/pin/daemon must fail on
    // the normal terminal, not inside the alternate screen.
    let host = LiveHost::connect(args).await?;
    let model = Model::seed(tab, folder, host.settings());

    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, host, model).await;
    ratatui::restore();
    result
}

/// The interpreter loop: draw, then wait for a key, a result, or the tick.
async fn run_loop<H: TuiHost>(
    terminal: &mut ratatui::DefaultTerminal,
    host: H,
    model: Model,
) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<Msg>(128);
    let (event_tx, mut event_rx) = mpsc::channel::<crossterm::event::Event>(128);
    spawn_input(event_tx);

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
                // Resize and focus events only need a redraw, which the loop
                // does every iteration; keys go through the pure keymap.
                if let crossterm::event::Event::Key(key) = event
                    && let Some(message) = keymap::translate(&model, key)
                {
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
    // The scope owns its children: leaving the screen aborts every fetch.
    for handle in handles {
        handle.abort();
    }
    Ok(())
}

/// Spawn the effects. Every fetch is bounded by [`host::effect_timeout`] and
/// its outcome comes back as a message, so a wedged store cannot wedge the
/// dashboard.
fn dispatch<H: TuiHost>(
    host: &H,
    tx: &mpsc::Sender<Msg>,
    handles: &mut Vec<JoinHandle<()>>,
    effects: Vec<Effect>,
) {
    for effect in effects {
        // Quit is the runtime's own job; the fold has already set should_quit.
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

/// Drop finished tasks and report how many effects are still owed an answer.
///
/// The count is DERIVED, never accumulated: a task that panics inside the host
/// never posts a result, so an accumulator would stay above zero forever and
/// the fold (`tick_refresh`) would refuse to dispatch again — a frozen console
/// with no typed failure anywhere.
pub(crate) fn live_handles(handles: &mut Vec<JoinHandle<()>>) -> usize {
    handles.retain(|h| !h.is_finished());
    handles.len()
}

/// Fold the derived in-flight count back into the model.
fn note_inflight(model: &mut Model, inflight: usize) {
    let (next, _) = update::tick(model, Msg::Inflight(inflight));
    *model = next;
}

/// One effect, with its ceiling attached at the edge (effect law: bound every
/// wait; journal the attempt before the provider runs — the host records the
/// call before it answers, which is what the interpreter tests assert).
/// `timeout` is a parameter so the tests can bound a scripted hang without
/// touching the process environment.
pub(crate) async fn bounded<H: TuiHost>(host: &H, effect: Effect, timeout: Duration) -> Msg {
    let op = host::effect_op(&effect);
    match tokio::time::timeout(timeout, host.run(effect)).await {
        Ok(message) => message,
        Err(_) => Msg::Failed {
            op,
            message: format!(
                "no answer after {}ms — the store is not responding (TORON_TUI_TIMEOUT_MS raises this)",
                timeout.as_millis()
            ),
        },
    }
}

/// Key input on a blocking thread (crossterm has no async reader without the
/// `event-stream` feature; a dedicated thread keeps the dependency set as-is).
/// The thread ends when the receiver drops, and a 200ms poll keeps it
/// responsive to that.
fn spawn_input(tx: mpsc::Sender<crossterm::event::Event>) {
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
                Ok(false) => continue,
                Err(_) => break,
            }
        }
    });
}
