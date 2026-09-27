// governed-by: ADR-0003
//! The compose modal: a form over whatever tab is behind it.
//!
//! The only thing here that needs care is the caret. It is placed with
//! [`compose::caret_pos`] (through [`compose::body_view`] /
//! [`compose::single_line_window`]) over the very lines this file draws, so the
//! cursor cannot land on a row the text is not on. The body is drawn
//! pre-wrapped and its paragraph is given no `Wrap`, because a second wrap in
//! the renderer is exactly how the drawn caret and the drawn text come apart.
//!
//! Every stop is drawn at every size the console supports: a form stop that
//! focus can reach but nothing renders is the same defect as a focusable pane
//! nothing draws.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::theme::HackerNight;

use super::super::compose::{self, Field};
use super::super::model::Model;
use super::widgets;

/// Widest the form gets. A message box stretched across a 200-column terminal
/// is harder to read than a book-width one.
const MAX_W: u16 = 78;
/// Widest the label gutter gets.
const LABEL_W: usize = 11;

/// Draw the form over the frame.
pub fn render(f: &mut Frame, area: Rect, model: &Model) {
    let Some(c) = model.compose.as_ref() else {
        return;
    };
    let modal = centred(area);
    f.render_widget(Clear, modal);
    let title = if c.thread.is_some() {
        "reply".to_string()
    } else {
        "compose".to_string()
    };
    let block = widgets::block_focused(&title);
    let inner = block.inner(modal);
    f.render_widget(block, modal);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    // Every stop is always drawn, so `Tab` can never focus something invisible.
    // The body takes what is left; the minimum console size still leaves it a
    // row (see `MIN_H`).
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // To
            Constraint::Length(1), // Subject
            Constraint::Min(1),    // Body
            Constraint::Length(1), // Wake
            Constraint::Length(1), // Importance
            Constraint::Length(1), // Ack
            Constraint::Length(1), // send
        ])
        .split(inner);
    let value_w = inner.width.saturating_sub(LABEL_W as u16 + 2) as usize;

    // To and Subject are single lines, scrolled to keep the caret visible.
    for (field, row) in [(Field::To, rows[0]), (Field::Subject, rows[1])] {
        let text = c.text_of(field);
        let focused = c.field == field;
        let (visible, caret_col) = if focused {
            compose::single_line_window(text, value_w, c.caret)
        } else {
            (text.chars().take(value_w).collect::<String>(), 0)
        };
        f.render_widget(line_widget(field, &visible, focused), row);
        if focused {
            f.set_cursor_position((row.x + LABEL_W as u16 + 1 + caret_col as u16, row.y));
        }
    }

    // The body: pre-wrapped rows, no second wrap.
    let body_area = rows[2];
    let focused = c.field == Field::Body;
    f.render_widget(
        widgets::pane_block("Body", focused),
        Rect {
            height: 1,
            ..body_area
        },
    );
    if body_area.height > 2 {
        let box_area = Rect {
            y: body_area.y + 1,
            height: body_area.height - 1,
            ..body_area
        };
        let view = compose::body_view(
            &c.body,
            (body_area.width as usize).saturating_sub(2),
            (box_area.height as usize).saturating_sub(2),
            c.caret,
        );
        let text: Vec<Line> = view
            .lines
            .iter()
            .map(|l| {
                Line::from(Span::styled(
                    l.clone(),
                    if focused {
                        HackerNight::fg_default()
                    } else {
                        HackerNight::fg_muted()
                    },
                ))
            })
            .collect();
        // No `Wrap`: these rows already fit, and letting the renderer wrap
        // again is what would move the text out from under the caret.
        f.render_widget(
            Paragraph::new(text).block(widgets::pane_block("", focused)),
            box_area,
        );
        if focused {
            f.set_cursor_position((
                box_area.x + 1 + view.caret.1 as u16,
                box_area.y + view.caret.0 as u16,
            ));
        }
    }

    // The toggles and the send stop.
    let wake = match c.wake {
        toron_core::mailbox::send_tags::Wake::Tasked => {
            "tasked — the doorbell treats it as an assignment"
        }
        toron_core::mailbox::send_tags::Wake::Standing => "standing — a standing order",
    };
    let importance = format!("{} (normal / high / urgent)", c.importance.as_str());
    let ack = if c.ack {
        "yes — they are asked to acknowledge"
    } else {
        "no"
    };
    for (field, text, row) in [
        (Field::Wake, wake.to_string(), rows[3]),
        (Field::Importance, importance, rows[4]),
        (Field::Ack, ack.to_string(), rows[5]),
    ] {
        // Every stop is labelled the same way, so a toggle reads as a field
        // with a name rather than a stray value floating in the gutter.
        f.render_widget(line_widget(field, &text, c.field == field), row);
    }

    // The send stop, with the last refusal under it when there is one.
    let send_focused = c.field == Field::Send;
    let marker = if send_focused { "▶" } else { " " };
    let mut line = vec![
        Span::styled(format!("{marker} "), HackerNight::accent_cyan()),
        Span::styled(
            "[ send ]",
            if send_focused {
                HackerNight::selection()
            } else {
                HackerNight::fg_default()
            },
        ),
    ];
    if let Some(err) = &c.error {
        line.push(Span::styled(format!("  ✗ {err}"), HackerNight::error()));
    }
    f.render_widget(
        Paragraph::new(Line::from(line)).style(HackerNight::fg_default()),
        rows[6],
    );
}

/// A labelled single-line value.
fn line_widget(field: Field, value: &str, focused: bool) -> Paragraph<'static> {
    let label = format!("{:<width$}", format!("{}:", field.label()), width = LABEL_W);
    let mut spans = vec![Span::styled(
        label,
        if focused {
            HackerNight::fg_emphasis()
        } else {
            HackerNight::fg_muted()
        },
    )];
    if focused {
        spans.push(Span::styled("▏", HackerNight::accent_cyan()));
    } else {
        spans.push(Span::styled(" ", HackerNight::fg_default()));
    }
    spans.push(Span::styled(
        value.to_string(),
        if focused {
            HackerNight::fg_default()
        } else {
            HackerNight::fg_muted()
        },
    ));
    Paragraph::new(Line::from(spans)).style(HackerNight::fg_default())
}

/// The modal rectangle: most of the frame, never wider than [`MAX_W`], always
/// leaving a column and a row of the tab visible so the operator knows where
/// the form came from.
fn centred(area: Rect) -> Rect {
    let width = area.width.saturating_sub(2).min(MAX_W);
    let height = area.height.saturating_sub(2);
    Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    }
}
