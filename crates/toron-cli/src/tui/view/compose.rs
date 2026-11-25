// governed-by: ADR-0003
//! Compact `#seq` board-post compose modal.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::Model;

pub fn render(f: &mut Frame, area: Rect, model: &Model) {
    let w = (area.width * 3 / 4).clamp(36, 76).min(area.width);
    let h = 8u16.min(area.height);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    let card = Rect {
        x,
        y,
        width: w,
        height: h,
    };

    let who = model.settings.agent.as_deref().unwrap_or("<no pin>");
    let draft = model.compose_draft.as_deref().unwrap_or("");
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(format!(" post to #seq board as {who} "))
        .title_style(HackerNight::title_focused())
        .style(
            ratatui::style::Style::default()
                .bg(HackerNight::OVERLAY)
                .fg(HackerNight::FG),
        );

    let text = vec![
        Line::from(Span::styled(format!("{draft}_"), HackerNight::fg_default())),
        Line::from(""),
        Line::from(Span::styled(
            "[Enter] post  [Esc] cancel  (secret-class text refused)",
            HackerNight::fg_muted(),
        )),
    ];
    f.render_widget(Clear, card);
    f.render_widget(
        Paragraph::new(text).block(block).wrap(Wrap { trim: false }),
        card,
    );
}
