// governed-by: ADR-0003
//! `3`/`l` Leases & Locks inspector + sealed inbox count badge (bodies hidden).

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::Model;
use super::widgets;

pub fn render_leases(f: &mut Frame, area: Rect, model: &Model) {
    let mut lines = vec![Line::from(vec![
        Span::styled(
            format!("Leases & Locks ({}) ", model.leases.len()),
            HackerNight::fg_emphasis(),
        ),
        Span::styled(
            format!(
                "· sealed inbox: {} total, {} unread (bodies hidden) · receipts owed: {}",
                model.sealed_inbox.total, model.sealed_inbox.unread, model.receipts.owed
            ),
            HackerNight::fg_muted(),
        ),
    ])];

    if model.leases.is_empty() {
        lines.push(Line::from(Span::styled(
            "  ✓ no active file reservations",
            HackerNight::success(),
        )));
    } else {
        for lease in &model.leases {
            let mode = if lease.exclusive { "excl" } else { "shared" };
            let ttl = lease.expires_at.saturating_sub(model.now_unix);
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {:<14} ", lease.holder),
                    HackerNight::accent_lav(),
                ),
                Span::styled(format!("{:<6} ", mode), HackerNight::info()),
                Span::styled(
                    format!("ttl {:<5} ", widgets::age_of(ttl)),
                    HackerNight::fg_muted(),
                ),
                Span::styled(lease.path.clone(), HackerNight::fg_default()),
            ]));
        }
    }

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), area);
}
