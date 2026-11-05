// governed-by: ADR-0003

use std::io::IsTerminal;

use crate::cli::Globals;

/// Output rendering policy (clig.dev: stdout = data, stderr = messages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-readable (default for a TTY).
    Human,
    /// `--json`: machine-parseable on stdout.
    Json,
    /// `--plain`: flat tabular text for grep/awk.
    Plain,
}

/// Color policy: auto off when piped, `NO_COLOR`, `TERM=dumb`, or `--no-color`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorChoice {
    Always,
    Never,
}

impl OutputFormat {
    /// Resolve the format from global flags.
    pub const fn from_globals(g: &Globals) -> Self {
        if g.json {
            Self::Json
        } else if g.plain {
            Self::Plain
        } else {
            Self::Human
        }
    }
}

impl ColorChoice {
    /// Resolve color from flags + environment + TTY.
    pub fn from_globals(g: &Globals, out_is_tty: bool) -> Self {
        if g.no_color || std::env::var_os("NO_COLOR").is_some() {
            return Self::Never;
        }
        if std::env::var_os("FORCE_COLOR").is_some() {
            return Self::Always;
        }
        if std::env::var_os("TERM").is_some_and(|t| t == "dumb") {
            return Self::Never;
        }
        if out_is_tty {
            Self::Always
        } else {
            Self::Never
        }
    }
}

/// Render a JSON value to stdout in the requested format.
pub fn render_json(value: &serde_json::Value, format: OutputFormat) {
    match format {
        OutputFormat::Json | OutputFormat::Plain => {
            println!("{}", serde_json::to_string_pretty(value).unwrap());
        }
        OutputFormat::Human => {
            println!("{}", serde_json::to_string_pretty(value).unwrap());
        }
    }
}

/// Whether stdout is a TTY (used to gate color + human formatting).
pub fn stdout_is_tty() -> bool {
    std::io::stdout().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_format_and_color_precedence() {
        let g = Globals {
            json: true,
            plain: true,
            ..Globals::default()
        };
        assert_eq!(OutputFormat::from_globals(&g), OutputFormat::Json);
        let g = Globals {
            plain: true,
            ..Globals::default()
        };
        assert_eq!(OutputFormat::from_globals(&g), OutputFormat::Plain);
        let g = Globals {
            no_color: true,
            ..Globals::default()
        };
        assert_eq!(ColorChoice::from_globals(&g, true), ColorChoice::Never);
    }
}
