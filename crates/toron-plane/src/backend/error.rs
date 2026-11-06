// governed-by: ADR-0003
//! Re-export shim: the error vocabulary lives in core (`pane_types`) so the
//! [`crate::plane_port::PanePlane`] port can name it without depending on
//! this crate. All behavior stays here.

pub use toron_core::pane_types::{BackendError, BackendErrorCode};
