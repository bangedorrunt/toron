// governed-by: ADR-0031
// Platform boundary (bd-nc5d, RUST PRACTICES): OS-specific process
// inspection lives in `platform/<os>.rs`. `#[cfg]` selection happens HERE
// and only here — leaves carry no cfg attributes.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::*;

#[cfg(not(target_os = "linux"))]
mod portable;
#[cfg(not(target_os = "linux"))]
pub(crate) use portable::*;
