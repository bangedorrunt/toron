// governed-by: ADR-0003
// F23.BUS-DEDUP (batch-fix): the publish-time verify block was copy-pasted
// between in_process.rs and relay_bus.rs. This is the canonical
// VerifyError -> BusError mapping; both publish paths call it (in_process
// keeps its stage_log on the returned error).

use crate::bus::BusError;
use crate::nostr::event::VerifyError;

/// Reject forged/mutated events at publish: wrong id or signature fails
/// before any fanout/store. Canonical mapping shared by every bus impl.
pub fn verify_publish_event(event: &nostr_sdk::nostr::Event) -> Result<(), BusError> {
    match crate::nostr::event::verify_event(event) {
        Ok(()) => Ok(()),
        Err(VerifyError::BadId) => Err(BusError::InvalidEvent("id mismatch".into())),
        Err(VerifyError::BadSignature) => {
            Err(BusError::InvalidEvent("signature mismatch".into()))
        }
        Err(e) => Err(BusError::InvalidEvent(e.to_string())),
    }
}
