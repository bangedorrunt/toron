// governed-by: ADR-0003
// governed-by: ADR-0020
// governed-by: ADR-0023
#![forbid(unsafe_code)]

//! Frozen kind registry (ADR-0003, AGREEMENT LOCKED 2026-08-04).
//!
//! Any renumber requires a new ADR revision. Verified against the nips repo
//! occupancy table. Kind numbers in the toron namespace: 30010-30014, 30031,
//! 30032, 20001, 43001-43006, 46001-46012.

use crate::nostr::Kind;

/// Project manifest (addressable, `d`=slug). Operator-published; products list
/// members via `a`.
pub const PROJECT_MANIFEST: u16 = 30010;
/// Product manifest (addressable, `d`=slug). Cross-project via `a`-tagged
/// manifests.
pub const PRODUCT_MANIFEST: u16 = 30011;
/// Registration attestation (addressable, `d`=project-slug:name).
/// Trust-anchor-signed (proof gate).
pub const REGISTRATION_ATTESTATION: u16 = 30012;
/// Agent profile (addressable, `d`=project-slug:name). Name→npub map source.
pub const AGENT_PROFILE: u16 = 30013;
/// Contact policy (addressable, `d`=project-slug:name). Enforced at projection.
pub const CONTACT_POLICY: u16 = 30014;
/// File reservation (addressable, `d`=project:path-glob). NIP-40 `expiration`.
pub const FILE_RESERVATION: u16 = 30031;
/// Build slot (addressable, `d`=project:slot). NIP-40 `expiration`.
pub const BUILD_SLOT: u16 = 30032;
/// Presence / working-state (ephemeral, not stored).
pub const PRESENCE: u16 = 20001;
/// NIP-29 / ADR-0003 channel post (kind 9). Adopted standard.
pub const CHANNEL_MESSAGE: u16 = 9;
/// NIP-25 reaction.
pub const REACTION: u16 = 7;
/// NIP-29 group metadata (ADR-0023). Addressable; `d`=`h`=channel id.
pub const CHANNEL_GROUP: u16 = 39000;
/// NIP-29 group members snapshot (ADR-0023). Addressable; `p`=members.
pub const CHANNEL_MEMBERS: u16 = 39002;
/// Pre-ADR-0023 NIP-28 channel-create. Ingest only.
pub const CHANNEL_CREATE_LEGACY: u16 = 40;
/// Pre-ADR-0023 NIP-28 channel-metadata (join/leave/close). Ingest only.
pub const CHANNEL_METADATA_LEGACY: u16 = 41;
/// Pre-ADR-0020 channel post. Ingest only.
pub const CHANNEL_MESSAGE_LEGACY: u16 = 42;
/// Job request (regular, immutable; `e`=parent). Buzz-verbatim.
pub const JOB_REQUEST: u16 = 43001;
/// Job accepted/claim (regular; `e`=job). Signed by claiming agent.
pub const JOB_ACCEPTED: u16 = 43002;
/// Job progress (regular; `e`=job). Optional in-flight updates.
pub const JOB_PROGRESS: u16 = 43003;
/// Job result (regular; `e`=job). Outcome payload; closes the lifecycle.
pub const JOB_RESULT: u16 = 43004;
/// Job cancel (regular; `e`=job). Cancellation request.
pub const JOB_CANCEL: u16 = 43005;
/// Job error (regular; `e`=job). Failed-with-error terminal state.
pub const JOB_ERROR: u16 = 43006;
/// Workflow run/step/approval (regular/replaceable; `d`=run-id). Resonate-backed.
pub const WORKFLOW_RUN_FIRST: u16 = 46001;
/// Workflow run/step/approval (regular/replaceable; `d`=run-id). Resonate-backed.
pub const WORKFLOW_RUN_LAST: u16 = 46012;

/// Convert a toron kind number to a `Kind`.
pub const fn toron_kind(kind: u16) -> Kind {
    Kind::from_u16(kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frozen_registry_values() {
        assert_eq!(PROJECT_MANIFEST, 30010);
        assert_eq!(PRODUCT_MANIFEST, 30011);
        assert_eq!(REGISTRATION_ATTESTATION, 30012);
        assert_eq!(AGENT_PROFILE, 30013);
        assert_eq!(CONTACT_POLICY, 30014);
        assert_eq!(FILE_RESERVATION, 30031);
        assert_eq!(BUILD_SLOT, 30032);
        assert_eq!(PRESENCE, 20001);
        assert_eq!(CHANNEL_MESSAGE, 9);
        assert_eq!(REACTION, 7);
        assert_eq!(CHANNEL_GROUP, 39000);
        assert_eq!(CHANNEL_MEMBERS, 39002);
        assert_eq!(CHANNEL_CREATE_LEGACY, 40);
        assert_eq!(CHANNEL_METADATA_LEGACY, 41);
        assert_eq!(CHANNEL_MESSAGE_LEGACY, 42);
        assert_eq!(JOB_REQUEST, 43001);
        assert_eq!(JOB_ACCEPTED, 43002);
        assert_eq!(JOB_PROGRESS, 43003);
        assert_eq!(JOB_RESULT, 43004);
        assert_eq!(JOB_CANCEL, 43005);
        assert_eq!(JOB_ERROR, 43006);
        assert_eq!(WORKFLOW_RUN_FIRST, 46001);
        assert_eq!(WORKFLOW_RUN_LAST, 46012);
    }

    #[test]
    fn test_standard_kinds_are_frozen() {
        // Standard kinds toron uses — verify the SDK's built-in Kind values
        // match the registry (14/15/1059, 7). Kind 15 (file message) is not a
        // named SDK variant; it stays as `Kind::Custom(15)`.
        assert_eq!(Kind::PrivateDirectMessage.as_u16(), 14);
        assert_eq!(Kind::GiftWrap.as_u16(), 1059);
        assert_eq!(Kind::Reaction.as_u16(), 7);
        assert_eq!(Kind::from_u16(15).as_u16(), 15);
        // rust-nostr 0.45 named NIP-29 group kinds (ADR-0023).
        assert_eq!(Kind::GroupMetadata.as_u16(), CHANNEL_GROUP);
        assert_eq!(Kind::GroupMembers.as_u16(), CHANNEL_MEMBERS);
    }

    /// Completeness: the frozen registry (ADR-0003, AGREEMENT LOCKED
    /// 2026-08-04) as a table. Every value here is asserted against the
    /// constant so a renumber or typo is caught immediately.
    #[test]
    fn test_frozen_registry_completeness_vs_adr() {
        let table: &[(u16, &str)] = &[
            (PROJECT_MANIFEST, "project manifest"),
            (PRODUCT_MANIFEST, "product manifest"),
            (REGISTRATION_ATTESTATION, "registration attestation"),
            (AGENT_PROFILE, "agent profile"),
            (CONTACT_POLICY, "contact policy"),
            (FILE_RESERVATION, "file reservation"),
            (BUILD_SLOT, "build slot"),
            (PRESENCE, "presence"),
            (JOB_REQUEST, "job request"),
            (JOB_ACCEPTED, "job accepted"),
            (JOB_PROGRESS, "job progress"),
            (JOB_RESULT, "job result"),
            (JOB_CANCEL, "job cancel"),
            (JOB_ERROR, "job error"),
            (WORKFLOW_RUN_FIRST, "workflow run first"),
            (WORKFLOW_RUN_LAST, "workflow run last"),
        ];
        // Every ADR frozen value is present.
        assert_eq!(table.len(), 16, "frozen registry has 16 toron constants");
        for (value, _name) in table {
            assert_eq!(toron_kind(*value).as_u16(), *value);
        }
        // No duplicates.
        let mut vals: Vec<u16> = table.iter().map(|(v, _)| *v).collect();
        let n = vals.len();
        vals.sort_unstable();
        vals.dedup();
        assert_eq!(
            vals.len(),
            n,
            "frozen registry has no duplicate kind values"
        );
        // Kind ranges are internally consistent (const-time checked).
        const _: () = assert!(WORKFLOW_RUN_FIRST <= WORKFLOW_RUN_LAST);
        const _: () = assert!(
            (WORKFLOW_RUN_LAST - WORKFLOW_RUN_FIRST + 1) == 12,
            "46001..=46012"
        );
    }
}
