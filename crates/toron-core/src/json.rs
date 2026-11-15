//! Shared JSON projection: `ToJson` hand-mirrors toron-core result structs
//! into their AM-Rust parity JSON shape (parity-by-construction).
//!
//! Moved from `toron-mcp` (wave5-c): the trait lives where the types live.
//! toron-mcp re-exports it (`pub use toron_core::json::ToJson;`) so existing
//! callers compile unchanged.

use serde_json::{Value, json};

use crate::index::slots::{BuildSlot, GrantedSlot, SlotConflict, SlotStatus};
use crate::mailbox::judgment::JudgmentOutcome;
use crate::mailbox::{
    AcquireBuildSlotResult, AcquireResult, ConflictReport, ContactHandshakeResult, ContactLink,
    Delivery, DeliveryPayload, GrantedReservation, HealthComponent, HealthReport, InboxMessage,
    PathConflict, PrepareThreadResult, ProductInfo, ProjectState, ReceiptResult, RegisteredAgent,
    ReleaseResult, RenewResult, RequestReleaseResult, ReservationCycleResult, SearchMessage,
    SendResult, StartSessionResult, ThreadSummary, TransferResult, WhoIs,
};

/// Serialize a toron-core result into its AM-Rust parity JSON shape.
///
/// The toron-core result structs do not derive `Serialize`, so the JSON shape
/// is hand-mirrored field-by-field (parity-by-construction).
pub trait ToJson {
    fn to_json(&self) -> Value;
}

impl ToJson for bool {
    fn to_json(&self) -> Value {
        json!(self)
    }
}

impl ToJson for String {
    fn to_json(&self) -> Value {
        json!(self)
    }
}

impl ToJson for () {
    fn to_json(&self) -> Value {
        json!({ "ok": true })
    }
}

impl<T: ToJson> ToJson for Vec<T> {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(ToJson::to_json).collect())
    }
}

impl<T: ToJson> ToJson for Option<T> {
    fn to_json(&self) -> Value {
        self.as_ref().map(ToJson::to_json).unwrap_or(Value::Null)
    }
}

impl ToJson for HealthComponent {
    fn to_json(&self) -> Value {
        json!({ "ok": self.ok, "message": self.message })
    }
}

impl ToJson for HealthReport {
    fn to_json(&self) -> Value {
        json!({ "bus": self.bus.to_json(), "archive": self.archive.to_json() })
    }
}

impl ToJson for ProjectState {
    fn to_json(&self) -> Value {
        json!({
            "slug": self.slug,
            "manifest_event_id": self.manifest_event_id,
            "created": self.created,
        })
    }
}

impl ToJson for RegisteredAgent {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "project_slug": self.project_slug,
            "npub": self.npub,
            "profile_event_id": self.profile_event_id,
            "key_path": self.key_path,
            "created": self.created,
        })
    }
}

impl ToJson for WhoIs {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "npub": self.npub,
            "program": self.program,
            "model": self.model,
            "task_description": self.task_description,
            "last_active_ts_us": self.last_active_ts_us,
            "stale": self.stale,
            "pane_id": self.pane_id,
            "workspace_id": self.workspace_id,
            "tab_id": self.tab_id,
            "pane_npub": self.pane_npub,
            "identity_divergent": self.identity_divergent,
        })
    }
}

impl ToJson for ContactLink {
    fn to_json(&self) -> Value {
        json!({
            "project_slug": self.project_slug,
            "from_name": self.from_name,
            "from_pubkey": self.from_pubkey,
            "to_name": self.to_name,
            "to_pubkey": self.to_pubkey,
            "to_project": self.to_project,
            "status": self.status.as_str(),
            "created_ts_us": self.created_ts_us,
        })
    }
}

impl ToJson for DeliveryPayload {
    fn to_json(&self) -> Value {
        json!({ "recipient": self.recipient, "event_id": self.event_id })
    }
}

impl ToJson for Delivery {
    fn to_json(&self) -> Value {
        json!({ "project": self.project, "payload": self.payload.to_json() })
    }
}

impl ToJson for SendResult {
    fn to_json(&self) -> Value {
        json!({
            "deliveries": self.deliveries.to_json(),
            "count": self.count,
            "verified_sender": self.verified_sender,
        })
    }
}

impl ToJson for RequestReleaseResult {
    fn to_json(&self) -> Value {
        json!({
            "message_id": self.message_id,
            "delivered": self.delivered,
            "path_pattern": self.path_pattern,
            "holder": self.holder,
        })
    }
}

impl ToJson for TransferResult {
    fn to_json(&self) -> Value {
        json!({
            "path_pattern": self.path_pattern,
            "from_agent": self.from_agent,
            "to_agent": self.to_agent,
            "exclusive": self.exclusive,
            "reason": self.reason,
            "released_id": self.released_id,
        })
    }
}

impl ToJson for InboxMessage {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "sender": self.sender,
            "subject": self.subject,
            "body_md": self.body_md,
            "body": self.body_md,
            "importance": self.importance,
            "ack_required": self.ack_required,
            "thread_id": self.thread_id,
            "created_ts_us": self.created_ts_us,
            "read_ts_us": self.read_ts_us,
            "duplicate_of": self.duplicate_of,
            "corr": self.corr,
            "wake": self.wake,
            "priority": self.priority,
            "audience": self.audience,
            "result_path": self.result_path,
            "verdict": self.verdict,
        })
    }
}

impl ToJson for ReceiptResult {
    fn to_json(&self) -> Value {
        json!({
            "message_id": self.message_id,
            "acknowledged": self.acknowledged,
            "acknowledged_at_us": self.acknowledged_at_us,
            "read": self.read,
        })
    }
}

impl ToJson for GrantedReservation {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "path_pattern": self.path_pattern,
            "exclusive": self.exclusive,
            "reason": self.reason,
            "expires_ts_us": self.expires_ts_us,
        })
    }
}

impl ToJson for PathConflict {
    fn to_json(&self) -> Value {
        let mut out = json!({
            "path": self.path,
            "holders": self.holders,
            "reservation_ids": self.reservation_ids,
        });
        // bd-0uzrq.7: a loser must be able to prove which lease won, so the
        // fencing epochs ride the conflict when known. Omitted when empty
        // (older rows, older fixtures).
        if !self.epochs.is_empty()
            && let Some(obj) = out.as_object_mut()
        {
            obj.insert("epochs".into(), json!(self.epochs));
        }
        out
    }
}

impl ToJson for AcquireResult {
    fn to_json(&self) -> Value {
        json!({
            "granted": self.granted.to_json(),
            "conflicts": self.conflicts.to_json(),
            "soft_conflicts": self.soft_conflicts.to_json(),
        })
    }
}

impl ToJson for ConflictReport {
    fn to_json(&self) -> Value {
        json!({
            "conflict_free": self.conflict_free,
            "conflicts": self.conflicts.to_json(),
            "clear_paths": self.clear_paths,
            "checked_paths": self.checked_paths,
            "total_conflicting_reservations": self.total_conflicting_reservations,
            "soft_conflicts": self.soft_conflicts.to_json(),
            "has_soft_conflicts": self.has_soft_conflicts,
        })
    }
}

impl ToJson for RenewResult {
    fn to_json(&self) -> Value {
        json!({
            "renewed": self.renewed,
            "reservations": self.reservations.to_json(),
        })
    }
}

impl ToJson for ReleaseResult {
    fn to_json(&self) -> Value {
        json!({
            "released": self.released,
            "leases": self.leases.iter().map(|l| json!({
                "path": l.path,
                "mode": l.mode,
                "holder": l.holder,
            })).collect::<Vec<_>>(),
        })
    }
}

impl ToJson for SearchMessage {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "subject": self.subject,
            "from": self.from,
            "created_ts_us": self.created_ts_us,
            "importance": self.importance,
            "ack_required": self.ack_required,
            "thread_id": self.thread_id,
            "body_md": self.body_md,
        })
    }
}

impl ToJson for (String, SearchMessage) {
    fn to_json(&self) -> Value {
        json!({
            "project": self.0,
            "id": self.1.id,
            "subject": self.1.subject,
            "from": self.1.from,
            "created_ts_us": self.1.created_ts_us,
            "importance": self.1.importance,
            "ack_required": self.1.ack_required,
            "thread_id": self.1.thread_id,
            "body_md": self.1.body_md,
        })
    }
}

impl ToJson for (String, InboxMessage) {
    fn to_json(&self) -> Value {
        json!({
            "project": self.0,
            "id": self.1.id,
            "sender": self.1.sender,
            "subject": self.1.subject,
            "body_md": self.1.body_md,
            "body": self.1.body_md,
            "importance": self.1.importance,
            "ack_required": self.1.ack_required,
            "thread_id": self.1.thread_id,
            "created_ts_us": self.1.created_ts_us,
            "corr": self.1.corr,
            "wake": self.1.wake,
            "priority": self.1.priority,
            "audience": self.1.audience,
            "result_path": self.1.result_path,
            "verdict": self.1.verdict,
        })
    }
}

impl ToJson for ThreadSummary {
    fn to_json(&self) -> Value {
        json!({
            "thread_id": self.thread_id,
            "participants": self.participants,
            "key_points": self.key_points,
            "action_items": self.action_items,
            "message_count": self.message_count,
        })
    }
}

impl ToJson for ProductInfo {
    fn to_json(&self) -> Value {
        json!({
            "product_key": self.product_key,
            "name": self.name,
            "member_projects": self.member_projects,
            "created": self.created,
        })
    }
}

impl ToJson for GrantedSlot {
    fn to_json(&self) -> Value {
        json!({
            "slot": self.slot,
            "exclusive": self.exclusive,
            "expires_ts_us": self.expires_ts_us,
        })
    }
}

impl ToJson for SlotConflict {
    fn to_json(&self) -> Value {
        json!({ "slot": self.slot, "holders": self.holders })
    }
}

impl ToJson for SlotStatus {
    fn to_json(&self) -> Value {
        json!(self.as_str())
    }
}

impl ToJson for BuildSlot {
    fn to_json(&self) -> Value {
        json!({
            "slot": self.slot,
            "agent_name": self.agent_name,
            "agent_pubkey": self.agent_pubkey,
            "exclusive": self.exclusive,
            "status": self.status.to_json(),
            "expires_ts_us": self.expires_ts_us,
        })
    }
}

impl ToJson for AcquireBuildSlotResult {
    fn to_json(&self) -> Value {
        json!({
            "granted": self.granted.to_json(),
            "conflicts": self.conflicts.to_json(),
        })
    }
}

impl ToJson for StartSessionResult {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "agent_name": self.agent_name,
            "npub": self.npub,
            "granted_reservations": self.granted_reservations.to_json(),
            "inbox_count": self.inbox_count,
        })
    }
}

impl ToJson for PrepareThreadResult {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "agent_name": self.agent_name,
            "npub": self.npub,
            "thread_id": self.thread_id,
            "summary": self.summary.to_json(),
            "inbox_count": self.inbox_count,
        })
    }
}

impl ToJson for ContactHandshakeResult {
    fn to_json(&self) -> Value {
        json!({
            "requester": self.requester,
            "target": self.target,
            "link": self.link.to_json(),
            "welcome_sent": self.welcome_sent,
        })
    }
}

impl ToJson for ReservationCycleResult {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "agent_name": self.agent_name,
            "granted": self.granted.to_json(),
            "released": self.released,
        })
    }
}

impl ToJson for JudgmentOutcome {
    fn to_json(&self) -> Value {
        let r = &self.record;
        json!({
            "effect_key": r.effect_key,
            "pack_id": r.pack_id,
            "pack_version": r.pack_version,
            "model_id": r.model_id,
            "confidence": r.confidence,
            "usage": r.usage,
            "fallback_flag": r.fallback_flag,
            "escalated": r.escalated,
            "note": r.note,
            "answers": r.answers,
            "replayed": self.replayed,
        })
    }
}

#[cfg(test)]
mod to_json_tests {
    use super::ToJson;
    use crate::index::slots::{BuildSlot, GrantedSlot, SlotConflict, SlotStatus};
    use crate::mailbox::contacts::{ContactLink, LinkStatus};
    use crate::mailbox::judgment::{JudgmentEffectRecord, JudgmentOutcome};
    use crate::mailbox::reservations::ReleasedLease;
    use crate::mailbox::{
        AcquireBuildSlotResult, AcquireResult, ConflictReport, ContactHandshakeResult, Delivery,
        DeliveryPayload, GrantedReservation, HealthComponent, HealthReport, InboxMessage,
        PathConflict, PrepareThreadResult, ProductInfo, ProjectState, ReceiptResult,
        RegisteredAgent, ReleaseResult, RenewResult, RequestReleaseResult, ReservationCycleResult,
        SearchMessage, SendResult, StartSessionResult, ThreadSummary, TransferResult, WhoIs,
    };
    use serde_json::{Value, json};

    fn lease(path: &str) -> GrantedReservation {
        GrantedReservation {
            id: "id-1".to_string(),
            path_pattern: path.to_string(),
            exclusive: true,
            reason: "bd-1".to_string(),
            expires_ts_us: Some(7),
            epoch: 9,
        }
    }

    #[test]
    fn primitives_and_containers_keep_shape() {
        assert_eq!(true.to_json(), Value::Bool(true)); // bool
        assert_eq!("s".to_string().to_json(), json!("s")); // String
        assert_eq!(().to_json(), json!({ "ok": true })); // ()
        assert_eq!(vec![true, false].to_json(), json!([true, false])); // Vec<T>
        assert_eq!(Option::<bool>::None.to_json(), Value::Null); // Option<T> None
        assert_eq!(Some(true).to_json(), Value::Bool(true)); // Option<T> Some
    }

    #[test]
    fn health_and_project_shapes() {
        let c = HealthComponent {
            ok: true,
            message: "m".to_string(),
        };
        assert_eq!(c.to_json(), json!({ "ok": true, "message": "m" })); // HealthComponent
        let r = HealthReport {
            bus: c.clone(),
            archive: HealthComponent {
                ok: false,
                message: "x".to_string(),
            },
        };
        assert_eq!(r.to_json()["archive"]["ok"], json!(false)); // HealthReport
        let p = ProjectState {
            slug: "s".to_string(),
            manifest_event_id: "e".to_string(),
            created: true,
        };
        assert_eq!(p.to_json()["slug"], json!("s")); // ProjectState
    }

    #[test]
    fn identity_shapes() {
        let a = RegisteredAgent {
            name: "PaleHarbor".to_string(),
            project_slug: "p".to_string(),
            npub: "n".to_string(),
            profile_event_id: "e".to_string(),
            key_path: "k".to_string(),
            created: false,
        };
        assert_eq!(a.to_json()["name"], json!("PaleHarbor")); // RegisteredAgent
        let w = WhoIs {
            name: "PaleHarbor".to_string(),
            npub: "n".to_string(),
            program: None,
            model: None,
            task_description: None,
            last_active_ts_us: 1,
            stale: false,
            pane_id: None,
            workspace_id: None,
            tab_id: None,
            pane_npub: None,
            identity_divergent: false,
        };
        assert_eq!(w.to_json()["npub"], json!("n")); // WhoIs
        let c = ContactLink {
            project_slug: "p".to_string(),
            from_name: "a".to_string(),
            from_pubkey: "k1".to_string(),
            to_name: "b".to_string(),
            to_pubkey: "k2".to_string(),
            to_project: None,
            status: LinkStatus::Pending,
            created_ts_us: 3,
        };
        assert_eq!(c.to_json()["status"], json!("pending")); // ContactLink
    }

    #[test]
    fn mail_shapes() {
        let d = Delivery {
            project: "p".to_string(),
            payload: DeliveryPayload {
                recipient: "r".to_string(),
                event_id: "e".to_string(),
            },
        };
        assert_eq!(d.to_json()["payload"]["recipient"], json!("r")); // Delivery + DeliveryPayload
        let s = SendResult {
            deliveries: vec![d],
            count: 1,
            verified_sender: true,
        };
        assert_eq!(s.to_json()["count"], json!(1)); // SendResult
        let m = InboxMessage {
            id: "i".to_string(),
            sender: "s".to_string(),
            subject: "t".to_string(),
            body_md: "b".to_string(),
            importance: "high".to_string(),
            ack_required: true,
            thread_id: None,
            created_ts_us: 1,
            read_ts_us: None,
            duplicate_of: None,
            corr: None,
            wake: None,
            priority: None,
            audience: None,
            result_path: None,
            verdict: None,
            bead: Some("bd-1".to_string()),
        };
        let v = m.to_json();
        assert_eq!(v["body"], json!("b")); // InboxMessage (body mirrors body_md)
        assert!(v.get("bead").is_none()); // InboxMessage omits internal bead
        let r = ReceiptResult {
            message_id: "m".to_string(),
            acknowledged: true,
            acknowledged_at_us: Some(2),
            read: true,
        };
        assert_eq!(r.to_json()["acknowledged"], json!(true)); // ReceiptResult
        let s = SearchMessage {
            id: "i".to_string(),
            subject: "t".to_string(),
            from: "f".to_string(),
            created_ts_us: 1,
            importance: "high".to_string(),
            ack_required: false,
            thread_id: None,
            body_md: Some("b".to_string()),
        };
        assert_eq!(s.to_json()["from"], json!("f")); // SearchMessage
        let t = ThreadSummary {
            thread_id: "t".to_string(),
            participants: vec!["a".to_string()],
            key_points: vec![],
            action_items: vec![],
            message_count: 2,
        };
        assert_eq!(t.to_json()["message_count"], json!(2)); // ThreadSummary
        assert_eq!(("k".to_string(), s).to_json()["project"], json!("k")); // (String, SearchMessage)
        assert_eq!(("k".to_string(), m).to_json()["project"], json!("k")); // (String, InboxMessage)
    }

    #[test]
    fn reservation_shapes() {
        let g = lease("src/**");
        assert!(g.to_json().get("epoch").is_none()); // GrantedReservation omits internal epoch
        let p = PathConflict {
            path: "p".to_string(),
            holders: vec!["h".to_string()],
            reservation_ids: vec![],
            epochs: vec![],
        };
        assert_eq!(p.to_json()["holders"], json!(["h"])); // PathConflict
        assert!(
            p.to_json().get("epochs").is_none(),
            "epochs are omitted when empty"
        );
        let with_epoch = PathConflict {
            path: "p".to_string(),
            holders: vec!["h".to_string()],
            reservation_ids: vec!["id".to_string()],
            epochs: vec![7],
        };
        assert_eq!(
            with_epoch.to_json()["epochs"],
            json!([7]),
            "the winner's epoch rides the conflict (bd-0uzrq.7)"
        );
        let a = AcquireResult {
            granted: vec![g.clone()],
            conflicts: vec![],
            soft_conflicts: vec![],
            proposal: None,
        };
        assert_eq!(a.to_json()["granted"][0]["reason"], json!("bd-1")); // AcquireResult
        let c = ConflictReport {
            conflict_free: true,
            conflicts: vec![],
            clear_paths: vec!["c".to_string()],
            checked_paths: 1,
            total_conflicting_reservations: 0,
            soft_conflicts: vec![],
            has_soft_conflicts: false,
            slot_holders: vec![],
        };
        assert_eq!(c.to_json()["conflict_free"], json!(true)); // ConflictReport
        let r = RenewResult {
            renewed: 2,
            reservations: vec![],
        };
        assert_eq!(r.to_json()["renewed"], json!(2)); // RenewResult
        let l = ReleasedLease {
            path: "p".to_string(),
            mode: "exclusive".to_string(),
            holder: "h".to_string(),
        };
        let r = ReleaseResult {
            released: 1,
            leases: vec![l],
        };
        assert_eq!(r.to_json()["leases"][0]["holder"], json!("h")); // ReleaseResult
        let r = RequestReleaseResult {
            message_id: "m".to_string(),
            delivered: true,
            path_pattern: "p".to_string(),
            holder: "h".to_string(),
        };
        assert_eq!(r.to_json()["delivered"], json!(true)); // RequestReleaseResult
        let r = TransferResult {
            path_pattern: "p".to_string(),
            from_agent: "a".to_string(),
            to_agent: "b".to_string(),
            exclusive: false,
            reason: "bd-1".to_string(),
            released_id: Some("r".to_string()),
        };
        assert_eq!(r.to_json()["to_agent"], json!("b")); // TransferResult
    }

    #[test]
    fn macro_product_slot_judgment_shapes() {
        let s = StartSessionResult {
            project_key: "p".to_string(),
            agent_name: "a".to_string(),
            npub: "n".to_string(),
            granted_reservations: vec![],
            inbox_count: 3,
        };
        assert_eq!(s.to_json()["inbox_count"], json!(3)); // StartSessionResult
        let t = ThreadSummary {
            thread_id: "t".to_string(),
            participants: vec![],
            key_points: vec![],
            action_items: vec![],
            message_count: 0,
        };
        let p = PrepareThreadResult {
            project_key: "p".to_string(),
            agent_name: "a".to_string(),
            npub: "n".to_string(),
            thread_id: "t".to_string(),
            summary: t,
            inbox_count: 0,
        };
        assert_eq!(p.to_json()["thread_id"], json!("t")); // PrepareThreadResult
        let i = ProductInfo {
            product_key: "k".to_string(),
            name: "n".to_string(),
            member_projects: vec!["p".to_string()],
            created: true,
        };
        assert_eq!(i.to_json()["member_projects"], json!(["p"])); // ProductInfo
        let g = GrantedSlot {
            slot: "cargo".to_string(),
            exclusive: true,
            expires_ts_us: None,
        };
        assert_eq!(g.to_json()["slot"], json!("cargo")); // GrantedSlot
        let c = SlotConflict {
            slot: "cargo".to_string(),
            holders: vec!["h".to_string()],
        };
        assert_eq!(c.to_json()["holders"], json!(["h"])); // SlotConflict
        assert_eq!(SlotStatus::Active.to_json(), json!("active")); // SlotStatus
        let b = BuildSlot {
            slot: "cargo".to_string(),
            agent_name: "a".to_string(),
            agent_pubkey: "k".to_string(),
            exclusive: true,
            status: SlotStatus::Released,
            expires_ts_us: Some(5),
        };
        assert_eq!(b.to_json()["status"], json!("released")); // BuildSlot
        let a = AcquireBuildSlotResult {
            granted: vec![g],
            conflicts: vec![c],
        };
        assert_eq!(a.to_json()["conflicts"][0]["slot"], json!("cargo")); // AcquireBuildSlotResult
        let h = ContactHandshakeResult {
            requester: "a".to_string(),
            target: "b".to_string(),
            link: Some(ContactLink {
                project_slug: "p".to_string(),
                from_name: "a".to_string(),
                from_pubkey: "k1".to_string(),
                to_name: "b".to_string(),
                to_pubkey: "k2".to_string(),
                to_project: None,
                status: LinkStatus::Approved,
                created_ts_us: 0,
            }),
            welcome_sent: true,
        };
        assert_eq!(h.to_json()["welcome_sent"], json!(true)); // ContactHandshakeResult
        let r = ReservationCycleResult {
            project_key: "p".to_string(),
            agent_name: "a".to_string(),
            granted: vec![lease("x")],
            released: 1,
        };
        assert_eq!(r.to_json()["released"], json!(1)); // ReservationCycleResult
        let o = JudgmentOutcome {
            record: JudgmentEffectRecord {
                effect_key: "e".to_string(),
                pack_id: "p".to_string(),
                pack_version: 1,
                model_id: "m".to_string(),
                confidence: 0.5,
                usage: "u".to_string(),
                fallback_flag: false,
                escalated: false,
                note: "n".to_string(),
                answers: json!({}),
                state_hash: String::new(),
            },
            replayed: false,
        };
        assert_eq!(o.to_json()["pack_id"], json!("p")); // JudgmentOutcome
    }
}
