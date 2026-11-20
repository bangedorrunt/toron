// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! Pure fact → catalogue-event classifier and the six-name catalogue.

use serde::Serialize;
use serde_json::json;

/// The six event names U1 advertises (R4); `gap` is synthetic per R11.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventName {
    MailAddressed,
    ReservationConflict,
    AckSlaExpired,
    RoleChanged,
    PolicyChanged,
    Gap,
}

impl EventName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MailAddressed => "mail.addressed",
            Self::ReservationConflict => "reservation.conflict",
            Self::AckSlaExpired => "ack_sla_expired",
            Self::RoleChanged => "role.changed",
            Self::PolicyChanged => "policy.changed",
            Self::Gap => "gap",
        }
    }
}

/// One catalogue row served by `events/list`. Payload is HINT-ONLY (R5): an
/// id, name, project, sender pin, correlation id and timestamp — never a body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CatalogueEvent {
    pub name: &'static str,
    pub description: &'static str,
    pub arg_schema: serde_json::Value,
    pub payload_schema: serde_json::Value,
}

fn hint_payload() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "event_id": {"type": "string"},
            "name": {"type": "string"},
            "project": {"type": "string"},
            "sender_pin": {"type": "string"},
            "correlation_id": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"}
        },
        "additionalProperties": false
    })
}

pub fn catalogue() -> Vec<CatalogueEvent> {
    vec![
        CatalogueEvent {
            name: "mail.addressed",
            description: "A gift-wrapped or addressed post resolved to your pin",
            arg_schema: json!({"type": "object", "properties": {"project": {"type": "string"}}}),
            payload_schema: json!({
                "type": "object",
                "properties": {
                    "event_id": {"type": "string"},
                    "name": {"type": "string"},
                    "project": {"type": "string"},
                    "sender_pin": {"type": "string"},
                    "correlation_id": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"}
                },
                "additionalProperties": false
            }),
        },
        CatalogueEvent {
            name: "reservation.conflict",
            description: "An acquire refusal, request-release or revocation touching your leases",
            arg_schema: json!({"type": "object", "properties": {"project": {"type": "string"}}}),
            payload_schema: hint_payload(),
        },
        CatalogueEvent {
            name: "ack_sla_expired",
            description: "An ack-required post you left past its SLA",
            arg_schema: json!({"type": "object", "properties": {"project": {"type": "string"}}}),
            payload_schema: hint_payload(),
        },
        CatalogueEvent {
            name: "role.changed",
            description: "A 30013 role binding for your pin was added or removed",
            arg_schema: json!({"type": "object", "properties": {"project": {"type": "string"}}}),
            payload_schema: hint_payload(),
        },
        CatalogueEvent {
            name: "policy.changed",
            description: "A contact policy on your pin changed",
            arg_schema: json!({"type": "object", "properties": {"project": {"type": "string"}}}),
            payload_schema: hint_payload(),
        },
        CatalogueEvent {
            name: "gap",
            description: "Events were shed; resume from the cursor in the hint",
            arg_schema: json!({"type": "object", "properties": {"resync_from": {"type": "string"}}}),
            payload_schema: hint_payload(),
        },
    ]
}

/// A bus fact reduced to the fields the classifier needs (no Nostr types here).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fact {
    pub kind: u16,
    pub toron_kind: Option<String>,
    pub addressed_pin: Option<String>,
    pub role_change: bool,
    pub policy_change: bool,
}

/// Pure classifier. A mail fact maps only to the subscriber it addresses;
/// chatter with no addressing maps to nothing.
pub fn classify(fact: &Fact, subscriber_pin: &str) -> Option<EventName> {
    if fact.toron_kind.as_deref() == Some("ack-sla-expired") {
        return Some(EventName::AckSlaExpired);
    }
    if matches!(
        fact.toron_kind.as_deref(),
        Some("request-release" | "reservation-revoked" | "reservation-conflict")
    ) {
        return Some(EventName::ReservationConflict);
    }
    if fact.role_change {
        return Some(EventName::RoleChanged);
    }
    if fact.policy_change {
        return Some(EventName::PolicyChanged);
    }
    if matches!(fact.kind, 9 | 1059) && fact.addressed_pin.as_deref() == Some(subscriber_pin) {
        return Some(EventName::MailAddressed);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_has_six_valid_names() {
        let rows = catalogue();
        assert_eq!(rows.len(), 6);
        let mut seen = std::collections::BTreeSet::new();
        for row in rows {
            assert!(seen.insert(row.name), "duplicate event name {row:?}");
            assert!(!row.name.is_empty(), "{row:?}");
            assert_eq!(row.arg_schema["type"], "object");
            assert_eq!(row.payload_schema["type"], "object");
            let keys = row.payload_schema["properties"]
                .as_object()
                .expect("properties object");
            assert!(keys.contains_key("event_id"));
            assert!(!keys.contains_key("body"));
            assert!(!keys.contains_key("subject"));
        }
    }

    #[test]
    fn addressed_mail_maps_only_to_its_recipient() {
        let fact = Fact {
            kind: 1059,
            addressed_pin: Some("PinA".into()),
            ..Fact::default()
        };
        assert_eq!(classify(&fact, "PinA"), Some(EventName::MailAddressed));
        assert_eq!(classify(&fact, "PinB"), None);
    }

    #[test]
    fn sla_and_chatter_classify_as_specified() {
        let sla = Fact {
            kind: 9,
            toron_kind: Some("ack-sla-expired".into()),
            ..Fact::default()
        };
        assert_eq!(classify(&sla, "PinA"), Some(EventName::AckSlaExpired));
        let chatter = Fact {
            kind: 9,
            ..Fact::default()
        };
        assert_eq!(classify(&chatter, "PinA"), None);
    }
}
