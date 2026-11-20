// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! Pure fact → catalogue-event classifier and the SEP-3415 six-name catalogue.

use serde::Serialize;
use serde_json::{Value, json};

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

    fn description(self) -> &'static str {
        match self {
            Self::MailAddressed => "A gift-wrapped or addressed post resolved to your pin",
            Self::ReservationConflict => {
                "An acquire refusal, request-release or revocation touching your leases"
            }
            Self::AckSlaExpired => "An ack-required post you left past its SLA",
            Self::RoleChanged => "A 30013 role binding for your pin was added or removed",
            Self::PolicyChanged => "A contact policy on your pin changed",
            Self::Gap => "Events were shed; resume from the cursor in the hint",
        }
    }
}

const NAMES: [EventName; 6] = [
    EventName::MailAddressed,
    EventName::ReservationConflict,
    EventName::AckSlaExpired,
    EventName::RoleChanged,
    EventName::PolicyChanged,
    EventName::Gap,
];

/// `events/list` advertises poll and push in U1; webhook lands in P2.
const DELIVERY: &[&str] = &["poll", "push"];

/// One SEP-3415 `events/list` row. The wire keys are camelCase.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogueEvent {
    pub name: &'static str,
    pub description: &'static str,
    pub delivery: &'static [&'static str],
    pub input_schema: Value,
    pub payload_schema: Value,
}

/// The event's OWN arguments (SEP-3415), not the subscribe envelope: each
/// catalogue fact is addressed by its project.
fn input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "project": {"type": "string"}
        },
        "required": ["project"]
    })
}

/// HINT-ONLY (R5): an id, name, project, sender pin, correlation id and
/// timestamp — never a body or a subject.
fn hint_payload() -> Value {
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
    NAMES
        .into_iter()
        .map(|name| CatalogueEvent {
            name: name.as_str(),
            description: name.description(),
            delivery: DELIVERY,
            input_schema: input_schema(),
            payload_schema: hint_payload(),
        })
        .collect()
}

/// A bus fact reduced to the fields the classifier needs (no Nostr types).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fact {
    /// The bus event's OWN unique id — D8 bridges and doorbell dedupe by it.
    pub id: String,
    pub kind: u16,
    pub toron_kind: Option<String>,
    /// The pin this fact concerns: recipient, lease holder or bound pin.
    pub subject_pin: Option<String>,
    pub project: Option<String>,
    pub role_change: bool,
    pub policy_change: bool,
}

/// Pure classifier. D6: every event is own-pin only — a fact about somebody
/// else's mail, leases, role or policy maps to nothing for this subscriber.
pub fn classify(fact: &Fact, subscriber_pin: &str) -> Option<EventName> {
    if fact.subject_pin.as_deref() != Some(subscriber_pin) {
        return None;
    }
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
    if matches!(fact.kind, 9 | 1059) {
        return Some(EventName::MailAddressed);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_rows_use_sep3415_wire_keys() {
        let rows = catalogue();
        assert_eq!(rows.len(), 6);
        let value = serde_json::to_value(&rows).expect("serialize catalogue");
        let mut seen = std::collections::BTreeSet::new();
        for row in value.as_array().expect("array") {
            let mut keys: Vec<&str> = row
                .as_object()
                .expect("object")
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                [
                    "delivery",
                    "description",
                    "inputSchema",
                    "name",
                    "payloadSchema"
                ],
                "exact SEP-3415 row keys"
            );
            let name = row["name"].as_str().expect("name");
            assert!(seen.insert(name), "duplicate event name {name}");
            assert_eq!(row["delivery"], json!(["poll", "push"]));
            assert_eq!(row["inputSchema"]["properties"]["project"]["type"], "string");
            assert_eq!(row["payloadSchema"]["type"], "object");
            let props = row["payloadSchema"]["properties"].as_object().unwrap();
            assert!(props.contains_key("event_id"));
            assert!(!props.contains_key("body"), "hint-only payload (R5)");
            assert!(!props.contains_key("subject"));
        }
    }

    #[test]
    fn every_event_kind_requires_the_subscribers_own_subject() {
        let cases = [
            (
                Fact {
                    kind: 9,
                    toron_kind: Some("ack-sla-expired".into()),
                    ..Fact::default()
                },
                EventName::AckSlaExpired,
            ),
            (
                Fact {
                    kind: 9,
                    toron_kind: Some("request-release".into()),
                    ..Fact::default()
                },
                EventName::ReservationConflict,
            ),
            (
                Fact {
                    kind: 30013,
                    role_change: true,
                    ..Fact::default()
                },
                EventName::RoleChanged,
            ),
            (
                Fact {
                    kind: 30014,
                    policy_change: true,
                    ..Fact::default()
                },
                EventName::PolicyChanged,
            ),
            (
                Fact {
                    kind: 1059,
                    ..Fact::default()
                },
                EventName::MailAddressed,
            ),
        ];
        for (mut fact, expected) in cases {
            fact.subject_pin = Some("PinA".into());
            assert_eq!(classify(&fact, "PinA"), Some(expected), "{fact:?}");
            assert_eq!(classify(&fact, "PinB"), None, "other pin: {fact:?}");
            fact.subject_pin = Some("PinB".into());
            assert_eq!(classify(&fact, "PinA"), None, "other subject: {fact:?}");
            fact.subject_pin = None;
            assert_eq!(classify(&fact, "PinA"), None, "no subject: {fact:?}");
        }
    }

    #[test]
    fn chatter_with_no_subject_maps_to_nothing() {
        let chatter = Fact {
            kind: 9,
            ..Fact::default()
        };
        assert_eq!(classify(&chatter, "PinA"), None);
    }
}
