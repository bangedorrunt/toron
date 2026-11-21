// governed-by: ADR-0039
//! bd-6zo01 G1/G2: the one boundary converter from toron's internal hint to
//! the SEP-3415 Final `EventOccurrence`: `{eventId, name, timestamp, data,
//! cursor?, _meta?}`. Draft names (`event_id`, numeric `timestamp`, flat hint
//! fields) never cross the wire. Pure: a wrong mapping is a failed unit test.

use serde_json::{Value, json};

/// Microseconds since epoch to an ISO-8601 string (Final requires a string).
pub fn iso8601(us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(us)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

fn string(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Convert one stored hint into a Final occurrence. `seq` is the delivery's
/// position and supplies both the dedup id fallback and the safe cursor.
pub fn occurrence(payload: &str, seq: u64, now_us: i64) -> Value {
    let hint: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    let event_id = string(hint.get("event_id"))
        .or_else(|| string(hint.get("eventId")))
        .unwrap_or_else(|| format!("seq-{seq}"));
    let name = string(hint.get("name")).unwrap_or_else(|| "unknown".to_string());
    let ts_us = hint
        .get("timestamp")
        .and_then(Value::as_i64)
        .unwrap_or(now_us);
    let mut data = json!({});
    if let Some(v) = string(hint.get("project")) {
        data["project"] = json!(v);
    }
    if let Some(v) = string(hint.get("sender_pin")) {
        data["senderPin"] = json!(v);
    }
    if let Some(v) = string(hint.get("correlation_id")) {
        data["correlationId"] = json!(v);
    }
    json!({
        "eventId": event_id,
        "name": name,
        "timestamp": iso8601(ts_us),
        "data": data,
        "cursor": seq.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hint_becomes_a_final_occurrence_with_no_draft_keys() {
        let hint = json!({
            "event_id": "evt-7", "name": "mail.addressed",
            "project": "flywheel", "sender_pin": "BoldCliff",
            "correlation_id": "c-9", "timestamp": 1_700_000_000_000_000i64,
        })
        .to_string();
        let occ = occurrence(&hint, 42, 0);
        assert_eq!(occ["eventId"], "evt-7");
        assert_eq!(occ["name"], "mail.addressed");
        assert_eq!(occ["timestamp"], "2023-11-14T22:13:20.000Z");
        assert_eq!(occ["data"]["project"], "flywheel");
        assert_eq!(occ["data"]["senderPin"], "BoldCliff");
        assert_eq!(occ["data"]["correlationId"], "c-9");
        assert_eq!(occ["cursor"], "42");
        let s = occ.to_string();
        assert!(!s.contains("event_id") && !s.contains("sender_pin"));
        assert!(!s.contains("correlation_id") && !s.contains("\"timestamp\":17"));
    }

    #[test]
    fn a_bad_or_empty_hint_still_yields_a_valid_occurrence() {
        let occ = occurrence("not json", 9, 0);
        assert_eq!(occ["eventId"], "seq-9");
        assert_eq!(occ["name"], "unknown");
        assert_eq!(occ["data"], json!({}));
        assert_eq!(occ["timestamp"], "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn a_final_shaped_occurrence_is_not_double_converted() {
        let final_occ = json!({
            "eventId": "evt-x", "name": "gap",
            "timestamp": "2026-01-01T00:00:00Z", "data": {}, "cursor": "3",
        })
        .to_string();
        let occ = occurrence(&final_occ, 10, 0);
        assert_eq!(occ["eventId"], "evt-x");
        assert_eq!(
            occ["timestamp"], "1970-01-01T00:00:00.000Z",
            "string ts is not a micros int"
        );
    }
}
