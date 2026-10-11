// governed-by: ADR-0041
//! Replays the Lean reference model's committed vectors (`proto/vectors/`)
//! against the Rust implementation: `DeliveryFsm::step` for every
//! (state, event) pair, and the real SQLite store's L16 append for every
//! log trace. Rust CI needs no Lean: the vectors are committed and the
//! `lean` CI job checks they are fresh (`lake exe proto vectors --check`).

use toron_core::delivery::{DeliveryEvent, DeliveryFsm, DeliveryState};
use toron_core::inbox::{InboxStore, JOURNAL_STREAM};
use toron_core::index::schema;
use toron_core::kernel::{AppendError, durable_log_laws};

fn vectors(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../proto/vectors")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn delivery_vectors_from_lean_pass_in_rust() {
    let mut n = 0;
    for (i, line) in vectors("delivery.jsonl").lines().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        let state: DeliveryState = serde_json::from_value(v["state"].clone()).unwrap();
        let event: DeliveryEvent = serde_json::from_value(v["event"].clone()).unwrap();
        let got = DeliveryFsm::step(&state, &event);
        let expect = &v["expect"];
        match (got, expect.get("ok"), expect.get("reject")) {
            (Ok(s), Some(want), None) => {
                let want: DeliveryState = serde_json::from_value(want.clone()).unwrap();
                assert_eq!(s, want, "vector {i}: {line}");
            }
            (Err(r), None, Some(tag)) => {
                let got_tag = serde_json::to_value(&r).unwrap()["reject"].clone();
                assert_eq!(&got_tag, tag, "vector {i}: {line} (rust said {r})");
            }
            (got, _, _) => panic!("vector {i}: rust {got:?}, lean {expect}: {line}"),
        }
        n += 1;
    }
    assert_eq!(
        n, 2160,
        "the committed vector count changed; regenerate deliberately"
    );
}

/// Negative control: a corrupted expectation is caught by the same replay.
#[test]
fn a_wrong_vector_is_caught() {
    let line = vectors("delivery.jsonl")
        .lines()
        .next()
        .unwrap()
        .replace("\"phase\":\"sent\"", "\"phase\":\"delivered\"");
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    let state: DeliveryState = serde_json::from_value(v["state"].clone()).unwrap();
    let event: DeliveryEvent = serde_json::from_value(v["event"].clone()).unwrap();
    let want: DeliveryState = serde_json::from_value(v["expect"]["ok"].clone()).unwrap();
    assert_ne!(DeliveryFsm::step(&state, &event).unwrap(), want);
}

#[tokio::test]
async fn log_traces_from_lean_pass_on_the_real_store() {
    let mut n = 0;
    for (i, line) in vectors("log_traces.jsonl").lines().enumerate() {
        let path = std::env::temp_dir().join(format!(
            "toron-proto-trace-{i}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(path.to_str().unwrap()).await.unwrap();
        schema::migrate(&pool).await.unwrap();
        let store = InboxStore::new(pool);
        let steps: Vec<serde_json::Value> = serde_json::from_str(line).unwrap();
        for step in steps {
            let t = &step["append"];
            let env = durable_log_laws::envelope(
                t["producer"].as_str().unwrap(),
                t["epoch"].as_u64().unwrap(),
                t["seq"].as_u64().unwrap(),
                "{}",
            );
            let got = store.append_json(JOURNAL_STREAM, &env).await;
            match (
                got,
                step["expect"].get("ok"),
                step["expect"].get("err").and_then(|e| e.as_str()),
            ) {
                (Ok(a), Some(off), None) => {
                    assert_eq!(a.offset, off.as_u64().unwrap(), "trace {i}: {line}")
                }
                (Err(AppendError::StaleEpoch { .. }), None, Some("stale_epoch")) => {}
                (Err(AppendError::SeqGap { .. }), None, Some("seq_gap")) => {}
                (got, _, _) => panic!("trace {i}: rust {got:?}, lean {}: {line}", step["expect"]),
            }
        }
        let _ = std::fs::remove_file(&path);
        n += 1;
    }
    assert_eq!(n, 125);
}
