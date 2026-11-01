// governed-by: ADR-0005
//! `WorkflowStateStore`: save, load, and delete one run's model JSON.

use toron_core::index::durable_workflow::WorkflowStateStore;
use toron_core::index::schema::{connect, migrate};

fn unique_db(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "toron-durable-workflow-{tag}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

#[tokio::test]
async fn state_round_trips_and_deletes() {
    let path = unique_db("store");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let store = WorkflowStateStore::new(pool.clone());

    assert!(store.load("run-1").await.expect("load empty").is_none());
    store
        .save("run-1", r#"{"phase":"running","step_index":1}"#)
        .await
        .expect("save");
    assert_eq!(
        store.load("run-1").await.expect("load"),
        Some(r#"{"phase":"running","step_index":1}"#.to_string())
    );

    store
        .save("run-1", r#"{"phase":"succeeded","step_index":2}"#)
        .await
        .expect("replace");
    assert_eq!(
        store.load("run-1").await.expect("load replaced"),
        Some(r#"{"phase":"succeeded","step_index":2}"#.to_string())
    );

    store.delete("run-1").await.expect("delete");
    assert!(store.load("run-1").await.expect("load deleted").is_none());

    drop(store);
    drop(pool);
    let _ = std::fs::remove_file(path);
}
