// governed-by: ADR-0005
//! `WorkflowStateStore`: save, load, and delete one run's model JSON.

use toron_core::index::durable_workflow::{RunState, SyncWorkflowState, WorkflowStateStore};
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

async fn on_blocking<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.expect("blocking task")
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_handle_round_trips_and_deletes() {
    let path = unique_db("sync");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());

    on_blocking({
        let state = state.clone();
        move || state.save("run-sync", r#"{"phase":"running","step_index":1}"#)
    })
    .await
    .expect("save");

    let loaded = on_blocking({
        let state = state.clone();
        move || state.load("run-sync")
    })
    .await
    .expect("load");
    assert_eq!(
        loaded,
        Some(r#"{"phase":"running","step_index":1}"#.to_string())
    );

    on_blocking({
        let state = state.clone();
        move || state.save("run-sync", r#"{"phase":"succeeded","step_index":2}"#)
    })
    .await
    .expect("replace");

    let replaced = on_blocking({
        let state = state.clone();
        move || state.load("run-sync")
    })
    .await
    .expect("load replaced");
    assert_eq!(
        replaced,
        Some(r#"{"phase":"succeeded","step_index":2}"#.to_string())
    );

    on_blocking({
        let state = state.clone();
        move || state.delete("run-sync")
    })
    .await
    .expect("delete");

    let gone = on_blocking({
        let state = state.clone();
        move || state.load("run-sync")
    })
    .await
    .expect("load deleted");
    assert!(gone.is_none());

    drop(state);
    drop(pool);
    let _ = std::fs::remove_file(path);
}
