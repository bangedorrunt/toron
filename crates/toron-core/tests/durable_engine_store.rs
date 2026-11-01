// governed-by: ADR-0005
//! Integration witness: the elm-effect durable engine over toron's SQLite
//! journal and engine store. Start, park, restart, wake.
//!
//! The engine is synchronous; the test thread is not a Tokio worker, and the
//! bridge tasks live on the runtime. This is the same threading contract as
//! `ToronJournal` and the comms loop.

use toron_core::index::durable_engine::ToronEngineStore;
use toron_core::index::schema::{connect, migrate};
use toron_core::journal::{JournalDb, ToronJournal};
use toron_core::kernel::{
    DurableEngine, EngineMsg, OurEngine, ParkOutcome, RunPhase, StartOutcome,
};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "toron-durable-engine-{tag}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn engine_store_round_trips_start_park_restart_and_wake() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let dir = unique_dir("store");
    std::fs::create_dir_all(&dir).expect("temp dir");
    let db = dir.join("engine.sqlite");
    let pool = rt
        .block_on(async {
            let pool = connect(db.to_str().expect("path")).await?;
            migrate(&pool).await?;
            anyhow::Ok(pool)
        })
        .expect("sqlite pool + migrations");

    let journal = ToronJournal::spawn(
        JournalDb::new(pool.clone(), "durable-engine-test", 64),
        rt.handle(),
    );
    let store = ToronEngineStore::spawn(pool.clone(), rt.handle());
    let mut engine = OurEngine::with_journal(store.clone(), journal.clone());

    let started = engine.start("run-1", 0).expect("start");
    assert!(matches!(started, StartOutcome::Started { .. }));

    let parked = engine
        .park_until("run-1", 5_000, 60, 0, None)
        .expect("park");
    assert!(matches!(parked, ParkOutcome::Wait { .. }));
    let parks = engine.parked().expect("parked");
    assert_eq!(parks.len(), 1);
    assert_eq!(parks[0].until_ms, 5_000);
    assert_eq!(
        engine.query("run-1").expect("query").phase,
        RunPhase::Parked
    );

    // Crash: drop the engine and both handles, then rebuild on the same DB.
    drop(engine);
    let journal = ToronJournal::spawn(
        JournalDb::new(pool.clone(), "durable-engine-test", 64),
        rt.handle(),
    );
    let store = ToronEngineStore::spawn(pool.clone(), rt.handle());
    let mut recovered = OurEngine::with_journal(store, journal);

    assert_eq!(
        recovered.query("run-1").expect("query").phase,
        RunPhase::Parked,
        "the parked row and journal intent survive the restart"
    );
    let msgs = recovered.drain_due(6_000).expect("drain");
    assert_eq!(
        msgs,
        vec![EngineMsg::Woke {
            key: "run-1".into(),
            overdue_ms: 1_000,
            park_generation: 1,
        }]
    );
    assert_eq!(
        recovered.query("run-1").expect("query").phase,
        RunPhase::Running
    );

    drop(recovered);
    drop(pool);
    drop(rt);
    let _ = std::fs::remove_dir_all(dir);
}
