// governed-by: ADR-0003
//! bd-reserve-yield-taqf: the yield suppression contract, end to end.
//!
//! Measured friction: the loop re-resumed the same in-progress bead every pass
//! while a live peer held its lane, and every resumed pane answered with a
//! stand-down comment commit — dozens of commits for one fact. One structured
//! yield marker replaces N commits, and it has to do three things:
//!
//! 1. two yields for the same (bead, holder) collapse to ONE marker;
//! 2. the loop skips the bead while the holder's lease is live;
//! 3. the marker clears when the holder releases.
//!
//! (3) is why suppression is DERIVED, not stored: the marker plus the live
//! lease board are the two halves of the rule, so a released holder stops
//! suppressing with no delete path to drift out of sync. The marker itself
//! survives, inert — it is a record of what happened, not a lock.

use std::path::PathBuf;
use std::sync::Arc;

use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "yield-fixture";
/// NOT shaped like a tracker id on purpose: the commit guard charges every
/// bead token the staged diff adds, and a fixture is not a bead.
const BEAD: &str = "lane-held-bead";
const OTHER: &str = "lane-free-bead";

async fn test_mailbox(tag: &str) -> (Mailbox, PathBuf, PathBuf) {
    let db_path = std::env::temp_dir().join(format!(
        "toron-reserve-yield-{tag}-{}-{}.sqlite",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(db_path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let data_dir = std::env::temp_dir().join(format!(
        "toron-reserve-yield-data-{tag}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&data_dir).expect("mkdir");
    let mailbox = Mailbox::new(
        Arc::new(bus),
        data_dir.clone(),
        Some(toron_core::keys::generate()),
    )
    .with_agent_passphrase("reserve-yield-test")
    .with_index_pool(pool);
    (mailbox, db_path, data_dir)
}

#[tokio::test]
async fn reserve_yield_suppresses_resume() {
    let (mailbox, db_path, data_dir) = test_mailbox("suppress").await;
    for name in ["Holder", "Yielder"] {
        mailbox
            .register_agent(PROJECT, &AgentProfile::new(name))
            .await
            .expect("register");
    }

    // The holder takes the lane. A live lease is the other half of the rule.
    mailbox
        .file_reservation_paths(
            PROJECT,
            "Holder",
            &["src/**".to_string()],
            Some(3600),
            true,
            Some(BEAD),
        )
        .await
        .expect("Holder acquires the lane");

    // No marker yet: nothing is suppressed, even with the lane taken.
    assert!(
        mailbox
            .suppressed_resume_beads(PROJECT)
            .await
            .expect("read")
            .is_empty(),
        "a live lease alone suppresses nothing — the yield is the missing half"
    );

    // (2) one yield suppresses the bead while the holder's lease is live.
    mailbox
        .record_reservation_yield(PROJECT, "Yielder", BEAD, "Holder", "handing the lane back")
        .await
        .expect("record");
    assert_eq!(
        mailbox
            .suppressed_resume_beads(PROJECT)
            .await
            .expect("read"),
        vec![BEAD.to_string()],
        "a yield naming a live holder suppresses that bead"
    );

    // A yield for a bead nobody yielded claims nothing.
    mailbox
        .record_reservation_yield(PROJECT, "Yielder", OTHER, "GhostHolder", "not a holder")
        .await
        .expect("record");
    assert_eq!(
        mailbox
            .suppressed_resume_beads(PROJECT)
            .await
            .expect("read"),
        vec![BEAD.to_string()],
        "an unresolvable holder suppresses nothing (no live lease to join)"
    );

    // (1) a second yield for the same (bead, holder) collapses to one marker.
    mailbox
        .record_reservation_yield(PROJECT, "Yielder", BEAD, "Holder", "saying it twice")
        .await
        .expect("record again");
    let markers = mailbox
        .list_reservation_yields(PROJECT)
        .await
        .expect("list");
    assert_eq!(markers.len(), 2, "two (bead, holder) pairs, two markers");
    let held = markers
        .iter()
        .find(|m| m.bead_id == BEAD)
        .expect("the yielded bead");
    assert_eq!(
        held.note, "saying it twice",
        "the repeat refreshes the note instead of adding a row"
    );

    // The board the loop reads resolves holder liveness in the same call.
    let (views, suppressed) = mailbox
        .reservation_yield_board(PROJECT)
        .await
        .expect("board");
    assert_eq!(views.len(), 2);
    assert!(
        views
            .iter()
            .find(|v| v.bead_id == BEAD)
            .expect("row")
            .holder_live,
        "the held bead's holder reads live"
    );
    assert!(
        !views
            .iter()
            .find(|v| v.bead_id == OTHER)
            .expect("row")
            .holder_live,
        "the ghost holder does not"
    );
    assert_eq!(suppressed, vec![BEAD.to_string()]);

    // (3) the holder releases: the marker clears without any delete path.
    mailbox
        .release_file_reservations(PROJECT, "Holder", &["src/**".to_string()])
        .await
        .expect("release");
    assert!(
        mailbox
            .suppressed_resume_beads(PROJECT)
            .await
            .expect("read")
            .is_empty(),
        "a released holder suppresses nothing — the marker clears itself"
    );
    assert_eq!(
        mailbox
            .list_reservation_yields(PROJECT)
            .await
            .expect("list")
            .len(),
        2,
        "the marker is an inert record after the release, never deleted"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_file(&db_path);
}
