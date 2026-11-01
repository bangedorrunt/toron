// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Host-driven cron scheduler over the framework cron kernel and `OurEngine`.
//!
//! `cron.rs` still owns the Resonate durable-sleep manager. This is the
//! replacement path: the host calls [`ElmCron::tick`] at its cadence, the
//! latest due tick becomes one idempotent `OurEngine::start` under
//! `fire_key(name, tick_ms)`, and the host runs the tick body. Registration,
//! pause/resume, `Skip` catch-up, and the body stay with the host.
//!
//! No clock: the caller passes `now_ms`. Catch-up is the one-tick `Latest`
//! policy; a host that wants `Skip` advances `last_fired_ms` without calling
//! `tick`, or compares the returned tick to its host window.

use std::collections::BTreeMap;

use toron_core::kernel::{
    CronError, CronSchedule, DurableEngine, EngineError, EngineStorage, InMemoryStore,
    JournalStore, MemStorage, OurEngine, StartOutcome, fire_key,
};

/// One schedule the host registered.
struct CronEntry {
    name: String,
    schedule: CronSchedule,
    last_fired_ms: u64,
    enabled: bool,
}

/// One firing the host must turn into a tick body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronFire {
    /// Schedule name.
    pub name: String,
    /// Absolute tick that fired.
    pub tick_ms: u64,
    /// Engine run key (`cron:{name}:{tick_ms}`).
    pub key: String,
    /// Whether the engine started it or re-attached.
    pub outcome: StartOutcome,
}

/// Host-driven cron scheduler over `OurEngine`.
pub struct ElmCron<S: EngineStorage = MemStorage, J: JournalStore = InMemoryStore> {
    engine: OurEngine<S, J>,
    entries: BTreeMap<String, CronEntry>,
}

impl ElmCron<MemStorage, InMemoryStore> {
    /// In-memory scheduler for tests and single-process hosts.
    pub fn new() -> Self {
        Self::with_engine(OurEngine::new())
    }
}

impl Default for ElmCron<MemStorage, InMemoryStore> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: EngineStorage, J: JournalStore> ElmCron<S, J> {
    /// Scheduler over a host engine (persistent store/journal).
    pub fn with_engine(engine: OurEngine<S, J>) -> Self {
        Self {
            engine,
            entries: BTreeMap::new(),
        }
    }

    /// Register or replace a schedule. `last_fired_ms` is the host's durable
    /// checkpoint; `0` means never fired. Replacing a schedule preserves its
    /// enabled flag.
    ///
    /// # Errors
    ///
    /// Returns [`CronError`] when the expression does not parse.
    pub fn add(
        &mut self,
        name: &str,
        expression: &str,
        last_fired_ms: u64,
    ) -> Result<(), CronError> {
        let schedule = CronSchedule::parse(expression)?;
        let enabled = self.entries.get(name).is_none_or(|entry| entry.enabled);
        self.entries.insert(
            name.to_string(),
            CronEntry {
                name: name.to_string(),
                schedule,
                last_fired_ms,
                enabled,
            },
        );
        Ok(())
    }

    /// Pause or resume one schedule. Returns `false` for an unknown name.
    pub fn set_enabled(&mut self, name: &str, enabled: bool) -> bool {
        let Some(entry) = self.entries.get_mut(name) else {
            return false;
        };
        entry.enabled = enabled;
        true
    }

    /// Remove one schedule. Returns `false` for an unknown name.
    pub fn remove(&mut self, name: &str) -> bool {
        self.entries.remove(name).is_some()
    }

    /// The host's last-fired checkpoint for one schedule.
    pub fn last_fired_ms(&self, name: &str) -> Option<u64> {
        self.entries.get(name).map(|entry| entry.last_fired_ms)
    }

    /// Restore the host's last-fired checkpoint (for example after a restart).
    /// Returns `false` for an unknown name.
    pub fn set_last_fired_ms(&mut self, name: &str, last_fired_ms: u64) -> bool {
        let Some(entry) = self.entries.get_mut(name) else {
            return false;
        };
        entry.last_fired_ms = last_fired_ms;
        true
    }

    /// Compute every due firing and start each one.
    ///
    /// One schedule yields at most one firing per call: the latest tick
    /// strictly after its checkpoint and at or before `now_ms`. A second call
    /// at the same instant yields none; a host that resets the checkpoint
    /// re-attaches to the same engine key (`Already`).
    ///
    /// # Errors
    ///
    /// Returns [`EngineError`] when the engine store or journal fails. A firing
    /// that already advanced its checkpoint stays advanced; the idempotent key
    /// makes a retry safe.
    pub fn tick(&mut self, now_ms: u64) -> Result<Vec<CronFire>, EngineError> {
        let engine = &mut self.engine;
        let mut fires = Vec::new();
        for entry in self.entries.values_mut() {
            if !entry.enabled {
                continue;
            }
            let Some(tick_ms) = entry
                .schedule
                .latest_at_or_before_ms(entry.last_fired_ms, now_ms)
            else {
                continue;
            };
            let key = fire_key(&entry.name, tick_ms);
            let outcome = engine.start(&key, now_ms)?;
            entry.last_fired_ms = tick_ms;
            fires.push(CronFire {
                name: entry.name.clone(),
                tick_ms,
                key,
                outcome,
            });
        }
        Ok(fires)
    }
}
