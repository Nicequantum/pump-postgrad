//! Wall-clock slot timing helpers for latency attribution.

use dashmap::DashMap;
use once_cell::sync::Lazy;

static SLOT_STARTS: Lazy<DashMap<u64, u64>> = Lazy::new(DashMap::new);

const MAX_SLOTS: usize = 1024;
const DEFAULT_SLOT_MS: u64 = 400;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn record_slot(slot: u64) {
    if slot == 0 {
        return;
    }
    let ts = now_ms();
    SLOT_STARTS.entry(slot).or_insert(ts);
    if SLOT_STARTS.len() > MAX_SLOTS {
        let min_slot = SLOT_STARTS.iter().map(|e| *e.key()).min().unwrap_or(slot);
        SLOT_STARTS.remove(&min_slot);
    }
}