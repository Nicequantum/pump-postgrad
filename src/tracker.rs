//! Token watchlist + graduation queue — feeds the entry engine.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::PostGradConfig;
use crate::parser::{GraduationEvent, LaunchEvent, TradeEvent};
use crate::store::PostGradStores;
use crate::strategy::{EntryContext, EntryRejectReason};
use crate::types::{is_plausible_mint, GraduationSource, TradeSide};

#[derive(Debug, Clone, Serialize)]
pub struct GraduationRecord {
    pub ts: String,
    pub mint: String,
    pub mint_short: String,
    pub source: String,
    pub signature: String,
    pub slot: u64,
    pub receive_ms: u64,
    pub feed: String,
    pub curve_pct: f64,
    pub unique_buyers: u32,
    pub buy_sell_ratio: f64,
    pub trade_count: u32,
}

#[derive(Debug, Clone)]
pub struct GraduationCandidate {
    pub mint: String,
    pub source: GraduationSource,
    pub grad_ms: u64,
    pub signature: String,
    pub slot: u64,
    pub curve_pct: f64,
    pub unique_buyers: u32,
    pub buy_sell_ratio: f64,
    pub trade_count: u32,
}

#[derive(Debug, Clone)]
pub struct PollCandidate {
    pub mint: String,
    pub curve_pct: f64,
    pub last_activity: Instant,
}

#[derive(Debug)]
struct TokenState {
    mint: String,
    creator: String,
    create_ms: u64,
    trade_count: u32,
    buy_count: u32,
    sell_count: u32,
    unique_buyers: HashSet<String>,
    curve_pct: f64,
    graduated: bool,
    grad_source: Option<GraduationSource>,
    last_activity: Instant,
    last_grad_attempt: Option<Instant>,
}

impl TokenState {
    fn new(mint: String, creator: String, create_ms: u64) -> Self {
        Self {
            mint,
            creator,
            create_ms,
            trade_count: 0,
            buy_count: 0,
            sell_count: 0,
            unique_buyers: HashSet::new(),
            curve_pct: 0.0,
            graduated: false,
            grad_source: None,
            last_activity: Instant::now(),
            last_grad_attempt: None,
        }
    }

    fn buy_sell_ratio(&self) -> f64 {
        if self.sell_count == 0 {
            return self.buy_count as f64;
        }
        self.buy_count as f64 / self.sell_count as f64
    }
}

pub struct PostGradTracker {
    cfg: Arc<PostGradConfig>,
    stores: Arc<PostGradStores>,
    tokens: DashMap<String, RwLock<TokenState>>,
    grad_tx: mpsc::Sender<GraduationCandidate>,
    launches_total: AtomicU64,
    trades_total: AtomicU64,
    graduations_total: AtomicU64,
    entries_queued: AtomicU64,
    entries_rejected: AtomicU64,
    channel_full_total: AtomicU64,
    pruned_total: AtomicU64,
    recent_entry_times: RwLock<VecDeque<u64>>,
    pending_mints: RwLock<HashSet<String>>,
    reject_counts: [AtomicU64; 10],
}

impl PostGradTracker {
    pub fn new(
        cfg: Arc<PostGradConfig>,
        stores: Arc<PostGradStores>,
        grad_tx: mpsc::Sender<GraduationCandidate>,
    ) -> Self {
        Self {
            cfg,
            stores,
            tokens: DashMap::new(),
            grad_tx,
            launches_total: AtomicU64::new(0),
            trades_total: AtomicU64::new(0),
            graduations_total: AtomicU64::new(0),
            entries_queued: AtomicU64::new(0),
            entries_rejected: AtomicU64::new(0),
            channel_full_total: AtomicU64::new(0),
            pruned_total: AtomicU64::new(0),
            recent_entry_times: RwLock::new(VecDeque::new()),
            pending_mints: RwLock::new(HashSet::new()),
            reject_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn ensure_token(&self, mint: &str, creator: &str, create_ms: u64) {
        if !is_plausible_mint(mint) {
            return;
        }
        if self.tokens.len() >= self.cfg.max_tracked_tokens {
            self.prune_one_stale();
        }
        self.tokens
            .entry(mint.to_string())
            .or_insert_with(|| RwLock::new(TokenState::new(mint.to_string(), creator.to_string(), create_ms)));
    }

    pub fn on_launch(&self, launch: LaunchEvent) {
        self.launches_total.fetch_add(1, Ordering::Relaxed);
        self.ensure_token(&launch.mint, &launch.creator, launch.receive_ms);
    }

    pub fn on_trade(&self, trade: TradeEvent) {
        self.trades_total.fetch_add(1, Ordering::Relaxed);
        self.ensure_token(&trade.mint, "", trade.receive_ms);
        if let Some(entry) = self.tokens.get(&trade.mint) {
            let mut s = entry.write();
            s.trade_count += 1;
            s.last_activity = Instant::now();
            match trade.side {
                TradeSide::Buy => {
                    s.buy_count += 1;
                    if !trade.trader.is_empty() {
                        s.unique_buyers.insert(trade.trader);
                    }
                }
                TradeSide::Sell => s.sell_count += 1,
            }
        }
    }

    pub fn on_graduation_shred(&self, grad: GraduationEvent) {
        self.ensure_token(&grad.mint, "", grad.receive_ms);
        self.emit_graduation(
            &grad.mint,
            grad.source,
            grad.receive_ms,
            &grad.signature,
            grad.slot,
        );
    }

    pub fn on_curve_poll(
        &self,
        mint: &str,
        curve_pct: f64,
        _price: f64,
        complete: bool,
    ) {
        self.ensure_token(mint, "", crate::slot_timing::now_ms());
        if let Some(entry) = self.tokens.get(mint) {
            let mut s = entry.write();
            s.curve_pct = curve_pct;
            s.last_activity = Instant::now();
            if complete && !s.graduated {
                drop(s);
                self.emit_graduation(
                    mint,
                    GraduationSource::CurveComplete,
                    crate::slot_timing::now_ms(),
                    "",
                    0,
                );
            }
        }
    }

    fn emit_graduation(
        &self,
        mint: &str,
        source: GraduationSource,
        grad_ms: u64,
        signature: &str,
        slot: u64,
    ) {
        if !is_plausible_mint(mint) {
            return;
        }
        self.ensure_token(mint, "", grad_ms);
        let entry = match self.tokens.get(mint) {
            Some(e) => e,
            None => return,
        };
        let mut s = entry.write();

        if s.graduated {
            if let Some(existing) = s.grad_source {
                if source.priority() <= existing.priority() {
                    return;
                }
            } else {
                return;
            }
        }

        let dedup = Duration::from_secs(self.cfg.grad_dedup_secs);
        if let Some(last) = s.last_grad_attempt {
            if last.elapsed() < dedup && s.graduated {
                return;
            }
        }

        if source == GraduationSource::RaydiumSwap && s.curve_pct < self.cfg.min_curve_pct_to_watch {
            return;
        }

        let candidate = GraduationCandidate {
            mint: mint.to_string(),
            source,
            grad_ms,
            signature: signature.to_string(),
            slot,
            curve_pct: if source == GraduationSource::CurveComplete || s.curve_pct >= 99.0 {
                s.curve_pct.max(100.0)
            } else {
                s.curve_pct
            },
            unique_buyers: s.unique_buyers.len() as u32,
            buy_sell_ratio: s.buy_sell_ratio(),
            trade_count: s.trade_count,
        };

        let record = GraduationRecord {
            ts: Utc::now().to_rfc3339(),
            mint: mint.to_string(),
            mint_short: mint.chars().take(8).collect(),
            source: source.as_str().to_string(),
            signature: signature.to_string(),
            slot,
            receive_ms: grad_ms,
            feed: source.as_str().to_string(),
            curve_pct: candidate.curve_pct,
            unique_buyers: candidate.unique_buyers,
            buy_sell_ratio: candidate.buy_sell_ratio,
            trade_count: candidate.trade_count,
        };

        if self.pending_mints.read().contains(mint) {
            return;
        }

        s.last_grad_attempt = Some(Instant::now());
        drop(s);

        match self.grad_tx.try_send(candidate.clone()) {
            Ok(()) => {
                let mut s = entry.write();
                s.graduated = true;
                s.grad_source = Some(source);
                drop(s);

                let _ = self.stores.write_graduation(&record);
                self.graduations_total.fetch_add(1, Ordering::Relaxed);
                self.entries_queued.fetch_add(1, Ordering::Relaxed);

                info!(
                    "🎓 GRADUATION {} | src={} | buyers={} | ratio={:.1} | trades={} | curve={:.0}%",
                    &mint[..8.min(mint.len())],
                    source.as_str(),
                    candidate.unique_buyers,
                    candidate.buy_sell_ratio,
                    candidate.trade_count,
                    candidate.curve_pct,
                );
            }
            Err(e) => {
                self.channel_full_total.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "graduation channel full for {} — will retry: {}",
                    &mint[..8.min(mint.len())],
                    e
                );
            }
        }
    }

    /// Poll candidates sorted by curve proximity (highest first).
    pub fn mints_for_poll(&self) -> Vec<PollCandidate> {
        let ttl = Duration::from_secs(self.cfg.token_activity_ttl_secs);
        let max = self.cfg.max_tracked_tokens;
        let mut out: Vec<PollCandidate> = self
            .tokens
            .iter()
            .filter_map(|e| {
                let s = e.value().read();
                if s.graduated || s.last_activity.elapsed() >= ttl {
                    return None;
                }
                Some(PollCandidate {
                    mint: e.key().clone(),
                    curve_pct: s.curve_pct,
                    last_activity: s.last_activity,
                })
            })
            .collect();
        out.sort_by(|a, b| {
            b.curve_pct
                .partial_cmp(&a.curve_pct)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(max);
        out
    }

    pub fn prune_stale(&self) -> usize {
        let ttl = Duration::from_secs(self.cfg.token_activity_ttl_secs);
        let mut removed = 0usize;
        let keys: Vec<String> = self
            .tokens
            .iter()
            .filter_map(|e| {
                let s = e.value().read();
                if s.graduated || s.last_activity.elapsed() < ttl {
                    None
                } else {
                    Some(e.key().clone())
                }
            })
            .collect();
        for key in keys {
            if self.tokens.remove(&key).is_some() {
                removed += 1;
            }
        }
        if removed > 0 {
            self.pruned_total.fetch_add(removed as u64, Ordering::Relaxed);
            debug!("tracker pruned {} stale tokens", removed);
        }
        removed
    }

    fn prune_one_stale(&self) {
        let ttl = Duration::from_secs(self.cfg.token_activity_ttl_secs);
        if let Some(oldest) = self
            .tokens
            .iter()
            .filter(|e| {
                let s = e.value().read();
                !s.graduated && s.last_activity.elapsed() >= ttl
            })
            .min_by_key(|e| e.value().read().last_activity)
        {
            let key = oldest.key().clone();
            if self.tokens.remove(&key).is_some() {
                self.pruned_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn entry_context(&self, open_mints: Vec<String>) -> EntryContext {
        EntryContext {
            open_positions: open_mints.len(),
            open_mints,
            entries_last_hour: self.entries_last_hour(),
        }
    }

    pub fn mark_entry_pending(&self, mint: &str) {
        self.pending_mints.write().insert(mint.to_string());
    }

    pub fn clear_entry_pending(&self, mint: &str) {
        self.pending_mints.write().remove(mint);
    }

    pub fn record_entry_attempt(&self) {
        let now = crate::slot_timing::now_ms();
        let mut q = self.recent_entry_times.write();
        q.push_back(now);
        while q.len() > 128 {
            q.pop_front();
        }
    }

    pub fn record_entry_reject(&self, reason: EntryRejectReason) {
        self.entries_rejected.fetch_add(1, Ordering::Relaxed);
        let idx = reject_index(reason);
        self.reject_counts[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub fn entries_last_hour(&self) -> u32 {
        let cutoff = crate::slot_timing::now_ms().saturating_sub(3_600_000);
        self.recent_entry_times
            .read()
            .iter()
            .filter(|&&t| t >= cutoff)
            .count() as u32
    }

    pub fn heartbeat_line(&self) -> String {
        let ttl = Duration::from_secs(self.cfg.token_activity_ttl_secs);
        let active = self
            .tokens
            .iter()
            .filter(|e| {
                let s = e.value().read();
                !s.graduated && s.last_activity.elapsed() < ttl
            })
            .count();
        let near_grad = self
            .tokens
            .iter()
            .filter(|e| {
                let s = e.value().read();
                !s.graduated
                    && s.last_activity.elapsed() < ttl
                    && s.curve_pct >= self.cfg.min_curve_pct_to_watch
            })
            .count();
        format!(
            "active={} near_grad={} tracked={} launches={} trades={} grads={} queued={} rejected={} ch_full={} pruned={}",
            active,
            near_grad,
            self.tokens.len(),
            self.launches_total.load(Ordering::Relaxed),
            self.trades_total.load(Ordering::Relaxed),
            self.graduations_total.load(Ordering::Relaxed),
            self.entries_queued.load(Ordering::Relaxed),
            self.entries_rejected.load(Ordering::Relaxed),
            self.channel_full_total.load(Ordering::Relaxed),
            self.pruned_total.load(Ordering::Relaxed),
        )
    }
}

fn reject_index(reason: EntryRejectReason) -> usize {
    match reason {
        EntryRejectReason::InvalidMint => 0,
        EntryRejectReason::StaleGraduation => 1,
        EntryRejectReason::AlreadyOpen => 2,
        EntryRejectReason::MaxOpenPositions => 3,
        EntryRejectReason::HourlyCap => 4,
        EntryRejectReason::LowBuyers => 5,
        EntryRejectReason::LowBuySellRatio => 6,
        EntryRejectReason::LowActivity => 7,
        EntryRejectReason::IncompleteCurve => 8,
        EntryRejectReason::NoRoute => 9,
    }
}