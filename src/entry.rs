//! Entry coordinator — strategy evaluation and preflight between tracker and execution.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::PostGradConfig;
use crate::execution::JupiterSwap;
use crate::positions::PositionManager;
use crate::store::PostGradStores;
use crate::strategy::{EntryRejectRecord, EntryStrategy};
use crate::tracker::{GraduationCandidate, PostGradTracker};

pub struct EntryEngine {
    cfg: Arc<PostGradConfig>,
    strategy: EntryStrategy,
    tracker: Arc<PostGradTracker>,
    positions: Arc<PositionManager>,
    jupiter: Arc<JupiterSwap>,
    stores: Arc<PostGradStores>,
}

impl EntryEngine {
    pub fn new(
        cfg: Arc<PostGradConfig>,
        tracker: Arc<PostGradTracker>,
        positions: Arc<PositionManager>,
        jupiter: Arc<JupiterSwap>,
        stores: Arc<PostGradStores>,
    ) -> Self {
        Self {
            strategy: EntryStrategy::new(cfg.clone()),
            cfg,
            tracker,
            positions,
            jupiter,
            stores,
        }
    }

    pub async fn run(self: Arc<Self>, mut grad_rx: mpsc::Receiver<GraduationCandidate>) {
        info!("entry engine started");
        while let Some(candidate) = grad_rx.recv().await {
            self.handle_candidate(candidate).await;
        }
        warn!("graduation channel closed — entry engine exiting");
    }

    async fn handle_candidate(&self, candidate: GraduationCandidate) {
        let short = &candidate.mint[..8.min(candidate.mint.len())];
        let ctx = self.tracker.entry_context(self.positions.open_mints());

        let decision = match self.strategy.evaluate(&candidate, &ctx) {
            Err(reason) => {
                self.tracker.record_entry_reject(reason);
                let _ = self.stores.write_entry_reject(&EntryRejectRecord {
                    ts: Utc::now().to_rfc3339(),
                    mint: candidate.mint.clone(),
                    mint_short: short.to_string(),
                    reason: reason.as_str().to_string(),
                    grad_source: candidate.source.as_str().to_string(),
                    unique_buyers: candidate.unique_buyers,
                    buy_sell_ratio: candidate.buy_sell_ratio,
                    curve_pct: candidate.curve_pct,
                });
                return;
            }
            Ok(d) => d,
        };

        if let Err(e) = self.preflight(&decision.mint).await {
            warn!("preflight failed {}: {:#}", short, e);
            self.tracker
                .record_entry_reject(crate::strategy::EntryRejectReason::NoRoute);
            let _ = self.stores.write_entry_reject(&EntryRejectRecord {
                ts: Utc::now().to_rfc3339(),
                mint: candidate.mint.clone(),
                mint_short: short.to_string(),
                reason: "no_route".to_string(),
                grad_source: candidate.source.as_str().to_string(),
                unique_buyers: candidate.unique_buyers,
                buy_sell_ratio: candidate.buy_sell_ratio,
                curve_pct: candidate.curve_pct,
            });
            return;
        }

        self.tracker.mark_entry_pending(&decision.mint);
        tokio::time::sleep(Duration::from_millis(self.cfg.grad_snipe_delay_ms)).await;

        match self.positions.execute_entry(decision).await {
            Ok(()) => {
                self.tracker.record_entry_attempt();
            }
            Err(e) => {
                error!("entry failed {}: {:#}", short, e);
            }
        }
        self.tracker.clear_entry_pending(&candidate.mint);
    }

    async fn preflight(&self, mint: &str) -> anyhow::Result<()> {
        let sol = self.cfg.buy_sol_lamports;
        if !self.cfg.dry_run {
            if !self.jupiter.has_buy_route(mint, sol).await {
                anyhow::bail!("no Jupiter buy route");
            }
        }
        Ok(())
    }
}