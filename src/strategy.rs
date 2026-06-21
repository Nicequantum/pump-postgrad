//! Entry and exit policy — pure decision logic, no execution side effects.

use std::sync::Arc;

use serde::Serialize;
use tracing::{info, warn};

use crate::config::PostGradConfig;
use crate::tracker::GraduationCandidate;
use crate::types::{is_plausible_mint, ExitReason, GraduationSource};

/// Portfolio + rate-limit snapshot for entry evaluation.
#[derive(Debug, Clone, Default)]
pub struct EntryContext {
    pub open_positions: usize,
    pub open_mints: Vec<String>,
    pub entries_last_hour: u32,
}

#[derive(Debug, Clone)]
pub struct EntryDecision {
    pub mint: String,
    pub candidate: GraduationCandidate,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryRejectReason {
    InvalidMint,
    StaleGraduation,
    AlreadyOpen,
    MaxOpenPositions,
    HourlyCap,
    LowBuyers,
    LowBuySellRatio,
    LowActivity,
    IncompleteCurve,
    NoRoute,
}

impl EntryRejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidMint => "invalid_mint",
            Self::StaleGraduation => "stale_graduation",
            Self::AlreadyOpen => "already_open",
            Self::MaxOpenPositions => "max_open_positions",
            Self::HourlyCap => "hourly_cap",
            Self::LowBuyers => "low_buyers",
            Self::LowBuySellRatio => "low_buy_sell_ratio",
            Self::LowActivity => "low_activity",
            Self::IncompleteCurve => "incomplete_curve",
            Self::NoRoute => "no_route",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EntryRejectRecord {
    pub ts: String,
    pub mint: String,
    pub mint_short: String,
    pub reason: String,
    pub grad_source: String,
    pub unique_buyers: u32,
    pub buy_sell_ratio: f64,
    pub curve_pct: f64,
}

/// Snapshot of an open position for exit evaluation.
#[derive(Debug, Clone)]
pub struct PositionView {
    pub mint: String,
    pub entry_ms: u64,
    pub entry_sol_lamports: u64,
    pub token_amount: u64,
    pub peak_pnl_pct: f64,
    pub post_grad_sell_count: u32,
    pub post_grad_sell_lamports: u64,
}

pub struct EntryStrategy {
    cfg: Arc<PostGradConfig>,
}

impl EntryStrategy {
    pub fn new(cfg: Arc<PostGradConfig>) -> Self {
        Self { cfg }
    }

    pub fn evaluate(
        &self,
        candidate: &GraduationCandidate,
        ctx: &EntryContext,
    ) -> Result<EntryDecision, EntryRejectReason> {
        let short = &candidate.mint[..8.min(candidate.mint.len())];

        if !is_plausible_mint(&candidate.mint) {
            warn!("skip {} — invalid mint address", short);
            return Err(EntryRejectReason::InvalidMint);
        }

        let grad_age_ms = crate::slot_timing::now_ms().saturating_sub(candidate.grad_ms);
        if grad_age_ms > self.cfg.max_grad_age_ms {
            warn!(
                "skip {} — graduation stale ({}ms > {}ms)",
                short,
                grad_age_ms,
                self.cfg.max_grad_age_ms
            );
            return Err(EntryRejectReason::StaleGraduation);
        }

        if ctx.open_mints.iter().any(|m| m == &candidate.mint) {
            warn!("skip {} — already in position", short);
            return Err(EntryRejectReason::AlreadyOpen);
        }

        if ctx.open_positions >= self.cfg.max_open_positions {
            warn!(
                "skip {} — max open positions ({})",
                short,
                self.cfg.max_open_positions
            );
            return Err(EntryRejectReason::MaxOpenPositions);
        }

        if ctx.entries_last_hour >= self.cfg.max_positions_per_hour {
            warn!("skip {} — hourly entry cap", short);
            return Err(EntryRejectReason::HourlyCap);
        }

        let min_buyers = match candidate.source {
            GraduationSource::MigrateIx | GraduationSource::RaydiumSwap => {
                self.cfg.min_unique_buyers_migrate
            }
            GraduationSource::CurveComplete => self.cfg.min_unique_buyers,
        };

        if candidate.unique_buyers < min_buyers {
            warn!(
                "skip {} — buyers {} < {} (src={})",
                short,
                candidate.unique_buyers,
                min_buyers,
                candidate.source.as_str()
            );
            return Err(EntryRejectReason::LowBuyers);
        }

        if candidate.buy_sell_ratio < self.cfg.min_buy_sell_ratio {
            warn!(
                "skip {} — buy/sell {:.2} < {:.2}",
                short,
                candidate.buy_sell_ratio,
                self.cfg.min_buy_sell_ratio
            );
            return Err(EntryRejectReason::LowBuySellRatio);
        }

        match candidate.source {
            GraduationSource::MigrateIx | GraduationSource::RaydiumSwap => {}
            GraduationSource::CurveComplete => {
                if candidate.trade_count < self.cfg.min_trades_curve_complete {
                    warn!("skip {} — curve_complete but low activity", short);
                    return Err(EntryRejectReason::LowActivity);
                }
                if candidate.curve_pct < self.cfg.min_curve_pct_complete {
                    warn!(
                        "skip {} — curve_complete but curve_pct={:.1}",
                        short,
                        candidate.curve_pct
                    );
                    return Err(EntryRejectReason::IncompleteCurve);
                }
            }
        }

        let reason = format!(
            "postgrad_snipe src={} buyers={} ratio={:.1} curve={:.0}%",
            candidate.source.as_str(),
            candidate.unique_buyers,
            candidate.buy_sell_ratio,
            candidate.curve_pct
        );

        info!("✅ ENTRY SIGNAL {} | {}", short, reason);

        Ok(EntryDecision {
            mint: candidate.mint.clone(),
            candidate: candidate.clone(),
            reason,
        })
    }
}

pub struct ExitStrategy {
    cfg: Arc<PostGradConfig>,
}

impl ExitStrategy {
    pub fn new(cfg: Arc<PostGradConfig>) -> Self {
        Self { cfg }
    }

    pub fn evaluate(&self, pos: &PositionView, pnl_pct: f64, now_ms: u64) -> Option<ExitReason> {
        let hold_secs = now_ms.saturating_sub(pos.entry_ms) / 1000;

        let rug_signal = pos.post_grad_sell_count >= self.cfg.rug_min_sell_count
            && pos.post_grad_sell_lamports > pos.entry_sol_lamports / self.cfg.rug_sell_lamport_divisor;

        let trailing_hit = pos.peak_pnl_pct >= self.cfg.take_profit_pct * 0.5
            && pnl_pct <= pos.peak_pnl_pct - self.cfg.trailing_stop_pct;

        if pnl_pct >= self.cfg.take_profit_pct {
            Some(ExitReason::TakeProfit)
        } else if trailing_hit {
            Some(ExitReason::TrailingStop)
        } else if pnl_pct <= -self.cfg.stop_loss_pct {
            Some(ExitReason::StopLoss)
        } else if hold_secs >= self.cfg.max_hold_secs {
            Some(ExitReason::TimeStop)
        } else if rug_signal {
            Some(ExitReason::RugSignal)
        } else {
            None
        }
    }
}