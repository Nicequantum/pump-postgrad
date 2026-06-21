//! Post-graduation entry filters — snipe Raydium shortly after migrate.

use std::sync::Arc;

use tracing::{info, warn};

use crate::config::PostGradConfig;
use crate::tracker::GraduationCandidate;
use crate::types::GraduationSource;

#[derive(Debug, Clone)]
pub struct EntryDecision {
    pub mint: String,
    pub candidate: GraduationCandidate,
    pub reason: String,
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
        open_positions: usize,
        open_mints: &[String],
        entries_last_hour: u32,
    ) -> Option<EntryDecision> {
        let short = &candidate.mint[..8.min(candidate.mint.len())];

        if open_mints.iter().any(|m| m == &candidate.mint) {
            warn!("skip {} — already in position", short);
            return None;
        }

        if open_positions >= self.cfg.max_open_positions {
            warn!("skip {} — max open positions ({})", short, self.cfg.max_open_positions);
            return None;
        }

        if entries_last_hour >= self.cfg.max_positions_per_hour {
            warn!("skip {} — hourly entry cap", short);
            return None;
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
            return None;
        }

        if candidate.buy_sell_ratio < self.cfg.min_buy_sell_ratio {
            warn!(
                "skip {} — buy/sell {:.2} < {:.2}",
                short,
                candidate.buy_sell_ratio,
                self.cfg.min_buy_sell_ratio
            );
            return None;
        }

        match candidate.source {
            GraduationSource::MigrateIx | GraduationSource::RaydiumSwap => {}
            GraduationSource::CurveComplete => {
                if candidate.trade_count < 25 {
                    warn!("skip {} — curve_complete but low activity", short);
                    return None;
                }
                if candidate.curve_pct < 99.0 {
                    warn!("skip {} — curve_complete but curve_pct={:.1}", short, candidate.curve_pct);
                    return None;
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

        Some(EntryDecision {
            mint: candidate.mint.clone(),
            candidate: candidate.clone(),
            reason,
        })
    }
}