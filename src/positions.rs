//! Open position lifecycle — Jupiter buy, monitor PnL, fast exit.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use parking_lot::RwLock;
use serde::Serialize;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::PostGradConfig;
use crate::execution::rpc::SignaturePoll;
use crate::execution::{JupiterSwap, RpcClient, TradingWallet};
use crate::parser::TradeEvent;
use crate::store::PostGradStores;
use crate::strategy::EntryDecision;
use crate::tracker::{GraduationCandidate, PostGradTracker};
use crate::types::{ExitReason, TradeSide};

#[derive(Debug, Clone, Serialize)]
pub struct EntryRecord {
    pub ts: String,
    pub mint: String,
    pub mint_short: String,
    pub sol_lamports: u64,
    pub token_amount: u64,
    pub entry_sig: String,
    pub grad_source: String,
    pub dry_run: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExitRecord {
    pub ts: String,
    pub mint: String,
    pub mint_short: String,
    pub exit_reason: String,
    pub entry_sol_lamports: u64,
    pub exit_sol_lamports: u64,
    pub pnl_pct: f64,
    pub hold_secs: u64,
    pub exit_sig: String,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionSnapshot {
    pub ts: String,
    pub mint: String,
    pub pnl_pct: f64,
    pub peak_pnl_pct: f64,
    pub hold_secs: u64,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    mint: String,
    entry_ms: u64,
    entry_sol_lamports: u64,
    token_amount: u64,
    entry_sig: String,
    grad_source: String,
    peak_pnl_pct: f64,
    post_grad_sell_count: u32,
    post_grad_sell_lamports: u64,
    last_snapshot_ms: u64,
}

pub struct PositionManager {
    cfg: Arc<PostGradConfig>,
    stores: Arc<PostGradStores>,
    wallet: Arc<TradingWallet>,
    rpc: Arc<RpcClient>,
    jupiter: Arc<JupiterSwap>,
    tracker: Arc<PostGradTracker>,
    open: RwLock<HashMap<String, OpenPosition>>,
}

impl PositionManager {
    pub fn new(
        cfg: Arc<PostGradConfig>,
        stores: Arc<PostGradStores>,
        wallet: Arc<TradingWallet>,
        rpc: Arc<RpcClient>,
        jupiter: Arc<JupiterSwap>,
        tracker: Arc<PostGradTracker>,
    ) -> Self {
        Self {
            cfg,
            stores,
            wallet,
            rpc,
            jupiter,
            tracker,
            open: RwLock::new(HashMap::new()),
        }
    }

    pub fn open_count(&self) -> usize {
        self.open.read().len()
    }

    fn open_mint_list(&self) -> Vec<String> {
        self.open.read().keys().cloned().collect()
    }

    pub async fn run_entry_loop(
        self: Arc<Self>,
        mut grad_rx: mpsc::Receiver<GraduationCandidate>,
    ) {
        while let Some(candidate) = grad_rx.recv().await {
            let strategy = crate::strategy::EntryStrategy::new(self.cfg.clone());
            let decision = strategy.evaluate(
                &candidate,
                self.open_count(),
                &self.open_mint_list(),
                self.tracker.entries_last_hour(),
            );
            let Some(decision) = decision else {
                continue;
            };

            self.tracker.mark_entry_pending(&decision.mint);
            tokio::time::sleep(Duration::from_millis(self.cfg.grad_snipe_delay_ms)).await;

            match self.execute_entry(decision).await {
                Ok(()) => {
                    self.tracker.record_entry_attempt();
                }
                Err(e) => {
                    error!(
                        "entry failed {}: {:#}",
                        &candidate.mint[..8.min(candidate.mint.len())],
                        e
                    );
                }
            }
            self.tracker.clear_entry_pending(&candidate.mint);
        }
    }

    async fn execute_entry(&self, decision: EntryDecision) -> anyhow::Result<()> {
        let mint = decision.mint.clone();
        let short = mint.chars().take(8).collect::<String>();
        let sol = self.cfg.buy_sol_lamports;

        if self.open.read().contains_key(&mint) {
            anyhow::bail!("duplicate position for {}", short);
        }

        if self.cfg.dry_run {
            let token_amount = self
                .jupiter
                .quote_tokens_out(&mint, sol)
                .await
                .unwrap_or(1_000_000);

            let row = EntryRecord {
                ts: Utc::now().to_rfc3339(),
                mint: mint.clone(),
                mint_short: short.clone(),
                sol_lamports: sol,
                token_amount,
                entry_sig: "DRY_RUN".to_string(),
                grad_source: decision.candidate.source.as_str().to_string(),
                dry_run: true,
                reason: decision.reason,
            };
            self.stores.write_entry(&row)?;
            info!("DRY_RUN BUY {} | {} lamports | ~{} tokens", short, sol, token_amount);
            self.open.write().insert(
                mint.clone(),
                OpenPosition {
                    mint,
                    entry_ms: crate::slot_timing::now_ms(),
                    entry_sol_lamports: sol,
                    token_amount,
                    entry_sig: "DRY_RUN".to_string(),
                    grad_source: decision.candidate.source.as_str().to_string(),
                    peak_pnl_pct: 0.0,
                    post_grad_sell_count: 0,
                    post_grad_sell_lamports: 0,
                    last_snapshot_ms: crate::slot_timing::now_ms(),
                },
            );
            return Ok(());
        }

        if !self.jupiter.has_buy_route(&mint, sol).await {
            anyhow::bail!("no Jupiter buy route for {}", short);
        }

        let balance = self
            .rpc
            .get_sol_balance(&self.wallet.keypair().pubkey())
            .await?;
        let required = self
            .cfg
            .buy_sol_lamports
            .saturating_add(self.cfg.sol_reserve_lamports);
        if balance < required {
            anyhow::bail!(
                "wallet balance {} lamports < required {} (buy + reserve)",
                balance,
                required
            );
        }
        if balance < self.cfg.min_wallet_sol_lamports {
            anyhow::bail!(
                "wallet balance {} lamports < min {} lamports",
                balance,
                self.cfg.min_wallet_sol_lamports
            );
        }

        let (sig, quoted_tokens) = self
            .jupiter
            .buy_with_sol(&mint, sol, self.wallet.keypair(), &self.rpc)
            .await?;

        match self
            .rpc
            .wait_for_confirmation(&sig, self.cfg.tx_confirm_timeout_ms)
            .await?
        {
            SignaturePoll::Confirmed => {}
            SignaturePoll::Reverted(e) => anyhow::bail!("buy tx reverted: {}", e),
            SignaturePoll::Pending => warn!("buy tx {} not confirmed in time — checking balance", sig),
        }

        let token_amount = self
            .rpc
            .get_mint_balance(&self.wallet.keypair().pubkey(), &mint)
            .await?;
        let tokens = if token_amount > 0 {
            token_amount
        } else {
            quoted_tokens
        };

        if tokens == 0 {
            anyhow::bail!("buy landed with zero token balance for {}", short);
        }

        let row = EntryRecord {
            ts: Utc::now().to_rfc3339(),
            mint: mint.clone(),
            mint_short: short.clone(),
            sol_lamports: sol,
            token_amount: tokens,
            entry_sig: sig.to_string(),
            grad_source: decision.candidate.source.as_str().to_string(),
            dry_run: false,
            reason: decision.reason,
        };
        self.stores.write_entry(&row)?;

        self.open.write().insert(
            mint.clone(),
            OpenPosition {
                mint,
                entry_ms: crate::slot_timing::now_ms(),
                entry_sol_lamports: sol,
                token_amount: tokens,
                entry_sig: sig.to_string(),
                grad_source: decision.candidate.source.as_str().to_string(),
                peak_pnl_pct: 0.0,
                post_grad_sell_count: 0,
                post_grad_sell_lamports: 0,
                last_snapshot_ms: crate::slot_timing::now_ms(),
            },
        );

        info!("🟢 OPEN {} | sig={} | tokens={}", short, sig, tokens);
        Ok(())
    }

    pub fn on_trade(&self, trade: &TradeEvent) {
        let mut open = self.open.write();
        if let Some(pos) = open.get_mut(&trade.mint) {
            if trade.side == TradeSide::Sell {
                pos.post_grad_sell_count += 1;
                if let Some(amt) = trade.amount_lamports {
                    pos.post_grad_sell_lamports = pos.post_grad_sell_lamports.saturating_add(amt);
                }
            }
        }
    }

    pub async fn run_exit_monitor(self: Arc<Self>) {
        let poll = Duration::from_millis(self.cfg.position_poll_ms);
        loop {
            tokio::time::sleep(poll).await;
            let mints: Vec<String> = self.open.read().keys().cloned().collect();
            for mint in mints {
                if let Err(e) = self.check_exit(&mint).await {
                    warn!("exit check {}: {}", &mint[..8.min(mint.len())], e);
                }
            }
        }
    }

    async fn current_pnl_pct(&self, pos: &OpenPosition) -> anyhow::Result<f64> {
        if pos.token_amount == 0 || pos.entry_sol_lamports == 0 {
            return Ok(0.0);
        }
        let out = self
            .jupiter
            .quote_sol_out(&pos.mint, pos.token_amount)
            .await?;
        Ok(((out as f64 / pos.entry_sol_lamports as f64) - 1.0) * 100.0)
    }

    async fn check_exit(&self, mint: &str) -> anyhow::Result<()> {
        let pos = {
            let open = self.open.read();
            open.get(mint).cloned()
        };
        let Some(pos) = pos else {
            return Ok(());
        };

        let now = crate::slot_timing::now_ms();
        let hold_secs = (now.saturating_sub(pos.entry_ms)) / 1000;

        let pnl_pct = match self.current_pnl_pct(&pos).await {
            Ok(p) => p,
            Err(e) => {
                warn!("pnl quote {}: {}", &mint[..8.min(mint.len())], e);
                0.0
            }
        };

        let peak_pnl = {
            let mut open = self.open.write();
            if let Some(p) = open.get_mut(mint) {
                if pnl_pct > p.peak_pnl_pct {
                    p.peak_pnl_pct = pnl_pct;
                }
                p.peak_pnl_pct
            } else {
                return Ok(());
            }
        };

        let should_snapshot = {
            let mut open = self.open.write();
            if let Some(p) = open.get_mut(mint) {
                let now = crate::slot_timing::now_ms();
                if now.saturating_sub(p.last_snapshot_ms) >= self.cfg.position_snapshot_ms {
                    p.last_snapshot_ms = now;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if should_snapshot {
            let _ = self.stores.write_position(&PositionSnapshot {
                ts: Utc::now().to_rfc3339(),
                mint: mint.to_string(),
                pnl_pct,
                peak_pnl_pct: peak_pnl,
                hold_secs,
                dry_run: self.cfg.dry_run,
            });
        }

        let rug_signal = pos.post_grad_sell_count >= 3
            && pos.post_grad_sell_lamports > pos.entry_sol_lamports / 2;

        let trailing_hit = peak_pnl >= self.cfg.take_profit_pct * 0.5
            && pnl_pct <= peak_pnl - self.cfg.trailing_stop_pct;

        let reason = if pnl_pct >= self.cfg.take_profit_pct {
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
        };

        if let Some(r) = reason {
            self.close_position(mint, &pos, r, pnl_pct, hold_secs).await?;
        }
        Ok(())
    }

    async fn close_position(
        &self,
        mint: &str,
        pos: &OpenPosition,
        reason: ExitReason,
        pnl_pct: f64,
        hold_secs: u64,
    ) -> anyhow::Result<()> {
        let short = &mint[..8.min(mint.len())];
        let mut exit_sig = Signature::default().to_string();
        let mut exit_lamports = 0u64;

        if self.cfg.dry_run {
            exit_sig = "DRY_RUN".to_string();
            exit_lamports = self
                .jupiter
                .quote_sol_out(mint, pos.token_amount)
                .await
                .unwrap_or((pos.entry_sol_lamports as f64 * (1.0 + pnl_pct / 100.0)) as u64);
        } else {
            let balance = self
                .rpc
                .get_mint_balance(&self.wallet.keypair().pubkey(), mint)
                .await?;
            let sell_amount = if balance > 0 { balance } else { pos.token_amount };
            if sell_amount == 0 {
                anyhow::bail!("no tokens to sell for {}", short);
            }

            if !self.jupiter.has_sell_route(mint, sell_amount).await {
                warn!("no Jupiter sell route for {} — will retry on next poll", short);
                return Ok(());
            }

            let mut last_err = None;
            for attempt in 1..=self.cfg.sell_max_retries.max(1) {
                match self
                    .jupiter
                    .sell_tokens(mint, sell_amount, self.wallet.keypair(), &self.rpc)
                    .await
                {
                    Ok((sig, out)) => {
                        exit_sig = sig.to_string();
                        exit_lamports = out;

                        match self
                            .rpc
                            .wait_for_confirmation(&sig, self.cfg.tx_confirm_timeout_ms)
                            .await?
                        {
                            SignaturePoll::Reverted(e) => {
                                warn!("exit tx reverted {} (attempt {}): {}", short, attempt, e);
                                last_err = Some(anyhow::anyhow!("reverted: {}", e));
                            }
                            SignaturePoll::Pending => {
                                warn!("exit tx {} pending after timeout", sig);
                                break;
                            }
                            SignaturePoll::Confirmed => {
                                last_err = None;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("sell attempt {} failed for {}: {:#}", attempt, short, e);
                        last_err = Some(e);
                    }
                }
                if attempt < self.cfg.sell_max_retries && exit_sig == Signature::default().to_string()
                {
                    tokio::time::sleep(Duration::from_millis(400 * attempt as u64)).await;
                }
            }
            if exit_sig == Signature::default().to_string() {
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("sell failed for {}", short)));
            }
        }

        let row = ExitRecord {
            ts: Utc::now().to_rfc3339(),
            mint: mint.to_string(),
            mint_short: short.to_string(),
            exit_reason: reason.as_str().to_string(),
            entry_sol_lamports: pos.entry_sol_lamports,
            exit_sol_lamports: exit_lamports,
            pnl_pct,
            hold_secs,
            exit_sig: exit_sig.clone(),
            dry_run: self.cfg.dry_run,
        };
        self.stores.write_exit(&row)?;
        self.open.write().remove(mint);

        info!(
            "🔴 CLOSE {} | {} | pnl={:.1}% | hold={}s | sig={}",
            short,
            reason.as_str(),
            pnl_pct,
            hold_secs,
            exit_sig
        );
        Ok(())
    }
}