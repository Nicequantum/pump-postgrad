//! RPC bonding-curve poller — graduation detection via complete flag.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use reqwest::Client;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use tracing::{info, warn};

use crate::config::PostGradConfig;
use crate::tracker::{PollCandidate, PostGradTracker};
use crate::types::{INITIAL_VIRTUAL_TOKEN, PUMP_PROGRAM_ID};

#[derive(Debug, Clone, Default)]
pub struct BondingCurveState {
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub complete: bool,
    pub price_sol_per_token: f64,
    pub curve_pct: f64,
}

#[derive(Debug, Default)]
pub struct PollerMetrics {
    pub polls_total: AtomicU64,
    pub polls_ok: AtomicU64,
    pub polls_err: AtomicU64,
    pub graduations_from_poll: AtomicU64,
    pub rpc_retries: AtomicU64,
    pub fast_poll_rounds: AtomicU64,
}

impl PollerMetrics {
    pub fn summary(&self) -> String {
        format!(
            "polls={} ok={} err={} grad={} retries={} fast_rounds={}",
            self.polls_total.load(Ordering::Relaxed),
            self.polls_ok.load(Ordering::Relaxed),
            self.polls_err.load(Ordering::Relaxed),
            self.graduations_from_poll.load(Ordering::Relaxed),
            self.rpc_retries.load(Ordering::Relaxed),
            self.fast_poll_rounds.load(Ordering::Relaxed),
        )
    }
}

pub struct CurvePoller {
    client: Client,
    query_url: String,
    retries: u32,
    error_backoff_ms: u64,
}

impl CurvePoller {
    pub fn new(cfg: &PostGradConfig) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(8))
                .build()
                .expect("curve poller http"),
            query_url: cfg.rpc_url_with_token(&cfg.query_rpc),
            retries: cfg.poll_rpc_retries,
            error_backoff_ms: cfg.poll_error_backoff_ms,
        }
    }

    pub async fn get_bonding_curve_state(&self, mint: &Pubkey) -> Result<BondingCurveState> {
        let program = PUMP_PROGRAM_ID.parse::<Pubkey>().unwrap();
        let (bonding_curve, _) =
            Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program);

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                bonding_curve.to_string(),
                { "encoding": "base64", "commitment": "confirmed" }
            ]
        });

        let resp: serde_json::Value = self
            .client
            .post(&self.query_url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;

        if let Some(err) = resp.get("error") {
            return Err(anyhow::anyhow!("RPC error: {}", err));
        }

        let value = &resp["result"]["value"];
        if value.is_null() {
            return Ok(BondingCurveState {
                complete: true,
                curve_pct: 100.0,
                ..Default::default()
            });
        }

        let data_b64 = value["data"][0]
            .as_str()
            .context("missing account data")?;
        let data = base64::engine::general_purpose::STANDARD.decode(data_b64)?;

        if data.len() < 49 {
            return Ok(BondingCurveState {
                complete: true,
                curve_pct: 100.0,
                ..Default::default()
            });
        }

        let mut cursor = 8usize;
        let virtual_tok = u64::from_le_bytes(data[cursor..cursor + 8].try_into()?);
        cursor += 8;
        let virtual_sol = u64::from_le_bytes(data[cursor..cursor + 8].try_into()?);
        cursor += 8 + 8 + 8 + 8;
        let complete = data.len() > cursor && data[cursor] == 1;

        let price_sol_per_token = if virtual_tok > 0 {
            virtual_sol as f64 / virtual_tok as f64
        } else {
            0.0
        };

        Ok(BondingCurveState {
            virtual_sol_reserves: virtual_sol,
            virtual_token_reserves: virtual_tok,
            complete,
            price_sol_per_token,
            curve_pct: curve_pct_from_reserves(virtual_tok, complete),
        })
    }

    pub async fn poll_with_retry(
        &self,
        mint: &Pubkey,
        metrics: &PollerMetrics,
    ) -> Result<BondingCurveState> {
        let mut last_err = anyhow::anyhow!("poll not attempted");
        for attempt in 1..=self.retries.max(1) {
            match self.get_bonding_curve_state(mint).await {
                Ok(state) => return Ok(state),
                Err(e) => {
                    last_err = e;
                    if attempt < self.retries {
                        metrics.rpc_retries.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(
                            self.error_backoff_ms * attempt as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(last_err)
    }
}

pub fn curve_pct_from_reserves(virtual_token: u64, complete: bool) -> f64 {
    if complete {
        return 100.0;
    }
    let vt = virtual_token as f64;
    if vt <= 0.0 {
        return 0.0;
    }
    ((INITIAL_VIRTUAL_TOKEN - vt) / INITIAL_VIRTUAL_TOKEN * 100.0).clamp(0.0, 100.0)
}

async fn poll_candidates(
    poller: &CurvePoller,
    tracker: &PostGradTracker,
    metrics: &PollerMetrics,
    candidates: &[PollCandidate],
    per_mint_delay: Duration,
    label: &str,
) {
    if candidates.is_empty() {
        return;
    }
    info!(
        "poller {} round: {} mints",
        label,
        candidates.len()
    );
    for PollCandidate { mint, curve_pct, .. } in candidates {
        let Ok(mint_pk) = mint.parse::<Pubkey>() else {
            continue;
        };
        metrics.polls_total.fetch_add(1, Ordering::Relaxed);
        match poller.poll_with_retry(&mint_pk, metrics).await {
            Ok(curve) => {
                metrics.polls_ok.fetch_add(1, Ordering::Relaxed);
                if curve.complete {
                    metrics.graduations_from_poll.fetch_add(1, Ordering::Relaxed);
                    info!(
                        "curve complete {} | was {:.0}% now {:.0}%",
                        &mint[..8.min(mint.len())],
                        curve_pct,
                        curve.curve_pct
                    );
                }
                tracker.on_curve_poll(
                    mint,
                    curve.curve_pct,
                    curve.price_sol_per_token,
                    curve.complete,
                );
            }
            Err(e) => {
                metrics.polls_err.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "curve poll {}: {}",
                    &mint[..8.min(mint.len())],
                    e
                );
            }
        }
        tokio::time::sleep(per_mint_delay).await;
    }
}

/// Run one immediate poll cycle (startup diagnostic).
pub async fn force_initial_poll(
    cfg: &PostGradConfig,
    tracker: &PostGradTracker,
    metrics: &PollerMetrics,
) {
    let poller = CurvePoller::new(cfg);
    let candidates = tracker.mints_for_poll();
    info!(
        "startup initial poll: {} candidates tracked",
        candidates.len()
    );
    poll_candidates(
        &poller,
        tracker,
        metrics,
        &candidates,
        Duration::from_millis(cfg.poll_per_mint_delay_ms),
        "initial",
    )
    .await;
}

pub async fn run_curve_poller(
    cfg: Arc<PostGradConfig>,
    tracker: Arc<PostGradTracker>,
    metrics: Arc<PollerMetrics>,
) {
    let poller = CurvePoller::new(&cfg);
    let interval = Duration::from_millis(cfg.poll_interval_ms);
    let per_mint_delay = Duration::from_millis(cfg.poll_per_mint_delay_ms);
    let near_interval = Duration::from_millis(cfg.near_grad_poll_interval_ms);
    let near_delay = Duration::from_millis(cfg.near_grad_per_mint_delay_ms);
    let prune_interval = Duration::from_secs(cfg.tracker_prune_interval_secs);
    let mut last_prune = std::time::Instant::now();

    force_initial_poll(&cfg, &tracker, &metrics).await;

    let cfg_fast = cfg.clone();
    let tracker_fast = tracker.clone();
    let metrics_fast = metrics.clone();
    tokio::spawn(async move {
        let poller_fast = CurvePoller::new(&cfg_fast);
        loop {
            tokio::time::sleep(near_interval).await;
            let near = tracker_fast.mints_for_aggressive_poll(cfg_fast.near_grad_curve_pct);
            if near.is_empty() {
                continue;
            }
            metrics_fast.fast_poll_rounds.fetch_add(1, Ordering::Relaxed);
            poll_candidates(
                &poller_fast,
                &tracker_fast,
                &metrics_fast,
                &near,
                near_delay,
                "fast",
            )
            .await;
        }
    });

    loop {
        if last_prune.elapsed() >= prune_interval {
            tracker.prune_stale();
            last_prune = std::time::Instant::now();
        }

        let candidates = tracker.mints_for_poll();
        poll_candidates(
            &poller,
            &tracker,
            &metrics,
            &candidates,
            per_mint_delay,
            "main",
        )
        .await;

        tokio::time::sleep(interval).await;
    }
}