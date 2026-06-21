//! RPC bonding-curve poller — graduation detection via complete flag.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use reqwest::Client;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use tracing::warn;

use crate::config::PostGradConfig;
use crate::tracker::PostGradTracker;
use crate::types::{INITIAL_VIRTUAL_TOKEN, PUMP_PROGRAM_ID};

#[derive(Debug, Clone, Default)]
pub struct BondingCurveState {
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub complete: bool,
    pub price_sol_per_token: f64,
    pub curve_pct: f64,
}

pub struct CurvePoller {
    client: Client,
    query_url: String,
}

impl CurvePoller {
    pub fn new(cfg: &PostGradConfig) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(8))
                .build()
                .expect("curve poller http"),
            query_url: cfg.rpc_url_with_token(&cfg.query_rpc),
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

        let value = &resp["result"]["value"];
        if value.is_null() {
            // Bonding curve PDA closed — token has migrated off the curve.
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
        cursor += 8 + 8 + 8 + 8; // real_tok, real_sol, token_total_supply
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

pub async fn run_curve_poller(cfg: Arc<PostGradConfig>, tracker: Arc<PostGradTracker>) {
    let poller = CurvePoller::new(&cfg);
    let interval = Duration::from_millis(cfg.poll_interval_ms);
    loop {
        tokio::time::sleep(interval).await;
        let mints = tracker.mints_for_poll();
        if mints.is_empty() {
            continue;
        }
        for chunk in mints.chunks(cfg.poll_batch_size) {
            for mint_str in chunk {
                let Ok(mint_pk) = mint_str.parse::<Pubkey>() else {
                    continue;
                };
                match poller.get_bonding_curve_state(&mint_pk).await {
                    Ok(curve) => {
                        tracker.on_curve_poll(
                            mint_str,
                            curve.curve_pct,
                            curve.price_sol_per_token,
                            curve.complete,
                        );
                    }
                    Err(e) => {
                        warn!(
                            "curve poll {}: {}",
                            &mint_str[..8.min(mint_str.len())],
                            e
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}