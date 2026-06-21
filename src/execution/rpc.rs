use anyhow::{Context, Result};
use base64::Engine as _;
use reqwest::Client;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::PostGradConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignaturePoll {
    Pending,
    Confirmed,
    Reverted(String),
}

pub struct RpcClient {
    client: Client,
    query_url: String,
    send_url: String,
}

impl RpcClient {
    pub fn new(cfg: &PostGradConfig) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(12))
                .build()
                .expect("rpc client"),
            query_url: cfg.rpc_url_with_token(&cfg.query_rpc),
            send_url: cfg.rpc_url_with_token(&cfg.send_rpc),
        }
    }

    pub async fn get_sol_balance(&self, pubkey: &Pubkey) -> Result<u64> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [pubkey.to_string(), {"commitment": "confirmed"}]
        });
        let resp: Value = self.client.post(&self.query_url).json(&body).send().await?.json().await?;
        Ok(resp["result"]["value"].as_u64().unwrap_or(0))
    }

    /// Sum token balances for `mint` across all token accounts owned by `owner`.
    pub async fn get_mint_balance(&self, owner: &Pubkey, mint: &str) -> Result<u64> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner.to_string(),
                { "mint": mint },
                { "encoding": "jsonParsed", "commitment": "confirmed" }
            ]
        });
        let resp: Value = self.client.post(&self.query_url).json(&body).send().await?.json().await?;
        let mut total = 0u64;
        if let Some(arr) = resp["result"]["value"].as_array() {
            for item in arr {
                if let Some(amt) = item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
                    .as_str()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    total = total.saturating_add(amt);
                }
            }
        }
        Ok(total)
    }

    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> Result<Signature> {
        let serialized = bincode::serialize(tx).context("serialize tx")?;
        let tx_base64 = base64::engine::general_purpose::STANDARD.encode(serialized);
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "skipPreflight": true,
                    "encoding": "base64",
                    "maxRetries": 0
                }
            ]
        });

        for attempt in 1..=3 {
            match self.client.post(&self.send_url).json(&payload).send().await {
                Ok(resp) => {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(sig_str) = json["result"].as_str() {
                            let sig: Signature = sig_str.parse().context("bad sig")?;
                            info!("tx sent: {}", sig);
                            return Ok(sig);
                        } else if let Some(err) = json.get("error") {
                            error!("RPC send error: {:?}", err);
                            return Err(anyhow::anyhow!("RPC send error: {}", err));
                        }
                    }
                }
                Err(e) => {
                    warn!("send attempt {} failed: {}", attempt, e);
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
        Err(anyhow::anyhow!("send failed after 3 attempts"))
    }

    pub async fn poll_signature(&self, sig: &Signature) -> SignaturePoll {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [[sig.to_string()], {"searchTransactionHistory": true}]
        });
        let resp = match self.client.post(&self.query_url).json(&payload).send().await {
            Ok(r) => r,
            Err(_) => return SignaturePoll::Pending,
        };
        let json: Value = match resp.json().await {
            Ok(v) => v,
            Err(_) => return SignaturePoll::Pending,
        };
        let entry = &json["result"]["value"][0];
        if entry.is_null() {
            return SignaturePoll::Pending;
        }
        if !entry["err"].is_null() {
            return SignaturePoll::Reverted(entry["err"].to_string());
        }
        let status = entry["confirmationStatus"].as_str().unwrap_or("");
        if status == "confirmed" || status == "finalized" {
            SignaturePoll::Confirmed
        } else {
            SignaturePoll::Pending
        }
    }

    pub async fn wait_for_confirmation(
        &self,
        sig: &Signature,
        timeout_ms: u64,
    ) -> Result<SignaturePoll> {
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        while std::time::Instant::now() < deadline {
            match self.poll_signature(sig).await {
                SignaturePoll::Confirmed => return Ok(SignaturePoll::Confirmed),
                SignaturePoll::Reverted(e) => return Ok(SignaturePoll::Reverted(e)),
                SignaturePoll::Pending => sleep(Duration::from_millis(400)).await,
            }
        }
        Ok(SignaturePoll::Pending)
    }
}