use anyhow::Result;
use base64::Engine as _;
use reqwest::Client;
use solana_sdk::signature::Keypair;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use std::time::Duration;
use tracing::info;

use crate::config::PostGradConfig;
use crate::execution::rpc::RpcClient;
use crate::types::WSOL_MINT;

pub struct JupiterSwap {
    http: Client,
    api_key: String,
    buy_slippage_bps: u32,
    sell_slippage_bps: u32,
    priority_fee_lamports: u64,
}

impl JupiterSwap {
    pub fn new(cfg: &PostGradConfig) -> Self {
        Self {
            http: Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("jupiter http"),
            api_key: cfg.jupiter_api_key.clone(),
            buy_slippage_bps: cfg.buy_slippage_bps,
            sell_slippage_bps: cfg.sell_slippage_bps,
            priority_fee_lamports: cfg.priority_fee_lamports,
        }
    }

    pub async fn quote_tokens_out(&self, mint: &str, sol_lamports: u64) -> Result<u64> {
        let quote = self
            .fetch_quote(WSOL_MINT, mint, sol_lamports, self.buy_slippage_bps)
            .await?;
        Ok(quote["outAmount"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    pub async fn quote_sol_out(
        &self,
        mint: &str,
        amount_tokens: u64,
    ) -> Result<u64> {
        let quote = self
            .fetch_quote(mint, WSOL_MINT, amount_tokens, self.sell_slippage_bps)
            .await?;
        Ok(quote["outAmount"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    pub async fn buy_with_sol(
        &self,
        mint: &str,
        sol_lamports: u64,
        wallet: &Keypair,
        rpc: &RpcClient,
    ) -> Result<(Signature, u64)> {
        let quote = self
            .fetch_quote(WSOL_MINT, mint, sol_lamports, self.buy_slippage_bps)
            .await?;
        let out_tokens = quote["outAmount"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        info!(
            "Jupiter buy quote: {} lamports → {} tokens ({})",
            sol_lamports,
            out_tokens,
            &mint[..8.min(mint.len())]
        );

        let sig = self.execute_swap(quote, wallet, rpc).await?;
        Ok((sig, out_tokens))
    }

    pub async fn sell_tokens(
        &self,
        mint: &str,
        amount_tokens: u64,
        wallet: &Keypair,
        rpc: &RpcClient,
    ) -> Result<(Signature, u64)> {
        let quote = self
            .fetch_quote(mint, WSOL_MINT, amount_tokens, self.sell_slippage_bps)
            .await?;
        let out_lamports = quote["outAmount"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        info!(
            "Jupiter sell quote: {} tokens → {} lamports ({})",
            amount_tokens,
            out_lamports,
            &mint[..8.min(mint.len())]
        );

        let sig = self.execute_swap(quote, wallet, rpc).await?;
        Ok((sig, out_lamports))
    }

    async fn fetch_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount: u64,
        slippage_bps: u32,
    ) -> Result<serde_json::Value> {
        let quote_url = format!(
            "https://api.jup.ag/swap/v1/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&restrictIntermediateTokens=true",
            input_mint, output_mint, amount, slippage_bps
        );
        let mut req = self.http.get(&quote_url);
        if !self.api_key.is_empty() {
            req = req.header("Authorization", format!("Bearer {}", self.api_key));
        }
        let quote: serde_json::Value = req.send().await?.json().await?;
        if let Some(err) = quote.get("error").or_else(|| quote.get("errorCode")) {
            return Err(anyhow::anyhow!("Jupiter quote failed: {}", err));
        }
        if quote.get("outAmount").is_none() {
            return Err(anyhow::anyhow!("Jupiter empty quote for {}", &input_mint[..8.min(input_mint.len())]));
        }
        Ok(quote)
    }

    async fn execute_swap(
        &self,
        quote: serde_json::Value,
        wallet: &Keypair,
        rpc: &RpcClient,
    ) -> Result<Signature> {
        let swap_body = serde_json::json!({
            "quoteResponse": quote,
            "userPublicKey": wallet.pubkey().to_string(),
            "dynamicComputeUnitLimit": true,
            "prioritizationFeeLamports": self.priority_fee_lamports,
            "wrapAndUnwrapSol": true,
        });

        let mut swap_req = self
            .http
            .post("https://api.jup.ag/swap/v1/swap")
            .json(&swap_body);
        if !self.api_key.is_empty() {
            swap_req = swap_req.header("Authorization", format!("Bearer {}", self.api_key));
        }

        let swap_resp: serde_json::Value = swap_req.send().await?.json().await?;
        let tx_b64 = swap_resp["swapTransaction"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Jupiter swap missing tx: {:?}", swap_resp))?;

        let tx_bytes = base64::engine::general_purpose::STANDARD.decode(tx_b64)?;
        let mut tx: VersionedTransaction = bincode::deserialize(&tx_bytes)
            .map_err(|e| anyhow::anyhow!("deserialize jupiter tx: {}", e))?;

        let message_bytes = tx.message.serialize();
        let sig = wallet.sign_message(&message_bytes);
        if tx.signatures.is_empty() {
            tx.signatures.push(sig);
        } else {
            tx.signatures[0] = sig;
        }

        let static_keys = tx.message.static_account_keys();
        if static_keys.first() != Some(&wallet.pubkey()) {
            return Err(anyhow::anyhow!("Jupiter fee payer mismatch"));
        }

        rpc.send_transaction(&tx).await
    }
}