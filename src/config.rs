//! Configuration for the post-graduation Raydium sniper.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct PostGradConfig {
    pub data_dir: PathBuf,
    pub lock_path: PathBuf,
    pub query_rpc: String,
    pub send_rpc: String,
    pub solana_rpc_token: String,
    pub wallet_secret: Option<String>,
    pub dry_run: bool,

    pub shredstream_grpc_url: String,
    pub shredstream_auth_token: String,
    pub grpc_enabled: bool,

    pub poll_interval_ms: u64,
    pub poll_batch_size: usize,
    pub poll_per_mint_delay_ms: u64,
    pub poll_rpc_retries: u32,
    pub poll_error_backoff_ms: u64,
    pub min_curve_pct_to_watch: f64,
    pub min_curve_pct_complete: f64,
    pub max_tracked_tokens: usize,
    pub token_activity_ttl_secs: u64,
    pub tracker_prune_interval_secs: u64,

    pub buy_sol_lamports: u64,
    pub min_wallet_sol_lamports: u64,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub trailing_stop_pct: f64,
    pub max_hold_secs: u64,
    pub grad_snipe_delay_ms: u64,
    pub max_open_positions: usize,
    pub min_unique_buyers: u32,
    pub min_unique_buyers_migrate: u32,
    pub min_buy_sell_ratio: f64,
    pub min_trades_curve_complete: u32,
    pub max_positions_per_hour: u32,
    pub grad_dedup_secs: u64,
    pub rug_min_sell_count: u32,
    pub rug_sell_lamport_divisor: u64,

    pub jupiter_api_key: String,
    pub buy_slippage_bps: u32,
    pub sell_slippage_bps: u32,
    pub priority_fee_lamports: u64,
    pub tx_confirm_timeout_ms: u64,

    pub heartbeat_interval_secs: u64,
    pub position_poll_ms: u64,
    pub position_snapshot_ms: u64,
    pub ingest_reconnect_delay_ms: u64,
    pub ingest_max_reconnect_ms: u64,
    pub ingest_channel_capacity: usize,
    pub ingest_stall_warn_secs: u64,
    pub grad_channel_capacity: usize,
    pub max_grad_age_ms: u64,
    pub jupiter_quote_retries: u32,
    pub sell_max_retries: u32,
    pub sol_reserve_lamports: u64,
}

impl PostGradConfig {
    pub fn load() -> Result<Self> {
        dotenvy::dotenv().ok();

        let dry_run = std::env::var("DRY_RUN")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(true);

        let wallet_secret = std::env::var("POSTGRAD_WALLET")
            .ok()
            .or_else(|| std::env::var("WALLET_PRIVATE_KEY").ok())
            .or_else(|| {
                std::env::var("WALLETS").ok().and_then(|w| {
                    let first = w.split(',').next()?.trim();
                    if first.is_empty() {
                        None
                    } else {
                        Some(first.to_string())
                    }
                })
            });

        if !dry_run && wallet_secret.as_ref().is_none_or(|s| s.is_empty()) {
            bail!("POSTGRAD_WALLET required when DRY_RUN=false");
        }

        let data_dir = PathBuf::from(
            std::env::var("POSTGRAD_DATA_DIR")
                .unwrap_or_else(|_| "/root/pump-postgrad/data".to_string()),
        );

        let query_rpc = std::env::var("QUERY_RPC")
            .or_else(|_| std::env::var("SOLANA_RPC_URL"))
            .or_else(|_| std::env::var("Solana_RPC"))
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

        let send_rpc = std::env::var("SEND_RPC")
            .or_else(|_| std::env::var("BEAM_RPC"))
            .unwrap_or_else(|_| query_rpc.clone());

        let shredstream_grpc_url = std::env::var("SHREDSTREAM_GRPC_URL")
            .or_else(|_| std::env::var("SHREDSTREAM_RAW_GRPC_URL"))
            .unwrap_or_else(|_| "https://solana-shredstream-grpc.rpcfast.com:443".to_string());

        let grpc_enabled = std::env::var("POSTGRAD_GRPC_FALLBACK")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(true);

        Ok(Self {
            data_dir,
            lock_path: PathBuf::from(
                std::env::var("POSTGRAD_LOCK")
                    .unwrap_or_else(|_| "/root/.pump_postgrad.lock".to_string()),
            ),
            query_rpc,
            send_rpc,
            solana_rpc_token: std::env::var("SOLANA_RPC_TOKEN").unwrap_or_default(),
            wallet_secret,
            dry_run,
            shredstream_grpc_url,
            shredstream_auth_token: std::env::var("SHREDSTREAM_AUTH_TOKEN")
                .or_else(|_| std::env::var("SHREDSTREAM_GRPC_TOKEN"))
                .unwrap_or_default(),
            grpc_enabled,
            poll_interval_ms: env_u64("POSTGRAD_POLL_INTERVAL_MS", 2_000),
            poll_batch_size: env_usize("POSTGRAD_POLL_BATCH_SIZE", 30),
            poll_per_mint_delay_ms: env_u64("POSTGRAD_POLL_PER_MINT_MS", 20),
            poll_rpc_retries: env_u32("POSTGRAD_POLL_RPC_RETRIES", 2),
            poll_error_backoff_ms: env_u64("POSTGRAD_POLL_ERROR_BACKOFF_MS", 500),
            min_curve_pct_to_watch: env_f64("POSTGRAD_MIN_CURVE_PCT", 85.0),
            min_curve_pct_complete: env_f64("POSTGRAD_MIN_CURVE_PCT_COMPLETE", 99.0),
            max_tracked_tokens: env_usize("POSTGRAD_MAX_TRACKED", 5_000),
            token_activity_ttl_secs: env_u64("POSTGRAD_TOKEN_TTL_SECS", 3_600),
            tracker_prune_interval_secs: env_u64("POSTGRAD_TRACKER_PRUNE_SECS", 300),
            buy_sol_lamports: env_u64("POSTGRAD_BUY_SOL_LAMPORTS", 50_000_000),
            min_wallet_sol_lamports: env_u64("POSTGRAD_MIN_WALLET_SOL", 80_000_000),
            take_profit_pct: env_f64("POSTGRAD_TAKE_PROFIT_PCT", 25.0),
            stop_loss_pct: env_f64("POSTGRAD_STOP_LOSS_PCT", 12.0),
            trailing_stop_pct: env_f64("POSTGRAD_TRAILING_STOP_PCT", 8.0),
            max_hold_secs: env_u64("POSTGRAD_MAX_HOLD_SECS", 90),
            grad_snipe_delay_ms: env_u64("POSTGRAD_SNIPE_DELAY_MS", 400),
            max_open_positions: env_usize("POSTGRAD_MAX_OPEN_POSITIONS", 3),
            min_unique_buyers: env_u32("POSTGRAD_MIN_UNIQUE_BUYERS", 15),
            min_unique_buyers_migrate: env_u32("POSTGRAD_MIN_UNIQUE_BUYERS_MIGRATE", 8),
            min_buy_sell_ratio: env_f64("POSTGRAD_MIN_BUY_SELL_RATIO", 1.5),
            min_trades_curve_complete: env_u32("POSTGRAD_MIN_TRADES_CURVE_COMPLETE", 25),
            max_positions_per_hour: env_u32("POSTGRAD_MAX_POSITIONS_PER_HOUR", 12),
            grad_dedup_secs: env_u64("POSTGRAD_GRAD_DEDUP_SECS", 300),
            rug_min_sell_count: env_u32("POSTGRAD_RUG_MIN_SELL_COUNT", 3),
            rug_sell_lamport_divisor: env_u64("POSTGRAD_RUG_SELL_LAMPORT_DIV", 2),
            jupiter_api_key: std::env::var("JUPITER_API_KEY").unwrap_or_default(),
            buy_slippage_bps: env_u32("POSTGRAD_BUY_SLIPPAGE_BPS", 3000),
            sell_slippage_bps: env_u32("POSTGRAD_SELL_SLIPPAGE_BPS", 5000),
            priority_fee_lamports: env_u64("POSTGRAD_PRIORITY_FEE_LAMPORTS", 80_000),
            tx_confirm_timeout_ms: env_u64("POSTGRAD_TX_CONFIRM_MS", 25_000),
            heartbeat_interval_secs: env_u64("POSTGRAD_HEARTBEAT_SECS", 30),
            position_poll_ms: env_u64("POSTGRAD_POSITION_POLL_MS", 500),
            position_snapshot_ms: env_u64("POSTGRAD_POSITION_SNAPSHOT_MS", 5_000),
            ingest_reconnect_delay_ms: env_u64("POSTGRAD_INGEST_RECONNECT_MS", 3_000),
            ingest_max_reconnect_ms: env_u64("POSTGRAD_INGEST_MAX_RECONNECT_MS", 60_000),
            ingest_channel_capacity: env_usize("POSTGRAD_INGEST_CHANNEL", 4096),
            ingest_stall_warn_secs: env_u64("POSTGRAD_INGEST_STALL_SECS", 30),
            grad_channel_capacity: env_usize("POSTGRAD_GRAD_CHANNEL", 512),
            max_grad_age_ms: env_u64("POSTGRAD_MAX_GRAD_AGE_MS", 30_000),
            jupiter_quote_retries: env_u32("POSTGRAD_JUPITER_QUOTE_RETRIES", 3),
            sell_max_retries: env_u32("POSTGRAD_SELL_MAX_RETRIES", 3),
            sol_reserve_lamports: env_u64("POSTGRAD_SOL_RESERVE_LAMPORTS", 30_000_000),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.buy_sol_lamports == 0 {
            bail!("POSTGRAD_BUY_SOL_LAMPORTS must be > 0");
        }
        if self.min_wallet_sol_lamports < self.buy_sol_lamports.saturating_add(self.sol_reserve_lamports)
        {
            bail!(
                "POSTGRAD_MIN_WALLET_SOL must cover buy + reserve ({} + {})",
                self.buy_sol_lamports,
                self.sol_reserve_lamports
            );
        }
        if self.take_profit_pct <= 0.0 || self.stop_loss_pct <= 0.0 {
            bail!("take_profit and stop_loss must be > 0");
        }
        if self.trailing_stop_pct <= 0.0 || self.trailing_stop_pct >= self.take_profit_pct {
            bail!("trailing_stop must be > 0 and < take_profit");
        }
        if self.buy_slippage_bps == 0 || self.buy_slippage_bps > 10_000 {
            bail!("POSTGRAD_BUY_SLIPPAGE_BPS must be in (0, 10000]");
        }
        if self.sell_slippage_bps == 0 || self.sell_slippage_bps > 10_000 {
            bail!("POSTGRAD_SELL_SLIPPAGE_BPS must be in (0, 10000]");
        }
        if self.max_open_positions == 0 {
            bail!("POSTGRAD_MAX_OPEN_POSITIONS must be > 0");
        }
        if self.min_curve_pct_to_watch < 0.0 || self.min_curve_pct_to_watch > 100.0 {
            bail!("POSTGRAD_MIN_CURVE_PCT must be in [0, 100]");
        }
        if self.grpc_enabled && self.shredstream_auth_token.is_empty() {
            bail!("SHREDSTREAM_AUTH_TOKEN required for gRPC ingest");
        }
        if self.ingest_channel_capacity == 0 || self.grad_channel_capacity == 0 {
            bail!("ingest and grad channel capacities must be > 0");
        }
        if !self.dry_run && self.jupiter_api_key.is_empty() {
            tracing::warn!("JUPITER_API_KEY empty — swap API may rate-limit or reject");
        }
        Ok(())
    }

    pub fn ensure_data_dir(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("create data dir {}", self.data_dir.display()))
    }

    pub fn graduations_path(&self) -> PathBuf {
        self.data_dir.join("graduations.jsonl")
    }

    pub fn entries_path(&self) -> PathBuf {
        self.data_dir.join("entries.jsonl")
    }

    pub fn entry_rejects_path(&self) -> PathBuf {
        self.data_dir.join("entry_rejects.jsonl")
    }

    pub fn exits_path(&self) -> PathBuf {
        self.data_dir.join("exits.jsonl")
    }

    pub fn trades_path(&self) -> PathBuf {
        self.data_dir.join("trades.jsonl")
    }

    pub fn positions_path(&self) -> PathBuf {
        self.data_dir.join("positions.jsonl")
    }

    pub fn stats_path(&self) -> PathBuf {
        self.data_dir.join("stats.jsonl")
    }

    pub fn rpc_url_with_token(&self, base: &str) -> String {
        if self.solana_rpc_token.is_empty() || base.contains(&self.solana_rpc_token) {
            base.to_string()
        } else if base.contains('?') {
            format!("{}&api_key={}", base, self.solana_rpc_token)
        } else {
            format!("{}?api_key={}", base, self.solana_rpc_token)
        }
    }

    pub fn summary_line(&self) -> String {
        format!(
            "dry_run={} buy={}lam tp={}% sl={}% max_open={} poll={}ms ingest_ch={}",
            self.dry_run,
            self.buy_sol_lamports,
            self.take_profit_pct,
            self.stop_loss_pct,
            self.max_open_positions,
            self.poll_interval_ms,
            self.ingest_channel_capacity,
        )
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}