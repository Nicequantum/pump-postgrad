//! Post-graduation Raydium sniper — Shredstream gRPC + Jupiter execution.

pub mod error;

mod config;
mod execution;
mod ingest;
mod parser;
mod poller;
mod positions;
mod slot_timing;
mod store;
mod strategy;
mod tracker;
mod types;

use std::fs::OpenOptions;
use std::io::Write;
use std::os::raw::c_int;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use solana_sdk::signer::Signer;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::PostGradConfig;
use crate::execution::{JupiterSwap, RpcClient, TradingWallet};
use crate::positions::PositionManager;
use crate::store::PostGradStores;
use crate::tracker::{GraduationCandidate, PostGradTracker};

#[derive(Serialize)]
struct StatsRow {
    ts: String,
    heartbeat: String,
    open_positions: usize,
    dry_run: bool,
}

#[link(name = "c")]
extern "C" {
    fn flock(fd: c_int, op: c_int) -> c_int;
}

const LOCK_EX: c_int = 2;
const LOCK_NB: c_int = 4;

fn acquire_lock(path: &std::path::Path) {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|e| {
            eprintln!("FATAL: cannot open lock {}: {}", path.display(), e);
            std::process::exit(1);
        });
    let fd = file.as_raw_fd();
    let rc = unsafe { flock(fd, LOCK_EX | LOCK_NB) };
    if rc != 0 {
        eprintln!(
            "FATAL: another pump-postgrad-bot holds {} — exiting",
            path.display()
        );
        std::process::exit(1);
    }
    let _ = writeln!(&file, "pid={}", std::process::id());
}

pub async fn run() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("pump_postgrad=info,info")),
        )
        .with_target(false)
        .json()
        .init();

    let cfg = Arc::new(PostGradConfig::load()?);
    cfg.validate()?;
    acquire_lock(&cfg.lock_path);
    cfg.ensure_data_dir()?;

    info!(
        "pump-postgrad-bot starting | data={} | dry_run={} | grpc={} @ {} | buy={} lamports | tp={}% | sl={}% | trail={}% | hold={}s",
        cfg.data_dir.display(),
        cfg.dry_run,
        cfg.grpc_enabled,
        cfg.shredstream_grpc_url,
        cfg.buy_sol_lamports,
        cfg.take_profit_pct,
        cfg.stop_loss_pct,
        cfg.trailing_stop_pct,
        cfg.max_hold_secs,
    );

    if cfg.dry_run {
        info!("DRY_RUN=true — no on-chain transactions will be sent");
    }

    let stores = Arc::new(PostGradStores::open(&cfg)?);
    let wallet = Arc::new(TradingWallet::load(
        cfg.wallet_secret.as_deref(),
        cfg.dry_run,
    )?);
    let rpc = Arc::new(RpcClient::new(&cfg));
    let jupiter = Arc::new(JupiterSwap::new(&cfg));

    let bal = rpc
        .get_sol_balance(&wallet.keypair().pubkey())
        .await
        .unwrap_or(0);
    info!(
        "wallet {} | balance {:.4} SOL",
        wallet.pubkey_str(),
        bal as f64 / 1e9
    );

    let (grad_tx, grad_rx) = mpsc::channel::<GraduationCandidate>(512);
    let tracker = Arc::new(PostGradTracker::new(cfg.clone(), stores.clone(), grad_tx));

    let positions = Arc::new(PositionManager::new(
        cfg.clone(),
        stores.clone(),
        wallet,
        rpc,
        jupiter,
        tracker.clone(),
    ));

    let tracker_hb = tracker.clone();
    let positions_hb = positions.clone();
    let stores_hb = stores.clone();
    let cfg_hb = cfg.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(cfg_hb.heartbeat_interval_secs));
        loop {
            interval.tick().await;
            let line = tracker_hb.heartbeat_line();
            let open = positions_hb.open_count();
            info!("💓 postgrad heartbeat | {} | open={}", line, open);
            let _ = stores_hb.write_stat(&StatsRow {
                ts: Utc::now().to_rfc3339(),
                heartbeat: line,
                open_positions: open,
                dry_run: cfg_hb.dry_run,
            });
        }
    });

    let tracker_poll = tracker.clone();
    let cfg_poll = cfg.clone();
    tokio::spawn(async move {
        poller::run_curve_poller(cfg_poll, tracker_poll).await;
    });

    let positions_entry = positions.clone();
    tokio::spawn(async move {
        positions_entry.run_entry_loop(grad_rx).await;
    });

    let positions_exit = positions.clone();
    tokio::spawn(async move {
        positions_exit.run_exit_monitor().await;
    });

    ingest::run_ingest(cfg, tracker, positions).await
}