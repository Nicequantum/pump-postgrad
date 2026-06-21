//! Shredstream ingest — gRPC SubscribeEntries with automatic reconnect.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use solana_entry::entry::Entry;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::PostGradConfig;
use crate::parser::parse_entries;
use crate::positions::PositionManager;
use crate::tracker::PostGradTracker;
use crate::types::PUMP_PROGRAM_ID;

#[derive(Debug, Default)]
pub struct IngestMetrics {
    pub batches: AtomicU64,
    pub launches: AtomicU64,
    pub trades: AtomicU64,
    pub graduations: AtomicU64,
    pub deserialize_errors: AtomicU64,
    pub reconnects: AtomicU64,
}

impl IngestMetrics {
    pub fn summary(&self) -> String {
        format!(
            "batches={} launches={} trades={} grads={} deser_err={} reconnects={}",
            self.batches.load(Ordering::Relaxed),
            self.launches.load(Ordering::Relaxed),
            self.trades.load(Ordering::Relaxed),
            self.graduations.load(Ordering::Relaxed),
            self.deserialize_errors.load(Ordering::Relaxed),
            self.reconnects.load(Ordering::Relaxed),
        )
    }
}

pub async fn run_ingest(
    cfg: Arc<PostGradConfig>,
    tracker: Arc<PostGradTracker>,
    positions: Arc<PositionManager>,
    metrics: Arc<IngestMetrics>,
) -> Result<()> {
    let (entry_tx, mut entry_rx) = mpsc::channel::<(Vec<Entry>, u64, u64, String)>(4096);

    let tracker_proc = tracker.clone();
    let positions_proc = positions.clone();
    let metrics_proc = metrics.clone();
    tokio::spawn(async move {
        while let Some((entries, slot, receive_ms, feed)) = entry_rx.recv().await {
            if slot > 0 {
                crate::slot_timing::record_slot(slot);
            }
            let batch = parse_entries(entries, slot, receive_ms, &feed);
            metrics_proc.batches.fetch_add(1, Ordering::Relaxed);
            metrics_proc
                .launches
                .fetch_add(batch.launches.len() as u64, Ordering::Relaxed);
            metrics_proc
                .trades
                .fetch_add(batch.trades.len() as u64, Ordering::Relaxed);
            metrics_proc
                .graduations
                .fetch_add(batch.graduations.len() as u64, Ordering::Relaxed);

            for launch in batch.launches {
                tracker_proc.on_launch(launch);
            }
            for trade in batch.trades {
                positions_proc.on_trade(&trade);
                tracker_proc.on_trade(trade);
            }
            for grad in batch.graduations {
                tracker_proc.on_graduation_shred(grad);
            }
        }
        warn!("ingest processor channel closed");
    });

    if !(cfg.grpc_enabled && !cfg.shredstream_auth_token.is_empty()) {
        anyhow::bail!(
            "No ingest source — set POSTGRAD_GRPC_FALLBACK=true and SHREDSTREAM_AUTH_TOKEN in .env"
        );
    }

    info!(
        "postgrad ingest starting (grpc @ {})",
        cfg.shredstream_grpc_url
    );

    let base_delay = cfg.ingest_reconnect_delay_ms;
    let mut backoff_ms = base_delay;
    loop {
        let result = run_grpc_entries(
            cfg.shredstream_grpc_url.clone(),
            cfg.shredstream_auth_token.clone(),
            entry_tx.clone(),
            metrics.clone(),
            "postgrad_grpc".to_string(),
        )
        .await;

        metrics.reconnects.fetch_add(1, Ordering::Relaxed);
        match &result {
            Ok(()) => {
                warn!("gRPC stream ended cleanly — reconnecting in {}ms", base_delay);
                backoff_ms = base_delay;
            }
            Err(e) => {
                warn!("gRPC stream error: {:#} — reconnecting in {}ms", e, backoff_ms);
            }
        }

        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        if result.is_err() {
            backoff_ms = (backoff_ms.saturating_mul(2)).min(cfg.ingest_max_reconnect_ms);
        }
    }
}

async fn run_grpc_entries(
    url: String,
    token: String,
    tx: mpsc::Sender<(Vec<Entry>, u64, u64, String)>,
    metrics: Arc<IngestMetrics>,
    feed: String,
) -> Result<()> {
    use solana_stream_sdk::{CommitmentLevel, ShredstreamClient, SubscribeEntriesRequest};

    info!("gRPC SubscribeEntries connecting @ {}", url);
    let pump_program: solana_sdk::pubkey::Pubkey =
        PUMP_PROGRAM_ID.parse().context("pump program id")?;

    let client = ShredstreamClient::connect(&url).with_x_token(&token);
    let req = SubscribeEntriesRequest::new()
        .add_account(pump_program)
        .commitment(CommitmentLevel::Processed);

    let mut stream = client
        .subscribe_entries(req)
        .await
        .context("subscribe_entries")?;

    info!("gRPC SubscribeEntries active");

    while let Ok(Some(msg)) = stream.message().await {
        let receive_ms = crate::slot_timing::now_ms();
        let entries: Vec<Entry> = match bincode::deserialize(&msg.entries) {
            Ok(e) => e,
            Err(e) => {
                metrics.deserialize_errors.fetch_add(1, Ordering::Relaxed);
                warn!("grpc deserialize: {}", e);
                continue;
            }
        };
        if tx
            .send((entries, msg.slot, receive_ms, feed.clone()))
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(())
}