//! Shredstream ingest — gRPC SubscribeEntries with event dispatch and reconnect.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use solana_entry::entry::Entry;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::PostGradConfig;
use crate::parser::{parse_entries, GraduationEvent, LaunchEvent, ParsedBatch, TradeEvent};
use crate::types::PUMP_PROGRAM_ID;

/// Dispatches parsed shred events to downstream modules.
pub trait EventSink: Send + Sync {
    fn on_launch(&self, launch: LaunchEvent);
    fn on_trade(&self, trade: TradeEvent);
    fn on_graduation(&self, grad: GraduationEvent);
}

#[derive(Debug, Default)]
pub struct IngestMetrics {
    pub batches: AtomicU64,
    pub launches: AtomicU64,
    pub trades: AtomicU64,
    pub graduations: AtomicU64,
    pub deserialize_errors: AtomicU64,
    pub reconnects: AtomicU64,
    pub channel_backpressure: AtomicU64,
    pub last_batch_ms: AtomicU64,
}

impl IngestMetrics {
    pub fn summary(&self) -> String {
        format!(
            "batches={} launches={} trades={} grads={} deser_err={} reconnects={} backpressure={}",
            self.batches.load(Ordering::Relaxed),
            self.launches.load(Ordering::Relaxed),
            self.trades.load(Ordering::Relaxed),
            self.graduations.load(Ordering::Relaxed),
            self.deserialize_errors.load(Ordering::Relaxed),
            self.reconnects.load(Ordering::Relaxed),
            self.channel_backpressure.load(Ordering::Relaxed),
        )
    }

    pub fn record_batch(&self) {
        self.last_batch_ms.store(crate::slot_timing::now_ms(), Ordering::Relaxed);
    }

    pub fn stall_secs(&self) -> u64 {
        let last = self.last_batch_ms.load(Ordering::Relaxed);
        if last == 0 {
            return 0;
        }
        crate::slot_timing::now_ms().saturating_sub(last) / 1000
    }
}

struct IngestProcessor {
    sink: Arc<dyn EventSink>,
    metrics: Arc<IngestMetrics>,
}

impl IngestProcessor {
    fn dispatch(&self, batch: ParsedBatch) {
        self.metrics.batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.record_batch();
        self.metrics
            .launches
            .fetch_add(batch.launches.len() as u64, Ordering::Relaxed);
        self.metrics
            .trades
            .fetch_add(batch.trades.len() as u64, Ordering::Relaxed);
        self.metrics
            .graduations
            .fetch_add(batch.graduations.len() as u64, Ordering::Relaxed);

        for launch in batch.launches {
            self.sink.on_launch(launch);
        }
        for trade in batch.trades {
            self.sink.on_trade(trade);
        }
        for grad in batch.graduations {
            self.sink.on_graduation(grad);
        }
    }
}

pub async fn run_ingest(
    cfg: Arc<PostGradConfig>,
    sink: Arc<dyn EventSink>,
    metrics: Arc<IngestMetrics>,
) -> Result<()> {
    let capacity = cfg.ingest_channel_capacity;
    let (entry_tx, mut entry_rx) = mpsc::channel::<(Vec<Entry>, u64, u64, String)>(capacity);

    let processor = Arc::new(IngestProcessor {
        sink,
        metrics: metrics.clone(),
    });

    let proc = processor.clone();
    tokio::spawn(async move {
        while let Some((entries, slot, receive_ms, feed)) = entry_rx.recv().await {
            if slot > 0 {
                crate::slot_timing::record_slot(slot);
            }
            let batch = parse_entries(entries, slot, receive_ms, &feed);
            proc.dispatch(batch);
        }
        warn!("ingest processor channel closed");
    });

    let stall_cfg = cfg.clone();
    let stall_metrics = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let stall = stall_metrics.stall_secs();
            if stall >= stall_cfg.ingest_stall_warn_secs && stall_metrics.batches.load(Ordering::Relaxed) > 0
            {
                warn!(
                    "ingest stall: no batches for {}s (threshold {}s)",
                    stall,
                    stall_cfg.ingest_stall_warn_secs
                );
            }
        }
    });

    if !(cfg.grpc_enabled && !cfg.shredstream_auth_token.is_empty()) {
        anyhow::bail!(
            "No ingest source — set POSTGRAD_GRPC_FALLBACK=true and SHREDSTREAM_AUTH_TOKEN in .env"
        );
    }

    info!(
        "postgrad ingest starting (grpc @ {} | channel={})",
        cfg.shredstream_grpc_url,
        capacity
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
    let connected_at = Instant::now();

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
        match tx.try_send((entries, msg.slot, receive_ms, feed.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(payload)) => {
                metrics.channel_backpressure.fetch_add(1, Ordering::Relaxed);
                if tx.send(payload).await.is_err() {
                    break;
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
    }

    if connected_at.elapsed() < Duration::from_secs(2) {
        return Err(anyhow::anyhow!("gRPC stream ended immediately after connect"));
    }
    Ok(())
}