//! Event routing — connects ingest parser output to tracker and positions.

use std::sync::Arc;

use crate::ingest::EventSink;
use crate::parser::{GraduationEvent, LaunchEvent, TradeEvent};
use crate::positions::PositionManager;
use crate::tracker::PostGradTracker;

pub struct EventPipeline {
    tracker: Arc<PostGradTracker>,
    positions: Arc<PositionManager>,
}

impl EventPipeline {
    pub fn new(tracker: Arc<PostGradTracker>, positions: Arc<PositionManager>) -> Self {
        Self { tracker, positions }
    }
}

impl EventSink for EventPipeline {
    fn on_launch(&self, launch: LaunchEvent) {
        self.tracker.on_launch(launch);
    }

    fn on_trade(&self, trade: TradeEvent) {
        self.positions.on_trade(&trade);
        self.tracker.on_trade(trade);
    }

    fn on_graduation(&self, grad: GraduationEvent) {
        self.tracker.on_graduation_shred(grad);
    }
}