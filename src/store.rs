//! Append-only JSONL persistence for graduations, entries, exits, and stats.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::config::PostGradConfig;

pub struct PostGradStores {
    graduations: Mutex<BufWriter<File>>,
    entries: Mutex<BufWriter<File>>,
    entry_rejects: Mutex<BufWriter<File>>,
    exits: Mutex<BufWriter<File>>,
    trades: Mutex<BufWriter<File>>,
    positions: Mutex<BufWriter<File>>,
    stats: Mutex<BufWriter<File>>,
}

impl PostGradStores {
    pub fn open(cfg: &PostGradConfig) -> Result<Self> {
        Ok(Self {
            graduations: Mutex::new(open_append(&cfg.graduations_path())?),
            entries: Mutex::new(open_append(&cfg.entries_path())?),
            entry_rejects: Mutex::new(open_append(&cfg.entry_rejects_path())?),
            exits: Mutex::new(open_append(&cfg.exits_path())?),
            trades: Mutex::new(open_append(&cfg.trades_path())?),
            positions: Mutex::new(open_append(&cfg.positions_path())?),
            stats: Mutex::new(open_append(&cfg.stats_path())?),
        })
    }

    pub fn write_graduation<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.graduations, row)
    }

    pub fn write_entry<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.entries, row)
    }

    pub fn write_entry_reject<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.entry_rejects, row)
    }

    pub fn write_exit<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.exits, row)
    }

    pub fn write_trade<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.trades, row)
    }

    pub fn write_position<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.positions, row)
    }

    pub fn write_stat<T: Serialize>(&self, row: &T) -> Result<()> {
        write_row(&self.stats, row)
    }
}

fn open_append(path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn write_row<T: Serialize>(guard: &Mutex<BufWriter<File>>, row: &T) -> Result<()> {
    let mut w = guard.lock().expect("store lock");
    serde_json::to_writer(&mut *w, row)?;
    w.write_all(b"\n")?;
    w.flush()?;
    Ok(())
}