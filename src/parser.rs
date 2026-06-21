//! Pump.fun + Raydium instruction parser for shred Entry batches.

use once_cell::sync::Lazy;
use solana_entry::entry::Entry;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

use crate::types::{
    PUMP_BUY_DISCRIMINATOR, PUMP_BUY_V2_DISCRIMINATOR, PUMP_CREATE_DISCRIMINATOR,
    PUMP_CREATE_V2_DISCRIMINATOR, PUMP_MIGRATE_DISCRIMINATOR, PUMP_MIGRATE_V2_DISCRIMINATOR,
    PUMP_PROGRAM_ID, PUMP_SELL_DISCRIMINATOR, PUMP_SELL_V2_DISCRIMINATOR, RAYDIUM_AMM_V4,
    TradeSide,
};

static PUMP_PROGRAM: Lazy<Pubkey> =
    Lazy::new(|| PUMP_PROGRAM_ID.parse().expect("PUMP_PROGRAM_ID"));
static RAYDIUM_PROGRAM: Lazy<Pubkey> =
    Lazy::new(|| RAYDIUM_AMM_V4.parse().expect("RAYDIUM_AMM_V4"));

#[derive(Debug, Clone)]
pub struct LaunchEvent {
    pub mint: String,
    pub creator: String,
    pub signature: String,
    pub slot: u64,
    pub receive_ms: u64,
    pub feed: String,
}

#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub mint: String,
    pub trader: String,
    pub side: TradeSide,
    pub amount_lamports: Option<u64>,
    pub signature: String,
    pub slot: u64,
    pub receive_ms: u64,
    pub feed: String,
}

#[derive(Debug, Clone)]
pub struct GraduationEvent {
    pub mint: String,
    pub signature: String,
    pub slot: u64,
    pub receive_ms: u64,
    pub feed: String,
    pub source: crate::types::GraduationSource,
}

#[derive(Debug, Default)]
pub struct ParsedBatch {
    pub launches: Vec<LaunchEvent>,
    pub trades: Vec<TradeEvent>,
    pub graduations: Vec<GraduationEvent>,
}

fn ix_discriminator(data: &[u8]) -> Option<[u8; 8]> {
    if data.len() < 8 {
        return None;
    }
    let mut d = [0u8; 8];
    d.copy_from_slice(&data[..8]);
    Some(d)
}

fn account_key(accounts: &[Pubkey], idx: usize) -> Option<String> {
    accounts.get(idx).map(|p| p.to_string())
}

fn trade_mint(accounts: &[Pubkey], ix_accounts: &[u8]) -> Option<String> {
    let mint_idx = *ix_accounts.get(2)? as usize;
    account_key(accounts, mint_idx)
}

fn migrate_mint(accounts: &[Pubkey], ix_accounts: &[u8]) -> Option<String> {
    // pump migrate / migrate_v2: mint at index 2 per IDL
    let mint_idx = *ix_accounts.get(2)? as usize;
    account_key(accounts, mint_idx)
}

fn create_mint(accounts: &[Pubkey], ix_accounts: &[u8], create_v2: bool) -> Option<String> {
    if create_v2 {
        ix_accounts
            .first()
            .and_then(|&i| account_key(accounts, i as usize))
    } else {
        ix_accounts
            .first()
            .and_then(|&i| account_key(accounts, i as usize))
    }
}

fn amount_after_discriminator(data: &[u8]) -> Option<u64> {
    if data.len() < 16 {
        return None;
    }
    Some(u64::from_le_bytes(data[8..16].try_into().ok()?))
}

/// Raydium AMM v4 swap: tag 9/11. Mint is account index 15 (coin_mint) in standard layout.
fn raydium_swap_mint(accounts: &[Pubkey], ix_accounts: &[u8]) -> Option<String> {
    const COIN_MINT_IX: usize = 15;
    if let Some(&idx) = ix_accounts.get(COIN_MINT_IX) {
        if let Some(pk) = account_key(accounts, idx as usize) {
            if pk != crate::types::WSOL_MINT {
                return Some(pk);
            }
        }
    }
    None
}

fn process_pump_ix(
    out: &mut ParsedBatch,
    accounts: &[Pubkey],
    ix_accounts: &[u8],
    data: &[u8],
    trader: &str,
    sig: &str,
    slot: u64,
    receive_ms: u64,
    feed: &str,
) {
    let disc = match ix_discriminator(data) {
        Some(d) => d,
        None => return,
    };

    if disc == PUMP_CREATE_DISCRIMINATOR {
        if let Some(mint) = create_mint(accounts, ix_accounts, false) {
            out.launches.push(LaunchEvent {
                mint,
                creator: trader.to_string(),
                signature: sig.to_string(),
                slot,
                receive_ms,
                feed: feed.to_string(),
            });
        }
        return;
    }

    if disc == PUMP_CREATE_V2_DISCRIMINATOR {
        if let Some(mint) = create_mint(accounts, ix_accounts, true) {
            out.launches.push(LaunchEvent {
                mint,
                creator: trader.to_string(),
                signature: sig.to_string(),
                slot,
                receive_ms,
                feed: feed.to_string(),
            });
        }
        return;
    }

    if disc == PUMP_MIGRATE_DISCRIMINATOR || disc == PUMP_MIGRATE_V2_DISCRIMINATOR {
        if let Some(mint) = migrate_mint(accounts, ix_accounts) {
            out.graduations.push(GraduationEvent {
                mint,
                signature: sig.to_string(),
                slot,
                receive_ms,
                feed: feed.to_string(),
                source: crate::types::GraduationSource::MigrateIx,
            });
        }
        return;
    }

    let side = if disc == PUMP_BUY_DISCRIMINATOR || disc == PUMP_BUY_V2_DISCRIMINATOR {
        TradeSide::Buy
    } else if disc == PUMP_SELL_DISCRIMINATOR || disc == PUMP_SELL_V2_DISCRIMINATOR {
        TradeSide::Sell
    } else {
        return;
    };

    if let Some(mint) = trade_mint(accounts, ix_accounts) {
        out.trades.push(TradeEvent {
            mint,
            trader: trader.to_string(),
            side,
            amount_lamports: amount_after_discriminator(data),
            signature: sig.to_string(),
            slot,
            receive_ms,
            feed: feed.to_string(),
        });
    }
}

pub fn parse_entries(
    entries: Vec<Entry>,
    slot: u64,
    receive_ms: u64,
    feed: &str,
) -> ParsedBatch {
    let mut out = ParsedBatch::default();
    let feed = feed.to_string();

    for entry in entries {
        for tx in entry.transactions {
            let accounts = tx.message.static_account_keys();
            let sig = tx
                .signatures
                .first()
                .map(|s| s.to_string())
                .unwrap_or_else(|| Signature::default().to_string());
            let trader = accounts
                .first()
                .map(|p| p.to_string())
                .unwrap_or_default();

            for ix in tx.message.instructions() {
                let program = match accounts.get(ix.program_id_index as usize) {
                    Some(p) => *p,
                    None => continue,
                };

                if program == *PUMP_PROGRAM {
                    process_pump_ix(
                        &mut out,
                        accounts,
                        &ix.accounts,
                        &ix.data,
                        &trader,
                        &sig,
                        slot,
                        receive_ms,
                        &feed,
                    );
                    continue;
                }

                if program == *RAYDIUM_PROGRAM && !ix.data.is_empty() {
                    let tag = ix.data[0];
                    if tag == 9 || tag == 11 {
                        if let Some(mint) = raydium_swap_mint(accounts, &ix.accounts) {
                            out.graduations.push(GraduationEvent {
                                mint,
                                signature: sig.clone(),
                                slot,
                                receive_ms,
                                feed: feed.clone(),
                                source: crate::types::GraduationSource::RaydiumSwap,
                            });
                        }
                    }
                }
            }
        }
    }

    out
}