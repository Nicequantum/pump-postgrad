//! Pump.fun + Raydium constants and shared event types.

use solana_sdk::pubkey::Pubkey;

pub const PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

pub const PUMP_CREATE_DISCRIMINATOR: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
pub const PUMP_CREATE_V2_DISCRIMINATOR: [u8; 8] = [0xd6, 0x90, 0x4c, 0xec, 0x5f, 0x8b, 0x31, 0xb4];
pub const PUMP_BUY_DISCRIMINATOR: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
pub const PUMP_BUY_V2_DISCRIMINATOR: [u8; 8] = [0xb8, 0x17, 0xee, 0x61, 0x67, 0xc5, 0xd3, 0x3d];
pub const PUMP_SELL_DISCRIMINATOR: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];
pub const PUMP_SELL_V2_DISCRIMINATOR: [u8; 8] = [0x5d, 0xf6, 0x82, 0x3c, 0xe7, 0xe9, 0x40, 0xb2];
pub const PUMP_MIGRATE_DISCRIMINATOR: [u8; 8] = [155, 234, 231, 146, 236, 158, 162, 30];
pub const PUMP_MIGRATE_V2_DISCRIMINATOR: [u8; 8] = [187, 203, 18, 31, 206, 237, 254, 41];

pub const INITIAL_VIRTUAL_TOKEN: f64 = 1_073_000_191_231_177.0;

/// Valid base58 mint addresses are 32–44 chars and parse as on-curve pubkeys.
pub fn is_plausible_mint(mint: &str) -> bool {
    if mint.len() < 32 || mint.len() > 44 {
        return false;
    }
    if mint == WSOL_MINT || mint == SYSTEM_PROGRAM_ID {
        return false;
    }
    mint.parse::<Pubkey>().is_ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraduationSource {
    MigrateIx,
    CurveComplete,
    RaydiumSwap,
}

impl GraduationSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MigrateIx => "migrate_ix",
            Self::CurveComplete => "curve_complete",
            Self::RaydiumSwap => "raydium_swap",
        }
    }

    pub fn priority(self) -> u8 {
        match self {
            Self::MigrateIx => 3,
            Self::RaydiumSwap => 2,
            Self::CurveComplete => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    TakeProfit,
    TrailingStop,
    StopLoss,
    TimeStop,
    RugSignal,
    NoRoute,
    Manual,
}

impl ExitReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TakeProfit => "take_profit",
            Self::TrailingStop => "trailing_stop",
            Self::StopLoss => "stop_loss",
            Self::TimeStop => "time_stop",
            Self::RugSignal => "rug_signal",
            Self::NoRoute => "no_route",
            Self::Manual => "manual",
        }
    }
}