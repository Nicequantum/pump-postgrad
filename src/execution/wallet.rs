use anyhow::{Context, Result};
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tracing::info;

pub struct TradingWallet {
    keypair: Keypair,
    dry_run: bool,
}

impl TradingWallet {
    pub fn load(secret: Option<&str>, dry_run: bool) -> Result<Self> {
        let keypair = match secret.filter(|s| !s.is_empty()) {
            Some(s) => {
                let bytes = bs58::decode(s.trim()).into_vec().context("decode wallet")?;
                Keypair::try_from(&bytes[..]).context("invalid keypair bytes")?
            }
            None if dry_run => {
                info!("DRY_RUN: using ephemeral wallet (no POSTGRAD_WALLET)");
                Keypair::new()
            }
            None => anyhow::bail!("wallet secret required"),
        };
        info!(
            "wallet loaded: {} (dry_run={})",
            keypair.pubkey(),
            dry_run
        );
        Ok(Self { keypair, dry_run })
    }

    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    pub fn pubkey_str(&self) -> String {
        self.keypair.pubkey().to_string()
    }

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }
}