//! solana-stream-sdk
//!
//! Lightweight client for RPC Fast / custom Shredstream gRPC endpoints
//! that stream raw Solana `Entry` data (lowest latency path).
//!
//! Designed to match the usage pattern recommended by the RPC Fast team:
//!
//! ```ignore
//! use solana_stream_sdk::{ShredstreamClient, SubscribeEntriesRequest, CommitmentLevel};
//!
//! let mut client = ShredstreamClient::connect(&url)
//!     .await?
//!     .with_x_token(&token);
//!
//! let req = SubscribeEntriesRequest::new()
//!     .add_account(pump_program)
//!     .commitment(CommitmentLevel::Processed);
//!
//! let mut stream = client.subscribe_entries(req).await?;
//!
//! while let Some(msg) = stream.message().await? {
//!     let entries: Vec<solana_entry::entry::Entry> = bincode::deserialize(&msg.entries)?;
//!     ...
//! }
//! ```

use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use tonic::service::interceptor::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status};
use tracing::warn;

// Service renamed to `ShredstreamProxy` in proto/shredstream.proto → tonic now generates
// `shredstream_proxy_client::ShredstreamProxyClient` (dials /shredstream.ShredstreamProxy/SubscribeEntries).
pub use crate::shredstream::shredstream_proxy_client::ShredstreamProxyClient as RawClient;
pub use crate::shredstream::{SubscribeEntriesRequest as ProtoRequest, SubscribeEntriesResponse};

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

/// Commitment level for the subscription.
#[derive(Clone, Copy, Debug, Default)]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    #[default]
    Finalized,
}

impl From<CommitmentLevel> for i32 {
    fn from(level: CommitmentLevel) -> Self {
        match level {
            CommitmentLevel::Processed => 0,
            CommitmentLevel::Confirmed => 1,
            CommitmentLevel::Finalized => 2,
        }
    }
}

/// Concrete Interceptor that injects the `x-token` metadata header on every gRPC call.
/// RPC Fast endpoints (Aperture, dedicated Shredstream, Yellowstone) authenticate using this header.
#[derive(Clone)]
pub(crate) struct XTokenInterceptor {
    token: String,
}

impl Interceptor for XTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if !self.token.is_empty() {
            if let Ok(value) = self.token.parse() {
                request.metadata_mut().insert("x-token", value);
            }
        }
        Ok(request)
    }
}

fn x_token_interceptor(token: String) -> XTokenInterceptor {
    XTokenInterceptor { token }
}

/// Builder-style request for subscribing to raw entries.
#[derive(Clone, Debug, Default)]
pub struct SubscribeEntriesRequest {
    accounts: Vec<Pubkey>,
    commitment: CommitmentLevel,
}

impl SubscribeEntriesRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_account(mut self, account: Pubkey) -> Self {
        self.accounts.push(account);
        self
    }

    pub fn commitment(mut self, level: CommitmentLevel) -> Self {
        self.commitment = level;
        self
    }
}

/// High-level wrapper around the raw gRPC client.
pub struct ShredstreamClient {
    endpoint: String,
    x_token: Option<String>,
}

impl ShredstreamClient {
    /// Create a new client builder.
    pub fn connect(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            x_token: None,
        }
    }

    /// Set the x-token (used by RPC Fast for authentication).
    pub fn with_x_token(mut self, token: impl Into<String>) -> Self {
        self.x_token = Some(token.into());
        self
    }

    /// Establish the connection and return a raw gRPC client **with x-token interceptor attached**.
    /// This ensures the SHREDSTREAM_AUTH_TOKEN is sent on every RPC call as the `x-token` metadata header.
    pub(crate) async fn connect_raw(&self) -> Result<RawClient<tonic::service::interceptor::InterceptedService<Channel, XTokenInterceptor>>> {
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| anyhow::anyhow!("Invalid endpoint: {}", e))?
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| anyhow::anyhow!("TLS config failed: {}", e))?
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Shredstream: {}", e))?;

        // Always attach an interceptor so the client type is consistent.
        // When a token is present we inject "x-token" on every call (required by RPC Fast).
        let token = self.x_token.clone().unwrap_or_default();
        if !token.is_empty() {
            tracing::info!("solana-stream-sdk: attaching x-token (length={}) to all gRPC calls for {}", token.len(), self.endpoint);
        }
        let interceptor = x_token_interceptor(token);
        let client = RawClient::with_interceptor(channel, interceptor);

        Ok(client)
    }

    /// Subscribe to raw entries.
    /// This is the main method used by the NewParser style code.
    pub async fn subscribe_entries(
        &self,
        request: SubscribeEntriesRequest,
    ) -> Result<ShredstreamEntryStream> {
        let mut client = self.connect_raw().await?;

        let proto_req = ProtoRequest {
            accounts: request
                .accounts
                .into_iter()
                .map(|p| p.to_bytes().to_vec())
                .collect(),
            commitment: request.commitment.into(),
        };

        let response_stream = client
            .subscribe_entries(proto_req)
            .await
            .map_err(|e| anyhow::anyhow!("SubscribeEntries RPC failed: {}", e))?
            .into_inner();

        Ok(ShredstreamEntryStream { inner: response_stream })
    }
}

/// Stream wrapper that yields easy-to-use responses.
pub struct ShredstreamEntryStream {
    inner: tonic::Streaming<SubscribeEntriesResponse>,
}

impl ShredstreamEntryStream {
    /// Get the next message from the stream.
    pub async fn message(&mut self) -> Result<Option<ShredstreamEntryResponse>> {
        match self.inner.message().await {
            Ok(Some(resp)) => Ok(Some(ShredstreamEntryResponse {
                entries: resp.entries,
                slot: resp.slot,
            })),
            Ok(None) => Ok(None),
            Err(e) => {
                warn!("Shredstream stream error: {}", e);
                Err(e.into())
            }
        }
    }
}

/// Clean response type returned to the caller.
#[derive(Debug, Clone)]
pub struct ShredstreamEntryResponse {
    /// bincode serialized `Vec<solana_entry::entry::Entry>`
    pub entries: Vec<u8>,
    pub slot: u64,
}
