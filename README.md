# pump-postgrad

Live trading bot for **post-graduation** Pump.fun tokens on Raydium. Detects graduation (migrate instruction or bonding-curve `complete`), snipes via Jupiter shortly after pool creation, rides the short post-grad pump, and exits quickly (take-profit, stop-loss, time stop, rug signals).

Standalone Rust project — not part of the Jules monorepo.

## Architecture

```
Shredstream gRPC ──► parser ──► tracker ──► strategy ──► Jupiter buy
                              ▲                │
RPC curve poller ─────────────┘                └──► positions ──► Jupiter sell
```

| Module | File | Responsibility |
|--------|------|----------------|
| Ingest | `src/ingest.rs` | gRPC `SubscribeEntries` feed |
| Parser | `src/parser.rs` | Pump create/buy/sell/migrate + Raydium swaps |
| Poller | `src/poller.rs` | RPC bonding-curve polling (`complete` flag) |
| Tracker | `src/tracker.rs` | Watchlist, graduation queue, JSONL events |
| Strategy | `src/strategy.rs` | Entry filters (buyers, buy/sell ratio, caps) |
| Positions | `src/positions.rs` | Open positions, PnL monitor, fast exit |
| Execution | `src/execution/` | Wallet, RPC submit, Jupiter swap API |
| Error | `src/error.rs` | Typed `PostGradError` for operational clarity |
| Store | `src/store.rs` | Append-only JSONL logs |

## Quick start

```bash
cd ~/pump-postgrad
cp .env.example .env   # fill in wallet + RPC + shredstream token
./run_postgrad_bot.sh
```

Default mode is **`DRY_RUN=true`** — logs entries/exits without sending transactions.

### Live trading

```bash
DRY_RUN=false POSTGRAD_BUY_SOL_LAMPORTS=50000000 ./run_postgrad_bot.sh
```

## Configuration

Create `.env` in the project root (or use `$HOME/.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRAD_WALLET` | — | bs58 private key (required for live) |
| `QUERY_RPC` | mainnet-beta | Solana RPC for reads |
| `SEND_RPC` | same as query | RPC for transaction submit |
| `SHREDSTREAM_AUTH_TOKEN` | — | gRPC shredstream token (required) |
| `SHREDSTREAM_GRPC_URL` | rpcfast default | Shredstream endpoint |
| `JUPITER_API_KEY` | — | Jupiter swap API key |
| `DRY_RUN` | `true` | Paper-trade mode |
| `POSTGRAD_BUY_SOL_LAMPORTS` | `50000000` | ~0.05 SOL per entry |
| `POSTGRAD_TAKE_PROFIT_PCT` | `25` | Exit on +25% |
| `POSTGRAD_STOP_LOSS_PCT` | `12` | Exit on -12% |
| `POSTGRAD_MAX_HOLD_SECS` | `90` | Time stop |
| `POSTGRAD_SNIPE_DELAY_MS` | `400` | Delay after graduation before buy |
| `POSTGRAD_MIN_UNIQUE_BUYERS` | `15` | Pre-grad activity filter |
| `POSTGRAD_DATA_DIR` | `./data` | JSONL output directory |

## Data files

Written under `POSTGRAD_DATA_DIR`:

- `graduations.jsonl` — detected migrate/complete events
- `entries.jsonl` — buy attempts
- `exits.jsonl` — closed positions with PnL
- `positions.jsonl` — position snapshots
- `stats.jsonl` — heartbeat stats

## Build

```bash
cargo build --release --bin pump-postgrad-bot
```

## Dependencies

- **solana-sdk / solana-entry / solana-client** — on-chain types and RPC
- **tokio** — async runtime
- **serde / serde_json** — config and JSONL logging
- **reqwest** — RPC + Jupiter HTTP
- **solana-stream-sdk** — vendored in `vendor/solana-stream-sdk`
- **thiserror / anyhow** — error handling

## Safety

- Single-instance lock at `/root/.pump_postgrad.lock`
- Position caps and hourly entry limits
- High slippage tolerance on exits (get out, don't optimize)
- Always test with `DRY_RUN=true` first

## License

MIT