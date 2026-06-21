#!/usr/bin/env bash
# Post-graduation Raydium sniper — Pump.fun migrate → Jupiter buy → fast TP.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

export RUST_LOG="${RUST_LOG:-pump_postgrad=info,info}"
export POSTGRAD_DATA_DIR="${POSTGRAD_DATA_DIR:-$ROOT/data}"
export POSTGRAD_LOCK="${POSTGRAD_LOCK:-/root/.pump_postgrad.lock}"
export POSTGRAD_GRPC_FALLBACK="${POSTGRAD_GRPC_FALLBACK:-true}"
export DRY_RUN="${DRY_RUN:-true}"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
elif [[ -f "$HOME/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$HOME/.env"
  set +a
fi

export POSTGRAD_DATA_DIR="${POSTGRAD_DATA_DIR:-$ROOT/data}"
export POSTGRAD_LOCK="${POSTGRAD_LOCK:-/root/.pump_postgrad.lock}"
export POSTGRAD_GRPC_FALLBACK="${POSTGRAD_GRPC_FALLBACK:-true}"
export DRY_RUN="${DRY_RUN:-true}"

# --- preflight ---
if [[ "${POSTGRAD_GRPC_FALLBACK:-true}" =~ ^(1|true|yes)$ ]]; then
  if [[ -z "${SHREDSTREAM_AUTH_TOKEN:-}" && -z "${SHREDSTREAM_GRPC_TOKEN:-}" ]]; then
    echo "ERROR: SHREDSTREAM_AUTH_TOKEN required for gRPC ingest" >&2
    exit 1
  fi
fi

if [[ "${DRY_RUN:-true}" =~ ^(0|false|no)$ ]]; then
  if [[ -z "${POSTGRAD_WALLET:-}" && -z "${WALLET_PRIVATE_KEY:-}" && -z "${WALLETS:-}" ]]; then
    echo "ERROR: POSTGRAD_WALLET required when DRY_RUN=false" >&2
    exit 1
  fi
  echo "WARNING: LIVE MODE (DRY_RUN=false) — real funds at risk"
else
  echo "SAFE MODE: DRY_RUN=true — paper trading only"
fi

if [[ -f "$POSTGRAD_LOCK" ]]; then
  echo "NOTE: lock file exists at $POSTGRAD_LOCK — bot will exit if another instance is running"
fi

mkdir -p "$POSTGRAD_DATA_DIR"

BIN="${ROOT}/target/release/pump-postgrad-bot"
if [[ ! -x "$BIN" ]]; then
  echo "Building pump-postgrad-bot (release)..."
  cargo build --release --bin pump-postgrad-bot
fi

echo "pump-postgrad | dry_run=$DRY_RUN | data=$POSTGRAD_DATA_DIR | grpc=${POSTGRAD_GRPC_FALLBACK:-true}"
exec "$BIN"