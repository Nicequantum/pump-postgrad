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

mkdir -p "$POSTGRAD_DATA_DIR"

BIN="${ROOT}/target/release/pump-postgrad-bot"
if [[ ! -x "$BIN" ]]; then
  echo "Building pump-postgrad-bot (release)..."
  cargo build --release --bin pump-postgrad-bot
fi

echo "pump-postgrad | dry_run=$DRY_RUN | data=$POSTGRAD_DATA_DIR"
exec "$BIN"