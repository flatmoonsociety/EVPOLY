#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${EV_AUTO_RESTART_ROOT:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"
REPO_NAME="$(basename "$ROOT" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')"
REPO_NAME="${REPO_NAME:-evpoly}"
LOCK_FILE="/tmp/${REPO_NAME}_auto_restart.lock"
LOG_FILE="${EV_AUTO_RESTART_LOG:-$ROOT/history/auto_restart.log}"
DRY_RUN="${EV_AUTO_RESTART_DRY_RUN:-0}"

# Cron runs with a minimal PATH; include cargo/rustup toolchain bins explicitly.
export PATH="/root/.cargo/bin:/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH:-}"

mkdir -p "$ROOT/history"
cd "$ROOT"

{
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] auto-restart tick"

  # Prevent overlapping runs.
  flock -n 9 || {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] skip: lock busy"
    exit 0
  }

  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] status-before"
  ./ev status || true

  if ! command -v cargo >/dev/null 2>&1; then
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] abort: cargo not found in PATH=$PATH"
    exit 1
  fi

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] dry-run: skip ./ev restart live"
    exit 0
  fi

  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] restart begin"
  ./ev restart live

  sleep 5

  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] status-after"
  ./ev status

  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] restart done"
} >> "$LOG_FILE" 2>&1 9>"$LOCK_FILE"
