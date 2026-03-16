#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
CRON_EXPR="${EVPOLY_RETENTION_CRON_EXPR:-17 * * * *}"
CMD="cd '$ROOT_DIR' && '$ROOT_DIR/scripts/evpoly_retention_cleanup.sh' >> '$ROOT_DIR/history/retention_cleanup.log' 2>&1"

if ! command -v crontab >/dev/null 2>&1; then
  echo "error: crontab command not found; install cron/cronie first." >&2
  exit 1
fi

mkdir -p "$ROOT_DIR/history"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

crontab -l 2>/dev/null | grep -v "evpoly_retention_cleanup.sh" > "$tmp" || true
echo "$CRON_EXPR $CMD" >> "$tmp"
crontab "$tmp"

echo "installed cron: $CRON_EXPR"
echo "command: $CMD"
