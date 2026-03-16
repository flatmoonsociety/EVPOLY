#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env" || true
  set +a
fi

INTERVAL_SEC="${EVPOLY_MM_INVENTORY_RECONCILE_INTERVAL_SEC:-900}"
MIN_DRIFT_SHARES="${EVPOLY_MM_INVENTORY_RECONCILE_MIN_DRIFT_SHARES:-25}"
LIMIT_ROWS="${EVPOLY_MM_INVENTORY_RECONCILE_LIMIT:-256}"
ADMIN_BIND="${EVPOLY_ADMIN_API_BIND:-127.0.0.1:8787}"
ADMIN_TOKEN="${EVPOLY_ADMIN_API_TOKEN:-}"
LOG_PATH="${EVPOLY_MM_INVENTORY_RECONCILE_LOG:-$ROOT_DIR/history/mm_inventory_reconcile.log}"
ONE_SHOT="${1:-}"

mkdir -p "$(dirname "$LOG_PATH")"

log_line() {
  local msg="$1"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf "%s %s\n" "$ts" "$msg" | tee -a "$LOG_PATH"
}

admin_post() {
  local path="$1"
  if [[ -n "$ADMIN_TOKEN" ]]; then
    curl -sS --max-time 30 -X POST -H "x-evpoly-admin-token: $ADMIN_TOKEN" "http://$ADMIN_BIND$path"
  else
    curl -sS --max-time 30 -X POST "http://$ADMIN_BIND$path"
  fi
}

run_once() {
  local resp count repaired_shares
  if ! resp="$(admin_post "/admin/mm/reconcile/inventory?min_drift_shares=${MIN_DRIFT_SHARES}&limit=${LIMIT_ROWS}" 2>>"$LOG_PATH")"; then
    log_line "[WARN] inventory_reconcile_failed reason=admin_unavailable min_drift_shares=${MIN_DRIFT_SHARES}"
    return 0
  fi
  count="$(jq -r '.count // 0' <<<"$resp" 2>/dev/null || echo 0)"
  repaired_shares="$(jq -r '[.rows[]? | ((.applied_consume_shares // 0) + (.applied_add_shares // 0))] | add // 0' <<<"$resp" 2>/dev/null || echo 0)"
  log_line "[INFO] inventory_reconcile count=${count} repaired_shares=${repaired_shares} min_drift_shares=${MIN_DRIFT_SHARES}"
}

log_line "[INFO] mm_inventory_reconcile_worker_started interval_sec=${INTERVAL_SEC} min_drift_shares=${MIN_DRIFT_SHARES} limit=${LIMIT_ROWS} admin_bind=${ADMIN_BIND}"

while true; do
  run_once
  if [[ "$ONE_SHOT" == "--once" ]]; then
    break
  fi
  sleep "$INTERVAL_SEC"
done

log_line "[INFO] mm_inventory_reconcile_worker_stopped"
