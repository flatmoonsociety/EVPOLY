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

INTERVAL_SEC="${EVPOLY_BALANCE_GUARD_INTERVAL_SEC:-900}"
THRESHOLD_USDC="${EVPOLY_BALANCE_GUARD_MIN_USDC:-300}"
EQUITY_TIMEOUT_SEC="${EVPOLY_BALANCE_GUARD_EQUITY_TIMEOUT_SEC:-60}"
ADMIN_BIND="${EVPOLY_ADMIN_API_BIND:-127.0.0.1:8787}"
ADMIN_TOKEN="${EVPOLY_ADMIN_API_TOKEN:-}"
BIN_PATH="${EVPOLY_BALANCE_GUARD_BIN:-$ROOT_DIR/target/release/polymarket-arbitrage-bot}"
CONFIG_PATH="${EVPOLY_BALANCE_GUARD_CONFIG:-$ROOT_DIR/config.json}"
LOG_PATH="${EVPOLY_BALANCE_GUARD_LOG:-$ROOT_DIR/history/mm_balance_merge_guard.log}"
ONE_SHOT="${1:-}"

mkdir -p "$(dirname "$LOG_PATH")"

log_line() {
  local msg="$1"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf "%s %s\n" "$ts" "$msg" | tee -a "$LOG_PATH"
}

admin_get() {
  local path="$1"
  if [[ -n "$ADMIN_TOKEN" ]]; then
    curl -sS --max-time 20 -H "x-evpoly-admin-token: $ADMIN_TOKEN" "http://$ADMIN_BIND$path"
  else
    curl -sS --max-time 20 "http://$ADMIN_BIND$path"
  fi
}

admin_post() {
  local path="$1"
  if [[ -n "$ADMIN_TOKEN" ]]; then
    curl -sS --max-time 20 -X POST -H "x-evpoly-admin-token: $ADMIN_TOKEN" "http://$ADMIN_BIND$path"
  else
    curl -sS --max-time 20 -X POST "http://$ADMIN_BIND$path"
  fi
}

run_once() {
  local equity_json usdc_balance merge_status pending merge_resp accepted msg

  if ! equity_json="$(timeout "${EQUITY_TIMEOUT_SEC}s" "$BIN_PATH" --config "$CONFIG_PATH" --equity 2>>"$LOG_PATH")"; then
    log_line "[ERROR] equity_check_failed timeout_or_error timeout_sec=${EQUITY_TIMEOUT_SEC}"
    return 0
  fi

  if ! usdc_balance="$(jq -r '.equity.usdc_balance // empty' <<<"$equity_json")"; then
    log_line "[ERROR] equity_parse_failed missing_usdc_balance"
    return 0
  fi
  if [[ -z "$usdc_balance" || "$usdc_balance" == "null" ]]; then
    log_line "[ERROR] equity_parse_failed null_usdc_balance"
    return 0
  fi

  log_line "[INFO] usdc_balance=${usdc_balance} threshold=${THRESHOLD_USDC}"

  if awk "BEGIN { exit !($usdc_balance < $THRESHOLD_USDC) }"; then
    merge_status="$(admin_get "/admin/merge/status" 2>>"$LOG_PATH" || true)"
    pending="$(jq -r '.status.manual_request_pending // false' <<<"$merge_status" 2>/dev/null || echo false)"
    if [[ "$pending" == "true" ]]; then
      log_line "[WARN] low_balance_merge_skipped reason=manual_request_pending usdc_balance=${usdc_balance}"
      return 0
    fi

    merge_resp="$(admin_post "/admin/merge/sweep" 2>>"$LOG_PATH" || true)"
    accepted="$(jq -r '.result.accepted // false' <<<"$merge_resp" 2>/dev/null || echo false)"
    msg="$(jq -r '.message // \"\"' <<<"$merge_resp" 2>/dev/null || echo "")"
    log_line "[ALERT] low_balance_merge_triggered usdc_balance=${usdc_balance} accepted=${accepted} message=\"${msg}\""
  fi

  return 0
}

log_line "[INFO] mm_balance_merge_guard_started interval_sec=${INTERVAL_SEC} threshold_usdc=${THRESHOLD_USDC} admin_bind=${ADMIN_BIND}"

while true; do
  run_once
  if [[ "$ONE_SHOT" == "--once" ]]; then
    break
  fi
  sleep "$INTERVAL_SEC"
done

log_line "[INFO] mm_balance_merge_guard_stopped"
