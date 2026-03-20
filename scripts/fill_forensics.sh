#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/fill_forensics.sh [options]

Locate one fill and print:
  1) Fill details from tracking.db
  2) Nearby pending-order lifecycle for same condition/token
  3) Nearby strategy trade_events
  4) Nearby runtime events from events.jsonl*
  5) Public market trade tape around the fill (data-api)

Selector options (pick one):
  --fill-id N                fills_v2.id
  --order-id 0x...           pending_orders.order_id (must have filled_at_ms)
  --condition-id 0x...       latest fill for condition (optionally with --token-id)

Optional:
  --token-id TOKEN_ID        narrow condition lookup
  --strategy STRATEGY_ID     default: mm_sport_v1
  --window-sec N             default: 90
  --max-pending-rows N       default: 80
  --max-runtime-events N     default: 80
  --db PATH                  default: tracking.db
  --help

Examples:
  scripts/fill_forensics.sh --fill-id 1896
  scripts/fill_forensics.sh --order-id 0xcb6779...
  scripts/fill_forensics.sh --condition-id 0x61ed... --strategy mm_sport_v1
  scripts/fill_forensics.sh --condition-id 0x61ed... --token-id 1013...
EOF
}

require_bin() {
  local name="$1"
  command -v "$name" >/dev/null 2>&1 || {
    echo "error: required binary not found: $name" >&2
    exit 1
  }
}

sql_quote() {
  local s="$1"
  s="${s//\'/\'\'}"
  printf "'%s'" "$s"
}

ms_to_utc() {
  local ms="$1"
  if ! [[ "$ms" =~ ^-?[0-9]+$ ]]; then
    printf "n/a"
    return 0
  fi
  local sec=$((ms / 1000))
  date -u -d "@$sec" '+%Y-%m-%d %H:%M:%S UTC'
}

FILL_ID=""
ORDER_ID=""
CONDITION_ID=""
TOKEN_ID=""
STRATEGY_ID="mm_sport_v1"
WINDOW_SEC=90
MAX_PENDING_ROWS=80
MAX_RUNTIME_EVENTS=80
DB_PATH="tracking.db"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fill-id)
      FILL_ID="${2:-}"
      shift 2
      ;;
    --order-id)
      ORDER_ID="${2:-}"
      shift 2
      ;;
    --condition-id)
      CONDITION_ID="${2:-}"
      shift 2
      ;;
    --token-id)
      TOKEN_ID="${2:-}"
      shift 2
      ;;
    --strategy)
      STRATEGY_ID="${2:-}"
      shift 2
      ;;
    --window-sec)
      WINDOW_SEC="${2:-}"
      shift 2
      ;;
    --max-pending-rows)
      MAX_PENDING_ROWS="${2:-}"
      shift 2
      ;;
    --max-runtime-events)
      MAX_RUNTIME_EVENTS="${2:-}"
      shift 2
      ;;
    --db)
      DB_PATH="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_bin sqlite3
require_bin jq
require_bin curl

if [[ ! -f "$DB_PATH" ]]; then
  echo "error: db not found: $DB_PATH" >&2
  exit 1
fi

selector_count=0
[[ -n "$FILL_ID" ]] && selector_count=$((selector_count + 1))
[[ -n "$ORDER_ID" ]] && selector_count=$((selector_count + 1))
[[ -n "$CONDITION_ID" ]] && selector_count=$((selector_count + 1))
if [[ "$selector_count" -ne 1 ]]; then
  echo "error: pick exactly one selector: --fill-id OR --order-id OR --condition-id" >&2
  usage
  exit 1
fi

if ! [[ "$WINDOW_SEC" =~ ^[0-9]+$ ]] || [[ "$WINDOW_SEC" -lt 5 ]] || [[ "$WINDOW_SEC" -gt 3600 ]]; then
  echo "error: --window-sec must be integer in [5, 3600]" >&2
  exit 1
fi

if ! [[ "$MAX_RUNTIME_EVENTS" =~ ^[0-9]+$ ]] || [[ "$MAX_RUNTIME_EVENTS" -lt 1 ]] || [[ "$MAX_RUNTIME_EVENTS" -gt 2000 ]]; then
  echo "error: --max-runtime-events must be integer in [1, 2000]" >&2
  exit 1
fi

if ! [[ "$MAX_PENDING_ROWS" =~ ^[0-9]+$ ]] || [[ "$MAX_PENDING_ROWS" -lt 1 ]] || [[ "$MAX_PENDING_ROWS" -gt 2000 ]]; then
  echo "error: --max-pending-rows must be integer in [1, 2000]" >&2
  exit 1
fi

if [[ -n "$FILL_ID" ]] && ! [[ "$FILL_ID" =~ ^[0-9]+$ ]]; then
  echo "error: --fill-id must be numeric" >&2
  exit 1
fi

fill_row=""

if [[ -n "$FILL_ID" ]]; then
  fill_row="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
    "SELECT id, ts_ms, strategy_id, condition_id, token_id, side, price, units, notional_usd, source_event_type
     FROM fills_v2 WHERE id = $FILL_ID LIMIT 1;")"
elif [[ -n "$ORDER_ID" ]]; then
  q_order_id="$(sql_quote "$ORDER_ID")"
  order_row="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
    "SELECT filled_at_ms, strategy_id, condition_id, token_id
     FROM pending_orders
     WHERE order_id = $q_order_id
       AND filled_at_ms IS NOT NULL
     ORDER BY filled_at_ms DESC
     LIMIT 1;")"
  if [[ -z "$order_row" ]]; then
    echo "error: no filled pending_orders row found for order_id=$ORDER_ID" >&2
    exit 1
  fi
  IFS='|' read -r order_filled_ms order_strategy order_condition order_token <<<"$order_row"
  q_cond="$(sql_quote "$order_condition")"
  q_tok="$(sql_quote "$order_token")"
  q_strat="$(sql_quote "$order_strategy")"
  fill_row="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
    "SELECT id, ts_ms, strategy_id, condition_id, token_id, side, price, units, notional_usd, source_event_type
     FROM fills_v2
     WHERE strategy_id = $q_strat
       AND condition_id = $q_cond
       AND token_id = $q_tok
       AND ts_ms BETWEEN $((order_filled_ms - 60000)) AND $((order_filled_ms + 60000))
     ORDER BY ABS(ts_ms - $order_filled_ms) ASC, id DESC
     LIMIT 1;")"
  if [[ -z "$fill_row" ]]; then
    fill_row="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
      "SELECT id, ts_ms, strategy_id, condition_id, token_id, side, price, units, notional_usd, source_event_type
       FROM fills_v2
       WHERE strategy_id = $q_strat
         AND condition_id = $q_cond
         AND token_id = $q_tok
       ORDER BY ts_ms DESC
       LIMIT 1;")"
  fi
else
  q_cond="$(sql_quote "$CONDITION_ID")"
  q_strat="$(sql_quote "$STRATEGY_ID")"
  if [[ -n "$TOKEN_ID" ]]; then
    q_tok="$(sql_quote "$TOKEN_ID")"
    fill_row="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
      "SELECT id, ts_ms, strategy_id, condition_id, token_id, side, price, units, notional_usd, source_event_type
       FROM fills_v2
       WHERE strategy_id = $q_strat
         AND condition_id = $q_cond
         AND token_id = $q_tok
       ORDER BY ts_ms DESC
       LIMIT 1;")"
  else
    fill_row="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
      "SELECT id, ts_ms, strategy_id, condition_id, token_id, side, price, units, notional_usd, source_event_type
       FROM fills_v2
       WHERE strategy_id = $q_strat
         AND condition_id = $q_cond
       ORDER BY ts_ms DESC
       LIMIT 1;")"
  fi
fi

if [[ -z "$fill_row" ]]; then
  echo "error: no fill found for given selector" >&2
  exit 1
fi

IFS='|' read -r fill_id ts_ms strategy_id condition_id token_id side price units notional_usd source_event_type <<<"$fill_row"

from_ms=$((ts_ms - (WINDOW_SEC * 1000)))
to_ms=$((ts_ms + (WINDOW_SEC * 1000)))
history_from_ms=$((ts_ms - 1800000))
from_s=$((from_ms / 1000))
to_s=$((to_ms / 1000))

echo "== Fill =="
echo "fill_id:        $fill_id"
echo "ts_ms:          $ts_ms ($(ms_to_utc "$ts_ms"))"
echo "strategy_id:    $strategy_id"
echo "condition_id:   $condition_id"
echo "token_id:       $token_id"
echo "side:           $side"
echo "price:          $price"
echo "units:          $units"
echo "notional_usd:   $notional_usd"
echo "source_event:   $source_event_type"
echo

echo "== Market Meta (Gamma) =="
set +e
gamma_json="$(curl -fsS "https://gamma-api.polymarket.com/markets?condition_ids=$condition_id")"
gamma_rc=$?
set -e
if [[ "$gamma_rc" -eq 0 ]]; then
  echo "$gamma_json" | jq -r '
    if (type == "array" and length > 0) then
      .[0] as $m |
      "question:       \($m.question // "n/a")\nslug:           \($m.slug // "n/a")\noutcomes:       \((($m.outcomes // "[]") | fromjson?) // [])"
    else
      "question:       n/a\nslug:           n/a\noutcomes:       n/a"
    end
  '
else
  echo "warning: gamma-api unavailable"
fi
echo

q_strat="$(sql_quote "$strategy_id")"
q_cond="$(sql_quote "$condition_id")"
q_tok="$(sql_quote "$token_id")"

pending_total_count="$(sqlite3 -noheader "$DB_PATH" \
  "SELECT COUNT(*)
   FROM pending_orders
   WHERE strategy_id = $q_strat
     AND condition_id = $q_cond
     AND token_id = $q_tok
     AND created_at_ms BETWEEN $history_from_ms AND $to_ms;")"
echo "== Pending Orders (same condition/token, last 30m before fill; showing newest ${MAX_PENDING_ROWS}, total=${pending_total_count:-0}) =="
pending_rows="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
  "SELECT order_id, created_at_ms, filled_at_ms, side, price, size_usd, status
   FROM pending_orders
   WHERE strategy_id = $q_strat
     AND condition_id = $q_cond
     AND token_id = $q_tok
     AND created_at_ms BETWEEN $history_from_ms AND $to_ms
   ORDER BY created_at_ms DESC
   LIMIT $MAX_PENDING_ROWS;")"
if [[ -z "$pending_rows" ]]; then
  echo "(none)"
else
  while IFS='|' read -r o_id c_ms f_ms o_side o_price o_size o_status; do
    c_utc="$(ms_to_utc "$c_ms")"
    f_utc="n/a"
    if [[ -n "${f_ms:-}" ]] && [[ "$f_ms" =~ ^-?[0-9]+$ ]]; then
      f_utc="$(ms_to_utc "$f_ms")"
    fi
    echo "$c_ms ($c_utc) | status=$o_status side=$o_side price=$o_price size_usd=$o_size order_id=$o_id filled_at_ms=${f_ms:-null} ($f_utc)"
  done <<<"$pending_rows"
fi
echo

echo "== trade_events (strategy window +/- ${WINDOW_SEC}s) =="
trade_rows="$(sqlite3 -noheader -separator '|' "$DB_PATH" \
  "SELECT ts_ms, event_type, side, price, units, notional_usd, reason
   FROM trade_events
   WHERE strategy_id = $q_strat
     AND condition_id = $q_cond
     AND ts_ms BETWEEN $from_ms AND $to_ms
   ORDER BY ts_ms ASC;")"
if [[ -z "$trade_rows" ]]; then
  echo "(none)"
else
  while IFS='|' read -r e_ms e_type e_side e_price e_units e_notional e_reason; do
    e_utc="$(ms_to_utc "$e_ms")"
    echo "$e_ms ($e_utc) | $e_type side=${e_side:-n/a} price=${e_price:-n/a} units=${e_units:-n/a} notional=${e_notional:-n/a}"
    if [[ -n "${e_reason:-}" ]]; then
      echo "  reason: $e_reason"
    fi
  done <<<"$trade_rows"
fi
echo

echo "== Runtime Events (events.jsonl*, same condition, window +/- ${WINDOW_SEC}s) =="
event_files=()
while IFS= read -r -d '' f; do
  event_files+=("$f")
done < <(find . -maxdepth 1 -type f -name 'events.jsonl*' -print0 | sort -z)

if [[ "${#event_files[@]}" -eq 0 ]]; then
  echo "warning: no events.jsonl files found"
else
  set +e
  runtime_rows="$(jq -cR --arg cond "$condition_id" --argjson from "$from_ms" --argjson to "$to_ms" '
      fromjson?
      | select(.ts_ms >= $from and .ts_ms <= $to and .payload.condition_id? == $cond)
      | {ts_ms, kind, payload}
    ' "${event_files[@]}" | head -n "$MAX_RUNTIME_EVENTS")"
  jq_rc=$?
  set -e
  if [[ "$jq_rc" -ne 0 && "$jq_rc" -ne 141 ]]; then
    echo "warning: failed to scan runtime events"
  fi
  if [[ -z "${runtime_rows:-}" ]]; then
    echo "(none)"
  else
    echo "$runtime_rows"
  fi
fi
echo

echo "== Public Trade Tape (data-api, window +/- ${WINDOW_SEC}s) =="
set +e
tape_json="$(curl -fsS "https://data-api.polymarket.com/trades?market=$condition_id&limit=2000")"
tape_rc=$?
set -e
if [[ "$tape_rc" -ne 0 ]]; then
  echo "warning: data-api unavailable"
else
  tape_rows="$(echo "$tape_json" | jq -r --argjson from "$from_s" --argjson to "$to_s" '
    [.[] | select(.timestamp >= $from and .timestamp <= $to)]
    | sort_by(.timestamp)
    | .[]
    | "\(.timestamp)\t\(.outcome // "n/a")\t\(.side // "n/a")\tprice=\(.price // "n/a")\tsize=\(.size // "n/a")"
  ')"
  if [[ -z "${tape_rows:-}" ]]; then
    echo "(none in fetched window)"
  else
    while IFS=$'\t' read -r t_ts t_outcome t_side t_price t_size; do
      t_utc="$(date -u -d "@$t_ts" '+%Y-%m-%d %H:%M:%S UTC')"
      echo "$t_ts ($t_utc) | $t_outcome | $t_side | $t_price | $t_size"
    done <<<"$tape_rows"
  fi
fi
