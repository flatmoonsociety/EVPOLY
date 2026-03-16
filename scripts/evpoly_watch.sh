#!/usr/bin/env bash
set -euo pipefail

# Alert-only EVPoly watcher.
# Runs every 5 minutes, checks EVPoly health via tracking.db + tmux.
# Emits alert lines (stderr + syslog when available) only when issues are detected.

cd "$(dirname "$0")/.."

INTERVAL_SEC="${EVPOLY_WATCH_INTERVAL_SEC:-300}"

while true; do
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  out="$(./scripts/evpoly_healthcheck.py 2>&1 || true)"

  # Parse: we only alert on:
  # - any issues[]
  # - warns[] excluding redemption_quota_paused
  if echo "$out" | grep -q "EVPOLY_HEALTH"; then
    json="${out#EVPOLY_HEALTH OK }"
    json="${json#EVPOLY_HEALTH WARN }"
    json="${json#EVPOLY_HEALTH ERROR }"

    # use python for robust json parsing
    alert_msg="$({
      python3 - <<'PY'
import json,sys
raw=sys.stdin.read().strip()
try:
  d=json.loads(raw)
except Exception:
  print('')
  sys.exit(0)
issues=d.get('issues') or []
warns=[w for w in (d.get('warns') or []) if w!='redemption_quota_paused']
if not issues and not warns:
  print('')
  sys.exit(0)
ls=[]
if issues:
  ls.append('issues=' + ','.join(map(str,issues)))
if warns:
  ls.append('warns=' + ','.join(map(str,warns)))
latest=d.get('latest_strategy_decision') or {}
trade=d.get('latest_trade_event') or {}
# compact context
ctx=[]
for tf in ['5m','15m','1h']:
  if tf in latest:
    ctx.append(f"{tf}:{latest[tf].get('action')} {latest[tf].get('direction')} c={latest[tf].get('confidence')}")
print(' | '.join(ls) + ((' | decisions=' + ';'.join(ctx)) if ctx else ''))
PY
    } <<<"$json")"

    if [ -n "$alert_msg" ]; then
      echo "EVPOLY ALERT ($ts): $alert_msg" >&2
      logger -t evpoly-watch -- "EVPOLY ALERT ($ts): $alert_msg" >/dev/null 2>&1 || true
    fi
  fi

  sleep "$INTERVAL_SEC"
done
