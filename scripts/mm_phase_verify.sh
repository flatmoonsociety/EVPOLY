#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WINDOW="${1:-24h}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="reports/mm_phase_verify_${STAMP}.json"

echo "[phase-verify] cargo check"
cargo check >/tmp/mm_phase_verify_cargo_check.log 2>&1 || {
  echo "[phase-verify] cargo check failed. Log: /tmp/mm_phase_verify_cargo_check.log"
  tail -n 120 /tmp/mm_phase_verify_cargo_check.log
  exit 1
}

echo "[phase-verify] baseline snapshot (${WINDOW})"
python3 scripts/mm_phase_baseline.py --window "${WINDOW}" --output "${OUT}" >/tmp/mm_phase_verify_snapshot.log 2>&1 || {
  echo "[phase-verify] snapshot failed. Log: /tmp/mm_phase_verify_snapshot.log"
  cat /tmp/mm_phase_verify_snapshot.log
  exit 1
}

echo "[phase-verify] snapshot written: ${OUT}"
python3 - "$OUT" <<'PY'
import json, sys, pathlib
path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text())
db = data.get("db_metrics", {})
if db.get("degraded"):
    print("[phase-verify] warning: degraded snapshot mode")
    for item in db.get("warnings", []):
        print(f"[phase-verify] warning: {item}")
PY
echo "[phase-verify] done"
