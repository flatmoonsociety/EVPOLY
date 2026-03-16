#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/poly.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/poly.env" || true
  set +a
fi

if [[ -n "${EVPOLY_ADMIN_BIND:-}" && -z "${EVPOLY_ADMIN_API_BIND:-}" ]]; then
  echo "[WARN] EVPOLY_ADMIN_BIND is deprecated; use EVPOLY_ADMIN_API_BIND" >&2
fi

ADMIN_BIND="${EVPOLY_ADMIN_API_BIND:-${EVPOLY_ADMIN_BIND:-127.0.0.1:8787}}"
ADMIN_TOKEN="${EVPOLY_ADMIN_API_TOKEN:-}"
URL="http://${ADMIN_BIND}/admin/mm/status/global"

if [[ -n "$ADMIN_TOKEN" ]]; then
  CURL_CMD=(curl -fsS -H "x-evpoly-admin-token: ${ADMIN_TOKEN}" "${URL}")
else
  CURL_CMD=(curl -fsS "${URL}")
fi

if command -v jq >/dev/null 2>&1; then
  "${CURL_CMD[@]}" | jq '{
    as_of_ts_ms,
    totals,
    category_counts,
    idle_reasons,
    wallet_sync
  }'
else
  "${CURL_CMD[@]}"
fi
