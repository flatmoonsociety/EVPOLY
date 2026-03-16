#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  set -a && source "$ROOT_DIR/.env" && set +a || true
fi

EVENTS_KEEP="${EVPOLY_EVENTS_ROTATE_KEEP:-5}"
HISTORY_KEEP="${EVPOLY_HISTORY_ROTATE_KEEP:-7}"
DB_BACKUP_KEEP_DAYS="${EVPOLY_DB_BACKUP_RETENTION_DAYS:-3}"
HISTORY_DIR_KEEP_DAYS="${EVPOLY_HISTORY_DIR_RETENTION_DAYS:-7}"

num_or_default() {
  local v="$1" d="$2"
  if [[ "$v" =~ ^[0-9]+$ ]]; then
    echo "$v"
  else
    echo "$d"
  fi
}

EVENTS_KEEP="$(num_or_default "$EVENTS_KEEP" 5)"
HISTORY_KEEP="$(num_or_default "$HISTORY_KEEP" 7)"
DB_BACKUP_KEEP_DAYS="$(num_or_default "$DB_BACKUP_KEEP_DAYS" 3)"
HISTORY_DIR_KEEP_DAYS="$(num_or_default "$HISTORY_DIR_KEEP_DAYS" 7)"

maybe_rm() {
  local f="$1"
  if (( DRY_RUN == 1 )); then
    echo "[dry-run] rm -f $f"
  else
    rm -f -- "$f"
    echo "removed: $f"
  fi
}

trim_rotated_keep() {
  local glob="$1" keep="$2"
  mapfile -t files < <(find "$ROOT_DIR" -maxdepth 1 -type f -name "$glob" -printf '%T@ %p\n' | sort -nr | awk '{print $2}')
  local count="${#files[@]}"
  if (( count <= keep )); then
    echo "keep ok: $glob ($count <= $keep)"
    return 0
  fi
  for f in "${files[@]:keep}"; do
    maybe_rm "$f"
  done
}

echo "[retention] root=$ROOT_DIR dry_run=$DRY_RUN"
trim_rotated_keep 'events.jsonl.*' "$EVENTS_KEEP"
trim_rotated_keep 'history.toml.*' "$HISTORY_KEEP"

while IFS= read -r f; do
  maybe_rm "$f"
done < <(find "$ROOT_DIR" -maxdepth 1 -type f -name 'tracking.db.bak*' -mtime "+$DB_BACKUP_KEEP_DAYS" -print)

while IFS= read -r f; do
  maybe_rm "$f"
done < <(find "$ROOT_DIR/history" -type f -mtime "+$HISTORY_DIR_KEEP_DAYS" -print 2>/dev/null || true)

echo "[retention] done"
