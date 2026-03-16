#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[parallel-smoke] branch=$(git rev-parse --abbrev-ref HEAD) commit=$(git rev-parse --short HEAD)"
echo "[parallel-smoke] checking build"
cargo check -q

echo "[parallel-smoke] key parallel env defaults"
cat <<ENV
EVPOLY_PARALLEL_DISCOVERY_ENABLE=${EVPOLY_PARALLEL_DISCOVERY_ENABLE:-true}
EVPOLY_PREMARKET_PARALLEL_ENQUEUE_ENABLE=${EVPOLY_PREMARKET_PARALLEL_ENQUEUE_ENABLE:-true}
EVPOLY_PARALLEL_CANCEL_MAINTENANCE_ENABLE=${EVPOLY_PARALLEL_CANCEL_MAINTENANCE_ENABLE:-true}
EVPOLY_PARALLEL_CANCEL_RECONCILE_ENABLE=${EVPOLY_PARALLEL_CANCEL_RECONCILE_ENABLE:-true}
EVPOLY_MONITOR_ASYNC_CALLBACK_ENABLE=${EVPOLY_MONITOR_ASYNC_CALLBACK_ENABLE:-true}
ENV

echo "[parallel-smoke] done"
