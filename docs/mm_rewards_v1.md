# MM Rewards v1 Guide

## What It Does
`mm_rewards_v1` is the rewards-focused market-making loop. It scans reward markets, ranks candidates, then quotes two-sided ladders with inventory/risk controls.

## Default State
- Strategy toggle default: `EVPOLY_STRATEGY_MM_REWARDS_ENABLE=false`
- Recommended operation: run alone (do not combine with directional strategies unless intentional)

## Market Selection and Discovery
MM currently uses:
- Local discovery/scanning for candidates and market metadata
- Remote alpha for selection only
- Local preflight validation before submit

Remote selection config:
- `EVPOLY_REMOTE_MM_REWARDS_SELECTION_ALPHA_URL`
- `EVPOLY_REMOTE_MM_REWARDS_ALPHA_TOKEN`

Notes:
- MM preflight is local runtime logic (not remote).
- MM discovery is local runtime path.
- `EVPOLY_MM_MARKET_MODE` default is `auto`; `hybrid` adds target-selector path on top of auto selection.
- Optional CBB priority path can force CBB markets into selection and apply CBB-specific filter/rest settings.

## End-to-End Flow
1. Refresh rewards universe and enrich market metadata locally.
2. Rank candidates via reward/risk scoring.
3. Send candidate pool to remote alpha selection endpoint.
4. Apply local preflight/scoring guards per side.
5. Build per-mode ladder quote state.
6. Place/cancel/replace quote orders by side.
7. Refill on fills and manage inventory skew.
8. Apply weak-exit / unwind / toxicity / stale guards.
9. Reconcile open orders and balances on loop cadence.

## Sizing Defaults
- `EVPOLY_MM_QUOTE_SHARES_PER_SIDE` code default: `5.0`
- `EVPOLY_MM_REFILL_SHARES_PER_FILL` default: `5.0`
- `EVPOLY_MM_MAX_SHARES_PER_ORDER` default: `5000`
- `EVPOLY_MM_AUTO_RANK_BUDGET_USD` default: `5000`

## Key Env Knobs
- `EVPOLY_STRATEGY_MM_REWARDS_ENABLE`
- `EVPOLY_MM_MODE`
- `EVPOLY_MM_MARKET_MODE`
- `EVPOLY_MM_SINGLE_MARKET_SLUGS`
- `EVPOLY_MM_AUTO_TOP_N`
- `EVPOLY_MM_AUTO_REFRESH_SEC`
- `EVPOLY_MM_AUTO_SCAN_LIMIT`
- `EVPOLY_MM_AUTO_RANK_BUDGET_USD`
- `EVPOLY_MM_CBB_PRIORITY_ENABLE`
- `EVPOLY_MM_CBB_PRIORITY_MAX_MARKETS`
- `EVPOLY_MM_CBB_PRIORITY_MIN_REWARD_RATE_HINT`
- `EVPOLY_MM_CBB_PRIORITY_BYPASS_FILTERS`
- `EVPOLY_MM_CBB_PRIORITY_MIN_REST_MS`
- `EVPOLY_REMOTE_MM_REWARDS_SELECTION_ALPHA_URL`
- `EVPOLY_REMOTE_MM_REWARDS_ALPHA_TOKEN`
