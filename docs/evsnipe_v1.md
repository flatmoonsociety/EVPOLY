# EVSnipe v1 Guide

## What It Does
`evsnipe_v1` trades hit-price and close/range crypto markets using Binance live data and fast PM execution.

## Default Scope
- Symbols: `BTC, ETH, SOL, XRP`
- Strategy toggle default: `EVPOLY_STRATEGY_EVSNIPE_ENABLE=true`

## Discovery Model
EVSnipe discovery is remote-first with local fallback.

Remote config:
- `EVPOLY_REMOTE_EVSNIPE_DISCOVERY_URL`
- `EVPOLY_REMOTE_EVSNIPE_DISCOVERY_TOKEN`

Runtime behavior:
- Remote timeout is hardcoded to `2000ms` (no env timeout knob).
- Remote host failover is supported (`alpha.evplus.ai` -> `alpha2.evplus.ai`) for transport/timeout/429/5xx classes.
- If remote is missing/unavailable/empty, runtime falls back to local Gamma discovery.

Local and remote discovery are aligned to the same EVSnipe filtering model (Poly-builder parity), including strike-window filtering.

## End-to-End Flow
1. Periodic discovery refresh tries remote first, then local fallback when needed.
2. Refresh anchor spot and apply strike-window filter.
3. Binance trade stream handles hit triggers.
4. Binance `kline_1m` close stream handles close-at-time / between rules.
5. On trigger, map rule -> side/token.
6. Fetch PM orderbook and pick cross-ask execution price within configured depth.
7. Submit FAK buy with max-buy guard.
8. Enforce strategy cap and inflight task limits.
9. Dedupe/prune prevents duplicate fire for same condition/leg.

## Sizing and Caps
- `EVPOLY_EVSNIPE_SIZE_USD` default `100`
- `EVPOLY_EVSNIPE_STRATEGY_CAP_USD` default `10000`
- `EVPOLY_EVSNIPE_PRE_LEG_RATIO` default `0.30`

## Key Env Knobs
- `EVPOLY_STRATEGY_EVSNIPE_ENABLE`
- `EVPOLY_EVSNIPE_SYMBOLS`
- `EVPOLY_EVSNIPE_DISCOVERY_REFRESH_SEC`
- `EVPOLY_EVSNIPE_MAX_DAYS_TO_EXPIRY`
- `EVPOLY_EVSNIPE_STRIKE_WINDOW_PCT`
- `EVPOLY_EVSNIPE_SIZE_USD`
- `EVPOLY_EVSNIPE_STRATEGY_CAP_USD`
- `EVPOLY_REMOTE_EVSNIPE_DISCOVERY_URL`
- `EVPOLY_REMOTE_EVSNIPE_DISCOVERY_TOKEN`
