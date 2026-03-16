# Endgame Sweep v1 Guide

## What It Does
`endgame_sweep_v1` enters late in the period, with checkpoint timing controlled by remote alpha policy and a strict execution guard stack.

## Default Scope
- Symbols: `BTC, ETH, SOL, XRP`
- Timeframes: `5m, 15m, 1h, 4h`
- Strategy toggle default: `EVPOLY_STRATEGY_ENDGAME_ENABLE=true`

## Alpha-Driven Timing
At runtime the bot requests one alpha policy per symbol/timeframe/period near `T-60s`.

Policy includes:
- `tick_offsets_ms` (typically around `3000,1000,100` with jitter)
- `submit_proxy_max_age_ms` (submit-time stale guard)

Config:
- `EVPOLY_REMOTE_ENDGAME_ALPHA_URL`
- `EVPOLY_REMOTE_ENDGAME_ALPHA_TOKEN`
- `EVPOLY_ENDGAME_ALPHA_REQUIRED=true` (default)
- Runtime alpha timeout is hardcoded `1000ms`

If required policy is unavailable, the period is skipped (fail-closed).

## End-to-End Flow
1. Build symbol proxy feeds (Coinbase/Binance by timeframe path).
2. Compute/restore period base anchor.
3. Request alpha policy around `T-60s`.
4. If no valid policy and required mode is enabled, skip period.
5. For each due alpha tick, build direction/probability plan.
6. Apply mandatory near-base skip gate.
7. Resolve market with remote-first discovery + local fallback.
8. Enforce quote/book freshness + market constraints.
9. Apply poly price-band / entry-price guards.
10. Apply EV-safe sizing and cap checks.
11. Enqueue to arbiter/trader.
12. Enforce submit-time stale guard from alpha policy.

## Sizing Policy
Base key: `EVPOLY_ENDGAME_BASE_SIZE_USD` (blank defaults to `100`).

Multipliers:
- Symbol: `BTC=1.0`, `ETH=0.8`, `SOL/XRP=0.5`
- Tick split: `tick0=20%`, `tick1=40%`, `tick2=40%`

## Core Guards
- Mandatory near-base skip gate (`EVPOLY_NEAR_BASE_SKIP_BPS`, default `1.0`)
- Quote/proxy freshness gates
- Submit-time stale guard (policy-aware)
- Min entry / price-band gates
- Per-period and strategy cap gates

## Key Env Knobs
- `EVPOLY_STRATEGY_ENDGAME_ENABLE`
- `EVPOLY_ENDGAME_BASE_SIZE_USD`
- `EVPOLY_ENDGAME_PER_PERIOD_CAP_USD`
- `EVPOLY_ENDGAME_SYMBOLS`
- `EVPOLY_ENDGAME_TIMEFRAMES`
- `EVPOLY_ENDGAME_ALPHA_REQUIRED`
- `EVPOLY_REMOTE_ENDGAME_ALPHA_URL`
- `EVPOLY_REMOTE_ENDGAME_ALPHA_TOKEN`
