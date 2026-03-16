# Strategy Combo Guide

## Hard Rules
- `mm_rewards_v1` should run alone.
- `endgame_sweep_v1` and `sessionband_v1` should not run together.

## Why
- MM rewards is inventory/market-making logic; combining it with directional loops increases inventory conflict and order churn.
- Endgame and SessionBand both concentrate exposure near close checkpoints; combining them stacks late-window risk.

## Recommended Profiles

### Profile A: Premarket Only (Lowest Complexity)
- Enable: `premarket_v1`
- Disable: `endgame_sweep_v1`, `evcurve_v1`, `sessionband_v1`, `evsnipe_v1`, `mm_rewards_v1`

### Profile B: Premarket + EVcurve
- Enable: `premarket_v1`, `evcurve_v1`
- Disable: `endgame_sweep_v1`, `sessionband_v1`, `evsnipe_v1`, `mm_rewards_v1`

### Profile C: Endgame Stack (No SessionBand)
- Enable: `premarket_v1`, `endgame_sweep_v1`, optional `evcurve_v1`, optional `evsnipe_v1`
- Disable: `sessionband_v1`, `mm_rewards_v1`

### Profile D: SessionBand Stack (No Endgame)
- Enable: `premarket_v1`, `sessionband_v1`, optional `evcurve_v1`, optional `evsnipe_v1`
- Disable: `endgame_sweep_v1`, `mm_rewards_v1`

### Profile E: Directional All-In (Higher Ops Load)
- Enable: `premarket_v1`, `endgame_sweep_v1` or `sessionband_v1` (choose one), `evcurve_v1`, `evsnipe_v1`
- Disable: `mm_rewards_v1`

### Profile F: MM Rewards Solo
- Enable: `mm_rewards_v1`
- Disable: `premarket_v1`, `endgame_sweep_v1`, `evcurve_v1`, `sessionband_v1`, `evsnipe_v1`

## Example Toggle Set (MM Solo)
```bash
EVPOLY_STRATEGY_PREMARKET_ENABLE=false
EVPOLY_STRATEGY_ENDGAME_ENABLE=false
EVPOLY_STRATEGY_EVCURVE_ENABLE=false
EVPOLY_STRATEGY_SESSIONBAND_ENABLE=false
EVPOLY_STRATEGY_EVSNIPE_ENABLE=false
EVPOLY_STRATEGY_MM_REWARDS_ENABLE=true
```

## Example Toggle Set (Directional, No MM)
```bash
EVPOLY_STRATEGY_PREMARKET_ENABLE=true
EVPOLY_STRATEGY_ENDGAME_ENABLE=true
EVPOLY_STRATEGY_EVCURVE_ENABLE=true
EVPOLY_STRATEGY_SESSIONBAND_ENABLE=false
EVPOLY_STRATEGY_EVSNIPE_ENABLE=true
EVPOLY_STRATEGY_MM_REWARDS_ENABLE=false
```
