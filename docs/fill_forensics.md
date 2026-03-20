# Fill Forensics Tool

Use `scripts/fill_forensics.sh` to quickly investigate why a fill happened.

It combines:
- fill row from `tracking.db` (`fills_v2`)
- nearby order lifecycle (`pending_orders`)
- nearby strategy events (`trade_events`)
- nearby runtime events (`events.jsonl*`)
- public trade tape around the same time (`data-api.polymarket.com`)

## Usage

```bash
scripts/fill_forensics.sh --fill-id 1896
scripts/fill_forensics.sh --order-id 0xcb6779...
scripts/fill_forensics.sh --condition-id 0x61ed... --strategy mm_sport_v1
scripts/fill_forensics.sh --condition-id 0x61ed... --token-id 1013...
```

Useful flags:
- `--window-sec 60` (default `90`)
- `--max-pending-rows 80` (default `80`)
- `--max-runtime-events 80` (default `80`)
- `--db tracking.db`

## Notes

- For older fills, public tape may not contain the exact print anymore (API history limits).
- Runtime events depend on local `events.jsonl*` retention.
