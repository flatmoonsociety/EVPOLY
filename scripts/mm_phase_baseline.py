#!/usr/bin/env python3
"""Phase baseline snapshot for mm_rewards_v1.

Creates a deterministic JSON snapshot used as a regression baseline between
MM audit phases.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sqlite3
import subprocess
from collections import Counter
from typing import Any, Dict, Optional


ROOT = pathlib.Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "tracking.db"
EVENTS_PATH = ROOT / "events.jsonl"


def parse_window_to_ms(raw: str) -> Optional[int]:
    token = raw.strip().lower()
    if token == "all":
        return None
    if not token:
        return 24 * 60 * 60 * 1000
    unit = token[-1]
    if unit not in {"s", "m", "h", "d"}:
        raise ValueError(f"invalid window '{raw}'")
    value = int(token[:-1])
    scale = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    return value * scale * 1000


def utc_now_ms() -> int:
    return int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000)


def fmt_ts(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).isoformat()


def git_value(args: list[str]) -> Optional[str]:
    try:
        out = subprocess.check_output(args, cwd=ROOT, stderr=subprocess.DEVNULL, text=True)
        return out.strip() or None
    except Exception:
        return None


def one_row(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> Dict[str, Any]:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return {}
    return {k: row[k] for k in row.keys()}


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = one_row(
        conn,
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return bool(row)


def collect_db_metrics(conn: sqlite3.Connection, since_ms: Optional[int]) -> Dict[str, Any]:
    if since_ms is None:
        fill_where = "WHERE strategy_id='mm_rewards_v1'"
        settle_where = "WHERE strategy_id='mm_rewards_v1'"
        fill_params: tuple[Any, ...] = ()
        settle_params: tuple[Any, ...] = ()
    else:
        fill_where = "WHERE strategy_id='mm_rewards_v1' AND ts_ms >= ?"
        settle_where = "WHERE strategy_id='mm_rewards_v1' AND ts_ms >= ?"
        fill_params = (since_ms,)
        settle_params = (since_ms,)

    warnings: list[str] = []
    has_fills_v2 = table_exists(conn, "fills_v2")
    has_settlements_v2 = table_exists(conn, "settlements_v2")
    has_positions_v2 = table_exists(conn, "positions_v2")
    has_mm_states = table_exists(conn, "mm_market_states_v1")

    fill_source = "fills_v2" if has_fills_v2 else "trade_events"
    if has_fills_v2:
        fills = one_row(
            conn,
            f"""
SELECT
  COUNT(*) AS rows,
  COALESCE(SUM(units), 0.0) AS units,
  COALESCE(SUM(notional_usd), 0.0) AS notional_usd,
  MAX(ts_ms) AS last_fill_ts_ms
FROM fills_v2
{fill_where}
""",
            fill_params,
        )
    else:
        if not table_exists(conn, "trade_events"):
            warnings.append("missing fills_v2 and trade_events; fill metrics unavailable")
            fills = {"rows": 0, "units": 0.0, "notional_usd": 0.0, "last_fill_ts_ms": None}
        else:
            warnings.append("fills_v2 missing; using trade_events fallback for fill metrics")
            fallback_where = (
                "WHERE strategy_id='mm_rewards_v1' "
                "AND event_type IN ('ENTRY_FILL','ENTRY_FILL_RECONCILED')"
            )
            fallback_params: tuple[Any, ...] = ()
            if since_ms is not None:
                fallback_where += " AND ts_ms >= ?"
                fallback_params = (since_ms,)
            fills = one_row(
                conn,
                f"""
SELECT
  COUNT(*) AS rows,
  COALESCE(SUM(units), 0.0) AS units,
  COALESCE(SUM(notional_usd), 0.0) AS notional_usd,
  MAX(ts_ms) AS last_fill_ts_ms
FROM trade_events
{fallback_where}
""",
                fallback_params,
            )

    fills_by_side: Dict[str, Dict[str, Any]] = {}
    if has_fills_v2:
        cur = conn.execute(
            f"""
SELECT
  UPPER(COALESCE(TRIM(side), 'UNKNOWN')) AS side,
  COUNT(*) AS rows,
  COALESCE(SUM(units), 0.0) AS units,
  COALESCE(SUM(notional_usd), 0.0) AS notional_usd
FROM fills_v2
{fill_where}
GROUP BY UPPER(COALESCE(TRIM(side), 'UNKNOWN'))
""",
            fill_params,
        )
        for row in cur:
            fills_by_side[row["side"]] = {
                "rows": row["rows"],
                "units": row["units"],
                "notional_usd": row["notional_usd"],
            }
    elif table_exists(conn, "trade_events"):
        fallback_where = (
            "WHERE strategy_id='mm_rewards_v1' "
            "AND event_type IN ('ENTRY_FILL','ENTRY_FILL_RECONCILED')"
        )
        fallback_params: tuple[Any, ...] = ()
        if since_ms is not None:
            fallback_where += " AND ts_ms >= ?"
            fallback_params = (since_ms,)
        cur = conn.execute(
            f"""
SELECT
  UPPER(COALESCE(TRIM(side), 'UNKNOWN')) AS side,
  COUNT(*) AS rows,
  COALESCE(SUM(units), 0.0) AS units,
  COALESCE(SUM(notional_usd), 0.0) AS notional_usd
FROM trade_events
{fallback_where}
GROUP BY UPPER(COALESCE(TRIM(side), 'UNKNOWN'))
""",
            fallback_params,
        )
        for row in cur:
            fills_by_side[row["side"]] = {
                "rows": row["rows"],
                "units": row["units"],
                "notional_usd": row["notional_usd"],
            }

    settlement_source = "settlements_v2" if has_settlements_v2 else "unavailable"
    if has_settlements_v2:
        settlements = one_row(
            conn,
            f"""
SELECT
  COUNT(*) AS settled_rows,
  COALESCE(SUM(settled_pnl_usd), 0.0) AS realized_pnl_usd,
  COALESCE(SUM(CASE WHEN settled_pnl_usd > 0 THEN 1 ELSE 0 END), 0) AS wins,
  COALESCE(SUM(CASE WHEN settled_pnl_usd < 0 THEN 1 ELSE 0 END), 0) AS losses,
  MAX(ts_ms) AS last_settle_ts_ms
FROM settlements_v2
{settle_where}
""",
            settle_params,
        )
    else:
        warnings.append("settlements_v2 missing; settlement metrics unavailable")
        settlements = {
            "settled_rows": 0,
            "realized_pnl_usd": 0.0,
            "wins": 0,
            "losses": 0,
            "last_settle_ts_ms": None,
        }

    position_source = "positions_v2" if has_positions_v2 else "unavailable"
    if has_positions_v2:
        open_positions = one_row(
            conn,
            """
SELECT
  COUNT(*) AS open_rows,
  COALESCE(SUM(MAX(entry_units - exit_units, 0.0)), 0.0) AS remaining_units,
  COALESCE(SUM((COALESCE(entry_notional_usd,0.0) + COALESCE(entry_fee_usd,0.0))
              - (COALESCE(exit_notional_usd,0.0) + COALESCE(exit_fee_usd,0.0))), 0.0) AS remaining_cost_usd
FROM positions_v2
WHERE strategy_id='mm_rewards_v1'
  AND status='OPEN'
""",
            (),
        )
    else:
        warnings.append("positions_v2 missing; open-position metrics unavailable")
        open_positions = {"open_rows": 0, "remaining_units": 0.0, "remaining_cost_usd": 0.0}

    market_state_source = "mm_market_states_v1" if has_mm_states else "unavailable"
    if has_mm_states:
        market_states = one_row(
            conn,
            """
SELECT
  COUNT(*) AS tracked_markets,
  COALESCE(SUM(COALESCE(mtm_pnl_exit_usd, 0.0)), 0.0) AS mtm_pnl_exit_usd,
  COALESCE(SUM(ABS(COALESCE(imbalance_shares, 0.0))), 0.0) AS abs_imbalance_shares
FROM mm_market_states_v1
WHERE strategy_id='mm_rewards_v1'
""",
            (),
        )
    else:
        warnings.append("mm_market_states_v1 missing; MM state metrics unavailable")
        market_states = {
            "tracked_markets": 0,
            "mtm_pnl_exit_usd": 0.0,
            "abs_imbalance_shares": 0.0,
        }

    states_by_mode: Dict[str, int] = {}
    if has_mm_states:
        cur = conn.execute(
            """
SELECT
  COALESCE(mode, 'unknown') AS mode,
  COUNT(*) AS rows
FROM mm_market_states_v1
WHERE strategy_id='mm_rewards_v1'
GROUP BY COALESCE(mode, 'unknown')
ORDER BY rows DESC
"""
        )
        for row in cur:
            states_by_mode[row["mode"]] = row["rows"]

    states_by_state: Dict[str, int] = {}
    if has_mm_states:
        cur = conn.execute(
            """
SELECT
  COALESCE(state, 'unknown') AS state,
  COUNT(*) AS rows
FROM mm_market_states_v1
WHERE strategy_id='mm_rewards_v1'
GROUP BY COALESCE(state, 'unknown')
ORDER BY rows DESC
"""
        )
        for row in cur:
            states_by_state[row["state"]] = row["rows"]

    top_risk_markets: list[Dict[str, Any]] = []
    if has_mm_states:
        cur = conn.execute(
            """
SELECT
  condition_id,
  market_slug,
  timeframe,
  mode,
  state,
  reason,
  COALESCE(mtm_pnl_exit_usd, 0.0) AS mtm_pnl_exit_usd,
  COALESCE(imbalance_shares, 0.0) AS imbalance_shares,
  updated_at_ms
FROM mm_market_states_v1
WHERE strategy_id='mm_rewards_v1'
ORDER BY COALESCE(mtm_pnl_exit_usd, 0.0) ASC
LIMIT 12
"""
        )
        for row in cur:
            top_risk_markets.append(
                {
                    "condition_id": row["condition_id"],
                    "market_slug": row["market_slug"],
                    "timeframe": row["timeframe"],
                    "mode": row["mode"],
                    "state": row["state"],
                    "reason": row["reason"],
                    "mtm_pnl_exit_usd": row["mtm_pnl_exit_usd"],
                    "imbalance_shares": row["imbalance_shares"],
                    "updated_at_ms": row["updated_at_ms"],
                    "updated_at_utc": fmt_ts(row["updated_at_ms"]),
                }
            )

    return {
        "degraded": len(warnings) > 0,
        "warnings": warnings,
        "sources": {
            "fills": fill_source,
            "settlements": settlement_source,
            "open_positions": position_source,
            "market_states": market_state_source,
        },
        "fills": fills,
        "fills_by_side": fills_by_side,
        "settlements": settlements,
        "open_positions": open_positions,
        "market_states": market_states,
        "market_states_by_mode": states_by_mode,
        "market_states_by_state": states_by_state,
        "top_risk_markets": top_risk_markets,
    }


def parse_event_ts_ms(item: Dict[str, Any]) -> Optional[int]:
    raw = item.get("ts_ms")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    return None


def collect_event_metrics(events_path: pathlib.Path, since_ms: Optional[int]) -> Dict[str, Any]:
    counts = Counter()
    last_ts_ms: Optional[int] = None

    if not events_path.exists():
        return {"exists": False, "counts": {}, "last_ts_ms": None, "last_ts_utc": None}

    with events_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            payload = item.get("payload")
            if not isinstance(payload, dict):
                continue
            if payload.get("strategy_id") != "mm_rewards_v1":
                continue
            ts_ms = parse_event_ts_ms(item)
            if since_ms is not None and ts_ms is not None and ts_ms < since_ms:
                continue
            kind = str(item.get("kind") or "unknown")
            counts[kind] += 1
            if ts_ms is not None and (last_ts_ms is None or ts_ms > last_ts_ms):
                last_ts_ms = ts_ms

    top_counts = dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:30])
    return {
        "exists": True,
        "counts": top_counts,
        "distinct_kinds": len(counts),
        "last_ts_ms": last_ts_ms,
        "last_ts_utc": fmt_ts(last_ts_ms),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="MM phase baseline snapshot")
    parser.add_argument(
        "--window",
        default="24h",
        help="lookback window (e.g. 1h, 4h, 24h, 2d, all). default=24h",
    )
    parser.add_argument(
        "--output",
        help="output JSON path. default=reports/mm_phase_baseline_<utc>.json",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="print JSON to stdout without writing file",
    )
    args = parser.parse_args()

    now_ms = utc_now_ms()
    window_ms = parse_window_to_ms(args.window)
    since_ms = None if window_ms is None else now_ms - window_ms

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")

    snapshot = {
        "generated_at_ms": now_ms,
        "generated_at_utc": fmt_ts(now_ms),
        "window": args.window,
        "since_ms": since_ms,
        "since_utc": fmt_ts(since_ms),
        "cwd": str(ROOT),
        "git_branch": git_value(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "git_commit": git_value(["git", "rev-parse", "--short", "HEAD"]),
        "db_metrics": collect_db_metrics(conn, since_ms),
        "event_metrics": collect_event_metrics(EVENTS_PATH, since_ms),
    }
    conn.close()

    payload = json.dumps(snapshot, indent=2, sort_keys=True)
    if args.print_only:
        print(payload)
        return 0

    if args.output:
        out_path = pathlib.Path(args.output)
        if not out_path.is_absolute():
            out_path = ROOT / out_path
    else:
        stamp = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_path = ROOT / "reports" / f"mm_phase_baseline_{stamp}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(payload + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
