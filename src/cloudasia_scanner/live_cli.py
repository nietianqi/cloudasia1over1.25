from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from .cloudbet_client import SPORTS_ODDS_BASE_URL, CloudbetClient
from .live_monitor import LiveLayerTwoMonitor, LiveMonitorConfig, load_watchlist


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cloudasia-live-monitor",
        description="Monitor pre-filtered matches and evaluate TG125 late favorite live candidates.",
    )
    parser.add_argument("--watchlist", type=Path, required=True, help="Path to pre-match watchlist JSON or JSONL.")
    parser.add_argument("--output", type=Path, help="Optional output JSONL file.")
    parser.add_argument("--trigger-total-line", type=float, default=1.25, help="Trigger line for main total market.")
    parser.add_argument("--primary-minute-start", type=int, default=55, help="Primary minute window start.")
    parser.add_argument("--primary-minute-end", type=int, default=72, help="Primary minute window end.")
    parser.add_argument("--min-over-odds", type=float, default=1.8, help="Minimum over odds at trigger line.")
    parser.add_argument(
        "--min-seconds-since-reopen",
        type=float,
        default=20.0,
        help="Minimum seconds after market reopen before qualification.",
    )
    parser.add_argument("--max-line-jumps-last-60s", type=int, default=1, help="Reject if line jumps above this count.")
    parser.add_argument("--max-odds-jumps-last-60s", type=int, default=3, help="Reject if odds jumps above this count.")
    parser.add_argument("--normal-interval-seconds", type=int, default=15, help="Default polling interval.")
    parser.add_argument("--fast-interval-seconds", type=int, default=5, help="Fast polling interval.")
    parser.add_argument("--fast-line-threshold", type=float, default=1.75, help="Use fast polling below this line.")
    parser.add_argument("--api-key", type=str, help="Cloudbet API key. Defaults to CLOUDBET_API_KEY env.")
    parser.add_argument("--api-key-header", type=str, default="X-API-Key", help="Header name for API key.")
    parser.add_argument("--base-url", type=str, default=None, help="Override Cloudbet odds base URL.")
    parser.add_argument("--once", action="store_true", help="Run one monitoring cycle and exit.")
    return parser


def _append_jsonl(output_path: Path, rows: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _run_once(monitor: LiveLayerTwoMonitor, output_path: Path | None) -> int:
    signals = [signal.to_dict() for signal in monitor.monitor_once()]
    print(json.dumps(signals, ensure_ascii=False, indent=2))
    if output_path is not None:
        _append_jsonl(output_path, signals)
    return len(signals)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    watchlist = load_watchlist(args.watchlist)
    if not watchlist:
        print("No valid watchlist rows found. Run pre-match scan first and pass --watchlist.")
        raise SystemExit(1)

    config = LiveMonitorConfig(
        trigger_total_line=args.trigger_total_line,
        primary_minute_start=args.primary_minute_start,
        primary_minute_end=args.primary_minute_end,
        min_over_odds=args.min_over_odds,
        min_seconds_since_reopen=args.min_seconds_since_reopen,
        max_line_jumps_last_60s=args.max_line_jumps_last_60s,
        max_odds_jumps_last_60s=args.max_odds_jumps_last_60s,
        normal_poll_interval_seconds=args.normal_interval_seconds,
        fast_poll_interval_seconds=args.fast_interval_seconds,
        fast_poll_line_threshold=args.fast_line_threshold,
    )

    api_key = args.api_key or os.getenv("CLOUDBET_API_KEY")
    client = CloudbetClient(
        base_url=args.base_url or SPORTS_ODDS_BASE_URL,
        api_key=api_key,
        api_key_header=args.api_key_header,
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist=watchlist, config=config)

    try:
        if args.once:
            _run_once(monitor, args.output)
            return

        while True:
            count = _run_once(monitor, args.output)
            poll_seconds = monitor.recommended_poll_interval_seconds()
            now = datetime.now(timezone.utc).isoformat()
            print(f"[{now}] signals={count}, next poll in {poll_seconds}s")
            time.sleep(poll_seconds)
    except PermissionError as exc:
        print(str(exc))
        print("Hint: set CLOUDBET_API_KEY or pass --api-key.")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
