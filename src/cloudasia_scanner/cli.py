from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cloudasia_scanner.cloudbet_client import SPORTS_ODDS_BASE_URL, CloudbetClient
from cloudasia_scanner.prematch_scan import PreMatchScanner, ScanConfig


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cloudasia-scan",
        description="Scan Cloudbet pre-match soccer events and build a deep-AH watchlist.",
    )
    parser.add_argument("--minutes-to-kickoff-max", type=float, default=5.0, help="Only keep events within this window.")
    parser.add_argument("--min-favorite-line-abs", type=float, default=1.0, help="Minimum favorite handicap absolute value.")
    parser.add_argument("--min-favorite-odds", type=float, default=1.6, help="Minimum odds for the favorite side.")
    parser.add_argument("--interval-seconds", type=int, default=60, help="Daemon mode scan interval.")
    parser.add_argument("--output", type=Path, help="Optional output JSONL file.")
    parser.add_argument("--api-key", type=str, help="Cloudbet API key. Defaults to CLOUDBET_API_KEY env.")
    parser.add_argument("--api-key-header", type=str, default="X-API-Key", help="Header name for the API key.")
    parser.add_argument("--base-url", type=str, default=None, help="Override Cloudbet odds base URL.")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    return parser


def _print_records(records: list[dict]) -> None:
    print(json.dumps(records, ensure_ascii=False, indent=2))


def _append_jsonl(output_path: Path, records: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as fp:
        for row in records:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _run_once(scanner: PreMatchScanner, output: Path | None) -> None:
    records = [record.to_dict() for record in scanner.scan_once()]
    _print_records(records)
    if output is not None:
        _append_jsonl(output, records)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    config = ScanConfig(
        minutes_to_kickoff_max=args.minutes_to_kickoff_max,
        min_favorite_line_abs=args.min_favorite_line_abs,
        min_favorite_odds=args.min_favorite_odds,
    )
    api_key = args.api_key or os.getenv("CLOUDBET_API_KEY")
    client = CloudbetClient(
        base_url=args.base_url or SPORTS_ODDS_BASE_URL,
        api_key=api_key,
        api_key_header=args.api_key_header,
    )
    scanner = PreMatchScanner(client=client, config=config)

    try:
        if args.once:
            _run_once(scanner, args.output)
            return

        while True:
            _run_once(scanner, args.output)
            now = datetime.now(timezone.utc).isoformat()
            print(f"[{now}] scan complete, sleep {args.interval_seconds}s")
            time.sleep(args.interval_seconds)
    except PermissionError as exc:
        print(str(exc))
        print("Hint: set CLOUDBET_API_KEY or pass --api-key.")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
