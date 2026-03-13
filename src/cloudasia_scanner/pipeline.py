from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .bet_client import BetClient, BetRecord
from .live_monitor import LiveLayerTwoMonitor, WatchlistMatch
from .models import PreMatchWatchRecord
from .prematch_scan import PreMatchScanner


@dataclass(slots=True)
class PipelineConfig:
    prematch_interval_seconds: int = 60
    output_dir: Path = field(default_factory=lambda: Path("data"))
    persist_watchlist: bool = True
    persist_signals: bool = True
    persist_bets: bool = True
    finished_cleanup_interval_seconds: int = 300


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


@dataclass
class PipelineRunner:
    scanner: PreMatchScanner
    monitor: LiveLayerTwoMonitor
    bet_client: BetClient
    config: PipelineConfig = field(default_factory=PipelineConfig)
    bet_log: dict[str, BetRecord] = field(default_factory=dict)
    _last_prematch_scan: datetime | None = field(default=None, repr=False)
    _last_cleanup: datetime | None = field(default=None, repr=False)

    # ── Public entry point ────────────────────────────────────────────────────

    def run_forever(self) -> None:
        print(f"[{_ts()}] Pipeline started. dry_run={self.bet_client.config.dry_run}", flush=True)
        try:
            while True:
                self._tick()
        except KeyboardInterrupt:
            print(f"\n[{_ts()}] Pipeline stopped by user.", flush=True)

    # ── Core tick ─────────────────────────────────────────────────────────────

    def _tick(self) -> None:
        now = datetime.now(timezone.utc)

        # Layer 1: pre-match scan (every prematch_interval_seconds)
        if self._should_scan_prematch(now):
            self._run_prematch_scan(now)
            self._last_prematch_scan = now

        # Layer 2: live monitor
        self._run_live_monitor(now)

        # Cleanup finished matches from memory
        if self._should_cleanup(now):
            self._cleanup_finished()
            self._last_cleanup = now

        # Sleep for the recommended live-poll interval
        sleep_s = self.monitor.recommended_poll_interval_seconds()
        time.sleep(sleep_s)

    # ── Layer 1 ───────────────────────────────────────────────────────────────

    def _should_scan_prematch(self, now: datetime) -> bool:
        if self._last_prematch_scan is None:
            return True
        elapsed = (now - self._last_prematch_scan).total_seconds()
        return elapsed >= self.config.prematch_interval_seconds

    def _run_prematch_scan(self, now: datetime) -> None:
        try:
            records = self.scanner.scan_once(now)
        except Exception as exc:
            print(f"[{_ts()}] [SCAN ERROR] {exc}", file=sys.stderr, flush=True)
            return

        new_count = 0
        for rec in records:
            if rec.match_id not in self.monitor.watchlist:
                self.monitor.watchlist[rec.match_id] = _record_to_watchlist(rec)
                new_count += 1

        verbose = self.scanner.config.verbose
        print(
            f"[{_ts()}] [SCAN] competitions scanned  "
            f"candidates={len(records)}  new_to_watchlist={new_count}  "
            f"watchlist_size={len(self.monitor.watchlist)}",
            flush=True,
        )

        if self.config.persist_watchlist and records:
            path = self.config.output_dir / "watchlist.jsonl"
            _append_jsonl(path, [r.to_dict() for r in records])

    # ── Layer 2 ───────────────────────────────────────────────────────────────

    def _run_live_monitor(self, now: datetime) -> None:
        if not self.monitor.watchlist:
            return

        try:
            signals = self.monitor.monitor_once(now)
        except Exception as exc:
            print(f"[{_ts()}] [LIVE ERROR] {exc}", file=sys.stderr, flush=True)
            return

        if not signals:
            return

        qualified_count = 0
        bet_count = 0
        signal_dicts = []

        for sig in signals:
            signal_dicts.append(sig.to_dict())

            if sig.signal_status != "qualified":
                continue
            qualified_count += 1

            if sig.match_id in self.bet_log:
                continue

            # Active bets cap
            active = sum(
                1 for b in self.bet_log.values()
                if b.status in ("ACCEPTED", "PENDING", "DRY_RUN")
            )
            if active >= self.bet_client.config.max_active_bets:
                print(
                    f"[{_ts()}] [BET SKIP] max_active_bets={self.bet_client.config.max_active_bets} reached "
                    f"for {sig.match_id}",
                    flush=True,
                )
                continue

            bet = self.bet_client.place_bet(sig)
            self.bet_log[sig.match_id] = bet
            bet_count += 1

            tag = "[DRY-RUN]" if bet.dry_run else "[BET]"
            print(
                f"[{_ts()}] {tag} {sig.home_team} vs {sig.away_team}  "
                f"over {sig.main_total_line} @ {sig.over_odds}  "
                f"stake={bet.stake} {self.bet_client.config.currency}  "
                f"status={bet.status}  ref={bet.reference_id}",
                flush=True,
            )

            if self.config.persist_bets:
                _append_jsonl(self.config.output_dir / "bet_log.jsonl", [bet.to_dict()])

        print(
            f"[{_ts()}] [LIVE] signals={len(signals)}  qualified={qualified_count}  bets_placed={bet_count}",
            flush=True,
        )

        if self.config.persist_signals and signal_dicts:
            _append_jsonl(self.config.output_dir / "live_signals.jsonl", signal_dicts)

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def _should_cleanup(self, now: datetime) -> bool:
        if self._last_cleanup is None:
            return False
        return (now - self._last_cleanup).total_seconds() >= self.config.finished_cleanup_interval_seconds

    def _cleanup_finished(self) -> None:
        finished_ids = [
            mid for mid, state in self.monitor.states.items()
            if state.state == "FINISHED"
        ]
        for mid in finished_ids:
            self.monitor.watchlist.pop(mid, None)
            self.monitor.states.pop(mid, None)
        if finished_ids:
            print(
                f"[{_ts()}] [CLEANUP] removed {len(finished_ids)} finished matches from watchlist",
                flush=True,
            )


# ── Helper ─────────────────────────────────────────────────────────────────────

def _record_to_watchlist(rec: PreMatchWatchRecord) -> WatchlistMatch:
    return WatchlistMatch(
        match_id=rec.match_id,
        competition_key=rec.competition_key,
        home_team=rec.home_team,
        away_team=rec.away_team,
        favorite_side=rec.favorite_side,
        favorite_line_abs=rec.favorite_line_abs,
        pre_match_bucket=rec.pre_match_bucket,
        fav_odds_pre=rec.fav_odds,
        dog_odds_pre=rec.dog_odds,
    )
