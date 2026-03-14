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
from .money_manager import MoneyManager
from .prematch_scan import PreMatchScanner


@dataclass(slots=True)
class PipelineConfig:
    prematch_interval_seconds: int = 60
    output_dir: Path = field(default_factory=lambda: Path("data"))
    persist_watchlist: bool = True
    persist_signals: bool = True
    persist_bets: bool = True
    finished_cleanup_interval_seconds: int = 300
    settlement_check_interval_seconds: int = 300
    # Print per-match TG line detail every N ticks (0 = disabled)
    detail_log_every_n_ticks: int = 4


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
    money_manager: MoneyManager
    config: PipelineConfig = field(default_factory=PipelineConfig)
    bet_log: dict[str, BetRecord] = field(default_factory=dict)
    _last_prematch_scan: datetime | None = field(default=None, repr=False)
    _last_cleanup: datetime | None = field(default=None, repr=False)
    _last_settlement_check: datetime | None = field(default=None, repr=False)
    _tick_count: int = field(default=0, repr=False)

    # ── Public entry point ────────────────────────────────────────────────────

    def run_forever(self) -> None:
        print(
            f"[{_ts()}] ═══ Pipeline started ═══"
            f"  dry_run={self.bet_client.config.dry_run}"
            f"  {self.money_manager.summary_line()}",
            flush=True,
        )
        try:
            while True:
                self._tick()
        except KeyboardInterrupt:
            print(f"\n[{_ts()}] Pipeline stopped by user.", flush=True)

    # ── Core tick ─────────────────────────────────────────────────────────────

    def _tick(self) -> None:
        self._tick_count += 1
        now = datetime.now(timezone.utc)

        if self._should_scan_prematch(now):
            self._run_prematch_scan(now)
            self._last_prematch_scan = now

        self._run_live_monitor(now)

        if self._should_check_settlements(now):
            self._settle_open_bets()
            self._last_settlement_check = now

        if self._should_cleanup(now):
            self._cleanup_finished()
            self._last_cleanup = now

        sleep_s = self.monitor.recommended_poll_interval_seconds()
        time.sleep(sleep_s)

    # ── Layer 1 ───────────────────────────────────────────────────────────────

    def _should_scan_prematch(self, now: datetime) -> bool:
        if self._last_prematch_scan is None:
            return True
        return (now - self._last_prematch_scan).total_seconds() >= self.config.prematch_interval_seconds

    def _run_prematch_scan(self, now: datetime) -> None:
        try:
            records = self.scanner.scan_once(now)
        except PermissionError:
            raise
        except Exception as exc:
            print(f"[{_ts()}] [SCAN ERROR] {exc}", file=sys.stderr, flush=True)
            return

        new_count = 0
        for rec in records:
            if rec.match_id not in self.monitor.watchlist:
                self.monitor.watchlist[rec.match_id] = _record_to_watchlist(rec)
                new_count += 1
                print(
                    f"[{_ts()}] [SCAN+] Added: {rec.home_team} vs {rec.away_team}"
                    f"  AH={rec.favorite_line_abs:.2f} bucket={rec.pre_match_bucket}"
                    f"  kickoff_in={rec.minutes_to_kickoff:.0f}min"
                    f"  fav_odds={rec.fav_odds:.2f}",
                    flush=True,
                )

        print(
            f"[{_ts()}] [SCAN] candidates={len(records)}"
            f"  new_added={new_count}"
            f"  watchlist_total={len(self.monitor.watchlist)}",
            flush=True,
        )

        if self.config.persist_watchlist and records:
            _append_jsonl(self.config.output_dir / "watchlist.jsonl", [r.to_dict() for r in records])

    # ── Layer 2 ───────────────────────────────────────────────────────────────

    def _run_live_monitor(self, now: datetime) -> None:
        watchlist_size = len(self.monitor.watchlist)

        # ── Empty watchlist ───────────────────────────────────────────────────
        if watchlist_size == 0:
            # Log once per minute so user knows we're alive
            if self._tick_count % 4 == 0:
                next_scan_in = max(
                    0,
                    self.config.prematch_interval_seconds
                    - int((now - self._last_prematch_scan).total_seconds())
                    if self._last_prematch_scan else 0,
                )
                print(
                    f"[{_ts()}] [LIVE] watchlist=0"
                    f"  — waiting for pre-match candidates"
                    f"  (next scan in {next_scan_in}s)",
                    flush=True,
                )
            return

        # ── Poll live markets ─────────────────────────────────────────────────
        try:
            signals = self.monitor.monitor_once(now)
        except PermissionError:
            raise
        except Exception as exc:
            print(f"[{_ts()}] [LIVE ERROR] {exc}", file=sys.stderr, flush=True)
            return

        # ── Process signals ───────────────────────────────────────────────────
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

            # Hard limit: max concurrent bets
            active = sum(
                1 for b in self.bet_log.values()
                if b.status in ("ACCEPTED", "PENDING", "DRY_RUN")
            )
            if active >= self.bet_client.config.max_active_bets:
                print(
                    f"[{_ts()}] [BET SKIP] max_active_bets={self.bet_client.config.max_active_bets}"
                    f" reached — {sig.home_team} vs {sig.away_team}",
                    flush=True,
                )
                continue

            # Kelly stake sizing
            kelly_stake = self.money_manager.compute_stake(sig.over_odds, sig.quality_score)
            if kelly_stake <= 0:
                print(
                    f"[{_ts()}] [BET SKIP] no positive edge"
                    f"  {sig.home_team} vs {sig.away_team}"
                    f"  odds={sig.over_odds:.3f}  quality={sig.quality_score:.1f}",
                    flush=True,
                )
                continue

            # Bankroll / daily-loss / exposure guards
            ok, reason = self.money_manager.can_bet(kelly_stake, now_utc=now)
            if not ok:
                print(
                    f"[{_ts()}] [BET BLOCK] {reason}"
                    f"  {sig.home_team} vs {sig.away_team}",
                    flush=True,
                )
                continue

            # Place bet
            bet = self.bet_client.place_bet(sig, stake_override=kelly_stake)
            self.bet_log[sig.match_id] = bet
            bet_count += 1

            if not bet.dry_run and bet.status in ("ACCEPTED", "PENDING"):
                self.money_manager.on_bet_placed(bet.stake, now_utc=now)

            _skipped = bet.status.startswith("SKIPPED") or bet.status == "ERROR"
            if _skipped:
                print(
                    f"[{_ts()}] [BET_FAIL] {sig.home_team} vs {sig.away_team}"
                    f"  status={bet.status}"
                    f"  reason={bet.rejection_reason}",
                    flush=True,
                )
            else:
                tag = "[DRY-RUN]" if bet.dry_run else "[BET ✓]"
                print(
                    f"[{_ts()}] {tag} {sig.home_team} vs {sig.away_team}"
                    f"  over {sig.main_total_line} @ {sig.over_odds}"
                    f"  stake={bet.stake:.2f} USDT  status={bet.status}"
                    f"  ref={bet.reference_id[:8]}…"
                    f"  quality={sig.quality_score:.0f}"
                    f"  {self.money_manager.summary_line()}",
                    flush=True,
                )

            if self.config.persist_bets:
                _append_jsonl(self.config.output_dir / "bet_log.jsonl", [bet.to_dict()])

        # ── Always print poll summary ─────────────────────────────────────────
        state_counts: dict[str, int] = {}
        for s in self.monitor.states.values():
            state_counts[s.state] = state_counts.get(s.state, 0) + 1
        states_str = " ".join(f"{k}={v}" for k, v in sorted(state_counts.items()) if v > 0)

        print(
            f"[{_ts()}] [LIVE] watchlist={watchlist_size}  [{states_str}]"
            f"  signals={len(signals)}  qualified={qualified_count}"
            f"  bets_this_tick={bet_count}  bets_total={len(self.bet_log)}"
            f"  {self.money_manager.summary_line()}",
            flush=True,
        )

        # ── Per-match detail every N ticks ────────────────────────────────────
        n = self.config.detail_log_every_n_ticks
        if n > 0 and self._tick_count % n == 0:
            for mid, state in self.monitor.states.items():
                watch = self.monitor.watchlist.get(mid)
                if watch is None:
                    continue
                line_str = f"TG={state.last_total_line:.2f}" if state.last_total_line is not None else "TG=?"
                odds_str = f"over={state.last_over_odds:.3f}" if state.last_over_odds is not None else ""
                target_str = f"(target={self.monitor.config.trigger_total_line:.2f})"
                already_bet = "✓bet" if mid in self.bet_log else ""
                print(
                    f"  ↳ {watch.home_team} vs {watch.away_team}"
                    f"  {line_str} {odds_str} {target_str}"
                    f"  AH={watch.favorite_line_abs:.2f}[{watch.pre_match_bucket}]"
                    f"  state={state.state} {already_bet}",
                    flush=True,
                )

        if self.config.persist_signals and signal_dicts:
            _append_jsonl(self.config.output_dir / "live_signals.jsonl", signal_dicts)

    # ── Settlement check ──────────────────────────────────────────────────────

    def _should_check_settlements(self, now: datetime) -> bool:
        if self._last_settlement_check is None:
            self._last_settlement_check = now
            return False
        return (
            (now - self._last_settlement_check).total_seconds()
            >= self.config.settlement_check_interval_seconds
        )

    def _settle_open_bets(self) -> None:
        for match_id, bet in list(self.bet_log.items()):
            if bet.dry_run or bet.status not in ("ACCEPTED", "PENDING"):
                continue

            settled, won, accepted_odds = self.bet_client.is_bet_settled(bet.reference_id)
            if not settled:
                continue

            new_status = "SETTLED_WON" if won else "SETTLED_LOST"
            updated = BetRecord(
                match_id=bet.match_id,
                reference_id=bet.reference_id,
                event_id=bet.event_id,
                market_key=bet.market_key,
                selection_key=bet.selection_key,
                handicap=bet.handicap,
                stake=bet.stake,
                requested_price=bet.requested_price,
                accepted_price=accepted_odds or bet.accepted_price,
                status=new_status,
                rejection_reason=bet.rejection_reason,
                bet_time=bet.bet_time,
                signal_quality=bet.signal_quality,
                home_team=bet.home_team,
                away_team=bet.away_team,
                favorite_side=bet.favorite_side,
                minute=bet.minute,
                score_home=bet.score_home,
                score_away=bet.score_away,
                dry_run=False,
            )
            self.bet_log[match_id] = updated

            self.money_manager.on_bet_settled(
                bet.stake,
                won=won,
                accepted_odds=accepted_odds or bet.accepted_price,
            )
            self.bet_client.on_bet_settled()

            result_tag = "WON ✓" if won else "LOST ✗"
            print(
                f"[{_ts()}] [SETTLE] {bet.home_team} vs {bet.away_team}"
                f"  stake={bet.stake:.2f}  {result_tag}"
                f"  {self.money_manager.summary_line()}",
                flush=True,
            )

            if self.config.persist_bets:
                _append_jsonl(self.config.output_dir / "bet_log.jsonl", [updated.to_dict()])

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def _should_cleanup(self, now: datetime) -> bool:
        if self._last_cleanup is None:
            self._last_cleanup = now
            return False
        return (
            (now - self._last_cleanup).total_seconds()
            >= self.config.finished_cleanup_interval_seconds
        )

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
                f"[{_ts()}] [CLEANUP] removed {len(finished_ids)} finished matches"
                f"  watchlist_remaining={len(self.monitor.watchlist)}",
                flush=True,
            )


# ── Helpers ─────────────────────────────────────────────────────────────────

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
