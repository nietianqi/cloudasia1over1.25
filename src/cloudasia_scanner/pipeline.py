from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
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
    # Print per-match line detail every N ticks (0 disables detail logs)
    detail_log_every_n_ticks: int = 4


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_bet_line_for_log(signal: Any) -> str:
    selection = str(signal.bet_selection_key)
    handicap = float(signal.bet_handicap)
    if signal.strategy_name == "STRATEGY_B_AH" and selection == "away" and handicap > 0:
        # Cloudbet AH handicap is encoded from the home side perspective.
        return f"{selection} -{handicap:.2f} (api_handicap={handicap:.2f})"
    return f"{selection} {handicap}"


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

    _scan_executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(max_workers=1),
        repr=False,
    )
    _scan_future: Future[list[PreMatchWatchRecord]] | None = field(default=None, repr=False)
    _scan_started_at: datetime | None = field(default=None, repr=False)

    def run_forever(self) -> None:
        print(
            f"[{_ts()}] Pipeline started"
            f"  dry_run={self.bet_client.config.dry_run}"
            f"  {self.money_manager.summary_line()}",
            flush=True,
        )
        try:
            while True:
                self._tick()
        except KeyboardInterrupt:
            print(f"\n[{_ts()}] Pipeline stopped by user.", flush=True)
        finally:
            self._scan_executor.shutdown(wait=False, cancel_futures=True)

    def _tick(self) -> None:
        self._tick_count += 1
        now = datetime.now(timezone.utc)

        self._collect_finished_prematch_scan(now)
        self._maybe_start_prematch_scan(now)

        self._run_live_monitor(now)

        if self._should_check_settlements(now):
            self._settle_open_bets()
            self._last_settlement_check = now

        if self._should_cleanup(now):
            self._cleanup_finished()
            self._last_cleanup = now

        time.sleep(self.monitor.recommended_poll_interval_seconds())

    def _should_scan_prematch(self, now: datetime) -> bool:
        if self._last_prematch_scan is None:
            return True
        elapsed = (now - self._last_prematch_scan).total_seconds()
        return elapsed >= self.config.prematch_interval_seconds

    def _maybe_start_prematch_scan(self, now: datetime) -> None:
        if self._scan_future is not None and not self._scan_future.done():
            return
        if not self._should_scan_prematch(now):
            return

        self._scan_started_at = now
        self._scan_future = self._scan_executor.submit(self.scanner.scan_once, now)
        print(
            f"[{_ts()}] [SCAN] async started  watchlist={len(self.monitor.watchlist)}",
            flush=True,
        )

    def _collect_finished_prematch_scan(self, now: datetime) -> None:
        if self._scan_future is None or not self._scan_future.done():
            return

        started_at = self._scan_started_at or now
        duration_s = (now - started_at).total_seconds()

        try:
            records = self._scan_future.result()
        except PermissionError:
            raise
        except Exception as exc:
            print(f"[{_ts()}] [SCAN ERROR] {exc}", file=sys.stderr, flush=True)
            records = []

        self._scan_future = None
        self._scan_started_at = None
        self._last_prematch_scan = now

        if records:
            self._apply_prematch_records(records, duration_s)
        else:
            print(
                f"[{_ts()}] [SCAN] completed  candidates=0"
                f"  duration={duration_s:.1f}s"
                f"  watchlist={len(self.monitor.watchlist)}",
                flush=True,
            )

    def _apply_prematch_records(self, records: list[PreMatchWatchRecord], duration_s: float) -> None:
        new_count = 0
        update_count = 0

        for rec in records:
            row = _record_to_watchlist(rec)
            if rec.match_id in self.monitor.watchlist:
                update_count += 1
            else:
                new_count += 1
            self.monitor.watchlist[rec.match_id] = row

        print(
            f"[{_ts()}] [SCAN] completed  candidates={len(records)}"
            f"  new={new_count} updated={update_count}"
            f"  duration={duration_s:.1f}s"
            f"  watchlist={len(self.monitor.watchlist)}",
            flush=True,
        )

        if self.config.persist_watchlist:
            _append_jsonl(self.config.output_dir / "watchlist.jsonl", [r.to_dict() for r in records])

    def _run_live_monitor(self, now: datetime) -> None:
        watchlist_size = len(self.monitor.watchlist)
        n = max(1, self.config.detail_log_every_n_ticks)

        if watchlist_size == 0:
            if self._tick_count % n == 0:
                print(f"[{_ts()}] [LIVE] watchlist=0  waiting for scan", flush=True)
            return

        try:
            signals = self.monitor.monitor_once(now)
        except PermissionError:
            raise
        except Exception as exc:
            print(f"[{_ts()}] [LIVE ERROR] {exc}", file=sys.stderr, flush=True)
            return

        qualified_count = 0
        bet_count = 0
        signal_dicts: list[dict[str, Any]] = []

        for sig in signals:
            signal_dicts.append(sig.to_dict())
            if sig.signal_status != "qualified":
                continue
            qualified_count += 1

            if sig.match_id in self.bet_log:
                continue

            active = sum(
                1
                for b in self.bet_log.values()
                if b.status in ("ACCEPTED", "PENDING", "DRY_RUN")
            )
            if active >= self.bet_client.config.max_active_bets:
                print(
                    f"[{_ts()}] [BET SKIP] max_active_bets reached"
                    f"  match={sig.match_id}",
                    flush=True,
                )
                continue

            stake = self.money_manager.compute_stake(sig.bet_price, sig.quality_score)
            if stake <= 0:
                print(
                    f"[{_ts()}] [BET SKIP] no positive edge"
                    f"  match={sig.match_id} odds={sig.bet_price:.3f}"
                    f"  quality={sig.quality_score:.1f}",
                    flush=True,
                )
                continue

            ok, reason = self.money_manager.can_bet(stake, now_utc=now)
            if not ok:
                print(f"[{_ts()}] [BET BLOCK] {reason}  match={sig.match_id}", flush=True)
                continue

            bet = self.bet_client.place_bet(sig, stake_override=stake)
            self.bet_log[sig.match_id] = bet
            bet_count += 1
            watch = self.monitor.watchlist.get(sig.match_id)
            if watch is not None:
                if sig.strategy_name == "STRATEGY_A_OU":
                    watch.strategy_a_done = True
                elif sig.strategy_name == "STRATEGY_B_AH":
                    watch.strategy_b_done = True
                watch.bet_done = True

            if not bet.dry_run and bet.status in ("ACCEPTED", "PENDING"):
                self.money_manager.on_bet_placed(bet.stake, now_utc=now)

            if bet.status.startswith("SKIPPED") or bet.status == "ERROR":
                print(
                    f"[{_ts()}] [BET FAIL] match={sig.match_id}"
                    f"  status={bet.status}"
                    f"  reason={bet.rejection_reason}",
                    flush=True,
                )
            else:
                tag = "[DRY-RUN]" if bet.dry_run else "[BET]"
                print(
                    f"[{_ts()}] {tag} {sig.home_team} vs {sig.away_team}"
                    f"  {sig.strategy_name}"
                    f"  {_format_bet_line_for_log(sig)} @ {sig.bet_price}"
                    f"  stake={bet.stake:.2f}"
                    f"  status={bet.status}"
                    f"  ref={bet.reference_id}"
                    f"  {self.money_manager.summary_line()}",
                    flush=True,
                )

            if self.config.persist_bets:
                _append_jsonl(self.config.output_dir / "bet_log.jsonl", [bet.to_dict()])

        if signals or self._tick_count % n == 0:
            print(
                f"[{_ts()}] [LIVE] watchlist={watchlist_size}"
                f"  signals={len(signals)} qualified={qualified_count}"
                f"  bets_this_tick={bet_count} total_bets={len(self.bet_log)}"
                f"  {self.money_manager.summary_line()}",
                flush=True,
            )

        if self.config.detail_log_every_n_ticks > 0 and self._tick_count % n == 0:
            for mid, state in self.monitor.states.items():
                watch = self.monitor.watchlist.get(mid)
                if watch is None:
                    continue
                line = "?" if state.last_total_line is None else f"{state.last_total_line:.2f}"
                over = "?" if state.last_over_odds is None else f"{state.last_over_odds:.3f}"
                print(
                    f"  -> {watch.home_team} vs {watch.away_team}"
                    f"  TG={line} over={over}"
                    f"  target={self.monitor.config.trigger_total_line:.2f}"
                    f"  state={state.state}",
                    flush=True,
                )

        if self.config.persist_signals and signal_dicts:
            _append_jsonl(self.config.output_dir / "live_signals.jsonl", signal_dicts)

    def _should_check_settlements(self, now: datetime) -> bool:
        if self._last_settlement_check is None:
            self._last_settlement_check = now
            return False
        elapsed = (now - self._last_settlement_check).total_seconds()
        return elapsed >= self.config.settlement_check_interval_seconds

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

            result = "WON" if won else "LOST"
            print(
                f"[{_ts()}] [SETTLE] match={match_id}  result={result}"
                f"  {self.money_manager.summary_line()}",
                flush=True,
            )

            if self.config.persist_bets:
                _append_jsonl(self.config.output_dir / "bet_log.jsonl", [updated.to_dict()])

    def _should_cleanup(self, now: datetime) -> bool:
        if self._last_cleanup is None:
            self._last_cleanup = now
            return False
        elapsed = (now - self._last_cleanup).total_seconds()
        return elapsed >= self.config.finished_cleanup_interval_seconds

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
                f"[{_ts()}] [CLEANUP] removed={len(finished_ids)}"
                f"  watchlist={len(self.monitor.watchlist)}",
                flush=True,
            )


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
        strategy_a_done=rec.strategy_a_done,
        strategy_b_done=rec.strategy_b_done,
        bet_done=rec.bet_done,
    )
