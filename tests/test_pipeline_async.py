from __future__ import annotations

import time
from datetime import datetime, timezone

from cloudasia_scanner.bet_client import BetClient, BetConfig
from cloudasia_scanner.live_monitor import WatchlistMatch
from cloudasia_scanner.money_manager import MoneyConfig, MoneyManager
from cloudasia_scanner.pipeline import PipelineConfig, PipelineRunner
from cloudasia_scanner.prematch_scan import ScanConfig


class SlowScanner:
    config = ScanConfig()

    def scan_once(self, now_utc: datetime | None = None):
        time.sleep(0.25)
        return []


class FastMonitor:
    def __init__(self) -> None:
        self.calls = 0
        self.watchlist = {
            "m1": WatchlistMatch(
                match_id="m1",
                competition_key="c1",
                home_team="A",
                away_team="B",
                favorite_side="home",
                favorite_line_abs=1.25,
                pre_match_bucket="B",
                fav_odds_pre=1.70,
                dog_odds_pre=2.05,
            )
        }
        self.states = {}

    def monitor_once(self, now_utc: datetime | None = None):
        self.calls += 1
        return []

    def recommended_poll_interval_seconds(self) -> int:
        return 0


def test_pipeline_tick_does_not_block_on_slow_scan() -> None:
    scanner = SlowScanner()
    monitor = FastMonitor()
    bet_client = BetClient(api_key=None, config=BetConfig(dry_run=True))
    money = MoneyManager(config=MoneyConfig(initial_bankroll=1000.0, bankroll_file="data/test_bankroll_async.json"))

    runner = PipelineRunner(
        scanner=scanner,  # type: ignore[arg-type]
        monitor=monitor,  # type: ignore[arg-type]
        bet_client=bet_client,
        money_manager=money,
        config=PipelineConfig(
            prematch_interval_seconds=60,
            detail_log_every_n_ticks=0,
            persist_watchlist=False,
            persist_signals=False,
            persist_bets=False,
        ),
    )

    started = time.perf_counter()
    runner._tick()
    elapsed = time.perf_counter() - started

    try:
        # Live monitor should still run immediately while scan is in-flight.
        assert monitor.calls == 1
        assert runner._scan_future is not None
        assert elapsed < 0.15

        # Next tick should collect completed scan result.
        time.sleep(0.35)
        runner._tick()
        assert runner._last_prematch_scan is not None
    finally:
        runner._scan_executor.shutdown(wait=False, cancel_futures=True)
