from __future__ import annotations

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from cloudasia_scanner.money_manager import MoneyConfig, MoneyManager


def _manager(
    bankroll: float = 1000.0,
    tmp_dir: Path | None = None,
    **overrides,
) -> MoneyManager:
    bankroll_file = str(tmp_dir / "bankroll.json") if tmp_dir else "data/bankroll_test.json"
    cfg = MoneyConfig(
        initial_bankroll=bankroll,
        bankroll_file=bankroll_file,
        **overrides,
    )
    return MoneyManager(config=cfg)


def test_stake_increases_with_quality_and_odds() -> None:
    m = _manager(bankroll=1000.0)
    low_quality = m.compute_stake(over_odds=1.95, quality_score=60.0)
    high_quality = m.compute_stake(over_odds=1.95, quality_score=85.0)
    better_odds = m.compute_stake(over_odds=2.10, quality_score=85.0)

    assert low_quality > 0
    assert high_quality > low_quality
    assert better_odds > high_quality


def test_stake_returns_zero_when_edge_not_positive() -> None:
    m = _manager(bankroll=1000.0, force_min_stake=False)
    stake = m.compute_stake(over_odds=1.80, quality_score=50.0)
    assert stake == 0.0


def test_stake_obeys_caps() -> None:
    m = _manager(bankroll=100_000.0, max_stake_pct=0.05, max_stake=50.0)
    stake = m.compute_stake(over_odds=3.00, quality_score=95.0)
    assert stake <= 50.0
    assert stake <= 100_000.0 * 0.05


def test_can_bet_blocks_daily_loss_limit() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td), daily_loss_limit_pct=0.10)
        m._daily_pnl = -101.0
        ok, reason = m.can_bet(10.0)
        assert not ok
        assert "daily_loss_limit" in reason


def test_can_bet_blocks_max_drawdown() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td), max_drawdown_pct=0.20)
        m._peak_bankroll = 1000.0
        m._bankroll = 790.0
        ok, reason = m.can_bet(10.0)
        assert not ok
        assert "max_drawdown" in reason


def test_can_bet_blocks_consecutive_losses_and_daily_bet_count() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td), max_consecutive_losses=3, max_daily_bets=5)
        m._consecutive_losses = 3
        ok, reason = m.can_bet(10.0)
        assert not ok
        assert "max_consecutive_losses" in reason

        m._consecutive_losses = 0
        m._daily_bet_count = 5
        ok, reason = m.can_bet(10.0)
        assert not ok
        assert "max_daily_bets" in reason


def test_can_bet_blocks_cooldown() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td), bet_cooldown_seconds=30)
        now = datetime.now(timezone.utc)
        m._last_bet_time = now - timedelta(seconds=10)
        ok, reason = m.can_bet(10.0, now_utc=now)
        assert not ok
        assert "bet_cooldown" in reason


def test_can_bet_blocks_exposure() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td), max_concurrent_exposure_pct=0.25)
        m._open_exposure = 240.0
        ok, reason = m.can_bet(20.0)
        assert not ok
        assert "max_exposure" in reason


def test_bet_lifecycle_updates_bankroll_and_streak() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        t0 = datetime.now(timezone.utc)

        m.on_bet_placed(20.0, now_utc=t0)
        assert m.open_exposure == 20.0
        assert m.bankroll == 980.0
        assert m.daily_pnl == -20.0
        assert m.daily_bet_count == 1

        m.on_bet_settled(20.0, won=False)
        assert m.open_exposure == 0.0
        assert m.bankroll == 980.0
        assert m.consecutive_losses == 1

        m.on_bet_placed(20.0, now_utc=t0 + timedelta(minutes=1))
        m.on_bet_settled(20.0, won=True, accepted_odds=2.0)
        assert m.bankroll == pytest.approx(1000.0, abs=0.01)
        assert m.peak_bankroll >= 1000.0
        assert m.consecutive_losses == 0


def test_save_and_reload_persists_state() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        now = datetime.now(timezone.utc)
        m.on_bet_placed(30.0, now_utc=now)
        m.on_bet_settled(30.0, won=False)

        m2 = _manager(bankroll=9999.0, tmp_dir=Path(td))
        assert m2.bankroll == pytest.approx(970.0, abs=0.01)
        assert m2.open_exposure == pytest.approx(0.0, abs=0.01)
        assert m2.consecutive_losses == 1


def test_sync_bankroll_from_account_resets_runtime_state() -> None:
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        now = datetime.now(timezone.utc)
        m.on_bet_placed(25.0, now_utc=now)
        m.on_bet_settled(25.0, won=False)

        m.sync_bankroll_from_account(777.77)

        assert m.bankroll == pytest.approx(777.77, abs=0.01)
        assert m.peak_bankroll == pytest.approx(777.77, abs=0.01)
        assert m.open_exposure == pytest.approx(0.0, abs=0.01)
        assert m.daily_pnl == pytest.approx(0.0, abs=0.01)
        assert m.daily_bet_count == 0
        assert m.consecutive_losses == 0
