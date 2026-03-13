"""Tests for the Kelly-based MoneyManager."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from cloudasia_scanner.money_manager import MoneyConfig, MoneyManager


def _manager(
    bankroll: float = 1000.0,
    kelly_fraction: float = 0.25,
    max_stake: float = 100.0,
    min_stake: float = 5.0,
    max_stake_pct: float = 0.05,
    daily_loss_pct: float = 0.10,
    max_exposure_pct: float = 0.25,
    tmp_dir: Path | None = None,
) -> MoneyManager:
    bf = str(tmp_dir / "bankroll.json") if tmp_dir else "data/bankroll_test.json"
    config = MoneyConfig(
        initial_bankroll=bankroll,
        kelly_fraction=kelly_fraction,
        base_win_rate=0.55,
        max_stake_pct=max_stake_pct,
        min_stake=min_stake,
        max_stake=max_stake,
        daily_loss_limit_pct=daily_loss_pct,
        max_concurrent_exposure_pct=max_exposure_pct,
        bankroll_file=bf,
    )
    return MoneyManager(config=config)


# ── Stake sizing ──────────────────────────────────────────────────────────────

def test_compute_stake_increases_with_higher_quality():
    m = _manager(bankroll=1000.0)
    low = m.compute_stake(over_odds=1.90, quality_score=55.0)
    high = m.compute_stake(over_odds=1.90, quality_score=85.0)
    assert high > low


def test_compute_stake_increases_with_better_odds():
    m = _manager(bankroll=1000.0)
    tight = m.compute_stake(over_odds=1.82, quality_score=70.0)
    juicy = m.compute_stake(over_odds=2.10, quality_score=70.0)
    assert juicy > tight


def test_compute_stake_never_below_min():
    m = _manager(bankroll=20.0, min_stake=5.0)
    stake = m.compute_stake(over_odds=1.81, quality_score=50.0)
    assert stake >= 5.0


def test_compute_stake_never_above_max():
    m = _manager(bankroll=100_000.0, max_stake=50.0)
    stake = m.compute_stake(over_odds=3.00, quality_score=100.0)
    assert stake <= 50.0


def test_compute_stake_never_above_pct_of_bankroll():
    m = _manager(bankroll=1000.0, max_stake_pct=0.05, max_stake=1000.0)
    stake = m.compute_stake(over_odds=3.00, quality_score=100.0)
    assert stake <= 1000.0 * 0.05 + 0.01  # small float tolerance


# ── Guards ────────────────────────────────────────────────────────────────────

def test_can_bet_blocks_when_daily_limit_hit():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, daily_loss_pct=0.10, tmp_dir=Path(td))
        # Simulate 10% daily loss
        m._daily_pnl = -100.01
        ok, reason = m.can_bet(10.0)
        assert not ok
        assert "daily_loss_limit" in reason


def test_can_bet_blocks_when_exposure_cap_reached():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, max_exposure_pct=0.25, tmp_dir=Path(td))
        m._open_exposure = 240.0  # close to 25%
        ok, reason = m.can_bet(20.0)  # would push to 260 > 250
        assert not ok
        assert "max_exposure" in reason


def test_can_bet_passes_normally():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        ok, reason = m.can_bet(10.0)
        assert ok
        assert reason == ""


# ── Lifecycle callbacks ───────────────────────────────────────────────────────

def test_on_bet_placed_updates_exposure_and_bankroll():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        m.on_bet_placed(20.0)
        assert m.open_exposure == 20.0
        assert m.bankroll == 980.0
        assert m.daily_pnl == -20.0


def test_on_bet_settled_win_restores_bankroll():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        m.on_bet_placed(20.0)
        m.on_bet_settled(20.0, won=True, accepted_odds=2.0)
        assert m.open_exposure == 0.0
        # 1000 - 20 (placed) + 40 (payout at 2.0) = 1020
        assert m.bankroll == pytest.approx(1020.0, abs=0.01)


def test_on_bet_settled_loss_keeps_bankroll_reduced():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        m.on_bet_placed(20.0)
        m.on_bet_settled(20.0, won=False)
        assert m.open_exposure == 0.0
        assert m.bankroll == pytest.approx(980.0, abs=0.01)  # stake already deducted


# ── Persistence ───────────────────────────────────────────────────────────────

def test_save_and_reload():
    with tempfile.TemporaryDirectory() as td:
        m = _manager(bankroll=1000.0, tmp_dir=Path(td))
        m.on_bet_placed(30.0)

        # Reload fresh instance pointing to same file
        m2 = _manager(bankroll=9999.0, tmp_dir=Path(td))  # initial_bankroll ignored
        assert m2.bankroll == pytest.approx(970.0, abs=0.01)
        assert m2.open_exposure == pytest.approx(30.0, abs=0.01)
