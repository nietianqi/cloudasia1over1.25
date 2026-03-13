from __future__ import annotations

from datetime import datetime, timezone

from cloudasia_scanner.bet_client import BetClient, BetConfig
from cloudasia_scanner.live_monitor import LiveSignalRecord


def _signal() -> LiveSignalRecord:
    return LiveSignalRecord(
        signal="TG125_LATE_FAVORITE_SIGNAL",
        signal_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        match_id="match-1",
        home_team="A Team",
        away_team="B Team",
        minute=60,
        score_home=0,
        score_away=0,
        favorite_side="home",
        favorite_handicap_abs=1.25,
        pre_match_bucket="B",
        main_total_line=1.25,
        over_odds=1.90,
        under_odds=1.90,
        market_status="TRADING",
        seconds_since_reopen=60.0,
        line_jump_count_last_60s=0,
        odds_jump_count_last_60s=0,
        signal_status="qualified",
        reject_reason=None,
        quality_score=80.0,
        confidence="high",
        action="candidate_only",
        fav_odds_pre=1.65,
        dog_odds_pre=2.20,
    )


def test_live_bet_requires_ack_token() -> None:
    client = BetClient(
        api_key="key",
        config=BetConfig(
            enabled=True,
            dry_run=False,
            require_live_ack=True,
            live_ack_phrase="LIVE_BETTING_ACK",
            live_ack_token="WRONG",
        ),
    )
    record = client.place_bet(_signal(), stake_override=10.0)

    assert record.status == "SKIPPED_ACK_REQUIRED"
    assert record.dry_run is False


def test_dry_run_bet_updates_active_count() -> None:
    client = BetClient(
        api_key=None,
        config=BetConfig(enabled=True, dry_run=True, require_live_ack=False),
    )
    assert client.active_bets_count == 0
    record = client.place_bet(_signal(), stake_override=10.0)
    assert record.status == "DRY_RUN"
    assert client.active_bets_count == 1
    client.on_bet_settled()
    assert client.active_bets_count == 0


def test_is_bet_settled_status_matching_is_strict() -> None:
    client = BetClient(api_key=None, config=BetConfig())

    client.check_bet_status = lambda _rid: {"status": "UNSETTLED", "result": "WON"}  # type: ignore[method-assign]
    settled, won, _ = client.is_bet_settled("r1")
    assert settled is False
    assert won is False

    client.check_bet_status = lambda _rid: {"status": "BET_SETTLED", "result": "HALF_WON", "price": "1.95"}  # type: ignore[method-assign]
    settled, won, odds = client.is_bet_settled("r2")
    assert settled is True
    assert won is True
    assert odds == 1.95
