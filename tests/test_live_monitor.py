from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import pytest

from cloudasia_scanner.live_monitor import (
    LiveLayerTwoMonitor,
    LiveMonitorConfig,
    WatchlistMatch,
    load_watchlist,
)


class SequenceCloudbetClient:
    def __init__(self, events_by_match: dict[str, list[dict]]):
        self.events_by_match = events_by_match

    def get_event_odds(self, event_id: str, markets: list[str]) -> dict:
        sequence = self.events_by_match[event_id]
        if len(sequence) > 1:
            return sequence.pop(0)
        return sequence[0]


class UnauthorizedCloudbetClient:
    def get_event_odds(self, event_id: str, markets: list[str]) -> dict:
        raise PermissionError("Cloudbet API unauthorized")


def _event(
    match_id: str,
    total_line: float,
    over_odds: float,
    under_odds: float,
    minute: int,
    score: tuple[int, int],
    red_cards: tuple[int, int],
    market_status: str = "TRADING",
) -> dict:
    return {
        "id": match_id,
        "clock": {"minute": minute},
        "score": {"home": score[0], "away": score[1]},
        "redCards": {"home": red_cards[0], "away": red_cards[1]},
        "markets": {
            "soccer.total_goals": {
                "submarkets": {
                    f"period=ft&total={total_line}": {
                        "status": market_status,
                        "selections": {
                            "over": {"odds": str(over_odds), "status": market_status},
                            "under": {"odds": str(under_odds), "status": market_status},
                        },
                    }
                }
            }
        },
    }


def _event_v2(
    match_id: str,
    total_line: float,
    over_odds: float,
    under_odds: float,
    minute: int,
    score: tuple[int, int],
    red_cards: tuple[int, int],
    market_status: str = "SELECTION_ENABLED",
) -> dict:
    return {
        "id": match_id,
        "clock": {"minute": minute},
        "score": {"home": score[0], "away": score[1]},
        "redCards": {"home": red_cards[0], "away": red_cards[1]},
        "markets": {
            "soccer.total_goals": {
                "submarkets": {
                    "period=ft": {
                        "selections": [
                            {
                                "outcome": "over",
                                "params": f"total={total_line}",
                                "price": over_odds,
                                "status": market_status,
                            },
                            {
                                "outcome": "under",
                                "params": f"total={total_line}",
                                "price": under_odds,
                                "status": market_status,
                            },
                        ]
                    }
                }
            }
        },
    }


def _event_with_ah(
    match_id: str,
    total_line: float,
    minute: int,
    score: tuple[int, int],
    home_line_main: float,
    home_line_exact: float,
    market_status: str = "TRADING",
) -> dict:
    return {
        "id": match_id,
        "clock": {"minute": minute},
        "score": {"home": score[0], "away": score[1]},
        "redCards": {"home": 0, "away": 0},
        "markets": {
            "soccer.total_goals": {
                "submarkets": {
                    f"period=ft&total={total_line}": {
                        "status": market_status,
                        "selections": {
                            "over": {"odds": "1.90", "status": market_status},
                            "under": {"odds": "1.92", "status": market_status},
                        },
                    }
                }
            },
            "soccer.asian_handicap": {
                "submarkets": {
                    f"period=ft&handicap={home_line_main}": {
                        "status": market_status,
                        "selections": {
                            "home": {"odds": "1.90", "status": market_status},
                            "away": {"odds": "1.90", "status": market_status},
                        },
                    },
                    f"period=ft&handicap={home_line_exact}": {
                        "status": market_status,
                        "selections": {
                            "home": {"odds": "1.95", "status": market_status},
                            "away": {"odds": "1.85", "status": market_status},
                        },
                    },
                }
            },
        },
    }


def _watch(match_id: str = "match-1") -> WatchlistMatch:
    return WatchlistMatch(
        match_id=match_id,
        competition_key="comp-1",
        home_team="A Team",
        away_team="B Team",
        favorite_side="home",
        favorite_line_abs=1.25,
        pre_match_bucket="B",
        fav_odds_pre=1.65,
        dog_odds_pre=2.20,
    )


def test_load_watchlist_prefers_latest_scan_time(tmp_path) -> None:
    watchlist_path = tmp_path / "watchlist.jsonl"
    rows = [
        {
            "match_id": "m1",
            "competition_key": "c1",
            "home_team": "A",
            "away_team": "B",
            "favorite_side": "home",
            "favorite_line_abs": 1.25,
            "fav_odds": 1.60,
            "dog_odds": 2.30,
            "pre_match_bucket": "B",
            "scan_time": "2026-01-01T00:00:00+00:00",
            "watchlist_flag": True,
        },
        {
            "match_id": "m1",
            "competition_key": "c1",
            "home_team": "A",
            "away_team": "B",
            "favorite_side": "home",
            "favorite_line_abs": 1.25,
            "fav_odds": 1.66,
            "dog_odds": 2.25,
            "pre_match_bucket": "B",
            "scan_time": "2026-01-01T00:04:00+00:00",
            "watchlist_flag": True,
        },
    ]
    watchlist_path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    watchlist = load_watchlist(watchlist_path)

    assert "m1" in watchlist
    assert watchlist["m1"].fav_odds_pre == 1.66
    assert watchlist["m1"].dog_odds_pre == 2.25


def test_monitor_qualifies_immediately_on_trigger_line() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-1"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event(match_id, total_line=1.25, over_odds=1.90, under_odds=1.93, minute=60, score=(0, 0), red_cards=(0, 0)),
                _event(match_id, total_line=1.25, over_odds=1.92, under_odds=1.91, minute=61, score=(0, 0), red_cards=(0, 0)),
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    first = monitor.monitor_once(now)
    second = monitor.monitor_once(now + timedelta(seconds=30))

    assert len(first) == 1
    assert first[0].signal_status == "qualified"
    assert len(second) == 1
    assert second[0].signal_status == "qualified"
    assert second[0].signal == "TG125_LATE_FAVORITE_SIGNAL"
    assert second[0].action == "candidate_only"
    assert first[0].action == "candidate_only"


def test_monitor_ignores_reopen_and_still_qualifies_on_trigger_line() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-2"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event(match_id, total_line=1.50, over_odds=2.00, under_odds=1.85, minute=59, score=(0, 0), red_cards=(0, 0)),
                _event(
                    match_id,
                    total_line=1.25,
                    over_odds=1.92,
                    under_odds=1.91,
                    minute=60,
                    score=(0, 0),
                    red_cards=(0, 0),
                    market_status="SUSPENDED",
                ),
                _event(match_id, total_line=1.25, over_odds=1.91, under_odds=1.92, minute=60, score=(0, 0), red_cards=(0, 0)),
                _event(match_id, total_line=1.25, over_odds=1.90, under_odds=1.93, minute=61, score=(0, 0), red_cards=(0, 0)),
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    assert monitor.monitor_once(now) == []
    first_hit = monitor.monitor_once(now + timedelta(seconds=1))
    second_hit = monitor.monitor_once(now + timedelta(seconds=6))
    third_hit = monitor.monitor_once(now + timedelta(seconds=35))

    assert first_hit == []
    assert second_hit[0].signal_status == "qualified"
    assert third_hit[0].signal_status == "qualified"


def test_monitor_qualifies_even_if_old_filters_would_reject() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-3"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event(match_id, total_line=1.25, over_odds=1.70, under_odds=2.10, minute=80, score=(2, 0), red_cards=(1, 0)),
                _event(match_id, total_line=1.25, over_odds=1.70, under_odds=2.10, minute=80, score=(2, 0), red_cards=(1, 0)),
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    first = monitor.monitor_once(now)
    second = monitor.monitor_once(now + timedelta(seconds=30))

    assert first[0].signal_status == "qualified"
    assert second[0].signal_status == "qualified"
    assert second[0].reject_reason is None


def test_monitor_supports_cloudbet_v2_selection_list_shape() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-v2"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event_v2(match_id, total_line=1.25, over_odds=1.91, under_odds=1.91, minute=60, score=(0, 0), red_cards=(0, 0)),
                _event_v2(match_id, total_line=1.25, over_odds=1.92, under_odds=1.90, minute=61, score=(0, 0), red_cards=(0, 0)),
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    first = monitor.monitor_once(now)
    second = monitor.monitor_once(now + timedelta(seconds=30))

    assert first[0].signal_status == "qualified"
    assert second[0].signal_status == "qualified"


def test_strategy_a_triggers_on_line_below_125() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-a-low"
    client = SequenceCloudbetClient(
        {match_id: [_event(match_id, total_line=1.0, over_odds=1.88, under_odds=1.95, minute=70, score=(0, 0), red_cards=(0, 0))]}
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    records = monitor.monitor_once(now)

    assert len(records) == 1
    assert records[0].signal_status == "qualified"
    assert records[0].strategy_name == "STRATEGY_A_OU"
    assert records[0].bet_market_key in ("soccer.total_goals", "soccer.totalGoals", "soccer.totals")
    assert records[0].bet_selection_key == "over"
    assert records[0].bet_handicap == pytest.approx(1.0)


def test_strategy_b_triggers_on_draw_and_favorite_line_relaxed_to_075() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-b-ah"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event_with_ah(
                    match_id=match_id,
                    total_line=2.0,  # keep strategy A off
                    minute=60,
                    score=(1, 1),  # draw required
                    home_line_main=-0.5,  # favorite line <= 0.75
                    home_line_exact=-0.75,  # exact executable line
                )
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    records = monitor.monitor_once(now)

    assert len(records) == 1
    assert records[0].signal_status == "qualified"
    assert records[0].strategy_name == "STRATEGY_B_AH"
    assert records[0].bet_market_key in ("soccer.asian_handicap", "soccer.asianHandicap")
    assert records[0].bet_selection_key == "home"
    assert records[0].bet_handicap == pytest.approx(-0.75)


def test_strategy_b_requires_draw() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-b-not-draw"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event_with_ah(
                    match_id=match_id,
                    total_line=2.0,  # keep strategy A off
                    minute=60,
                    score=(2, 1),  # not draw
                    home_line_main=-0.5,
                    home_line_exact=-0.75,
                )
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    records = monitor.monitor_once(now)

    assert records == []


def test_strategy_b_triggers_for_away_favorite_draw() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-b-away"
    watch = _watch(match_id)
    watch.favorite_side = "away"
    watch.favorite_line_abs = 1.5

    client = SequenceCloudbetClient(
        {
            match_id: [
                _event_with_ah(
                    match_id=match_id,
                    total_line=2.0,  # keep strategy A off
                    minute=66,
                    score=(1, 1),  # draw required
                    home_line_main=0.5,  # away favorite metric <= 0.75
                    home_line_exact=0.75,  # away -0.75 representation
                )
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: watch}, config=LiveMonitorConfig())

    records = monitor.monitor_once(now)

    assert len(records) == 1
    assert records[0].signal_status == "qualified"
    assert records[0].strategy_name == "STRATEGY_B_AH"
    assert records[0].bet_selection_key == "away"
    # Cloudbet AH line is represented from home side; away -0.75 equals home +0.75.
    assert records[0].bet_handicap == pytest.approx(0.75)


def test_monitor_finishes_when_market_settled() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-4"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event(match_id, total_line=1.25, over_odds=1.90, under_odds=1.93, minute=60, score=(0, 0), red_cards=(0, 0)),
                _event(
                    match_id,
                    total_line=1.25,
                    over_odds=1.90,
                    under_odds=1.93,
                    minute=95,
                    score=(1, 0),
                    red_cards=(0, 0),
                    market_status="SETTLED",
                ),
                _event(match_id, total_line=1.25, over_odds=1.90, under_odds=1.93, minute=96, score=(1, 0), red_cards=(0, 0)),
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    qualified = monitor.monitor_once(now)
    finished_cycle = monitor.monitor_once(now + timedelta(seconds=30))
    after_finished = monitor.monitor_once(now + timedelta(seconds=60))

    assert qualified[0].signal_status == "qualified"
    assert finished_cycle == []
    assert after_finished == []
    assert monitor.states[match_id].state == "FINISHED"


def test_signal_record_includes_team_names() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-5"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event(match_id, total_line=1.25, over_odds=1.90, under_odds=1.93, minute=60, score=(0, 0), red_cards=(0, 0)),
            ]
        }
    )
    watch = WatchlistMatch(
        match_id=match_id,
        competition_key="comp-x",
        home_team="Real Madrid",
        away_team="Barcelona",
        favorite_side="home",
        favorite_line_abs=1.25,
        pre_match_bucket="B",
        fav_odds_pre=1.65,
        dog_odds_pre=2.20,
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: watch}, config=LiveMonitorConfig())

    records = monitor.monitor_once(now)

    assert len(records) == 1
    assert records[0].home_team == "Real Madrid"
    assert records[0].away_team == "Barcelona"


def test_qualified_action_is_candidate_only() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-6"
    client = SequenceCloudbetClient(
        {
            match_id: [
                _event(match_id, total_line=1.25, over_odds=1.90, under_odds=1.93, minute=60, score=(0, 0), red_cards=(0, 0)),
            ]
        }
    )
    monitor = LiveLayerTwoMonitor(client=client, watchlist={match_id: _watch(match_id)}, config=LiveMonitorConfig())

    records = monitor.monitor_once(now)

    assert records[0].signal_status == "qualified"
    assert records[0].action == "candidate_only"


def test_monitor_raises_permission_error() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    match_id = "match-auth"
    monitor = LiveLayerTwoMonitor(
        client=UnauthorizedCloudbetClient(),  # type: ignore[arg-type]
        watchlist={match_id: _watch(match_id)},
        config=LiveMonitorConfig(),
    )

    with pytest.raises(PermissionError):
        monitor.monitor_once(now)
