from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

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


def test_monitor_transitions_trigger_to_qualified() -> None:
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
    assert first[0].signal_status == "triggered"
    assert len(second) == 1
    assert second[0].signal_status == "qualified"
    assert second[0].signal == "TG125_LATE_FAVORITE_SIGNAL"
    assert second[0].action == "candidate_only"
    assert first[0].action == "monitoring"


def test_monitor_cooling_after_reopen_then_qualifies() -> None:
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
    triggered = monitor.monitor_once(now + timedelta(seconds=1))
    cooling = monitor.monitor_once(now + timedelta(seconds=6))
    qualified = monitor.monitor_once(now + timedelta(seconds=35))

    assert triggered[0].signal_status == "triggered"
    assert cooling[0].signal_status == "cooling"
    assert cooling[0].reject_reason == "recent_reopen"
    assert qualified[0].signal_status == "qualified"


def test_monitor_rejected_with_expected_reasons() -> None:
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

    assert first[0].signal_status == "triggered"
    assert second[0].signal_status == "rejected"
    assert "minute_out_of_window" in second[0].reject_reason
    assert "score_not_allowed" in second[0].reject_reason
    assert "red_card_present" in second[0].reject_reason
    assert "over_odds_too_low" in second[0].reject_reason


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

    triggered = monitor.monitor_once(now)
    finished_cycle = monitor.monitor_once(now + timedelta(seconds=30))
    after_finished = monitor.monitor_once(now + timedelta(seconds=60))

    assert triggered[0].signal_status == "triggered"
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


def test_triggered_action_is_monitoring() -> None:
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

    assert records[0].signal_status == "triggered"
    assert records[0].action == "monitoring"
