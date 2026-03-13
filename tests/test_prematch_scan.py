from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cloudasia_scanner.prematch_scan import PreMatchScanner, ScanConfig, classify_deep_ah_bucket


class FakeCloudbetClient:
    def __init__(self, payload_by_competition: dict):
        self.payload_by_competition = payload_by_competition

    def get_soccer_competitions(self) -> list[dict]:
        return [{"key": key, "name": "Test League"} for key in self.payload_by_competition]

    def get_competition_odds(self, competition_key: str, markets: list[str]) -> dict:
        return self.payload_by_competition[competition_key]


def _event(start_at: datetime, home_line: float, home_odds: float, away_odds: float, status: str = "TRADING") -> dict:
    return {
        "id": "match-1",
        "status": status,
        "startsAt": start_at.isoformat(),
        "home": {"name": "A Team"},
        "away": {"name": "B Team"},
        "markets": {
            "soccer.asian_handicap": {
                "submarkets": {
                    f"period=ft&handicap={home_line}": {
                        "selections": {
                            "home": {"odds": str(home_odds), "status": "TRADING"},
                            "away": {"odds": str(away_odds), "status": "TRADING"},
                        }
                    }
                }
            }
        },
    }


def _event_v2(start_at: datetime, home_line: float, home_odds: float, away_odds: float, status: str = "TRADING") -> dict:
    return {
        "id": "match-v2",
        "status": status,
        "startsAt": start_at.isoformat(),
        "home": {"name": "A Team"},
        "away": {"name": "B Team"},
        "markets": {
            "soccer.asian_handicap": {
                "submarkets": {
                    "period=ft": {
                        "selections": [
                            {
                                "outcome": "home",
                                "params": f"handicap={home_line}",
                                "price": home_odds,
                                "status": "SELECTION_ENABLED",
                            },
                            {
                                "outcome": "away",
                                "params": f"handicap={home_line}",
                                "price": away_odds,
                                "status": "SELECTION_ENABLED",
                            },
                        ]
                    }
                }
            }
        },
    }


def test_bucket_classifier() -> None:
    assert classify_deep_ah_bucket(1.0) == "A"
    assert classify_deep_ah_bucket(1.25) == "B"
    assert classify_deep_ah_bucket(1.5) == "C"
    assert classify_deep_ah_bucket(2.0) == "D"
    assert classify_deep_ah_bucket(2.25) == "E"
    assert classify_deep_ah_bucket(1.75) is None


def test_scan_filters_by_time_line_and_min_favorite_odds() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = {
        "comp-1": {
            "events": [
                _event(now + timedelta(minutes=3), home_line=-1.25, home_odds=1.65, away_odds=2.20),
                _event(now + timedelta(minutes=3), home_line=-1.0, home_odds=1.55, away_odds=2.40),
                _event(now + timedelta(minutes=8), home_line=-1.5, home_odds=1.80, away_odds=2.05),
            ]
        }
    }
    scanner = PreMatchScanner(
        client=FakeCloudbetClient(payload),
        config=ScanConfig(minutes_to_kickoff_max=5, min_favorite_line_abs=1.0, min_favorite_odds=1.6),
    )

    records = scanner.scan_once(now_utc=now)

    assert len(records) == 1
    row = records[0]
    assert row.match_id == "match-1"
    assert row.pre_match_bucket == "B"
    assert row.favorite_side == "home"
    assert row.favorite_line_abs == 1.25
    assert row.fav_odds == 1.65


def test_away_favorite_is_normalized() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = {
        "comp-2": {
            "events": [
                _event(now + timedelta(minutes=2), home_line=1.5, home_odds=2.05, away_odds=1.70),
            ]
        }
    }
    scanner = PreMatchScanner(client=FakeCloudbetClient(payload), config=ScanConfig())

    records = scanner.scan_once(now_utc=now)

    assert len(records) == 1
    row = records[0]
    assert row.favorite_side == "away"
    assert row.favorite_team == "B Team"
    assert row.underdog_team == "A Team"
    assert row.pre_match_bucket == "C"


def test_scan_supports_cloudbet_v2_selection_list_shape() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = {
        "comp-v2": {
            "events": [
                _event_v2(now + timedelta(minutes=3), home_line=-1.25, home_odds=1.65, away_odds=2.20),
            ]
        }
    }
    scanner = PreMatchScanner(
        client=FakeCloudbetClient(payload),
        config=ScanConfig(minutes_to_kickoff_max=5, min_favorite_line_abs=1.0, min_favorite_odds=1.6),
    )

    records = scanner.scan_once(now_utc=now)

    assert len(records) == 1
    row = records[0]
    assert row.match_id == "match-v2"
    assert row.favorite_side == "home"
    assert row.favorite_line_abs == 1.25
    assert row.pre_match_bucket == "B"
