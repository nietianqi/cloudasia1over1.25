from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs

from .cloudbet_client import CloudbetClient
from .models import PreMatchWatchRecord

AH_MARKET_KEYS = ("soccer.asian_handicap", "soccer.asianHandicap")


@dataclass(slots=True)
class ScanConfig:
    minutes_to_kickoff_max: float = 5.0
    min_favorite_line_abs: float = 1.0
    min_favorite_odds: float = 1.6
    markets: list[str] = field(default_factory=lambda: list(AH_MARKET_KEYS))


@dataclass(slots=True)
class MainAsianHandicapLine:
    line_home: float
    home_odds: float
    away_odds: float
    favorite_side: str
    favorite_line_abs: float
    fav_odds: float
    dog_odds: float
    imbalance: float


def classify_deep_ah_bucket(line_abs: float) -> str | None:
    rounded = round(line_abs * 4) / 4
    if abs(rounded - 1.0) < 1e-9:
        return "A"
    if abs(rounded - 1.25) < 1e-9:
        return "B"
    if abs(rounded - 1.5) < 1e-9:
        return "C"
    if abs(rounded - 2.0) < 1e-9:
        return "D"
    if rounded >= 2.25:
        return "E"
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso8601(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _selection_status_ok(status: Any) -> bool:
    if not isinstance(status, str):
        return True
    upper = status.upper()
    blocked = ("SUSPENDED", "SETTLED", "CANCELLED", "CLOSED")
    return not any(tag in upper for tag in blocked)


def _event_status_prematch(status: Any) -> bool:
    if not isinstance(status, str):
        return True
    upper = status.upper()
    if "LIVE" in upper:
        return False
    return "TRADING" in upper or "OPEN" in upper or "ACTIVE" in upper


def _extract_team_name(event: dict[str, Any], side: str, fallback: str) -> str:
    value = event.get(side)
    if isinstance(value, dict):
        name = value.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(value, str) and value.strip():
        return value.strip()

    teams = event.get("teams")
    if isinstance(teams, list) and len(teams) >= 2:
        index = 0 if side == "home" else 1
        item = teams[index]
        if isinstance(item, dict):
            name = item.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
        if isinstance(item, str) and item.strip():
            return item.strip()

    return fallback


def _extract_event_id(event: dict[str, Any]) -> str:
    for key in ("id", "key", "eventId", "event_id"):
        value = event.get(key)
        if isinstance(value, (str, int)):
            return str(value)
    return "unknown-match-id"


def _extract_kickoff(event: dict[str, Any]) -> datetime | None:
    for key in ("startsAt", "startTime", "start_time", "cutoffTime", "starts", "kickoff"):
        dt = _parse_iso8601(event.get(key))
        if dt is not None:
            return dt
    return None


def _extract_market_block(markets: Any) -> dict[str, Any]:
    if isinstance(markets, dict):
        return markets
    return {}


def _extract_submarkets(market_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    submarkets = market_data.get("submarkets")
    if isinstance(submarkets, dict):
        return {k: v for k, v in submarkets.items() if isinstance(v, dict)}
    return {}


def _extract_handicap_from_submarket_key(submarket_key: str) -> float | None:
    parsed = parse_qs(submarket_key, keep_blank_values=False)
    value = parsed.get("handicap", [None])[0]
    return _safe_float(value)


def _extract_period_from_submarket_key(submarket_key: str) -> str | None:
    parsed = parse_qs(submarket_key, keep_blank_values=False)
    value = parsed.get("period", [None])[0]
    if isinstance(value, str):
        return value.lower()
    return None


def _extract_submarket_params(submarket_key: str, submarket: dict[str, Any]) -> tuple[float | None, str | None]:
    params = submarket.get("params")
    if isinstance(params, dict):
        handicap = _safe_float(params.get("handicap"))
        period = params.get("period")
        period_norm = period.lower() if isinstance(period, str) else None
        return handicap, period_norm
    if isinstance(params, str):
        handicap = _safe_float(parse_qs(params).get("handicap", [None])[0])
        period = parse_qs(params).get("period", [None])[0]
        return handicap, (period.lower() if isinstance(period, str) else None)
    return _extract_handicap_from_submarket_key(submarket_key), _extract_period_from_submarket_key(submarket_key)


def _extract_selections(submarket: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    selections = submarket.get("selections")
    if isinstance(selections, dict):
        home = selections.get("home")
        away = selections.get("away")
        home_payload = home if isinstance(home, dict) else None
        away_payload = away if isinstance(away, dict) else None
        return home_payload, away_payload
    if isinstance(selections, list):
        home_payload = None
        away_payload = None
        for item in selections:
            if not isinstance(item, dict):
                continue
            outcome = item.get("outcome")
            if outcome == "home":
                home_payload = item
            if outcome == "away":
                away_payload = item
        return home_payload, away_payload
    return None, None


def _selection_odds(selection: dict[str, Any] | None) -> float | None:
    if not isinstance(selection, dict):
        return None
    for key in ("odds", "price"):
        odds = _safe_float(selection.get(key))
        if odds is not None:
            return odds
    return None


def _extract_events_from_competition_payload(
    payload: dict[str, Any], fallback_league: str
) -> list[tuple[dict[str, Any], str]]:
    events_with_league: list[tuple[dict[str, Any], str]] = []

    direct_events = payload.get("events")
    if isinstance(direct_events, list):
        for event in direct_events:
            if isinstance(event, dict):
                events_with_league.append((event, fallback_league))
        return events_with_league

    competitions = payload.get("competitions")
    if not isinstance(competitions, list):
        return events_with_league

    for comp in competitions:
        if not isinstance(comp, dict):
            continue
        league_name = comp.get("name") if isinstance(comp.get("name"), str) else fallback_league
        comp_events = comp.get("events")
        if isinstance(comp_events, list):
            for event in comp_events:
                if isinstance(event, dict):
                    events_with_league.append((event, league_name))

    return events_with_league


def _main_ah_line_for_event(event: dict[str, Any]) -> MainAsianHandicapLine | None:
    markets = _extract_market_block(event.get("markets"))
    market_data = None
    for key in AH_MARKET_KEYS:
        value = markets.get(key)
        if isinstance(value, dict):
            market_data = value
            break
    if market_data is None:
        return None

    submarkets = _extract_submarkets(market_data)
    candidates: list[MainAsianHandicapLine] = []

    for submarket_key, submarket in submarkets.items():
        handicap, period = _extract_submarket_params(submarket_key, submarket)
        if handicap is None or handicap == 0:
            continue
        if period is not None and period not in ("ft", "full_time", "regular"):
            continue

        home_selection, away_selection = _extract_selections(submarket)
        home_odds = _selection_odds(home_selection)
        away_odds = _selection_odds(away_selection)

        if home_odds is None or away_odds is None:
            continue
        if home_odds <= 1.0 or away_odds <= 1.0:
            continue
        if not _selection_status_ok(home_selection.get("status") if home_selection else None):
            continue
        if not _selection_status_ok(away_selection.get("status") if away_selection else None):
            continue

        if handicap < 0:
            favorite_side = "home"
            favorite_line_abs = abs(handicap)
            fav_odds = home_odds
            dog_odds = away_odds
        else:
            favorite_side = "away"
            favorite_line_abs = abs(handicap)
            fav_odds = away_odds
            dog_odds = home_odds

        imbalance = abs((1.0 / home_odds) - (1.0 / away_odds))
        candidates.append(
            MainAsianHandicapLine(
                line_home=float(handicap),
                home_odds=home_odds,
                away_odds=away_odds,
                favorite_side=favorite_side,
                favorite_line_abs=favorite_line_abs,
                fav_odds=fav_odds,
                dog_odds=dog_odds,
                imbalance=imbalance,
            )
        )

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda line: (line.imbalance, abs(((line.home_odds + line.away_odds) / 2.0) - 2.0), abs(line.line_home)),
    )


@dataclass(slots=True)
class PreMatchScanner:
    client: CloudbetClient
    config: ScanConfig = field(default_factory=ScanConfig)

    def scan_once(self, now_utc: datetime | None = None) -> list[PreMatchWatchRecord]:
        scan_time = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
        records: list[PreMatchWatchRecord] = []

        competitions = self.client.get_soccer_competitions()
        for comp in competitions:
            competition_key = comp.get("key")
            if not isinstance(competition_key, str) or not competition_key:
                continue
            league = comp.get("name") if isinstance(comp.get("name"), str) else "Unknown League"

            payload = self.client.get_competition_odds(competition_key, self.config.markets)
            events = _extract_events_from_competition_payload(payload, league)

            for event, event_league in events:
                kickoff = _extract_kickoff(event)
                if kickoff is None:
                    continue

                minutes_to_kickoff = (kickoff - scan_time).total_seconds() / 60.0
                if minutes_to_kickoff < 0 or minutes_to_kickoff > self.config.minutes_to_kickoff_max:
                    continue

                if not _event_status_prematch(event.get("status")):
                    continue

                main_line = _main_ah_line_for_event(event)
                if main_line is None:
                    continue
                if main_line.favorite_line_abs < self.config.min_favorite_line_abs:
                    continue
                if main_line.fav_odds < self.config.min_favorite_odds:
                    continue

                bucket = classify_deep_ah_bucket(main_line.favorite_line_abs)
                if bucket is None:
                    continue

                home_team = _extract_team_name(event, "home", "Home")
                away_team = _extract_team_name(event, "away", "Away")
                favorite_team = home_team if main_line.favorite_side == "home" else away_team
                underdog_team = away_team if main_line.favorite_side == "home" else home_team

                records.append(
                    PreMatchWatchRecord(
                        match_id=_extract_event_id(event),
                        competition_key=competition_key,
                        home_team=home_team,
                        away_team=away_team,
                        league=event_league,
                        kickoff_time=kickoff,
                        ah_main_line=main_line.line_home,
                        favorite_side=main_line.favorite_side,
                        favorite_team=favorite_team,
                        underdog_team=underdog_team,
                        favorite_line_abs=main_line.favorite_line_abs,
                        fav_odds=main_line.fav_odds,
                        dog_odds=main_line.dog_odds,
                        pre_match_bucket=bucket,
                        scan_time=scan_time,
                        minutes_to_kickoff=round(minutes_to_kickoff, 3),
                    )
                )

        records.sort(key=lambda item: (item.kickoff_time, item.favorite_line_abs * -1, item.match_id))
        return records
