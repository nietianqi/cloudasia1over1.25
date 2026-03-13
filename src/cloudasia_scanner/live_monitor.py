from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs
import json

from .cloudbet_client import CloudbetClient

TOTAL_MARKET_KEYS = ("soccer.total_goals", "soccer.totalGoals", "soccer.totals")
TRADING_MARKERS = ("TRADING", "OPEN", "ACTIVE", "ENABLED")
NON_TRADING_MARKERS = ("SUSPENDED", "CLOSED", "SETTLED", "CANCELLED", "DISABLED")
FINISHED_MARKERS = ("SETTLED", "CLOSED", "CANCELLED", "RESULTED")
ALLOWED_SCORE_SET = {(0, 0), (1, 0), (0, 1)}


@dataclass(slots=True)
class LiveMonitorConfig:
    trigger_total_line: float = 1.25
    primary_minute_start: int = 55
    primary_minute_end: int = 72
    allowed_scores: set[tuple[int, int]] = field(default_factory=lambda: set(ALLOWED_SCORE_SET))
    min_seconds_since_reopen: float = 20.0
    max_line_jumps_last_60s: int = 1
    max_odds_jumps_last_60s: int = 3
    min_over_odds: float = 1.8
    require_no_red_cards: bool = True
    jump_window_seconds: int = 60
    normal_poll_interval_seconds: int = 15
    fast_poll_interval_seconds: int = 5
    fast_poll_line_threshold: float = 1.75
    markets: list[str] = field(default_factory=lambda: list(TOTAL_MARKET_KEYS))


@dataclass(slots=True)
class WatchlistMatch:
    match_id: str
    competition_key: str
    home_team: str
    away_team: str
    favorite_side: str
    favorite_line_abs: float
    pre_match_bucket: str
    fav_odds_pre: float
    dog_odds_pre: float


@dataclass(slots=True)
class MainTotalMarket:
    main_total_line: float
    over_odds: float
    under_odds: float
    market_status: str
    max_stake: float | None
    source_market_key: str
    source_submarket_key: str
    seconds_since_reopen: float | None = None
    line_jump_count_last_60s: int = 0
    odds_jump_count_last_60s: int = 0
    line_last_change_ts: datetime | None = None
    odds_last_change_ts: datetime | None = None
    last_suspend_count: int = 0


@dataclass(slots=True)
class LiveGameState:
    minute: int | None
    score_home: int | None
    score_away: int | None
    red_home: int | None
    red_away: int | None


@dataclass(slots=True)
class LiveSignalRecord:
    signal: str
    signal_time: datetime
    match_id: str
    home_team: str
    away_team: str
    minute: int | None
    score_home: int | None
    score_away: int | None
    favorite_side: str
    favorite_handicap_abs: float
    pre_match_bucket: str
    main_total_line: float
    over_odds: float
    under_odds: float
    market_status: str
    seconds_since_reopen: float | None
    line_jump_count_last_60s: int
    odds_jump_count_last_60s: int
    signal_status: str
    reject_reason: str | None
    quality_score: float
    confidence: str
    action: str
    fav_odds_pre: float
    dog_odds_pre: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["signal_time"] = self.signal_time.isoformat()
        return payload


@dataclass(slots=True)
class MatchTrackingState:
    state: str = "WATCHING"
    last_market_status: str | None = None
    reopen_time: datetime | None = None
    last_total_line: float | None = None
    last_over_odds: float | None = None
    line_change_times: deque[datetime] = field(default_factory=deque)
    odds_change_times: deque[datetime] = field(default_factory=deque)
    line_last_change_ts: datetime | None = None
    odds_last_change_ts: datetime | None = None
    suspend_count: int = 0


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
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


def _is_trading_status(status: str | None) -> bool:
    if not status:
        return True
    upper = status.upper()
    if any(marker in upper for marker in NON_TRADING_MARKERS):
        return False
    return any(marker in upper for marker in TRADING_MARKERS)


def _extract_market_block(markets: Any) -> dict[str, Any]:
    if isinstance(markets, dict):
        return markets
    return {}


def _extract_submarkets(market_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    submarkets = market_data.get("submarkets")
    if isinstance(submarkets, dict):
        return {key: value for key, value in submarkets.items() if isinstance(value, dict)}
    return {}


def _extract_total_from_submarket_key(submarket_key: str) -> float | None:
    parsed = parse_qs(submarket_key, keep_blank_values=False)
    for field in ("total", "line", "handicap"):
        raw = parsed.get(field, [None])[0]
        line = _safe_float(raw)
        if line is not None:
            return line
    return None


def _extract_period_from_submarket_key(submarket_key: str) -> str | None:
    parsed = parse_qs(submarket_key, keep_blank_values=False)
    value = parsed.get("period", [None])[0]
    if isinstance(value, str):
        return value.lower()
    return None


def _extract_submarket_params(submarket_key: str, submarket: dict[str, Any]) -> tuple[float | None, str | None]:
    params = submarket.get("params")
    if isinstance(params, dict):
        total = None
        for field in ("total", "line", "handicap"):
            total = _safe_float(params.get(field))
            if total is not None:
                break
        period = params.get("period")
        period_norm = period.lower() if isinstance(period, str) else None
        return total, period_norm
    if isinstance(params, str):
        parsed = parse_qs(params, keep_blank_values=False)
        total = None
        for field in ("total", "line", "handicap"):
            total = _safe_float(parsed.get(field, [None])[0])
            if total is not None:
                break
        period = parsed.get("period", [None])[0]
        return total, (period.lower() if isinstance(period, str) else None)
    return _extract_total_from_submarket_key(submarket_key), _extract_period_from_submarket_key(submarket_key)


def _extract_over_under_selections(submarket: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    selections = submarket.get("selections")
    if isinstance(selections, dict):
        over = selections.get("over")
        under = selections.get("under")
        over_payload = over if isinstance(over, dict) else None
        under_payload = under if isinstance(under, dict) else None
        return over_payload, under_payload
    if isinstance(selections, list):
        over_payload = None
        under_payload = None
        for item in selections:
            if not isinstance(item, dict):
                continue
            outcome = item.get("outcome")
            if outcome == "over":
                over_payload = item
            if outcome == "under":
                under_payload = item
        return over_payload, under_payload
    return None, None


def _selection_odds(selection: dict[str, Any] | None) -> float | None:
    if not isinstance(selection, dict):
        return None
    for field in ("odds", "price"):
        odds = _safe_float(selection.get(field))
        if odds is not None:
            return odds
    return None


def _selection_param_float(selection: dict[str, Any], key: str) -> float | None:
    params = selection.get("params")
    if isinstance(params, dict):
        return _safe_float(params.get(key))
    if isinstance(params, str):
        parsed = parse_qs(params, keep_blank_values=False)
        return _safe_float(parsed.get(key, [None])[0])
    return None


def _selection_max_stake(selection: dict[str, Any] | None) -> float | None:
    if not isinstance(selection, dict):
        return None
    for field in ("maxStake", "max_stake", "maxBet"):
        value = _safe_float(selection.get(field))
        if value is not None:
            return value
    return None


def _first_str_value(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _market_status(submarket: dict[str, Any], over_selection: dict[str, Any] | None, under_selection: dict[str, Any] | None) -> str:
    status = _first_str_value(
        submarket.get("status"),
        over_selection.get("status") if isinstance(over_selection, dict) else None,
        under_selection.get("status") if isinstance(under_selection, dict) else None,
    )
    return status or "TRADING"


def _main_total_market_for_event(event: dict[str, Any]) -> MainTotalMarket | None:
    markets = _extract_market_block(event.get("markets"))
    candidates: list[tuple[float, float, MainTotalMarket]] = []

    for market_key in TOTAL_MARKET_KEYS:
        market_data = markets.get(market_key)
        if not isinstance(market_data, dict):
            continue

        submarkets = _extract_submarkets(market_data)
        for submarket_key, submarket in submarkets.items():
            total, period = _extract_submarket_params(submarket_key, submarket)
            if total is None or total < 0:
                # shape B: selections list carries many lines in selection params.
                selections = submarket.get("selections")
                if period is not None and period not in ("ft", "full_time", "regular"):
                    continue
                if not isinstance(selections, list):
                    continue
                grouped: dict[float, dict[str, dict[str, Any]]] = {}
                for item in selections:
                    if not isinstance(item, dict):
                        continue
                    outcome = item.get("outcome")
                    if outcome not in ("over", "under"):
                        continue
                    line = _selection_param_float(item, "total")
                    if line is None:
                        line = _selection_param_float(item, "line")
                    if line is None or line < 0:
                        continue
                    grouped.setdefault(float(line), {})[outcome] = item

                for line, sides in grouped.items():
                    over_selection = sides.get("over")
                    under_selection = sides.get("under")
                    if over_selection is None or under_selection is None:
                        continue
                    over_odds = _selection_odds(over_selection)
                    under_odds = _selection_odds(under_selection)
                    if over_odds is None or under_odds is None:
                        continue
                    if over_odds <= 1.0 or under_odds <= 1.0:
                        continue

                    imbalance = abs((1.0 / over_odds) - (1.0 / under_odds))
                    mean_to_even = abs(((over_odds + under_odds) / 2.0) - 2.0)
                    status = _market_status(submarket, over_selection, under_selection)
                    max_stake = _selection_max_stake(over_selection) or _selection_max_stake(under_selection)
                    candidates.append(
                        (
                            imbalance,
                            mean_to_even,
                            MainTotalMarket(
                                main_total_line=float(line),
                                over_odds=over_odds,
                                under_odds=under_odds,
                                market_status=status,
                                max_stake=max_stake,
                                source_market_key=market_key,
                                source_submarket_key=submarket_key,
                            ),
                        )
                    )
                continue

            if period is not None and period not in ("ft", "full_time", "regular"):
                continue

            over_selection, under_selection = _extract_over_under_selections(submarket)
            over_odds = _selection_odds(over_selection)
            under_odds = _selection_odds(under_selection)
            if over_odds is None or under_odds is None:
                continue
            if over_odds <= 1.0 or under_odds <= 1.0:
                continue

            imbalance = abs((1.0 / over_odds) - (1.0 / under_odds))
            mean_to_even = abs(((over_odds + under_odds) / 2.0) - 2.0)
            status = _market_status(submarket, over_selection, under_selection)
            max_stake = _selection_max_stake(over_selection) or _selection_max_stake(under_selection)
            candidates.append(
                (
                    imbalance,
                    mean_to_even,
                    MainTotalMarket(
                        main_total_line=float(total),
                        over_odds=over_odds,
                        under_odds=under_odds,
                        market_status=status,
                        max_stake=max_stake,
                        source_market_key=market_key,
                        source_submarket_key=submarket_key,
                    ),
                )
            )

    if not candidates:
        return None
    candidates.sort(key=lambda row: (row[0], row[1], abs(row[2].main_total_line - 2.0)))
    return candidates[0][2]


def _extract_minute(event: dict[str, Any]) -> int | None:
    direct = _safe_int(event.get("minute"))
    if direct is not None:
        return direct

    for parent_key in ("clock", "timer", "liveClock", "time"):
        parent = event.get(parent_key)
        if not isinstance(parent, dict):
            continue
        for field in ("minute", "minutes", "min"):
            minute = _safe_int(parent.get(field))
            if minute is not None:
                return minute

    for parent_key in ("scoreboard", "stats"):
        parent = event.get(parent_key)
        if not isinstance(parent, dict):
            continue
        timer = parent.get("clock") if isinstance(parent.get("clock"), dict) else parent.get("timer")
        if isinstance(timer, dict):
            for field in ("minute", "minutes", "min"):
                minute = _safe_int(timer.get(field))
                if minute is not None:
                    return minute

    return None


def _extract_score(event: dict[str, Any]) -> tuple[int | None, int | None]:
    home = _safe_int(event.get("homeScore"))
    away = _safe_int(event.get("awayScore"))
    if home is not None and away is not None:
        return home, away

    for key in ("score", "scores", "result", "scoreboard"):
        payload = event.get(key)
        if not isinstance(payload, dict):
            continue
        home = _safe_int(payload.get("home"))
        away = _safe_int(payload.get("away"))
        if home is None:
            home = _safe_int(payload.get("home_score"))
        if away is None:
            away = _safe_int(payload.get("away_score"))
        if home is not None and away is not None:
            return home, away

        current = payload.get("current")
        if isinstance(current, dict):
            home = _safe_int(current.get("home"))
            away = _safe_int(current.get("away"))
            if home is not None and away is not None:
                return home, away

    return None, None


def _extract_red_cards(event: dict[str, Any]) -> tuple[int | None, int | None]:
    cards = event.get("redCards")
    if isinstance(cards, dict):
        home = _safe_int(cards.get("home"))
        away = _safe_int(cards.get("away"))
        if home is not None and away is not None:
            return home, away

    cards = event.get("cards")
    if isinstance(cards, dict):
        home_payload = cards.get("home")
        away_payload = cards.get("away")
        home = _safe_int(home_payload.get("red") if isinstance(home_payload, dict) else None)
        away = _safe_int(away_payload.get("red") if isinstance(away_payload, dict) else None)
        if home is not None and away is not None:
            return home, away

    stats = event.get("stats")
    if isinstance(stats, dict):
        for key in ("redCards", "red_cards"):
            payload = stats.get(key)
            if not isinstance(payload, dict):
                continue
            home = _safe_int(payload.get("home"))
            away = _safe_int(payload.get("away"))
            if home is not None and away is not None:
                return home, away

    return None, None


def _extract_live_game_state(event: dict[str, Any]) -> LiveGameState:
    minute = _extract_minute(event)
    score_home, score_away = _extract_score(event)
    red_home, red_away = _extract_red_cards(event)
    return LiveGameState(
        minute=minute,
        score_home=score_home,
        score_away=score_away,
        red_home=red_home,
        red_away=red_away,
    )


def _event_match_id(event: dict[str, Any]) -> str:
    for key in ("id", "key", "eventId", "event_id"):
        value = event.get(key)
        if isinstance(value, (str, int)):
            return str(value)
    return "unknown-match-id"


def _prune_changes(queue: deque[datetime], now_utc: datetime, window_seconds: int) -> None:
    while queue and (now_utc - queue[0]).total_seconds() > window_seconds:
        queue.popleft()


def _load_json_lines(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("["):
        payload = json.loads(text)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    rows: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            rows.append(item)
    return rows


def load_watchlist(path: Path) -> dict[str, WatchlistMatch]:
    rows = _load_json_lines(path)
    best_by_match: dict[str, tuple[datetime | None, WatchlistMatch]] = {}

    for row in rows:
        if row.get("watchlist_flag") is False:
            continue
        match_id = row.get("match_id")
        if match_id is None:
            continue
        match_id_str = str(match_id).strip()
        if not match_id_str:
            continue

        favorite_side = row.get("favorite_side")
        if favorite_side not in ("home", "away"):
            continue

        favorite_line_abs = _safe_float(row.get("favorite_line_abs"))
        fav_odds_pre = _safe_float(row.get("fav_odds"))
        dog_odds_pre = _safe_float(row.get("dog_odds"))
        if favorite_line_abs is None or fav_odds_pre is None or dog_odds_pre is None:
            continue

        pre_match_bucket = row.get("pre_match_bucket")
        if not isinstance(pre_match_bucket, str):
            continue

        watch_row = WatchlistMatch(
            match_id=match_id_str,
            competition_key=str(row.get("competition_key") or ""),
            home_team=str(row.get("home_team") or "Home"),
            away_team=str(row.get("away_team") or "Away"),
            favorite_side=favorite_side,
            favorite_line_abs=favorite_line_abs,
            pre_match_bucket=pre_match_bucket,
            fav_odds_pre=fav_odds_pre,
            dog_odds_pre=dog_odds_pre,
        )
        scan_time = _parse_iso8601(row.get("scan_time"))

        current = best_by_match.get(match_id_str)
        if current is None:
            best_by_match[match_id_str] = (scan_time, watch_row)
            continue

        current_scan, _ = current
        if current_scan is None and scan_time is not None:
            best_by_match[match_id_str] = (scan_time, watch_row)
            continue
        if current_scan is not None and scan_time is not None and scan_time > current_scan:
            best_by_match[match_id_str] = (scan_time, watch_row)

    return {match_id: row for match_id, (_, row) in best_by_match.items()}


def _quality_score(watch: WatchlistMatch, state: LiveGameState, market: MainTotalMarket) -> float:
    score = 50.0

    if watch.favorite_line_abs >= 2.25:
        score += 15
    elif watch.favorite_line_abs >= 2.0:
        score += 12
    elif watch.favorite_line_abs >= 1.5:
        score += 8
    elif watch.favorite_line_abs >= 1.25:
        score += 5

    minute = state.minute
    if minute is not None:
        if 58 <= minute <= 68:
            score += 8
        elif 55 <= minute <= 72:
            score += 4
        elif 45 <= minute <= 78:
            score += 1

    if state.score_home == 0 and state.score_away == 0:
        score += 6
    elif (state.score_home, state.score_away) in ALLOWED_SCORE_SET:
        score += 3

    if 1.8 <= market.over_odds <= 2.05:
        score += 6
    elif 2.05 < market.over_odds <= 2.25:
        score += 4
    elif market.over_odds > 2.25:
        score += 2

    if market.seconds_since_reopen is not None:
        if market.seconds_since_reopen >= 60:
            score += 5
        elif market.seconds_since_reopen >= 30:
            score += 3

    score -= market.line_jump_count_last_60s * 3
    score -= market.odds_jump_count_last_60s

    return max(0.0, min(100.0, round(score, 2)))


def _confidence_from_quality(quality_score: float) -> str:
    if quality_score >= 80:
        return "high"
    if quality_score >= 65:
        return "medium"
    return "base"


@dataclass(slots=True)
class LiveLayerTwoMonitor:
    client: CloudbetClient
    watchlist: dict[str, WatchlistMatch]
    config: LiveMonitorConfig = field(default_factory=LiveMonitorConfig)
    states: dict[str, MatchTrackingState] = field(default_factory=dict)

    def monitor_once(self, now_utc: datetime | None = None) -> list[LiveSignalRecord]:
        now = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
        records: list[LiveSignalRecord] = []

        for match_id, watch in self.watchlist.items():
            tracking = self.states.setdefault(match_id, MatchTrackingState())
            if tracking.state == "FINISHED":
                continue

            try:
                event = self.client.get_event_odds(match_id, self.config.markets)
            except Exception:
                tracking.state = "WATCHING"
                continue

            market = _main_total_market_for_event(event)
            if market is None:
                tracking.state = "WATCHING"
                continue

            self._update_tracking(tracking, market, now)

            if any(m in market.market_status.upper() for m in FINISHED_MARKERS):
                tracking.state = "FINISHED"
                continue

            if round(market.main_total_line, 2) != round(self.config.trigger_total_line, 2):
                tracking.state = "WATCHING"
                continue

            game_state = _extract_live_game_state(event)
            first_touch = tracking.state == "WATCHING"
            if first_touch:
                tracking.state = "TRIGGERED"
                records.append(self._build_signal_record(watch, game_state, market, now, "triggered", None))
                continue

            signal_status, reject_reason = self._evaluate_filters(watch, game_state, market)
            tracking.state = signal_status.upper()
            records.append(self._build_signal_record(watch, game_state, market, now, signal_status, reject_reason))

        records.sort(key=lambda row: (row.signal_time, row.match_id))
        return records

    def recommended_poll_interval_seconds(self) -> int:
        for tracking in self.states.values():
            if tracking.last_total_line is not None and tracking.last_total_line <= self.config.fast_poll_line_threshold:
                return self.config.fast_poll_interval_seconds
        return self.config.normal_poll_interval_seconds

    def _update_tracking(self, tracking: MatchTrackingState, market: MainTotalMarket, now: datetime) -> None:
        current_status = market.market_status
        current_is_trading = _is_trading_status(current_status)
        previous_status = tracking.last_market_status
        previous_is_trading = _is_trading_status(previous_status)

        if previous_status is None:
            if current_is_trading:
                tracking.reopen_time = now
        elif current_status != previous_status:
            if previous_is_trading and not current_is_trading:
                tracking.suspend_count += 1
            if not previous_is_trading and current_is_trading:
                tracking.reopen_time = now

        if tracking.last_total_line is not None and market.main_total_line != tracking.last_total_line:
            tracking.line_change_times.append(now)
            tracking.line_last_change_ts = now
        if tracking.last_over_odds is not None and market.over_odds != tracking.last_over_odds:
            tracking.odds_change_times.append(now)
            tracking.odds_last_change_ts = now

        _prune_changes(tracking.line_change_times, now, self.config.jump_window_seconds)
        _prune_changes(tracking.odds_change_times, now, self.config.jump_window_seconds)

        if tracking.reopen_time is not None and current_is_trading:
            market.seconds_since_reopen = round((now - tracking.reopen_time).total_seconds(), 3)
        else:
            market.seconds_since_reopen = None
        market.line_jump_count_last_60s = len(tracking.line_change_times)
        market.odds_jump_count_last_60s = len(tracking.odds_change_times)
        market.line_last_change_ts = tracking.line_last_change_ts
        market.odds_last_change_ts = tracking.odds_last_change_ts
        market.last_suspend_count = tracking.suspend_count

        tracking.last_market_status = current_status
        tracking.last_total_line = market.main_total_line
        tracking.last_over_odds = market.over_odds

    def _evaluate_filters(
        self, watch: WatchlistMatch, game_state: LiveGameState, market: MainTotalMarket
    ) -> tuple[str, str | None]:
        reject_reasons: list[str] = []

        if not _is_trading_status(market.market_status):
            reject_reasons.append("market_not_trading")

        if market.seconds_since_reopen is not None and market.seconds_since_reopen < self.config.min_seconds_since_reopen:
            return "cooling", "recent_reopen"

        minute = game_state.minute
        if minute is None:
            reject_reasons.append("missing_minute")
        elif minute < self.config.primary_minute_start or minute > self.config.primary_minute_end:
            reject_reasons.append("minute_out_of_window")

        if game_state.score_home is None or game_state.score_away is None:
            reject_reasons.append("missing_score")
        elif (game_state.score_home, game_state.score_away) not in self.config.allowed_scores:
            reject_reasons.append("score_not_allowed")

        if self.config.require_no_red_cards:
            if game_state.red_home is None or game_state.red_away is None:
                reject_reasons.append("missing_red_cards")
            elif game_state.red_home > 0 or game_state.red_away > 0:
                reject_reasons.append("red_card_present")

        if market.line_jump_count_last_60s > self.config.max_line_jumps_last_60s:
            reject_reasons.append("line_too_volatile")
        if market.odds_jump_count_last_60s > self.config.max_odds_jumps_last_60s:
            reject_reasons.append("odds_too_volatile")
        if market.over_odds < self.config.min_over_odds:
            reject_reasons.append("over_odds_too_low")

        if reject_reasons:
            return "rejected", ";".join(reject_reasons)
        return "qualified", None

    def _build_signal_record(
        self,
        watch: WatchlistMatch,
        game_state: LiveGameState,
        market: MainTotalMarket,
        now: datetime,
        signal_status: str,
        reject_reason: str | None,
    ) -> LiveSignalRecord:
        quality_score = _quality_score(watch, game_state, market)
        confidence = _confidence_from_quality(quality_score)
        signal = "TG125_LATE_FAVORITE_SIGNAL" if signal_status == "qualified" else "TG125_LATE_FAVORITE_WATCH"

        if signal_status == "qualified":
            action = "candidate_only"
        elif signal_status in ("triggered", "cooling"):
            action = "monitoring"
        else:
            action = "rejected"

        return LiveSignalRecord(
            signal=signal,
            signal_time=now,
            match_id=watch.match_id,
            home_team=watch.home_team,
            away_team=watch.away_team,
            minute=game_state.minute,
            score_home=game_state.score_home,
            score_away=game_state.score_away,
            favorite_side=watch.favorite_side,
            favorite_handicap_abs=watch.favorite_line_abs,
            pre_match_bucket=watch.pre_match_bucket,
            main_total_line=market.main_total_line,
            over_odds=market.over_odds,
            under_odds=market.under_odds,
            market_status=market.market_status,
            seconds_since_reopen=market.seconds_since_reopen,
            line_jump_count_last_60s=market.line_jump_count_last_60s,
            odds_jump_count_last_60s=market.odds_jump_count_last_60s,
            signal_status=signal_status,
            reject_reason=reject_reason,
            quality_score=quality_score,
            confidence=confidence,
            action=action,
            fav_odds_pre=watch.fav_odds_pre,
            dog_odds_pre=watch.dog_odds_pre,
        )
