from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

BETTING_BASE_URL = "https://sports-api.cloudbet.com/pub/v2/betting"


@dataclass(slots=True)
class BetConfig:
    enabled: bool = True
    dry_run: bool = True
    stake_per_bet: float = 10.0
    currency: str = "USDT"
    min_accepted_price: float = 1.78
    max_active_bets: int = 5
    betting_base_url: str = BETTING_BASE_URL


@dataclass(slots=True)
class BetRecord:
    match_id: str
    reference_id: str
    event_id: str
    market_key: str
    selection_key: str
    handicap: str
    stake: float
    requested_price: float
    accepted_price: float | None
    status: str
    rejection_reason: str | None
    bet_time: datetime
    signal_quality: float
    home_team: str
    away_team: str
    favorite_side: str
    minute: int | None
    score_home: int | None
    score_away: int | None
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["bet_time"] = self.bet_time.isoformat()
        return payload


@dataclass
class BetClient:
    api_key: str | None
    config: BetConfig = field(default_factory=BetConfig)
    _session: requests.Session = field(default_factory=requests.Session, repr=False)

    def __post_init__(self) -> None:
        if self.api_key:
            self._session.headers.update({"X-API-Key": self.api_key})
        self._session.headers.update({"Content-Type": "application/json"})

    @property
    def active_bets_count(self) -> int:
        return self._active_count

    def _post_bet(self, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.config.betting_base_url.rstrip('/')}/place-bet"
        response = self._session.post(url, json=body, timeout=10.0)
        if response.status_code == 401:
            raise PermissionError("Cloudbet betting API unauthorized (401).")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected response from betting API: {data!r}")
        return data

    def place_bet(self, signal: Any) -> BetRecord:
        """Place a bet based on a qualified LiveSignalRecord.

        Returns a BetRecord regardless of dry_run. In dry_run mode,
        the bet is logged but no HTTP request is made.
        """
        from .live_monitor import LiveSignalRecord  # avoid circular import

        assert isinstance(signal, LiveSignalRecord)

        now = datetime.now(timezone.utc)
        reference_id = str(uuid.uuid4())

        if not self.config.enabled:
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key="soccer.total_goals",
                selection_key="over",
                handicap=str(signal.main_total_line),
                stake=self.config.stake_per_bet,
                requested_price=signal.over_odds,
                accepted_price=None,
                status="SKIPPED_DISABLED",
                rejection_reason="betting.enabled=false",
                bet_time=now,
                signal_quality=signal.quality_score,
                home_team=signal.home_team,
                away_team=signal.away_team,
                favorite_side=signal.favorite_side,
                minute=signal.minute,
                score_home=signal.score_home,
                score_away=signal.score_away,
                dry_run=self.config.dry_run,
            )

        if signal.over_odds < self.config.min_accepted_price:
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key="soccer.total_goals",
                selection_key="over",
                handicap=str(signal.main_total_line),
                stake=self.config.stake_per_bet,
                requested_price=signal.over_odds,
                accepted_price=None,
                status="SKIPPED_PRICE_TOO_LOW",
                rejection_reason=f"over_odds={signal.over_odds} < min_accepted_price={self.config.min_accepted_price}",
                bet_time=now,
                signal_quality=signal.quality_score,
                home_team=signal.home_team,
                away_team=signal.away_team,
                favorite_side=signal.favorite_side,
                minute=signal.minute,
                score_home=signal.score_home,
                score_away=signal.score_away,
                dry_run=self.config.dry_run,
            )

        body = {
            "referenceId": reference_id,
            "eventId": signal.match_id,
            "marketKey": "soccer.total_goals",
            "selectionKey": "over",
            "handicap": str(signal.main_total_line),
            "stake": str(round(self.config.stake_per_bet, 2)),
            "price": str(round(signal.over_odds, 4)),
            "currency": self.config.currency,
        }

        if self.config.dry_run:
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key="soccer.total_goals",
                selection_key="over",
                handicap=str(signal.main_total_line),
                stake=self.config.stake_per_bet,
                requested_price=signal.over_odds,
                accepted_price=signal.over_odds,
                status="DRY_RUN",
                rejection_reason=None,
                bet_time=now,
                signal_quality=signal.quality_score,
                home_team=signal.home_team,
                away_team=signal.away_team,
                favorite_side=signal.favorite_side,
                minute=signal.minute,
                score_home=signal.score_home,
                score_away=signal.score_away,
                dry_run=True,
            )

        try:
            resp = self._post_bet(body)
        except Exception as exc:
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key="soccer.total_goals",
                selection_key="over",
                handicap=str(signal.main_total_line),
                stake=self.config.stake_per_bet,
                requested_price=signal.over_odds,
                accepted_price=None,
                status="ERROR",
                rejection_reason=str(exc),
                bet_time=now,
                signal_quality=signal.quality_score,
                home_team=signal.home_team,
                away_team=signal.away_team,
                favorite_side=signal.favorite_side,
                minute=signal.minute,
                score_home=signal.score_home,
                score_away=signal.score_away,
                dry_run=False,
            )

        status = str(resp.get("status", "UNKNOWN")).upper()
        rejection_reason = resp.get("rejectionReason") or resp.get("rejection_reason")
        raw_price = resp.get("price")
        try:
            accepted_price = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            accepted_price = None

        return BetRecord(
            match_id=signal.match_id,
            reference_id=reference_id,
            event_id=signal.match_id,
            market_key="soccer.total_goals",
            selection_key="over",
            handicap=str(signal.main_total_line),
            stake=self.config.stake_per_bet,
            requested_price=signal.over_odds,
            accepted_price=accepted_price,
            status=status,
            rejection_reason=str(rejection_reason) if rejection_reason else None,
            bet_time=now,
            signal_quality=signal.quality_score,
            home_team=signal.home_team,
            away_team=signal.away_team,
            favorite_side=signal.favorite_side,
            minute=signal.minute,
            score_home=signal.score_home,
            score_away=signal.score_away,
            dry_run=False,
        )
