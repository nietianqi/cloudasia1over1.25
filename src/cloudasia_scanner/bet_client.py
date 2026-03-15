from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

BETTING_BASE_URL = "https://sports-api.cloudbet.com/pub/v2/betting"

# Cloudbet bet result values returned in settled bets
_WIN_RESULTS = {"WON", "WIN", "WINNER", "HALF_WON"}
_LOSS_RESULTS = {"LOST", "LOSE", "LOSER", "HALF_LOST"}
_SETTLED_STATUSES = {"SETTLED", "RESULTED", "CLOSED"}


def _status_matches(status: str, markers: set[str]) -> bool:
    if status in markers:
        return True
    return any(status.endswith(f"_{marker}") for marker in markers)


@dataclass(slots=True)
class BetConfig:
    enabled: bool = True
    dry_run: bool = True
    require_live_ack: bool = True
    live_ack_phrase: str = "LIVE_BETTING_ACK"
    live_ack_token: str = ""
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
    _active_count: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.api_key:
            self._session.headers.update({"X-API-Key": self.api_key})
        self._session.headers.update({"Content-Type": "application/json"})

    @property
    def active_bets_count(self) -> int:
        return self._active_count

    def on_bet_settled(self) -> None:
        self._active_count = max(0, self._active_count - 1)

    def check_bet_status(self, reference_id: str) -> dict[str, Any] | None:
        """Query Cloudbet for the current status of a placed bet.

        Returns the raw API response dict, or None on any error.
        Fields of interest: status, result/outcome, price (accepted odds).
        """
        url = f"{self.config.betting_base_url.rstrip('/')}/bets"
        try:
            resp = self._session.get(url, params={"referenceId": reference_id}, timeout=10.0)
            if resp.status_code in (401, 403, 404):
                return None
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return None
        if isinstance(data, list) and data:
            return data[0] if isinstance(data[0], dict) else None
        if isinstance(data, dict):
            # might be wrapped: {"bets": [...]}
            bets = data.get("bets")
            if isinstance(bets, list) and bets:
                return bets[0] if isinstance(bets[0], dict) else None
            return data
        return None

    def is_bet_settled(self, reference_id: str) -> tuple[bool, bool, float | None]:
        """Check if a bet is settled. Returns (settled, won, accepted_odds).

        settled=False means still open/pending (or API unavailable).
        """
        raw = self.check_bet_status(reference_id)
        if raw is None:
            return False, False, None
        status = str(raw.get("status", "")).upper()
        if not _status_matches(status, _SETTLED_STATUSES):
            return False, False, None
        result_raw = str(raw.get("result") or raw.get("outcome") or "").upper()
        won = _status_matches(result_raw, _WIN_RESULTS)
        raw_price = raw.get("price") or raw.get("acceptedPrice") or raw.get("accepted_price")
        try:
            accepted_odds = float(raw_price) if raw_price is not None else None
        except (TypeError, ValueError):
            accepted_odds = None
        return True, won, accepted_odds

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

    def place_bet(self, signal: Any, stake_override: float | None = None) -> BetRecord:
        """Place a bet based on a qualified LiveSignalRecord.

        stake_override — if provided, overrides config.stake_per_bet (used for
                         Kelly-based dynamic sizing from MoneyManager).

        Returns a BetRecord regardless of dry_run. In dry_run mode,
        the bet is logged but no HTTP request is made.
        """
        from .live_monitor import LiveSignalRecord  # avoid circular import

        assert isinstance(signal, LiveSignalRecord)

        now = datetime.now(timezone.utc)
        reference_id = str(uuid.uuid4())
        stake = stake_override if stake_override is not None else self.config.stake_per_bet
        market_key = str(getattr(signal, "bet_market_key", "soccer.total_goals"))
        selection_key = str(getattr(signal, "bet_selection_key", "over"))
        try:
            handicap_value = float(getattr(signal, "bet_handicap", signal.main_total_line))
        except (TypeError, ValueError):
            handicap_value = float(signal.main_total_line)
        try:
            requested_price = float(getattr(signal, "bet_price", signal.over_odds))
        except (TypeError, ValueError):
            requested_price = float(signal.over_odds)

        if not self.config.enabled:
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key=market_key,
                selection_key=selection_key,
                handicap=str(handicap_value),
                stake=stake,
                requested_price=requested_price,
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

        if (
            not self.config.dry_run
            and self.config.require_live_ack
            and self.config.live_ack_token != self.config.live_ack_phrase
        ):
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key=market_key,
                selection_key=selection_key,
                handicap=str(handicap_value),
                stake=stake,
                requested_price=requested_price,
                accepted_price=None,
                status="SKIPPED_ACK_REQUIRED",
                rejection_reason=(
                    "Set betting.live_ack_token to the exact value of "
                    "betting.live_ack_phrase to enable real-money bets."
                ),
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

        if requested_price < self.config.min_accepted_price:
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key=market_key,
                selection_key=selection_key,
                handicap=str(handicap_value),
                stake=stake,
                requested_price=requested_price,
                accepted_price=None,
                status="SKIPPED_PRICE_TOO_LOW",
                rejection_reason=f"price={requested_price} < min_accepted_price={self.config.min_accepted_price}",
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
            "marketKey": market_key,
            "selectionKey": selection_key,
            "handicap": str(handicap_value),
            "stake": str(round(stake, 2)),
            "price": str(round(requested_price, 4)),
            "currency": self.config.currency,
        }

        if self.config.dry_run:
            self._active_count += 1
            return BetRecord(
                match_id=signal.match_id,
                reference_id=reference_id,
                event_id=signal.match_id,
                market_key=market_key,
                selection_key=selection_key,
                handicap=str(handicap_value),
                stake=stake,
                requested_price=requested_price,
                accepted_price=requested_price,
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
                market_key=market_key,
                selection_key=selection_key,
                handicap=str(handicap_value),
                stake=stake,
                requested_price=requested_price,
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

        if status in ("ACCEPTED", "PENDING"):
            self._active_count += 1

        return BetRecord(
            match_id=signal.match_id,
            reference_id=reference_id,
            event_id=signal.match_id,
            market_key=market_key,
            selection_key=selection_key,
            handicap=str(handicap_value),
            stake=stake,
            requested_price=requested_price,
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
