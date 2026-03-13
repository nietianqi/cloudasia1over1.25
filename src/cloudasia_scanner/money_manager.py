"""Bankroll and risk controls for live betting."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(slots=True)
class MoneyConfig:
    initial_bankroll: float = 500.0

    # Kelly sizing parameters
    kelly_fraction: float = 0.25
    base_win_rate: float = 0.55
    min_win_rate: float = 0.50
    max_win_rate: float = 0.70
    quality_adjustment: float = 0.07
    min_kelly_edge: float = 0.0

    # Stake and capital controls
    reserve_bankroll_pct: float = 0.0
    max_stake_pct: float = 0.05
    min_stake: float = 5.0
    max_stake: float = 50.0
    force_min_stake: bool = False

    # Session / risk controls
    daily_loss_limit_pct: float = 0.10
    max_drawdown_pct: float = 0.20
    max_concurrent_exposure_pct: float = 0.25
    max_consecutive_losses: int = 5
    max_daily_bets: int = 20
    bet_cooldown_seconds: int = 15

    bankroll_file: str = "data/bankroll.json"
    auto_settle_after_minutes: int = 130


@dataclass
class MoneyManager:
    config: MoneyConfig = field(default_factory=MoneyConfig)

    _bankroll: float = field(init=False)
    _peak_bankroll: float = field(init=False)
    _open_exposure: float = field(default=0.0, init=False)
    _daily_pnl: float = field(default=0.0, init=False)
    _day_start_bankroll: float = field(init=False)
    _today: date = field(default_factory=date.today, init=False)
    _daily_bet_count: int = field(default=0, init=False)
    _consecutive_losses: int = field(default=0, init=False)
    _last_bet_time: datetime | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._bankroll = self.config.initial_bankroll
        self._peak_bankroll = self._bankroll
        self._day_start_bankroll = self._bankroll
        self._load()

    def compute_stake(self, over_odds: float, quality_score: float) -> float:
        """Return stake in quote currency, or 0 if no positive edge."""
        b = over_odds - 1.0
        if b <= 0:
            return 0.0

        p = self._quality_to_win_prob(quality_score)
        edge = (b * p) - (1.0 - p)
        f_star = edge / b
        if f_star <= self.config.min_kelly_edge:
            return 0.0

        available = self._bankroll * max(0.0, 1.0 - self.config.reserve_bankroll_pct)
        if available <= 0:
            return 0.0

        raw_stake = f_star * self.config.kelly_fraction * available
        cap_stake = min(self.config.max_stake_pct * self._bankroll, self.config.max_stake)
        stake = min(raw_stake, cap_stake)
        if stake <= 0:
            return 0.0

        if stake < self.config.min_stake:
            if not self.config.force_min_stake:
                return 0.0
            stake = self.config.min_stake

        return round(stake, 2)

    def can_bet(self, stake: float, now_utc: datetime | None = None) -> tuple[bool, str]:
        """Return permission and reason for blocking if denied."""
        self._maybe_reset_day(now_utc)

        if stake <= 0:
            return False, "no_edge"
        if stake > self._bankroll:
            return False, "insufficient_bankroll"

        daily_loss_limit = self.config.daily_loss_limit_pct * self._day_start_bankroll
        if self._daily_pnl <= -daily_loss_limit:
            return False, (
                f"daily_loss_limit (pnl={self._daily_pnl:+.2f} "
                f"/ limit=-{daily_loss_limit:.2f})"
            )

        if self._peak_bankroll > 0:
            drawdown = (self._peak_bankroll - self._bankroll) / self._peak_bankroll
            if drawdown >= self.config.max_drawdown_pct:
                return False, (
                    f"max_drawdown (drawdown={drawdown:.2%} "
                    f"/ limit={self.config.max_drawdown_pct:.2%})"
                )

        if self._consecutive_losses >= self.config.max_consecutive_losses:
            return False, (
                f"max_consecutive_losses ({self._consecutive_losses} "
                f">= {self.config.max_consecutive_losses})"
            )

        if self._daily_bet_count >= self.config.max_daily_bets:
            return False, (
                f"max_daily_bets ({self._daily_bet_count} "
                f">= {self.config.max_daily_bets})"
            )

        if self.config.bet_cooldown_seconds > 0 and self._last_bet_time is not None:
            now = now_utc.astimezone(timezone.utc) if now_utc is not None else _utc_now()
            elapsed = (now - self._last_bet_time).total_seconds()
            if elapsed < self.config.bet_cooldown_seconds:
                return False, (
                    f"bet_cooldown ({elapsed:.1f}s < {self.config.bet_cooldown_seconds}s)"
                )

        max_exposure = self.config.max_concurrent_exposure_pct * self._bankroll
        if self._open_exposure + stake > max_exposure:
            return False, (
                f"max_exposure (open={self._open_exposure:.2f} + new={stake:.2f} "
                f"> limit={max_exposure:.2f})"
            )

        return True, ""

    def on_bet_placed(self, stake: float, now_utc: datetime | None = None) -> None:
        self._maybe_reset_day(now_utc)
        now = now_utc.astimezone(timezone.utc) if now_utc is not None else _utc_now()

        self._open_exposure = round(self._open_exposure + stake, 2)
        self._bankroll = round(self._bankroll - stake, 2)
        self._daily_pnl = round(self._daily_pnl - stake, 2)
        self._daily_bet_count += 1
        self._last_bet_time = now
        self.save()

    def on_bet_settled(
        self,
        stake: float,
        won: bool,
        accepted_odds: float | None = None,
    ) -> None:
        self._open_exposure = round(max(0.0, self._open_exposure - stake), 2)

        if won:
            odds = accepted_odds if (accepted_odds and accepted_odds > 1.0) else 2.0
            payout = round(stake * odds, 2)
            self._bankroll = round(self._bankroll + payout, 2)
            self._daily_pnl = round(self._daily_pnl + payout, 2)
            self._consecutive_losses = 0
        else:
            self._consecutive_losses += 1

        self._peak_bankroll = max(self._peak_bankroll, self._bankroll)
        self.save()

    @property
    def bankroll(self) -> float:
        return self._bankroll

    @property
    def peak_bankroll(self) -> float:
        return self._peak_bankroll

    @property
    def open_exposure(self) -> float:
        return self._open_exposure

    @property
    def daily_pnl(self) -> float:
        return self._daily_pnl

    @property
    def daily_bet_count(self) -> int:
        return self._daily_bet_count

    @property
    def consecutive_losses(self) -> int:
        return self._consecutive_losses

    def summary_line(self) -> str:
        return (
            f"bankroll={self._bankroll:.2f} peak={self._peak_bankroll:.2f} "
            f"exposure={self._open_exposure:.2f} daily_pnl={self._daily_pnl:+.2f} "
            f"bets_today={self._daily_bet_count} loss_streak={self._consecutive_losses}"
        )

    def save(self) -> None:
        path = Path(self.config.bankroll_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "bankroll": self._bankroll,
            "peak_bankroll": self._peak_bankroll,
            "open_exposure": self._open_exposure,
            "daily_pnl": self._daily_pnl,
            "today": self._today.isoformat(),
            "day_start_bankroll": self._day_start_bankroll,
            "daily_bet_count": self._daily_bet_count,
            "consecutive_losses": self._consecutive_losses,
            "last_bet_time": self._last_bet_time.isoformat() if self._last_bet_time else None,
            "saved_at": _utc_now().isoformat(),
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _load(self) -> None:
        path = Path(self.config.bankroll_file)
        if not path.exists():
            return
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return

        self._bankroll = float(state.get("bankroll", self._bankroll))
        self._peak_bankroll = float(state.get("peak_bankroll", self._bankroll))
        self._open_exposure = float(state.get("open_exposure", 0.0))

        today = date.today()
        saved_today = state.get("today")
        if saved_today == today.isoformat():
            self._daily_pnl = float(state.get("daily_pnl", 0.0))
            self._day_start_bankroll = float(state.get("day_start_bankroll", self._bankroll))
            self._daily_bet_count = int(state.get("daily_bet_count", 0))
            self._consecutive_losses = int(state.get("consecutive_losses", 0))
            self._last_bet_time = _parse_utc_datetime(state.get("last_bet_time"))
        else:
            self._daily_pnl = 0.0
            self._day_start_bankroll = self._bankroll
            self._daily_bet_count = 0
            self._consecutive_losses = 0
            self._last_bet_time = None

        self._today = today
        self._peak_bankroll = max(self._peak_bankroll, self._bankroll)

    def _maybe_reset_day(self, now_utc: datetime | None = None) -> None:
        now = now_utc.astimezone(timezone.utc) if now_utc is not None else _utc_now()
        today = now.date()
        if today == self._today:
            return

        self._today = today
        self._daily_pnl = 0.0
        self._day_start_bankroll = self._bankroll
        self._daily_bet_count = 0
        self._consecutive_losses = 0
        self._last_bet_time = None
        self.save()

    def _quality_to_win_prob(self, quality_score: float) -> float:
        adjustment = (quality_score - 50.0) / 50.0 * self.config.quality_adjustment
        estimate = self.config.base_win_rate + adjustment
        return round(
            max(self.config.min_win_rate, min(self.config.max_win_rate, estimate)),
            4,
        )
