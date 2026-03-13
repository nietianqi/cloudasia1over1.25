"""Kelly-based bankroll management for the TG1.25 live betting pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass(slots=True)
class MoneyConfig:
    """Full bankroll management configuration."""

    # ── Starting bankroll ────────────────────────────────────────────────────
    # If data/bankroll.json exists from a previous session, that balance is used
    # instead of initial_bankroll.
    initial_bankroll: float = 500.0

    # ── Kelly Criterion parameters ───────────────────────────────────────────
    # Fractional Kelly multiplier.  0.25 = quarter-Kelly (strongly recommended).
    # Full Kelly (1.0) maximises log-growth but produces extreme variance.
    kelly_fraction: float = 0.25

    # Conservative base win-rate estimate for the TG1.25 late-favourite strategy.
    # This is deliberately kept below true edge to add a margin of safety.
    base_win_rate: float = 0.55

    # ── Per-bet caps ─────────────────────────────────────────────────────────
    # Never commit more than this fraction of current bankroll on one bet.
    max_stake_pct: float = 0.05         # 5 %
    min_stake: float = 5.0              # floor in USDT
    max_stake: float = 50.0             # hard ceiling in USDT

    # ── Session / daily guards ───────────────────────────────────────────────
    # Halt betting for the rest of the day when daily P&L falls below this
    # fraction of the day's opening bankroll.
    daily_loss_limit_pct: float = 0.10  # -10 %

    # Total money currently in open (unsettled) bets as % of bankroll.
    max_concurrent_exposure_pct: float = 0.25  # 25 %

    # ── Persistence ──────────────────────────────────────────────────────────
    bankroll_file: str = "data/bankroll.json"

    # ── Automatic settlement ─────────────────────────────────────────────────
    # Assume a bet is settled after this many minutes (match should be over).
    auto_settle_after_minutes: int = 130


@dataclass
class MoneyManager:
    """
    Kelly-based bankroll manager.

    Win probability is estimated from the signal's quality_score (0-100):
        p = base_win_rate ± 0.07  (linearly mapped from score relative to 50)

    Kelly fraction:
        f* = (b·p − q) / b     where b = odds − 1,  q = 1 − p
        stake = kelly_fraction · f* · bankroll    (capped at max_stake / max_stake_pct)
    """

    config: MoneyConfig = field(default_factory=MoneyConfig)

    # Private state — do NOT set directly; use on_bet_placed / on_bet_settled
    _bankroll: float = field(init=False)
    _open_exposure: float = field(default=0.0, init=False)
    _daily_pnl: float = field(default=0.0, init=False)
    _day_start_bankroll: float = field(init=False)
    _today: date = field(default_factory=date.today, init=False)

    def __post_init__(self) -> None:
        self._bankroll = self.config.initial_bankroll
        self._day_start_bankroll = self._bankroll
        self._load()

    # ── Stake sizing ─────────────────────────────────────────────────────────

    def compute_stake(self, over_odds: float, quality_score: float) -> float:
        """Return the optimal Kelly-sized stake (USDT), floored/capped."""
        p = self._quality_to_win_prob(quality_score)
        b = over_odds - 1.0
        if b <= 0:
            return self.config.min_stake

        f_star = max(0.0, (b * p - (1.0 - p)) / b)
        raw = f_star * self.config.kelly_fraction * self._bankroll
        capped = min(raw, self.config.max_stake_pct * self._bankroll, self.config.max_stake)
        return round(max(self.config.min_stake, capped), 2)

    # ── Guards ───────────────────────────────────────────────────────────────

    def can_bet(self, stake: float) -> tuple[bool, str]:
        """Return (True, '') if the bet is allowed; (False, reason) otherwise."""
        self._maybe_reset_day()

        limit = self.config.daily_loss_limit_pct * self._day_start_bankroll
        if self._daily_pnl <= -limit:
            return False, (
                f"daily_loss_limit (pnl={self._daily_pnl:+.2f} USDT"
                f" / limit=-{limit:.2f} USDT)"
            )

        max_exp = self.config.max_concurrent_exposure_pct * self._bankroll
        if self._open_exposure + stake > max_exp:
            return False, (
                f"max_exposure (open={self._open_exposure:.2f}"
                f" + new={stake:.2f} > limit={max_exp:.2f} USDT)"
            )

        return True, ""

    # ── Lifecycle callbacks ──────────────────────────────────────────────────

    def on_bet_placed(self, stake: float) -> None:
        """Call immediately after a real (non-dry-run) bet is placed."""
        self._open_exposure = round(self._open_exposure + stake, 2)
        self._bankroll = round(self._bankroll - stake, 2)
        self._daily_pnl = round(self._daily_pnl - stake, 2)
        self.save()

    def on_bet_settled(
        self,
        stake: float,
        won: bool,
        accepted_odds: float | None = None,
    ) -> None:
        """
        Call when a bet outcome is confirmed.

        stake         — the original stake (same value passed to on_bet_placed)
        won           — True = bet won, False = bet lost
        accepted_odds — accepted decimal odds (used to compute payout)
        """
        self._open_exposure = round(max(0.0, self._open_exposure - stake), 2)
        if won:
            odds = accepted_odds if (accepted_odds and accepted_odds > 1.0) else 2.0
            payout = round(stake * odds, 2)        # full return (stake + profit)
            self._bankroll = round(self._bankroll + payout, 2)
            self._daily_pnl = round(self._daily_pnl + payout, 2)
        # On loss: stake was already deducted from bankroll in on_bet_placed.
        self.save()

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def bankroll(self) -> float:
        return self._bankroll

    @property
    def open_exposure(self) -> float:
        return self._open_exposure

    @property
    def daily_pnl(self) -> float:
        return self._daily_pnl

    def summary_line(self) -> str:
        return (
            f"bankroll={self._bankroll:.2f} USDT  "
            f"exposure={self._open_exposure:.2f}  "
            f"daily_pnl={self._daily_pnl:+.2f}"
        )

    # ── Persistence ──────────────────────────────────────────────────────────

    def save(self) -> None:
        path = Path(self.config.bankroll_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "bankroll": self._bankroll,
                    "open_exposure": self._open_exposure,
                    "daily_pnl": self._daily_pnl,
                    "today": self._today.isoformat(),
                    "day_start_bankroll": self._day_start_bankroll,
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def _load(self) -> None:
        path = Path(self.config.bankroll_file)
        if not path.exists():
            return
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        self._bankroll = float(state.get("bankroll", self._bankroll))
        self._open_exposure = float(state.get("open_exposure", 0.0))
        today_str = date.today().isoformat()
        if state.get("today") == today_str:
            self._daily_pnl = float(state.get("daily_pnl", 0.0))
            self._day_start_bankroll = float(
                state.get("day_start_bankroll", self._bankroll)
            )
        else:
            # New calendar day: reset daily counters
            self._daily_pnl = 0.0
            self._day_start_bankroll = self._bankroll

    def _maybe_reset_day(self) -> None:
        today = date.today()
        if today != self._today:
            self._today = today
            self._daily_pnl = 0.0
            self._day_start_bankroll = self._bankroll
            self.save()

    # ── Kelly internals ──────────────────────────────────────────────────────

    def _quality_to_win_prob(self, quality_score: float) -> float:
        """
        Map quality score [0..100] → estimated win probability [0.51..0.70].

        quality=50  → base_win_rate          (no adjustment)
        quality=100 → base_win_rate + 0.07   (best signals)
        quality=0   → base_win_rate - 0.07   (weakest signals; floored at 0.51)
        """
        adjustment = (quality_score - 50.0) / 50.0 * 0.07
        return round(max(0.51, min(0.70, self.config.base_win_rate + adjustment)), 4)
