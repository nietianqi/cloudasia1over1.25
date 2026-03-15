from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(slots=True)
class PreMatchWatchRecord:
    match_id: str
    competition_key: str
    home_team: str
    away_team: str
    league: str
    kickoff_time: datetime
    ah_main_line: float
    favorite_side: str
    favorite_team: str
    underdog_team: str
    favorite_line_abs: float
    fav_odds: float
    dog_odds: float
    pre_match_bucket: str
    scan_time: datetime
    minutes_to_kickoff: float
    watchlist_flag: bool = True
    strategy_tag: str = "PRE_FAVORITE_DEEP_AH"
    strategy_a_done: bool = False
    strategy_b_done: bool = False
    bet_done: bool = False

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["kickoff_time"] = self.kickoff_time.isoformat()
        payload["scan_time"] = self.scan_time.isoformat()
        return payload
