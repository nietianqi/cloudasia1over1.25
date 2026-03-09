from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

SPORTS_ODDS_BASE_URL = "https://sports-api.cloudbet.com/pub/v2/odds"


@dataclass(slots=True)
class CloudbetClient:
    """Minimal public odds client used by the pre-match scanner."""

    base_url: str = SPORTS_ODDS_BASE_URL
    api_key: str | None = None
    api_key_header: str = "X-API-Key"
    timeout_seconds: float = 10.0

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        headers: dict[str, str] = {}
        if self.api_key:
            headers[self.api_key_header] = self.api_key
        response = requests.get(url, params=params, headers=headers, timeout=self.timeout_seconds)
        if response.status_code == 401:
            raise PermissionError(
                "Cloudbet API unauthorized. Please provide a valid API key "
                f"via `{self.api_key_header}` header."
            )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected response type from {url}: {type(data)!r}")
        return data

    def get_soccer_competitions(self) -> list[dict[str, Any]]:
        payload = self._get_json("/sports/soccer")
        competitions: list[dict[str, Any]] = []

        sports = payload.get("sports", [])
        if isinstance(sports, list):
            for sport in sports:
                categories = sport.get("categories", []) if isinstance(sport, dict) else []
                if isinstance(categories, list):
                    for category in categories:
                        comp_list = category.get("competitions", []) if isinstance(category, dict) else []
                        if isinstance(comp_list, list):
                            competitions.extend(c for c in comp_list if isinstance(c, dict))

        if not competitions:
            direct = payload.get("competitions", [])
            if isinstance(direct, list):
                competitions.extend(c for c in direct if isinstance(c, dict))

        return competitions

    def get_competition_odds(self, competition_key: str, markets: list[str]) -> dict[str, Any]:
        params = {"markets": markets}
        return self._get_json(f"/competitions/{competition_key}", params=params)
