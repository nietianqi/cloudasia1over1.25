from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

SPORTS_ODDS_BASE_URL = "https://sports-api.cloudbet.com/pub/v2/odds"
ACCOUNT_BASE_URL = "https://sports-api.cloudbet.com/pub"


@dataclass(slots=True)
class CloudbetClient:
    """Minimal public odds client used by the pre-match scanner."""

    base_url: str = SPORTS_ODDS_BASE_URL
    account_base_url: str = ACCOUNT_BASE_URL
    api_key: str | None = None
    api_key_header: str = "X-API-Key"
    timeout_seconds: float = 10.0

    def _request_json(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        headers: dict[str, str] = {}
        if self.api_key:
            headers[self.api_key_header] = self.api_key
        response = requests.get(url, params=params, headers=headers, timeout=self.timeout_seconds)
        if response.status_code in (401, 403):
            raise PermissionError(
                "Cloudbet API unauthorized. Please provide a valid API key "
                f"via `{self.api_key_header}` header."
            )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected response type from {url}: {type(data)!r}")
        return data

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request_json(self.base_url, path, params=params)

    def _get_account_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request_json(self.account_base_url, path, params=params)

    def get_soccer_competitions(self) -> list[dict[str, Any]]:
        payload = self._get_json("/sports/soccer")
        competitions: list[dict[str, Any]] = []

        # Newer payload shape: a single sport object with top-level categories.
        categories_direct = payload.get("categories", [])
        if isinstance(categories_direct, list):
            for category in categories_direct:
                comp_list = category.get("competitions", []) if isinstance(category, dict) else []
                if isinstance(comp_list, list):
                    competitions.extend(c for c in comp_list if isinstance(c, dict))

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

    def get_event_odds(self, event_id: str, markets: list[str]) -> dict[str, Any]:
        params = {"markets": markets}
        payload = self._get_json(f"/events/{event_id}", params=params)
        event = self._extract_event_payload(payload)
        if event is None:
            raise ValueError(f"Cloudbet event payload not found for event_id={event_id}")
        return event

    def validate_odds_auth(self) -> None:
        """Raise if odds API credentials are invalid."""
        self._get_json("/sports/soccer")

    def get_account_info(self) -> dict[str, Any]:
        return self._get_account_json("/v1/account/info")

    def get_account_currencies(self) -> list[str]:
        payload = self._get_account_json("/v1/account/currencies")
        raw = payload.get("currencies")
        if not isinstance(raw, list):
            return []
        values: list[str] = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                values.append(item.strip().upper())
        return values

    def get_account_balance(self, currency: str) -> float | None:
        code = str(currency or "").strip().upper()
        if not code:
            return None
        payload = self._get_account_json(f"/v1/account/currencies/{code}/balance")
        raw_amount = payload.get("amount")
        try:
            return float(raw_amount) if raw_amount is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_event_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
        direct_event = payload.get("event")
        if isinstance(direct_event, dict):
            return direct_event

        if "markets" in payload and any(k in payload for k in ("id", "key", "eventId", "event_id")):
            return payload

        events = payload.get("events")
        if isinstance(events, list) and events:
            first = events[0]
            if isinstance(first, dict):
                return first

        competitions = payload.get("competitions")
        if isinstance(competitions, list):
            for comp in competitions:
                if not isinstance(comp, dict):
                    continue
                comp_events = comp.get("events")
                if isinstance(comp_events, list) and comp_events:
                    first = comp_events[0]
                    if isinstance(first, dict):
                        return first

        return None
