from __future__ import annotations

from cloudasia_scanner.cloudbet_client import CloudbetClient


class FakeCloudbetClient(CloudbetClient):
    def __init__(self, payload: dict):
        super().__init__(base_url="http://example.com", api_key=None)
        self.payload = payload

    def _get_json(self, path: str, params=None):  # type: ignore[override]
        return self.payload


def test_get_soccer_competitions_supports_direct_categories_shape() -> None:
    payload = {
        "name": "Soccer",
        "categories": [
            {
                "name": "Japan",
                "competitions": [
                    {"name": "J.League", "key": "soccer-japan-j-league"},
                    {"name": "J2 League", "key": "soccer-japan-j2-league"},
                ],
            }
        ],
    }
    client = FakeCloudbetClient(payload)

    rows = client.get_soccer_competitions()

    assert len(rows) == 2
    assert rows[0]["key"] == "soccer-japan-j-league"
