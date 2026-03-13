from __future__ import annotations

import pytest

from cloudasia_scanner.cloudbet_client import CloudbetClient


class FakeCloudbetClient(CloudbetClient):
    def __init__(self, payload: dict, account_payloads: dict[str, dict] | None = None):
        super().__init__(base_url="http://example.com", api_key=None)
        self.payload = payload
        self.account_payloads = account_payloads or {}

    def _get_json(self, path: str, params=None):  # type: ignore[override]
        return self.payload

    def _get_account_json(self, path: str, params=None):  # type: ignore[override]
        if path not in self.account_payloads:
            raise ValueError(f"missing account payload for path={path}")
        return self.account_payloads[path]


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


def test_get_account_endpoints_parse_payloads() -> None:
    client = FakeCloudbetClient(
        payload={},
        account_payloads={
            "/v1/account/info": {"nickname": "tester", "uuid": "abc"},
            "/v1/account/currencies": {"currencies": ["usdt", "btc"]},
            "/v1/account/currencies/USDT/balance": {"amount": "123.4567"},
        },
    )

    info = client.get_account_info()
    currencies = client.get_account_currencies()
    balance = client.get_account_balance("usdt")

    assert info["nickname"] == "tester"
    assert currencies == ["USDT", "BTC"]
    assert balance == pytest.approx(123.4567)


def test_get_account_balance_returns_none_on_invalid_amount() -> None:
    client = FakeCloudbetClient(
        payload={},
        account_payloads={"/v1/account/currencies/USDT/balance": {"amount": "N/A"}},
    )
    assert client.get_account_balance("USDT") is None
