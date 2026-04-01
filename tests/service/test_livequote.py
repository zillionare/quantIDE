import datetime
import json
from unittest.mock import MagicMock, patch

import pandas as pd

from quantide.service.livequote import LiveQuote


def _reset_singleton():
    LiveQuote._instance = None


def test_parse_gateway_ws_payload():
    _reset_singleton()
    quote = LiveQuote()
    raw = json.dumps(
        {
            "symbol": "000001.SZ",
            "timestamp": datetime.datetime.now().timestamp(),
            "1m": {"open": 10.0, "high": 10.3, "low": 9.9, "close": 10.2, "vol": 1000, "amount": 10200},
            "1d": {"open": 9.8, "high": 10.5, "low": 9.7, "close": 10.1, "vol": 9000, "amount": 90000},
        }
    )
    payload = quote._parse_ws_payload(raw)
    assert "000001.SZ" in payload
    item = payload["000001.SZ"]
    assert item["price"] == 10.2
    assert item["open"] == 10.0


def test_refresh_limits():
    _reset_singleton()
    quote = LiveQuote()
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "up_limit": [11.0],
            "down_limit": [9.0],
        }
    )
    fake_fetcher = MagicMock()
    fake_fetcher.fetch_limit_price.return_value = (df, None)
    with patch("quantide.service.livequote.get_data_fetcher", return_value=fake_fetcher):
        quote._refresh_limits(datetime.date(2024, 1, 2))
    down, up = quote.get_price_limits("000001.SZ")
    assert up == 11.0
    assert down == 9.0
