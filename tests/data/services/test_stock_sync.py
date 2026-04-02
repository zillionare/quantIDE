import datetime
from unittest.mock import MagicMock

import pandas as pd

from quantide.data.services.stock_sync import StockSyncService


def test_sync_stock_list_uses_injected_fetcher(monkeypatch):
    monkeypatch.setattr(
        "quantide.data.services.stock_sync.get_epoch",
        lambda: datetime.date(2024, 1, 1),
    )
    fetcher = MagicMock()
    fetcher.fetch_stock_list.return_value = pd.DataFrame(
        {
            "asset": ["000001.SZ"],
            "name": ["平安银行"],
            "pinyin": ["PAYH"],
            "list_date": [datetime.date(1991, 4, 3)],
            "delist_date": [pd.NaT],
        }
    )
    stock_list = MagicMock()
    daily_store = MagicMock()
    calendar = MagicMock()

    service = StockSyncService(stock_list, daily_store, calendar, fetcher=fetcher)

    assert service.sync_stock_list() == 1
    fetcher.fetch_stock_list.assert_called_once_with()
    stock_list.update.assert_called_once()
    saved_df = stock_list.update.call_args.args[0]
    assert list(saved_df["asset"]) == ["000001.SZ"]
