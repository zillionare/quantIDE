import datetime
from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest

from quantide.data.models.calendar import Calendar
from quantide.data.stores.bars import DailyBarsStore


@pytest.fixture
def store(asset_dir):
    data = asset_dir / "2024_bars_ext_cols.parquet"
    calendar = asset_dir / "baseline_calendar.parquet"
    bars = DailyBarsStore(data, calendar)

    return bars


def test_rec_couns_per_date(store, bars):
    result = store.rec_counts_per_date()

    dt = datetime.date(2024, 3, 26)
    actual = result.get(dt)
    # bars["date"] is datetime64[ns], need to compare with Timestamp
    expect = bars[bars.date == pd.Timestamp(dt)].shape[0]
    assert actual == expect

    assert len(result) == 242

    start = datetime.date(2024, 3, 26)
    end = datetime.date(2024, 3, 27)
    result = store.rec_counts_per_date(start, end)

    assert len(result) == 2
    actual = result.get(dt)
    expect = bars[bars.date == pd.Timestamp(dt)].shape[0]
    assert actual == expect


def test_daily_bars_store_uses_injected_fetcher(asset_dir, tmp_path):
    fake_fetcher = MagicMock()
    fake_fetcher.fetch_bars_ext.return_value = (
        pl.DataFrame(
            {
                "asset": ["000001.SZ"],
                "date": [datetime.datetime(2024, 1, 2)],
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [10.2],
                "volume": [1000.0],
                "amount": [10200.0],
                "adjust": [1.0],
                "is_st": [False],
                "up_limit": [11.0],
                "down_limit": [9.0],
            },
            schema={
                "asset": pl.Utf8,
                "date": pl.Datetime("ms"),
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "amount": pl.Float64,
                "adjust": pl.Float64,
                "is_st": pl.Boolean,
                "up_limit": pl.Float64,
                "down_limit": pl.Float64,
            },
        ),
        [],
    )
    calendar = Calendar().load(asset_dir / "baseline_calendar.parquet")
    store = DailyBarsStore(tmp_path / "daily", calendar, data_fetcher=fake_fetcher)
    store.append_data = MagicMock()
    store._update_dates = MagicMock()

    count = store.fetch_with_daily_progress(
        start=datetime.date(2024, 1, 2),
        end=datetime.date(2024, 1, 2),
        force=True,
    )

    assert count == 1
    fake_fetcher.fetch_bars_ext.assert_called_once()
