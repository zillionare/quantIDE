import datetime

import pandas as pd
import pytest

from pyqmt.data.stores.bars import DailyBarsStore
from tests import asset_dir, bars


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
