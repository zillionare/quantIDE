import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest
from freezegun import freeze_time

from quantide.data.models.daily_bars import daily_bars as bars


def test_connect(asset_dir):
    bars.connect(
        asset_dir / "2024_bars.parquet", asset_dir / "baseline_calendar.parquet"
    )
    assert bars.store.start == datetime.date(2024, 1, 2)
    assert bars.store.end == datetime.date(2024, 12, 31)


def test_get_bars_in_range(asset_dir):
    data = asset_dir / "2024_bars_ext_cols.parquet"
    calendar = asset_dir / "baseline_calendar.parquet"
    bars.connect(data, calendar)

    # 01 时间范围及三种复权方式检验以及 assets = None | [...]
    start = datetime.date(2024, 10, 1)
    end = datetime.date(2024, 12, 31)
    actual = bars.get_bars_in_range(start, end, adjust=None)
    assert actual["date"].min().date() == datetime.date(2024, 10, 8)
    assert actual["date"].max().date() == datetime.date(2024, 12, 31)
    assert "is_st" in actual.columns
    assert "st" not in actual.columns
    assert actual.schema["volume"] == pl.Float64

    expected_start = datetime.date(2024, 10, 10)
    np.testing.assert_array_almost_equal(
        [12.88, 11.68, 11.98],
        actual.filter(
            (pl.col("asset") == "000001.SZ") & (pl.col("date") <= expected_start)
        )["close"],
        decimal=2,
    )

    actual = bars.get_bars_in_range(start, end, ["000001.SZ"], adjust="qfq")
    np.testing.assert_array_almost_equal(
        [12.6, 11.43, 11.98], actual.head(3)["close"], decimal=2
    )

    actual = bars.get_bars_in_range(start, end, ["000001.SZ"], adjust="hfq")
    np.testing.assert_array_almost_equal(
        [12.88, 11.68, 12.24], actual.head(3)["close"], decimal=2
    )

    # 02 end is None
    actual = bars.get_bars_in_range(start)
    assert actual["date"].min().date() == datetime.date(2024, 10, 8)
    assert actual["date"].max().date() == datetime.date(2024, 12, 31)

    # 03 测试 eager_mode
    actual = bars.get_bars_in_range(
        start, end, ["000001.SZ"], adjust="qfq", eager_mode=False
    )
    assert isinstance(actual, pl.LazyFrame)


def test_get_bars(asset_dir):
    data = asset_dir / "2024_bars_ext_cols.parquet"
    calendar = asset_dir / "baseline_calendar.parquet"
    bars.connect(data, calendar)

    # 01 验证 n 有效
    end = datetime.date(2024, 10, 10)
    actual = bars.get_bars(3, end, adjust=None)
    assert actual["date"].min().date() == datetime.date(2024, 10, 8)
    assert actual["date"].max().date() == datetime.date(2024, 10, 10)
    assert len(actual["date"].unique()) == 3
    no_adjust_close = actual.filter(pl.col("asset") == "000001.SZ")["close"]

    assert abs(no_adjust_close.item(-1) - 11.98) < 1e-2

    # 02 验证end = None
    with freeze_time("2024-12-20"):
        actual = bars.get_bars(3)
        assert len(actual["date"].unique()) == 3
        assert actual["date"].min().date() == datetime.date(2024, 12, 17)

    # 03 end = None, 取当前时间，但是是盘后
    with freeze_time("2024-12-20 16:00:00", 8):
        actual = bars.get_bars(3)
        assert len(actual["date"].unique()) == 3
        assert actual["date"].min().date() == datetime.date(2024, 12, 18)

    # 04 检查复权参数被正确传递
    actual = bars.get_bars(3, end, adjust="hfq")
    hfq_close = actual.filter(pl.col("asset") == "000001.SZ")["close"]
    np.testing.assert_array_almost_equal(
        hfq_close.head(2), no_adjust_close.head(2), decimal=2
    )
    assert abs(hfq_close.item(-1) - 12.24) < 1e-2

    ## 测试lazy mode
    end = datetime.date(2024, 10, 10)
    actual = bars.get_bars(3, end, adjust=None, eager_mode=False)
    assert isinstance(actual, pl.LazyFrame)
    collected = actual.collect()
    assert len(collected) == 16025
    assert len(collected.columns) == 12
    assert "is_st" in collected.columns


def test_property(asset_dir):
    data = asset_dir / "2024_bars_ext_cols.parquet"
    calendar = asset_dir / "baseline_calendar.parquet"
    bars.connect(data, calendar)

    assert bars.start == bars._store.start
    assert bars.start == datetime.date(2024, 1, 2)
    assert bars.end == bars._store.end
    assert bars.end == datetime.date(2024, 12, 31)
    assert bars.total_dates == bars._store.total_dates
    assert bars.size == bars._store.size
    assert bars.last_update_time == bars._store.last_update_time
