import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import polars as pl
import pytest
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

from quantide.data.fetchers.tushare import (
    fetch_adjust_factor,
    fetch_bars,
    fetch_bars_ext,
    fetch_calendar,
    fetch_limit_price,
    fetch_st_info,
    fetch_stock_list,
)
from tests import (
    adjust_factor,
    asset_dir,
    bars,
    calendar_data,
    cfg,
    limit_price,
    st,
    year_2024_trade_dates,
)


class TestTushareFetcher:
    def test_fetch_calendar(self, calendar_data):
        fetched = fetch_calendar(datetime.date(2024, 1, 1))
        start = datetime.date(2024, 1, 1)
        end = datetime.date(2024, 12, 31)

        # Arrow Table 无 query 接口，需转 pandas 并设定索引为 date
        baseline = calendar_data.to_pandas().query("index >= @start and index <= @end")
        left = fetched.query("index >= @start and index <= @end")

        assert left.equals(baseline)

    def test_fetch_stock_list(self):
        with patch("quantide.data.fetchers.tushare.ts") as mock:
            mock.pro_api.return_value.stock_basic.return_value = pd.DataFrame()
            with pytest.raises(ValueError):
                fetch_stock_list()

        df = fetch_stock_list()
        assert df.columns.tolist() == [
            "asset",
            "name",
            "pinyin",
            "list_date",
            "delist_date",
        ]

        assert len(df) > 5000
        assert df[df.asset == "000001.SZ"]["list_date"].item() == datetime.date(
            1991, 4, 3
        )

    def test_fetch_adjust_factor(self, adjust_factor):
        # 返回空值的情况
        dates = [datetime.date(2024, 1, 1)]
        df, errors = fetch_adjust_factor(dates)
        assert df.empty
        assert len(errors) == 1
        assert errors[0][0] == "adj_factor"
        assert errors[0][1] == dates[0]

        # 传入单个日期
        date = datetime.date(2024, 1, 2)
        df, errors = fetch_adjust_factor(date)

        expected = (
            adjust_factor[adjust_factor["date"] == pd.Timestamp(date)]
            .sort_values(by="asset")
            .reset_index(drop=True)
        )
        actual = df.sort_values(by="asset").reset_index(drop=True)

        assert_frame_equal(expected, actual)

        # 传入多个日期
        dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
        df, errors = fetch_adjust_factor(dates)

        actual = df.sort_values(by=["asset", "date"]).reset_index(drop=True)
        ts_dates = [pd.Timestamp(d) for d in dates]
        expected = (
            adjust_factor[adjust_factor["date"].isin(ts_dates)]
            .sort_values(by=["asset", "date"])
            .reset_index(drop=True)
        )

        assert_frame_equal(expected, actual)

    def test_fetch_bars(self, bars):
        # 返回空值的情况
        dates = [datetime.date(2024, 1, 1)]

        df, errors = fetch_bars(dates)
        assert df.empty
        assert len(errors) == 1
        assert errors[0][0] == "daily"
        assert errors[0][1] == dates[0]

        # 传入单个日期
        date = datetime.date(2024, 1, 2)
        df, errors = fetch_bars(date)

        expected = (
            bars[bars["date"] == pd.Timestamp(date)]
            .sort_values(by="asset")
            .reset_index(drop=True)
        )
        actual = df.sort_values(by="asset").reset_index(drop=True)

        assert_frame_equal(expected, actual)

        # 传入多个日期
        dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
        df, errors = fetch_bars(dates)

        actual = df.sort_values(by=["asset", "date"]).reset_index(drop=True)
        ts_dates = [pd.Timestamp(d) for d in dates]
        expected = (
            bars[bars["date"].isin(ts_dates)]
            .sort_values(by=["asset", "date"])
            .reset_index(drop=True)
        )

        assert_frame_equal(expected, actual)

    def test_fetch_limit_price(self, limit_price):
        # 返回空值的情况
        dates = [datetime.date(2024, 1, 1)]

        df, errors = fetch_limit_price(dates)
        assert df.empty
        assert len(errors) == 1
        assert errors[0][0] == "stk_limit"
        assert errors[0][1] == dates[0]

        # 传入单个日期
        date = datetime.date(2024, 1, 2)
        df, errors = fetch_limit_price(date)

        expected = (
            limit_price[limit_price["date"] == pd.Timestamp(date)]
            .sort_values(by="asset")
            .reset_index(drop=True)
        )
        actual = df.sort_values(by="asset").reset_index(drop=True)

        assert_frame_equal(expected, actual)

        # 传入多个日期
        dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
        df, errors = fetch_limit_price(dates)

        actual = df.sort_values(by=["asset", "date"]).reset_index(drop=True)
        ts_dates = [pd.Timestamp(d) for d in dates]
        expected = (
            limit_price[limit_price["date"].isin(ts_dates)]
            .sort_values(by=["asset", "date"])
            .reset_index(drop=True)
        )

        assert_frame_equal(expected, actual)

        # 2007年之前，返回 类型稳定的空 DataFrame，且不触发远程调用
        pre_date = datetime.date(2006, 12, 31)
        with patch("quantide.data.fetchers.tushare._fetch_by_dates") as mock:
            empty_df, empty_errors = fetch_limit_price([pre_date])
            mock.assert_not_called()
        assert empty_df.empty
        assert empty_errors == []
        assert empty_df.columns.tolist() == ["asset", "date", "up_limit", "down_limit"]
        assert empty_df["asset"].dtype == object
        assert "datetime64" in str(empty_df["date"].dtype)
        assert "[ms]" in str(empty_df["date"].dtype)
        assert str(empty_df["up_limit"].dtype) == "float64"
        assert str(empty_df["down_limit"].dtype) == "float64"

        # 混合日期：仅请求 >= 2007-01-01 的日期
        cutoff = datetime.date(2007, 1, 1)
        valid_dates = [datetime.date(2007, 1, 4), datetime.date(2007, 1, 5)]

        def fake_fetch_by_dates(
            func_name, dates, *args, fields=None, rename_as=None, **kwargs
        ):
            assert all(d >= cutoff for d in dates)
            s_dates = pd.to_datetime(dates).astype("datetime64[ms]")
            # build minimal DataFrame with expected numeric columns
            return (
                pd.DataFrame(
                    {
                        "asset": ["000001.SZ"] * len(dates),
                        "date": s_dates,
                        "up_limit": [10.0] * len(dates),
                        "down_limit": [5.0] * len(dates),
                    }
                ),
                [],
            )

        with patch(
            "quantide.data.fetchers.tushare._fetch_by_dates",
            side_effect=fake_fetch_by_dates,
        ):
            mixed_df, mixed_errors = fetch_limit_price([pre_date] + valid_dates)

        assert mixed_errors == []
        assert mixed_df is not None
        # confirm returned dates are all >= cutoff and dtypes are float64
        returned_dates = mixed_df["date"].dt.date.unique().tolist()
        assert all(d >= cutoff for d in returned_dates)
        assert str(mixed_df["up_limit"].dtype) == "float64"
        assert str(mixed_df["down_limit"].dtype) == "float64"

    def test_fetch_st_info(self, st):
        # 返回空值的情况
        dates = [datetime.date(2024, 1, 1)]
        df, errors = fetch_st_info(dates)
        assert df.empty
        assert len(errors) == 1
        assert errors[0][0] == "stock_st"
        assert errors[0][1] == dates[0]

        # 传入单个日期
        date = datetime.date(2024, 1, 2)
        df, errors = fetch_st_info(date)

        expected = (
            st[st["date"] == pd.Timestamp(date)]
            .sort_values(by="asset")
            .reset_index(drop=True)
        )
        actual = df.sort_values(by="asset").reset_index(drop=True)

        # normalize 'st' semantics for comparison
        # For non-empty returns, fetch_st_info marks ST rows as True
        expected["st"] = True
        expected["st"] = expected["st"].astype("boolean")
        if "st" in actual.columns:
            actual["st"] = actual["st"].astype("boolean")

        assert_frame_equal(expected, actual)

        # 传入多个日期
        dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
        actual, errors = fetch_st_info(dates)

        actual = actual.sort_values(by=["asset", "date"]).reset_index(drop=True)
        ts_dates = [pd.Timestamp(d) for d in dates]
        expected = (
            st[st["date"].isin(ts_dates)]
            .sort_values(by=["asset", "date"])
            .reset_index(drop=True)
        )

        # ensure expected has 'st' set to True and boolean dtype
        expected["st"] = True
        expected["st"] = expected["st"].astype("boolean")
        if "st" in actual.columns:
            actual["st"] = actual["st"].astype("boolean")

        assert_frame_equal(expected, actual)

        # test pre-2016 dates return typed empty and no remote call
        pre_date = datetime.date(2015, 12, 31)
        with patch("quantide.data.fetchers.tushare._fetch_by_dates") as mock:
            df_pre, errors_pre = fetch_st_info([pre_date])
            mock.assert_not_called()

        assert df_pre.empty
        assert errors_pre == []
        assert df_pre.columns.tolist() == ["asset", "date", "st"]
        assert df_pre["asset"].dtype == object
        assert "datetime64" in str(df_pre["date"].dtype)
        assert "[ms]" in str(df_pre["date"].dtype)
        assert str(df_pre["st"].dtype) == "boolean"

        # test mixed dates only fetch >= 2016-01-01 and mark st=True
        cutoff = datetime.date(2016, 1, 1)
        valid_dates = [datetime.date(2016, 1, 4), datetime.date(2016, 1, 5)]

        def fake_fetch_by_dates(
            func_name, dates, *args, fields=None, rename_as=None, **kwargs
        ):
            assert all(d >= cutoff for d in dates)
            s_dates = pd.to_datetime(dates).astype("datetime64[ms]")
            df_ = pd.DataFrame({"asset": ["000001.SZ"] * len(dates), "date": s_dates})
            return df_, []

        with patch(
            "quantide.data.fetchers.tushare._fetch_by_dates",
            side_effect=fake_fetch_by_dates,
        ):
            df_mix, errors_mix = fetch_st_info([pre_date] + valid_dates)

        assert errors_mix == []
        returned_dates = df_mix["date"].dt.date.unique().tolist()
        assert all(d >= cutoff for d in returned_dates)
        assert df_mix["st"].eq(True).all()

        # test empty input returns typed empty and no remote call
        with patch("quantide.data.fetchers.tushare._fetch_by_dates") as mock2:
            df_empty, errors_empty = fetch_st_info([])
            mock2.assert_not_called()

        assert df_empty.empty
        assert errors_empty == []
        assert df_empty.columns.tolist() == ["asset", "date", "st"]
        assert df_empty["asset"].dtype == object
        assert "datetime64" in str(df_empty["date"].dtype)
        assert "[ms]" in str(df_empty["date"].dtype)
        assert str(df_empty["st"].dtype) == "boolean"

    def test_fetch_st_info_uses_stock_st(self):
        date = datetime.date(2024, 1, 5)
        pro = MagicMock()
        pro.stock_st.return_value = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "trade_date": ["20240105", "20240105"],
                "name": ["ST平安", "平安银行"],
                "type": ["S", "S"],
                "type_name": ["特别处理", "特别处理"],
            }
        )
        with patch("quantide.data.fetchers.tushare.ts.pro_api", return_value=pro):
            df, errors = fetch_st_info(date)

        assert len(errors) == 0
        assert len(df) == 2
        assert str(df["date"].dtype) == "datetime64[ms]"
        assert df.iloc[0]["asset"] == "000001.SZ"
        assert df.iloc[0]["st"] is True

    def test_fetch_bars_ext(self, bars, st, adjust_factor, limit_price):
        # 将 DatetimeIndex 转为 Python date 列表，避免对 DatetimeIndex 使用 .date 属性
        dates = pd.bdate_range("2024-01-02", "2024-01-04").to_series().dt.date.tolist()

        actual, errors = fetch_bars_ext(dates)

        start = pd.Timestamp(dates[0])
        end = pd.Timestamp(dates[-1])

        bars_pl = pl.from_pandas(bars.query("date >= @start and date <= @end")).lazy()
        st_pl = pl.from_pandas(st.query("date >= @start and date <= @end")).lazy()
        adjust_pl = pl.from_pandas(
            adjust_factor.query("date >= @start and date <= @end")
        ).lazy()
        limit_pl = pl.from_pandas(
            limit_price.query("date >= @start and date <= @end")
        ).lazy()

        expect = (
            bars_pl.join(adjust_pl, on=["date", "asset"], how="left")
            .join(st_pl, on=["date", "asset"], how="left")
            .join(limit_pl, on=["date", "asset"], how="left")
            .collect()
            .to_pandas()
        )

        # expect["date"] = expect["date"].dt.date  <-- Removed this

        # normalize actual to pandas for comparison
        if isinstance(actual, pl.LazyFrame):
            actual_pd = actual.collect().to_pandas()
        elif isinstance(actual, pl.DataFrame):
            actual_pd = actual.to_pandas()
        else:
            actual_pd = actual
        # normalize st column semantics to boolean with False default
        if "st" in expect.columns:
            expect["st"] = expect["st"].fillna(False).astype("boolean")
        if "st" in actual_pd.columns:
            actual_pd["st"] = actual_pd["st"].fillna(False).astype("boolean")
        assert_frame_equal(
            expect.sort_values(["asset", "date"]).reset_index(drop=True),
            actual_pd.sort_values(["asset", "date"]).reset_index(drop=True),
        )

        assert len(errors) == 0

        # when limit returns typed empty (pre-2007), bars_ext should still succeed
        empty_limit = pd.DataFrame(
            {
                "asset": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ms]"),
                "up_limit": pd.Series(dtype="float64"),
                "down_limit": pd.Series(dtype="float64"),
            }
        )
        with patch(
            "quantide.data.fetchers.tushare.fetch_limit_price",
            return_value=(empty_limit, []),
        ):
            actual2, errors2 = fetch_bars_ext(dates)
            # normalize actual2 to pandas
            actual2_pd = (
                actual2.collect().to_pandas()
                if isinstance(actual2, pl.LazyFrame)
                else actual2
            )
            assert (
                "up_limit" in actual2_pd.columns and "down_limit" in actual2_pd.columns
            )
            # with no limit rows, these columns are all NaN
            assert actual2_pd["up_limit"].isna().all()
            assert actual2_pd["down_limit"].isna().all()

        # 02 测试修改 func
        df = bars.iloc[-2:]
        with patch(
            "quantide.data.fetchers.tushare.fetch_bars",
            return_value=(df, [["daily", datetime.date(2024, 1, 2), "msg"]]),
        ):
            actual, errors = fetch_bars_ext(dates)
            assert errors[0][0] == "daily"
