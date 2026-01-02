import datetime
import tempfile
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest
import pytz
from freezegun import freeze_time

from alpha.config import cfg
from alpha.data.models.daily_bars import DailyBars
from alpha.data.models.stocks import StockList
from tests import asset_dir, bars_ext, calendar


@pytest.fixture
def model(asset_dir):
    model_ = StockList()
    model_.load(asset_dir / "stock_list.parquet")
    return model_

@pytest.fixture
def mock_daily_bars(asset_dir):
    bars = DailyBars()
    bars.connect(asset_dir / "2024_bars_ext_cols.parquet", asset_dir / "calendar.parquet")
    return bars

@patch("alpha.data.models.stocks.logger")
@patch("alpha.data.models.stocks.fetch_stock_list")
def test_load(mock_fetch, mock_logger):
    model = StockList()
    with patch.object(model, "save") as mock_save:
        model.load("")

        assert mock_logger.warning.call_args.args[0] == "指定的股票列表文件不存在,{}"

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            model.load(f.name)
            assert mock_logger.warning.call_args.args[0] == "加载股票列表失败,{}"

            assert mock_save.called


@freeze_time("2025-01-01")
def test_basic_apis(asset_dir, model):
    assert model.size >= 5000
    assert model.path == asset_dir / "stock_list.parquet"

    assert model.days_since_ipo("000001.SZ") == 12327

    # 查询日期早于上市日期，返回0
    assert model.days_since_ipo("920009.BJ") == 0

    assert model.get_delist_date("000001.SZ") is None
    assert model.get_delist_date("000003.SZ") == datetime.date(2002, 6, 14)
    assert model.get_name("000001.SZ") == "平安银行"
    assert model.get_pinyin("000001.SZ") == "PAYH"


def test_fuzzy_search(model):
    assert set(model.fuzzy_search("6888")).issuperset({"688800.SH", "688819.SH"})
    assert set(model.fuzzy_search("平安")).issuperset(
        {"000001.SZ", "001359.SZ", "601318.SH"}
    )
    assert "000006.SZ" in model.fuzzy_search("sz")  # 深振业 A
    assert "000001.SZ" not in model.fuzzy_search("sz")  # 平安银行
    assert model.fuzzy_search("PAYH") == ["000001.SZ"]
    assert model.fuzzy_search("000007") == ["000007.SZ"]

    assert isinstance(model.fuzzy_search("6888", id_only=False), pd.DataFrame)


def test_is_st(asset_dir, calendar, bars_ext):
    bars = DailyBars()
    data = asset_dir / "2024_bars_ext_cols.parquet"
    calendar = asset_dir / "baseline_calendar.parquet"
    bars.connect(data, calendar)

    model = StockList()

    dates = [datetime.date(2024, 6, 28), datetime.date(2024, 7, 2)]
    expects = [True, False]

    for date, expect in zip(dates, expects):
        actual = model.is_st("000007.SZ", date)
        assert actual == expect


@freeze_time("2024-06-28")
def test_sample(asset_dir, mock_daily_bars):
    """测试StockList的sample方法"""
    from alpha.data import daily_bars
    daily_bars._set_impl(mock_daily_bars)
    model = StockList()
    model.load(asset_dir / "stock_list.parquet")

    test_date = datetime.date.today()

    # 获取所有上市股票作为参考
    all_stocks = model.stocks_listed(test_date)
    total_stocks = len(all_stocks)

    # 确保有足够的股票进行测试
    assert total_stocks > 10, f"测试需要至少10支股票，实际只有{total_stocks}支"

    # 测试1: 正常抽样 - 抽样数量小于总数
    sample_size = 5
    sampled_stocks = model.sample(test_date, sample_size, seed=42)

    assert len(sampled_stocks) == sample_size
    assert all(stock in all_stocks for stock in sampled_stocks)
    assert len(set(sampled_stocks)) == sample_size  # 确保没有重复

    # 测试2: 抽样数量大于总数
    large_sample_size = total_stocks + 10
    sampled_stocks_large = model.sample(test_date, large_sample_size, seed=42)

    assert len(sampled_stocks_large) == total_stocks  # 应该返回所有股票

    # 测试3: 可重复性 - 相同seed应该返回相同结果
    sampled_stocks_repeat = model.sample(test_date, sample_size, seed=42)
    assert sampled_stocks == sampled_stocks_repeat

    # 测试4: 不同seed应该产生不同结果
    sampled_stocks_diff_seed = model.sample(test_date, sample_size, seed=123)
    assert sampled_stocks != sampled_stocks_diff_seed

    # 测试5: 抽样数量为0
    sampled_empty = model.sample(test_date, 0, seed=42)
    assert len(sampled_empty) == 0

    # 测试6: 抽样数量为1
    sampled_one = model.sample(test_date, 1, seed=42)
    assert len(sampled_one) == 1
    assert sampled_one[0] in all_stocks

    # 07 包含 ST
    sampled_all = model.sample(test_date, size=6000, exclude_st=False)
    assert "000007.SZ" in sampled_all

    sampled_all = model.sample(test_date, size=6000)
    assert "000007.SZ" not in sampled_all


def test_stocks_listed(model, asset_dir, mock_daily_bars):
    from alpha.data import daily_bars
    daily_bars._set_impl(mock_daily_bars)

    # 测试exclude_st参数 - 获取包含ST股票的完整列表
    date = datetime.date(2024, 6, 28)
    all_stocks = model.stocks_listed(date)
    assert len(all_stocks) > 5000
    assert "000007.SZ" not in all_stocks

    all_stocks = model.stocks_listed(date, exclude_st=False)
    assert "000007.SZ" in all_stocks


def test_update(model):
    start = datetime.datetime.now(cfg.TIMEZONE)
    model.update()
    end = datetime.datetime.now(cfg.TIMEZONE)

    assert model.last_update_time >= start
    assert model.last_update_time <= end
