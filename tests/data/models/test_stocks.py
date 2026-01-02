import datetime
from pathlib import Path
import tempfile
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest
import pytz
from freezegun import freeze_time


from tests import asset_dir, bars_ext, calendar
from pyqmt.data import daily_bars
from pyqmt.data import stock_list
from pyqmt.config import cfg

@pytest.fixture
def setup(asset_dir: Path):
    cfg.epoch = datetime.date(2024, 1, 1) # type: ignore
    stock_list.load(asset_dir / "stock_list.parquet")
    daily_bars.connect(asset_dir / "2024_bars_ext_cols.parquet", asset_dir / "calendar.parquet")

@patch("pyqmt.data.models.stocks.logger")
@patch("pyqmt.data.models.stocks.fetch_stock_list")
def test_load(mock_fetch, mock_logger):
    with patch.object(stock_list, "save") as mock_save:
        stock_list.load("")

        assert mock_logger.warning.call_args.args[0] == "指定的股票列表文件不存在,{}"

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            stock_list.load(f.name)
            assert mock_logger.warning.call_args.args[0] == "加载股票列表失败,{}"

            assert mock_save.called


@freeze_time("2025-01-01")
def test_basic_apis(asset_dir: Path, setup: None):
    assert stock_list.size >= 5000
    assert stock_list.path == asset_dir / "stock_list.parquet"

    assert stock_list.days_since_ipo("000001.SZ") == 12327

    # 查询日期早于上市日期，返回0
    assert stock_list.days_since_ipo("920009.BJ") == 0

    assert stock_list.get_delist_date("000001.SZ") is None
    assert stock_list.get_delist_date("000003.SZ") == datetime.date(2002, 6, 14)
    assert stock_list.get_name("000001.SZ") == "平安银行"
    assert stock_list.get_pinyin("000001.SZ") == "PAYH"


def test_fuzzy_search(setup):
    assert set(stock_list.fuzzy_search("6888")).issuperset({"688800.SH", "688819.SH"})
    assert set(stock_list.fuzzy_search("平安")).issuperset(
        {"000001.SZ", "001359.SZ", "601318.SH"}
    )
    assert "000006.SZ" in stock_list.fuzzy_search("sz")  # 深振业 A
    assert "000001.SZ" not in stock_list.fuzzy_search("sz")  # 平安银行
    assert stock_list.fuzzy_search("PAYH") == ["000001.SZ"]
    assert stock_list.fuzzy_search("000007") == ["000007.SZ"]

    assert isinstance(stock_list.fuzzy_search("6888", id_only=False), pd.DataFrame)


def test_is_st(setup):
    dates = [datetime.date(2024, 6, 28), datetime.date(2024, 7, 2)]
    expects = [True, False]

    for date, expect in zip(dates, expects):
        actual = stock_list.is_st("000007.SZ", date)
        assert actual == expect


@freeze_time("2024-06-28")
def test_sample(setup):
    """测试StockList的sample方法"""
    test_date = datetime.date.today()

    # 获取所有上市股票作为参考
    all_stocks = stock_list.stocks_listed(test_date)
    total_stocks = len(all_stocks)

    # 确保有足够的股票进行测试
    assert total_stocks > 10, f"测试需要至少10支股票，实际只有{total_stocks}支"

    # 测试1: 正常抽样 - 抽样数量小于总数
    sample_size = 5
    sampled_stocks = stock_list.sample(test_date, sample_size, seed=42)

    assert len(sampled_stocks) == sample_size
    assert all(stock in all_stocks for stock in sampled_stocks)
    assert len(set(sampled_stocks)) == sample_size  # 确保没有重复

    # 测试2: 抽样数量大于总数
    large_sample_size = total_stocks + 10
    sampled_stocks_large = stock_list.sample(test_date, large_sample_size, seed=42)

    assert len(sampled_stocks_large) == total_stocks  # 应该返回所有股票

    # 测试3: 可重复性 - 相同seed应该返回相同结果
    sampled_stocks_repeat = stock_list.sample(test_date, sample_size, seed=42)
    assert sampled_stocks == sampled_stocks_repeat

    # 测试4: 不同seed应该产生不同结果
    sampled_stocks_diff_seed = stock_list.sample(test_date, sample_size, seed=123)
    assert sampled_stocks != sampled_stocks_diff_seed

    # 测试5: 抽样数量为0
    sampled_empty = stock_list.sample(test_date, 0, seed=42)
    assert len(sampled_empty) == 0

    # 测试6: 抽样数量为1
    sampled_one = stock_list.sample(test_date, 1, seed=42)
    assert len(sampled_one) == 1
    assert sampled_one[0] in all_stocks

    # 07 包含 ST
    sampled_all = stock_list.sample(test_date, size=6000, exclude_st=False)
    assert "000007.SZ" in sampled_all

    sampled_all = stock_list.sample(test_date, size=6000)
    assert "000007.SZ" not in sampled_all


def test_stocks_listed(setup):
    # 测试exclude_st参数 - 获取包含ST股票的完整列表
    date = datetime.date(2024, 6, 28)
    all_stocks = stock_list.stocks_listed(date)
    assert len(all_stocks) > 5000
    assert "000007.SZ" not in all_stocks

    all_stocks = stock_list.stocks_listed(date, exclude_st=False)
    assert "000007.SZ" in all_stocks


def test_update(setup):
    start = datetime.datetime.now(cfg.TIMEZONE)
    stock_list.update()
    end = datetime.datetime.now(cfg.TIMEZONE)

    assert stock_list.last_update_time >= start
    assert stock_list.last_update_time <= end
