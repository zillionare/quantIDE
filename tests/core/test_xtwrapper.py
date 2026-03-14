import datetime
import time
from unittest.mock import patch

import cfg4py
import numpy as np
import pytest
from pytest import approx

xtdata = pytest.importorskip("xtquant.xtdata")

from pyqmt.core.xtwrapper import (
    cache_bars,
    get_calendar,
    get_factor_ratio,
    get_stock_list,
    subcribe_live,
)
from tests.config import setup


def test_get_ashare_list():
    """测试get_ashare_list的cache机制"""
    ashares = get_stock_list()
    with patch(
        "pyqmt.core.xtwrapper.xt.get_stock_list_in_sector", return_value=["hello"]
    ):
        cached = get_stock_list()
        assert np.array_equal(cached, ashares)

        get_stock_list.cache_clear()
        result = get_stock_list()
        assert np.array_equal(result, ["hello"])


def test_get_calendar():
    calendar = get_calendar()

    assert calendar[0] == datetime.date(2005, 1, 4)
    assert len(calendar) > 4659


def test_get_factor(setup):
    start = datetime.date(2022, 7, 8)
    end = datetime.date(2024, 3, 8)
    symbol = "002594.SZ"
    factors = get_factor_ratio(symbol, start, end)
    assert factors.index[0] == 20220708
    assert factors.index[-1] == 20240308
    assert 1.020403 == approx(factors.loc[20220708])
    assert approx(factors.loc[20230728]) == 1.025215
    assert approx(factors.loc[20240308]) == 1.025215

    # 测试在2005年以前上市并有分红的股票的因子是否正确
    symbol = "000002.SZ"
    factors = get_factor_ratio(symbol, start, end)
    assert approx(factors.loc[20220708]) == 5.685115
    assert approx(factors.loc[20220825]) == 6.037479


def test_subscribe_live():
    subcribe_live()

    for i in range(100):
        import time

        time.sleep(1)
