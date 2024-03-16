import datetime
import sqlite3
import time

import arrow
import cfg4py
import pytest
from coretypes import FrameType, SecurityType
from freezegun import freeze_time

from pyqmt.core.timeframe import tf
from pyqmt.core.xtwrapper import subcribe_live
from pyqmt.dal import init, cache
from pyqmt.service.sync import (
    sync_bars,
    sync_bars_backward,
    sync_bars_forward,
    sync_calendar,
    sync_security_list,
)
from tests.config import get_config_dir, init_chores, init_haystore


@pytest.fixture(scope="function", autouse=True)
def setup():
    cfg = cfg4py.init(get_config_dir())
    cfg.cache = RedisCache() # type: ignore
    sync_calendar()

    init_dal()
    init_haystore()
    init_chores()

@pytest.mark.parametrize(
    "symbols, frame_type",
    [(["000001.SZ"], FrameType.DAY), (["000001.SZ"], FrameType.MIN1)],
)
def test_sync_bars_forward(symbols, frame_type):
    sync_bars_forward(symbols, frame_type)


@pytest.mark.parametrize(
    "symbols, frame_type",
    [(["000001.SZ"], FrameType.DAY), (["000001.SZ"], FrameType.MIN1)],
)
def test_sync_bars_backward(symbols, frame_type):
    sync_bars_forward(symbols, frame_type)
    sync_bars_backward(symbols, frame_type)


def test_sync_calendar():
    cfg = cfg4py.get_instance()

    cache.r.flushall()
    sync_calendar()

    keys = cache.r.keys()
    assert "calendar:1d" in keys

    tf.init()
    last_trading_day = tf.floor(arrow.now().date(), FrameType.DAY)
    assert tf.int2date(tf.day_frames[-1]) == last_trading_day


@freeze_time("2024-03-13")
def test_sync_ashare_list():
    t0 = time.time()
    sync_security_list()
    print("sync_ashare_list cost:", time.time() - t0)

    # 第二次应该被拦截。否则clickhouse仍然会允许插入.clickhouse没有惟一主键的概念
    sync_security_list()
    cfg = cfg4py.get_instance()
    sql = "select * from securities where dt=%(dt)s and symbol=%(symbol)s"
    r = haystore.query_df(sql, dt="2024-03-13", symbol="000001.SZ")

    assert r.iloc[0]["dt"].date() == datetime.date(2024,3,13)
    assert r.iloc[0]["type"] == SecurityType.STOCK.value
    assert r.iloc[0]["ipo"].date() == datetime.date(1991,4,3)

    r = haystore.query_df(sql, dt="2024-03-13", symbol="600000.SH")
    assert r.iloc[0]["dt"].date() == datetime.date(2024,3,13)
    assert r.iloc[0]["type"] == SecurityType.STOCK.value
    assert r.iloc[0]["ipo"].date() == datetime.date(1999,11,10)


def test_sync_bars():
    subcribe_live()

    for i in range(120):
        time.sleep(1)

    tm = datetime.datetime.now()
    sync_bars(tm, FrameType.MIN1)
