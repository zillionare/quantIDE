import datetime
import time
from unittest.mock import patch

import arrow
import cfg4py
import pytest
from coretypes import FrameType, SecurityType
from freezegun import freeze_time

from pyqmt.core.constants import EPOCH
from pyqmt.core.context import g
from pyqmt.core.timeframe import tf
from pyqmt.core.utils import str2date
from pyqmt.core.xtwrapper import subcribe_live
from pyqmt.dal.haystore import Haystore
from pyqmt.sametime import ExecutorPool
from pyqmt.service.sync import on_startup_sync, sync_calendar, sync_security_list
from tests.config import setup


def on_worker_start():
    import cfg4py

    from pyqmt.core.context import g
    from tests.config import get_config_dir
    cfg4py.init(get_config_dir())
    g.haystore = Haystore()


def test_on_startup_sync(setup):
    g.pool = ExecutorPool(before_start=on_worker_start, max_workers=1)
    start = str2date(EPOCH)
    end = tf.day_shift(datetime.datetime.now(), -2)
    for frame_type in tf.day_level_frames:
        g.chores.save_bars_cache_status(start, end, frame_type)

    for frame_type in tf.minute_level_frames:
        g.chores.save_bars_cache_status(start, end, frame_type)
        
    on_startup_sync()
    g.pool.join()


def test_sync_calendar():
    cfg = cfg4py.get_instance()

    g.cache.r.flushall()
    sync_calendar()

    keys = g.cache.r.keys()
    assert "calendar:1d" in keys # type: ignore

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
    r = g.haystore.query_df(sql, dt="2024-03-13", symbol="000001.SZ")

    assert r.iloc[0]["dt"].date() == datetime.date(2024, 3, 13)
    assert r.iloc[0]["type"] == SecurityType.STOCK.value
    assert r.iloc[0]["ipo"].date() == datetime.date(1991, 4, 3)

    r = g.haystore.query_df(sql, dt="2024-03-13", symbol="600000.SH")
    assert r.iloc[0]["dt"].date() == datetime.date(2024, 3, 13)
    assert r.iloc[0]["type"] == SecurityType.STOCK.value
    assert r.iloc[0]["ipo"].date() == datetime.date(1999, 11, 10)



