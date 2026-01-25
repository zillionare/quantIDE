"""

## 数据同步

### 程序启动时的同步

在程序每次启动时，检查：

1. 从chores.bars_cache_status表中，对照start, end和epoch，进行运算
2. 如果start > epoch，则创建[epoch, now, frame_type]的下载任务,完成
3. 如果now > end, 则创建[end, now, frame_type]的下载任务。完成

下载任务成功与否，以xtquant.xtdata的返回值为准。基于xtquant的开发状态，如果xtdata出错，很难补救，放弃努力。


### 每日同步

在每天凌晨进行数据同步。

### 日内tick同步

在日内进行tick订阅，将其存入clickhouse。通过clickhouse的聚合方法合成日内的分钟线。
"""

import datetime
import logging
from functools import partial
from typing import Callable, List, Optional, Tuple, Union

import cfg4py
import pandas as pd
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.job import Job
from coretypes import FrameType

from pyqmt.core.constants import EPOCH
from pyqmt.core.context import g
from pyqmt.core.timeframe import tf
from pyqmt.core.xtwrapper import (
    cache_bars,
    get_bars,
    get_calendar,
    get_factor_ratio,
    get_security_info,
    get_stock_list,
)
from pyqmt.sametime import Actor, pool
from pyqmt.sametime.actor import GeneralActor

Frame = Union[datetime.date, datetime.datetime]

from copy import copy

import arrow

from pyqmt.config import get_config_dir
from pyqmt.core.constants import DATE_FORMAT, TIME_FORMAT
from pyqmt.core.scheduler import scheduler
from pyqmt.core.utils import date2str, handle_xt_error, str2date, str2time, time2str

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


def on_startup_sync():
    """ "在程序启动时进行数据同步。

    程序启动时的数据同步主要场景：
    1. 程序刚安装，第一次同步
    2. 程序意外退出后的再次启动

    程序第一次启动时，后台线程启动全量下载，直到完成，并删除first_startup标志。此后启动，只从bars_sync_end开始，直到最后一个已结束的交易日。
    """

    # 2024/3/17 get_market_data_ex, 参数不为1m, 1d时装饰出runtimeError错，且为乱码
    cache_bars(FrameType.DAY)

    start = g.chores.get_bars_sync_end(FrameType.DAY, "OHLC")
    last_closed = tf.floor(datetime.datetime.now(), FrameType.DAY)

    if start > last_closed:
        logger.warning(
            "向haystore同步行情时，发现记录错误: 已收盘%s, 记录为%s", last_closed, start
        )

    if start < last_closed:
        # 一次sync约100万条记录
        n = len(get_stock_list())
        tasks = []
        while start < last_closed:
            end = tf.shift(start, 1_000_000 // n, FrameType.DAY)
            if end == start:  # 当索引越界时，tf.shift会返回moment
                end = last_closed
            if end > last_closed:
                end = last_closed
            tasks.append(
                GeneralActor(
                    sync_bars,
                    (FrameType.DAY, start, end),
                    f"sync-bars-1d-{start}-{end}",
                )
            )
            start = tf.shift(end, 1, FrameType.DAY)

        g.pool.submit(tasks)

    frame_type = FrameType.MIN1
    cache_bars(frame_type)

    start = g.chores.get_bars_sync_end(frame_type, "OHLC")
    last_close_day = tf.floor(datetime.datetime.now(), FrameType.DAY)

    if start > last_close_day:
        logger.warning(
            "向haystore同步行情时，发现记录错误: 已收盘%s, 记录为%s",
            last_close_day,
            start,
        )

    if start < last_close_day:
        # 一次sync约100万条记录
        n = len(get_stock_list())
        tasks = []
        while start < last_close_day:
            start_tm = tf.first_min_frame(start, frame_type)
            end_tm = tf.shift(start_tm, 1_000_000 // n, frame_type)
            end = end_tm.date()  # type: ignore
            if end > last_closed:
                end = last_closed
            tasks.append(
                GeneralActor(
                    sync_bars,
                    (frame_type, start, end),
                    f"sync-bars-{frame_type.value}-{start}-{end}",
                )
            )
            start = tf.shift(end, 1, frame_type)

        g.pool.submit(tasks)


def sync_bars(frame_type: FrameType, start: datetime.date, end: datetime.date):
    """将下载到本地的数据同步到haystore中

    Args:
        frame_type: 要同步的行情周期
        start: 起始日期
        end: 结束日期
    """
    stock_list = get_stock_list()

    bars = get_bars(stock_list, frame_type, start, end)
    logger.info(
        "syncing %s securities bars from %s to %s of %s",
        len(set(bars.symbols)),
        start,
        end,
        frame_type,
    )

    g.haystore.save_bars(frame_type, bars)


def schedule_after(after: Actor, job_func: Callable, args: Tuple[List[str], FrameType]):
    def listener(after, job_func, args, event):
        if event.job_id != after.id:
            return

        if event.exception:
            logger.warning(
                "任务%s执行失败，任务%s终止启动", after.name, job_func.__name__
            )
            return

        # 增加任务，立即执行
        scheduler.add_job(job_func, args=args)

    my_listener = partial(listener, after, job_func, args)
    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


def sync_sector_list(force=False):
    """保存当天的板块列表

    Args:
        force: 如果dt在事务数据库中存在，则只有force为true时，才会重新转存。
    """


def sync_security_list(force=False):
    last_trading_day: datetime.date = tf.floor(arrow.now().date(), FrameType.DAY)
    if ashares_sync_status(last_trading_day) and not force:
        return

    # todo: if force, then make sure exists records be purged beforehand
    data = []
    secs = get_stock_list()
    for sec in secs:
        items = get_security_info(sec)
        data.append((last_trading_day, sec, *items))

    df = pd.DataFrame(data, columns=["dt", "symbol", "alias", "ipo", "type"])
    g.haystore.save_ashare_list(df)
    # chores.save_ashares_sync_status(last_trading_day)


def sync_calendar():
    """交易日历"""
    calendar = get_calendar()
    tf.save_calendar(calendar)


def sync_factor():
    secs = get_stock_list()
    last_trade_day = tf.floor(arrow.now().date(), FrameType.DAY)

    data = []
    for sec in secs:
        factor = get_factor_ratio(sec, arrow.get(EPOCH).date(), last_trade_day)
        factor["sec"] = [sec] * len(factor)
        data.append(factor)

    g.haystore.save_factors(data)


def start_intraday_sync():
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=9,
        minute="31-59",
        second=1,
        name=f"{FrameType.MIN1.value}:9:31-59",
    )

    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=10,
        minute="*",
        second=1,
        name=f"{FrameType.MIN1.value}:10:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=11,
        minute="0-30",  # 0-30，执行31次
        second=1,
        name=f"{FrameType.MIN1.value}:11:0-30",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=13,
        minute="1-59",
        second=1,
        name=f"{FrameType.MIN1.value}:13:1-59",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=14,
        minute="*",
        second=1,
        name=f"{FrameType.MIN1.value}:14:*",
    )
    scheduler.add_job(
        sync_minute_bars,
        "cron",
        hour=15,
        minute=0,  # 15:00, 执行1次
        second=1,
        name=f"{FrameType.MIN1.value}:15:00",
    )
