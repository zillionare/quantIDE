import datetime
from functools import cache
from typing import Any, Dict, List, Optional, Tuple, Union

import arrow
import numpy as np
import pandas as pd
from arrow import Arrow
from coretypes import Frame, FrameType
from numpy.typing import NDArray
from xtquant import xtdata as xt

from pyqmt.core.constants import DATE_FORMAT, EPOCH, TIME_FORMAT, min_level_frames
from pyqmt.core.errors import XtQuantError
from pyqmt.core.timeframe import tf


def _format_date(dt: datetime.date):
    """format date to str to facilitate QMT"""
    return dt.strftime("%Y%m%d")

def cache_bars(
    symbols: Union[str, List[str]],
    frame_type: FrameType,
    start_time: Arrow,
    end_time: Arrow,
) -> bool:
    """让xtdata缓存行情数据"""
    if isinstance(symbols, str):
        symbols = [symbols]

    if frame_type in min_level_frames:
        start = start_time.format(TIME_FORMAT)
        end = end_time.format(TIME_FORMAT)
    else:
        start = start_time.format(DATE_FORMAT)
        end = end_time.format(DATE_FORMAT)

    # todo: 增加重启重连功能、超时功能
    try:
        return xt.download_history_data2(symbols, frame_type.value, start, end)  # type: ignore
    except Exception as e:
        raise XtQuantError.parse_msg(str(e))


def get_bars(symbols: List[str], frame_type: FrameType, start: Arrow, end: Arrow):
    if frame_type in min_level_frames:
        FMT = TIME_FORMAT
    else:
        FMT = DATE_FORMAT

    return xt.get_market_data(
        stock_list=symbols,
        period=frame_type.value,
        start_time=start.format(FMT),
        end_time=end.format(FMT),
        fill_data=False,
        dividend_type="none",
    )


@cache
def get_ashare_list():
    ashare_all = "沪深A股"
    return xt.get_stock_list_in_sector(ashare_all)


def get_sectors():
    sectors = xt.download_sector_data()


@cache
def get_calendar(end: datetime.date|None = None)->NDArray: # type: ignore
    """获取交易日历
    
    获取从EPOCH以来，至今的交易日历。

    根据QMT文档，也可以获取未来日期下的交易日历，但要先通过get_holidays获取节假日。但2024/3/8测试，此API会抛出runtime error,乱码。

    Args:
        end: the date fetch calendar till to

    Returns:
        a numpy array of datetime.date
    """
    market = "SH"

    if end is None:
        end_time = ""
    else:
        end_time = f"{end.year:04d}{end.month:02d}{end.day:02d}"

    days = xt.get_trading_dates(market, start_time=EPOCH, end_time=end_time)
    utc_datetime = pd.Series(days, dtype="datetime64[ms]").dt.tz_localize("UTC")
    return utc_datetime.dt.tz_convert("Asia/Shanghai").dt.date.values # type: ignore


def get_security_info(symbol: str) -> Tuple[str, datetime.date, datetime.date]:
    """获取证券详细信息, 即别名、IPO日、退市日

    Args:
        symbol: 证券品种代码
    Returns:
        证券显示名、IPO日和退市日。其它信息忽略掉。
    """
    item = xt.get_instrument_detail(symbol)
    if item is None:
        raise ValueError(f"invalid symbol: {symbol}")

    if item["ExpireDate"] == 99999999:
        exit_day = datetime.date(2099, 12, 31)
    else:
        exit_day = arrow.get(item["ExpireDate"]).date()

    return item["InstrumentName"], arrow.get(item["OpenDate"]).date(), exit_day


@cache
def get_factor_ratio(symbol: str, start: datetime.date, end: datetime.date)->pd.Series:
    """获取`symbol`在`start`到`end`期间的复权因子
    
    复权因子以EPOCH日为1，依次向后增加。返回值取整个复权因子区间中[start, end]这一段。

    Args:
        symbol: 个股代码，以.SZ/.SH等结尾
        start: 起始日期，不得早于EPOCH
        end: 结束日期，不得晚于当前时间

    Returns:
        以日期为index, factor为column的DataFrame
    """
    if start < tf.int2date(EPOCH):
        raise ValueError(f"start date should not be earlier than {EPOCH}: {start}")
    
    start_ = tf.date2int(start)
    end_ = tf.date2int(end)
    df = xt.get_divid_factors(symbol, EPOCH)


    df.index = df.index.astype(int)
    frames = pd.DataFrame([], index=tf.day_frames)
    factor = pd.concat([frames, df["dr"]], axis=1)
    factor.sort_index(inplace=True)
    factor.fillna(1, inplace=True)

    query = f'index >= {start_} and index <= {end_}'
    return factor.cumprod().query(query)["dr"]
