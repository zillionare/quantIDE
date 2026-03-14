import datetime
import logging
from functools import cache
from typing import Any, Dict, List, Optional, Tuple, Union

import arrow
import cfg4py
import numpy as np
import pandas as pd
from arrow import Arrow
from numpy.typing import NDArray

from pyqmt.core.enums import FrameType
from pyqmt.core.errors import XtQuantError
from pyqmt.core.utils import date2str, time2minute

Frame = datetime.datetime | datetime.date

logger = logging.getLogger(__name__)

cfg = cfg4py.get_instance()

# xtquant 延迟导入
_xt = None


def _require_xt() -> Any:
    """获取 xtquant.xtdata 模块，延迟导入"""
    global _xt
    if _xt is None:
        from xtquant import xtdata as xt
        _xt = xt
    return _xt


def require_xt() -> Any:
    """获取 xtquant.xtdata 模块，延迟导入（公开版本）"""
    return _require_xt()


def _format_date(dt: datetime.date):
    """format date to str to facilitate QMT"""
    return dt.strftime("%Y%m%d")


def on_subscribe_callback(data):
    """从订阅数据中提取lastPrice，存入缓存中

    Args:
        data: 具有如下结果的json:

    ```json
    {
        '000001.SZ':
        {
            'time': 1710127194000,
            'lastPrice': 10.42,
            'open': 10.38,
            'high': 10.47,
            'low': 10.34,
            'lastClose': 10.38,
            'amount': 710422600.0,
            'volume': 683450,
            'pvolume': 68345011,
            'stockStatus': 0,
            'openInt': 13,
            'transactionNum': 0,
            'lastSettlementPrice': 0.0,
            'settlementPrice': 0.0,
            'pe': 0.0,
            'askPrice': [10.42, 10.43, 10.44, 0.0, 0.0],
            'bidPrice': [10.41, 10.4, 10.39, 0.0, 0.0],
            'askVol': [9349, 7557, 6217, 0, 0],
            'bidVol': [3119, 6685, 4865, 0, 0],
            'volRatio': 0.0,
            'speed1Min': 0.0,
            'speed5Min': 0.0
        }
    }
    ```
    """
    last_prices = {code: item["lastPrice"] for code, item in data.items()}
    cache.security.hset(key_price, mapping=last_prices)


def subcribe_live():
    _require_xt().subscribe_whole_quote(["SH", "SZ"])


def cache_bars(frame_type: FrameType):
    """让xtdata缓存行情数据

    Args:
        frame_type: 行情周期，比如FrameType.MIN1, FrameType.DAY
    Raises:
        XtQuantError: 如果错误来自XtQuant

    """
    time_range = g.chores.get_bars_cache_status(frame_type)
    if time_range is None:
        start = g.chores.calc_bars_cache_start(frame_type)
    else:
        start = time_range[1]

    # 直到 2024/3/15, dhd_2 API 只能接受start/end为日期，不能到分钟，与文档不一致，原因不明。
    end = tf.floor(datetime.datetime.now(), FrameType.DAY)

    if start >= end:
        logger.info("%s is cached already.", frame_type)
        return

    # todo: 增加重启重连功能、超时功能
    try:
        xt_ = _require_xt()
        symbols = get_stock_list()
        # 以下API提供了返回值，但该返回值并不可信。经测试，它可能返回None,但缓存已成功
        xt_.download_history_data2(
            symbols, frame_type.value, date2str(start), date2str(end)
        )  # type: ignore
    except Exception as e:
        raise XtQuantError.parse_msg(str(e))

    g.chores.save_bars_cache_status(start, end, frame_type)


def get_bars(
    symbols: List[str], frame_type: FrameType, start: Frame, end: Frame | None
):
    if frame_type in min_level_frames:
        start_ = time2minute(start)  # type: ignore
        end_ = time2minute(end) if end else ""  # type: ignore
    else:
        start_ = date2str(start)
        end_ = date2str(end) if end else ""

    field_list = [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "suspendFlag",
    ]

    data = _require_xt().get_market_data_ex(
        field_list,
        stock_list=symbols,
        period=frame_type.value,
        start_time=start_,
        end_time=end_,
        fill_data=False,
        dividend_type="none",
        count=-1,
    )

    # convert to dataframe. Since py3.6, keys() and values() are all same order
    for symbol, df in data.items():
        df["symbol"] = symbol

    df = pd.concat(data.values(), ignore_index=True)

    df.time = np.array(df.time, dtype="datetime64[ms]").astype(datetime.datetime)
    df.time = df["time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
    df.rename(
        {"time": "frame", "amount": "money", "suspendFlag": "suspend"},
        axis="columns",
        inplace=True,
    )
    return df


@cache
def get_stock_list():
    ashare_all = "沪深A股"
    return _require_xt().get_stock_list_in_sector(ashare_all)


def get_sectors():
    sectors = _require_xt().download_sector_data()


@cache
def get_calendar(end: datetime.date | None = None) -> NDArray:  # type: ignore
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

    days = _require_xt().get_trading_dates(market, start_time=EPOCH, end_time=end_time)
    utc_datetime = pd.Series(days, dtype="datetime64[ms]").dt.tz_localize("UTC")
    return utc_datetime.dt.tz_convert("Asia/Shanghai").dt.date.values  # type: ignore


def get_security_info(symbol: str) -> Tuple[str, datetime.date, str]:
    """获取证券详细信息, 即别名、IPO日

    Args:
        symbol: 证券品种代码
    Returns:
        证券显示名、IPO日和类型。其它信息忽略掉。
    """
    item = _require_xt().get_instrument_detail(symbol)
    if item is None:
        raise ValueError(f"invalid symbol: {symbol}")

    code, exchange = symbol.split(".")
    _type = SecurityType.UNKNOWN

    if exchange == "SH":
        if code.startswith("6"):
            _type = SecurityType.STOCK
        elif code.startswith("00"):
            _type = SecurityType.INDEX
    elif exchange == "SZ":
        if code[:3] == "399":
            _type = SecurityType.INDEX
        if code[:2] in ("00", "30"):
            _type = SecurityType.STOCK

    return item["InstrumentName"], arrow.get(item["OpenDate"]).date(), _type.value


@cache
def get_factor_ratio(
    symbol: str, start: datetime.date, end: datetime.date
) -> pd.Series:
    """获取`symbol`在`start`到`end`期间的复权因子

    复权因子以EPOCH日为1，依次向后增加。返回值取整个复权因子区间中[start, end]这一段。

    Args:
        symbol: 个股代码，以.SZ/.SH等结尾
        start: 起始日期，不得早于EPOCH
        end: 结束日期，不得晚于当前时间

    Returns:
        以日期为index的Series
    """
    if start < tf.int2date(EPOCH):
        raise ValueError(f"start date should not be earlier than {EPOCH}: {start}")

    start_ = tf.date2int(start)
    end_ = tf.date2int(end)
    df = _require_xt().get_divid_factors(symbol, EPOCH)

    df.index = df.index.astype(int)
    frames = pd.DataFrame([], index=tf.day_frames)
    factor = pd.concat([frames, df["dr"]], axis=1)
    factor.sort_index(inplace=True)
    factor.fillna(1, inplace=True)

    query = f"index >= {start_} and index <= {end_}"
    return factor.cumprod().query(query)["dr"]
