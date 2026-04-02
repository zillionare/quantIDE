"""Tushare 数据获取

提供从 Tushare 获取股票数据的功能。
"""

import datetime
import itertools
from typing import Callable, Iterable, Iterator, Optional

import pandas as pd
import tushare as ts
from loguru import logger

from quantide.config.settings import get_tushare_token
from quantide.core.message import msg_hub


def _ensure_tushare_token() -> None:
    token = str(get_tushare_token() or "").strip()
    if token:
        ts.set_token(token)


class TushareFetcher:
    """Tushare 数据获取器"""

    def __init__(self):
        _ensure_tushare_token()
        self.pro = ts.pro_api()

    def fetch_calendar(self, epoch: datetime.date) -> pd.DataFrame:
        """从tushare获取交易日历，并保存到SQLite数据库

        Returns:
            包含交易日历的DataFrame
        """
        logger.info(f"获取从 {epoch} 起的交易日历")

        # 获取交易日历数据
        df = self.pro.trade_cal(exchange="SSE", start_date=epoch.strftime("%Y%m%d"))

        if df is None or len(df) == 0:
            logger.warning("没有获取到交易日历数据")
            return pd.DataFrame()

        # 转换日期格式
        df["date"] = pd.to_datetime(df["cal_date"], format="%Y%m%d").dt.date
        df["prev"] = pd.to_datetime(df["pretrade_date"], format="%Y%m%d").dt.date

        df = df.sort_values("date").set_index("date")
        return df[["is_open", "prev"]]

    def fetch_stock_list(self) -> pd.DataFrame | None:
        """
        获取股票列表

        Returns:
            pd.DataFrame: 股票列表数据
        """
        dfs = []
        for status in ("L", "D", "P"):
            df = self.pro.stock_basic(
                list_status=status,
                exchange="",
                fields="ts_code,name,cnspell,list_date,delist_date",
            )
            if df is not None and len(df) > 0:
                dfs.append(df)

        if not dfs:
            logger.warning("未获取到股票列表数据")
            return None

        df = pd.concat(dfs)
        cols = ["asset", "name", "pinyin", "list_date", "delist_date"]

        logger.info(f"获取股票列表成功，共 {len(df)} 条记录")
        df = df.rename(
            columns={"ts_code": "asset", "cnspell": "pinyin", "list_date": "list_date"}
        )

        df["pinyin"] = df["pinyin"].str.upper()
        df["list_date"] = pd.to_datetime(df["list_date"]).dt.date
        df["delist_date"] = pd.to_datetime(df["delist_date"]).dt.date

        return df[cols]


class TushareDataFetcher:
    """符合标准端口的数据源适配器."""

    def fetch_calendar(self, epoch: datetime.date) -> pd.DataFrame:
        return fetch_calendar(epoch)

    def fetch_stock_list(self) -> pd.DataFrame | None:
        return fetch_stock_list()

    def fetch_adjust_factor(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        return fetch_adjust_factor(dates)

    def fetch_bars(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        return fetch_bars(dates)

    def fetch_limit_price(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        return fetch_limit_price(dates)

    def fetch_st_info(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        return fetch_st_info(dates)

    def fetch_bars_ext(
        self,
        dates: Iterable[datetime.date] | datetime.date,
        phase_callback: Callable[[str], None] | None = None,
    ) -> tuple[pd.DataFrame, list[list]]:
        return fetch_bars_ext(dates, phase_callback=phase_callback)


def _fetch_by_dates(
    func_name: str, dates: Iterable[datetime.date] | datetime.date, **kwargs
) -> tuple[pd.DataFrame, list[list]]:
    """按日期获取数据

    Args:
        func_name: API函数名称
        dates: 日期列表或单个日期
        **kwargs: 传递给API的参数

    Returns:
        数据和错误信息
    """
    _ensure_tushare_token()
    pro = ts.pro_api()
    func = getattr(pro, func_name)

    if isinstance(dates, datetime.date):
        dates = [dates]

    fields = kwargs.pop("fields", None)
    _rename_as = kwargs.pop("rename_as", {})
    all_data = []
    errors = []

    for date in dates:
        str_date = date.strftime("%Y%m%d")
        try:
            df = func(trade_date=str_date, fields=fields, **kwargs)
        except Exception as e:
            logger.error("调用 {} 时出错, {}", func_name, e)
            errors.append([func_name, date, f"调用{func_name}时出现异常"])
            continue

        if df is None or len(df) == 0:
            all_data.append(df)
            error_msg = f"{func_name}获取{date}日数据失败"
            logger.warning(error_msg)
            errors.append([func_name, date, error_msg])
        else:
            all_data.append(df)

    if len(all_data) == 0:
        return pd.DataFrame(), errors

    result = pd.concat(all_data, ignore_index=True)
    if fields:
        columns = map(lambda x: x.strip(), fields.split(","))
        result = result[columns]

    if len(_rename_as) != 0:
        result = result.rename(columns=_rename_as)

    result["date"] = pd.to_datetime(result["date"], format="%Y%m%d").astype(
        "datetime64[ms]"
    )
    result = result.sort_values(by="date")

    return result, errors


def fetch_calendar(epoch: datetime.date) -> pd.DataFrame:
    """从tushare获取交易日历，并保存到SQLite数据库

    Returns:
        包含交易日历的DataFrame
    """
    logger.info(f"获取从 {epoch} 起的交易日历")

    _ensure_tushare_token()
    pro = ts.pro_api()

    # 获取交易日历数据
    df = pro.trade_cal(exchange="SSE", start_date=epoch.strftime("%Y%m%d"))

    if df is None or len(df) == 0:
        logger.warning("没有获取到交易日历数据")
        return pd.DataFrame()

    # 转换日期格式
    df["date"] = pd.to_datetime(df["cal_date"], format="%Y%m%d").dt.date
    df["prev"] = pd.to_datetime(df["pretrade_date"], format="%Y%m%d").dt.date

    df = df.sort_values("date").set_index("date")
    return df[["is_open", "prev"]]


def fetch_fina_audit(start: datetime.date, end: datetime.date) -> pd.DataFrame | None:
    """
    通过 tushare 接口，获取财务审计意见数据。

    Args:
        start: 开始日期 (基于公告日期 ann_date)
        end: 结束日期 (基于公告日期 ann_date)

    Returns:
        DataFrame: 包含审计意见等数据的DataFrame，如果无数据则返回None
    """
    _ensure_tushare_token()
    pro = ts.pro_api()
    all_data = []

    df = pro.daily_basic()
    if df is None:
        logger.warning(f"在 {start} 到 {end} 之间，没有获取到任何财务审计意见数据")
        return None

    securities = df["ts_code"].tolist()

    for sec in securities:
        df = pro.fina_audit(
            ts_code=sec,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        if df is not None and len(df) > 0:
            all_data.append(df)

    if len(all_data) == 0:
        logger.warning(f"在 {start} 到 {end} 之间，没有获取到任何财务审计意见数据")
        return None

    df = pd.concat(all_data, ignore_index=True)
    df = df.rename(columns={"end_date": "date", "ts_code": "asset"})

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").astype("datetime64[ms]")
    return df


def fetch_dividend(start: datetime.date, end: datetime.date) -> pd.DataFrame:
    """
    通过 tushare 接口，获取分红送股数据。

    Args:
        start: 开始日期 (基于除权除息日期 ex_date)
        end: 结束日期 (基于除权除息日期 ex_date)

    Returns:
        包含股息率等数据的DataFrame
    """
    _ensure_tushare_token()
    pro = ts.pro_api()
    cols = "ts_code,trade_date,dv_ttm,total_mv,turnover_rate,pe_ttm"
    dfs = []
    for dt in pd.bdate_range(start, end):
        dtstr = dt.strftime("%Y%m%d")
        df = pro.daily_basic(trade_date=dtstr, fields=cols)
        if df is not None and len(df) > 0:
            dfs.append(df)

    # 如果没有获取到任何数据，返回空的DataFrame
    if not dfs:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "trade_date",
                "dv_ttm",
                "total_mv",
                "turnover_rate",
                "pe_ttm",
            ]
        )

    df = pd.concat(dfs)
    df = df.rename(columns={"trade_date": "date", "ts_code": "asset"})

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", unit="ms")

    return df


def fetch_stock_list() -> pd.DataFrame | None:
    """
    获取股票列表

    Returns:
        pd.DataFrame: 股票列表数据
    """
    _ensure_tushare_token()
    pro = ts.pro_api()
    dfs = []
    for status in ("L", "D", "P"):
        df = pro.stock_basic(
            list_status=status,
            exchange="",
            fields="ts_code,name,cnspell,list_date,delist_date",
        )
        if df is not None and len(df) > 0:
            dfs.append(df)

    if not dfs:
        logger.warning("未获取到股票列表数据")
        return None

    df = pd.concat(dfs)
    cols = ["asset", "name", "pinyin", "list_date", "delist_date"]

    logger.info(f"获取股票列表成功，共 {len(df)} 条记录")
    df = df.rename(
        columns={"ts_code": "asset", "cnspell": "pinyin", "list_date": "list_date"}
    )

    df["pinyin"] = df["pinyin"].str.upper()
    df["list_date"] = pd.to_datetime(df["list_date"]).dt.date
    df["delist_date"] = pd.to_datetime(df["delist_date"]).dt.date

    return df[cols]


def fetch_adjust_factor(
    dates: Iterable[datetime.date] | datetime.date,
) -> tuple[pd.DataFrame, list[list]]:
    """获取指定交易日的复权因子

    因子以 adjust 字段返回
    """
    rename_as = {
        "trade_date": "date",
        "adj_factor": "adjust",
        "ts_code": "asset",
    }
    return _fetch_by_dates("adj_factor", dates, rename_as=rename_as)


def fetch_bars(
    dates: Iterable[datetime.date] | datetime.date,
) -> tuple[pd.DataFrame, list[list]]:
    """通过 tushare 接口，获取日线行情数据

    返回数据未复权，但包含了复权因子，因此可以增量获取叠加。返回数据为升序。

    Args:
        dates: 需要获取的交易日列表，允许不连续

    Returns:
        DataFrame: 包含date, asset, open,high,low,close,volume,amount
        Error: date, msg
    """
    fields = "trade_date,ts_code,open,high,low,close,vol,amount"
    rename_as = {"ts_code": "asset", "trade_date": "date", "vol": "volume"}

    # df = pd.merge(df, adj_factor, on=["ts_code", "trade_date"], how="inner")
    df, errors = _fetch_by_dates("daily", dates, fields=fields, rename_as=rename_as)
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("float64")
    return df, errors


def _empty_st_info_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "asset": pd.Series(dtype="object"),
            "date": pd.Series(dtype="datetime64[ms]"),
            "is_st": pd.Series(dtype="boolean"),
        }
    )


def fetch_limit_price(
    dates: Iterable[datetime.date] | datetime.date,
) -> tuple[pd.DataFrame, list[list]]:
    """通过 tushare 接口，获取涨跌停价格数据

    Args:
        dates: 需要获取的交易日列表，允许不连续

    Returns:
        DataFrame: 包含date, asset, up_limit, down_limit
        Error: date, msg
    """
    if isinstance(dates, datetime.date):
        dates = [dates]

    dates = list(dates)
    valid_dates = [date for date in dates if date >= datetime.date(2007, 1, 1)]
    if not valid_dates:
        return (
            pd.DataFrame(
                {
                    "asset": pd.Series(dtype="object"),
                    "date": pd.Series(dtype="datetime64[ms]"),
                    "up_limit": pd.Series(dtype="float64"),
                    "down_limit": pd.Series(dtype="float64"),
                }
            ),
            [],
        )

    fields = "trade_date,ts_code,up_limit,down_limit"
    rename_as = {"ts_code": "asset", "trade_date": "date"}
    df, errors = _fetch_by_dates("stk_limit", valid_dates, fields=fields, rename_as=rename_as)
    if df.empty:
        return df, errors
    return df[["asset", "date", "up_limit", "down_limit"]], errors


def fetch_st_info(
    dates: Iterable[datetime.date] | datetime.date,
) -> tuple[pd.DataFrame, list[list]]:
    """通过 tushare 接口，获取 ST 股票数据

    Args:
        dates: 需要获取的交易日列表，允许不连续

    Returns:
        DataFrame: 包含date, asset, is_st
        Error: date, msg
    """
    if isinstance(dates, datetime.date):
        dates = [dates]

    dates = list(dates)
    if not dates:
        return _empty_st_info_frame(), []

    valid_dates = [date for date in dates if date >= datetime.date(2016, 1, 1)]
    if not valid_dates:
        return _empty_st_info_frame(), []

    _ensure_tushare_token()
    pro = ts.pro_api()

    all_data = []
    errors = []

    for date in valid_dates:
        str_date = date.strftime("%Y%m%d")
        try:
            df = pro.stock_st(
                trade_date=str_date,
                fields="trade_date,ts_code,name,type,type_name",
            )
        except Exception as e:
            logger.error("调用 stock_st 时出错, {}", e)
            errors.append(["stock_st", date, f"调用stock_st时出现异常: {e}"])
            all_data.append(pd.DataFrame())
            continue

        if df is None or len(df) == 0:
            all_data.append(pd.DataFrame())
            error_msg = f"stock_st获取{date}日数据失败"
            logger.warning(error_msg)
            errors.append(["stock_st", date, error_msg])
        else:
            all_data.append(df)

    if len(all_data) == 0:
        return _empty_st_info_frame(), errors

    df = pd.concat(all_data, ignore_index=True)
    df = df.rename(columns={"ts_code": "asset", "trade_date": "date"})

    if len(df) == 0:
        df = _empty_st_info_frame()
    else:
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").astype(
            "datetime64[ms]"
        )
        df = df.sort_values(by=["date", "asset"]).reset_index(drop=True)
        # 接口仅返回当天为 ST 的资产，将这些行标记为 True
        df["is_st"] = pd.Series([True] * len(df), dtype="boolean")
        df = df[["asset", "date", "is_st"]]

    return df, errors


def fetch_bars_ext(
    dates: Iterable[datetime.date] | datetime.date,
    phase_callback: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, list[list]]:
    """获取日线行情、ST 和涨跌停价

    返回的 dataframe 将包含以下字段：
        date, asset, open,high,low,close,volume,amount,adjust,is_st,up_limit,down_limit
    Args:
        dates (list[datetime.date] | datetime.date): 交易日

    Returns:
        tuple[pd.DataFrame, list[list]]: 行情数据和错误信息
    """
    if phase_callback:
        phase_callback("bars")
    msg_hub.publish("fetch_data_progress", {"msg": "正在获取日线数据..."})
    bars, errors1 = fetch_bars(dates)

    if phase_callback:
        phase_callback("adjust")
    msg_hub.publish("fetch_data_progress", {"msg": "正在获取复权因子..."})
    adjust, errors2 = fetch_adjust_factor(dates)

    if phase_callback:
        phase_callback("limit")
    msg_hub.publish(
        "fetch_data_progress", {"msg": "正在获取每日涨跌停限价信息..."}
    )
    limit, errors3 = fetch_limit_price(dates)

    if phase_callback:
        phase_callback("st")
    msg_hub.publish(
        "fetch_data_progress", {"msg": "正在获取 ST（特别处理） 信息..."}
    )
    st, errors4 = fetch_st_info(dates)

    errors = []
    errors.extend(errors1)
    errors.extend(errors2)
    errors.extend(errors3)
    errors.extend(errors4)

    if len(bars) == 0:
        if isinstance(dates, Iterable):
            _dates = dates
        else:
            _dates = [dates]

        # 构造一个空的 DataFrame，包含所有需要的列
        df = pd.DataFrame(
            {
                "date": pd.Series([], dtype="datetime64[ms]"),
                "asset": pd.Series([], dtype="object"),
                "open": pd.Series([], dtype="float64"),
                "high": pd.Series([], dtype="float64"),
                "low": pd.Series([], dtype="float64"),
                "close": pd.Series([], dtype="float64"),
                "volume": pd.Series([], dtype="float64"),
                "amount": pd.Series([], dtype="float64"),
                "adjust": pd.Series([], dtype="float64"),
                "is_st": pd.Series([], dtype="boolean"),
                "up_limit": pd.Series([], dtype="float64"),
                "down_limit": pd.Series([], dtype="float64"),
            }
        )
        return df, errors

    # 合并数据
    df = bars.merge(adjust, on=["date", "asset"], how="left")
    df = df.merge(st, on=["date", "asset"], how="left")
    df = df.merge(limit, on=["date", "asset"], how="left")

    # 填充缺失值
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("float64")
    df["adjust"] = df["adjust"].fillna(1.0)  # 复权因子默认1
    df["is_st"] = df["is_st"].fillna(False)  # ST默认False
    df["up_limit"] = df["up_limit"].fillna(0.0)  # 涨跌停价默认0
    df["down_limit"] = df["down_limit"].fillna(0.0)

    return df, errors


def get_sector_info():
    """获取板块信息"""
    pro = ts.pro_api()
    df = pro.index_basic(
        market="SSE",
        publisher="SW",
        category="SW",
        fields="ts_code,name,category",
    )
    return df


def get_industry_info():
    """获取行业信息"""
    pro = ts.pro_api()
    df = pro.index_classify(src="SW")
    return df
