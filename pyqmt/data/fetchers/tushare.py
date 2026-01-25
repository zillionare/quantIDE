import datetime
from collections.abc import Iterable
from functools import wraps
from itertools import chain
from typing import Any, Callable, Protocol

import pandas as pd
import polars as pl
import tushare as ts
from loguru import logger


class MessageHubStub:
    """消息机制存根。提供此方法是因为本模块的功能往往是在后台、长时间执行的。它们需要一种消息报告机制，但又不希望本模块尽可能独立，减少依赖：如果不需要及时报告消息，可以简单地忽略掉，主体功能不受任何影响。

    如果要使用消息机制，请在应用初始化时调用 mount，以实现代理。

    在调用 mount 时，传入的 hub 对象必须提供 publish 接口
    """

    proxy: Any = None

    @classmethod
    def mount(cls, hub: Any) -> None:
        cls.proxy = hub

    @classmethod
    def publish(cls, msg_type: str, content: Any) -> None:
        """消息发布接口"""
        if cls.proxy:
            cls.proxy.publish(msg_type, content)


class Paginable(Protocol):
    """分页回调函数接口"""

    def __call__(
        self, *args: Any, limit: int, offset: int = 0, **kwargs: Any
    ) -> pd.DataFrame | None: ...

    def __name__(self): ...


def paginate(func: Paginable) -> Callable:
    """装饰器：自动处理需要limit和offset参数的API调用

    该装饰器会自动循环调用API直到获取所有数据

    Args:
        func (Callable): 被装饰的函数，要求返回 DataFrame|None

    Returns:
        Callable: 装饰后的函数
    """

    @wraps(func)
    def wrapper(*args, limit: int = 0, **kwargs) -> pd.DataFrame:
        """
        包装函数

        Args:
            *args: 位置参数
            limit (int): 每页数据量
            offset (int): 数据偏移量
            **kwargs: 关键字参数

        Returns:
            List[Any]: 所有数据的列表
        """
        assert limit > 0, "limit must be greater than 0"

        all_data = []
        current_offset = 0

        while True:
            result = func(*args, limit=limit, offset=current_offset, **kwargs)
            all_data.append(result)

            if result is None:
                logger.warning(
                    "{}获取数据时返回None，当成结束处理。偏移: {}",
                    func.__name__,
                    current_offset,
                )
                break

            # 如果返回数据量不足limit，说明已获取完所有数据
            if len(result) < limit:
                logger.info(
                    "{}已获取完所有数据，偏移: {}, size: {}",
                    func.__name__,
                    current_offset,
                    len(result),
                )
                break

            current_offset += limit

        if len(all_data) > 0:
            return pd.concat(all_data)

        return pd.DataFrame()

    return wrapper


def _fetch_by_dates(
    func_name: str,
    dates: Iterable[datetime.date] | datetime.date,
    *args,
    fields: str | None = None,
    rename_as: dict[str, str] | None = None,
    **kwargs,
) -> tuple[pd.DataFrame, list[list]]:
    """通用 fetch 函数。

    Tushare 许多方法都是通过 trade_date 为主要参数，以获得该交易日某一类型全部数据。本方法在这些方法的基础上，提供多日数据的聚合，排序、字段重命名、错误处理等功能，对日期字段转换为 datetime.date 等。

    Args:
        func_name: tushare 方法名
        dates: 日期，可以是单个日期，也可以是日期列表。
        fields: tushare 返回字段。它的顺序也将成为返回 dataframe中的列序。
        rename_as: 如果不为 None，则将据此重命名字段。如果为 None，则固定将 ts_code/trade_date 重命名为 asset/date。如果不希望发生重命名，请传入{}
    """
    all_data = []
    errors = []

    if isinstance(dates, Iterable):
        _dates = list(dates)
    else:
        _dates = [dates]

    _rename_as = (
        {"ts_code": "asset", "trade_date": "date"} if rename_as is None else rename_as
    )

    pro = ts.pro_api()
    func = getattr(pro, func_name)

    for i, date in enumerate(sorted(_dates)):  # type: ignore
        if (i + 1) % 3 == 0:
            logger.info(f"进度{i+1}/{len(_dates)}，当前日期{date}")
            MessageHubStub.publish(
                "fetch_data_progress",
                {
                    "progress": (i + 1) / (len(_dates) + 1),
                    "msg": f"当前正在处理: {date}",
                },
            )

        str_date = date.strftime("%Y%m%d")
        try:
            df = func(trade_date=str_date, fields=fields)
        except Exception as e:
            logger.error("调用 {} 时出错, {}", func_name, e)
            errors.append([func_name, date, f"调用{func_name}时出现异常"])
            continue

        if df is None or df.empty:
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

    pro = ts.pro_api()

    # 获取交易日历数据
    df = pro.trade_cal(exchange="SSE", start_date=epoch.strftime("%Y%m%d"))

    if df is None or df.empty:
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
        if df is not None and not df.empty:
            all_data.append(df)

    if len(all_data) == 0:
        logger.warning(f"在 {start} 到 {end} 之间，没有获取到任何财务审计意见数据")
        return None

    df = pd.concat(all_data, ignore_index=True)
    df = df.rename(columns={"end_date": "date", "ts_code": "asset"})

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").astype("datetime64[ms]")

    return df


def fetch_dv_ttm(start: datetime.date, end: datetime.date) -> pd.DataFrame:
    """从tushare获取股息率数据

    Args:
        start: 开始日期
        end: 结束日期

    Returns:
        包含股息率等数据的DataFrame
    """
    pro = ts.pro_api()
    cols = "ts_code,trade_date,dv_ttm,total_mv,turnover_rate,pe_ttm"
    dfs = []
    for dt in pd.bdate_range(start, end):
        dtstr = dt.strftime("%Y%m%d")
        df = pro.daily_basic(trade_date=dtstr, fields=cols)
        if df is not None and not df.empty:
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
    pro = ts.pro_api()
    dfs = []
    for status in ("L", "D", "P"):
        df = pro.stock_basic(
            list_status=status,
            exchange="",
            fields="ts_code,name,cnspell,list_date,delist_date",
        )
        if df is not None and not df.empty:
            dfs.append(df)

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

    return _fetch_by_dates("daily", dates, fields=fields, rename_as=rename_as)


def fetch_limit_price(
    dates: datetime.date | Iterable[datetime.date],
) -> tuple[pd.DataFrame, list[list]]:
    """获取指定交易日的全市场个股涨跌停价

    Args:
        dates (datetime.date | list[datetime.date]): 日期

    Returns:
        tuple[pd.DataFrame, list[list]]: 涨跌停价数据，错误信息
    """
    fields = "ts_code,trade_date,up_limit,down_limit"

    cutoff = datetime.date(2007, 1, 1)
    _dates = dates if isinstance(dates, Iterable) else [dates]
    sorted_dates = sorted(_dates)

    # if all dates < cutoff or empty input, short-circuit with typed empty DataFrame
    if len(sorted_dates) == 0 or sorted_dates[-1] < cutoff:
        empty = pd.DataFrame(
            {
                "asset": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ms]"),
                "up_limit": pd.Series(dtype="float64"),
                "down_limit": pd.Series(dtype="float64"),
            }
        )
        return empty, []

    # only fetch valid dates to avoid noise/errors
    valid_dates = [d for d in sorted_dates if d >= cutoff]
    df, errors = _fetch_by_dates("stk_limit", valid_dates, fields=fields)
    if df is None:
        # build typed empty DataFrame when upstream returns None
        df = pd.DataFrame(
            {
                "asset": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ms]"),
                "up_limit": pd.Series(dtype="float64"),
                "down_limit": pd.Series(dtype="float64"),
            }
        )

    # ensure numeric dtype even when DataFrame is empty
    for col in ("up_limit", "down_limit"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].astype("float64")
        else:
            df[col] = pd.Series(dtype="float64")

    return df, errors


def fetch_st_info(
    dates: Iterable[datetime.date] | datetime.date,
) -> tuple[pd.DataFrame, list[list]]:
    """获取ST股票信息

    在 tushare 中，要通过名字变更 API 来获取股票的 ST 信息
    """
    fields = "ts_code,trade_date"
    # st信息在tushare从2016-01-01起才有数据
    cutoff = datetime.date(2016, 1, 1)
    _dates = dates if isinstance(dates, Iterable) else [dates]
    sorted_dates = sorted(_dates)

    # 全部早于cutoff，直接返回类型正确的空表
    if len(sorted_dates) == 0 or sorted_dates[-1] < cutoff:
        empty = pd.DataFrame(
            {
                "asset": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ms]"),
                "st": pd.Series(dtype="boolean"),
            }
        )
        return empty, []

    # 仅获取 >= cutoff 的日期，避免产生不必要的错误日志
    valid_dates = [d for d in sorted_dates if d >= cutoff]
    df, errors = _fetch_by_dates("stock_st", valid_dates, fields=fields)
    if df is None:
        df = pd.DataFrame(
            {
                "asset": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ms]"),
                "st": pd.Series(dtype="boolean"),
            }
        )
        return df, errors

    if df.empty:
        df["st"] = pd.Series(dtype="boolean")
    else:
        # 接口仅返回当天为 ST 的资产，将这些行标记为 True
        df["st"] = True

    return df, errors


def fetch_bars_ext(
    dates: Iterable[datetime.date] | datetime.date,
) -> tuple[pl.LazyFrame, list[list]]:
    """获取日线行情、ST 和涨跌停价

    返回的 dataframe 将包含以下字段：
        date, asset, open,high,low,close,volume,amount,adjust,st,up_limit,down_limit
    Args:
        dates (list[datetime.date] | datetime.date): 交易日

    Returns:
        tuple[pd.DataFrame, list[list]]: 行情数据和错误信息
    """
    MessageHubStub.publish("fetch_data_progress", {"msg": "正在获取日线数据..."})
    bars, errors1 = fetch_bars(dates)

    MessageHubStub.publish("fetch_data_progress", {"msg": "正在获取复权因子..."})
    adjust, errors2 = fetch_adjust_factor(dates)

    MessageHubStub.publish(
        "fetch_data_progress", {"msg": "正在获取 ST（特别处理） 信息..."}
    )
    st, errors3 = fetch_st_info(dates)

    MessageHubStub.publish(
        "fetch_data_progress", {"msg": "正在获取每日涨跌停限价信息..."}
    )
    limit, errors4 = fetch_limit_price(dates)

    errors = []

    if bars.empty:
        if isinstance(dates, Iterable):
            _dates = dates
        else:
            _dates = [dates]
        errors = [["fetch_bars_ext", date, "emtpy bars"] for date in _dates]
        empty = pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Datetime("ms")),
                "asset": pl.Series([], dtype=pl.Utf8),
                "open": pl.Series([], dtype=pl.Float64),
                "high": pl.Series([], dtype=pl.Float64),
                "low": pl.Series([], dtype=pl.Float64),
                "close": pl.Series([], dtype=pl.Float64),
                "volume": pl.Series([], dtype=pl.Float64),
                "amount": pl.Series([], dtype=pl.Float64),
                "adjust": pl.Series([], dtype=pl.Float64),
                "st": pl.Series([], dtype=pl.Boolean),
                "up_limit": pl.Series([], dtype=pl.Float64),
                "down_limit": pl.Series([], dtype=pl.Float64),
            }
        ).lazy()
        return empty, errors

    for error in chain(errors1, errors2, errors3, errors4):
        error[0] = "fetch_bars_ext"
        errors.append(error)

    # use polars for performance
    bars_pl = pl.from_pandas(bars).lazy()
    st_pl = pl.from_pandas(st).lazy()
    adjust_pl = pl.from_pandas(adjust).lazy()
    limit_pl = pl.from_pandas(limit).lazy()

    lf = (
        bars_pl.join(adjust_pl, on=["date", "asset"], how="left")
        .join(st_pl, on=["date", "asset"], how="left")
        .join(limit_pl, on=["date", "asset"], how="left")
        .with_columns(pl.col("st").fill_null(False).cast(pl.Boolean))
    )

    return lf, errors
