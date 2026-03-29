"""K线数据查询 API"""

import datetime

import polars as pl
from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import JSONResponse

from quantide.data.models.index_bars import index_bars
from quantide.data.models.daily_bars import daily_bars
from quantide.data.sqlite import db
from quantide.data.utils.resampler import Resampler

app, rt = fast_app()


def _feature_removed(message: str) -> JSONResponse:
    """返回已下线功能响应。"""
    return JSONResponse({"code": 410, "message": message}, status_code=410)


def bars_to_list(df) -> list[dict]:
    """将 DataFrame 转换为列表"""
    if df.is_empty():
        return []

    result = []
    for row in df.iter_rows(named=True):
        result.append({
            "dt": row["frame"].isoformat() if isinstance(row["frame"], (datetime.date, datetime.datetime)) else row["frame"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "amount": float(row["amount"]),
        })
    return result


def add_ma_to_list(df, ma_periods: list[int]) -> list[dict]:
    """将 DataFrame 转换为列表，包含均线数据"""
    if df.is_empty():
        return []

    result = []
    for row in df.iter_rows(named=True):
        item = {
            "dt": row["frame"].isoformat() if isinstance(row["frame"], (datetime.date, datetime.datetime)) else row["frame"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "amount": float(row["amount"]),
        }

        # 添加均线数据
        for period in ma_periods:
            ma_key = f"ma{period}"
            if ma_key in row and row[ma_key] is not None:
                item[ma_key] = float(row[ma_key])

        result.append(item)

    return result


def _get_stock_bars(
    symbol: str,
    start: datetime.date,
    end: datetime.date,
    freq: str = "day",
) -> pl.DataFrame:
    """从 DailyBars 获取个股行情数据"""
    df = daily_bars.get_bars_in_range(
        assets=[symbol],
        start=start,
        end=end,
        adjust="qfq",
        eager_mode=True,
    )

    if df.is_empty():
        return pl.DataFrame(schema={
            "frame": pl.Date,
            "asset": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
        })

    # 重命名列
    df = df.rename({"date": "frame", "asset": "asset"})

    # 重采样
    if freq != "day":
        df = Resampler.resample(df, freq)

    return df


def _get_index_bars(
    symbol: str,
    start: datetime.date,
    end: datetime.date,
    freq: str = "day",
) -> pl.DataFrame:
    """从 IndexBars 获取指数行情数据。"""
    df = index_bars.get_bars_in_range(
        start=start,
        end=end,
        symbols=[symbol],
        eager_mode=True,
    )

    if df.is_empty():
        return pl.DataFrame(schema={
            "frame": pl.Date,
            "asset": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
        })

    df = df.rename({"date": "frame", "asset": "asset"})

    if freq != "day":
        df = Resampler.resample(df, freq)

    return df


def _get_bars_with_ma(
    symbol: str,
    start: datetime.date,
    end: datetime.date,
    freq: str = "day",
    ma_periods: list[int] | None = None,
) -> pl.DataFrame:
    """获取行情数据并计算均线"""
    df = _get_stock_bars(symbol, start, end, freq)

    if df.is_empty() or not ma_periods:
        return df

    # 计算均线
    for period in ma_periods:
        df = df.with_columns(
            pl.col("close").rolling_mean(window_size=period).alias(f"ma{period}")
        )

    return df


@rt("/stock/{symbol}")
async def get_stock_kline(
    request: Request,
    symbol: str,
    start: str | None = None,
    end: str | None = None,
    freq: str = "day",
    ma: str | None = None,
):
    """获取个股K线数据"""
    # 解析日期
    try:
        if start:
            start_date = datetime.date.fromisoformat(start)
        else:
            start_date = datetime.date.today() - datetime.timedelta(days=365)

        if end:
            end_date = datetime.date.fromisoformat(end)
        else:
            end_date = datetime.date.today()
    except ValueError:
        return JSONResponse(
            {"code": 400, "message": "Invalid date format, use YYYY-MM-DD"},
            status_code=400,
        )

    # 解析均线周期
    ma_periods = []
    if ma:
        try:
            ma_periods = [int(x.strip()) for x in ma.split(",") if x.strip()]
        except ValueError:
            return JSONResponse(
                {"code": 400, "message": "Invalid ma format"},
                status_code=400,
            )

    # 验证 freq
    if freq not in ("day", "week", "month"):
        return JSONResponse(
            {"code": 400, "message": "Invalid freq, use day/week/month"},
            status_code=400,
        )

    try:
        if ma_periods:
            df = _get_bars_with_ma(symbol, start_date, end_date, freq, ma_periods)
            data = add_ma_to_list(df, ma_periods)
        else:
            df = _get_stock_bars(symbol, start_date, end_date, freq)
            data = bars_to_list(df)

        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": {
                "symbol": symbol,
                "freq": freq,
                "items": data,
            },
        })

    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to get kline: {e}"},
            status_code=500,
        )


@rt("/sector/{sector_id}")
async def get_sector_kline(
    request: Request,
    sector_id: str,
    start: str | None = None,
    end: str | None = None,
    freq: str = "day",
    ma: str | None = None,
):
    """获取板块K线数据"""
    _ = (request, sector_id, start, end, freq, ma)
    return _feature_removed("sector kline has been retired from the subject app")


@rt("/index/{symbol}")
async def get_index_kline(
    request: Request,
    symbol: str,
    start: str | None = None,
    end: str | None = None,
    freq: str = "day",
    ma: str | None = None,
):
    """获取指数K线数据"""
    # 解析日期
    try:
        if start:
            start_date = datetime.date.fromisoformat(start)
        else:
            start_date = datetime.date.today() - datetime.timedelta(days=365)

        if end:
            end_date = datetime.date.fromisoformat(end)
        else:
            end_date = datetime.date.today()
    except ValueError:
        return JSONResponse(
            {"code": 400, "message": "Invalid date format, use YYYY-MM-DD"},
            status_code=400,
        )

    # 解析均线周期
    ma_periods = []
    if ma:
        try:
            ma_periods = [int(x.strip()) for x in ma.split(",") if x.strip()]
        except ValueError:
            return JSONResponse(
                {"code": 400, "message": "Invalid ma format"},
                status_code=400,
            )

    # 验证 freq
    if freq not in ("day", "week", "month"):
        return JSONResponse(
            {"code": 400, "message": "Invalid freq, use day/week/month"},
            status_code=400,
        )

    try:
        if ma_periods:
            df = _get_index_bars(symbol, start_date, end_date, freq)
            # 计算均线
            for period in ma_periods:
                df = df.with_columns(
                    pl.col("close").rolling_mean(window_size=period).alias(f"ma{period}")
                )
            data = add_ma_to_list(df, ma_periods)
        else:
            df = _get_index_bars(symbol, start_date, end_date, freq)
            data = bars_to_list(df)

        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": {
                "symbol": symbol,
                "freq": freq,
                "items": data,
            },
        })

    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to get kline: {e}"},
            status_code=500,
        )


@rt("/compare")
async def compare_kline(
    request: Request,
    symbol: str,
    compare: str,
    start: str | None = None,
    end: str | None = None,
    freq: str = "day",
):
    """对比两个标的的K线数据"""
    # 解析日期
    try:
        if start:
            start_date = datetime.date.fromisoformat(start)
        else:
            start_date = datetime.date.today() - datetime.timedelta(days=365)

        if end:
            end_date = datetime.date.fromisoformat(end)
        else:
            end_date = datetime.date.today()
    except ValueError:
        return JSONResponse(
            {"code": 400, "message": "Invalid date format, use YYYY-MM-DD"},
            status_code=400,
        )

    # 验证 freq
    if freq not in ("day", "week", "month"):
        return JSONResponse(
            {"code": 400, "message": "Invalid freq, use day/week/month"},
            status_code=400,
        )

    try:
        # 获取主标的K线
        df1 = _get_stock_bars(symbol, start_date, end_date, freq)
        data1 = bars_to_list(df1)

        # 获取对比标的K线
        df2 = _get_stock_bars(compare, start_date, end_date, freq)
        data2 = bars_to_list(df2)

        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": {
                "symbol": symbol,
                "compare": compare,
                "freq": freq,
                "primary": data1,
                "compare_data": data2,
            },
        })

    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to get kline: {e}"},
            status_code=500,
        )
