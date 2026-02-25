"""K线数据查询 API"""

import datetime

from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import JSONResponse

from pyqmt.data.dal.bar_dal import BarDAL
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.sqlite import db

app, rt = fast_app()


def get_bar_dal() -> BarDAL:
    """获取 BarDAL 实例"""
    return BarDAL(db, daily_bars.store)


def bars_to_list(df) -> list[dict]:
    """将 DataFrame 转换为列表"""
    if df.is_empty():
        return []

    result = []
    for row in df.iter_rows(named=True):
        result.append({
            "dt": row["dt"].isoformat() if isinstance(row["dt"], (datetime.date, datetime.datetime)) else row["dt"],
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
            "dt": row["dt"].isoformat() if isinstance(row["dt"], (datetime.date, datetime.datetime)) else row["dt"],
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
        dal = get_bar_dal()

        if ma_periods:
            df = dal.get_bars_with_ma(symbol, start_date, end_date, freq, ma_periods)
            data = add_ma_to_list(df, ma_periods)
        else:
            df = dal.get_stock_bars(symbol, start_date, end_date, freq)
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
        dal = get_bar_dal()

        if ma_periods:
            df = dal.get_bars_with_ma(sector_id, start_date, end_date, freq, ma_periods)
            data = add_ma_to_list(df, ma_periods)
        else:
            df = dal.get_sector_bars(sector_id, start_date, end_date, freq)
            data = bars_to_list(df)

        return JSONResponse({
            "code": 0,
            "message": "success",
            "data": {
                "sector_id": sector_id,
                "freq": freq,
                "items": data,
            },
        })

    except Exception as e:
        return JSONResponse(
            {"code": 500, "message": f"Failed to get kline: {e}"},
            status_code=500,
        )


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
        dal = get_bar_dal()

        if ma_periods:
            df = dal.get_bars_with_ma(symbol, start_date, end_date, freq, ma_periods)
            data = add_ma_to_list(df, ma_periods)
        else:
            df = dal.get_index_bars(symbol, start_date, end_date, freq)
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
        dal = get_bar_dal()

        # 获取主标的K线
        df1 = dal.get_stock_bars(symbol, start_date, end_date, freq)
        data1 = bars_to_list(df1)

        # 获取对比标的K线
        df2 = dal.get_stock_bars(compare, start_date, end_date, freq)
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
