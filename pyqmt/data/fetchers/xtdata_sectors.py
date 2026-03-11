"""QMT (xtdata) 板块数据获取模块

通过 xtquant.xtdata 获取板块列表、成分股和行情数据。
"""

import datetime
from typing import Any

import polars as pl
from loguru import logger

from pyqmt.core.xtwrapper import require_xt


def _get_xt() -> Any:
    """获取 xtdata 模块"""
    return require_xt()


def _get_sector_type_by_name(sector_name: str) -> str:
    """根据板块名称判断板块类型

    Args:
        sector_name: 板块名称

    Returns:
        板块类型: 'concept', 'etf', 'convertible', 'sw1', 'sw2', 'index', 'other'
    """
    if "转债" in sector_name or "可转债" in sector_name:
        return "convertible"
    elif sector_name.startswith("ETF") or "ETF" in sector_name:
        return "etf"
    elif sector_name.startswith("SW1") or sector_name.startswith("迅投一级"):
        return "sw1"
    elif sector_name.startswith("SW2") or sector_name.startswith("迅投二级"):
        return "sw2"
    elif sector_name.startswith("SW3") or sector_name.startswith("迅投三级"):
        return "sw3"
    elif sector_name in ["沪深指数", "沪深A股"]:
        return "index"
    elif any(sector_name.startswith(prefix) for prefix in ["上证", "深证", "中证", "国证", "沪深"]):
        return "index"
    elif sector_name.startswith("TGN") or sector_name.startswith("GN"):
        return "concept"
    else:
        return "other"


def fetch_sector_list(trade_date: datetime.date | None = None) -> pl.DataFrame:
    """获取板块列表

    从 QMT 获取所有板块列表，包括概念、ETF、转债、申万行业、各类指数等。

    Args:
        trade_date: 数据日期，默认为今天

    Returns:
        Polars DataFrame with columns:
        - id: 板块代码/名称
        - name: 板块名称
        - sector_type: 板块类型
        - source: 数据来源
        - trade_date: 数据日期
    """
    xt = _get_xt()
    if trade_date is None:
        trade_date = datetime.date.today()

    logger.info("从 QMT 获取板块列表...")

    try:
        sector_names = xt.get_sector_list()
        logger.info(f"获取到 {len(sector_names)} 个板块")

        records = []
        for sector_name in sector_names:
            sector_type = _get_sector_type_by_name(sector_name)
            records.append({
                "id": sector_name,
                "name": sector_name,
                "sector_type": sector_type,
                "source": "qmt",
                "trade_date": trade_date,
            })

        df = pl.DataFrame(records)
        logger.info(f"板块列表获取完成，共 {len(df)} 个板块")
        return df

    except Exception as e:
        logger.error(f"获取板块列表失败: {e}")
        return pl.DataFrame(schema={
            "id": pl.Utf8,
            "name": pl.Utf8,
            "sector_type": pl.Utf8,
            "source": pl.Utf8,
            "trade_date": pl.Date,
        })


def fetch_sector_constituents(sector_id: str, trade_date: datetime.date | None = None) -> pl.DataFrame:
    """获取板块成分股

    Args:
        sector_id: 板块ID/名称
        trade_date: 数据日期，默认为今天

    Returns:
        Polars DataFrame with columns:
        - sector_id: 板块ID
        - trade_date: 数据日期
        - symbol: 股票代码
        - name: 股票名称
        - weight: 权重（QMT不提供，默认为0）
    """
    xt = _get_xt()
    if trade_date is None:
        trade_date = datetime.date.today()

    try:
        symbols = xt.get_stock_list_in_sector(sector_id)

        records = []
        for symbol in symbols:
            # 尝试获取股票名称
            try:
                detail = xt.get_instrument_detail(symbol)
                name = detail.get("InstrumentName", "") if detail else ""
            except Exception:
                name = ""

            records.append({
                "sector_id": sector_id,
                "trade_date": trade_date,
                "symbol": symbol,
                "name": name,
                "weight": 0.0,
            })

        df = pl.DataFrame(records)
        logger.debug(f"板块 {sector_id} 获取到 {len(df)} 个成分股")
        return df

    except Exception as e:
        logger.warning(f"获取板块 {sector_id} 成分股失败: {e}")
        return pl.DataFrame(schema={
            "sector_id": pl.Utf8,
            "trade_date": pl.Date,
            "symbol": pl.Utf8,
            "name": pl.Utf8,
            "weight": pl.Float64,
        })


def fetch_sector_bars(
    sector_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pl.DataFrame:
    """获取板块行情数据

    Args:
        sector_id: 板块ID/名称（需要是支持行情的板块，如指数）
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        Polars DataFrame with columns:
        - sector_id: 板块ID
        - dt: 交易日期
        - open: 开盘价
        - high: 最高价
        - low: 最低价
        - close: 收盘价
        - volume: 成交量
        - amount: 成交额
    """
    xt = _get_xt()

    try:
        # 下载历史数据
        xt.download_history_data(sector_id, "1d")

        # 获取行情数据
        start_time = start_date.strftime("%Y%m%d")
        end_time = end_date.strftime("%Y%m%d")

        data = xt.get_market_data(
            field_list=["open", "high", "low", "close", "volume", "amount"],
            stock_list=[sector_id],
            period="1d",
            start_time=start_time,
            end_time=end_time,
        )

        if not data or sector_id not in data.get("close", {}):
            logger.warning(f"板块 {sector_id} 无行情数据")
            return pl.DataFrame(schema={
                "sector_id": pl.Utf8,
                "dt": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        # 转换数据格式
        records = []
        dates = list(data["close"][sector_id].keys())

        for date_str in dates:
            try:
                dt = datetime.datetime.strptime(str(date_str), "%Y%m%d").date()
                records.append({
                    "sector_id": sector_id,
                    "dt": dt,
                    "open": float(data["open"][sector_id][date_str]),
                    "high": float(data["high"][sector_id][date_str]),
                    "low": float(data["low"][sector_id][date_str]),
                    "close": float(data["close"][sector_id][date_str]),
                    "volume": int(data["volume"][sector_id][date_str]),
                    "amount": float(data["amount"][sector_id][date_str]),
                })
            except (KeyError, ValueError) as e:
                logger.debug(f"处理 {sector_id} {date_str} 数据时出错: {e}")
                continue

        df = pl.DataFrame(records)
        df = df.sort("dt")
        logger.debug(f"板块 {sector_id} 获取到 {len(df)} 条行情数据")
        return df

    except Exception as e:
        logger.warning(f"获取板块 {sector_id} 行情失败: {e}")
        return pl.DataFrame(schema={
            "sector_id": pl.Utf8,
            "dt": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
        })


def get_index_list() -> list[str]:
    """获取指数列表

    Returns:
        指数代码列表
    """
    xt = _get_xt()
    try:
        return xt.get_stock_list_in_sector("沪深指数")
    except Exception as e:
        logger.error(f"获取指数列表失败: {e}")
        return []


def get_tradeable_sectors() -> list[str]:
    """获取可交易的板块（有行情的板块）

    Returns:
        可交易板块名称列表，主要包括各类指数
    """
    xt = _get_xt()
    tradeable = []

    # 主要指数板块
    index_sectors = [
        "沪深指数",
        "沪深A股",
        "迅投一级行业板块指数",
        "迅投二级行业板块指数",
        "迅投三级行业板块指数",
    ]

    for sector in index_sectors:
        try:
            codes = xt.get_stock_list_in_sector(sector)
            tradeable.extend(codes)
        except Exception:
            continue

    return tradeable
