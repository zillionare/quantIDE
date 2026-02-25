"""Tushare 扩展数据获取 - 板块和指数

提供板块列表、指数列表、板块行情、指数行情的获取功能。
"""

import datetime

import pandas as pd
import polars as pl
import tushare as ts
from loguru import logger

from pyqmt.config import cfg


def _convert_ts_code_to_symbol(ts_code: str) -> str:
    """将 tushare 代码格式转换为标准格式
    
    Examples:
        000001.SH -> 000001.SH
        000001.SZ -> 000001.SZ
    """
    return ts_code


def fetch_sector_list() -> pd.DataFrame | None:
    """获取行业板块列表
    
    Returns:
        DataFrame with columns: [id, name, sector_type]
    """
    pro = ts.pro_api()
    
    try:
        # 获取行业分类（申万行业）
        df = pro.index_classify(level="L1", src="SW")
        if df is None or df.empty:
            logger.warning("未获取到行业板块列表")
            return None
        
        # 重命名列
        df = df.rename(columns={
            "index_code": "id",
            "industry_name": "name",
        })
        
        df["sector_type"] = "industry"
        df["source"] = "tushare"
        
        return df[["id", "name", "sector_type", "source"]]
        
    except Exception as e:
        logger.error(f"获取行业板块列表失败: {e}")
        return None


def fetch_concept_list() -> pd.DataFrame | None:
    """获取概念板块列表
    
    Returns:
        DataFrame with columns: [id, name, sector_type]
    """
    pro = ts.pro_api()
    
    try:
        # 获取概念分类
        df = pro.ths_index(type="N")
        if df is None or df.empty:
            logger.warning("未获取到概念板块列表")
            return None
        
        # 重命名列
        df = df.rename(columns={
            "ts_code": "id",
            "name": "name",
        })
        
        df["sector_type"] = "concept"
        df["source"] = "tushare"
        
        return df[["id", "name", "sector_type", "source"]]
        
    except Exception as e:
        logger.error(f"获取概念板块列表失败: {e}")
        return None


def fetch_sector_stocks(sector_id: str) -> pd.DataFrame | None:
    """获取板块成分股
    
    Args:
        sector_id: 板块代码
        
    Returns:
        DataFrame with columns: [symbol, name]
    """
    pro = ts.pro_api()
    
    try:
        # 获取行业成分股
        df = pro.index_member(index_code=sector_id)
        if df is None or df.empty:
            logger.warning(f"未获取到板块 {sector_id} 的成分股")
            return None
        
        # 重命名列
        df = df.rename(columns={
            "con_code": "symbol",
            "con_name": "name",
        })
        
        return df[["symbol", "name"]]
        
    except Exception as e:
        logger.error(f"获取板块 {sector_id} 成分股失败: {e}")
        return None


def fetch_index_list() -> pd.DataFrame | None:
    """获取指数列表
    
    Returns:
        DataFrame with columns: [symbol, name, index_type, category, publisher]
    """
    pro = ts.pro_api()
    
    try:
        # 获取指数基本信息
        df = pro.index_basic()
        if df is None or df.empty:
            logger.warning("未获取到指数列表")
            return None
        
        # 重命名列
        df = df.rename(columns={
            "ts_code": "symbol",
            "name": "name",
            "publisher": "publisher",
            "category": "category",
        })
        
        # 映射指数类型
        type_mapping = {
            "MSCI": "market",
            "CSI": "market",  # 中证
            "SSE": "market",  # 上证
            "SZSE": "market",  # 深证
            "CICC": "market",  # 中金所
            "SW": "industry",  # 申万
            "OTH": "concept",  # 其他/概念
        }
        df["index_type"] = df["category"].map(type_mapping).fillna("market")
        
        df["source"] = "tushare"
        
        return df[["symbol", "name", "index_type", "category", "publisher", "source"]]
        
    except Exception as e:
        logger.error(f"获取指数列表失败: {e}")
        return None


def fetch_index_bars(
    symbol: str,
    start: datetime.date,
    end: datetime.date,
) -> pd.DataFrame | None:
    """获取指数行情数据
    
    Args:
        symbol: 指数代码，如 '000001.SH'
        start: 开始日期
        end: 结束日期
        
    Returns:
        DataFrame with columns: [dt, open, high, low, close, volume, amount]
    """
    pro = ts.pro_api()
    
    try:
        # 获取指数日线
        df = pro.index_daily(
            ts_code=symbol,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        
        if df is None or df.empty:
            logger.warning(f"未获取到指数 {symbol} 的行情数据")
            return None
        
        # 重命名和转换列
        df = df.rename(columns={
            "trade_date": "dt",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
            "amount": "amount",
        })
        
        # 转换日期格式
        df["dt"] = pd.to_datetime(df["dt"], format="%Y%m%d").dt.date
        
        # 转换数值类型
        df["volume"] = df["volume"].astype(int)
        df["amount"] = df["amount"].astype(float)
        
        # 按日期排序
        df = df.sort_values("dt")
        
        return df[["dt", "open", "high", "low", "close", "volume", "amount"]]
        
    except Exception as e:
        logger.error(f"获取指数 {symbol} 行情失败: {e}")
        return None


def fetch_sector_bars(
    sector_id: str,
    start: datetime.date,
    end: datetime.date,
) -> pd.DataFrame | None:
    """获取板块行情数据（通过板块指数）
    
    注意：tushare 的板块行情需要通过对应的板块指数代码获取。
    申万行业板块代码可以直接使用。
    
    Args:
        sector_id: 板块代码
        start: 开始日期
        end: 结束日期
        
    Returns:
        DataFrame with columns: [dt, open, high, low, close, volume, amount]
    """
    # 板块行情实际上也是指数行情
    return fetch_index_bars(sector_id, start, end)


def get_last_trade_date() -> datetime.date:
    """获取最近一个交易日"""
    pro = ts.pro_api()
    
    try:
        # 获取最近交易日历
        df = pro.trade_cal(exchange="SSE", limit=10)
        if df is not None and not df.empty:
            # 找到最近一个交易日
            df = df[df["is_open"] == 1]
            if not df.empty:
                last_date = df["cal_date"].iloc[0]
                return datetime.datetime.strptime(last_date, "%Y%m%d").date()
    except Exception as e:
        logger.error(f"获取最近交易日失败: {e}")
    
    # 默认返回昨天
    return datetime.date.today() - datetime.timedelta(days=1)
