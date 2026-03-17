"""xtquant 分钟数据探测脚本."""

import argparse
import datetime as dt
from typing import Any

from qmt_gateway.config import config
from qmt_gateway.core.xtwrapper import clear_xt_cache, require_xtdata


def _to_date_str(value: dt.date) -> str:
    """将日期转换为 xtquant 常用字符串格式.

    Args:
        value: 日期对象。

    Returns:
        YYYYMMDD 格式字符串。

    """
    return value.strftime("%Y%m%d")


def _resolve_trade_date(raw: str | None) -> dt.date:
    """解析交易日期参数.

    Args:
        raw: 用户输入的日期字符串。

    Returns:
        目标日期。

    """
    if raw:
        return dt.datetime.strptime(raw, "%Y-%m-%d").date()
    return dt.date.today() - dt.timedelta(days=1)


def _normalize_optional_path(value: str | None) -> str | None:
    """将空路径和当前目录路径统一为 None.

    Args:
        value: 原始路径字符串。

    Returns:
        标准化后的路径。

    """
    text = (value or "").strip()
    if not text or text == ".":
        return None
    return text


def _pick_symbol(xtdata: Any, fallback: str) -> str:
    """从沪深A股列表选择一个可用股票代码.

    Args:
        xtdata: xtdata 模块对象。
        fallback: 兜底股票代码。

    Returns:
        可用股票代码。

    """
    symbols = xtdata.get_stock_list_in_sector("沪深A股") or []
    if symbols:
        return symbols[0]
    return fallback


def _extract_frame(data: Any, symbol: str) -> Any:
    """从 xtdata 返回值中提取单标的结果对象.

    Args:
        data: get_market_data_ex 返回值。
        symbol: 股票代码。

    Returns:
        对应标的数据对象。

    """
    if isinstance(data, dict):
        return data.get(symbol)
    return None


def run_probe(
    trade_date: dt.date,
    period: str,
    symbol: str,
    xtquant_path: str | None,
    qmt_path: str | None,
) -> dict[str, Any]:
    """执行 xtquant 分钟数据探测.

    Args:
        trade_date: 目标交易日。
        period: 周期字符串。
        symbol: 股票代码。
        xtquant_path: xtquant 路径。
        qmt_path: qmt 路径。

    Returns:
        探测结果。

    """
    clear_xt_cache()
    xtdata = require_xtdata(xtquant_path=xtquant_path, qmt_path=qmt_path)
    target_symbol = _pick_symbol(xtdata, symbol)
    day = _to_date_str(trade_date)
    xtdata.download_history_data(
        target_symbol, period=period, start_time=day, end_time=day
    )
    data = xtdata.get_market_data_ex(
        field_list=["time", "open", "high", "low", "close", "volume", "amount"],
        stock_list=[target_symbol],
        period=period,
        start_time=day,
        end_time=day,
        count=-1,
        dividend_type="none",
        fill_data=False,
    )
    frame = _extract_frame(data, target_symbol)
    row_count = len(frame) if frame is not None else 0
    columns = list(getattr(frame, "columns", [])) if frame is not None else []
    first_index = str(frame.index[0]) if row_count > 0 else ""
    last_index = str(frame.index[-1]) if row_count > 0 else ""
    return {
        "symbol": target_symbol,
        "trade_date": trade_date.isoformat(),
        "period": period,
        "rows": row_count,
        "columns": columns,
        "first_index": first_index,
        "last_index": last_index,
    }


def main() -> None:
    """命令行入口."""
    parser = argparse.ArgumentParser(description="探测 xtquant 分钟数据可用性")
    parser.add_argument("--trade-date", default="", help="交易日，格式 YYYY-MM-DD")
    parser.add_argument("--period", default="1m", help="K线周期，默认 1m")
    parser.add_argument("--symbol", default="000001.SZ", help="测试股票代码")
    parser.add_argument("--xtquant-path", default="", help="xtquant 路径")
    parser.add_argument("--qmt-path", default="", help="QMT 路径")
    args = parser.parse_args()

    trade_date = _resolve_trade_date(args.trade_date)
    xtquant_path = _normalize_optional_path(
        args.xtquant_path or config.get("xtquant_path", "")
    )
    qmt_path = _normalize_optional_path(
        args.qmt_path or config.get("qmt_path", "") or r"C:\apps"
    )
    result = run_probe(
        trade_date=trade_date,
        period=args.period,
        symbol=args.symbol,
        xtquant_path=xtquant_path,
        qmt_path=qmt_path,
    )
    print(result)


if __name__ == "__main__":
    main()
