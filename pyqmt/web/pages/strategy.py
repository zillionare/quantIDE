import asyncio
import datetime
import json
import re
import uuid

import arrow
import polars as pl
from fasthtml.common import *
from loguru import logger
from monsterui.all import *
from starlette.websockets import WebSocket, WebSocketDisconnect

from pyqmt.core.enums import BrokerKind, FrameType, OrderSide
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.sqlite import db
from pyqmt.service.discovery import strategy_loader
from pyqmt.service.grid_search import GridSearch
from pyqmt.service.metrics import metrics
from pyqmt.service.runner import BacktestRunner
from pyqmt.web.layouts.main import MainLayout

strategy_app, rt = fast_app()

BENCHMARK_ASSET = "000300.SH"

def _normalize_stats(stats):
    """规范化 quantstats 指标名称并返回字典。

    Args:
        stats: quantstats 输出的指标表

    Returns:
        dict: 指标键值对，键为下划线风格
    """
    if stats is None or stats.empty:
        return {}
    series = stats.iloc[:, 0]
    index = series.index.astype(str)
    normalized = (
        index.str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    series.index = normalized
    return series.to_dict()


def _to_number(value) -> float:
    """将指标值转换为 float。

    Args:
        value: 指标值

    Returns:
        float: 数值化结果
    """
    if value is None:
        return 0.0
    if isinstance(value, str):
        cleaned = value.strip().rstrip("%")
        if cleaned == "" or cleaned.lower() == "nan":
            return 0.0
        value = cleaned
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_date_axis(portfolio_id: str) -> list[str]:
    """构建完整时间轴。

    Args:
        portfolio_id: 组合 ID

    Returns:
        list[str]: 日期字符串列表
    """
    portfolio = db.get_portfolio(portfolio_id)
    if portfolio:
        start_date = portfolio.start
        end_date = portfolio.end or portfolio.start
    else:
        assets_df = db.query_assets(portfolio_id)
        if assets_df.is_empty():
            return []
        assets_df = assets_df.sort("dt")
        start_date = assets_df.row(0, named=True)["dt"]
        end_date = assets_df.row(-1, named=True)["dt"]
    dates = calendar.get_frames(start_date, end_date, FrameType.DAY)
    return [arrow.get(dt).format("YYYY-MM-DD") for dt in dates]


def _build_series_payload(portfolio_id: str, date_axis: list[str]) -> dict:
    """构建曲线及日度序列。

    Args:
        portfolio_id: 组合 ID
        date_axis: 时间轴

    Returns:
        dict: 序列数据
    """
    assets_df = db.query_assets(portfolio_id)
    if assets_df.is_empty() or not date_axis:
        return {
            "date_axis": date_axis,
            "total": [None for _ in date_axis],
            "benchmark": [None for _ in date_axis],
            "daily_pnl": [None for _ in date_axis],
            "trade_count": [0 for _ in date_axis],
        }

    assets_df = assets_df.sort("dt")
    asset_rows = assets_df.iter_rows(named=True)
    total_by_date = {}
    last_date = None
    totals = []
    for row in asset_rows:
        dt = arrow.get(row["dt"]).format("YYYY-MM-DD")
        total_by_date[dt] = row["total"]
        last_date = dt
        totals.append(row["total"])

    trade_count_by_date: dict[str, int] = {}
    trades_df = db.trades_all(portfolio_id)
    if not trades_df.is_empty():
        for row in trades_df.iter_rows(named=True):
            tm = row.get("tm")
            if tm is None:
                continue
            dt = arrow.get(tm).format("YYYY-MM-DD")
            trade_count_by_date[dt] = trade_count_by_date.get(dt, 0) + 1

    total_series = []
    daily_pnl_series = []
    trade_count_series = []
    prev_total = None
    for dt in date_axis:
        if last_date is not None and dt > last_date:
            total_series.append(None)
            daily_pnl_series.append(None)
        else:
            total = total_by_date.get(dt)
            total_series.append(total)
            if total is None or prev_total is None:
                daily_pnl_series.append(0.0)
            else:
                daily_pnl_series.append(total - prev_total)
            prev_total = total if total is not None else prev_total
        trade_count_series.append(trade_count_by_date.get(dt, 0))

    benchmark_series = [None for _ in date_axis]
    try:
        start_date = arrow.get(date_axis[0]).date()
        end_date = arrow.get(date_axis[-1]).date()
        benchmark_df = daily_bars.get_bars_in_range(
            start_date,
            end_date,
            assets=[BENCHMARK_ASSET],
            eager_mode=True,
        )
    except Exception:
        benchmark_df = pl.DataFrame()

    if not benchmark_df.is_empty():
        benchmark_df = benchmark_df.sort("date")
        close_by_date = {}
        for row in benchmark_df.iter_rows(named=True):
            dt = arrow.get(row["date"]).format("YYYY-MM-DD")
            close_by_date[dt] = row.get("close")
        first_total = next((v for v in total_series if v is not None), None)
        first_close = next(
            (close_by_date.get(dt) for dt in date_axis if close_by_date.get(dt)),
            None,
        )
        if first_total is None:
            first_total = 1.0
        if first_close:
            benchmark_series = []
            for dt in date_axis:
                close = close_by_date.get(dt)
                if close is None:
                    benchmark_series.append(None)
                else:
                    benchmark_series.append(first_total * float(close) / float(first_close))

    return {
        "date_axis": date_axis,
        "total": total_series,
        "benchmark": benchmark_series,
        "daily_pnl": daily_pnl_series,
        "trade_count": trade_count_series,
    }


def _build_metrics_payload(portfolio_id: str) -> dict:
    """构建指标数据。

    Args:
        portfolio_id: 组合 ID

    Returns:
        dict: 指标键值对
    """
    stats = metrics(portfolio_id)
    stats_dict = _normalize_stats(stats)
    annual_return = _to_number(stats_dict.get("annual_return", stats_dict.get("cagr", 0.0)))
    sharpe = _to_number(stats_dict.get("sharpe", 0.0))
    max_drawdown = _to_number(stats_dict.get("max_drawdown", 0.0))
    total_returns = _to_number(
        stats_dict.get("cumulative_return", stats_dict.get("total_return", stats_dict.get("total_returns", 0.0)))
    )
    volatility = _to_number(stats_dict.get("volatility_ann", stats_dict.get("volatility", 0.0)))
    sortino = _to_number(stats_dict.get("sortino", 0.0))
    calmar = _to_number(stats_dict.get("calmar", 0.0))
    win_rate = _to_number(stats_dict.get("win_rate", 0.0))
    profit_factor = _to_number(stats_dict.get("profit_factor", 0.0))
    alpha = _to_number(stats_dict.get("alpha", 0.0))
    beta = _to_number(stats_dict.get("beta", 0.0))
    payoff_ratio = _to_number(stats_dict.get("payoff_ratio", 0.0))
    avg_return = _to_number(stats_dict.get("avg_return", 0.0))
    avg_win = _to_number(stats_dict.get("avg_win", 0.0))
    avg_loss = _to_number(stats_dict.get("avg_loss", 0.0))
    best_day = _to_number(stats_dict.get("best_day", 0.0))
    worst_day = _to_number(stats_dict.get("worst_day", 0.0))
    tail_ratio = _to_number(stats_dict.get("tail_ratio", 0.0))
    skew = _to_number(stats_dict.get("skew", 0.0))
    kurtosis = _to_number(stats_dict.get("kurtosis", 0.0))
    value_at_risk = _to_number(
        stats_dict.get("daily_value_at_risk", stats_dict.get("value_at_risk", 0.0))
    )
    information_ratio = _to_number(stats_dict.get("information_ratio", 0.0))
    return {
        "annual_return": annual_return,
        "total_returns": total_returns,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "volatility": volatility,
        "sortino": sortino,
        "calmar": calmar,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "alpha": alpha,
        "beta": beta,
        "payoff_ratio": payoff_ratio,
        "avg_return": avg_return,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "best_day": best_day,
        "worst_day": worst_day,
        "tail_ratio": tail_ratio,
        "skew": skew,
        "kurtosis": kurtosis,
        "value_at_risk": value_at_risk,
        "information_ratio": information_ratio,
    }


def _build_trade_rows(portfolio_id: str, limit: int = 200) -> list[dict]:
    """构建交易明细行。

    Args:
        portfolio_id: 组合 ID
        limit: 最大行数

    Returns:
        list[dict]: 交易明细
    """
    trades_df = db.trades_all(portfolio_id)
    if trades_df.is_empty():
        return []
    trades_df = trades_df.sort("tm", descending=True).head(limit)
    rows = []
    for row in trades_df.iter_rows(named=True):
        side = row.get("side")
        if isinstance(side, OrderSide):
            side_text = str(side)
            side_value = side.value
        else:
            side_value = int(side) if side is not None else 0
            side_text = "买入" if side_value == OrderSide.BUY else "卖出"
        rows.append(
            {
                "tm": str(row.get("tm", "")),
                "asset": row.get("asset", ""),
                "side": side_text,
                "side_value": side_value,
                "price": float(row.get("price") or 0),
                "shares": float(row.get("shares") or 0),
                "amount": float(row.get("amount") or 0),
                "fee": float(row.get("fee") or 0),
            }
        )
    return rows


def _build_daily_positions(portfolio_id: str) -> list[dict]:
    """构建每日持仓明细。

    Args:
        portfolio_id: 组合 ID

    Returns:
        list[dict]: 持仓明细
    """
    positions_df = db.positions_all(portfolio_id)
    if positions_df.is_empty():
        return []
    positions_df = positions_df.sort(["dt", "asset"])
    rows = []
    for row in positions_df.iter_rows(named=True):
        rows.append(
            {
                "dt": str(row.get("dt", "")),
                "asset": row.get("asset", ""),
                "shares": float(row.get("shares") or 0),
                "avail": float(row.get("avail") or 0),
                "price": float(row.get("price") or 0),
                "mv": float(row.get("mv") or 0),
                "profit": float(row.get("profit") or 0),
            }
        )
    return rows


def _build_daily_summary(portfolio_id: str) -> list[dict]:
    """构建每日收益概览。

    Args:
        portfolio_id: 组合 ID

    Returns:
        list[dict]: 每日收益概览
    """
    assets_df = db.query_assets(portfolio_id)
    if assets_df.is_empty():
        return []
    assets_df = assets_df.sort("dt")
    rows = []
    prev_total = None
    for row in assets_df.iter_rows(named=True):
        total = float(row.get("total") or 0)
        daily_pnl = 0.0 if prev_total is None else total - prev_total
        daily_return = 0.0 if prev_total in (None, 0) else daily_pnl / prev_total
        rows.append(
            {
                "dt": str(row.get("dt", "")),
                "cash": float(row.get("cash") or 0),
                "market_value": float(row.get("market_value") or 0),
                "total": total,
                "daily_pnl": daily_pnl,
                "daily_return": daily_return,
            }
        )
        prev_total = total
    return rows


def _build_log_rows(portfolio_id: str, limit: int = 200) -> list[dict]:
    """构建日志行。

    Args:
        portfolio_id: 组合 ID
        limit: 最大行数

    Returns:
        list[dict]: 日志行
    """
    logs_df = db.get_strategy_logs(portfolio_id)
    if logs_df.is_empty():
        return []
    logs_df = logs_df.sort("dt", descending=True).head(limit)
    rows = []
    for row in logs_df.iter_rows(named=True):
        rows.append(
            {
                "dt": str(row.get("dt", "")),
                "key": row.get("key", ""),
                "value": row.get("value"),
                "extra": row.get("extra", ""),
            }
        )
    return rows


def _format_date(value) -> str:
    """格式化日期。

    Args:
        value: 日期值

    Returns:
        str: 格式化后的日期文本
    """
    if value is None:
        return "--"
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _format_range(start, end) -> str:
    """格式化回测区间。

    Args:
        start: 开始日期
        end: 结束日期

    Returns:
        str: 回测区间文本
    """
    if start is None and end is None:
        return "--"
    return f"{_format_date(start)} ~ {_format_date(end)}"


def _format_percent(value) -> str:
    """格式化百分比值。

    Args:
        value: 百分比数值

    Returns:
        str: 百分比文本
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "--"
    return f"{number * 100:.1f}%"


def _format_number(value) -> str:
    """格式化数值。

    Args:
        value: 数值

    Returns:
        str: 格式化后的数字文本
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "--"
    return f"{number:.2f}"


def _params_to_text(params: dict) -> str:
    """格式化参数显示文本。

    Args:
        params: 参数字典

    Returns:
        str: 参数文本
    """
    if not params:
        return "--"
    items = []
    for key, value in params.items():
        text_value = value
        if isinstance(value, dict):
            text_value = value.get("default", "")
        items.append(f"{key}={text_value}")
    return ", ".join(items) if items else "--"


def _extract_params_from_info(info) -> dict:
    """从回测信息中提取参数。

    Args:
        info: 组合信息字段

    Returns:
        dict: 参数字典
    """
    if not info:
        return {}
    if isinstance(info, dict):
        return info
    if isinstance(info, str):
        try:
            payload = json.loads(info)
        except json.JSONDecodeError:
            return {}
        if isinstance(payload, dict):
            config = payload.get("config")
            if isinstance(config, dict):
                return config
            return payload
    return {}


def _strategy_version(strategy_cls) -> str:
    """获取策略版本。

    Args:
        strategy_cls: 策略类

    Returns:
        str: 版本文本
    """
    if strategy_cls is None:
        return "v1.0.0"
    return getattr(strategy_cls, "VERSION", getattr(strategy_cls, "__version__", "v1.0.0"))


def _build_strategy_rows(strategies: dict) -> list:
    """构建策略列表行。

    Args:
        strategies: 策略字典

    Returns:
        list: 行数据
    """
    rows = []
    for name, cls in strategies.items():
        doc = cls.__doc__ or "暂无描述"
        version = _strategy_version(cls)
        portfolios = db.get_portfolios_by_strategy(name)
        history_count = portfolios.height if hasattr(portfolios, "height") else 0
        latest_link = None
        latest_date = "--"
        if not portfolios.is_empty():
            portfolios = portfolios.sort("start", descending=True)
            latest_row = portfolios.row(0, named=True)
            latest_date = _format_date(latest_row.get("end") or latest_row.get("start"))
            latest_link = latest_row.get("portfolio_id")

        latest_cell = (
            A(latest_date, href=f"/strategy/backtest/{latest_link}", cls="text-blue-600 hover:underline")
            if latest_link
            else Span("--", cls="text-gray-400")
        )

        rows.append(
            Tr(
                Td(
                    Button(
                        UkIcon("play", size=16, cls="text-blue-600"),
                        title="重新运行回测",
                        hx_get=f"/strategy/{name}/backtest/modal",
                        hx_target="#modal-container",
                        cls="w-8 h-8 flex items-center justify-center bg-transparent border-0 shadow-none p-0 hover:text-blue-800",
                        type="button",
                    )
                ),
                Td(name, cls="text-gray-900 font-medium"),
                Td(doc, cls="text-gray-600"),
                Td(version, cls="text-gray-900"),
                Td(f"{history_count}次", cls="text-gray-900"),
                Td(latest_cell),
                cls="border-b border-gray-200 hover:bg-gray-50",
            )
        )
    return rows


def _build_backtest_rows(strategies: dict) -> list:
    """构建回测报告列表行。

    Args:
        strategies: 策略字典

    Returns:
        list: 行数据
    """
    rows = []
    portfolios = db.portfolios_all()
    if portfolios.is_empty():
        return rows
    if "start" in portfolios.columns:
        portfolios = portfolios.sort("start", descending=True)
    for row in portfolios.iter_rows(named=True):
        kind = row.get("kind")
        if isinstance(kind, str):
            try:
                kind = BrokerKind(kind)
            except ValueError:
                continue
        if kind != BrokerKind.BACKTEST:
            continue
        portfolio_id = row.get("portfolio_id")
        name = row.get("name") or "--"
        strategy_cls = strategies.get(name)
        version = _strategy_version(strategy_cls)
        info_params = _extract_params_from_info(row.get("info"))
        if not info_params and strategy_cls:
            info_params = getattr(strategy_cls, "PARAMS", {})
        params_text = _params_to_text(info_params)
        range_text = _format_range(row.get("start"), row.get("end"))
        metrics_payload = _build_metrics_payload(portfolio_id)
        annual_return = metrics_payload.get("annual_return", 0.0)
        sharpe = metrics_payload.get("sharpe", 0.0)
        max_drawdown = metrics_payload.get("max_drawdown", 0.0)
        sortino = metrics_payload.get("sortino", 0.0)
        annual_cls = "text-green-600" if annual_return >= 0 else "text-red-600"
        drawdown_cls = "text-red-600" if max_drawdown < 0 else "text-gray-900"

        rows.append(
            Tr(
                Td(name, cls="text-gray-900 font-medium"),
                Td(version, cls="text-gray-600"),
                Td(params_text, cls="text-gray-600"),
                Td(range_text, cls="text-gray-600"),
                Td(_format_percent(annual_return), cls=f"{annual_cls} font-medium"),
                Td(_format_number(sharpe), cls="text-gray-900"),
                Td(_format_percent(max_drawdown), cls=drawdown_cls),
                Td(_format_number(sortino), cls="text-gray-900"),
                Td(
                    A("查看报告", href=f"/strategy/backtest/{portfolio_id}", cls="text-blue-600 hover:underline")
                ),
                cls="border-b border-gray-200 hover:bg-gray-50",
            )
        )
    return rows

@rt("/")
def index(req, session):
    layout = MainLayout(title="策略列表", user=session.get("auth"))
    layout.header_active = "策略"
    layout.sidebar_menu = [
        {
            "title": "策略列表",
            "url": "/strategy",
            "icon_path": "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2",
            "active": True,
        },
        {
            "title": "回测报告",
            "url": "/strategy#backtest-list",
            "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
        },
    ]

    # 从缓存加载策略（不再每次扫描）
    strategies = strategy_loader.load_from_cache()
    scan_dir = strategy_loader.get_scan_directory()
    strategy_rows = _build_strategy_rows(strategies)
    backtest_rows = _build_backtest_rows(strategies)

    strategy_table = Table(
        Thead(
            Tr(
                Th("操作", cls="px-6 py-3 font-medium w-20"),
                Th("名称", cls="px-6 py-3 font-medium"),
                Th("简介", cls="px-6 py-3 font-medium"),
                Th("版本", cls="px-6 py-3 font-medium"),
                Th("历史回测", cls="px-6 py-3 font-medium"),
                Th("最新回测", cls="px-6 py-3 font-medium"),
                cls="text-left text-sm text-gray-600 border-b border-gray-200",
            )
        ),
        Tbody(*strategy_rows, cls="text-sm"),
        cls="w-full",
    )

    backtest_table = Table(
        Thead(
            Tr(
                Th("策略名", cls="px-6 py-3 font-medium"),
                Th("版本", cls="px-6 py-3 font-medium"),
                Th("参数", cls="px-6 py-3 font-medium"),
                Th("回测区间", cls="px-6 py-3 font-medium"),
                Th("年化收益", cls="px-6 py-3 font-medium"),
                Th("夏普", cls="px-6 py-3 font-medium"),
                Th("最大回撤", cls="px-6 py-3 font-medium"),
                Th("索提诺", cls="px-6 py-3 font-medium"),
                Th("操作", cls="px-6 py-3 font-medium"),
                cls="text-left text-sm text-gray-600 border-b border-gray-200",
            )
        ),
        Tbody(*backtest_rows, cls="text-sm"),
        cls="w-full",
    )

    layout.main_block = lambda: Div(
        Div(
            Nav(
                A("首页", href="/", cls="hover:text-blue-600"),
                Span(">", cls="text-gray-400"),
                A("策略", href="/strategy", cls="hover:text-blue-600"),
                Span(">", cls="text-gray-400"),
                Span("策略列表", cls="text-gray-900 font-medium"),
                cls="flex items-center space-x-2 text-sm text-gray-600",
            ),
            cls="mb-4",
        ),
        Div(
            Div(
                H2("策略列表", cls="text-lg font-semibold text-gray-900"),
                Button(
                    Span(
                        Svg(
                            Path(
                                d="M12 4v16m8-8H4",
                                **{
                                    "stroke-linecap": "round",
                                    "stroke-linejoin": "round",
                                    "stroke-width": "2",
                                },
                            ),
                            cls="w-5 h-5",
                            fill="none",
                            stroke="currentColor",
                            viewBox="0 0 24 24",
                        ),
                        cls="flex items-center",
                    ),
                    Span("扫描策略列表"),
                    cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2",
                    type="button",
                    hx_get="/strategy/scan/confirm",
                    hx_target="#modal-container",
                ),
                Button(
                    UkIcon("cog", size=20),
                    cls="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg ml-2",
                    type="button",
                    title="配置扫描目录",
                    hx_get="/strategy/scan/config-modal",
                    hx_target="#modal-container",
                ),
                cls="p-6 border-b border-gray-200 flex justify-between items-center",
            ),
            Div(strategy_table, cls="overflow-x-auto"),
            cls="bg-white rounded-lg shadow mb-6",
        ),
        Div(
            Div(
                Div(
                    H2("回测报告列表", cls="text-lg font-semibold text-gray-900"),
                    cls="flex items-center",
                ),
                Div(
                    Div(
                        Input(
                            type="text",
                            placeholder="按策略名过滤...",
                            cls="pl-10 pr-4 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-blue-500 focus:border-transparent",
                        ),
                        Svg(
                            Path(
                                d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z",
                                **{
                                    "stroke-linecap": "round",
                                    "stroke-linejoin": "round",
                                    "stroke-width": "2",
                                },
                            ),
                            cls="w-5 h-5 text-gray-400 absolute left-3 top-2.5",
                            fill="none",
                            stroke="currentColor",
                            viewBox="0 0 24 24",
                        ),
                        cls="relative",
                    ),
                    Select(
                        Option("按年化收益排序", value="annual", selected=True),
                        Option("按夏普比率排序", value="sharpe"),
                        Option("按最大回撤排序", value="drawdown"),
                        Option("按索提诺排序", value="sortino"),
                        cls="px-4 rounded-lg text-gray-900 focus:ring-2 focus:ring-blue-500 focus:outline-none",
                    ),
                    cls="flex items-center space-x-4",
                ),
                cls="p-6 border-b border-gray-200 flex justify-between items-center",
            ),
            Div(backtest_table, cls="overflow-x-auto"),
            cls="bg-white rounded-lg shadow",
            id="backtest-list",
        ),
        Div(id="modal-container"),
        cls="space-y-6",
    )

    return layout.render()

@rt("/{name}")
def strategy_detail(req, session, name: str):
    layout = MainLayout(title=f"策略详情 - {name}", user=session.get("auth"))

    strategies = strategy_loader.load_from_cache()

    if name not in strategies:
        return RedirectResponse("/strategy")

    cls = strategies[name]
    doc = cls.__doc__ or "暂无描述"
    params = getattr(cls, "PARAMS", {})

    # Get history
    portfolios = db.get_portfolios_by_strategy(name)
    history_rows = []

    if not portfolios.is_empty():
        # Sort by start time desc (assuming DB returns roughly in order, or we sort manually)
        # Polars sort:
        portfolios = portfolios.sort("start", descending=True)

        for row in portfolios.iter_rows(named=True):
            pid = row["portfolio_id"]
            start = row["start"]
            end = row["end"]

            history_rows.append(
                Tr(
                    Td(pid[:8]),
                    Td(str(start)),
                    Td(str(end)),
                    Td(
                        A("查看报告", href=f"/strategy/backtest/{pid}", cls="text-blue-600 hover:underline"),
                    )
                )
            )

    history_table = Table(
        Thead(Tr(Th("ID"), Th("开始时间"), Th("结束时间"), Th("操作"))),
        Tbody(*history_rows),
        cls=TableT.striped
    )

    layout.main_block = lambda: Div(
        Div(
            A("← 返回列表", href="/strategy", cls="text-gray-500 hover:text-gray-800 mb-4 inline-block"),
            H1(name, cls="text-3xl font-bold mb-2"),
            P(doc, cls="text-gray-600 mb-6"),

            Card(
                CardHeader(H3("默认参数")),
                CardBody(Pre(str(params), cls="bg-gray-50 p-4 rounded text-sm")),
                cls="mb-6"
            ),

            Div(
                H3("回测历史", cls="text-xl font-bold mb-4"),
                history_table,
                cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100"
            ),

            cls="max-w-4xl mx-auto py-8"
        )
    )

    return layout.render()

# --- Backtest Modal & Runner ---

def _parse_params(form, prefix="param_"):
    config = {}
    for k, v in form.items():
        if k.startswith(prefix):
            param_name = k[len(prefix):]
            # Try to infer type
            if v.lower() == 'true': v = True
            elif v.lower() == 'false': v = False
            else:
                try:
                    if '.' in v: v = float(v)
                    else: v = int(v)
                except:
                    pass
            config[param_name] = v
    return config

@rt("/{name}/backtest/modal")
def backtest_modal(name: str):
    strategies = strategy_loader.load_from_cache()
    cls = strategies.get(name)
    if not cls: return "Strategy not found"

    default_params = getattr(cls, "PARAMS", {})

    param_inputs = []
    for k, v in default_params.items():
        param_inputs.append(
            Div(
                Label(k, cls="block text-sm font-medium text-gray-700 mb-1"),
                Input(name=f"param_{k}", value=str(v), cls="input input-sm"),
                cls="mb-3"
            )
        )

    return Div(
        Div(
            H3(f"运行回测 - {name}", cls="text-lg font-bold mb-4"),
            Form(
                Div(
                    Label("开始日期", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(name="start_date", type="date", value="2024-01-01", required=True, cls="input input-sm"),
                    cls="mb-3"
                ),
                Div(
                    Label("结束日期", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(name="end_date", type="date", value=arrow.now().format("YYYY-MM-DD"), required=True, cls="input input-sm"),
                    cls="mb-3"
                ),
                Div(
                    Label("初始资金", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(name="initial_cash", type="number", value="1000000", cls="input input-sm"),
                    cls="mb-3"
                ),
                Div(
                    Label("周期", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Select(
                        Option("日线", value="1d"),
                        Option("1分钟", value="1m"),
                        name="interval",
                        cls="select select-sm"
                    ),
                    cls="mb-3"
                ),
                Div(H4("策略参数", cls="text-sm font-bold mt-4 mb-2"), *param_inputs),
                Div(
                    Button("取消", type="button", cls="btn btn-ghost", onclick="document.getElementById('modal-container').innerHTML=''"),
                    Button(
                        "开始运行",
                        type="button",
                        cls="btn btn-primary",
                        hx_post=f"/strategy/{name}/backtest/run",
                        hx_target="#modal-container",
                        hx_include="closest form"
                    ),
                    cls="flex justify-end gap-2 mt-6"
                ),
            ),
            cls="bg-white p-6 rounded-xl shadow-xl max-w-lg w-full"
        ),
        cls="fixed inset-0 bg-black/50 flex items-center justify-center z-50",
        id="strategy-modal"
    )

@rt("/{name}/backtest/run", methods=["POST"])
async def run_backtest(req, name: str):
    form = await req.form()

    try:
        start_date = arrow.get(form.get("start_date")).date()
        end_date = arrow.get(form.get("end_date")).date()
        initial_cash = float(form.get("initial_cash", 1000000))
        interval = form.get("interval", "1d")
        config = _parse_params(form)

        strategies = strategy_loader.load_from_cache()
        cls = strategies.get(name)

        if not cls: raise Exception("Strategy not found")

        portfolio_id = uuid.uuid4().hex
        runner = BacktestRunner()
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None,
            lambda: asyncio.run(
                runner.run(
                    strategy_cls=cls,
                    config=config,
                    start_date=start_date,
                    end_date=end_date,
                    frame_type=FrameType(interval),
                    initial_cash=initial_cash,
                    portfolio_id=portfolio_id,
                )
            ),
        )
        return Response(
            "",
            headers={"HX-Redirect": f"/strategy/backtest/{portfolio_id}"},
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return Div(
            Div(f"回测失败: {str(e)}", cls="text-red-500 mb-4"),
            Button("关闭", cls="btn btn-secondary", onclick="this.closest('.modal').remove()"),
            cls="p-4 bg-white rounded shadow"
        )

# --- Grid Search Modal & Runner ---

@rt("/{name}/grid_search/modal")
def grid_search_modal(name: str):
    strategies = strategy_loader.load_from_cache()
    cls = strategies.get(name)
    if not cls: return "Strategy not found"

    default_params = getattr(cls, "PARAMS", {})

    param_inputs = []
    for k, v in default_params.items():
        # For grid search, we expect comma separated values
        param_inputs.append(
            Div(
                Label(f"{k} (逗号分隔)", cls="block text-sm font-medium text-gray-700 mb-1"),
                Input(name=f"param_{k}", value=str(v), placeholder="例如: 10, 20, 30", cls="input input-sm"),
                cls="mb-3"
            )
        )

    return Div(
        Div(
            H3(f"运行网格搜索 - {name}", cls="text-lg font-bold mb-4"),
            Form(
                Div(
                    Label("开始日期", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(name="start_date", type="date", value="2024-01-01", required=True, cls="input input-sm"),
                    cls="mb-3"
                ),
                Div(
                    Label("结束日期", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(name="end_date", type="date", value=arrow.now().format("YYYY-MM-DD"), required=True, cls="input input-sm"),
                    cls="mb-3"
                ),
                Div(
                    Label("并发数量", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(name="max_workers", type="number", value="4", cls="input input-sm"),
                    cls="mb-3"
                ),
                Div(
                    H4("参数网格", cls="text-sm font-bold mt-4 mb-2"),
                    P("输入多个值以逗号分隔，如: 5, 10, 20", cls="text-xs text-gray-500 mb-2"),
                    *param_inputs
                ),
                Div(
                    Button("取消", type="button", cls="btn btn-ghost", onclick="document.getElementById('modal-container').innerHTML=''"),
                    Button(
                        "开始运行",
                        type="button",
                        cls="btn btn-primary",
                        hx_post=f"/strategy/{name}/grid_search/run",
                        hx_target="#modal-container",
                        hx_include="closest form"
                    ),
                    cls="flex justify-end gap-2 mt-6"
                ),
            ),
            cls="bg-white p-6 rounded-xl shadow-xl max-w-lg w-full"
        ),
        cls="fixed inset-0 bg-black/50 flex items-center justify-center z-50",
        id="grid-search-modal"
    )

@rt("/{name}/grid_search/run", methods=["POST"])
async def run_grid_search(req, name: str):
    form = await req.form()

    try:
        start_date = arrow.get(form.get("start_date")).date()
        end_date = arrow.get(form.get("end_date")).date()
        max_workers = int(form.get("max_workers", 4))

        # Parse grid params
        param_grid = {}
        base_config = {}

        strategies = strategy_loader.load_from_cache()
        cls = strategies.get(name)
        if not cls: raise Exception("Strategy not found")
        default_params = getattr(cls, "PARAMS", {})

        for k, v in form.items():
            if k.startswith("param_"):
                param_name = k[6:]
                # Split by comma
                if ',' in v:
                    values = [x.strip() for x in v.split(',')]
                    # Try convert types
                    converted_values = []
                    for val in values:
                        try:
                            if '.' in val: converted_values.append(float(val))
                            else: converted_values.append(int(val))
                        except:
                            converted_values.append(val)
                    param_grid[param_name] = converted_values
                else:
                    # Single value, treat as base config
                    try:
                        if '.' in v: base_config[param_name] = float(v)
                        else: base_config[param_name] = int(v)
                    except:
                        base_config[param_name] = v

        gs = GridSearch(
            strategy_cls=cls,
            base_config=base_config,
            param_grid=param_grid,
            start_date=start_date,
            end_date=end_date,
            initial_cash=1000000,
            max_workers=max_workers
        )

        # Run grid search in a separate thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        results_df = await loop.run_in_executor(None, lambda: gs.run(save_logs=True))

        # Show results in a modal or redirect?
        # Let's show a summary table in a modal

        rows = []
        # Sort by annual_return desc
        if not results_df.is_empty():
            results_df = results_df.sort("annual_return", descending=True).head(10)
            for row in results_df.iter_rows(named=True):
                rows.append(
                    Tr(
                        Td(str(row["params"])),
                        Td(f"{row['annual_return']:.2%}"),
                        Td(f"{row['sharpe']:.2f}"),
                        Td(f"{row['max_drawdown']:.2%}"),
                    )
                )

        result_table = Table(
            Thead(Tr(Th("参数"), Th("年化"), Th("夏普"), Th("回撤"))),
            Tbody(*rows),
            cls=TableT.striped + " text-xs"
        )

        return Modal(
            ModalTitle("网格搜索结果 (Top 10)"),
            ModalBody(result_table),
            ModalFooter(
                Button("关闭", cls=ButtonT.primary, onclick="document.getElementById('modal-container').innerHTML=''")
            ),
            open=True
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return Div(
            Div(f"搜索失败: {str(e)}", cls="text-red-500 mb-4"),
            Button("关闭", cls=ButtonT.secondary, onclick="this.closest('.modal').remove()"),
            cls="p-4 bg-white rounded shadow"
        )

# --- Backtest Result View ---

@rt("/backtest/{portfolio_id}")
def backtest_result(req, session, portfolio_id: str):
    layout = MainLayout(title="回测报告", user=session.get("auth"))
    layout.header_active = "策略"
    layout.sidebar_menu = [
        {
            "title": "策略列表",
            "url": "/strategy",
            "icon_path": "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2",
        },
        {
            "title": "回测报告",
            "url": f"/strategy/backtest/{portfolio_id}",
            "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
            "active": True,
            "children": [
                {"title": "收益概述", "url": "#overview", "active": True},
                {"title": "交易详情", "url": "#trades"},
                {"title": "每日持仓", "url": "#positions"},
                {"title": "日志输出", "url": "#logs"},
            ],
        },
    ]

    portfolio = db.get_portfolio(portfolio_id)
    status = "running" if portfolio is None or portfolio.status else "finished"
    metrics_payload = _build_metrics_payload(portfolio_id)
    is_running = status == "running"

    date_axis = _build_date_axis(portfolio_id)
    series_payload = _build_series_payload(portfolio_id, date_axis)

    metrics_items = [
        ("annual_return", "年化收益", "percent"),
        ("total_returns", "累计收益", "percent"),
        ("max_drawdown", "最大回撤", "percent"),
        ("volatility", "波动率", "percent"),
        ("sharpe", "夏普比率", "number"),
        ("sortino", "索提诺比率", "number"),
        ("calmar", "卡玛比率", "number"),
        ("alpha", "Alpha", "number"),
        ("beta", "Beta", "number"),
        ("win_rate", "胜率", "percent"),
        ("profit_factor", "盈亏比", "number"),
        ("payoff_ratio", "收益风险比", "number"),
        ("avg_return", "平均收益", "percent"),
        ("avg_win", "平均盈利", "percent"),
        ("avg_loss", "平均亏损", "percent"),
        ("best_day", "最佳单日", "percent"),
        ("worst_day", "最差单日", "percent"),
        ("tail_ratio", "尾部比", "number"),
        ("skew", "偏度", "number"),
        ("kurtosis", "峰度", "number"),
        ("value_at_risk", "VaR", "percent"),
        ("information_ratio", "信息比率", "number"),
    ]

    metrics_entries = []
    for key, label, fmt in metrics_items:
        value = metrics_payload.get(key, 0.0)
        value_text = f"{value:.2%}" if fmt == "percent" else f"{value:.2f}"
        if value > 0:
            value_cls = "text-red-600"
        elif value < 0:
            value_cls = "text-green-600"
        else:
            value_cls = "text-gray-700"
        metrics_entries.append(
            Div(
                Span(f"{label}:", cls="text-gray-500"),
                Span(value_text, id=f"metric_{key}", cls=f"{value_cls} ml-1"),
                cls="text-sm",
            )
        )
    metrics_text = Div(*metrics_entries, cls="flex flex-wrap gap-x-6 gap-y-2")

    filter_start_value = date_axis[0] if date_axis else ""
    filter_end_value = date_axis[-1] if date_axis else ""

    chart_id = f"chart_{portfolio_id}"
    date_axis_json = json.dumps(series_payload["date_axis"])
    total_series_json = json.dumps(series_payload["total"])
    benchmark_series_json = json.dumps(series_payload["benchmark"])
    daily_pnl_json = json.dumps(series_payload["daily_pnl"])
    trade_count_json = json.dumps(series_payload["trade_count"])
    metric_format_json = json.dumps({key: fmt for key, _, fmt in metrics_items})
    chart_script = Script(f"""
        var chart = echarts.init(document.getElementById('{chart_id}'));
        var dateAxis = {date_axis_json};
        var totalSeries = {total_series_json};
        var benchmarkSeries = {benchmark_series_json};
        var dailyPnlSeries = {daily_pnl_json};
        var tradeCountSeries = {trade_count_json};
        var seriesData = {{
            dateAxis: dateAxis,
            total: totalSeries,
            benchmark: benchmarkSeries,
            dailyPnl: dailyPnlSeries,
            tradeCount: tradeCountSeries
        }};
        var option = {{
            tooltip: {{ trigger: 'axis' }},
            grid: [
                {{ left: 60, right: 20, top: 40, height: 200 }},
                {{ left: 60, right: 20, top: 260, height: 110 }},
                {{ left: 60, right: 20, top: 400, height: 120 }}
            ],
            xAxis: [
                {{ type: 'category', data: dateAxis, boundaryGap: false, axisLabel: {{ show: false }} }},
                {{ type: 'category', data: dateAxis, boundaryGap: true, axisLabel: {{ show: false }}, gridIndex: 1 }},
                {{ type: 'category', data: dateAxis, boundaryGap: true, axisLabel: {{ show: true }}, gridIndex: 2 }}
            ],
            yAxis: [
                {{ type: 'value', scale: true }},
                {{ type: 'value', scale: true, gridIndex: 1 }},
                {{ type: 'value', scale: true, gridIndex: 2 }}
            ],
            dataZoom: [
                {{ type: 'inside', xAxisIndex: [0, 1, 2] }},
                {{ type: 'slider', xAxisIndex: [0, 1, 2], bottom: 0 }}
            ],
            series: [
                {{
                    name: '总资产',
                    type: 'line',
                    data: totalSeries,
                    smooth: true,
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    itemStyle: {{ color: '#d9534f' }}
                }},
                {{
                    name: '基准',
                    type: 'line',
                    data: benchmarkSeries,
                    smooth: true,
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    lineStyle: {{ type: 'dashed' }},
                    itemStyle: {{ color: '#9ca3af' }}
                }},
                {{
                    name: '每日盈亏',
                    type: 'bar',
                    data: dailyPnlSeries,
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    itemStyle: {{ color: '#5b8ff9' }}
                }},
                {{
                    name: '每日交易',
                    type: 'bar',
                    data: tradeCountSeries,
                    xAxisIndex: 2,
                    yAxisIndex: 2,
                    itemStyle: {{ color: '#30bf78' }}
                }}
            ]
        }};
        chart.setOption(option);
        window.addEventListener('resize', function() {{ chart.resize(); }});

        function fmtPercent(v) {{
            if (v === null || v === undefined || isNaN(v)) return "0.00%";
            return (v * 100).toFixed(2) + "%";
        }}

        function fmtNumber(v) {{
            if (v === null || v === undefined || isNaN(v)) return "0.00";
            return Number(v).toFixed(2);
        }}

        function fmtValue(v, fmt) {{
            if (fmt === "percent") return fmtPercent(v);
            return fmtNumber(v);
        }}

        function metricColor(v) {{
            if (v > 0) return "text-red-600";
            if (v < 0) return "text-green-600";
            return "text-gray-700";
        }}

        function updateMetric(key, value, fmt) {{
            var el = document.getElementById('metric_' + key);
            if (!el) return;
            el.textContent = fmtValue(value, fmt);
            el.className = metricColor(value) + " ml-1";
        }}

        function getDateValue(id) {{
            var el = document.getElementById(id);
            return el ? el.value : "";
        }}

        function filterSeries(series) {{
            var start = getDateValue("filter_start");
            var end = getDateValue("filter_end");
            var filtered = {{
                dateAxis: [],
                total: [],
                benchmark: [],
                dailyPnl: [],
                tradeCount: []
            }};
            for (var i = 0; i < series.dateAxis.length; i++) {{
                var dt = series.dateAxis[i];
                if (start && dt < start) continue;
                if (end && dt > end) continue;
                filtered.dateAxis.push(dt);
                filtered.total.push(series.total[i]);
                filtered.benchmark.push(series.benchmark[i]);
                filtered.dailyPnl.push(series.dailyPnl[i]);
                filtered.tradeCount.push(series.tradeCount[i]);
            }}
            return filtered;
        }}

        function updateChart(series) {{
            var filtered = filterSeries(series);
            chart.setOption({{
                xAxis: [
                    {{ data: filtered.dateAxis }},
                    {{ data: filtered.dateAxis }},
                    {{ data: filtered.dateAxis }}
                ],
                series: [
                    {{ data: filtered.total }},
                    {{ data: filtered.benchmark }},
                    {{ data: filtered.dailyPnl }},
                    {{ data: filtered.tradeCount }}
                ]
            }});
        }}

        var filterStart = document.getElementById("filter_start");
        if (filterStart) {{
            filterStart.addEventListener("change", function() {{ updateChart(seriesData); }});
        }}
        var filterEnd = document.getElementById("filter_end");
        if (filterEnd) {{
            filterEnd.addEventListener("change", function() {{ updateChart(seriesData); }});
        }}
        updateChart(seriesData);

        function renderTrades(trades) {{
            var body = document.getElementById('trade_table_body');
            if (!body) return;
            var html = "";
            for (var i = 0; i < trades.length; i++) {{
                var t = trades[i];
                var color = t.side_value === 1 ? "text-red-500" : "text-green-500";
                html += "<tr>" +
                    "<td>" + t.tm + "</td>" +
                    "<td>" + t.asset + "</td>" +
                    "<td class='" + color + "'>" + t.side + "</td>" +
                    "<td>" + fmtNumber(t.price) + "</td>" +
                    "<td>" + fmtNumber(t.shares) + "</td>" +
                    "<td>" + fmtNumber(t.amount) + "</td>" +
                    "<td>" + fmtNumber(t.fee) + "</td>" +
                    "</tr>";
            }}
            body.innerHTML = html;
        }}

        function renderDailySummary(rows) {{
            var body = document.getElementById('daily_summary_body');
            if (!body) return;
            var html = "";
            for (var i = 0; i < rows.length; i++) {{
                var r = rows[i];
                html += "<tr>" +
                    "<td>" + r.dt + "</td>" +
                    "<td>" + fmtNumber(r.cash) + "</td>" +
                    "<td>" + fmtNumber(r.market_value) + "</td>" +
                    "<td>" + fmtNumber(r.total) + "</td>" +
                    "<td>" + fmtNumber(r.daily_pnl) + "</td>" +
                    "<td>" + fmtPercent(r.daily_return) + "</td>" +
                    "</tr>";
            }}
            body.innerHTML = html;
        }}

        function renderPositions(rows) {{
            var body = document.getElementById('positions_body');
            if (!body) return;
            var html = "";
            var lastDate = null;
            for (var i = 0; i < rows.length; i++) {{
                var r = rows[i];
                if (lastDate !== r.dt) {{
                    html += "<tr><td colspan='6' class='bg-gray-50 font-semibold'>" + r.dt + "</td></tr>";
                    lastDate = r.dt;
                }}
                html += "<tr>" +
                    "<td>" + r.asset + "</td>" +
                    "<td>" + fmtNumber(r.shares) + "</td>" +
                    "<td>" + fmtNumber(r.avail) + "</td>" +
                    "<td>" + fmtNumber(r.price) + "</td>" +
                    "<td>" + fmtNumber(r.mv) + "</td>" +
                    "<td>" + fmtNumber(r.profit) + "</td>" +
                    "</tr>";
            }}
            body.innerHTML = html;
        }}

        function renderLogs(rows) {{
            var logEl = document.getElementById('log_output');
            if (!logEl) return;
            var lines = [];
            for (var i = 0; i < rows.length; i++) {{
                var r = rows[i];
                lines.push(r.dt + " | " + r.key + " | " + r.value + " " + (r.extra || ""));
            }}
            logEl.textContent = lines.join("\\n");
        }}

        var wsScheme = window.location.protocol === "https:" ? "wss" : "ws";
        var wsUrl = wsScheme + "://" + window.location.host + "/strategy/backtest/{portfolio_id}/ws";
        var ws = new WebSocket(wsUrl);

        ws.onmessage = function(event) {{
            try {{
                var data = JSON.parse(event.data);
                if (data.series) {{
                    var series = data.series;
                    seriesData = {{
                        dateAxis: series.date_axis,
                        total: series.total,
                        benchmark: series.benchmark,
                        dailyPnl: series.daily_pnl,
                        tradeCount: series.trade_count
                    }};
                    updateChart(seriesData);
                }}
                if (data.metrics) {{
                    var formats = {metric_format_json};
                    for (var key in formats) {{
                        updateMetric(key, data.metrics[key], formats[key]);
                    }}
                }}
                if (data.trades) {{
                    renderTrades(data.trades);
                }}
                if (data.daily_summary) {{
                    renderDailySummary(data.daily_summary);
                }}
                if (data.positions) {{
                    renderPositions(data.positions);
                }}
                if (data.logs) {{
                    renderLogs(data.logs);
                }}
                var statusEl = document.getElementById('backtest_status');
                if (statusEl && data.status) {{
                    statusEl.textContent = data.status === 'running' ? '回测进行中，实时推送中' : '回测已完成';
                }}
                if (data.status && data.status !== 'running') {{
                    ws.close();
                }}
            }} catch (e) {{}}
        }};
    """)

    status_badge = Div(
        Div(
            "回测进行中，实时推送中"
            if is_running
            else "回测已完成",
            id="backtest_status",
            cls="text-sm text-gray-500"
        ),
        cls="mb-4"
    )

    trade_rows = _build_trade_rows(portfolio_id, limit=200)
    positions_rows = _build_daily_positions(portfolio_id)
    log_rows = _build_log_rows(portfolio_id, limit=200)
    trade_table = Table(
        Thead(Tr(Th("时间"), Th("标的"), Th("方向"), Th("价格"), Th("数量"), Th("成交额"), Th("费用"))),
        Tbody(
            *[
                Tr(
                    Td(row["tm"]),
                    Td(row["asset"]),
                    Td(
                        row["side"],
                        cls="text-red-500"
                        if row["side_value"] == OrderSide.BUY
                        else "text-green-500",
                    ),
                    Td(f"{row['price']:.2f}"),
                    Td(f"{row['shares']:.2f}"),
                    Td(f"{row['amount']:.2f}"),
                    Td(f"{row['fee']:.2f}"),
                )
                for row in trade_rows
            ],
            id="trade_table_body",
        ),
        cls=TableT.striped + " text-xs"
    )
    position_trs = []
    last_date = None
    for row in positions_rows:
        dt = row.get("dt")
        if dt != last_date:
            position_trs.append(
                Tr(Td(dt, colspan="6", cls="bg-gray-50 font-semibold"))
            )
            last_date = dt
        position_trs.append(
            Tr(
                Td(row.get("asset", "")),
                Td(f"{row.get('shares', 0):.2f}"),
                Td(f"{row.get('avail', 0):.2f}"),
                Td(f"{row.get('price', 0):.2f}"),
                Td(f"{row.get('mv', 0):.2f}"),
                Td(f"{row.get('profit', 0):.2f}"),
            )
        )
    positions_table = Table(
        Thead(Tr(Th("标的"), Th("持仓"), Th("可用"), Th("价格"), Th("市值"), Th("浮盈"))),
        Tbody(*position_trs, id="positions_body"),
        cls=TableT.striped + " text-xs"
    )
    log_lines = [
        f"{row.get('dt', '')} | {row.get('key', '')} | {row.get('value', '')} {row.get('extra', '')}"
        for row in log_rows
    ]

    layout.main_block = lambda: Div(
        Script(src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"),

        Div(
            Div(
                A("← 返回策略详情", href="javascript:history.back()", cls="text-gray-500 hover:text-gray-800 mb-4 inline-block"),
                H1("回测报告", cls="text-3xl font-bold mb-2"),
                status_badge,

                Div(
                    H3("收益概述", cls="text-xl font-bold mb-3"),
                    metrics_text,
                    cls="mb-4",
                    id="overview"
                ),

                Div(
                    H3("收益曲线", cls="text-xl font-bold mb-4"),
                    Div(
                        Div(
                            Label("开始日期", cls="block text-xs text-gray-500 mb-1"),
                            Input(
                                id="filter_start",
                                type="date",
                                value=filter_start_value,
                                cls="input input-sm",
                            ),
                            cls="flex flex-col"
                        ),
                        Div(
                            Label("结束日期", cls="block text-xs text-gray-500 mb-1"),
                            Input(
                                id="filter_end",
                                type="date",
                                value=filter_end_value,
                                cls="input input-sm",
                            ),
                            cls="flex flex-col"
                        ),
                        cls="flex flex-wrap gap-4 mb-4"
                    ),
                    Div(id=chart_id, cls="w-full h-[560px] bg-white p-4 rounded-xl shadow-sm border border-gray-100"),
                    chart_script,
                    cls="mt-6"
                ),

                Div(
                    H3("交易详情", cls="text-xl font-bold mb-4"),
                    trade_table,
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6",
                    id="trades"
                ),

                Div(
                    H3("每日持仓", cls="text-xl font-bold mb-4"),
                    positions_table,
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6",
                    id="positions"
                ),

                Div(
                    H3("日志输出", cls="text-xl font-bold mb-4"),
                    Pre("\n".join(log_lines), id="log_output", cls="text-xs whitespace-pre-wrap"),
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6",
                    id="logs"
                ),

                cls="max-w-6xl mx-auto py-8"
            )
        )
    )

    return layout.render()


async def backtest_ws(websocket: WebSocket):
    """回测报告 websocket 推送。

    Args:
        websocket: WebSocket 连接
    """
    await websocket.accept()
    portfolio_id = websocket.path_params.get("portfolio_id")
    if not portfolio_id:
        await websocket.close(code=1008)
        return
    try:
        while True:
            portfolio = db.get_portfolio(portfolio_id)
            status = "running" if portfolio is None or portfolio.status else "finished"
            date_axis = _build_date_axis(portfolio_id)
            payload = {
                "status": status,
                "metrics": _build_metrics_payload(portfolio_id),
                "series": _build_series_payload(portfolio_id, date_axis),
                "trades": _build_trade_rows(portfolio_id, limit=200),
                "daily_summary": _build_daily_summary(portfolio_id),
                "positions": _build_daily_positions(portfolio_id),
                "logs": _build_log_rows(portfolio_id, limit=200),
            }
            await websocket.send_text(json.dumps(payload))
            if status != "running":
                await websocket.close()
                return
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        return


strategy_app.add_websocket_route("/backtest/{portfolio_id}/ws", backtest_ws)


# 配置对话框路由
@rt("/scan/config-modal")
def config_modal_route(req):
    scan_dir = strategy_loader.get_scan_directory()
    return _config_modal_html(scan_dir, is_error=False)


# 扫描确认对话框
@rt("/scan/confirm")
def scan_confirm_modal(req):
    scan_dir = strategy_loader.get_scan_directory()

    # 如果未配置目录，显示配置对话框
    if not scan_dir:
        return _config_modal_html(scan_dir, is_error=True)

    return Modal(
        ModalTitle("确认扫描"),
        ModalBody(
            P(f"确定要扫描策略目录吗？", cls="text-gray-700"),
            P(f"当前扫描目录: {scan_dir}", cls="text-sm text-gray-500 mt-2"),
        ),
        ModalFooter(
            Button(
                "取消",
                type="button",
                cls="px-4 py-2 text-gray-600 hover:text-gray-800",
                onclick="document.getElementById('modal-container').innerHTML=''"
            ),
            Button(
                "确定扫描",
                type="button",
                cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700",
                hx_post="/strategy/scan/run",
                hx_target="#modal-container",
            ),
        ),
        id="scan-confirm-modal"
    )


# 执行扫描
@rt("/scan/run", methods=["POST"])
def run_scan(req):
    try:
        strategies = strategy_loader.scan_and_cache()
        return Modal(
            ModalTitle("扫描完成"),
            ModalBody(
                P(f"成功发现 {len(strategies)} 个策略", cls="text-green-600 font-medium"),
                P("页面即将刷新...", cls="text-sm text-gray-500 mt-2"),
            ),
            ModalFooter(
                Button(
                    "确定",
                    type="button",
                    cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700",
                    onclick="location.reload()"
                ),
            ),
            id="scan-result-modal"
        )
    except Exception as e:
        logger.error(f"Failed to scan strategies: {e}")
        return Modal(
            ModalTitle("扫描失败"),
            ModalBody(
                P(f"错误: {str(e)}", cls="text-red-600"),
            ),
            ModalFooter(
                Button(
                    "关闭",
                    type="button",
                    cls="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700",
                    onclick="document.getElementById('modal-container').innerHTML=''"
                ),
            ),
            id="scan-error-modal"
        )


def _config_modal_html(scan_dir: str, is_error: bool = False):
    """配置对话框HTML"""
    title = "请先配置扫描目录" if is_error else "配置扫描目录"
    message = P("您尚未配置策略扫描目录，请先设置。", cls="text-red-600 mb-4") if is_error else ""

    return Div(
        Div(
            Div(
                H3(title, cls="text-lg font-semibold text-gray-900 mb-4"),
                message,
                Div(
                    Div(
                        Input(
                            id="scan-dir-input",
                            value=scan_dir,
                            placeholder="请输入绝对路径，例如: /Users/name/strategies",
                            cls="flex-1 px-3 py-2 border border-gray-300 rounded-l-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                        ),
                        Button(
                            "...",
                            type="button",
                            cls="px-3 py-2 bg-gray-100 border border-l-0 border-gray-300 rounded-r-lg hover:bg-gray-200 text-gray-700",
                            onclick="document.getElementById('dir-file-input').click()",
                        ),
                        cls="flex mb-4"
                    ),
                    Input(
                        id="dir-file-input",
                        type="file",
                        webkitdirectory="",
                        directory="",
                        cls="hidden",
                        onchange="document.getElementById('scan-dir-input').value = this.files[0]?.path || ''"
                    ),
                    cls="mb-4"
                ),
                Div(
                    Button(
                        "取消",
                        type="button",
                        cls="px-4 py-2 text-gray-600 hover:text-gray-800",
                        onclick="document.getElementById('modal-container').innerHTML=''"
                    ),
                    Button(
                        "保存",
                        type="button",
                        cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 ml-2",
                        hx_post="/strategy/scan/config",
                        hx_include="#scan-dir-input",
                        hx_target="#modal-container",
                    ),
                    cls="flex justify-end"
                ),
                cls="bg-white rounded-lg shadow-xl p-6 w-[480px]"
            ),
            cls="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50"
        ),
        id="config-modal"
    )


# API 路由：配置扫描目录
@rt("/scan/config", methods=["POST"])
def save_scan_config(req):
    from pathlib import Path

    try:
        form = req.form()
        directory = form.get("scan-dir-input", "").strip()

        if not directory:
            return Div(
                P("目录不能为空", cls="text-red-600 mb-2"),
                _config_modal_html(directory, is_error=False),
                id="config-error"
            )

        # 转换 ~ 为绝对路径
        if directory.startswith("~"):
            directory = str(Path(directory).expanduser())

        # 检查是否为绝对路径
        if not directory.startswith("/"):
            return Div(
                P("必须使用绝对路径，例如: /Users/name/strategies", cls="text-red-600 mb-2"),
                _config_modal_html(directory, is_error=False),
                id="config-error"
            )

        # 检查目录是否存在
        path = Path(directory)
        if not path.exists():
            return Div(
                P(f"目录不存在: {directory}", cls="text-red-600 mb-2"),
                _config_modal_html(directory, is_error=False),
                id="config-error"
            )

        if not path.is_dir():
            return Div(
                P(f"路径不是目录: {directory}", cls="text-red-600 mb-2"),
                _config_modal_html(directory, is_error=False),
                id="config-error"
            )

        strategy_loader.set_scan_directory(directory)

        return Modal(
            ModalTitle("配置已保存"),
            ModalBody(
                P(f"扫描目录已设置为: {directory}", cls="text-green-600"),
                P("页面即将刷新...", cls="text-sm text-gray-500 mt-2"),
            ),
            ModalFooter(
                Button(
                    "确定",
                    type="button",
                    cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700",
                    onclick="location.reload()"
                ),
            ),
            id="config-success-modal"
        )
    except Exception as e:
        logger.error(f"Failed to set scan directory: {e}")
        return Div(
            P(f"保存失败: {str(e)}", cls="text-red-600 mb-2"),
            _config_modal_html("", is_error=False),
            id="config-error"
        )
