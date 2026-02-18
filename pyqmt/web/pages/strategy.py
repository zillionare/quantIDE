import asyncio
import json
import re
import uuid

import arrow
import polars as pl
from fasthtml.common import *
from monsterui.all import *
from starlette.websockets import WebSocket, WebSocketDisconnect

from pyqmt.core.enums import FrameType, OrderSide
from pyqmt.data.models.calendar import calendar
from pyqmt.data.sqlite import db
from pyqmt.service.discovery import strategy_loader
from pyqmt.service.grid_search import GridSearch
from pyqmt.service.metrics import metrics
from pyqmt.service.runner import BacktestRunner
from pyqmt.web.layouts.main import MainLayout

strategy_app, rt = fast_app()

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
            "daily_pnl": [None for _ in date_axis],
            "buy_amount": [0 for _ in date_axis],
            "sell_amount": [0 for _ in date_axis],
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

    buy_by_date: dict[str, float] = {}
    sell_by_date: dict[str, float] = {}
    trades_df = db.trades_all(portfolio_id)
    if not trades_df.is_empty():
        for row in trades_df.iter_rows(named=True):
            tm = row.get("tm")
            if tm is None:
                continue
            dt = arrow.get(tm).format("YYYY-MM-DD")
            amount = row.get("amount")
            if amount is None:
                price = row.get("price") or 0
                shares = row.get("shares") or 0
                amount = price * shares
            side = row.get("side")
            if isinstance(side, OrderSide):
                side_value = side.value
            else:
                side_value = int(side) if side is not None else 0
            if side_value == OrderSide.BUY:
                buy_by_date[dt] = buy_by_date.get(dt, 0.0) + float(amount)
            elif side_value == OrderSide.SELL:
                sell_by_date[dt] = sell_by_date.get(dt, 0.0) + float(amount)

    total_series = []
    daily_pnl_series = []
    buy_series = []
    sell_series = []
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
        buy_series.append(buy_by_date.get(dt, 0.0))
        sell_series.append(-sell_by_date.get(dt, 0.0))

    return {
        "date_axis": date_axis,
        "total": total_series,
        "daily_pnl": daily_pnl_series,
        "buy_amount": buy_series,
        "sell_amount": sell_series,
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
    annual_return = _to_number(
        stats_dict.get("annual_return", stats_dict.get("cagr", 0.0))
    )
    sharpe = _to_number(stats_dict.get("sharpe", 0.0))
    max_drawdown = _to_number(stats_dict.get("max_drawdown", 0.0))
    total_returns = _to_number(
        stats_dict.get(
            "cumulative_return",
            stats_dict.get("total_return", stats_dict.get("total_returns", 0.0)),
        )
    )
    volatility = _to_number(
        stats_dict.get("volatility_ann", stats_dict.get("volatility", 0.0))
    )
    sortino = _to_number(stats_dict.get("sortino", 0.0))
    calmar = _to_number(stats_dict.get("calmar", 0.0))
    win_rate = _to_number(stats_dict.get("win_rate", 0.0))
    profit_factor = _to_number(stats_dict.get("profit_factor", 0.0))
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


def StrategyList(strategies: dict):
    cards = []
    for name, cls in strategies.items():
        doc = cls.__doc__ or "暂无描述"
        params = getattr(cls, "PARAMS", {})

        # 历史回测数量
        # portfolios = db.get_portfolios_by_strategy(name)
        history_count = 0 # len(portfolios) if not portfolios.is_empty() else 0

        cards.append(
            Card(
                CardHeader(
                    H3(name, cls="text-lg font-bold"),
                    P(doc, cls="text-sm text-gray-500 line-clamp-2"),
                ),
                CardBody(
                    Div(
                        P(f"参数: {', '.join(params.keys())}", cls="text-xs text-gray-400 mb-2"),
                        P(f"历史回测: {history_count} 次", cls="text-xs text-blue-500"),
                    )
                ),
                CardFooter(
                    Div(
                        Button("运行回测", type="submit",
                               hx_get=f"/strategy/{name}/backtest/modal",
                               hx_target="#modal-container",
                               cls=ButtonT.primary),
                        Button("网格搜索", type="submit",
                               hx_get=f"/strategy/{name}/grid_search/modal",
                               hx_target="#modal-container",
                               cls=ButtonT.secondary + " ml-2"),
                        cls="flex"
                    ),
                    Div(
                        A("查看详情", href=f"/strategy/{name}", cls="text-sm text-blue-600 hover:underline"),
                        cls="mt-2 text-right"
                    )
                ),
                cls="hover:shadow-lg transition-shadow duration-200"
            )
        )

    if not cards:
        return Div(
            Div(UkIcon("info", size=48), cls="text-gray-300 mb-4"),
            H3("暂无策略", cls="text-xl text-gray-500"),
            P("请在 pyqmt/strategies 目录下添加策略文件", cls="text-gray-400"),
            cls="flex flex-col items-center justify-center py-20 border-2 border-dashed border-gray-200 rounded-xl"
        )

    return Grid(*cards, cols=1, cols_md=2, cols_lg=3, gap=6)

@rt("/")
def index(req, session):
    layout = MainLayout(title="策略列表", user=session.get("auth"))

    # Load strategies
    workspace = "pyqmt/strategies"
    strategies = strategy_loader.load(workspace)

    layout.main_block = lambda: Div(
        Div(
            H2("我的策略", cls="text-2xl font-bold mb-6"),
            StrategyList(strategies),
            Div(id="modal-container"),
            cls="p-6"
        )
    )

    return layout.render()

@rt("/{name}")
def strategy_detail(req, session, name: str):
    layout = MainLayout(title=f"策略详情 - {name}", user=session.get("auth"))

    workspace = "pyqmt/strategies"
    strategies = strategy_loader.load(workspace)

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
    workspace = "pyqmt/strategies"
    strategies = strategy_loader.load(workspace)
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

        workspace = "pyqmt/strategies"
        strategies = strategy_loader.load(workspace)
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
    workspace = "pyqmt/strategies"
    strategies = strategy_loader.load(workspace)
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

        workspace = "pyqmt/strategies"
        strategies = strategy_loader.load(workspace)
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
        ("sharpe", "夏普比率", "number"),
        ("volatility", "波动率", "percent"),
        ("sortino", "索提诺比率", "number"),
        ("calmar", "卡玛比率", "number"),
        ("win_rate", "胜率", "percent"),
        ("profit_factor", "盈亏比", "number"),
    ]

    metrics_cards = Grid(
        *[
            Card(
                CardBody(
                    H2(
                        Span(
                            f"{metrics_payload.get(key, 0.0):.2%}"
                            if fmt == "percent"
                            else f"{metrics_payload.get(key, 0.0):.2f}",
                            id=f"metric_{key}",
                        ),
                        cls="text-2xl font-bold",
                    ),
                    P(label, cls="text-xs text-gray-500"),
                )
            )
            for key, label, fmt in metrics_items
        ],
        cols=2,
        cols_md=3,
        gap=4,
    )

    # Chart Script (ECharts)
    chart_id = f"chart_{portfolio_id}"
    date_axis_json = json.dumps(series_payload["date_axis"])
    total_series_json = json.dumps(series_payload["total"])
    daily_pnl_json = json.dumps(series_payload["daily_pnl"])
    buy_series_json = json.dumps(series_payload["buy_amount"])
    sell_series_json = json.dumps(series_payload["sell_amount"])
    metric_format_json = json.dumps({key: fmt for key, _, fmt in metrics_items})
    chart_script = Script(f"""
        var chart = echarts.init(document.getElementById('{chart_id}'));
        var dateAxis = {date_axis_json};
        var totalSeries = {total_series_json};
        var dailyPnlSeries = {daily_pnl_json};
        var buySeries = {buy_series_json};
        var sellSeries = {sell_series_json};
        var option = {{
            title: {{ text: '收益曲线' }},
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
                    name: '每日盈亏',
                    type: 'bar',
                    data: dailyPnlSeries,
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    itemStyle: {{ color: '#5b8ff9' }}
                }},
                {{
                    name: '每日买入',
                    type: 'bar',
                    data: buySeries,
                    xAxisIndex: 2,
                    yAxisIndex: 2,
                    stack: 'trade',
                    itemStyle: {{ color: '#f4664a' }}
                }},
                {{
                    name: '每日卖出',
                    type: 'bar',
                    data: sellSeries,
                    xAxisIndex: 2,
                    yAxisIndex: 2,
                    stack: 'trade',
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
                    chart.setOption({{
                        xAxis: [
                            {{ data: series.date_axis }},
                            {{ data: series.date_axis }},
                            {{ data: series.date_axis }}
                        ],
                        series: [
                            {{ data: series.total }},
                            {{ data: series.daily_pnl }},
                            {{ data: series.buy_amount }},
                            {{ data: series.sell_amount }}
                        ]
                    }});
                }}
                if (data.metrics) {{
                    var formats = {metric_format_json};
                    for (var key in formats) {{
                        var el = document.getElementById('metric_' + key);
                        if (el) {{
                            el.textContent = fmtValue(data.metrics[key], formats[key]);
                        }}
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

    daily_summary_rows = _build_daily_summary(portfolio_id)
    daily_summary_table = Table(
        Thead(
            Tr(
                Th("日期"),
                Th("现金"),
                Th("市值"),
                Th("总资产"),
                Th("每日盈亏"),
                Th("每日收益率"),
            )
        ),
        Tbody(
            *[
                Tr(
                    Td(row["dt"]),
                    Td(f"{row['cash']:.2f}"),
                    Td(f"{row['market_value']:.2f}"),
                    Td(f"{row['total']:.2f}"),
                    Td(f"{row['daily_pnl']:.2f}"),
                    Td(f"{row['daily_return']:.2%}"),
                )
                for row in daily_summary_rows
            ],
            id="daily_summary_body",
        ),
        cls=TableT.striped + " text-xs"
    )

    position_rows = _build_daily_positions(portfolio_id)
    position_table_rows = []
    last_position_date = None
    for row in position_rows:
        if last_position_date != row["dt"]:
            position_table_rows.append(
                Tr(
                    Td(row["dt"], colspan="6", cls="bg-gray-50 font-semibold"),
                )
            )
            last_position_date = row["dt"]
        position_table_rows.append(
            Tr(
                Td(row["asset"]),
                Td(f"{row['shares']:.2f}"),
                Td(f"{row['avail']:.2f}"),
                Td(f"{row['price']:.2f}"),
                Td(f"{row['mv']:.2f}"),
                Td(f"{row['profit']:.2f}"),
            )
        )
    positions_table = Table(
        Thead(
            Tr(
                Th("标的"),
                Th("持仓"),
                Th("可用"),
                Th("成本价"),
                Th("市值"),
                Th("浮动盈亏"),
            )
        ),
        Tbody(
            *position_table_rows,
            id="positions_body",
        ),
        cls=TableT.striped + " text-xs"
    )

    log_rows = _build_log_rows(portfolio_id, limit=200)
    log_text = "\n".join(
        [
            f"{row['dt']} | {row['key']} | {row['value']} {row['extra']}"
            for row in log_rows
        ]
    )

    layout.main_block = lambda: Div(
        Script(src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"),

        Div(
            Div(
                A("← 返回策略详情", href="javascript:history.back()", cls="text-gray-500 hover:text-gray-800 mb-4 inline-block"),
                H1("回测报告", cls="text-3xl font-bold mb-2"),
                status_badge,

                Div(
                    H3("收益概述", cls="text-xl font-bold mb-4"),
                    metrics_cards,
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100"
                ),

                Div(
                    H3("收益曲线", cls="text-xl font-bold mb-4"),
                    Div(id=chart_id, cls="w-full h-[560px] bg-white p-4 rounded-xl shadow-sm border border-gray-100"),
                    chart_script,
                    cls="mt-6"
                ),

                Div(
                    H3("交易详情", cls="text-xl font-bold mb-4"),
                    trade_table,
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6"
                ),

                Div(
                    H3("每日持仓 & 收益", cls="text-xl font-bold mb-4"),
                    daily_summary_table,
                    Div(positions_table, cls="mt-4"),
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6"
                ),

                Div(
                    H3("日志输出", cls="text-xl font-bold mb-4"),
                    Pre(
                        log_text,
                        id="log_output",
                        cls="bg-black text-green-400 text-xs p-4 rounded-lg overflow-auto h-[320px]",
                    ),
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6"
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
