import asyncio
import json
import re
import uuid

import arrow
import polars as pl
from fasthtml.common import *
from monsterui.all import *

from pyqmt.core.enums import FrameType
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


def _build_chart_data(portfolio_id: str) -> list[list[float]]:
    """构建资产曲线数据。

    Args:
        portfolio_id: 组合 ID

    Returns:
        list[list[float]]: [timestamp_ms, total] 数据
    """
    assets_df = db.query_assets(portfolio_id)
    chart_data = []
    if not assets_df.is_empty():
        for row in assets_df.iter_rows(named=True):
            ts = arrow.get(row["dt"]).timestamp() * 1000
            chart_data.append([ts, row["total"]])
    return chart_data


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
    return {
        "annual_return": annual_return,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "total_returns": total_returns,
    }


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

    chart_data = _build_chart_data(portfolio_id)

    metrics_cards = Grid(
        Card(CardBody(H2(Span(f"{metrics_payload['annual_return']:.2%}", id="annual_return_val"), cls="text-2xl font-bold text-red-500"), P("年化收益", cls="text-xs text-gray-500"))),
        Card(CardBody(H2(Span(f"{metrics_payload['sharpe']:.2f}", id="sharpe_val"), cls="text-2xl font-bold"), P("夏普比率", cls="text-xs text-gray-500"))),
        Card(CardBody(H2(Span(f"{metrics_payload['max_drawdown']:.2%}", id="max_drawdown_val"), cls="text-2xl font-bold text-green-500"), P("最大回撤", cls="text-xs text-gray-500"))),
        Card(CardBody(H2(Span(f"{metrics_payload['total_returns']:.2%}", id="total_returns_val"), cls="text-2xl font-bold"), P("累计收益", cls="text-xs text-gray-500"))),
        cols=2, cols_md=4, gap=4
    )

    # Chart Script (ECharts)
    chart_id = f"chart_{portfolio_id}"
    chart_script = Script(f"""
        var chart = echarts.init(document.getElementById('{chart_id}'));
        var option = {{
            title: {{ text: '资产曲线' }},
            tooltip: {{ trigger: 'axis' }},
            xAxis: {{ type: 'time' }},
            yAxis: {{ type: 'value', scale: true }},
            series: [{{
                name: '总资产',
                type: 'line',
                data: {json.dumps(chart_data)},
                smooth: true,
                itemStyle: {{ color: '#d9534f' }}
            }}]
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

        async function refreshBacktest() {{
            try {{
                var resp = await fetch('/strategy/backtest/{portfolio_id}/live');
                if (!resp.ok) return;
                var data = await resp.json();
                if (data.chart_data) {{
                    chart.setOption({{ series: [{{ data: data.chart_data }}] }});
                }}
                if (data.metrics) {{
                    var annualEl = document.getElementById('annual_return_val');
                    var sharpeEl = document.getElementById('sharpe_val');
                    var mddEl = document.getElementById('max_drawdown_val');
                    var totalEl = document.getElementById('total_returns_val');
                    if (annualEl) annualEl.textContent = fmtPercent(data.metrics.annual_return);
                    if (sharpeEl) sharpeEl.textContent = fmtNumber(data.metrics.sharpe);
                    if (mddEl) mddEl.textContent = fmtPercent(data.metrics.max_drawdown);
                    if (totalEl) totalEl.textContent = fmtPercent(data.metrics.total_returns);
                }}
                var statusEl = document.getElementById('backtest_status');
                if (statusEl && data.status) {{
                    statusEl.textContent = data.status === 'running' ? '回测进行中，页面每 3 秒自动刷新' : '回测已完成';
                }}
                if (data.status && data.status !== 'running') {{
                    clearInterval(window.__bt_timer__);
                }}
            }} catch (e) {{}}
        }}

        window.__bt_timer__ = setInterval(refreshBacktest, 3000);
        refreshBacktest();
    """)

    status_badge = Div(
        Div(
            "回测进行中，页面每 3 秒自动刷新"
            if is_running
            else "回测已完成",
            id="backtest_status",
            cls="text-sm text-gray-500"
        ),
        cls="mb-4"
    )

    # Trades Table
    trades_df = db.trades_all(portfolio_id)
    trade_rows = []
    if not trades_df.is_empty():
        # Limit to last 50 trades
        trades_df = trades_df.sort("tm", descending=True).head(50)
        for row in trades_df.iter_rows(named=True):
            color = "text-red-500" if row["side"] == "BUY" else "text-green-500"
            trade_rows.append(
                Tr(
                    Td(str(row["tm"])),
                    Td(row["asset"]),
                    Td(row["side"], cls=color),
                    Td(f"{row['price']:.2f}"),
                    Td(str(row["shares"])),
                    Td(f"{row['fee']:.2f}"),
                )
            )

    trade_table = Table(
        Thead(Tr(Th("时间"), Th("标的"), Th("方向"), Th("价格"), Th("数量"), Th("费用"))),
        Tbody(*trade_rows),
        cls=TableT.striped + " text-xs"
    )

    layout.main_block = lambda: Div(
        # ECharts CDN
        Script(src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"),

        Div(
            Div(
                A("← 返回策略详情", href="javascript:history.back()", cls="text-gray-500 hover:text-gray-800 mb-4 inline-block"),
                H1("回测报告", cls="text-3xl font-bold mb-2"),
                status_badge,

                metrics_cards,

                Div(id=chart_id, cls="w-full h-[400px] bg-white p-4 rounded-xl shadow-sm border border-gray-100 mt-6"),
                chart_script,

                Div(
                    H3("最近交易 (Top 50)", cls="text-xl font-bold mb-4"),
                    trade_table,
                    cls="bg-white p-6 rounded-lg shadow-sm border border-gray-100 mt-6"
                ),
                cls="max-w-6xl mx-auto py-8"
            )
        )
    )

    return layout.render()


@rt("/backtest/{portfolio_id}/live")
def backtest_live(req, session, portfolio_id: str):
    """返回回测实时数据。

    Args:
        req: 请求对象
        session: 会话对象
        portfolio_id: 组合 ID

    Returns:
        JSONResponse: 实时数据
    """
    portfolio = db.get_portfolio(portfolio_id)
    status = "running" if portfolio is None or portfolio.status else "finished"
    return JSONResponse(
        {
            "status": status,
            "metrics": _build_metrics_payload(portfolio_id),
            "chart_data": _build_chart_data(portfolio_id),
        }
    )
