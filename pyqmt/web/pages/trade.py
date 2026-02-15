"""交易模块页面"""
from typing import Any

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from pyqmt.core.enums import BrokerKind
from pyqmt.data.sqlite import Asset, Position
from pyqmt.service.registry import BrokerRegistry
from pyqmt.service.sim_broker import SimulationBroker
from pyqmt.web.layouts.main import MainLayout

trade_app, rt = fast_app()


def _get_registry(req) -> Any:
    return req.scope.get("registry")


def _build_asset_overview(asset: Asset | None) -> dict:
    if asset is None:
        return {
            "total": 0,
            "cash": 0,
            "frozen_cash": 0,
            "market_value": 0,
            "pnl": 0,
            "pnl_pct": 0,
        }

    pnl = asset.total - asset.principal
    ppnl = pnl / asset.principal if asset.principal else 0.0
    return {
        "total": asset.total,
        "cash": asset.cash,
        "frozen_cash": asset.frozen_cash,
        "market_value": asset.market_value,
        "pnl": pnl,
        "pnl_pct": ppnl,
    }


def AssetSummary(asset_overview: dict | None = None, **kwargs):
    metric_cls = "flex flex-col items-center justify-center p-4 text-red-400"
    val_cls = "text-xl font-bold"
    label_cls = "text-xs opacity-80"

    asset_overview = asset_overview or {}

    total = asset_overview.get("total")
    cash = asset_overview.get("cash")
    frozen_cash = asset_overview.get("frozen_cash")
    market_value = asset_overview.get("market_value")
    pnl = asset_overview.get("pnl")
    pnl_pct = asset_overview.get("pnl_pct")

    total_text = f"{total:,.2f}" if total is not None else "0.00"
    cash_text = f"{cash:,.2f}" if cash is not None else "0.00"
    frozen_cash_text = f"{frozen_cash:,.2f}" if frozen_cash is not None else "0.00"
    market_value_text = f"{market_value:,.2f}" if market_value is not None else "0.00"
    pnl_text = f"{pnl:,.2f}" if pnl is not None else "0.00"
    pnl_pct_text = f"{pnl_pct * 100:.2f}%" if pnl_pct is not None else "0.00%"

    return Div(
        Div(
            Div(P(total_text, cls=val_cls), P("总资产", cls=label_cls), cls=metric_cls),
            Div(P(cash_text, cls=val_cls), P("可用金额", cls=label_cls), cls=metric_cls),
            Div(
                P(frozen_cash_text, cls=val_cls),
                P("冻结金额", cls=label_cls),
                cls=metric_cls,
            ),
            Div(
                P(market_value_text, cls=val_cls),
                P("总市值", cls=label_cls),
                cls=metric_cls,
            ),
            Div(
                P(pnl_text, cls="text-xl font-bold text-yellow-300"),
                P("盈亏", cls=label_cls),
                cls=metric_cls,
            ),
            Div(
                P(pnl_pct_text, cls="text-xl font-bold text-yellow-300"),
                P("盈亏比例", cls=label_cls),
                cls=metric_cls,
            ),
            cls="flex justify-around items-center h-24",
        ),
        style="background: linear-gradient(90deg, #d32f2f 0%, #ff5252 100%); border-radius: 12px;",
        cls="shadow-lg mb-6",
        id="asset-summary",
        **kwargs,
    )


def PositionInfo(positions: list[Position] | None = None, portfolio_id: str = ""):
    headers = [
        "证券代码",
        "证券名称",
        "当前持股",
        "可用股数",
        "冻结数量",
        "市值",
        "最新价",
        "成本价",
        "盈亏",
        "盈亏比例",
    ]

    rows = []
    if positions:
        for p in positions:
            pnl_pct = (p.profit / (p.mv - p.profit)) if (p.mv - p.profit) != 0 else 0
            current_price = p.mv / p.shares if p.shares > 0 else 0
            color_cls = "text-red-500" if p.profit > 0 else ("text-green-500" if p.profit < 0 else "")

            rows.append(
                Tr(
                    Td(p.asset),
                    Td(p.asset),
                    Td(f"{p.shares:,}"),
                    Td(f"{p.avail:,}"),
                    Td(f"{p.shares - p.avail:,}"),
                    Td(f"{p.mv:,.2f}"),
                    Td(f"{current_price:,.2f}"),
                    Td(f"{p.price:,.2f}"),
                    Td(f"{p.profit:,.2f}", cls=color_cls),
                    Td(f"{pnl_pct * 100:.2f}%", cls=color_cls),
                )
            )
    else:
        rows.append(
            Tr(
                Td(
                    "暂无持仓",
                    colspan=len(headers),
                    cls="text-center py-10 text-gray-500",
                )
            )
        )

    return Div(
        Div(
            Div(
                UkIcon("briefcase", cls="mr-2 text-red-600"),
                H3("持仓信息", cls="text-lg font-bold mb-0"),
                cls="flex items-center",
            ),
            Button(
                UkIcon("refresh-cw", size=16),
                " 刷新",
                cls="uk-button-primary uk-button-small flex items-center gap-1",
                style="background-color: #d32f2f;",
                hx_get=f"/trade/simulation/{portfolio_id}/positions",
                hx_target="#position-info",
                hx_swap="outerHTML",
            ),
            cls="flex justify-between items-center mb-4",
        ),
        Table(
            Thead(Tr(*[Th(h, cls="text-xs font-bold") for h in headers])),
            Tbody(*rows),
            cls="uk-table uk-table-divider uk-table-small uk-table-hover border border-gray-100 rounded-lg overflow-hidden",
        ),
        id="position-info",
        cls="bg-white p-4 rounded-xl shadow-sm border border-gray-100 h-full",
    )


def TradePanel(portfolio_id: str):
    input_cls = "uk-input uk-form-small rounded-md mb-4 mt-1"
    label_cls = "text-xs text-red-100 block"
    ratio_btn_cls = "uk-button uk-button-default uk-button-small text-white text-[10px] px-2 min-w-[36px] border-white/30 hover:bg-white/20"

    return Div(
        Form(
            Div(
                Ul(
                    Li(
                        A(
                            UkIcon("plus-circle", size=14),
                            " 买入",
                            href="#",
                            cls="flex items-center text-xs",
                            onclick="document.getElementById('side').value='BUY'; this.closest('ul').querySelectorAll('li').forEach(li => li.classList.remove('uk-active')); this.parentElement.classList.add('uk-active');",
                        ),
                        cls="uk-active",
                    ),
                    Li(
                        A(
                            UkIcon("minus-circle", size=14),
                            " 卖出",
                            href="#",
                            cls="flex items-center text-xs",
                            onclick="document.getElementById('side').value='SELL'; this.closest('ul').querySelectorAll('li').forEach(li => li.classList.remove('uk-active')); this.parentElement.classList.add('uk-active');",
                        )
                    ),
                    cls="uk-tab uk-child-width-expand mb-4 bg-white/10 rounded-lg p-0.5",
                    style="border-bottom: none;",
                )
            ),
            Input(type="hidden", id="side", name="side", value="BUY"),
            Div(
                Label("股票代码", cls=label_cls),
                Input(
                    name="asset",
                    placeholder="例如: 000001.SZ",
                    cls=input_cls,
                    required=True,
                ),
                Label("买入价格", cls=label_cls),
                Input(
                    name="price",
                    type="number",
                    step="0.01",
                    value="0.00",
                    cls=input_cls,
                ),
                Label("仓位选择", cls=label_cls),
                Div(
                    Button("满仓", type="button", cls=ratio_btn_cls),
                    Button("1/2", type="button", cls=ratio_btn_cls),
                    Button("1/3", type="button", cls=ratio_btn_cls),
                    Button("1/4", type="button", cls=ratio_btn_cls),
                    Button("1/10", type="button", cls=ratio_btn_cls),
                    cls="flex gap-1 mb-4 mt-1",
                ),
                Label("交易数量 (股)", cls=label_cls),
                Input(
                    name="shares", type="number", step="100", cls=input_cls, required=True
                ),
                Button(
                    UkIcon("shopping-cart", size=16),
                    " 下单",
                    type="submit",
                    cls="uk-button-default w-full bg-white text-red-700 font-bold border-none hover:bg-gray-100",
                ),
            ),
            hx_post=f"/trade/simulation/{portfolio_id}/order",
            hx_target="#position-info",
            hx_swap="outerHTML",
        ),
        cls="p-6 rounded-xl shadow-xl h-full",
    )


def PortfolioList(portfolios: list[dict]):
    headers = ["账户ID", "账户名称", "初始资金", "总资产", "收益率", "状态", "操作"]

    rows = []
    if portfolios:
        for pf in portfolios:
            portfolio_id = pf.get("portfolio_id", "")
            name = pf.get("name", "")
            principal = pf.get("principal", 0)
            total = pf.get("total", 0)
            pnl_pct = pf.get("pnl_pct", 0)
            status = pf.get("status", True)

            status_text = "运行中" if status else "已停止"
            status_cls = "text-green-600" if status else "text-gray-500"

            rows.append(
                Tr(
                    Td(portfolio_id[:8] + "..."),
                    Td(name),
                    Td(f"{principal:,.2f}"),
                    Td(f"{total:,.2f}"),
                    Td(f"{pnl_pct * 100:.2f}%", cls="text-red-500" if pnl_pct > 0 else "text-green-500"),
                    Td(status_text, cls=status_cls),
                    Td(
                        A(
                            "详情",
                            href=f"/trade/simulation/{portfolio_id}",
                            cls="uk-button uk-button-primary uk-button-small",
                            style="background-color: #d32f2f;",
                        ),
                        cls="flex gap-2",
                    ),
                )
            )
    else:
        rows.append(
            Tr(
                Td(
                    "暂无仿真账户",
                    colspan=len(headers),
                    cls="text-center py-10 text-gray-500",
                )
            )
        )

    return Div(
        Div(
            H3("仿真账户列表", cls="text-lg font-bold mb-0"),
            Button(
                UkIcon("plus", size=16),
                " 创建账户",
                cls="uk-button-primary uk-button-small flex items-center gap-1",
                style="background-color: #d32f2f;",
                hx_get="/trade/simulation/create",
                hx_target="#create-modal",
                hx_swap="innerHTML",
            ),
            cls="flex justify-between items-center mb-4",
        ),
        Table(
            Thead(Tr(*[Th(h, cls="text-xs font-bold") for h in headers])),
            Tbody(*rows),
            cls="uk-table uk-table-divider uk-table-small uk-table-hover border border-gray-100 rounded-lg overflow-hidden",
        ),
        cls="bg-white p-4 rounded-xl shadow-sm border border-gray-100",
    )


def CreatePortfolioModal():
    return Div(
        Div(
            H3("创建仿真账户", cls="text-lg font-bold mb-4"),
            Form(
                Div(
                    Label("账户名称", cls="block text-sm font-medium mb-1"),
                    Input(
                        name="name",
                        placeholder="例如: 策略A",
                        cls="uk-input uk-form-small rounded-md",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("初始资金", cls="block text-sm font-medium mb-1"),
                    Input(
                        name="principal",
                        type="number",
                        value="1000000",
                        cls="uk-input uk-form-small rounded-md",
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("佣金费率", cls="block text-sm font-medium mb-1"),
                    Input(
                        name="commission",
                        type="number",
                        step="0.0001",
                        value="0.0001",
                        cls="uk-input uk-form-small rounded-md",
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("市值更新间隔 (秒)", cls="block text-sm font-medium mb-1"),
                    Select(
                        Option("3 秒", value="3"),
                        Option("5 秒", value="5"),
                        Option("10 秒 (推荐)", value="10", selected=True),
                        cls="uk-select uk-form-small rounded-md",
                        name="market_value_update_interval",
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("描述信息", cls="block text-sm font-medium mb-1"),
                    Textarea(
                        name="info",
                        placeholder="账户描述（可选）",
                        cls="uk-textarea uk-form-small rounded-md",
                        rows=3,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Button(
                        "取消",
                        type="button",
                        cls="uk-button uk-button-default",
                        onclick="document.getElementById('create-modal').innerHTML = ''",
                    ),
                    Button(
                        "创建",
                        type="submit",
                        cls="uk-button uk-button-primary ml-2",
                        style="background-color: #d32f2f;",
                    ),
                    cls="flex justify-end gap-2",
                ),
                hx_post="/trade/simulation/create",
                hx_target="#portfolio-list",
                hx_swap="outerHTML",
            ),
            cls="bg-white p-6 rounded-xl shadow-xl max-w-md",
        ),
        cls="fixed inset-0 bg-black/50 flex items-center justify-center z-50",
        id="create-modal-content",
    )


@rt("/")
def simulation_list(req, session):
    layout = MainLayout(title="仿真交易", user=session.get("auth"))
    layout.sidebar_menu = [
        {
            "title": "交易",
            "children": [
                {"title": "仿真", "url": "/trade/simulation"},
                {"title": "实盘", "url": "/trade/live"},
            ],
        }
    ]

    reg = _get_registry(req)
    portfolios = []

    if reg:
        brokers = reg.list_by_kind(BrokerKind.SIMULATION)
        for broker_info in brokers:
            portfolio_id = broker_info.get("id")
            broker = reg.get(BrokerKind.SIMULATION, portfolio_id)
            if broker:
                asset = broker.asset if hasattr(broker, "asset") else None
                pnl_pct = 0
                total = 0
                principal = 0
                if asset:
                    total = asset.total
                    principal = asset.principal
                    pnl_pct = (total - principal) / principal if principal > 0 else 0

                portfolios.append({
                    "portfolio_id": portfolio_id,
                    "name": broker_info.get("name", ""),
                    "principal": principal,
                    "total": total,
                    "pnl_pct": pnl_pct,
                    "status": broker_info.get("status", True),
                })

    def main_block():
        return Div(
            PortfolioList(portfolios),
            Div(id="create-modal"),
            cls="p-4",
        )

    layout.main_block = main_block
    return layout.render()


@rt("/create")
def show_create_modal():
    return CreatePortfolioModal()


@rt("/create", methods=["POST"])
async def create_portfolio(req):
    form = await req.form()
    name = form.get("name", "simulation")
    principal = float(form.get("principal", 1_000_000))
    commission = float(form.get("commission", 0.0001))
    info = form.get("info", "")
    interval = float(form.get("market_value_update_interval", 10))

    import uuid
    portfolio_id = uuid.uuid4().hex

    try:
        broker = SimulationBroker.create(
            portfolio_id=portfolio_id,
            principal=principal,
            commission=commission,
            portfolio_name=name,
            info=info,
            market_value_update_interval=interval,
        )

        reg = _get_registry(req)
        if reg:
            reg.register(BrokerKind.SIMULATION, portfolio_id, broker)

        return Div("", hx_redirect="/trade/simulation")
    except Exception as e:
        logger.exception(e)
        return Div(f"创建失败: {str(e)}", cls="text-red-500 p-4")


@rt("/{portfolio_id}")
def portfolio_detail(req, session, portfolio_id: str):
    layout = MainLayout(title="仿真账户详情", user=session.get("auth"))
    layout.sidebar_menu = [
        {
            "title": "交易",
            "children": [
                {"title": "仿真", "url": "/trade/simulation"},
                {"title": "实盘", "url": "/trade/live"},
            ],
        }
    ]

    reg = _get_registry(req)
    broker = reg.get(BrokerKind.SIMULATION, portfolio_id) if reg else None

    asset_overview = None
    positions = []

    if broker:
        if hasattr(broker, "asset"):
            asset_overview = _build_asset_overview(broker.asset)
        if hasattr(broker, "positions"):
            positions = list(broker.positions.values()) if isinstance(broker.positions, dict) else broker.positions

    def main_block():
        return Div(
            Div(
                A(
                    UkIcon("arrow-left", size=16),
                    " 返回列表",
                    href="/trade/simulation",
                    cls="text-sm text-gray-600 hover:text-gray-900 mb-4 inline-flex items-center gap-1",
                ),
                cls="mb-4",
            ),
            AssetSummary(asset_overview),
            Div(
                Div(PositionInfo(positions, portfolio_id), cls="w-3/4 pr-4"),
                Div(TradePanel(portfolio_id), cls="w-1/4"),
                cls="flex",
            ),
            cls="p-4",
        )

    layout.main_block = main_block
    return layout.render()


@rt("/{portfolio_id}/positions")
def get_positions(req, portfolio_id: str):
    reg = _get_registry(req)
    broker = reg.get(BrokerKind.SIMULATION, portfolio_id) if reg else None
    positions = []
    if broker and hasattr(broker, "positions"):
        pos = broker.positions
        if isinstance(pos, dict):
            positions = list(pos.values())
        else:
            positions = list(pos)
    return PositionInfo(positions, portfolio_id)


@rt("/{portfolio_id}/order", methods=["POST"])
async def place_order(req, portfolio_id: str):
    reg = _get_registry(req)
    broker = reg.get(BrokerKind.SIMULATION, portfolio_id) if reg else None

    if not broker:
        return Div("Broker not found", cls="text-red-500 p-4")

    form = await req.form()
    side = form.get("side", "BUY")
    asset = form.get("asset")
    price = float(form.get("price", 0))
    shares = int(form.get("shares", 0))

    try:
        if side == "BUY":
            await broker.buy(asset, shares, price)
        else:
            await broker.sell(asset, shares, price)

        positions = list(broker.positions.values()) if hasattr(broker, "positions") else []

        overview = _build_asset_overview(broker.asset if hasattr(broker, "asset") else None)

        return (
            PositionInfo(positions, portfolio_id),
            AssetSummary(overview, hx_swap_oob="true"),
        )
    except Exception as e:
        logger.exception(e)
        return Div(f"下单失败: {str(e)}", cls="text-red-500 font-bold p-4 bg-red-100 rounded")
