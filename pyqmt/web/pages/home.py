from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from pyqmt.core.enums import OrderSide
from pyqmt.data.sqlite import Position
from pyqmt.service.registry import BrokerRegistry
from pyqmt.web.apis.broker import build_asset_overview
from pyqmt.web.layouts.main import MainLayout

home_app, rt = fast_app()


def AccountTabs():
    return Div(Ul(Li(A("我的资产", cls="active", href="#")), cls="uk-tab"), cls="mb-4")


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
            Div(
                P(cash_text, cls=val_cls),
                P("可用金额", cls=label_cls),
                cls=metric_cls,
            ),
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


def PositionInfo(positions: list[Position] | None = None):
    # 持仓信息表格
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

            # 简单的颜色逻辑：红涨绿跌
            color_cls = "text-red-500" if p.profit > 0 else ("text-green-500" if p.profit < 0 else "")

            rows.append(
                Tr(
                    Td(p.asset),
                    Td(p.asset),  # 暂无名称
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
                hx_get="/home/positions",
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


def TradePanel():
    # 交易操作面板 (中国红风格)
    input_cls = "uk-input uk-form-small rounded-md mb-4 mt-1"
    label_cls = "text-xs text-red-100 block"

    ratio_btn_cls = "uk-button uk-button-default uk-button-small text-white text-[10px] px-2 min-w-[36px] border-white/30 hover:bg-white/20"

    return Div(
        Form(
            # Buy/Sell Tabs
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
            # Inputs
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
            hx_post="/home/order",
            hx_target="#position-info",
            hx_swap="outerHTML",
        ),
        cls="p-6 rounded-xl shadow-xl h-full",
    )


def BrokerList(items: list[dict]):
    links = []
    for it in items:
        href = f"/home?kind={it['kind']}&id={it['id']}"
        links.append(Li(A(f"{it['kind']}:{it['id']}", href=href, cls="text-xs")))
    return Div(
        H3("实例列表", cls="text-sm font-bold mb-2"),
        Ul(*links, cls="uk-list uk-list-striped"),
        cls="bg-white p-3 rounded-xl shadow-sm border border-gray-100 mb-4",
    )


def main_block(asset_overview: dict | None = None, brokers: list[dict] | None = None, positions: list[Position] | None = None):
    return Div(
        BrokerList(brokers or []),
        AccountTabs(),
        AssetSummary(asset_overview),
        Div(
            Div(PositionInfo(positions), cls="w-3/4 pr-4"),
            Div(TradePanel(), cls="w-1/4"),
            cls="flex",
        ),
        cls="p-2 h-full",
    )

def _get_broker(req):
    reg = req.scope.get("registry")
    if not reg:
        return None
    # Use query params or default
    kind = req.query_params.get("kind")
    bid = req.query_params.get("id")
    if kind and bid:
        return reg.get(kind, bid)

    d = reg.get_default()
    if d:
        return reg.get(d[0], d[1])
    return None

@rt("/", methods="get")
def index(req, session):
    layout = MainLayout(
        title="交易",
        user=session.get("auth"),
    )

    asset_overview = None
    brokers = []
    positions = []
    reg: BrokerRegistry | None = req.scope.get("registry")
    if reg is not None:
        brokers = reg.list()
        broker = _get_broker(req)
        if broker is not None:
            if hasattr(broker, "asset"):
                asset_overview = build_asset_overview(broker.asset)  # type: ignore
            if hasattr(broker, "positions"):
                positions = broker.positions

    layout.main_block = lambda: main_block(asset_overview, brokers, positions)

    return layout.render()

@rt("/positions")
async def get_positions(req):
    broker = _get_broker(req)
    positions = broker.positions if broker else []
    return PositionInfo(positions)

@rt("/order", methods=["POST"])
async def place_order(req):
    broker = _get_broker(req)
    if not broker:
        return "Broker not found"

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

        # Return updated components with OOB swap
        positions = broker.positions

        # Build overview manually or use helper if possible
        overview = {
            "total": broker.total_assets,
            "cash": broker.cash,
            "frozen_cash": 0,
            "market_value": broker.total_assets - broker.cash,
            "pnl": broker.total_assets - broker.principal,
            "pnl_pct": (broker.total_assets - broker.principal) / broker.principal if broker.principal else 0
        }

        return (
            PositionInfo(positions),
            AssetSummary(overview, hx_swap_oob="true")
        )
    except Exception as e:
        logger.exception(e)
        # Return Error Alert or similar. For now just an alert that will replace position info temporarily or append?
        # Actually returning a simple div will replace PositionInfo.
        # Better to use hx-swap-oob to show error in a specific container.
        # But for simplicity:
        return Div(f"Error: {str(e)}", cls="text-red-500 font-bold p-4 bg-red-100 rounded")
