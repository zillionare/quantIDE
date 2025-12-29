from fasthtml.common import *
from monsterui.all import *

from pyqmt.web.layouts.main import MainLayout
from dataclasses import asdict

home_app, rt = fast_app()


def AccountTabs():
    return Div(Ul(Li(A("账户1", cls="active", href="#")), cls="uk-tab"), cls="mb-4")


def AssetSummary(asset_overview: dict | None = None):
    metric_cls = "flex flex-col items-center justify-center p-4 text-white"
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
    market_value_text = (
        f"{market_value:,.2f}" if market_value is not None else "0.00"
    )
    pnl_text = f"{pnl:,.2f}" if pnl is not None else "0.00"
    pnl_pct_text = (
        f"{pnl_pct * 100:.2f}%" if pnl_pct is not None else "0.00%"
    )

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
    )


def PositionInfo():
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
            ),
            cls="flex justify-between items-center mb-4",
        ),
        Table(
            Thead(Tr(*[Th(h, cls="text-xs font-bold") for h in headers])),
            Tbody(
                Tr(
                    Td(
                        "暂无持仓",
                        colspan=len(headers),
                        cls="text-center py-10 text-gray-500",
                    )
                )
            ),
            cls="uk-table uk-table-divider uk-table-small uk-table-hover border border-gray-100 rounded-lg overflow-hidden",
        ),
        cls="bg-white p-4 rounded-xl shadow-sm border border-gray-100 h-full",
    )


def TradePanel():
    # 交易操作面板 (中国红风格)
    input_cls = "uk-input uk-form-small rounded-md mb-4 mt-1"
    label_cls = "text-xs text-red-100 block"

    ratio_btn_cls = "uk-button uk-button-default uk-button-small text-white text-[10px] px-2 min-w-[36px] border-white/30 hover:bg-white/20"

    return Div(
        # Buy/Sell Tabs
        Div(
            Ul(
                Li(
                    A(
                        UkIcon("plus-circle", size=14),
                        " 买入",
                        href="#",
                        cls="flex items-center text-xs",
                    ),
                    cls="uk-active",
                ),
                Li(
                    A(
                        UkIcon("minus-circle", size=14),
                        " 卖出",
                        href="#",
                        cls="flex items-center text-xs",
                    )
                ),
                cls="uk-tab uk-child-width-expand mb-4 bg-white/10 rounded-lg p-0.5",
                style="border-bottom: none;",
            )
        ),
        # Inputs
        Div(
            Label("股票代码", cls=label_cls),
            Input(placeholder="例如: 000001.SZ", cls=input_cls),
            Label("买入价格", cls=label_cls),
            Input(type="number", value="0.00", cls=input_cls),
            Label("仓位选择", cls=label_cls),
            Div(
                Button("满仓", cls=ratio_btn_cls),
                Button("1/2", cls=ratio_btn_cls),
                Button("1/3", cls=ratio_btn_cls),
                Button("1/4", cls=ratio_btn_cls),
                Button("1/10", cls=ratio_btn_cls),
                cls="flex gap-1 mb-4 mt-1",
            ),
            Label("仓位比例", cls=label_cls),
            Input(type="number", value="0.1", cls=input_cls),
            Button(
                UkIcon("shopping-cart", size=16),
                " 买入",
                cls="uk-button-default w-full bg-white text-red-700 font-bold border-none hover:bg-gray-100",
            ),
        ),
        cls="p-6 rounded-xl shadow-xl text-white h-full",
        style="background: linear-gradient(135deg, #b91c1c 0%, #ef4444 100%);",
    )


def main_block(asset_overview: dict | None = None):
    return Div(
        AccountTabs(),
        AssetSummary(asset_overview),
        Div(
            Div(PositionInfo(), cls="w-3/4 pr-4"),
            Div(TradePanel(), cls="w-1/4"),
            cls="flex",
        ),
        cls="p-2 h-full",
    )


@rt("/", methods="get")
def index(req, session):
    layout = MainLayout(
        title="交易",
        user=session.get("auth"),
    )

    asset_overview = None
    
    # 直接从主应用实例获取 broker
    broker = getattr(main_app.state, "broker", None)
    
    if broker is not None and hasattr(broker, "asset"):
        try:
            # 与 /asset_overview 路由中相同的逻辑
            asset = broker.asset
            pnl = asset.total - asset.principal
            ppnl = pnl / asset.principal if asset.principal else 0.0
            data = asdict(asset)
            data["pnl"] = pnl
            data["pnl_pct"] = ppnl
            asset_overview = data
        except Exception:
            asset_overview = None

    layout.main_block = lambda: main_block(asset_overview)

    return layout.render()
