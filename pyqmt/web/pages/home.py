import datetime

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from pyqmt.core.enums import BrokerKind, OrderSide, OrderStatus
from pyqmt.data.sqlite import Position, db
from pyqmt.service.registry import BrokerRegistry
from pyqmt.web.apis.broker import build_asset_overview
from pyqmt.web.layouts.main import MainLayout

from pyqmt.web.theme import AppTheme

home_app, rt = fast_app(hdrs=AppTheme.headers())


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


def _format_amount(value: float | None) -> str:
    """格式化金额数值。

    Args:
        value: 金额数值

    Returns:
        str: 格式化文本
    """
    if value is None:
        return "--"
    return f"{value:,.2f}"


def _format_amount_wan(value: float | None) -> str:
    """格式化万元显示。

    Args:
        value: 金额数值

    Returns:
        str: 格式化文本
    """
    if value is None:
        return "--"
    return f"{value / 10000:,.2f}"


def _format_percent(value: float | None) -> str:
    """格式化百分比显示。

    Args:
        value: 百分比数值

    Returns:
        str: 格式化文本
    """
    if value is None:
        return "--"
    return f"{value * 100:.2f}%"


def _position_tag(profit_pct: float) -> tuple[str, str]:
    """生成持仓预警标签。

    Args:
        profit_pct: 盈亏比例

    Returns:
        tuple[str, str]: 标签文本和样式
    """
    if profit_pct <= -0.05:
        return "风险", "bg-red-100 text-red-800"
    if profit_pct <= 0:
        return "关注", "bg-yellow-100 text-yellow-800"
    return "正常", "bg-green-100 text-green-800"


def OverviewCards(asset_overview: dict | None = None):
    """构建首页资产概览卡片。

    Args:
        asset_overview: 资产概览数据

    Returns:
        Div: 资产概览卡片区域
    """
    asset_overview = asset_overview or {}
    total = asset_overview.get("total")
    market_value = asset_overview.get("market_value")
    cash = asset_overview.get("cash")
    pnl = asset_overview.get("pnl")
    pnl_pct = asset_overview.get("pnl_pct")
    cash_pct = None
    if total and total != 0:
        cash_pct = cash / total if cash is not None else None

    return Div(
        Div(
            Div(
                P("总资产", cls="text-sm text-gray-600"),
                P(f"{_format_amount_wan(total)}万", cls="text-2xl font-bold text-gray-900 mt-1"),
            ),
            Div(
                Span(f"↑ {_format_percent(pnl_pct)}", cls="text-green-600"),
                Span("较昨日", cls="ml-2 text-gray-600"),
                cls="mt-4 flex items-center text-sm",
            ),
            cls="bg-white rounded-lg shadow p-6",
        ),
        Div(
            Div(
                P("持仓市值", cls="text-sm text-gray-600"),
                P(f"{_format_amount_wan(market_value)}万", cls="text-2xl font-bold text-gray-900 mt-1"),
            ),
            Div(
                Span(f"↑ {_format_percent(pnl_pct)}", cls="text-green-600"),
                Span("较昨日", cls="ml-2 text-gray-600"),
                cls="mt-4 flex items-center text-sm",
            ),
            cls="bg-white rounded-lg shadow p-6",
        ),
        Div(
            Div(
                P("可用资金", cls="text-sm text-gray-600"),
                P(f"{_format_amount_wan(cash)}万", cls="text-2xl font-bold text-gray-900 mt-1"),
            ),
            Div(
                Span(f"占比 {_format_percent(cash_pct)}", cls="text-gray-600"),
                cls="mt-4 flex items-center text-sm",
            ),
            cls="bg-white rounded-lg shadow p-6",
        ),
        Div(
            Div(
                P("今日盈亏", cls="text-sm text-gray-600"),
                P(f"{_format_amount_wan(pnl)}万", cls="text-2xl font-bold text-green-600 mt-1"),
            ),
            Div(
                Span(f"↑ {_format_percent(pnl_pct)}", cls="text-green-600"),
                Span("收益率", cls="ml-2 text-gray-600"),
                cls="mt-4 flex items-center text-sm",
            ),
            cls="bg-white rounded-lg shadow p-6",
        ),
        cls="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6",
    )


def PositionTable(positions: list[Position] | None = None):
    """构建持仓明细表格。

    Args:
        positions: 持仓列表

    Returns:
        Div: 持仓表格区域
    """
    rows = []
    if positions:
        for p in positions:
            cost = p.mv - p.profit
            profit_pct = p.profit / cost if cost else 0
            tag_text, tag_cls = _position_tag(profit_pct)
            current_price = p.mv / p.shares if p.shares else 0
            rows.append(
                Tr(
                    Td(Span(tag_text, cls=f"inline-flex items-center px-2 py-1 rounded text-xs font-medium {tag_cls}")),
                    Td(p.asset, cls="text-gray-900"),
                    Td(p.asset, cls="text-gray-900"),
                    Td(f"{p.shares:,.0f}", cls="text-gray-900"),
                    Td(f"{p.avail:,.0f}", cls="text-gray-900"),
                    Td(f"{p.price:,.2f}", cls="text-gray-900"),
                    Td(f"{current_price:,.2f}", cls="text-gray-900"),
                    Td(f"{p.mv / 10000:,.2f}", cls="text-gray-900"),
                    Td(
                        f"{p.profit / 10000:,.2f} ({profit_pct * 100:.2f}%)",
                        cls="text-green-600" if p.profit >= 0 else "text-red-600",
                    ),
                    Td(Button("卖出", cls="text-blue-600 hover:underline")),
                    cls="border-t border-gray-200",
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无持仓", colspan="10", cls="py-6 text-center text-gray-500"),
                cls="border-t border-gray-200",
            )
        )

    return Div(
        Div(
            H2("持仓明细", cls="text-lg font-semibold text-gray-900"),
            cls="p-6 border-b border-gray-200",
        ),
        Div(
            Div(
                Table(
                    Thead(
                        Tr(
                            Th("AI预警", cls="pb-3 font-medium w-16"),
                            Th("股票代码", cls="pb-3 font-medium"),
                            Th("股票名称", cls="pb-3 font-medium"),
                            Th("持仓数量", cls="pb-3 font-medium"),
                            Th("可用", cls="pb-3 font-medium"),
                            Th("成本价", cls="pb-3 font-medium"),
                            Th("现价", cls="pb-3 font-medium"),
                            Th("市值(万)", cls="pb-3 font-medium"),
                            Th("盈亏(万)", cls="pb-3 font-medium"),
                            Th("操作", cls="pb-3 font-medium"),
                            cls="text-left text-sm text-gray-600",
                        )
                    ),
                    Tbody(*rows, cls="text-sm"),
                    cls="w-full",
                ),
                cls="overflow-x-auto",
            ),
            cls="p-6",
        ),
        cls="bg-white rounded-lg shadow mb-6",
    )


def _order_status_badge(status: OrderStatus) -> tuple[str, str]:
    """构建订单状态标签。

    Args:
        status: 订单状态

    Returns:
        tuple[str, str]: 文本与样式
    """
    if status == OrderStatus.SUCCEEDED:
        return "已成交", "bg-green-100 text-green-800"
    if status == OrderStatus.PART_SUCC:
        return "部分成交", "bg-blue-100 text-blue-800"
    if status == OrderStatus.CANCELED:
        return "已撤单", "bg-gray-100 text-gray-600"
    if status == OrderStatus.JUNK:
        return "废单", "bg-red-100 text-red-800"
    return "待成交", "bg-yellow-100 text-yellow-800"


def OrderTable(orders: list[dict] | None = None):
    """构建当日委托表格。

    Args:
        orders: 订单数据

    Returns:
        Div: 当日委托区域
    """
    rows = []
    if orders:
        for order in orders:
            side = order.get("side", OrderSide.BUY)
            if isinstance(side, int):
                side = OrderSide(side)
            status = order.get("status", OrderStatus.UNREPORTED)
            if isinstance(status, int):
                status = OrderStatus(status)
            status_text, status_cls = _order_status_badge(status)
            rows.append(
                Tr(
                    Td(order.get("tm", ""), cls="py-4 text-gray-900"),
                    Td(order.get("asset", ""), cls="py-4 text-gray-900"),
                    Td(order.get("asset", ""), cls="py-4 text-gray-900"),
                    Td(
                        Span(
                            "买入" if side == OrderSide.BUY else "卖出",
                            cls="text-red-600 font-medium"
                            if side == OrderSide.BUY
                            else "text-green-600 font-medium",
                        )
                    ),
                    Td(_format_amount(order.get("price")), cls="py-4 text-gray-900"),
                    Td(f"{order.get('shares', 0):,.0f}", cls="py-4 text-gray-900"),
                    Td(f"{order.get('filled', 0):,.0f}", cls="py-4 text-gray-900"),
                    Td(
                        Span(
                            status_text,
                            cls=f"inline-flex items-center px-2 py-1 rounded text-xs font-medium {status_cls}",
                        ),
                        cls="py-4",
                    ),
                    Td(
                        Button(
                            "撤单",
                            cls="text-blue-600 hover:underline" if status not in [OrderStatus.SUCCEEDED, OrderStatus.CANCELED] else "text-gray-400 cursor-not-allowed",
                            disabled=status in [OrderStatus.SUCCEEDED, OrderStatus.CANCELED],
                        ),
                        cls="py-4",
                    ),
                    cls="border-t border-gray-200",
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无委托", colspan="9", cls="py-6 text-center text-gray-500"),
                cls="border-t border-gray-200",
            )
        )

    return Div(
        Div(
            H2("当日委托", cls="text-lg font-semibold text-gray-900"),
            cls="p-6 border-b border-gray-200",
        ),
        Div(
            Div(
                Table(
                    Thead(
                        Tr(
                            Th("时间", cls="pb-3 font-medium"),
                            Th("股票代码", cls="pb-3 font-medium"),
                            Th("股票名称", cls="pb-3 font-medium"),
                            Th("方向", cls="pb-3 font-medium"),
                            Th("委托价", cls="pb-3 font-medium"),
                            Th("委托量", cls="pb-3 font-medium"),
                            Th("成交量", cls="pb-3 font-medium"),
                            Th("状态", cls="pb-3 font-medium"),
                            Th("操作", cls="pb-3 font-medium"),
                            cls="text-left text-sm text-gray-600",
                        )
                    ),
                    Tbody(*rows, cls="text-sm"),
                    cls="w-full",
                ),
                cls="overflow-x-auto",
            ),
            cls="p-6",
        ),
        cls="bg-white rounded-lg shadow",
    )


def NoAccountDialog():
    """无账户提示对话框"""
    return Div(
        Div(
            Div(
                # 图标
                Div(
                    UkIcon("alert-circle", size=48, cls="text-yellow-500"),
                    cls="flex justify-center mb-4",
                ),
                # 标题
                H3("欢迎使用匡醍", cls="text-xl font-semibold text-gray-900 text-center mb-2"),
                # 说明
                P("您还没有配置任何交易账号。请创建至少一个模拟交易账户或配置实盘账户，才能开始使用系统。",
                  cls="text-gray-600 text-center mb-6"),
                # 按钮组
                Div(
                    A(
                        "跳转账号管理",
                        href="/system/accounts",
                        cls="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 font-medium",
                    ),
                    A(
                        "创建模拟交易账户",
                        href="/system/accounts?create_sim=1",
                        cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium",
                    ),
                    cls="flex justify-center space-x-3",
                ),
                cls="bg-white p-8 rounded-lg shadow-lg max-w-md w-full mx-4",
            ),
            cls="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50",
            id="no-account-dialog",
        ),
        cls="max-w-[1400px] mx-auto w-full",
    )


def main_block(
    asset_overview: dict | None = None,
    brokers: list[dict] | None = None,
    positions: list[Position] | None = None,
    orders: list[dict] | None = None,
    show_no_account_dialog: bool = False,
):
    if show_no_account_dialog:
        return NoAccountDialog()

    return Div(
        Div(
            Nav(
                A("首页", href="#", cls="hover:text-blue-600"),
                Span(" / ", cls="text-gray-400"),
                Span("概览", cls="text-gray-900 font-medium"),
                cls="flex items-center space-x-2 text-sm text-gray-600",
            ),
            cls="mb-4",
        ),
        OverviewCards(asset_overview),
        PositionTable(positions),
        OrderTable(orders),
        cls="max-w-[1400px] mx-auto w-full",
    )

def _get_broker(req):
    reg = req.scope.get("registry")
    if not reg:
        return None
    # Use query params first
    kind = req.query_params.get("kind")
    bid = req.query_params.get("id")
    if kind and bid:
        return reg.get(kind, bid)

    # Then check session for active account
    session = req.scope.get("session", {})
    active_kind = session.get("active_account_kind")
    active_id = session.get("active_account_id")
    if active_kind and active_id:
        from pyqmt.core.enums import BrokerKind
        broker = reg.get(BrokerKind(active_kind), active_id)
        if broker:
            return broker

    # Finally use default
    d = reg.get_default()
    if d:
        return reg.get(d[0], d[1])
    return None

def _auto_select_account(reg: BrokerRegistry, session: dict) -> tuple[str, str] | None:
    """自动选择活动账户

    优先级：
    1. 实盘账户（只允许一个）
    2. 最后创建的模拟账户

    Returns:
        (kind, account_id) 或 None
    """
    if reg is None:
        return None

    # 检查是否有实盘账户
    live_accounts = reg.list_by_kind(BrokerKind.QMT)
    if live_accounts:
        # 实盘账户只允许一个，选择第一个
        info = live_accounts[0]
        account_id = info.get("id")
        if account_id:
            return (BrokerKind.QMT.value, account_id)

    # 检查模拟账户，选择最后创建的
    sim_accounts = reg.list_by_kind(BrokerKind.SIMULATION)
    if sim_accounts:
        # 选择最后一个（最后创建的）
        info = sim_accounts[-1]
        account_id = info.get("id")
        if account_id:
            return (BrokerKind.SIMULATION.value, account_id)

    return None


@rt("/", methods="get")
def index(req, session):
    layout = MainLayout(
        title="首页",
        user=session.get("auth"),
    )

    asset_overview = None
    brokers = []
    positions = []
    orders = []
    accounts = []
    active_account = None
    reg: BrokerRegistry | None = req.scope.get("registry")

    if reg is not None:
        # 获取所有账户
        for kind in [BrokerKind.QMT, BrokerKind.SIMULATION]:
            for info in reg.list_by_kind(kind):
                account = {
                    "id": info.get("id"),
                    "name": info.get("name") or info.get("id"),
                    "kind": kind.value,
                    "label": "实盘" if kind == BrokerKind.QMT else "仿真",
                    "status": info.get("status", False),
                    "is_live": kind == BrokerKind.QMT,
                    "switch_url": f"/home?kind={kind.value}&id={info.get('id')}",
                }
                accounts.append(account)

        # 检查是否有任何账户
        show_no_account_dialog = not accounts

        # 检查 session 中是否有活动账户
        active_kind = session.get("active_account_kind")
        active_id = session.get("active_account_id")

        if active_kind and active_id:
            # 验证活动账户是否仍然存在
            broker = reg.get(BrokerKind(active_kind), active_id)
            if broker is None:
                # 活动账户已不存在，需要重新选择
                selected = _auto_select_account(reg, session)
                if selected:
                    session["active_account_kind"] = selected[0]
                    session["active_account_id"] = selected[1]
                    active_kind, active_id = selected
        else:
            # 没有活动账户，自动选择
            selected = _auto_select_account(reg, session)
            if selected:
                session["active_account_kind"] = selected[0]
                session["active_account_id"] = selected[1]
                active_kind, active_id = selected

        # 获取当前活动账户信息
        if active_kind and active_id:
            for acc in accounts:
                if acc["kind"] == active_kind and acc["id"] == active_id:
                    active_account = acc
                    break

        # 获取经纪人数据
        broker = _get_broker(req)
        if broker is not None:
            if hasattr(broker, "asset"):
                asset_overview = build_asset_overview(broker.asset)  # type: ignore
            elif hasattr(broker, "total_assets"):
                # SimulationBroker 等没有 asset 属性，但有 total_assets, cash, principal
                total = broker.total_assets
                cash = broker.cash if hasattr(broker, "cash") else 0
                principal = broker.principal if hasattr(broker, "principal") else 0
                market_value = total - cash
                pnl = total - principal
                pnl_pct = pnl / principal if principal else 0
                asset_overview = {
                    "total": total,
                    "cash": cash,
                    "frozen_cash": 0,
                    "market_value": market_value,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                }
            if hasattr(broker, "positions"):
                positions = broker.positions
            portfolio_id = broker.portfolio_id if hasattr(broker, "portfolio_id") else None
            if portfolio_id:
                orders_df = db.get_orders(datetime.date.today(), portfolio_id)
                if orders_df is not None and not orders_df.is_empty():
                    orders = [
                        {
                            "tm": str(row.get("tm", ""))[:19],
                            "asset": row.get("asset", ""),
                            "side": row.get("side", OrderSide.BUY),
                            "price": row.get("price", 0.0),
                            "shares": row.get("shares", 0),
                            "filled": row.get("filled", 0),
                            "status": row.get("status", OrderStatus.UNREPORTED),
                        }
                        for row in orders_df.iter_rows(named=True)
                    ]

    layout.header_accounts = accounts
    layout.active_account = active_account
    layout.main_block = lambda: main_block(asset_overview, brokers, positions, orders, show_no_account_dialog)

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
