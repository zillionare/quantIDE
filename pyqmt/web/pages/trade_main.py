"""交易主页面 - 符合 livetrade-default.html 原型

包含：
1. 资产信息条
2. 闪电交易面板
3. 持仓明细
4. 当日委托
"""

from fasthtml.common import *
from monsterui.all import *
from starlette.responses import RedirectResponse

from pyqmt.core.enums import BrokerKind, OrderSide, OrderStatus
from pyqmt.data.sqlite import Position, Order
from pyqmt.service.registry import BrokerRegistry
from pyqmt.web.layouts.main import MainLayout


def _get_registry(req) -> BrokerRegistry:
    return req.scope.get("registry")


def _get_all_accounts(reg: BrokerRegistry) -> list[dict]:
    """获取所有账号列表"""
    accounts = []
    for kind in [BrokerKind.QMT, BrokerKind.SIMULATION]:
        for info in reg.list_by_kind(kind):
            accounts.append({
                "id": info.get("id"),
                "name": info.get("name") or info.get("id"),
                "kind": kind.value,
                "label": "实盘" if kind == BrokerKind.QMT else "仿真",
                "status": info.get("status", False),
                "is_live": kind == BrokerKind.QMT,
            })
    return accounts


def _get_active_account(reg: BrokerRegistry, session: dict) -> dict | None:
    """获取当前活动账号"""
    kind_str = session.get("active_account_kind")
    account_id = session.get("active_account_id")

    if not kind_str or not account_id:
        default = reg.get_default() if reg else None
        if not default:
            return None
        kind_str, account_id = default

    kind = BrokerKind(kind_str) if isinstance(kind_str, str) else kind_str
    broker = reg.get(kind, account_id) if reg else None

    if broker:
        name = ""
        status = True
        if hasattr(broker, "portfolio_name"):
            name = broker.portfolio_name
        if hasattr(broker, "status"):
            status = broker.status
        return {
            "id": account_id,
            "name": name or account_id,
            "kind": kind_str if isinstance(kind_str, str) else kind_str.value,
            "label": "实盘" if (kind == BrokerKind.QMT or kind_str == BrokerKind.QMT.value) else "仿真",
            "status": status,
            "is_live": kind == BrokerKind.QMT or kind_str == BrokerKind.QMT.value,
        }
    return None


def AssetInfoBar(total: float = 0, cash: float = 0, market_value: float = 0):
    """资产信息条"""
    cash_ratio = (cash / total * 100) if total > 0 else 0
    position_ratio = (market_value / total * 100) if total > 0 else 0

    return Div(
        Div(
            Div(
                Span("总资产", cls="text-gray-600 dark:text-gray-400 text-sm"),
                Span(f"{total/10000:.2f}万", cls="font-bold text-gray-900 dark:text-white text-base ml-2"),
                cls="flex items-center space-x-2",
            ),
            Div(
                Span("现金", cls="text-gray-600 dark:text-gray-400 text-sm"),
                Span(f"{cash/10000:.2f}万", cls="font-bold text-gray-900 dark:text-white text-base ml-2"),
                cls="flex items-center space-x-2",
            ),
            Div(
                Span("现金比", cls="text-gray-600 dark:text-gray-400 text-sm"),
                Span(f"{cash_ratio:.1f}%", cls="font-bold text-gray-900 dark:text-white text-base ml-2"),
                cls="flex items-center space-x-2",
            ),
            Div(
                Span("市值", cls="text-gray-600 dark:text-gray-400 text-sm"),
                Span(f"{market_value/10000:.2f}万", cls="font-bold text-gray-900 dark:text-white text-base ml-2"),
                cls="flex items-center space-x-2",
            ),
            Div(
                Span("仓位", cls="text-gray-600 dark:text-gray-400 text-sm"),
                Span(f"{position_ratio:.1f}%", cls="font-bold text-gray-900 dark:text-white text-base ml-2"),
                cls="flex items-center space-x-2",
            ),
            cls="flex items-center justify-between text-sm px-6 py-4",
        ),
        cls="bg-white dark:bg-gray-800 rounded-lg shadow mb-6",
    )


def LightningTradePanel(portfolio_id: str, kind: str):
    """闪电交易面板"""
    input_cls = "flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-red-500 dark:bg-gray-700 dark:text-white"
    label_cls = "w-16 text-sm font-medium text-gray-700 dark:text-gray-300"

    return Div(
        Form(
            Div(
                # 左边：下单输入区（2/5）
                Div(
                    # 代码输入
                    Div(
                        Span("代码", cls=label_cls),
                        Input(type="text", name="asset", placeholder="输入代码/名称/拼音", cls=input_cls),
                        cls="flex items-center mb-3",
                    ),
                    # 价格输入
                    Div(
                        Span("价格", cls=label_cls),
                        Input(type="text", name="price", value="0.00", cls=f"{input_cls} text-right text-lg font-medium"),
                        cls="flex items-center mb-3",
                    ),
                    # 金额输入
                    Div(
                        Span("金额", cls=label_cls),
                        Input(type="text", name="amount", placeholder="万元", cls=f"{input_cls} text-right text-lg font-medium"),
                        cls="flex items-center mb-3",
                    ),
                    # 仓位选择
                    Div(
                        Span("仓位", cls="text-sm font-medium text-gray-700 dark:text-gray-300"),
                        Div(
                            Button("全仓", type="button", cls="px-2 py-2 text-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-200 font-medium"),
                            Button("1/2", type="button", cls="px-2 py-2 text-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-200 font-medium"),
                            Button("1/3", type="button", cls="px-2 py-2 text-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-200 font-medium"),
                            Button("1/4", type="button", cls="px-2 py-2 text-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-200 font-medium"),
                            cls="grid grid-cols-4 gap-2 mt-1",
                        ),
                        cls="mb-3",
                    ),
                    # 买入/卖出按钮
                    Div(
                        Button(
                            "买入",
                            type="submit",
                            name="side",
                            value="BUY",
                            cls="bg-red-600 hover:bg-red-700 text-white font-bold py-3 px-4 rounded-lg text-lg w-full",
                        ),
                        Button(
                            "卖出",
                            type="submit",
                            name="side",
                            value="SELL",
                            cls="bg-green-600 hover:bg-green-700 text-white font-bold py-3 px-4 rounded-lg text-lg w-full",
                        ),
                        cls="grid grid-cols-2 gap-3 pt-2",
                    ),
                    cls="col-span-2 space-y-3",
                ),
                # 中间：价格快捷输入区（1/5）
                Div(
                    Div(
                        Span("快捷价格", cls="text-sm font-medium text-gray-700 dark:text-gray-300"),
                        Span("实时: 0.00", cls="text-xs text-gray-500 dark:text-gray-400"),
                        cls="flex items-center justify-between mb-2",
                    ),
                    Div(
                        # 涨幅按钮
                        Div(
                            Button("1%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("2%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("3%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("4%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("5%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("涨停", type="button", cls="w-full px-1 py-1.5 text-xs bg-red-100 dark:bg-red-900 text-red-700 dark:text-red-300 rounded hover:bg-red-200 font-medium"),
                            cls="space-y-1",
                        ),
                        # 跌幅按钮
                        Div(
                            Button("跌停", type="button", cls="w-full px-1 py-1.5 text-xs bg-green-100 dark:bg-green-900 text-green-700 dark:text-green-300 rounded hover:bg-green-200 font-medium"),
                            Button("-5%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("-4%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("-3%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("-2%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            Button("-1%", type="button", cls="w-full px-1 py-1.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded hover:bg-gray-200 font-medium"),
                            cls="space-y-1",
                        ),
                        # MA均线按钮
                        Div(
                            Button("MA5", type="button", cls="w-full px-1 py-1.5 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 font-medium"),
                            Button("MA10", type="button", cls="w-full px-1 py-1.5 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 font-medium"),
                            Button("MA20", type="button", cls="w-full px-1 py-1.5 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 font-medium"),
                            Button("MA30", type="button", cls="w-full px-1 py-1.5 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 font-medium"),
                            Button("MA60", type="button", cls="w-full px-1 py-1.5 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 font-medium"),
                            Button("MA120", type="button", cls="w-full px-1 py-1.5 text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded hover:bg-blue-200 font-medium"),
                            cls="space-y-1",
                        ),
                        cls="grid grid-cols-3 gap-1",
                    ),
                    cls="space-y-2",
                ),
                # 右边：候选股票池（2/5）
                Div(
                    Div(
                        H3("候选票池", cls="text-base font-semibold text-gray-900 dark:text-white"),
                        Input(type="text", placeholder="搜索股票...", cls="w-32 px-2 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-red-500 dark:bg-gray-600 dark:text-white"),
                        cls="flex items-center justify-between mb-3",
                    ),
                    # 候选股票列表
                    Div(
                        Div(
                            Div(
                                Div("平安银行", cls="text-sm font-medium text-gray-900 dark:text-white"),
                                Div("000001.SZ", cls="text-xs text-gray-500 dark:text-gray-400"),
                                cls="",
                            ),
                            Div("13.80", cls="text-sm font-medium text-red-600"),
                            cls="p-2 bg-white dark:bg-gray-800 rounded-lg cursor-pointer hover:ring-2 hover:ring-red-500 transition-all flex items-center justify-between",
                        ),
                        Div(
                            Div(
                                Div("贵州茅台", cls="text-sm font-medium text-gray-900 dark:text-white"),
                                Div("600519.SH", cls="text-xs text-gray-500 dark:text-gray-400"),
                                cls="",
                            ),
                            Div("1688.00", cls="text-sm font-medium text-green-600"),
                            cls="p-2 bg-white dark:bg-gray-800 rounded-lg cursor-pointer hover:ring-2 hover:ring-red-500 transition-all flex items-center justify-between",
                        ),
                        cls="space-y-2",
                    ),
                    cls="col-span-2 bg-gray-50 dark:bg-gray-700 rounded-lg p-4",
                ),
                cls="grid grid-cols-5 gap-4",
            ),
            hx_post=f"/trade/order",
            hx_target="#trade-result",
        ),
        Div(id="trade-result"),
        cls="bg-white dark:bg-gray-800 rounded-lg shadow mb-6 p-6",
    )


def PositionTable(positions: list[Position]):
    """持仓明细表格"""
    headers = ["证券代码", "证券名称", "当前持股", "可用股数", "市值", "最新价", "成本价", "盈亏", "盈亏比例", "操作"]

    rows = []
    if positions:
        for p in positions:
            pnl_pct = (p.profit / (p.mv - p.profit) * 100) if (p.mv - p.profit) != 0 else 0
            current_price = p.mv / p.shares if p.shares > 0 else 0
            color_cls = "text-red-600" if p.profit > 0 else ("text-green-600" if p.profit < 0 else "")

            rows.append(
                Tr(
                    Td(p.asset),
                    Td(p.asset),  # TODO: 获取证券名称
                    Td(f"{p.shares:,}"),
                    Td(f"{p.avail:,}"),
                    Td(f"{p.mv:,.2f}"),
                    Td(f"{current_price:,.2f}"),
                    Td(f"{p.price:,.2f}"),
                    Td(f"{p.profit:,.2f}", cls=color_cls),
                    Td(f"{pnl_pct:.2f}%", cls=color_cls),
                    Td(
                        Button("卖出", cls="uk-button uk-button-small bg-green-600 text-white hover:bg-green-700"),
                    ),
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无持仓", colspan=len(headers), cls="text-center py-10 text-gray-500")
            )
        )

    return Div(
        Div(
            H3("持仓明细", cls="text-lg font-semibold text-gray-900 dark:text-white"),
            Button(
                UkIcon("refresh-cw", size=16),
                " 刷新",
                cls="uk-button uk-button-primary uk-button-small",
                style="background-color: #d32f2f;",
                hx_get=f"/trade/positions",
                hx_target="#position-table",
            ),
            cls="flex items-center justify-between mb-4 px-6 pt-4",
        ),
        Table(
            Thead(Tr(*[Th(h, cls="text-xs font-bold bg-gray-50") for h in headers])),
            Tbody(*rows),
            cls="uk-table uk-table-divider uk-table-small uk-table-hover",
        ),
        id="position-table",
        cls="bg-white dark:bg-gray-800 rounded-lg shadow mb-6 overflow-hidden",
    )


def TodayOrdersTable(orders: list[Order]):
    """当日委托表格"""
    headers = ["时间", "代码", "名称", "方向", "委托价", "委托量", "成交量", "状态", "操作"]

    rows = []
    if orders:
        for o in orders:
            side_color = "text-red-600" if o.side == OrderSide.BUY else "text-green-600"
            side_text = "买入" if o.side == OrderSide.BUY else "卖出"

            status_map = {
                OrderStatus.PENDING: ("待成交", "text-yellow-600"),
                OrderStatus.PARTIAL: ("部分成交", "text-blue-600"),
                OrderStatus.FILLED: ("已成交", "text-green-600"),
                OrderStatus.CANCELLED: ("已撤单", "text-gray-500"),
                OrderStatus.REJECTED: ("已拒绝", "text-red-600"),
            }
            status_text, status_color = status_map.get(o.status, ("未知", "text-gray-500"))

            rows.append(
                Tr(
                    Td(o.tm.strftime("%H:%M:%S") if hasattr(o, 'tm') else "--"),
                    Td(o.asset),
                    Td(o.asset),  # TODO: 获取证券名称
                    Td(side_text, cls=side_color),
                    Td(f"{o.price:,.2f}"),
                    Td(f"{o.shares:,}"),
                    Td(f"{o.filled:,}"),
                    Td(status_text, cls=status_color),
                    Td(
                        Button(
                            "撤单",
                            cls="uk-button uk-button-small uk-button-default",
                            hx_post=f"/trade/cancel/{o.qtoid}",
                            hx_confirm="确定要撤单吗？",
                        ) if o.status in [OrderStatus.PENDING, OrderStatus.PARTIAL] else "",
                    ),
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无当日委托", colspan=len(headers), cls="text-center py-10 text-gray-500")
            )
        )

    return Div(
        Div(
            H3("当日委托", cls="text-lg font-semibold text-gray-900 dark:text-white"),
            Button(
                UkIcon("refresh-cw", size=16),
                " 刷新",
                cls="uk-button uk-button-primary uk-button-small",
                style="background-color: #d32f2f;",
                hx_get=f"/trade/orders",
                hx_target="#today-orders",
            ),
            cls="flex items-center justify-between mb-4 px-6 pt-4",
        ),
        Table(
            Thead(Tr(*[Th(h, cls="text-xs font-bold bg-gray-50") for h in headers])),
            Tbody(*rows),
            cls="uk-table uk-table-divider uk-table-small uk-table-hover",
        ),
        id="today-orders",
        cls="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden",
    )


def NoAccountView():
    """无账号视图"""
    return Div(
        Div(
            UkIcon("alert-circle", size=48, cls="text-yellow-500 mb-4"),
            H2("暂无交易账号", cls="text-2xl font-bold text-gray-900 mb-4"),
            P("您还没有配置任何交易账号。请先创建或配置账号。", cls="text-gray-600 mb-6"),
            Div(
                A(
                    UkIcon("plus", size=16),
                    " 创建仿真账号",
                    href="/trade/simulation",
                    cls="uk-button uk-button-primary mr-4",
                    style="background-color: #d32f2f;",
                ),
                A(
                    UkIcon("settings", size=16),
                    " 账号管理",
                    href="/system/accounts",
                    cls="uk-button uk-button-default",
                ),
                cls="flex justify-center gap-4",
            ),
            cls="text-center py-16",
        ),
        cls="max-w-2xl mx-auto bg-white rounded-xl shadow-sm border border-gray-100 p-8",
    )


def SelectAccountView(accounts: list[dict]):
    """选择账号视图"""
    live_accounts = [a for a in accounts if a["is_live"]]
    sim_accounts = [a for a in accounts if not a["is_live"]]

    def AccountCard(account: dict):
        return Div(
            Div(
                Div(
                    Span(
                        "实盘" if account["is_live"] else "仿真",
                        cls=f"px-2 py-1 text-xs font-medium rounded-full {'bg-red-100 text-red-700' if account['is_live'] else 'bg-blue-100 text-blue-700'}"
                    ),
                    Span(
                        "在线" if account["status"] else "离线",
                        cls=f"ml-2 text-xs {'text-green-600' if account['status'] else 'text-gray-400'}"
                    ),
                    cls="mb-2",
                ),
                H4(account["name"], cls="text-lg font-semibold text-gray-900"),
                P(f"ID: {account['id'][:16]}...", cls="text-xs text-gray-500"),
                cls="mb-4",
            ),
            Form(
                Input(type="hidden", name="kind", value=account["kind"]),
                Input(type="hidden", name="id", value=account["id"]),
                Button(
                    "设为活动账号",
                    type="submit",
                    cls="uk-button uk-button-primary w-full",
                    style="background-color: #d32f2f;",
                ),
                hx_post="/trade/set-active",
                hx_target="body",
                hx_swap="outerHTML",
            ),
            cls="bg-white rounded-xl shadow-sm border border-gray-100 p-6 hover:shadow-md transition-shadow",
        )

    return Div(
        Div(
            H2("选择活动账号", cls="text-2xl font-bold text-gray-900 mb-2"),
            P("请选择一个账号作为当前活动账号，所有交易操作将与该账号关联。", cls="text-gray-600 mb-6"),
            cls="mb-6",
        ),
        Div(
            Div(
                H3("实盘账号", cls="text-lg font-semibold text-gray-900 mb-4"),
                Div(
                    *[AccountCard(a) for a in live_accounts],
                    cls="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4",
                ) if live_accounts else P("暂无实盘账号", cls="text-gray-500 italic"),
                cls="mb-8",
            ),
            Div(
                H3("仿真账号", cls="text-lg font-semibold text-gray-900 mb-4"),
                Div(
                    *[AccountCard(a) for a in sim_accounts],
                    cls="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4",
                ) if sim_accounts else P("暂无仿真账号", cls="text-gray-500 italic"),
                cls="mb-8",
            ),
            Div(
                A(
                    UkIcon("settings", size=16),
                    " 管理账号",
                    href="/system/accounts",
                    cls="uk-button uk-button-default",
                ),
                cls="flex justify-center",
            ),
            cls="max-w-6xl mx-auto",
        ),
        cls="p-6",
    )


def trade_main_page(request):
    """交易主页面 - 符合 livetrade-default.html 原型"""
    from starlette.responses import HTMLResponse

    session = request.scope.get("session", {})
    layout = MainLayout(title="交易", user=session.get("auth"))
    layout.header_active = "交易"
    layout.set_sidebar_active("/trade")

    reg = _get_registry(request)

    # 获取所有账号
    accounts = _get_all_accounts(reg) if reg else []

    # 如果没有配置任何账号
    if not accounts:
        def main_block():
            return Div(NoAccountView(), cls="p-6")
        layout.main_block = main_block
        return HTMLResponse(to_xml(layout.render()))

    # 获取活动账号
    active_account = _get_active_account(reg, session)

    # 如果没有活动账号，显示选择页面
    if not active_account:
        def main_block():
            return SelectAccountView(accounts)
        layout.main_block = main_block
        return HTMLResponse(to_xml(layout.render()))

    # 获取账号数据
    portfolio_id = active_account["id"]
    kind = active_account["kind"]

    broker = reg.get(BrokerKind(kind), portfolio_id) if reg else None

    # 获取资产信息
    total = cash = market_value = 0
    positions = []
    orders = []

    if broker:
        if hasattr(broker, "asset") and broker.asset:
            total = broker.asset.total
            cash = broker.asset.cash
            market_value = broker.asset.market_value
        elif hasattr(broker, "total_assets"):
            # SimulationBroker 等没有 asset 属性
            total = broker.total_assets
            cash = broker.cash if hasattr(broker, "cash") else 0
            market_value = total - cash
        if hasattr(broker, "positions"):
            positions = list(broker.positions.values()) if isinstance(broker.positions, dict) else broker.positions
        if hasattr(broker, "orders"):
            orders = list(broker.orders.values()) if isinstance(broker.orders, dict) else broker.orders

    def main_block():
        return Div(
            # 账号信息
            Div(
                Div(
                    Span("★", cls="text-yellow-500 mr-1"),
                    Span(f"当前账号: {active_account['name']}", cls="font-medium"),
                    Span(f"({active_account['label']})", cls="ml-2 text-sm text-gray-500"),
                    cls="flex items-center mb-4 px-6",
                ),
            ),
            # 资产信息条
            AssetInfoBar(total, cash, market_value),
            # 闪电交易面板
            LightningTradePanel(portfolio_id, kind),
            # 持仓明细
            PositionTable(positions),
            # 当日委托
            TodayOrdersTable(orders),
            cls="p-6",
        )

    layout.main_block = main_block
    return HTMLResponse(to_xml(layout.render()))


async def set_active_account(req, session):
    """设置活动账号"""
    form = await req.form()
    kind = form.get("kind")
    account_id = form.get("id")

    if not kind or not account_id:
        return Div("参数错误", cls="text-red-500 p-4")

    session["active_account_kind"] = kind
    session["active_account_id"] = account_id

    return RedirectResponse(url="/trade", status_code=303)
