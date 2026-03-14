"""实盘交易界面

4 行布局：
1. 账号资金
2. 持仓列表
3. 委托下单
4. 当日委托/成交
"""

from fasthtml.common import *
from monsterui.all import *

from qmt_gateway.web.layouts.main import create_main_page
from qmt_gateway.web.theme import PRIMARY_COLOR, PrimaryButton, SecondaryButton


def AccountInfo(asset: dict | None = None):
    """账号资金信息"""
    if asset is None:
        asset = {
            "total": 1000000,
            "cash": 300000,
            "market_value": 700000,
            "position_ratio": 70.0,
        }

    # 转换为万为单位
    total_wan = asset.get("total", 0) / 10000
    cash_wan = asset.get("cash", 0) / 10000
    mv_wan = asset.get("market_value", 0) / 10000
    ratio = asset.get("position_ratio", 0)

    return Card(
        CardBody(
            Div(
                # 总资产
                Div(
                    Div("总资产", cls="text-sm text-gray-500"),
                    Div(f"{total_wan:.1f}万", cls="text-2xl font-bold", style=f"color: {PRIMARY_COLOR};"),
                    cls="text-center px-6",
                ),
                # 仓位
                Div(
                    Div("仓位", cls="text-sm text-gray-500"),
                    Div(f"{ratio:.1f}%", cls="text-2xl font-bold", style=f"color: {PRIMARY_COLOR};"),
                    cls="text-center px-6 border-l border-gray-200",
                ),
                # 可用资金
                Div(
                    Div("可用资金", cls="text-sm text-gray-500"),
                    Div(f"{cash_wan:.1f}万", cls="text-2xl font-bold", style=f"color: {PRIMARY_COLOR};"),
                    cls="text-center px-6 border-l border-gray-200",
                ),
                # 市值
                Div(
                    Div("市值", cls="text-sm text-gray-500"),
                    Div(f"{mv_wan:.1f}万", cls="text-2xl font-bold", style=f"color: {PRIMARY_COLOR};"),
                    cls="text-center px-6 border-l border-gray-200",
                ),
                cls="flex justify-center py-4",
            ),
        ),
        cls="mb-4",
    )


def PositionTable(positions: list[dict] | None = None):
    """持仓列表"""
    if positions is None:
        positions = []

    headers = ["代码", "名称", "持仓", "可用", "现价", "成本", "盈亏", "盈亏率"]

    rows = []
    for pos in positions:
        # 计算盈亏
        profit = pos.get("profit", 0)
        profit_ratio = pos.get("profit_ratio", 0)
        profit_color = "#ef4444" if profit >= 0 else "#22c55e"  # 红涨绿跌

        rows.append(
            Tr(
                Td(pos.get("symbol", "")),
                Td(pos.get("name", "")),
                Td(str(pos.get("shares", 0))),
                Td(str(pos.get("avail", 0))),
                Td(f"{pos.get('price', 0):.2f}"),
                Td(f"{pos.get('cost', 0):.2f}"),
                Td(f"{profit:.2f}", style=f"color: {profit_color};"),
                Td(f"{profit_ratio:.2f}%", style=f"color: {profit_color};"),
                # 双击事件：切换到卖出，设置全仓数量
                hx_on="dblclick: document.getElementById('order-side-sell').checked=true; document.getElementById('order-symbol').value='{symbol}'; document.getElementById('order-shares').value='{shares}';".format(
                    symbol=pos.get("symbol", ""),
                    shares=pos.get("avail", 0),
                ),
                cls="hover:bg-gray-50 cursor-pointer",
            )
        )

    if not rows:
        rows.append(
            Tr(
                Td("暂无持仓", colspan=len(headers), cls="text-center text-gray-500 py-8"),
            )
        )

    return Card(
        CardHeader(H4("持仓列表", cls="text-lg font-semibold")),
        CardBody(
            Div(
                Table(
                    Thead(Tr(*[Th(h, cls="text-left") for h in headers])),
                    Tbody(*rows),
                    cls="w-full",
                ),
                cls="overflow-x-auto",
            ),
            P("提示：双击持仓可快速切换到卖出", cls="text-xs text-gray-500 mt-2"),
        ),
        cls="mb-4",
    )


def OrderForm():
    """委托下单表单"""
    return Card(
        CardHeader(H4("委托下单", cls="text-lg font-semibold")),
        CardBody(
            Form(
                # 买卖切换
                Div(
                    Label(cls="label cursor-pointer justify-start gap-4"),
                    Input(
                        type="radio",
                        name="side",
                        value="buy",
                        id="order-side-buy",
                        cls="radio radio-primary",
                        checked=True,
                    ),
                    Span("买入", cls="label-text text-green-600 font-bold"),
                    Input(
                        type="radio",
                        name="side",
                        value="sell",
                        id="order-side-sell",
                        cls="radio radio-primary",
                    ),
                    Span("卖出", cls="label-text text-red-600 font-bold"),
                    cls="form-control flex-row gap-4 mb-4",
                ),
                # 代码
                Div(
                    Label("代码", cls="label"),
                    Input(
                        type="text",
                        name="symbol",
                        id="order-symbol",
                        placeholder="请输入股票代码",
                        cls="input input-bordered w-full",
                    ),
                    cls="mb-4",
                ),
                # 价格和数量
                Div(
                    Div(
                        Label("价格", cls="label"),
                        Input(
                            type="number",
                            name="price",
                            id="order-price",
                            placeholder="委托价格",
                            step="0.01",
                            cls="input input-bordered w-full",
                        ),
                        cls="w-1/2 pr-2",
                    ),
                    Div(
                        Label("数量", cls="label"),
                        Input(
                            type="number",
                            name="shares",
                            id="order-shares",
                            placeholder="委托数量",
                            cls="input input-bordered w-full",
                        ),
                        cls="w-1/2 pl-2",
                    ),
                    cls="flex mb-4",
                ),
                # 下单按钮
                Div(
                    PrimaryButton("下单", type="submit", cls="w-full"),
                    cls="mt-4",
                ),
                # 结果显示区域
                Div(id="order-result", cls="mt-4"),
                hx_post="/api/v1/orders",
                hx_target="#order-result",
            ),
        ),
        cls="mb-4",
    )


def OrderList(orders: list[dict] | None = None):
    """当日委托列表"""
    if orders is None:
        orders = []

    headers = ["时间", "代码", "名称", "方向", "价格", "数量", "成交", "状态", "操作"]

    rows = []
    for order in orders:
        side_color = "#22c55e" if order.get("side") == "buy" else "#ef4444"
        side_text = "买入" if order.get("side") == "buy" else "卖出"

        # 状态样式
        status = order.get("status", "")
        status_class = {
            "filled": "badge-success",
            "partial": "badge-warning",
            "cancelled": "badge-error",
            "unreported": "badge-ghost",
        }.get(status, "badge-ghost")

        rows.append(
            Tr(
                Td(order.get("time", "")),
                Td(order.get("symbol", "")),
                Td(order.get("name", "")),
                Td(side_text, style=f"color: {side_color};"),
                Td(f"{order.get('price', 0):.2f}"),
                Td(str(order.get("shares", 0))),
                Td(str(order.get("filled", 0))),
                Td(Span(status, cls=f"badge {status_class}")),
                Td(
                    Button(
                        "撤单",
                        cls="btn btn-xs btn-error",
                        hx_post=f"/api/v1/orders/{order.get('qtoid', '')}/cancel",
                        hx_confirm="确认撤单？",
                    ) if status not in ["filled", "cancelled"] else "",
                ),
            )
        )

    if not rows:
        rows.append(
            Tr(
                Td("暂无委托", colspan=len(headers), cls="text-center text-gray-500 py-8"),
            )
        )

    return Card(
        CardHeader(H4("当日委托", cls="text-lg font-semibold")),
        CardBody(
            Div(
                Table(
                    Thead(Tr(*[Th(h, cls="text-left") for h in headers])),
                    Tbody(*rows),
                    cls="w-full",
                ),
                cls="overflow-x-auto",
            ),
        ),
        cls="mb-4",
    )


def TradeList(trades: list[dict] | None = None):
    """当日成交列表"""
    if trades is None:
        trades = []

    headers = ["时间", "代码", "名称", "方向", "价格", "数量", "金额"]

    rows = []
    for trade in trades:
        side_color = "#22c55e" if trade.get("side") == "buy" else "#ef4444"
        side_text = "买入" if trade.get("side") == "buy" else "卖出"

        rows.append(
            Tr(
                Td(trade.get("time", "")),
                Td(trade.get("symbol", "")),
                Td(trade.get("name", "")),
                Td(side_text, style=f"color: {side_color};"),
                Td(f"{trade.get('price', 0):.2f}"),
                Td(str(trade.get("shares", 0))),
                Td(f"{trade.get('amount', 0):.2f}"),
            )
        )

    if not rows:
        rows.append(
            Tr(
                Td("暂无成交", colspan=len(headers), cls="text-center text-gray-500 py-8"),
            )
        )

    return Card(
        CardHeader(H4("当日成交", cls="text-lg font-semibold")),
        CardBody(
            Div(
                Table(
                    Thead(Tr(*[Th(h, cls="text-left") for h in headers])),
                    Tbody(*rows),
                    cls="w-full",
                ),
                cls="overflow-x-auto",
            ),
        ),
        cls="mb-4",
    )


def TradingPage(
    asset: dict | None = None,
    positions: list[dict] | None = None,
    orders: list[dict] | None = None,
    trades: list[dict] | None = None,
    user: dict | None = None,
):
    """实盘交易页面"""
    return create_main_page(
        # 第1行：账号资金
        AccountInfo(asset),
        # 第2行：持仓列表
        PositionTable(positions),
        # 第3行：委托下单
        OrderForm(),
        # 第4行：当日委托和成交
        Div(
            Div(OrderList(orders), cls="w-1/2 pr-2"),
            Div(TradeList(trades), cls="w-1/2 pl-2"),
            cls="flex",
        ),
        page_title="实盘交易 - QMT Gateway",
        active_menu="trading",
        user=user,
    )
