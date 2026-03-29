"""历史持仓页面"""

import datetime

from fasthtml.common import *
from monsterui.all import *
from starlette.responses import HTMLResponse

from quantide.core.enums import BrokerKind
from quantide.data.sqlite import db
from quantide.web.layouts.main import MainLayout


def _get_registry(req):
    return req.scope.get("registry")


def _get_active_account(req) -> tuple[str, str] | None:
    """获取当前活动账户"""
    session = req.scope.get("session", {})
    kind = session.get("active_account_kind")
    account_id = session.get("active_account_id")
    if kind and account_id:
        return kind, account_id
    return None


def DateRangePicker(start_date: str = "", end_date: str = ""):
    """日期范围选择器"""
    today = datetime.date.today().isoformat()
    if not start_date:
        start_date = today
    if not end_date:
        end_date = today

    return Div(
        Div(
            Span("开始日期", cls="text-sm text-gray-600 mr-2"),
            Input(
                type="date",
                name="start_date",
                value=start_date,
                cls="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500",
            ),
            cls="flex items-center",
        ),
        Div(
            Span("结束日期", cls="text-sm text-gray-600 mr-2"),
            Input(
                type="date",
                name="end_date",
                value=end_date,
                cls="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500",
            ),
            cls="flex items-center ml-4",
        ),
        Button(
            "查询",
            type="submit",
            cls="ml-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700",
        ),
        cls="flex items-center mb-6",
    )


def PositionTable(positions: list[dict]):
    """持仓表格"""
    if not positions:
        return Div(
            P("暂无持仓数据", cls="text-gray-500 text-center py-8"),
            cls="bg-white rounded-lg shadow",
        )

    return Div(
        Table(
            Thead(
                Tr(
                    Th("日期", cls="pb-3 font-medium text-left"),
                    Th("代码", cls="pb-3 font-medium text-left"),
                    Th("名称", cls="pb-3 font-medium text-left"),
                    Th("持仓数量", cls="pb-3 font-medium text-right"),
                    Th("成本价", cls="pb-3 font-medium text-right"),
                    Th("当前价", cls="pb-3 font-medium text-right"),
                    Th("市值", cls="pb-3 font-medium text-right"),
                    Th("盈亏", cls="pb-3 font-medium text-right"),
                    Th("盈亏率", cls="pb-3 font-medium text-right"),
                ),
                cls="text-sm text-gray-600 border-b",
            ),
            Tbody(
                *[
                    Tr(
                        Td(pos.get("dt", ""), cls="py-3 text-sm"),
                        Td(pos.get("asset", ""), cls="py-3 text-sm font-medium"),
                        Td(pos.get("name", ""), cls="py-3 text-sm text-gray-500"),
                        Td(f"{pos.get('shares', 0):,}", cls="py-3 text-sm text-right"),
                        Td(f"{pos.get('cost_price', 0):.2f}", cls="py-3 text-sm text-right"),
                        Td(f"{pos.get('current_price', 0):.2f}", cls="py-3 text-sm text-right"),
                        Td(f"{pos.get('market_value', 0)/10000:.2f}万", cls="py-3 text-sm text-right"),
                        Td(
                            f"{pos.get('pnl', 0)/10000:.2f}万",
                            cls=f"py-3 text-sm text-right {'text-red-600' if pos.get('pnl', 0) >= 0 else 'text-green-600'}",
                        ),
                        Td(
                            f"{pos.get('pnl_pct', 0)*100:.2f}%",
                            cls=f"py-3 text-sm text-right {'text-red-600' if pos.get('pnl_pct', 0) >= 0 else 'text-green-600'}",
                        ),
                    )
                    for pos in positions
                ],
                cls="text-sm",
            ),
            cls="w-full",
        ),
        cls="bg-white rounded-lg shadow overflow-hidden",
    )


def HistoryPositionsPage(positions: list[dict], start_date: str, end_date: str, account_name: str = ""):
    """历史持仓页面内容"""
    return Div(
        Div(
            H1("历史持仓", cls="text-2xl font-bold text-gray-900"),
            P(f"当前账户: {account_name}" if account_name else "", cls="text-sm text-gray-500 mt-1"),
            cls="mb-6",
        ),
        Form(
            DateRangePicker(start_date, end_date),
            action="/trade/positions/history",
            method="GET",
            cls="mb-6",
        ),
        PositionTable(positions),
        cls="p-6",
    )


def history_positions_list(request):
    """历史持仓列表页面"""
    session = request.scope.get("session", {})
    layout = MainLayout(title="历史持仓", user=session.get("auth"))
    layout.header_active = "交易"
    layout.set_sidebar_active("/trade/positions/history")

    # 获取活动账户
    active = _get_active_account(request)
    if not active:
        layout.main_block = lambda: Div(
            P("请先选择活动账户", cls="text-gray-500 text-center py-8"),
            cls="p-6",
        )
        return HTMLResponse(to_xml(layout.render()))

    kind, account_id = active

    # 获取账户名称
    reg = _get_registry(request)
    account_name = account_id
    if reg:
        broker = reg.get(BrokerKind(kind), account_id)
        if broker and hasattr(broker, "portfolio_name"):
            account_name = broker.portfolio_name

    # 获取查询日期范围
    start_date = request.query_params.get("start_date", "")
    end_date = request.query_params.get("end_date", "")

    if not start_date:
        start_date = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
    if not end_date:
        end_date = datetime.date.today().isoformat()

    # 查询持仓数据
    positions = []
    try:
        df = db.query_positions(
            portfolio_id=account_id,
            start=datetime.date.fromisoformat(start_date),
            end=datetime.date.fromisoformat(end_date),
        )
        if not df.is_empty():
            for row in df.iter_rows(named=True):
                # 计算盈亏
                shares = row.get("shares", 0)
                cost_price = row.get("price", 0)  # 成本价
                current_price = row.get("current_price", cost_price)  # 当前价
                market_value = shares * current_price
                cost_value = shares * cost_price
                pnl = market_value - cost_value
                pnl_pct = (pnl / cost_value) if cost_value else 0

                positions.append({
                    "dt": row.get("dt", ""),
                    "asset": row.get("asset", ""),
                    "name": row.get("name", ""),
                    "shares": shares,
                    "cost_price": cost_price,
                    "current_price": current_price,
                    "market_value": market_value,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                })
    except Exception as e:
        print(f"Failed to query positions: {e}")

    layout.main_block = lambda: HistoryPositionsPage(positions, start_date, end_date, account_name)
    return HTMLResponse(to_xml(layout.render()))
