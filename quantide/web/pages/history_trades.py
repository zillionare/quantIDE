"""历史成交页面"""

import datetime

from fasthtml.common import *
from monsterui.all import *
from starlette.responses import HTMLResponse

from quantide.core.enums import BrokerKind
from quantide.data.sqlite import db
from quantide.web.layouts.main import MainLayout


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
            Input(type="date", name="start_date", value=start_date,
                  cls="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"),
            cls="flex items-center",
        ),
        Div(
            Span("结束日期", cls="text-sm text-gray-600 mr-2"),
            Input(type="date", name="end_date", value=end_date,
                  cls="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"),
            cls="flex items-center ml-4",
        ),
        Button("查询", type="submit",
               cls="ml-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"),
        cls="flex items-center mb-6",
    )


def TradesTable(trades: list[dict]):
    """成交表格"""
    if not trades:
        return Div(P("暂无成交数据", cls="text-gray-500 text-center py-8"), cls="bg-white rounded-lg shadow")

    return Div(
        Table(
            Thead(
                Tr(
                    Th("时间", cls="pb-3 font-medium text-left"),
                    Th("代码", cls="pb-3 font-medium text-left"),
                    Th("名称", cls="pb-3 font-medium text-left"),
                    Th("方向", cls="pb-3 font-medium text-left"),
                    Th("成交价", cls="pb-3 font-medium text-right"),
                    Th("成交量", cls="pb-3 font-medium text-right"),
                    Th("成交金额", cls="pb-3 font-medium text-right"),
                    Th("手续费", cls="pb-3 font-medium text-right"),
                ),
                cls="text-sm text-gray-600 border-b",
            ),
            Tbody(*[
                Tr(
                    Td(trade.get("tm", ""), cls="py-3 text-sm"),
                    Td(trade.get("asset", ""), cls="py-3 text-sm font-medium"),
                    Td(trade.get("name", ""), cls="py-3 text-sm text-gray-500"),
                    Td(trade.get("side", ""), cls=f"py-3 text-sm {'text-red-600' if trade.get('side') == '买入' else 'text-green-600'}"),
                    Td(f"{trade.get('price', 0):.2f}", cls="py-3 text-sm text-right"),
                    Td(f"{trade.get('shares', 0):,}", cls="py-3 text-sm text-right"),
                    Td(f"{trade.get('amount', 0)/10000:.2f}万", cls="py-3 text-sm text-right"),
                    Td(f"{trade.get('fee', 0):.2f}", cls="py-3 text-sm text-right"),
                )
                for trade in trades
            ], cls="text-sm"),
            cls="w-full",
        ),
        cls="bg-white rounded-lg shadow overflow-hidden",
    )


def history_trades_list(request):
    """历史成交列表页面"""
    session = request.scope.get("session", {})
    layout = MainLayout(title="历史成交", user=session.get("auth"))
    layout.header_active = "交易"
    layout.set_sidebar_active("/trade/records/history")

    active = _get_active_account(request)
    if not active:
        layout.main_block = lambda: Div(P("请先选择活动账户", cls="text-gray-500 text-center py-8"), cls="p-6")
        return HTMLResponse(to_xml(layout.render()))

    kind, account_id = active

    start_date = request.query_params.get("start_date", "")
    end_date = request.query_params.get("end_date", "")

    if not start_date:
        start_date = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
    if not end_date:
        end_date = datetime.date.today().isoformat()

    trades = []
    try:
        # 从数据库查询成交记录
        df = db.get_trades(
            dt=datetime.date.fromisoformat(start_date),
            portfolio_id=account_id,
        )
        if not df.is_empty():
            for row in df.iter_rows(named=True):
                trades.append({
                    "tm": str(row.get("tm", ""))[:19],
                    "asset": row.get("asset", ""),
                    "name": row.get("name", ""),
                    "side": "买入" if row.get("side") == "BUY" else "卖出",
                    "price": row.get("price", 0),
                    "shares": row.get("shares", 0),
                    "amount": row.get("amount", 0),
                    "fee": row.get("fee", 0),
                })
    except Exception as e:
        print(f"Failed to query trades: {e}")

    def main_block():
        return Div(
            Div(H1("历史成交", cls="text-2xl font-bold text-gray-900"), cls="mb-6"),
            Form(DateRangePicker(start_date, end_date), action="/trade/records/history", method="GET", cls="mb-6"),
            TradesTable(trades),
            cls="p-6",
        )

    layout.main_block = main_block
    return HTMLResponse(to_xml(layout.render()))
