"""系统维护 - 行情数据模块"""

import asyncio
import datetime

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.calendar import calendar
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR


def _get_market_data(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    adjust: str = "none",
    page: int = 1,
    per_page: int = 20,
):
    """获取行情数据"""
    try:
        if not code and not start_date and not end_date:
            return [], 0

        if not start_date:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")

        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        if not code:
            return [], 0

        df = daily_bars.get_bars_in_range(
            start=start_dt,
            end=end_dt,
            assets=[code],
            adjust=adjust if adjust != "none" else None,
            eager_mode=True,
        )

        if df is None or len(df) == 0:
            return [], 0

        data = df.to_pandas().to_dict("records")
        total = len(data)

        # 分页
        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total)
        page_data = data[start_idx:end_idx]

        return page_data, total

    except Exception as e:
        logger.error(f"获取行情数据失败: {e}")
        return [], 0


def _build_market_table(data: list, page: int = 1, per_page: int = 20, total: int = 0,
                        code: str = "", start_date: str = "", end_date: str = "", adjust: str = "none"):
    """构建行情数据表格"""
    rows = []
    if data:
        for row in data:
            trade_date = str(row.get("date", "")).split(" ")[0] if row.get("date") else "-"
            open_price = f"{row.get('open', 0):.2f}" if row.get('open') is not None else "-"
            high_price = f"{row.get('high', 0):.2f}" if row.get('high') is not None else "-"
            low_price = f"{row.get('low', 0):.2f}" if row.get('low') is not None else "-"
            close_price = f"{row.get('close', 0):.2f}" if row.get('close') is not None else "-"
            volume = f"{row.get('volume', 0):,.0f}" if row.get('volume') is not None else "-"
            amount = f"{row.get('amount', 0):,.2f}" if row.get('amount') is not None else "-"
            up_limit = f"{row.get('up_limit', 0):.2f}" if row.get('up_limit') is not None else "-"
            down_limit = f"{row.get('down_limit', 0):.2f}" if row.get('down_limit') is not None else "-"
            adjust_val = f"{row.get('adjust', 1.0):.4f}" if row.get('adjust') is not None else "-"
            is_st = "是" if row.get('is_st') else "否"

            # 计算涨跌
            pre_close = row.get('pre_close', row.get('open', 0))
            cur_close = row.get('close', 0)
            if pre_close and cur_close and pre_close != 0:
                change = cur_close - pre_close
                change_pct = (change / pre_close) * 100
                change_cls = "text-red-500" if change > 0 else ("text-green-500" if change < 0 else "text-gray-500")
                change_str = f"{change:+.2f} ({change_pct:+.2f}%)"
            else:
                change_cls = "text-gray-500"
                change_str = "-"

            rows.append(
                Tr(
                    Td(trade_date, cls="px-4 py-2 text-sm text-gray-900"),
                    Td(open_price, cls="px-4 py-2 text-sm text-gray-900"),
                    Td(high_price, cls="px-4 py-2 text-sm text-gray-900"),
                    Td(low_price, cls="px-4 py-2 text-sm text-gray-900"),
                    Td(close_price, cls="px-4 py-2 text-sm text-gray-900"),
                    Td(Span(change_str, cls=change_cls), cls="px-4 py-2 text-sm"),
                    Td(volume, cls="px-4 py-2 text-sm text-gray-500"),
                    Td(amount, cls="px-4 py-2 text-sm text-gray-500"),
                    Td(up_limit, cls="px-4 py-2 text-sm text-gray-500"),
                    Td(down_limit, cls="px-4 py-2 text-sm text-gray-500"),
                    Td(adjust_val, cls="px-4 py-2 text-sm text-gray-500"),
                    Td(is_st, cls="px-4 py-2 text-sm text-gray-500"),
                    cls="hover:bg-gray-50"
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无数据，请输入查询条件", colspan="12", cls="px-4 py-8 text-center text-gray-500")
            )
        )

    # 分页
    total_pages = max(1, (total + per_page - 1) // per_page) if total > 0 else 1
    pagination_info = f"显示第 {(page - 1) * per_page + 1} 到 {min(page * per_page, total)} 条，共 {total} 条" if total > 0 else "暂无数据"

    # 构建分页链接的基础URL
    def page_url(p):
        return f"/system/market?code={code}&start_date={start_date}&end_date={end_date}&adjust={adjust}&page={p}&per_page={per_page}"

    return Div(
        Div(
            Table(
                Thead(
                    Tr(
                        Th("日期", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("开盘", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("最高", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("最低", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("收盘", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("涨跌", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("成交量", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("成交额", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("涨停价", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("跌停价", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("复权因子", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("ST", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                    )
                ),
                Tbody(*rows),
                cls="min-w-full divide-y divide-gray-200"
            ),
            cls="overflow-x-auto"
        ),
        Div(
            Div(pagination_info, cls="text-sm text-gray-700"),
            Div(
                Span("每页: ", cls="text-sm text-gray-600 mr-1"),
                A("20", href=page_url(1).replace(f"per_page={per_page}", "per_page=20"),
                  cls=f"btn btn-sm {'btn-primary' if per_page == 20 else 'btn-outline'} mx-0.5"),
                A("50", href=page_url(1).replace(f"per_page={per_page}", "per_page=50"),
                  cls=f"btn btn-sm {'btn-primary' if per_page == 50 else 'btn-outline'} mx-0.5"),
                A("100", href=page_url(1).replace(f"per_page={per_page}", "per_page=100"),
                  cls=f"btn btn-sm {'btn-primary' if per_page == 100 else 'btn-outline'} mx-0.5"),
                cls="flex items-center ml-4"
            ),
            Div(
                *[
                    A(str(p), href=page_url(p),
                      cls=f"btn btn-sm {'btn-primary' if p == page else 'btn-outline'} mx-0.5")
                    for p in range(max(1, page - 2), min(total_pages + 1, page + 3))
                ],
                cls="flex items-center ml-4"
            ),
            cls="px-4 py-3 bg-gray-50 border-t border-gray-200 flex items-center justify-between"
        ),
        cls="bg-white rounded-lg shadow overflow-hidden"
    )


async def market_page(req, code: str = "", start_date: str = "", end_date: str = "",
                      adjust: str = "none", page: int = 1, per_page: int = 20):
    """行情数据页面"""
    code = code or req.query_params.get("code", "")
    start_date = start_date or req.query_params.get("start_date", "")
    end_date = end_date or req.query_params.get("end_date", "")
    adjust = adjust or req.query_params.get("adjust", "none")
    page = int(req.query_params.get("page", page))
    per_page = int(req.query_params.get("per_page", per_page))

    logger.info(f"market_page called with code='{code}', start_date='{start_date}', end_date='{end_date}'")

    # 获取数据
    data, total = _get_market_data(code, start_date, end_date, adjust, page, per_page)

    # 构建页面内容
    layout = MainLayout(title="行情数据")
    layout.set_sidebar_active("/system/market")

    # 复权按钮
    def adjust_btn(label: str, value: str) -> A:
        url = f"/system/market?code={code}&start_date={start_date}&end_date={end_date}&adjust={value}&page=1&per_page={per_page}"
        return A(label, href=url,
                 cls=f"btn btn-sm {'btn-primary' if adjust == value else 'btn-outline'} mx-0.5")

    page_content = Div(
        # 页面标题
        Div(
            Div(
                UkIcon("bar-chart-2", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("行情数据", cls="text-2xl font-bold"),
                cls="flex items-center"
            ),
            cls="mb-6"
        ),
        # 筛选条件
        Div(
            Form(
                Div(
                    Div(
                        Label("证券代码", cls="block text-sm font-medium text-gray-700 mb-1"),
                        Input(type="text", name="code", value=code, placeholder="输入股票代码",
                              cls="input input-bordered w-full"),
                        cls="mb-2"
                    ),
                    Div(
                        Label("起始日期", cls="block text-sm font-medium text-gray-700 mb-1"),
                        Input(type="date", name="start_date", value=start_date,
                              cls="input input-bordered w-full"),
                        cls="mb-2"
                    ),
                    Div(
                        Label("结束日期", cls="block text-sm font-medium text-gray-700 mb-1"),
                        Input(type="date", name="end_date", value=end_date,
                              cls="input input-bordered w-full"),
                        cls="mb-2"
                    ),
                    Div(
                        Label(" ", cls="block mb-1"),
                        Button("查询", type="submit", cls="btn btn-primary w-full"),
                        cls="flex items-end"
                    ),
                    cls="grid grid-cols-1 md:grid-cols-4 gap-4"
                ),
                cls="bg-white p-4 rounded-lg shadow mb-3"
            ),
            cls="mb-4"
        ),
        # 复权方式按钮和更新按钮
        Div(
            Div(
                Span("复权方式：", cls="text-sm font-medium text-gray-700"),
                adjust_btn("不复权", "none"),
                adjust_btn("前复权", "qfq"),
                cls="flex items-center"
            ),
            Button(
                "立即更新",
                hx_post="/system/market/sync",
                hx_target="#market-content",
                hx_swap="innerHTML",
                cls="btn btn-sm btn-secondary"
            ),
            cls="bg-white p-3 rounded-lg shadow flex justify-between items-center mb-4"
        ),
        # 行情表格
        Div(
            _build_market_table(data, page, per_page, total, code, start_date, end_date, adjust),
            id="market-content"
        ),
        cls="p-8"
    )

    layout.main_block = page_content
    return layout.render()


async def market_sync(req):
    """同步行情数据"""
    try:
        await asyncio.to_thread(daily_bars.store.update)
        logger.info("行情数据同步完成")

        # 返回当前页面
        code = req.query_params.get("code", "")
        start_date = req.query_params.get("start_date", "")
        end_date = req.query_params.get("end_date", "")
        adjust = req.query_params.get("adjust", "none")

        data, total = _get_market_data(code, start_date, end_date, adjust, 1, 20)
        return _build_market_table(data, 1, 20, total, code, start_date, end_date, adjust)
    except Exception as e:
        logger.error(f"同步行情数据失败: {e}")
        return Div(
            UkIcon("alert-circle", cls="text-red-500 mr-2"),
            f"同步失败: {str(e)}",
            cls="flex items-center p-4 bg-red-50 text-red-700 rounded-lg"
        )
