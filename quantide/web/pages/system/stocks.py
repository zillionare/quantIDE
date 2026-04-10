"""系统维护 - 股票列表模块"""

import asyncio
import datetime

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.data.models.stocks import stock_list
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR

# 定义子路由应用
system_stocks_app, rt = fast_app(hdrs=AppTheme.headers())


def _build_stock_table(data: list, page: int = 1, per_page: int = 20, total: int = 0, query: str = ""):
    """构建股票列表表格"""
    rows = []
    if data:
        for row in data:
            list_date_str = str(row.get("list_date", "")).split(" ")[0] if row.get("list_date") else ""
            delist_date_str = ""
            if row.get("delist_date") and str(row.get("delist_date")) != "NaT":
                delist_date_str = str(row.get("delist_date")).split(" ")[0]

            delist_info = ""
            if delist_date_str:
                delist_info = Span(f" (退市: {delist_date_str})", cls="text-xs text-red-400")

            rows.append(
                Tr(
                    Td(row.get("asset", ""), cls="px-4 py-2 text-sm text-gray-900"),
                    Td(
                        Span(row.get("name", "")),
                        delist_info,
                        cls="px-4 py-2 text-sm text-gray-900"
                    ),
                    Td(row.get("pinyin", ""), cls="px-4 py-2 text-sm text-gray-500"),
                    Td(list_date_str, cls="px-4 py-2 text-sm text-gray-500"),
                    cls="hover:bg-gray-50"
                )
            )
    else:
        rows.append(
            Tr(
                Td("暂无数据", colspan="4", cls="px-4 py-8 text-center text-gray-500")
            )
        )

    total_pages = max(1, (total + per_page - 1) // per_page) if total > 0 else 1
    pagination_info = f"显示第 {(page - 1) * per_page + 1} 到 {min(page * per_page, total)} 条，共 {total} 条" if total > 0 else "暂无数据"

    def page_url(p, pp=per_page):
        return f"/system/stocks/?page={p}&per_page={pp}&q={query}"

    return Div(
        Div(
            Table(
                Thead(
                    Tr(
                        Th("代码", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("名称", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("拼音", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
                        Th("上市日期", cls="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase"),
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
                A("20", href=page_url(1, 20),
                  cls=f"btn btn-sm {'btn-primary' if per_page == 20 else 'btn-outline'} mx-0.5"),
                A("50", href=page_url(1, 50),
                  cls=f"btn btn-sm {'btn-primary' if per_page == 50 else 'btn-outline'} mx-0.5"),
                A("100", href=page_url(1, 100),
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


@rt("/")
async def index(req, page: int = 1, per_page: int = 20, q: str = ""):
    """股票列表页面"""
    query = q or req.query_params.get("q", "")
    page = int(req.query_params.get("page", page))
    per_page = int(req.query_params.get("per_page", per_page))

    logger.info(f"stocks_page called with query='{query}', page={page}, per_page={per_page}")

    try:
        if query:
            result_df = stock_list.fuzzy_search(query, id_only=False)
            total = len(result_df)
            if total > 0:
                data = result_df.to_pandas().to_dict("records")
            else:
                data = []
        else:
            df = stock_list.data
            if df is not None:
                total = len(df)
                start_idx = (page - 1) * per_page
                end_idx = min(start_idx + per_page, total)
                data = df.slice(start_idx, end_idx - start_idx).to_pandas().to_dict("records")
            else:
                total = 0
                data = []
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        total = 0
        data = []

    layout = MainLayout(title="股票列表")
    layout.set_sidebar_active("/system/stocks")

    page_content = Div(
        Div(
            Div(
                UkIcon("list", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("股票列表", cls="text-2xl font-bold"),
                cls="flex items-center"
            ),
            cls="mb-6"
        ),
        Div(
            Div(
                Form(
                    Input(
                        type="text",
                        name="q",
                        value=query,
                        placeholder="搜索股票代码、名称或拼音...",
                        cls="input input-bordered w-full max-w-md"
                    ),
                    Button("搜索", type="submit", cls="btn btn-primary ml-2"),
                    cls="flex items-center"
                ),
                cls="flex-1"
            ),
            Button(
                "立即更新",
                hx_post="/system/stocks/sync",
                hx_target="#stocks-content",
                hx_swap="innerHTML",
                cls="btn btn-sm btn-secondary"
            ),
            cls="flex justify-between items-center mb-4"
        ),
        Div(
            _build_stock_table(data, page, per_page, total, query),
            id="stocks-content"
        ),
        cls="p-8"
    )

    layout.main_block = page_content
    return layout.render()


@rt("/sync", methods="post")
async def sync_stocks():
    """同步股票列表数据"""
    try:
        await asyncio.to_thread(stock_list.update)
        logger.info(f"Stock list synced: {stock_list.size} records")

        df = stock_list.data
        if df is not None:
            total = len(df)
            data = df.slice(0, 20).to_pandas().to_dict("records")
        else:
            total = 0
            data = []

        return _build_stock_table(data, 1, 20, total, "")
    except Exception as e:
        logger.error(f"同步股票列表失败: {e}")
        return Div(
            UkIcon("alert-circle", cls="text-red-500 mr-2"),
            f"同步失败: {str(e)}",
            cls="flex items-center p-4 bg-red-50 text-red-700 rounded-lg"
        )
