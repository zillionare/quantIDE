"""系统维护 - 交易日历模块"""

import calendar as cal_lib
import datetime

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.data.models.calendar import calendar as trade_calendar
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR


def _get_calendar_data(year: int, month: int) -> list[dict]:
    """获取指定年月的日历数据"""
    days_in_month = cal_lib.monthrange(year, month)[1]

    trade_cal_map = {}
    if trade_calendar._data is not None:
        try:
            date_col = trade_calendar._data.column("date")
            is_open_col = trade_calendar._data.column("is_open")

            for i in range(len(date_col)):
                trade_cal_map[date_col[i].as_py()] = bool(is_open_col[i].as_py())
        except Exception as e:
            logger.warning(f"Error building calendar map: {e}")

    data = []
    for day in range(1, days_in_month + 1):
        current_date = datetime.date(year, month, day)
        is_trading = trade_cal_map.get(current_date, False)

        data.append({
            "date": current_date,
            "is_trading": is_trading,
            "day_of_week": current_date.weekday(),
        })

    return data


def _build_calendar_grid(year: int, month: int) -> Div:
    """构建日历网格"""
    cal_data = _get_calendar_data(year, month)

    # 生成日历网格
    weeks = []
    current_week = [None] * 7

    for item in cal_data:
        day_of_week = item["day_of_week"]
        current_week[day_of_week] = item

        if day_of_week == 6:
            weeks.append(current_week)
            current_week = [None] * 7

    if any(day is not None for day in current_week):
        weeks.append(current_week)

    # 星期标题
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    header_cells = [
        Div(name, cls="text-center font-bold p-2 text-gray-600")
        for name in weekday_names
    ]

    # 日历行
    rows = []
    for week in weeks:
        cells = []
        for item in week:
            if item is None:
                cells.append(Div(cls="p-3 min-h-[70px] bg-gray-50"))
            else:
                day_num = item["date"].day
                if item["is_trading"]:
                    # 交易日：正常显示
                    cells.append(
                        Div(
                            Span(str(day_num), cls="text-gray-800 font-medium text-lg"),
                            cls="p-3 text-center min-h-[70px] flex flex-col items-center justify-center"
                        )
                    )
                else:
                    # 休市日
                    day_of_week = item["day_of_week"]
                    if day_of_week < 5:
                        # 工作日休市
                        cells.append(
                            Div(
                                Span(str(day_num), cls="text-red-500 font-medium text-lg"),
                                Span("休市", cls="text-xs text-red-400 mt-1"),
                                cls="p-3 text-center bg-red-50 min-h-[70px] flex flex-col items-center justify-center"
                            )
                        )
                    else:
                        # 周末
                        cells.append(
                            Div(
                                Span(str(day_num), cls="text-red-500 font-medium text-lg"),
                                cls="p-3 text-center bg-red-50 min-h-[70px] flex flex-col items-center justify-center"
                            )
                        )
        rows.append(Div(*cells, cls="grid grid-cols-7 gap-1 mb-1"))

    return Div(
        Div(*header_cells, cls="grid grid-cols-7 gap-1 mb-2 bg-gray-200 p-2 rounded-t-lg"),
        *rows,
        cls="bg-white rounded-lg shadow-lg overflow-hidden"
    )


async def calendar_page(req, year: int = None, month: int = None):
    """交易日历页面"""
    now = datetime.datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    # 计算导航参数
    prev_month = month - 1 if month > 1 else 12
    prev_month_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_month_year = year if month < 12 else year + 1
    prev_year = year - 1
    next_year = year + 1

    # 年份选项
    year_options = [
        Option(str(y), value=str(y), selected=(y == year))
        for y in range(year - 5, year + 6)
    ]

    # 月份选项
    month_options = [
        Option(str(m), value=str(m), selected=(m == month))
        for m in range(1, 13)
    ]

    # 构建页面内容
    layout = MainLayout(title="交易日历")
    layout.set_sidebar_active("/system/calendar")

    page_content = Div(
        # 页面标题
        Div(
            Div(
                UkIcon("calendar", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("交易日历", cls="text-2xl font-bold"),
                cls="flex items-center"
            ),
            cls="mb-6"
        ),
        # 第一行：更新按钮
        Div(
            Button(
                "立即更新",
                hx_post="/system/calendar/sync",
                hx_target="#calendar-content",
                hx_swap="innerHTML",
                cls="btn btn-sm btn-secondary"
            ),
            cls="flex justify-end mb-4"
        ),
        # 第二行：日历工具条
        Div(
            Div(
                A("<<", href=f"/system/calendar?year={prev_year}&month={month}",
                  cls="btn btn-sm btn-outline", title="上一年"),
                A("<", href=f"/system/calendar?year={prev_month_year}&month={prev_month}",
                  cls="btn btn-sm btn-outline ml-1", title="上一月"),
                A(">", href=f"/system/calendar?year={next_month_year}&month={next_month}",
                  cls="btn btn-sm btn-outline ml-1", title="下一月"),
                A(">>", href=f"/system/calendar?year={next_year}&month={month}",
                  cls="btn btn-sm btn-outline ml-1", title="下一年"),
                cls="flex items-center space-x-1"
            ),
            Div(
                Select(*year_options, name="year", id="year-select",
                       cls="select select-bordered select-sm w-24"),
                Span(" 年 ", cls="mx-1"),
                Select(*month_options, name="month", id="month-select",
                       cls="select select-bordered select-sm w-20"),
                Span(" 月", cls="ml-1"),
                cls="flex items-center"
            ),
            cls="flex justify-between items-center mb-6 p-4 bg-white rounded-lg shadow"
        ),
        # 第三行：日历表格
        Div(
            _build_calendar_grid(year, month),
            id="calendar-content"
        ),
        # 图例说明
        Div(
            Div(
                Div(cls="w-4 h-4 bg-white mr-2 border border-gray-300"),
                Span("交易日", cls="text-sm"),
                cls="flex items-center mr-4"
            ),
            Div(
                Div(cls="w-4 h-4 bg-red-50 mr-2 border border-gray-300"),
                Span("休市日（含周末）", cls="text-sm text-red-500"),
                cls="flex items-center"
            ),
            cls="flex mt-4 p-4 bg-white rounded-lg shadow"
        ),
        cls="p-8"
    )

    layout.main_block = page_content
    return layout.render()


async def calendar_sync(req):
    """同步交易日历数据"""
    try:
        await trade_calendar.update()
        logger.info("交易日历同步完成")

        now = datetime.datetime.now()
        return _build_calendar_grid(now.year, now.month)
    except Exception as e:
        logger.error(f"同步交易日历失败: {e}")
        return Div(
            UkIcon("alert-circle", cls="text-red-500 mr-2"),
            f"同步失败: {str(e)}",
            cls="flex items-center p-4 bg-red-50 text-red-700 rounded-lg"
        )
