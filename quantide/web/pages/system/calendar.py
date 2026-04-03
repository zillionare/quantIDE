"""系统维护 - 交易日历模块"""

import calendar
import datetime

from fasthtml.common import *
from starlette.responses import HTMLResponse
from loguru import logger

from quantide.data.models.calendar import calendar as trade_calendar

# 初始化日历数据
from pathlib import Path
cal_path = Path('/home/aaron/.config/quantide/data/calendar.parquet')
if cal_path.exists():
    try:
        trade_calendar.load(cal_path)
        logger.info(f"Calendar loaded: {len(trade_calendar._data)} records")
    except Exception as e:
        logger.warning(f"Failed to load calendar data: {e}")


def _get_calendar_data(year: int, month: int) -> list[dict]:
    """获取指定年月的日历数据"""
    days_in_month = calendar.monthrange(year, month)[1]
    
    trade_cal_map = {}
    if trade_calendar._data is not None:
        try:
            date_col = trade_calendar._data.column("date")
            is_open_col = trade_calendar._data.column("is_open")
            prev_col = trade_calendar._data.column("prev")
            
            for i in range(len(date_col)):
                trade_cal_map[date_col[i].as_py()] = {
                    "is_open": bool(is_open_col[i].as_py()),
                    "prev": prev_col[i].as_py()
                }
        except Exception as e:
            logger.warning(f"Error building calendar map: {e}")
    
    data = []
    for day in range(1, days_in_month + 1):
        current_date = datetime.date(year, month, day)
        cal_info = trade_cal_map.get(current_date, {"is_open": False, "prev": None})
        
        data.append({
            "date": current_date,
            "is_open": cal_info["is_open"],
            "prev": cal_info["prev"],
            "day_of_week": current_date.weekday()
        })
    
    return data


def _build_day_cell(item):
    """构建单日单元格 HTML"""
    day_of_week = item["day_of_week"]
    day_num = item["date"].day
    
    if item["is_open"]:
        # 交易日：正常显示
        return f'<div class="p-3 text-center min-h-[70px] flex flex-col items-center justify-center"><span class="text-gray-800 font-medium text-lg">{day_num}</span></div>'
    else:
        # 休市日（包括周末和工作日休市）：红色显示
        if day_of_week < 5:
            return f'<div class="p-3 text-center bg-red-50 min-h-[70px] flex flex-col items-center justify-center"><span class="text-red-500 font-medium text-lg">{day_num}</span><span class="text-xs text-red-400 mt-1">休市</span></div>'
        else:
            return f'<div class="p-3 text-center bg-red-50 min-h-[70px] flex flex-col items-center justify-center"><span class="text-red-500 font-medium text-lg">{day_num}</span></div>'


async def calendar_page(req, year: int = None, month: int = None):
    """交易日历页面"""
    now = datetime.datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month
    
    cal_data = _get_calendar_data(year, month)
    logger.info(f"Calendar data for {year}-{month}: {len(cal_data)} days, {sum(1 for d in cal_data if d['is_open'])} trading days")
    
    # 生成日历网格
    weeks = []
    current_week = [None] * 7
    
    for item in cal_data:
        day_of_week = item["day_of_week"]
        current_week[day_of_week] = _build_day_cell(item)
        
        if day_of_week == 6:
            weeks.append(current_week)
            current_week = [None] * 7
    
    if any(day is not None for day in current_week):
        for i in range(7):
            if current_week[i] is None:
                current_week[i] = '<div class="p-3 min-h-[70px]"></div>'
        weeks.append(current_week)
    
    # 构建星期标题
    weekday_headers = ''.join([f'<div class="text-center font-bold p-2 text-gray-600">{day}</div>' for day in ['周一', '周二', '周三', '周四', '周五', '周六', '周日']])
    
    # 构建日历行
    calendar_rows = ''.join([f'<div class="grid grid-cols-7 gap-1 mb-1">{"".join([day if day else "<div class=\'p-3 min-h-[70px]\'></div>" for day in week])}</div>' for week in weeks])
    
    # 构建年份选项
    year_opts = ''.join([f'<option value="{y}" {"selected" if y == year else ""}>{y}</option>' for y in range(year - 5, year + 6)])
    month_opts = ''.join([f'<option value="{m}" {"selected" if m == month else ""}>{m}</option>' for m in range(1, 13)])
    
    # 完整的 HTML 页面
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>交易日历 - QuantIDE</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50">
    <div class="container mx-auto p-4 max-w-6xl">
        <h2 class="text-2xl font-bold text-gray-800 mb-4">交易日历</h2>
        
        <div class="flex justify-between items-center mb-6 p-4 bg-white rounded-lg shadow">
            <div class="flex items-center">
                <select id="year-select" class="select select-bordered select-sm w-24 mr-2">
                    {year_opts}
                </select>
                <span class="mr-2">年</span>
                <select id="month-select" class="select select-bordered select-sm w-20">
                    {month_opts}
                </select>
                <span class="mr-2">月</span>
                <button onclick="navigateMonth()" class="btn btn-sm btn-primary mr-2">跳转</button>
            </div>
            <button hx-post="/system/calendar/sync" hx-target="body" hx-swap="innerHTML" class="btn btn-sm btn-secondary">
                立即更新
            </button>
        </div>
        
        <div class="bg-white rounded-lg shadow-lg overflow-hidden">
            <div class="grid grid-cols-7 gap-1 mb-2 bg-gray-200 p-2 rounded-t-lg">
                {weekday_headers}
            </div>
            {calendar_rows}
        </div>
        
        <div class="flex mt-4 p-4 bg-white rounded-lg shadow">
            <div class="flex items-center mr-4">
                <div class="w-4 h-4 bg-white mr-2"></div>
                <span class="text-sm">交易日</span>
            </div>
            <div class="flex items-center">
                <div class="w-4 h-4 bg-red-50 mr-2"></div>
                <span class="text-sm">休市日（含周末）</span>
            </div>
        </div>
    </div>
    
    <script>
    function navigateMonth() {{
        const year = document.getElementById('year-select').value;
        const month = document.getElementById('month-select').value;
        window.location.href = `/system/calendar?year=${{year}}&month=${{month}}`;
    }}
    </script>
</body>
</html>"""
    
    return HTMLResponse(content=html_content, status_code=200)


async def calendar_sync(req):
    """同步交易日历数据"""
    try:
        trade_calendar.update()
        now = datetime.datetime.now()
        return await calendar_page(req, now.year, now.month)
    except Exception as e:
        logger.error(f"同步交易日历失败: {e}")
        error_html = f"""<!DOCTYPE html>
<html>
<head><title>错误</title></head>
<body>
    <div class="container mx-auto p-4">
        <div class="alert alert-error">同步失败: {str(e)}</div>
    </div>
</body>
</html>"""
        return HTMLResponse(content=error_html, status_code=200)
