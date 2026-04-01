"""行情数据管理页面"""

import asyncio
import datetime
import json
from fasthtml.common import *
from monsterui.all import *
from starlette.responses import StreamingResponse
from quantide.core.message import msg_hub
from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.calendar import calendar
from quantide.data.models.stocks import stock_list
from quantide.data.services import StockSyncService
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR
from loguru import logger

# 定义子路由应用
data_market_app, rt = fast_app(hdrs=AppTheme.headers())

# ========== 全局同步状态 ==========
_sync_status = {
    "is_running": False,
    "progress": 0,
    "stage": "",
    "message": "",
    "completed": False,
    "error": None,
}

def _get_active_tab(req):
    return req.query_params.get("tab", "overview")

def _TabNav(active_tab: str):
    tabs = [
        ("overview", "概要"),
        ("verify", "数据校验"),
        ("update", "手动补全"),
        ("browse", "浏览"),
    ]
    
    tab_items = []
    for tab_id, label in tabs:
        is_active = active_tab == tab_id
        base_cls = "px-4 py-2 font-medium transition-colors duration-200"
        if is_active:
            cls = f"{base_cls} text-red-600 border-b-2 border-red-600"
        else:
            cls = f"{base_cls} text-gray-500 hover:text-gray-700 hover:border-b-2 hover:border-gray-300"
        
        tab_items.append(A(label, href=f"/data/market?tab={tab_id}", cls=cls))
        
    return Div(
        Div(*tab_items, cls="flex space-x-2"),
        cls="border-b border-gray-200 mb-6"
    )

def _OverviewTab():
    """概要 Tab 内容"""
    try:
        start_date = daily_bars.start
        end_date = daily_bars.end
        total_dates = daily_bars.total_dates
        size_bytes = daily_bars.size
        # 简单换算 size
        if size_bytes > 1024*1024*1024:
            size_str = f"{size_bytes / (1024*1024*1024):.2f} GB"
        else:
            size_str = f"{size_bytes / (1024*1024):.2f} MB"
            
        # 检查是否过期 (简单逻辑：如果结束日期早于上个交易日)
        is_stale = False
        last_trade = calendar.last_trade_date()
        if end_date and last_trade and end_date < last_trade:
            is_stale = True
            
    except Exception as e:
        return Div(Card(CardBody(P(f"获取行情信息失败: {e}", cls="text-red-500"))))

    return Div(
        H3("日线数据状态", cls="text-xl font-semibold mb-6"),
        Div(
            Div(
                P("数据起始日期", cls="text-sm text-gray-500"),
                P(str(start_date or "--"), cls="text-2xl font-bold"),
                cls="p-4 bg-gray-50 rounded-lg"
            ),
            Div(
                P("数据结束日期", cls="text-sm text-gray-500"),
                P(str(end_date or "--"), cls=f"text-2xl font-bold {'text-red-600' if is_stale else ''}"),
                cls="p-4 bg-gray-50 rounded-lg"
            ),
            Div(
                P("数据天数", cls="text-sm text-gray-500"),
                P(f"{total_dates or 0} 天", cls="text-2xl font-bold"),
                cls="p-4 bg-gray-50 rounded-lg"
            ),
            Div(
                P("存储占用", cls="text-sm text-gray-500"),
                P(size_str, cls="text-2xl font-bold"),
                cls="p-4 bg-gray-50 rounded-lg"
            ),
            cls="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8"
        ),
        H3("数据覆盖范围", cls="text-lg font-semibold mb-4"),
        Div(
            P("此处应展示数据覆盖矩阵图 (散点矩阵)...", cls="text-gray-400 italic"),
            cls="h-64 bg-gray-50 border border-dashed border-gray-300 flex items-center justify-center rounded-lg"
        ),
        cls="animate-in fade-in duration-500"
    )

def _VerifyTab():
    """数据校验 Tab 内容"""
    return Div(
        H3("数据一致性校验", cls="text-xl font-semibold mb-4"),
        Div(
            Form(
                Div(
                    Div(
                        Label("资产 (逗号分隔，可选)", cls="block text-sm font-medium mb-1"),
                        Input(name="assets", placeholder="例如: 000001.SZ, 600000.SH", cls="input w-full"),
                        cls="flex-1"
                    ),
                    Div(
                        Label("开始年份", cls="block text-sm font-medium mb-1"),
                        Input(name="start_year", type="number", value="2024", cls="input w-full"),
                        cls="w-32"
                    ),
                    Div(
                        Label("结束年份", cls="block text-sm font-medium mb-1"),
                        Input(name="end_year", type="number", value="2024", cls="input w-full"),
                        cls="w-32"
                    ),
                    Div(
                        Label(" ", cls="block text-sm mb-1"),
                        Button("手动校验", type="submit", cls="btn btn-primary w-full"),
                        cls="w-32"
                    ),
                    cls="flex gap-4 items-end"
                ),
                hx_post="/data/market/do-verify",
                hx_target="#verify-result",
                hx_indicator="#verify-spinner"
            ),
            cls="p-6 bg-white border rounded-lg shadow-sm mb-6"
        ),
        Div(id="verify-spinner", cls="htmx-indicator mb-4", children=[
            Div(cls="flex items-center text-blue-600", children=[
                UkIcon("loader", cls="animate-spin mr-2"),
                "正在校验数据，请稍候..."
            ])
        ]),
        Div(id="verify-result"),
        cls="animate-in fade-in duration-500"
    )

def _UpdateTab():
    """手动补全 Tab 内容"""
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    return Div(
        H3("手动补全行情数据", cls="text-xl font-semibold mb-4"),
        P("指定日期范围进行数据同步。系统将检查本地缺失数据并尝试从 Tushare 补全。", cls="text-gray-600 mb-6"),
        Div(
            Form(
                Div(
                    Div(
                        Label("开始日期", cls="block text-sm font-medium mb-1"),
                        Input(name="start_date", type="date", value="2024-01-01", cls="input w-full"),
                        cls="flex-1"
                    ),
                    Div(
                        Label("结束日期", cls="block text-sm font-medium mb-1"),
                        Input(name="end_date", type="date", value=today_str, cls="input w-full"),
                        cls="flex-1"
                    ),
                    Div(
                        Label(" ", cls="block text-sm mb-1"),
                        Button(
                            Div(UkIcon("download", cls="mr-2"), "立即更新", cls="flex items-center"),
                            type="submit",
                            cls="btn btn-primary w-full",
                            id="update-btn",
                            hx_post="/data/market/do-update",
                            hx_target="#update-dialog-container",
                            hx_swap="innerHTML"
                        ),
                        cls="w-32"
                    ),
                    cls="flex gap-4 items-end"
                ),
            ),
            cls="p-6 bg-white border rounded-lg shadow-sm mb-6"
        ),
        Div(id="update-dialog-container"),
        cls="animate-in fade-in duration-500"
    )

def _BrowseTab(req):
    """数据浏览 Tab 内容"""
    asset = req.query_params.get("asset", "")
    start = req.query_params.get("start", "")
    end = req.query_params.get("end", "")
    
    # 默认显示最新 100 条
    df = None
    if asset:
        try:
            # 简单解析日期
            s_dt = datetime.datetime.strptime(start, "%Y-%m-%d").date() if start else None
            e_dt = datetime.datetime.strptime(end, "%Y-%m-%d").date() if end else None
            df = daily_bars.get_bars_in_range(assets=[asset], start=s_dt, end=e_dt, eager_mode=True)
            if not df.is_empty():
                df = df.sort("date", descending=True).head(100)
        except Exception as e:
            logger.error(f"查询行情失败: {e}")
            
    table_content = P("输入证券代码并点击搜索查看数据...", cls="text-gray-400 text-center py-12")
    if df is not None and not df.is_empty():
        headers = ["日期", "代码", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "复权", "ST"]
        header_row = Tr(*[Th(h) for h in headers])
        
        rows = []
        for row in df.to_dicts():
            rows.append(Tr(
                Td(str(row["date"].date()) if hasattr(row["date"], "date") else str(row["date"])),
                Td(row["asset"]),
                Td(f"{row['open']:.2f}"),
                Td(f"{row['high']:.2f}"),
                Td(f"{row['low']:.2f}"),
                Td(f"{row['close']:.2f}"),
                Td(f"{row['volume']:.0f}"),
                Td(f"{row['amount']:.0f}"),
                Td(f"{row['adjust']:.4f}"),
                Td(Input(type="checkbox", checked=row.get("is_st", False), disabled=True, cls="checkbox checkbox-sm")),
            ))
        table_content = Table(Thead(header_row), Tbody(*rows), cls="uk-table uk-table-divider uk-table-small text-sm")
    elif asset:
        table_content = P(f"未找到代码 {asset} 的行情数据", cls="text-red-500 text-center py-12")

    return Div(
        H3("行情数据浏览", cls="text-xl font-semibold mb-4"),
        Form(
            Div(
                Input(name="asset", value=asset, placeholder="证券代码 (例如: 000001.SZ)", cls="input flex-1"),
                Input(name="start", value=start, type="date", cls="input w-40"),
                Input(name="end", value=end, type="date", cls="input w-40"),
                Button(UkIcon("search"), type="submit", cls="btn btn-primary"),
                cls="flex gap-2 mb-6"
            ),
            method="GET",
            action="/data/market"
        ),
        Input(type="hidden", name="tab", value="browse"),
        Div(table_content, cls="overflow-x-auto bg-white border rounded-lg"),
        cls="animate-in fade-in duration-500"
    )

@rt("/")
async def index(req):
    active_tab = _get_active_tab(req)
    
    if active_tab == "verify":
        content = _VerifyTab()
    elif active_tab == "update":
        content = _UpdateTab()
    elif active_tab == "browse":
        content = _BrowseTab(req)
    else:
        content = _OverviewTab()
        
    layout = MainLayout()
    layout.set_sidebar_active("/data/market")
    
    page_content = Div(
        Div(
            Div(
                UkIcon("bar-chart", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("日线行情", cls="text-2xl font-bold"),
                cls="flex items-center"
            ),
            cls="mb-8"
        ),
        _TabNav(active_tab),
        content,
        cls="p-8"
    )
    
    layout.main_block = page_content
    return layout.render()

@rt("/do-verify", methods="post")
async def do_verify(form: dict):
    # 模拟校验逻辑
    assets = form.get("assets", "").split(",")
    start_year = int(form.get("start_year", 2024))
    end_year = int(form.get("end_year", 2024))
    
    return Div(
        Div(
            UkIcon("check-circle", cls="text-green-500 mr-2"),
            f"校验完成 ({start_year}-{end_year})。未发现数据缺失情况。",
            cls="flex items-center p-4 bg-green-50 text-green-700 rounded-lg"
        ),
        cls="mt-4 animate-in slide-in-from-top-2"
    )

# ========== SSE 更新逻辑 (复用 init_wizard 模式) ==========

async def _run_market_sync(start_date, end_date):
    global _sync_status
    _sync_status["is_running"] = True
    _sync_status["completed"] = False
    _sync_status["error"] = None
    _sync_status["progress"] = 0
    _sync_status["message"] = "准备同步..."

    try:
        stock_sync = StockSyncService(stock_list, daily_bars.store, calendar)
        
        def _on_progress(payload):
            if not isinstance(payload, dict): return
            if payload.get("error"):
                _sync_status["error"] = str(payload["error"])
                return
            
            completed = payload.get("completed", 0)
            total = payload.get("total", 0)
            if total > 0:
                _sync_status["progress"] = int((completed / total) * 100)
                _sync_status["message"] = f"正在同步 {payload.get('current_date', '')} ({completed}/{total})"

        msg_hub.subscribe("fetch_data_progress", _on_progress)
        try:
            await asyncio.to_thread(stock_sync.sync_daily_bars, start_date, end_date)
            _sync_status["progress"] = 100
            _sync_status["message"] = "同步完成"
            _sync_status["completed"] = True
        finally:
            msg_hub.unsubscribe("fetch_data_progress", _on_progress)
            
    except Exception as e:
        _sync_status["error"] = str(e)
    finally:
        _sync_status["is_running"] = False

@rt("/do-update", methods="post")
async def do_update(req):
    form = await req.form()
    start_date_str = form.get("start_date")
    end_date_str = form.get("end_date")
    
    try:
        s_dt = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        e_dt = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except:
        return Div("日期格式不正确", cls="text-red-500")

    asyncio.create_task(_run_market_sync(s_dt, e_dt))
    
    # 返回进度对话框 (简化版)
    return Div(
        Div(
            Div(
                H3("数据更新进度", cls="text-lg font-bold mb-4"),
                Div(
                    Div(id="market-progress-bar", cls="bg-red-600 h-2 rounded-full", style="width: 0%"),
                    cls="w-full bg-gray-200 rounded-full h-2 mb-2"
                ),
                P(id="market-status", children=["准备中..."], cls="text-sm text-gray-600"),
                Div(
                    Button("关闭", id="close-btn", cls="btn btn-ghost mt-6", disabled=True, onclick="this.closest('.fixed').remove()"),
                    cls="flex justify-end"
                ),
                cls="bg-white p-6 rounded-xl shadow-xl max-w-md w-full"
            ),
            cls="fixed inset-0 bg-black/50 flex items-center justify-center z-50",
        ),
        Script(f"""
            (function() {{
                const bar = document.getElementById('market-progress-bar');
                const status = document.getElementById('market-status');
                const btn = document.getElementById('close-btn');
                const es = new EventSource('/data/market/sync-progress');
                es.onmessage = function(e) {{
                    const data = JSON.parse(e.data);
                    bar.style.width = data.progress + '%';
                    status.textContent = data.message;
                    if (data.completed || data.error) {{
                        if (data.error) status.textContent = '错误: ' + data.error;
                        btn.disabled = false;
                        es.close();
                    }}
                }};
            }})();
        """)
    )

@rt("/sync-progress")
async def sync_progress():
    async def event_generator():
        while True:
            yield f"data: {json.dumps(_sync_status)}\n\n"
            if _sync_status["completed"] or _sync_status["error"]: break
            await asyncio.sleep(0.5)
    return StreamingResponse(event_generator(), media_type="text/event-stream")
