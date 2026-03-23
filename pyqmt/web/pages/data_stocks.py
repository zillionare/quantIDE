"""股票列表管理页面"""

import asyncio
import datetime
import json
from fasthtml.common import *
from monsterui.all import *
from starlette.responses import StreamingResponse
from pyqmt.core.message import msg_hub
from pyqmt.data.models.stocks import stock_list
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.services import StockSyncService
from pyqmt.web.layouts.main import MainLayout
from pyqmt.web.theme import AppTheme, PRIMARY_COLOR
from loguru import logger

# 定义子路由应用
data_stocks_app, rt = fast_app(hdrs=AppTheme.headers())

# ========== 全局同步状态 ==========
_sync_status = {
    "is_running": False,
    "progress": 0,
    "message": "",
    "completed": False,
    "error": None,
}

def _get_active_tab(req):
    return req.query_params.get("tab", "overview")

def _TabNav(active_tab: str):
    tabs = [
        ("overview", "概要"),
        ("search", "查询"),
        ("update", "手动更新"),
    ]
    
    tab_items = []
    for tab_id, label in tabs:
        is_active = active_tab == tab_id
        base_cls = "px-4 py-2 font-medium transition-colors duration-200"
        if is_active:
            cls = f"{base_cls} text-red-600 border-b-2 border-red-600"
        else:
            cls = f"{base_cls} text-gray-500 hover:text-gray-700 hover:border-b-2 hover:border-gray-300"
        
        tab_items.append(A(label, href=f"/data/stocks?tab={tab_id}", cls=cls))
        
    return Div(
        Div(*tab_items, cls="flex space-x-2"),
        cls="border-b border-gray-200 mb-6"
    )

def _OverviewTab():
    """概要 Tab 内容"""
    try:
        total_stocks = stock_list.size
        last_update = stock_list.last_update_time
        file_path = str(stock_list.path)
    except Exception as e:
        return Div(Card(CardBody(P(f"获取股票列表信息失败: {e}", cls="text-red-500"))))

    return Div(
        H3("股票列表概要", cls="text-xl font-semibold mb-6"),
        Div(
            P(Span("共有股票：", cls="font-medium"), f"{total_stocks} 只"),
            P(Span("最后更新：", cls="font-medium"), str(last_update or "从未更新")),
            P(Span("文件位置：", cls="font-medium"), file_path, cls="text-sm text-gray-600 break-all"),
            cls="space-y-4"
        ),
        cls="animate-in fade-in duration-500"
    )

def _SearchTab(req):
    """查询 Tab 内容"""
    q = req.query_params.get("q", "").strip()
    
    table_content = P("输入关键词开始搜索 (支持代码、名称、拼音)...", cls="text-gray-400 text-center py-12")
    
    if q:
        try:
            # 使用 fuzzy_search 获取结果
            df = stock_list.fuzzy_search(q, id_only=False)
            if not df.empty:
                headers = ["股票代码", "公司名称", "拼音", "上市日期", "退市日期"]
                header_row = Tr(*[Th(h) for h in headers])
                
                rows = []
                # 限制显示前 100 条
                for _, row in df.head(100).iterrows():
                    rows.append(Tr(
                        Td(row["asset"], cls="font-mono"),
                        Td(row["name"]),
                        Td(row["pinyin"]),
                        Td(str(row["list_date"])),
                        Td(str(row["delist_date"]) if row["delist_date"] else "None"),
                    ))
                table_content = Table(Thead(header_row), Tbody(*rows), cls="uk-table uk-table-divider uk-table-small text-sm")
            else:
                table_content = P(f"未找到与 '{q}' 匹配的股票", cls="text-red-500 text-center py-12")
        except Exception as e:
            logger.error(f"查询股票失败: {e}")
            table_content = P(f"查询出错: {e}", cls="text-red-500 text-center py-12")

    return Div(
        H3("股票查询", cls="text-xl font-semibold mb-4"),
        Form(
            Div(
                Input(name="q", value=q, placeholder="输入关键词，停顿后自动搜索...", 
                      cls="input flex-1", 
                      hx_get="/data/stocks?tab=search", 
                      hx_trigger="keyup changed delay:500ms", 
                      hx_target="#search-results-container",
                      hx_select="#search-results-container"),
                Button(UkIcon("search"), type="submit", cls="btn btn-primary"),
                cls="flex gap-2 mb-6"
            ),
            method="GET",
            action="/data/stocks"
        ),
        Input(type="hidden", name="tab", value="search"),
        Div(table_content, id="search-results-container", cls="overflow-x-auto bg-white border rounded-lg"),
        cls="animate-in fade-in duration-500"
    )

def _UpdateTab():
    """手动更新 Tab 内容"""
    return Div(
        H3("手动更新股票列表", cls="text-xl font-semibold mb-4"),
        P("从 Tushare 同步最新的全市场股票基础信息。", cls="text-gray-600 mb-6"),
        Form(
            Button(
                Div(UkIcon("refresh-cw", cls="mr-2"), "立即更新", cls="flex items-center"),
                type="submit",
                cls="btn btn-primary",
                hx_post="/data/stocks/do-update",
                hx_target="#update-dialog-container",
                hx_swap="innerHTML"
            ),
        ),
        Div(id="update-dialog-container"),
        cls="animate-in fade-in duration-500"
    )

@rt("/")
async def index(req):
    active_tab = _get_active_tab(req)
    
    if active_tab == "search":
        content = _SearchTab(req)
    elif active_tab == "update":
        content = _UpdateTab()
    else:
        content = _OverviewTab()
        
    layout = MainLayout()
    layout.set_sidebar_active("/data/stocks")
    
    page_content = Div(
        Div(
            Div(
                UkIcon("list", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("股票列表", cls="text-2xl font-bold"),
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

# ========== SSE 更新逻辑 ==========

async def _run_stocks_sync():
    global _sync_status
    _sync_status["is_running"] = True
    _sync_status["completed"] = False
    _sync_status["error"] = None
    _sync_status["progress"] = 0
    _sync_status["message"] = "正在获取股票列表..."

    try:
        # stock_list.update 是同步方法
        await asyncio.to_thread(stock_list.update)
        _sync_status["progress"] = 100
        _sync_status["message"] = "同步完成"
        _sync_status["completed"] = True
    except Exception as e:
        logger.error(f"同步股票列表失败: {e}")
        _sync_status["error"] = str(e)
    finally:
        _sync_status["is_running"] = False

@rt("/do-update", methods="post")
async def do_update():
    asyncio.create_task(_run_stocks_sync())
    
    return Div(
        Div(
            Div(
                H3("股票列表更新进度", cls="text-lg font-bold mb-4"),
                Div(
                    Div(id="stocks-progress-bar", cls="bg-red-600 h-2 rounded-full", style="width: 0%"),
                    cls="w-full bg-gray-200 rounded-full h-2 mb-2"
                ),
                P(id="stocks-status", children=["准备中..."], cls="text-sm text-gray-600"),
                Div(
                    Button("关闭", id="stocks-close-btn", cls="btn btn-ghost mt-6", disabled=True, onclick="this.closest('.fixed').remove()"),
                    cls="flex justify-end"
                ),
                cls="bg-white p-6 rounded-xl shadow-xl max-w-md w-full"
            ),
            cls="fixed inset-0 bg-black/50 flex items-center justify-center z-50",
        ),
        Script("""
            (function() {
                const bar = document.getElementById('stocks-progress-bar');
                const status = document.getElementById('stocks-status');
                const btn = document.getElementById('stocks-close-btn');
                const es = new EventSource('/data/stocks/sync-progress');
                es.onmessage = function(e) {
                    const data = JSON.parse(e.data);
                    bar.style.width = data.progress + '%';
                    status.textContent = data.message;
                    if (data.completed || data.error) {
                        if (data.error) status.textContent = '错误: ' + data.error;
                        btn.disabled = false;
                        es.close();
                    }
                };
            })();
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
