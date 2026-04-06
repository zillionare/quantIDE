"""系统维护 - 股票列表模块"""

import datetime

import pandas as pd
import polars as pl
from fasthtml.common import *
from starlette.responses import HTMLResponse, JSONResponse
from loguru import logger

from quantide.data.models.stocks import stock_list

# 初始化股票列表
from pathlib import Path
stocks_path = Path('/home/aaron/.config/quantide/data/stock_list.parquet')
if stocks_path.exists():
    try:
        stock_list.load(stocks_path)
        logger.info(f"Stock list loaded: {stock_list.size} records")
    except Exception as e:
        logger.warning(f"Failed to load stock list: {e}")


def _build_stock_table_page(page: int = 1, per_page: int = 20, query: str = ""):
    """构建股票列表表格（支持分页和搜索）"""
    # 获取股票数据
    if query:
        # 模糊查询
        result_df = stock_list.fuzzy_search(query, id_only=False)
        total = len(result_df)
    else:
        # 显示所有股票
        result_df = stock_list.data.to_pandas()
        total = len(result_df)
    
    # 计算分页
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start_idx = (page - 1) * per_page
    end_idx = min(start_idx + per_page, total)
    
    page_data = result_df.iloc[start_idx:end_idx] if total > 0 else None
    
    # 构建表格行
    rows_html = ""
    if page_data is not None and len(page_data) > 0:
        for _, row in page_data.iterrows():
            # 格式化日期（只保留日期部分）
            list_date_str = str(row["list_date"]).split(" ")[0] if row["list_date"] else ""
            delist_date_str = str(row["delist_date"]).split(" ")[0] if row.get("delist_date") and str(row["delist_date"]) != "NaT" else ""
            
            delist_info = f' <span class="text-xs text-red-400">(退市: {delist_date_str})</span>' if delist_date_str else ""
            rows_html += f'''
            <tr class="hover:bg-gray-50">
                <td class="px-4 py-2 text-sm text-gray-900">{row["asset"]}</td>
                <td class="px-4 py-2 text-sm text-gray-900">{row["name"]}{delist_info}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{row.get("pinyin", "")}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{list_date_str}</td>
            </tr>
            '''
    
    # 构建分页控件
    pagination_html = _build_pagination(page, total_pages, per_page, total, query)
    
    return f"""
    <div class="bg-white rounded-lg shadow overflow-hidden">
        <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
                <tr>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">代码</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">名称</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">拼音</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">上市日期</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
                {rows_html if rows_html else '<tr><td colspan="4" class="px-4 py-8 text-center text-gray-500">暂无数据</td></tr>'}
            </tbody>
        </table>
        
        {pagination_html}
    </div>
    """


def _build_pagination(page: int, total_pages: int, per_page: int, total: int, query: str):
    """构建分页控件（最多7个页码）"""
    # 生成页码列表（最多7个）
    start_page = max(1, page - 3)
    end_page = min(total_pages, start_page + 6)
    if end_page - start_page < 6:
        start_page = max(1, end_page - 6)
    
    page_numbers = range(start_page, end_page + 1)
    
    page_links = ""
    for p in page_numbers:
        active_class = "btn btn-sm btn-primary" if p == page else "btn btn-sm btn-outline"
        page_links += f'<button onclick="goToPage({p})" class="{active_class} mx-1">{p}</button>'
    
    return f"""
    <div class="px-4 py-3 bg-gray-50 border-t border-gray-200 flex items-center justify-between">
        <div class="text-sm text-gray-700">
            {'暂无数据' if total == 0 else f'显示第 {(page - 1) * per_page + 1} 到 {min(page * per_page, total)} 条，共 {total} 条'}
        </div>
        <div class="flex items-center space-x-2">
            <select id="per-page-select" onchange="changePerPage()" class="select select-bordered select-sm">
                <option value="20" {"selected" if per_page == 20 else ""}>20条/页</option>
                <option value="50" {"selected" if per_page == 50 else ""}>50条/页</option>
                <option value="100" {"selected" if per_page == 100 else ""}>100条/页</option>
            </select>
            <div class="flex items-center ml-4">
                {page_links}
            </div>
            <div class="flex items-center ml-4">
                <span class="text-sm text-gray-600 mr-2">跳转到</span>
                <input type="number" id="jump-page" min="1" max="{total_pages}" value="{page}" 
                       class="input input-bordered input-sm w-16" 
                       onkeypress="if(event.key==='Enter') jumpToPage()">
                <button onclick="jumpToPage()" class="btn btn-sm btn-outline ml-2">GO</button>
            </div>
        </div>
    </div>
    """


async def stocks_page(req, page: int = 1, per_page: int = 20, q: str = ""):
    """股票列表页面"""
    # 从查询参数中获取（兼容 FastHTML 路由）
    query = q or req.query_params.get("q", "")
    page = int(req.query_params.get("page", page))
    per_page = int(req.query_params.get("per_page", per_page))
    logger.info(f"stocks_page called with query='{query}', page={page}, per_page={per_page}")
    
    # 构建搜索框和更新按钮
    search_box = f'''
    <div class="mb-4">
        <div class="flex justify-between items-center">
            <div class="flex-1 max-w-md">
                <input type="text" 
                       id="stock-search" 
                       placeholder="搜索股票代码、名称或拼音..." 
                       value="{query}"
                       class="input input-bordered w-full"
                       oninput="debouncedSearch(this.value)">
            </div>
            <button hx-post="/system/stocks/sync" 
                    hx-target="#stocks-container" 
                    hx-swap="innerHTML"
                    class="btn btn-sm btn-secondary ml-4">
                立即更新
            </button>
        </div>
    </div>
    '''
    
    # 构建股票表格
    table_html = _build_stock_table_page(page, per_page, query)
    
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>股票列表 - QuantIDE</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/htmx.org@2.0.7/dist/htmx.js"></script>
</head>
<body class="bg-gray-50">
    <div class="container mx-auto p-4 max-w-6xl">
        <h2 class="text-2xl font-bold text-gray-800 mb-4">股票列表</h2>
        
        {search_box}
        
        <div id="stocks-container">
            {table_html}
        </div>
    </div>
    
    <script>
        let searchTimeout;
        function debouncedSearch(query) {{
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {{
                window.location.href = `/system/stocks?q=${{encodeURIComponent(query)}}&page=1`;
            }}, 250);
        }}
        
        function goToPage(page) {{
            const query = document.getElementById('stock-search').value;
            const perPage = document.getElementById('per-page-select').value;
            window.location.href = `/system/stocks?q=${{encodeURIComponent(query)}}&page=${{page}}&per_page=${{perPage}}`;
        }}
        
        function changePerPage() {{
            const query = document.getElementById('stock-search').value;
            const perPage = document.getElementById('per-page-select').value;
            window.location.href = `/system/stocks?q=${{encodeURIComponent(query)}}&page=1&per_page=${{perPage}}`;
        }}
        
        function jumpToPage() {{
            const page = document.getElementById('jump-page').value;
            const query = document.getElementById('stock-search').value;
            const perPage = document.getElementById('per-page-select').value;
            window.location.href = `/system/stocks?q=${{encodeURIComponent(query)}}&page=${{page}}&per_page=${{perPage}}`;
        }}
    </script>
</body>
</html>"""
    
    return HTMLResponse(content=html_content, status_code=200)


async def stocks_sync(req):
    """同步股票列表数据"""
    try:
        stock_list.update()
        logger.info(f"Stock list synced: {stock_list.size} records")
        
        # 返回更新后的页面
        return await stocks_page(req)
    except Exception as e:
        logger.error(f"同步股票列表失败: {e}")
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
