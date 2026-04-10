"""系统维护 - 行情数据模块"""

import datetime

from fasthtml.common import *
from starlette.responses import HTMLResponse
from loguru import logger

from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.stocks import stock_list
from quantide.data.models.calendar import calendar


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
        # 如果没有指定任何过滤条件，默认显示第一页（空数据）
        if not code and not start_date and not end_date:
            return [], 0

        # 确定日期范围
        if not start_date:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")

        start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        # 如果没有指定股票代码，返回空
        if not code:
            return [], 0

        # 获取行情数据
        df = daily_bars.get_bars_in_range(
            start=start_dt,
            end=end_dt,
            assets=[code],
            adjust=adjust if adjust != "none" else None,
            eager_mode=True,
        )

        if df is None or len(df) == 0:
            return [], 0

        # 转换为列表
        data = df.to_pandas().to_dict("records")
        total = len(data)

        # 分页
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total)

        page_data = data[start_idx:end_idx]

        return page_data, total

    except Exception as e:
        logger.error(f"获取行情数据失败: {e}")
        return [], 0


def _build_market_table(data: list, code: str = "", start_date: str = "", end_date: str = "", adjust: str = "none", page: int = 1, per_page: int = 20, total: int = 0):
    """构建行情数据表格"""
    # 构建表格行
    rows_html = ""
    if data:
        for row in data:
            # 格式化日期
            trade_date = str(row.get("date", "")).split(" ")[0] if row.get("date") else "-"
            
            # 格式化价格
            open_price = f"{row.get('open', 0):.2f}" if row.get('open') is not None else "-"
            high_price = f"{row.get('high', 0):.2f}" if row.get('high') is not None else "-"
            low_price = f"{row.get('low', 0):.2f}" if row.get('low') is not None else "-"
            close_price = f"{row.get('close', 0):.2f}" if row.get('close') is not None else "-"
            volume = f"{row.get('volume', 0):,.0f}" if row.get('volume') is not None else "-"
            amount = f"{row.get('amount', 0):,.2f}" if row.get('amount') is not None else "-"
            
            # 涨跌停
            up_limit = f"{row.get('up_limit', 0):.2f}" if row.get('up_limit') is not None else "-"
            down_limit = f"{row.get('down_limit', 0):.2f}" if row.get('down_limit') is not None else "-"
            
            # 复权因子
            adjust = f"{row.get('adjust', 1.0):.4f}" if row.get('adjust') is not None else "-"
            
            # ST 标记
            is_st = "是" if row.get('is_st') else "否"
            
            # 计算涨跌 - using open and close as fallback if pre_close not available
            pre_close = row.get('pre_close', row.get('open', 0))  # fallback to open if pre_close not available
            cur_close = row.get('close', 0)
            if pre_close and cur_close and pre_close != 0:
                change = cur_close - pre_close
                change_pct = (change / pre_close) * 100
                change_class = "text-red-500" if change > 0 else ("text-green-500" if change < 0 else "text-gray-500")
                change_str = f'<span class="{change_class}">{change:+.2f} ({change_pct:+.2f}%)</span>'
            else:
                change_str = "-"
            
            rows_html += f'''
            <tr class="hover:bg-gray-50">
                <td class="px-4 py-2 text-sm text-gray-900">{trade_date}</td>
                <td class="px-4 py-2 text-sm text-gray-900">{open_price}</td>
                <td class="px-4 py-2 text-sm text-gray-900">{high_price}</td>
                <td class="px-4 py-2 text-sm text-gray-900">{low_price}</td>
                <td class="px-4 py-2 text-sm text-gray-900">{close_price}</td>
                <td class="px-4 py-2 text-sm text-gray-900">{change_str}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{volume}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{amount}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{up_limit}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{down_limit}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{adjust}</td>
                <td class="px-4 py-2 text-sm text-gray-500">{is_st}</td>
            </tr>
            '''
    
    # 计算分页
    total_pages = max(1, (total + per_page - 1) // per_page) if total > 0 else 1
    page = max(1, min(page, total_pages))
    
    # 构建分页控件
    pagination_html = _build_pagination(page, total_pages, per_page, total, code, start_date, end_date, adjust)
    
    return f"""
    <div class="bg-white rounded-lg shadow overflow-hidden">
        <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
                <tr>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">date</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">open</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">high</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">low</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">close</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">change</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">volume</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">amount</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">up_limit</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">down_limit</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">adjust</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">is_st</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
                {rows_html if rows_html else '<tr><td colspan="12" class="px-4 py-8 text-center text-gray-500">暂无数据</td></tr>'}
            </tbody>
        </table>
        
        {pagination_html}
    </div>
    """


def _build_pagination(page: int, total_pages: int, per_page: int, total: int, code: str, start_date: str, end_date: str, adjust: str):
    """构建分页控件"""
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
    
    # 构建查询参数
    query_params = f"code={code}&start_date={start_date}&end_date={end_date}&adjust={adjust}&per_page={per_page}"
    
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


async def market_page(req, code: str = "", start_date: str = "", end_date: str = "", adjust: str = "none", page: int = 1, per_page: int = 20):
    """行情数据页面"""
    # 从查询参数中获取（兼容 FastHTML 路由）
    code = code or req.query_params.get("code", "")
    start_date = start_date or req.query_params.get("start_date", "")
    end_date = end_date or req.query_params.get("end_date", "")
    adjust = adjust or req.query_params.get("adjust", "none")
    page = int(req.query_params.get("page", page))
    per_page = int(req.query_params.get("per_page", per_page))
    
    logger.info(f"market_page called with code='{code}', start_date='{start_date}', end_date='{end_date}', adjust='{adjust}', page={page}, per_page={per_page}")
    
    # 获取数据
    data, total = _get_market_data(code, start_date, end_date, adjust, page, per_page)
    
    # 构建过滤表单和复权按钮
    filter_form = f'''
    <div class="mb-4">
        <!-- 筛选条件 -->
        <div class="bg-white p-4 rounded-lg shadow mb-3">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">证券代码</label>
                    <input type="text" 
                           id="code-input" 
                           placeholder="输入股票代码" 
                           value="{code}"
                           class="input input-bordered w-full">
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">起始日期</label>
                    <input type="date" 
                           id="start-date-input" 
                           value="{start_date}"
                           class="input input-bordered w-full">
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">结束日期</label>
                    <input type="date" 
                           id="end-date-input" 
                           value="{end_date}"
                           class="input input-bordered w-full">
                </div>
                <div class="flex items-end">
                    <button onclick="applyFilter()" class="btn btn-primary w-full">查询</button>
                </div>
            </div>
        </div>
        
        <!-- 复权方式按钮 -->
        <div class="bg-white p-3 rounded-lg shadow flex items-center space-x-3">
            <span class="text-sm font-medium text-gray-700">复权方式：</span>
            <button onclick="setAdjust('none')" class="btn btn-sm {'btn-primary' if adjust == 'none' else 'btn-outline'}">
                不复权
            </button>
            <button onclick="setAdjust('qfq')" class="btn btn-sm {'btn-primary' if adjust == 'qfq' else 'btn-outline'}">
                前复权
            </button>
            <div class="flex-1"></div>
            <button hx-post="/system/market/sync" 
                    hx-target="#market-container" 
                    hx-swap="innerHTML"
                    class="btn btn-sm btn-secondary">
                立即更新
            </button>
        </div>
    </div>
    '''
    
    # 如果没有数据且没有过滤条件，显示引导信息
    if not data and not code and not start_date and not end_date:
        table_html = f"""
        <div class="bg-white rounded-lg shadow p-8 text-center">
            <div class="text-gray-500 mb-4">请输入查询条件以查看行情数据</div>
            <div class="text-sm text-gray-400">请设置证券代码、日期范围后点击查询按钮</div>
        </div>
        """
    else:
        # 构建行情表格
        table_html = _build_market_table(data, code, start_date, end_date, adjust, page, per_page, total)
    
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>行情数据 - QuantIDE</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/htmx.org@2.0.7/dist/htmx.js"></script>
</head>
<body class="bg-gray-50">
    <div class="container mx-auto p-4 max-w-7xl">
        <h2 class="text-2xl font-bold text-gray-800 mb-4">行情数据</h2>
        
        {filter_form}
        
        <div id="market-container">
            {table_html}
        </div>
    </div>
    
    <script>
        function applyFilter() {{
            const code = document.getElementById('code-input').value;
            const startDate = document.getElementById('start-date-input').value;
            const endDate = document.getElementById('end-date-input').value;
            const adjust = new URLSearchParams(window.location.search).get('adjust') || 'none';
            
            let url = `/system/market?code=${{encodeURIComponent(code)}}&start_date=${{startDate}}&end_date=${{endDate}}&adjust=${{adjust}}&page=1`;
            window.location.href = url;
        }}
        
        function setAdjust(adjustType) {{
            const code = document.getElementById('code-input').value;
            const startDate = document.getElementById('start-date-input').value;
            const endDate = document.getElementById('end-date-input').value;
            
            let url = `/system/market?code=${{encodeURIComponent(code)}}&start_date=${{startDate}}&end_date=${{endDate}}&adjust=${{adjustType}}&page=1`;
            window.location.href = url;
        }}
        
        function goToPage(page) {{
            const code = document.getElementById('code-input').value;
            const startDate = document.getElementById('start-date-input').value;
            const endDate = document.getElementById('end-date-input').value;
            const adjust = new URLSearchParams(window.location.search).get('adjust') || 'none';
            const perPage = document.getElementById('per-page-select').value;
            
            let url = `/system/market?code=${{encodeURIComponent(code)}}&start_date=${{startDate}}&end_date=${{endDate}}&adjust=${{adjust}}&page=${{page}}&per_page=${{perPage}}`;
            window.location.href = url;
        }}
        
        function changePerPage() {{
            const code = document.getElementById('code-input').value;
            const startDate = document.getElementById('start-date-input').value;
            const endDate = document.getElementById('end-date-input').value;
            const adjust = new URLSearchParams(window.location.search).get('adjust') || 'none';
            const perPage = document.getElementById('per-page-select').value;
            
            let url = `/system/market?code=${{encodeURIComponent(code)}}&start_date=${{startDate}}&end_date=${{endDate}}&adjust=${{adjust}}&page=1&per_page=${{perPage}}`;
            window.location.href = url;
        }}
        
        function jumpToPage() {{
            const page = document.getElementById('jump-page').value;
            const code = document.getElementById('code-input').value;
            const startDate = document.getElementById('start-date-input').value;
            const endDate = document.getElementById('end-date-input').value;
            const adjust = new URLSearchParams(window.location.search).get('adjust') || 'none';
            const perPage = document.getElementById('per-page-select').value;
            
            let url = `/system/market?code=${{encodeURIComponent(code)}}&start_date=${{startDate}}&end_date=${{endDate}}&adjust=${{adjust}}&page=${{page}}&per_page=${{perPage}}`;
            window.location.href = url;
        }}
    </script>
</body>
</html>"""
    
    return HTMLResponse(content=html_content, status_code=200)


async def market_sync(req):
    """同步行情数据"""
    try:
        # 实际同步行情数据到最新的交易日
        from quantide.data.models.daily_bars import daily_bars
        daily_bars.store.update()
        logger.info("行情数据同步完成")
        
        # 返回当前页面
        return await market_page(req)
    except Exception as e:
        logger.error(f"同步行情数据失败: {e}")
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
