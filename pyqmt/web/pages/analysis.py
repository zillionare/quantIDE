"""分析页面 - 新设计（v0.4）"""

import datetime

from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import HTMLResponse

from monsterui.all import Theme

from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.sqlite import db
from pyqmt.web.components.header import header_component


def get_sector_dal() -> SectorDAL:
    """获取 SectorDAL 实例"""
    return SectorDAL(db)


def analysis_page(request: Request):
    """分析页面 - 新设计（无sidebar）"""
    session = request.scope.get("session", {})
    user = session.get("auth")

    # 获取板块列表
    dal = get_sector_dal()
    sectors = dal.list_sectors()
    sectors_data = [
        {
            "id": s.id,
            "name": s.name,
            "sector_type": s.sector_type,
            "source": s.source,
        }
        for s in sectors
    ]

    # 默认选中第一个板块
    selected_sector_id = sectors_data[0]["id"] if sectors_data else None
    selected_sector_name = sectors_data[0]["name"] if sectors_data else ""

    # 获取选中板块的成分股
    stocks_data = []
    if selected_sector_id:
        stocks = dal.get_sector_stocks(selected_sector_id)
        stocks_data = [
            {"symbol": s.symbol, "name": s.name}
            for s in stocks
        ]

    # 默认股票代码
    default_symbol = stocks_data[0]["symbol"] if stocks_data else "000001.SZ"
    default_name = stocks_data[0]["name"] if stocks_data else "平安银行"

    # Header 导航项
    header_menu = [
        ("首页", "/"),
        ("交易", "/trade"),
        ("行情", "/system/stocks"),
        ("策略", "/strategy"),
        ("分析", "/analysis"),
    ]

    # 左侧 Sidebar 内容
    left_sidebar = Div(
        # 第一行：板块选择 Select
        Div(
            Label("板块选择", cls="block text-xs text-gray-500 mb-1"),
            Select(
                *[
                    Option(
                        f"{s['name']} -- {s['sector_type']}",
                        value=s["id"],
                        selected=(s["id"] == selected_sector_id),
                    )
                    for s in sectors_data
                ],
                Option("──────────", disabled=True),
                Option("+ 新建板块", value="__new__", cls="text-blue-600"),
                cls="w-full p-2 border rounded text-sm",
                id="sector-select",
                onchange="onSectorChange(this.value)",
            ),
            cls="p-3 border-b bg-gray-50",
        ),
        # 第二行：板块个股列表
        Div(
            Div(
                f"板块个股 ({selected_sector_name})",
                cls="text-sm font-medium text-gray-700 mb-2",
            ),
            Div(
                *[
                    Div(
                        Div(
                            "▶" if i == 0 else "",
                            cls=f"text-xs w-4 {'text-blue-600' if i == 0 else 'text-transparent'}",
                        ),
                        Div(
                            Div(s["name"], cls="font-medium text-gray-900"),
                            Div(s["symbol"], cls="text-xs text-gray-500"),
                        ),
                        cls=f"flex items-center gap-2 p-2 cursor-pointer hover:bg-gray-50 {'bg-blue-50 border-l-3 border-blue-600' if i == 0 else ''}",
                        data_symbol=s["symbol"],
                        data_name=s["name"],
                        onclick=f"onStockClick('{s['symbol']}', '{s['name']}')",
                    )
                    for i, s in enumerate(stocks_data[:20])
                ],
                id="stock-list",
                cls="divide-y",
            ),
            cls="flex-1 overflow-y-auto p-3",
        ),
        # 第三行：所属板块
        Div(
            Label("所属板块", cls="block text-xs text-gray-500 mb-2"),
            Div(
                id="stock-sectors",
                cls="space-y-1",
            ),
            cls="p-3 border-t bg-gray-50",
            id="stock-sectors-container",
        ),
        cls="w-72 bg-white border-r flex flex-col flex-shrink-0 h-full",
    )

    # 右侧 Main Content
    right_content = Div(
        # 工具条
        Div(
            # 类型切换
            Select(
                Option("个股", value="stock"),
                Option("指数", value="index"),
                cls="p-2 border rounded text-sm",
                id="search-type",
            ),
            # 搜索输入框
            Div(
                Input(
                    type="text",
                    placeholder="代码/名称",
                    cls="w-32 p-2 border rounded text-sm",
                    maxlength="8",
                    id="search-input",
                    oninput="onSearchInput(this.value)",
                ),
                # 搜索建议下拉
                Div(
                    id="search-suggestions",
                    cls="absolute top-full left-0 mt-1 w-48 bg-white border rounded shadow-lg hidden z-50",
                ),
                cls="relative",
            ),
            # 周期按钮组
            Div(
                Button(
                    "日线",
                    cls="px-3 py-1.5 text-sm rounded bg-blue-600 text-white",
                    data_freq="day",
                    onclick="onFreqChange('day')",
                ),
                Button(
                    "周线",
                    cls="px-3 py-1.5 text-sm rounded bg-gray-200 text-gray-700 hover:bg-gray-300",
                    data_freq="week",
                    onclick="onFreqChange('week')",
                ),
                Button(
                    "月线",
                    cls="px-3 py-1.5 text-sm rounded bg-gray-200 text-gray-700 hover:bg-gray-300",
                    data_freq="month",
                    onclick="onFreqChange('month')",
                ),
                Button(
                    "多周期",
                    cls="px-3 py-1.5 text-sm rounded bg-gray-200 text-gray-700 hover:bg-gray-300",
                    onclick="onMultiFreqView()",
                ),
                cls="flex gap-1 ml-auto",
            ),
            cls="bg-white border-b p-3 flex items-center gap-3 flex-shrink-0",
        ),
        # K线图列表区域
        Div(
            id="kline-list",
            cls="flex-1 overflow-y-auto p-4 space-y-4",
        ),
        cls="flex-1 flex flex-col bg-gray-100 overflow-hidden h-full",
    )

    # 主布局
    main_layout = Div(
        left_sidebar,
        right_content,
        cls="flex flex-1 overflow-hidden",
    )

    # 完整页面内容
    page_content = Div(
        # Header
        header_component(
            logo="/static/logo.png",
            brand="匡醍",
            nav_items=header_menu,
            user=user,
            accounts=[],
            active_account=None,
            active_title="分析",
        ),
        # 主内容区
        main_layout,
        # JavaScript 交互逻辑
        Script(get_analysis_js(sectors_data, stocks_data, default_symbol, default_name)),
        # Lightweight Charts CDN
        Script(src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"),
        cls="h-screen flex flex-col",
    )

    # 构建完整HTML响应，包含必要的CSS
    html_content = to_xml((
        Title("分析"),
        *Theme.blue.headers(),
        page_content,
    ))

    return HTMLResponse(html_content)


def get_analysis_js(sectors_data, stocks_data, default_symbol, default_name):
    """生成分析页面的 JavaScript 代码"""
    sectors_json = str(sectors_data).replace("'", '"')
    stocks_json = str(stocks_data).replace("'", '"')

    return f"""
    // 全局状态
    const state = {{
        sectors: {sectors_json},
        stocks: {stocks_json},
        selectedSectorId: '{sectors_data[0]["id"] if sectors_data else ""}',
        selectedStockSymbol: '{default_symbol}',
        selectedStockName: '{default_name}',
        freq: 'day',
        klineData: {{}},
        chartInstances: {{}},
        page: 1,
        pageSize: 5,
        hasMore: true,
        loading: false,
        multiFreqMode: false,
        multiFreqStock: null,
    }};

    // 初始化
    document.addEventListener('DOMContentLoaded', function() {{
        loadKlineList();
        loadStockSectors(state.selectedStockSymbol);
        setupInfiniteScroll();
    }});

    // 板块切换
    async function onSectorChange(sectorId) {{
        if (sectorId === '__new__') {{
            showCreateSectorModal();
            return;
        }}

        state.selectedSectorId = sectorId;
        state.page = 1;
        state.hasMore = true;
        state.klineData = {{}};

        await updateStockList(sectorId);

        document.getElementById('kline-list').innerHTML = '';
        loadKlineList();
    }}

    // 更新个股列表
    async function updateStockList(sectorId) {{
        try {{
            const response = await fetch(`/api/v1/sectors/${{sectorId}}/stocks`);
            const result = await response.json();

            if (result.code === 0) {{
                state.stocks = result.data;

                const container = document.getElementById('stock-list');
                const sectorName = state.sectors.find(s => s.id === sectorId)?.name || '';

                let html = `<div class="text-sm font-medium text-gray-700 mb-2">板块个股 (${{sectorName}})</div>`;

                result.data.forEach((stock, i) => {{
                    const isSelected = i === 0;
                    html += `
                        <div class="flex items-center gap-2 p-2 cursor-pointer hover:bg-gray-50 ${{isSelected ? 'bg-blue-50 border-l-3 border-blue-600' : ''}}"
                             data-symbol="${{stock.symbol}}"
                             data-name="${{stock.name}}"
                             onclick="onStockClick('${{stock.symbol}}', '${{stock.name}}')">
                            <div class="text-xs w-4 ${{isSelected ? 'text-blue-600' : 'text-transparent'}}">▶</div>
                            <div>
                                <div class="font-medium text-gray-900">${{stock.name}}</div>
                                <div class="text-xs text-gray-500">${{stock.symbol}}</div>
                            </div>
                        </div>
                    `;
                }});

                container.innerHTML = html;

                if (result.data.length > 0) {{
                    state.selectedStockSymbol = result.data[0].symbol;
                    state.selectedStockName = result.data[0].name;
                    loadStockSectors(state.selectedStockSymbol);
                }}
            }}
        }} catch (error) {{
            console.error('Error loading stocks:', error);
        }}
    }}

    // 个股点击
    function onStockClick(symbol, name) {{
        state.selectedStockSymbol = symbol;
        state.selectedStockName = name;

        document.querySelectorAll('#stock-list > div[data-symbol]').forEach(el => {{
            const isSelected = el.dataset.symbol === symbol;
            el.classList.toggle('bg-blue-50', isSelected);
            el.classList.toggle('border-l-3', isSelected);
            el.classList.toggle('border-blue-600', isSelected);
            el.querySelector('.text-xs.w-4').classList.toggle('text-blue-600', isSelected);
            el.querySelector('.text-xs.w-4').classList.toggle('text-transparent', !isSelected);
            el.querySelector('.text-xs.w-4').textContent = isSelected ? '▶' : '';
        }});

        highlightKlineCard(symbol);
        loadStockSectors(symbol);
    }}

    // 高亮K线卡片
    function highlightKlineCard(symbol) {{
        document.querySelectorAll('.kline-card').forEach(card => {{
            const isHighlighted = card.dataset.symbol === symbol;
            card.classList.toggle('highlighted-card', isHighlighted);
        }});

        const card = document.querySelector(`.kline-card[data-symbol="${{symbol}}"]`);
        if (card) {{
            card.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
        }}
    }}

    // 加载个股所属板块
    async function loadStockSectors(symbol) {{
        try {{
            const response = await fetch(`/api/v1/sectors/stock/${{symbol}}`);
            const result = await response.json();

            const container = document.getElementById('stock-sectors');

            if (result.code === 0 && result.data.length > 0) {{
                let html = '';
                result.data.forEach(sector => {{
                    html += `
                        <div class="text-sm text-blue-600 cursor-pointer hover:underline flex items-center gap-1"
                             onclick="onSectorClickFromStock('${{sector.id}}', '${{symbol}}')">
                            <span>•</span> ${{sector.name}}
                        </div>
                    `;
                }});
                container.innerHTML = html;
            }} else {{
                container.innerHTML = '<div class="text-sm text-gray-400">暂无板块数据</div>';
            }}
        }} catch (error) {{
            console.error('Error loading stock sectors:', error);
        }}
    }}

    // 从所属板块点击切换到板块
    async function onSectorClickFromStock(sectorId, stockSymbol) {{
        document.getElementById('sector-select').value = sectorId;
        await onSectorChange(sectorId);
        setTimeout(() => {{
            onStockClick(stockSymbol, '');
        }}, 100);
    }}

    // 周期切换
    function onFreqChange(freq) {{
        state.freq = freq;
        state.klineData = {{}};
        state.multiFreqMode = false;
        state.multiFreqStock = null;

        document.querySelectorAll('[data-freq]').forEach(btn => {{
            const isActive = btn.dataset.freq === freq;
            btn.className = isActive
                ? 'px-3 py-1.5 text-sm rounded bg-blue-600 text-white'
                : 'px-3 py-1.5 text-sm rounded bg-gray-200 text-gray-700 hover:bg-gray-300';
        }});

        document.getElementById('kline-list').innerHTML = '';
        state.page = 1;
        state.hasMore = true;
        loadKlineList();
    }}

    // 多周期视图 - 显示当前选中个股的日/周/月线
    // 布局：左侧日线（半届），右侧上周线，右侧下月线
    async function onMultiFreqView() {{
        if (!state.selectedStockSymbol) {{
            alert('请先选择一只个股');
            return;
        }}

        state.multiFreqMode = true;
        state.multiFreqStock = state.selectedStockSymbol;

        // 更新按钮样式
        document.querySelectorAll('[data-freq]').forEach(btn => {{
            btn.className = 'px-3 py-1.5 text-sm rounded bg-gray-200 text-gray-700 hover:bg-gray-300';
        }});

        const container = document.getElementById('kline-list');
        container.innerHTML = '';

        // 获取当前选中个股的信息
        const stock = state.stocks.find(s => s.symbol === state.selectedStockSymbol);
        if (!stock) return;

        // 创建三栏布局容器
        const multiFreqContainer = document.createElement('div');
        multiFreqContainer.style.cssText = 'display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: 1fr 1fr; gap: 16px; height: calc(100vh - 200px);';

        // 创建日线卡片（左侧，占两行）
        const dayCard = createMultiFreqCard(stock, 'day', '日线');
        dayCard.style.cssText = 'grid-row: 1 / 3; grid-column: 1;';
        multiFreqContainer.appendChild(dayCard);

        // 创建周线卡片（右上）
        const weekCard = createMultiFreqCard(stock, 'week', '周线');
        weekCard.style.cssText = 'grid-row: 1; grid-column: 2;';
        multiFreqContainer.appendChild(weekCard);

        // 创建月线卡片（右下）
        const monthCard = createMultiFreqCard(stock, 'month', '月线');
        monthCard.style.cssText = 'grid-row: 2; grid-column: 2;';
        multiFreqContainer.appendChild(monthCard);

        container.appendChild(multiFreqContainer);

        // 加载数据
        await loadKlineDataForFreq(stock.symbol, stock.name, dayCard, 'day');
        await loadKlineDataForFreq(stock.symbol, stock.name, weekCard, 'week');
        await loadKlineDataForFreq(stock.symbol, stock.name, monthCard, 'month');
    }}

    // 创建多周期卡片
    function createMultiFreqCard(stock, freq, freqLabel) {{
        const div = document.createElement('div');
        div.className = 'kline-card bg-white rounded-lg shadow overflow-hidden';
        div.dataset.symbol = stock.symbol;
        div.dataset.freq = freq;
        div.style.height = '100%';
        div.innerHTML = `
            <div class="p-3 border-b flex items-center justify-between" style="height: 50px;">
                <div class="flex items-center gap-2">
                    <span class="font-semibold text-gray-900">${{stock.name}}</span>
                    <span class="text-xs text-gray-500">${{stock.symbol}}</span>
                    <span class="px-2 py-0.5 text-xs rounded bg-blue-100 text-blue-700">${{freqLabel}}</span>
                </div>
                <div class="text-xs font-mono ma-info" data-symbol="${{stock.symbol}}">
                    <span class="text-gray-400">加载中...</span>
                </div>
            </div>
            <div class="kline-chart" style="height: calc(100% - 50px);"></div>
        `;
        return div;
    }}

    // 加载指定周期的K线数据
    async function loadKlineDataForFreq(symbol, name, card, freq) {{
        const end = '2024-12-31';
        const start = '2024-01-01';

        const url = `/api/v1/kline/stock/${{symbol}}?start=${{start}}&end=${{end}}&freq=${{freq}}&ma=5,10,20,60`;

        try {{
            const response = await fetch(url);
            const result = await response.json();

            if (result.code === 0) {{
                const items = result.data.items;
                if (items.length > 0) {{
                    const latest = items[items.length - 1];
                    const maColors = {{ 5: '#ff6d00', 10: '#2962ff', 20: '#00c853', 60: '#aa00ff' }};

                    // 更新卡片头部的均线信息
                    const maInfoEl = card.querySelector('.ma-info');
                    if (maInfoEl) {{
                        const maValues = [5, 10, 20, 60]
                            .filter(p => latest[`ma${{p}}`] !== null && latest[`ma${{p}}`] !== undefined)
                            .map(p => `<span style="color: ${{maColors[p]}}">MA${{p}}: ${{latest[`ma${{p}}`].toFixed(2)}}</span>`)
                            .join(' ');
                        maInfoEl.innerHTML = maValues;
                    }}
                }}

                renderChart(card.querySelector('.kline-chart'), items);
            }}
        }} catch (error) {{
            console.error('Error loading kline data:', error);
            const maInfoEl = card.querySelector('.ma-info');
            if (maInfoEl) {{
                maInfoEl.innerHTML = '<span class="text-red-500">加载失败</span>';
            }}
        }}
    }}

    // 加载K线列表
    async function loadKlineList() {{
        // 多周期模式下不加载列表
        if (state.multiFreqMode) return;

        if (state.loading || !state.hasMore) return;

        state.loading = true;

        const start = (state.page - 1) * state.pageSize;
        const end = start + state.pageSize;
        const stocksToLoad = state.stocks.slice(start, end);

        if (stocksToLoad.length === 0) {{
            state.hasMore = false;
            state.loading = false;
            return;
        }}

        const container = document.getElementById('kline-list');

        for (const stock of stocksToLoad) {{
            const card = createKlineCard(stock);
            container.appendChild(card);
            await loadKlineData(stock.symbol, stock.name, card);
        }}

        state.page++;
        state.loading = false;

        if (state.selectedStockSymbol) {{
            highlightKlineCard(state.selectedStockSymbol);
        }}
    }}

    // 创建K线卡片
    function createKlineCard(stock) {{
        const div = document.createElement('div');
        div.className = 'kline-card bg-white rounded-lg shadow overflow-hidden';
        div.dataset.symbol = stock.symbol;
        div.innerHTML = `
            <div class="p-3 border-b flex items-center justify-between">
                <div class="flex items-center gap-2">
                    <span class="font-semibold text-gray-900">${{stock.name}}</span>
                    <span class="text-xs text-gray-500">${{stock.symbol}}</span>
                </div>
                <div class="text-xs font-mono ma-info" data-symbol="${{stock.symbol}}">
                    <span class="text-gray-400">加载中...</span>
                </div>
            </div>
            <div class="kline-chart" style="height: 300px;"></div>
        `;
        return div;
    }}

    // 加载单只股票的K线数据
    async function loadKlineData(symbol, name, card) {{
        const end = '2024-12-31';
        const start = '2024-01-01';
        const freq = state.freq;

        const url = `/api/v1/kline/stock/${{symbol}}?start=${{start}}&end=${{end}}&freq=${{freq}}&ma=5,10,20,60`;

        try {{
            const response = await fetch(url);
            const result = await response.json();

            if (result.code === 0) {{
                const items = result.data.items;
                if (items.length > 0) {{
                    const latest = items[items.length - 1];
                    const maColors = {{ 5: '#ff6d00', 10: '#2962ff', 20: '#00c853', 60: '#aa00ff' }};

                    // 更新卡片头部的均线信息
                    const maInfoEl = card.querySelector('.ma-info');
                    if (maInfoEl) {{
                        const maValues = [5, 10, 20, 60]
                            .filter(p => latest[`ma${{p}}`] !== null && latest[`ma${{p}}`] !== undefined)
                            .map(p => `<span style="color: ${{maColors[p]}}">MA${{p}}: ${{latest[`ma${{p}}`].toFixed(2)}}</span>`)
                            .join(' ');
                        maInfoEl.innerHTML = maValues;
                    }}
                }}

                renderChart(card.querySelector('.kline-chart'), items);
            }}
        }} catch (error) {{
            console.error('Error loading kline data:', error);
            const maInfoEl = card.querySelector('.ma-info');
            if (maInfoEl) {{
                maInfoEl.innerHTML = '<span class="text-red-500">加载失败</span>';
            }}
        }}
    }}

    // 渲染图表
    function renderChart(container, data) {{
        if (!container || data.length === 0) return;

        // 创建图表容器结构
        const chartWrapper = document.createElement('div');
        chartWrapper.style.position = 'relative';
        chartWrapper.style.height = '100%';
        chartWrapper.style.width = '100%';
        container.innerHTML = '';
        container.appendChild(chartWrapper);

        // 创建指标信息显示区域
        const infoPanel = document.createElement('div');
        infoPanel.style.cssText = 'position: absolute; top: 5px; left: 10px; z-index: 10; font-size: 11px; font-family: monospace; background: rgba(255,255,255,0.9); padding: 5px; border-radius: 3px; pointer-events: none;';
        chartWrapper.appendChild(infoPanel);

        // 创建K线图容器
        const klineContainer = document.createElement('div');
        klineContainer.style.height = '70%';
        klineContainer.style.width = '100%';
        chartWrapper.appendChild(klineContainer);

        // 创建成交量图容器
        const volumeContainer = document.createElement('div');
        volumeContainer.style.height = '30%';
        volumeContainer.style.width = '100%';
        chartWrapper.appendChild(volumeContainer);

        // 格式化时间为数字格式 YYYY-MM-DD
        function formatTime(dt) {{
            if (!dt) return '';
            const date = new Date(dt);
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${{year}}-${{month}}-${{day}}`;
        }}

        // 创建K线图
        const chart = LightweightCharts.createChart(klineContainer, {{
            width: klineContainer.clientWidth,
            height: klineContainer.clientHeight,
            layout: {{
                background: {{ color: '#ffffff' }},
                textColor: '#333',
            }},
            grid: {{
                vertLines: {{ color: '#e0e0e0' }},
                horzLines: {{ visible: false }},
            }},
            crosshair: {{
                mode: LightweightCharts.CrosshairMode.Normal,
            }},
            rightPriceScale: {{
                borderColor: '#e0e0e0',
                scaleMargins: {{
                    top: 0.1,
                    bottom: 0.1,
                }},
            }},
            timeScale: {{
                borderColor: '#e0e0e0',
                timeVisible: false,
                tickMarkFormatter: (time) => {{
                    return formatTime(time);
                }},
            }},
            handleScale: {{
                axisPressedMouseMove: {{
                    time: true,
                    price: false,
                }},
            }},
            handleScroll: {{
                horzTouchDrag: true,
                vertTouchDrag: false,
                mouseWheel: false,
                pressedMouseMove: true,
            }},
        }});

        // 创建成交量图
        const volumeChart = LightweightCharts.createChart(volumeContainer, {{
            width: volumeContainer.clientWidth,
            height: volumeContainer.clientHeight,
            layout: {{
                background: {{ color: '#ffffff' }},
                textColor: '#333',
            }},
            grid: {{
                vertLines: {{ color: '#e0e0e0' }},
                horzLines: {{ visible: false }},
            }},
            crosshair: {{
                mode: LightweightCharts.CrosshairMode.Normal,
            }},
            rightPriceScale: {{
                borderColor: '#e0e0e0',
                scaleMargins: {{
                    top: 0.1,
                    bottom: 0.1,
                }},
            }},
            timeScale: {{
                visible: false,
            }},
            handleScale: false,
            handleScroll: {{
                horzTouchDrag: true,
                vertTouchDrag: false,
                mouseWheel: false,
                pressedMouseMove: true,
            }},
        }});

        const candlestickSeries = chart.addCandlestickSeries({{
            upColor: '#ef4444',
            downColor: '#22c55e',
            borderUpColor: '#ef4444',
            borderDownColor: '#22c55e',
            wickUpColor: '#ef4444',
            wickDownColor: '#22c55e',
            lastValueVisible: false,
            priceLineVisible: false,
        }});

        const candleData = data.map(item => ({{
            time: item.dt,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
        }}));

        candlestickSeries.setData(candleData);

        // 成交量数据
        const volumeSeries = volumeChart.addHistogramSeries({{
            color: '#26a69a',
            priceFormat: {{
                type: 'volume',
            }},
            priceScaleId: '',
        }});

        const volumeData = data.map(item => ({{
            time: item.dt,
            value: item.volume,
            color: item.close >= item.open ? '#ef4444' : '#22c55e',
        }}));

        volumeSeries.setData(volumeData);

        // 均线配置
        const maColors = {{ 5: '#ff6d00', 10: '#2962ff', 20: '#00c853', 60: '#aa00ff' }};
        const maSeriesMap = {{}};

        [5, 10, 20, 60].forEach(period => {{
            const maKey = `ma${{period}}`;
            const maData = data
                .filter(item => item[maKey] !== null && item[maKey] !== undefined)
                .map(item => ({{
                    time: item.dt,
                    value: item[maKey],
                }}));

            if (maData.length > 0) {{
                const maSeries = chart.addLineSeries({{
                    color: maColors[period],
                    lineWidth: 1,
                    lastValueVisible: false,
                    priceLineVisible: false,
                }});
                maSeries.setData(maData);
                maSeriesMap[period] = maSeries;
            }}
        }});

        // 更新信息显示面板
        function updateInfoPanel(crosshairData) {{
            const item = crosshairData || data[data.length - 1];
            if (!item) return;

            const ohlcColor = item.close >= item.open ? '#ef4444' : '#22c55e';

            infoPanel.innerHTML = `
                <div style="color: ${{ohlcColor}}">
                    开盘: ${{item.open.toFixed(2)}} 最高: ${{item.high.toFixed(2)}} 最低: ${{item.low.toFixed(2)}} 收盘: ${{item.close.toFixed(2)}}
                </div>
                <div style="color: #666;">成交量: ${{(item.volume / 10000).toFixed(2)}}万</div>
            `;
        }}

        // 初始化显示最后一条数据
        updateInfoPanel(null);

        // 监听十字准星移动
        chart.subscribeCrosshairMove(param => {{
            if (param.time) {{
                const item = data.find(d => d.dt === param.time);
                if (item) {{
                    updateInfoPanel(item);
                }}
            }}
        }});

        // 同步两个图表的时间轴
        chart.timeScale().subscribeVisibleTimeRangeChange(timeRange => {{
            if (timeRange) {{
                volumeChart.timeScale().setVisibleLogicalRange(chart.timeScale().getVisibleLogicalRange());
            }}
        }});

        chart.timeScale().fitContent();
        volumeChart.timeScale().fitContent();

        window.addEventListener('resize', () => {{
            chart.applyOptions({{ width: klineContainer.clientWidth, height: klineContainer.clientHeight }});
            volumeChart.applyOptions({{ width: volumeContainer.clientWidth, height: volumeContainer.clientHeight }});
        }});
    }}

    // 搜索输入
    let searchTimeout;
    async function onSearchInput(value) {{
        clearTimeout(searchTimeout);

        if (value.length < 1) {{
            document.getElementById('search-suggestions').classList.add('hidden');
            return;
        }}

        searchTimeout = setTimeout(async () => {{
            try {{
                const response = await fetch(`/api/v1/search?q=${{encodeURIComponent(value)}}&limit=10`);
                const result = await response.json();

                const container = document.getElementById('search-suggestions');

                if (result.code === 0 && result.data.length > 0) {{
                    let html = '';
                    result.data.forEach(stock => {{
                        html += `
                            <div class="p-2 hover:bg-gray-50 cursor-pointer border-b last:border-b-0"
                                 onclick="onSearchSelect('${{stock.symbol}}', '${{stock.name}}')">
                                <div class="flex justify-between">
                                    <span class="font-medium">${{stock.name}}</span>
                                    <span class="text-gray-500 text-xs">${{stock.symbol}}</span>
                                </div>
                            </div>
                        `;
                    }});
                    container.innerHTML = html;
                    container.classList.remove('hidden');
                }} else {{
                    container.innerHTML = '<div class="p-2 text-gray-400">无搜索结果</div>';
                    container.classList.remove('hidden');
                }}
            }} catch (error) {{
                console.error('Search error:', error);
            }}
        }}, 300);
    }}

    // 搜索选择
    async function onSearchSelect(symbol, name) {{
        document.getElementById('search-input').value = '';
        document.getElementById('search-suggestions').classList.add('hidden');

        const inCurrentSector = state.stocks.some(s => s.symbol === symbol);

        if (inCurrentSector) {{
            onStockClick(symbol, name);
        }} else {{
            // 临时加入对比队列
            state.selectedStockSymbol = symbol;
            state.selectedStockName = name;
            loadStockSectors(symbol);

            // 检查是否已经在对比队列中
            const container = document.getElementById('kline-list');
            const existingCard = container.querySelector(`.kline-card[data-symbol="${{symbol}}"]`);

            if (!existingCard) {{
                // 创建临时对比卡片
                const tempStock = {{ symbol: symbol, name: name }};
                const card = createKlineCard(tempStock);
                card.classList.add('temp-compare-card');
                card.dataset.temp = 'true';

                // 添加到对比队列区域（在第一个位置）
                if (container.firstChild) {{
                    container.insertBefore(card, container.firstChild);
                }} else {{
                    container.appendChild(card);
                }}

                await loadKlineData(symbol, name, card);
                highlightKlineCard(symbol);
            }} else {{
                highlightKlineCard(symbol);
            }}
        }}
    }}

    // 无限滚动
    function setupInfiniteScroll() {{
        const container = document.getElementById('kline-list');

        container.addEventListener('scroll', () => {{
            if (container.scrollTop + container.clientHeight >= container.scrollHeight - 100) {{
                loadKlineList();
            }}
        }});
    }}

    // 创建板块弹窗
    function showCreateSectorModal() {{
        const name = prompt('请输入板块名称：');
        if (!name) return;
        console.log('Create sector:', name);
    }}

    // 点击外部关闭搜索建议
    document.addEventListener('click', function(e) {{
        const searchContainer = document.querySelector('.relative');
        if (searchContainer && !searchContainer.contains(e.target)) {{
            const suggestions = document.getElementById('search-suggestions');
            if (suggestions) suggestions.classList.add('hidden');
        }}
    }});

    // 添加样式
    const style = document.createElement('style');
    style.textContent = `
        .highlighted-card {{
            box-shadow: 0 0 0 3px #3b82f6, 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            transform: scale(1.01);
            transition: all 0.2s ease;
        }}
        .temp-compare-card {{
            border: 2px dashed #f59e0b;
            background: linear-gradient(135deg, #fffbeb 0%, #ffffff 100%);
        }}
        .temp-compare-card::before {{
            content: '对比';
            position: absolute;
            top: 5px;
            right: 5px;
            background: #f59e0b;
            color: white;
            font-size: 10px;
            padding: 2px 6px;
            border-radius: 3px;
            z-index: 5;
        }}
    `;
    document.head.appendChild(style);
    """


# 路由处理函数
async def analysis_handler(request: Request):
    """处理分析页面请求"""
    return analysis_page(request)
