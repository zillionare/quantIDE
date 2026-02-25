"""分析导航主页面"""

import datetime

from fasthtml.common import *
from starlette.requests import Request
from starlette.responses import HTMLResponse

from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.sqlite import db
from pyqmt.web.components.analysis.kline_chart import KlineChart
from pyqmt.web.components.analysis.sector_list import SectorList
from pyqmt.web.components.analysis.stock_list import StockList
from pyqmt.web.layouts.main import MainLayout


def get_sector_dal() -> SectorDAL:
    """获取 SectorDAL 实例"""
    return SectorDAL(db)


def analysis_page(request: Request):
    """分析导航主页面"""
    session = request.scope.get("session", {})
    layout = MainLayout(title="分析导航", user=session.get("auth"))
    layout.header_active = "分析"
    layout.set_sidebar_active("/analysis")

    # 获取板块列表
    dal = get_sector_dal()
    sectors = dal.list_sectors()
    sectors_data = [
        {
            "id": s.id,
            "name": s.name,
            "sector_type": s.sector_type,
            "source": s.source,
            "stock_count": len(dal.get_sector_stocks(s.id)),
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

    # K线图组件
    kline_chart = KlineChart(
        chart_id="main-kline",
        height=500,
        symbol=default_symbol,
        name=default_name,
        freq="day",
    )

    # 页面内容
    layout.main_block = lambda: Div(
        # 顶部工具栏
        Div(
            # 周期切换
            KlineChart.freq_buttons("main-kline", "day"),
            # 均线切换
            KlineChart.ma_buttons("main-kline", [5, 10, 20, 60]),
            # 对比选择
            Div(
                Select(
                    Option("不对比", value=""),
                    Option("上证指数", value="000001.SH"),
                    Option("深证成指", value="399001.SZ"),
                    Option("创业板指", value="399006.SZ"),
                    cls="px-3 py-1 border rounded text-sm",
                    onchange="setCompare(this.value)",
                ),
                cls="ml-auto",
            ),
            cls="flex items-center gap-4 mb-4",
        ),
        # 主内容区
        Div(
            # 左侧：板块列表
            Div(
                SectorList.toolbar(),
                SectorList(sectors_data, selected_sector_id).render(),
                cls="w-64 flex-shrink-0",
            ),
            # 中间：K线图
            Div(
                kline_chart.render(),
                cls="flex-1 mx-4",
            ),
            # 右侧：成分股列表
            Div(
                StockList.toolbar(selected_sector_name),
                StockList(stocks_data).render(),
                cls="w-64 flex-shrink-0",
            ),
            cls="flex gap-4",
        ),
        # JavaScript 交互
        Script("""
        // 全局变量
        let currentSymbol = '""" + default_symbol + """';
        let currentName = '""" + default_name + """';
        let currentFreq = 'day';
        let currentSectorId = '""" + (selected_sector_id if selected_sector_id else "") + """';

        // 加载K线数据
        async function loadKlineData(symbol, freq, maPeriods) {
            // 暂时使用固定的2024年日期范围，因为目前只有2024年的数据
            const end = '2024-12-31';
            const start = '2024-01-01';
            const ma = maPeriods ? maPeriods.join(',') : '';

            const url = `/api/v1/kline/stock/${symbol}?start=${start}&end=${end}&freq=${freq}&ma=${ma}`;

            try {
                const response = await fetch(url);
                const result = await response.json();

                if (result.code === 0) {
                    updateKlineData_main_kline(result.data.items);
                    setSymbol_main_kline(symbol, currentName);
                } else {
                    console.error('Failed to load kline data:', result.message);
                }
            } catch (error) {
                console.error('Error loading kline data:', error);
            }
        }

        // 切换周期
        function switchFreq_main_kline(freq) {
            currentFreq = freq;
            loadKlineData(currentSymbol, currentFreq, [5, 10, 20, 60]);

            // 更新按钮样式
            document.querySelectorAll('[data-freq]').forEach(btn => {
                if (btn.dataset.freq === freq) {
                    btn.className = 'px-3 py-1 text-sm rounded bg-blue-600 text-white';
                } else {
                    btn.className = 'px-3 py-1 text-sm rounded bg-gray-200 text-gray-700 hover:bg-gray-300';
                }
            });
        }

        // 选择板块
        async function selectSector(sectorId) {
            currentSectorId = sectorId;

            // 更新选中样式
            document.querySelectorAll('[data-sector-id]').forEach(el => {
                if (el.dataset.sectorId === sectorId) {
                    el.classList.add('bg-blue-50', 'border-blue-200');
                    el.classList.remove('border-transparent');
                } else {
                    el.classList.remove('bg-blue-50', 'border-blue-200');
                    el.classList.add('border-transparent');
                }
            });

            // 加载板块成分股
            try {
                const response = await fetch(`/api/v1/sectors/${sectorId}/stocks`);
                const result = await response.json();

                if (result.code === 0 && result.data.length > 0) {
                    // 更新股票列表
                    updateStockList(result.data);

                    // 默认选中第一只股票
                    const firstStock = result.data[0];
                    selectStock(firstStock.symbol);
                }
            } catch (error) {
                console.error('Error loading sector stocks:', error);
            }
        }

        // 选择股票
        function selectStock(symbol) {
            currentSymbol = symbol;

            // 更新选中样式
            document.querySelectorAll('[data-symbol]').forEach(el => {
                if (el.dataset.symbol === symbol) {
                    el.classList.add('bg-blue-50');
                } else {
                    el.classList.remove('bg-blue-50');
                }
            });

            // 加载K线数据
            loadKlineData(symbol, currentFreq, [5, 10, 20, 60]);
        }

        // 更新股票列表
        function updateStockList(stocks) {
            const container = document.querySelector('.stock-list-container');
            if (!container) return;

            // 这里可以通过 HTMX 或重新渲染来更新列表
            // 简化处理：刷新页面
            // location.reload();
        }

        // 设置对比
        function setCompare(compareSymbol) {
            if (!compareSymbol) return;

            // TODO: 实现对比功能
            console.log('Compare with:', compareSymbol);
        }

        // 搜索板块
        function filterSectors(keyword) {
            const items = document.querySelectorAll('[data-sector-id]');
            items.forEach(item => {
                const name = item.querySelector('.font-medium').textContent;
                if (name.toLowerCase().includes(keyword.toLowerCase())) {
                    item.style.display = 'flex';
                } else {
                    item.style.display = 'none';
                }
            });
        }

        // 显示创建板块弹窗
        function showCreateSectorModal() {
            // TODO: 实现创建板块弹窗
            alert('创建板块功能待实现');
        }

        // 刷新板块列表
        function refreshSectors() {
            location.reload();
        }

        // 显示添加股票弹窗
        function showAddStockModal() {
            // TODO: 实现添加股票弹窗
            alert('添加股票功能待实现');
        }

        // 显示导入弹窗
        function showImportModal() {
            // TODO: 实现导入弹窗
            alert('导入功能待实现');
        }

        // 查看股票
        function viewStock(symbol) {
            selectStock(symbol);
        }

        // 页面加载完成后初始化
        document.addEventListener('DOMContentLoaded', function() {
            // 加载初始K线数据
            loadKlineData(currentSymbol, currentFreq, [5, 10, 20, 60]);
        });
        """),
        cls="p-4",
    )

    return HTMLResponse(to_xml(layout.render()))


# 路由处理函数
async def analysis_handler(request: Request):
    """处理分析页面请求"""
    return analysis_page(request)
