"""股票信息 API

提供股票查询、搜索等功能。
"""

from fasthtml.common import *
from loguru import logger

from qmt_gateway.services.stock_service import stock_service


def register_routes(app):
    """注册股票相关路由"""

    @app.get("/api/stocks/search")
    def search_stocks(request, stock_search: str = ""):
        """搜索股票（带自动补全）

        Args:
            stock_search: 搜索关键词（支持代码、名称、拼音）

        Returns:
            HTML 格式的下拉列表
        """
        q = stock_search
        logger.info(f"[DEBUG] search_stocks called with q='{q}'")
        
        if not q or len(q) < 1:
            logger.info("[DEBUG] Empty query, returning empty")
            return ""

        # 检查股票列表是否已加载
        all_stocks = stock_service.get_all_stocks()
        logger.info(f"[DEBUG] Total stocks in memory: {len(all_stocks)}")
        
        stocks = stock_service.search_stocks(q)
        logger.info(f"[DEBUG] Found {len(stocks)} stocks for query '{q}'")
        
        if not stocks:
            return Div("无匹配结果", cls="p-2 text-gray-500 text-sm")
        
        # 如果只有一个结果，自动填充
        if len(stocks) == 1:
            stock = stocks[0]
            safe_name = stock.name.replace("'", "\\'")
            return Div(
                # 下拉列表显示
                Div(
                    Div(
                        Span(stock.name, cls="font-medium"),
                        Span(f" ({stock.symbol})", cls="text-gray-500 text-sm ml-1"),
                        Span(f" {stock.pinyin}", cls="text-gray-400 text-xs ml-1"),
                        cls="p-2 hover:bg-blue-50 cursor-pointer",
                        onclick=f"if(window.selectStock){{window.selectStock('{stock.symbol}', '{safe_name}', {stock.last_close});}}else{{console.error('selectStock not found');}}",
                    ),
                ),
                # 自动填充脚本
                Script(f"""
                    setTimeout(function() {{
                        if(window.selectStock) {{
                            window.selectStock('{stock.symbol}', '{safe_name}', {stock.last_close});
                        }} else {{
                            console.error('selectStock not found');
                        }}
                    }}, 100);
                """),
            )

        # 多个结果，显示下拉列表
        items = []
        for stock in stocks[:10]:  # 最多显示10个
            # 转义股票名称中的单引号，避免 JavaScript 语法错误
            safe_name = stock.name.replace("'", "\\'")
            items.append(
                Div(
                    Div(
                        Span(stock.name, cls="font-medium"),
                        Span(f" ({stock.symbol})", cls="text-gray-500 text-sm ml-1"),
                        Span(f" {stock.pinyin}", cls="text-gray-400 text-xs ml-1"),
                        cls="p-2 hover:bg-blue-50 cursor-pointer",
                        onclick=f"if(window.selectStock){{window.selectStock('{stock.symbol}', '{safe_name}', {stock.last_close});}}else{{console.error('selectStock not found');}}",
                    ),
                )
            )
        
        return Div(*items)

    @app.get("/api/stock/info")
    def get_stock_info(request, symbol: str = ""):
        """获取股票信息并返回 Speed Dial HTML

        Args:
            symbol: 股票代码

        Returns:
            Speed Dial HTML
        """
        from qmt_gateway.web.pages.trading import SpeedDialGrid

        if not symbol:
            return SpeedDialGrid(0)

        stock = stock_service.get_stock(symbol)
        if stock:
            return SpeedDialGrid(stock.last_close)

        # 尝试从 xtquant 获取实时价格
        try:
            from qmt_gateway.core.xtwrapper import require_xtdata
            xtdata = require_xtdata()

            # 获取昨收价格
            last_close = 0.0
            try:
                history = xtdata.get_market_data(['close'], [symbol], period='1d', count=1)
                if history and symbol in history:
                    last_close = float(history[symbol]['close'].iloc[-1])
            except Exception:
                pass

            return SpeedDialGrid(last_close)
        except Exception as e:
            logger.warning(f"获取股票 {symbol} 信息失败: {e}")
            return SpeedDialGrid(0)

    @app.get("/api/stocks")
    def get_all_stocks(request):
        """获取所有股票列表"""
        stocks = stock_service.get_all_stocks()
        return [s.to_dict() for s in stocks]
