"""股票数据服务

提供股票列表获取、拼音转换、定时更新等功能。
"""

from datetime import datetime
from typing import List, Optional

from loguru import logger
from pypinyin import lazy_pinyin

from qmt_gateway.core.xtwrapper import require_xtdata
from qmt_gateway.db.models import Stock


def chinese_to_pinyin(text: str) -> str:
    """将中文转换为拼音首字母
    
    使用 pypinyin 库实现准确的拼音转换。
    
    Args:
        text: 中文字符串
        
    Returns:
        拼音首字母字符串（小写）
    """
    try:
        # 获取拼音首字母
        pinyin_list = lazy_pinyin(text, style=0)  # style=0 返回普通拼音
        # 取每个拼音的首字母
        result = ''.join([p[0].lower() for p in pinyin_list if p])
        return result
    except Exception as e:
        logger.warning(f"拼音转换失败: {text}, 错误: {e}")
        return ""


class StockService:
    """股票数据服务"""

    def __init__(self):
        self._stocks: dict[str, Stock] = {}
        self._last_update: Optional[datetime] = None

    def update_stock_list(self) -> bool:
        """从 xtquant 获取股票列表并更新

        Returns:
            是否更新成功
        """
        try:
            xtdata = require_xtdata()
            
            # 获取沪深A股列表
            stock_list = xtdata.get_stock_list_in_sector('沪深A股')
            
            stocks = []
            for symbol in stock_list:
                try:
                    # 获取股票详细信息（包含名称）
                    detail = xtdata.get_instrument_detail(symbol)
                    if detail is None:
                        logger.warning(f"获取股票 {symbol} 详细信息失败")
                        continue
                    
                    name = detail.get("InstrumentName", symbol)
                    
                    # 获取昨收价格
                    last_close = 0.0
                    try:
                        # 尝试获取历史数据
                        history = xtdata.get_market_data(['close'], [symbol], period='1d', count=1)
                        if history and symbol in history:
                            last_close = float(history[symbol]['close'].iloc[-1])
                    except Exception:
                        pass
                    
                    # 生成拼音
                    pinyin = chinese_to_pinyin(name)
                    
                    stock = Stock(
                        symbol=symbol,
                        name=name,
                        pinyin=pinyin,
                        last_close=last_close,
                        updated_at=datetime.now(),
                    )
                    stocks.append(stock)
                    
                except Exception as e:
                    logger.warning(f"获取股票 {symbol} 信息失败: {e}")
                    continue
            
            # 更新内存中的股票列表
            self._stocks = {s.symbol: s for s in stocks}
            self._last_update = datetime.now()
            
            logger.info(f"股票列表更新成功，共 {len(stocks)} 只股票")
            return True
            
        except Exception as e:
            logger.error(f"更新股票列表失败: {e}")
            return False

    def search_stocks(self, query: str) -> List[Stock]:
        """搜索股票

        支持按代码、名称、拼音搜索。

        Args:
            query: 搜索关键词

        Returns:
            匹配的股票列表
        """
        logger.info(f"[DEBUG] search_stocks: query='{query}', stocks_count={len(self._stocks)}")
        
        if not query:
            logger.info("[DEBUG] search_stocks: empty query")
            return []
        
        query = query.lower()
        results = []
        
        # 只检查前5个股票作为示例
        sample_stocks = list(self._stocks.values())[:5]
        for stock in sample_stocks:
            logger.info(f"[DEBUG] Sample stock: {stock.symbol}, name='{stock.name}', pinyin='{stock.pinyin}'")
        
        for stock in self._stocks.values():
            # 按代码搜索
            if query in stock.symbol.lower():
                logger.info(f"[DEBUG] Matched by symbol: {stock.symbol}")
                results.append(stock)
                continue
            
            # 按名称搜索
            if query in stock.name.lower():
                logger.info(f"[DEBUG] Matched by name: {stock.name}")
                results.append(stock)
                continue
            
            # 按拼音搜索
            if stock.pinyin and query in stock.pinyin.lower():
                logger.info(f"[DEBUG] Matched by pinyin: {stock.pinyin}")
                results.append(stock)
                continue
        
        logger.info(f"[DEBUG] search_stocks: found {len(results)} results")
        # 限制返回数量
        return results[:20]

    def get_stock(self, symbol: str) -> Optional[Stock]:
        """获取单只股票信息

        Args:
            symbol: 股票代码

        Returns:
            股票信息，如果不存在则返回 None
        """
        return self._stocks.get(symbol)

    def get_all_stocks(self) -> List[Stock]:
        """获取所有股票列表

        Returns:
            股票列表
        """
        return list(self._stocks.values())


# 全局股票服务实例
stock_service = StockService()
