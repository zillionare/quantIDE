"""交易服务

基于 qmt_broker.py 封装 xttrader 交易功能。
"""

import datetime
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from qmt_gateway.config import config


# xtquant 模块（延迟导入）
_xtquant_modules: dict[str, Any] = {}


def _get_xtquant(name: str):
    """延迟获取 xtquant 模块或类"""
    if name not in _xtquant_modules:
        if name == "xtconstant":
            from xtquant import xtconstant as mod
        elif name == "XtQuantTrader":
            from xtquant.xttrader import XtQuantTrader as mod
        elif name == "XtQuantTraderCallback":
            from xtquant.xttrader import XtQuantTraderCallback as mod
        elif name == "StockAccount":
            from xtquant.xttype import StockAccount as mod
        elif name == "XtAsset":
            from xtquant.xttype import XtAsset as mod
        elif name == "XtPosition":
            from xtquant.xttype import XtPosition as mod
        elif name == "XtOrder":
            from xtquant.xttype import XtOrder as mod
        elif name == "XtTrade":
            from xtquant.xttype import XtTrade as mod
        else:
            raise ImportError(f"Unknown xtquant module: {name}")
        _xtquant_modules[name] = mod
    return _xtquant_modules[name]


class TradeCallback:
    """交易回调实现"""

    def on_disconnected(self):
        """连接断开"""
        logger.warning("交易接口连接断开")

    def on_stock_order(self, order):
        """委托回报推送"""
        logger.info(f"委托回报: {order.stock_code} {order.order_status}")

    def on_stock_asset(self, asset):
        """资产回报推送"""
        logger.info(f"资产回报: {asset}")

    def on_stock_position(self, position):
        """持仓回报推送"""
        logger.info(f"持仓回报: {position}")

    def on_stock_trade(self, trade):
        """成交回报推送"""
        logger.info(f"成交回报: {trade}")

    def on_order_error(self, order_error):
        """委托失败推送"""
        logger.error(f"委托失败: {order_error}")

    def on_cancel_error(self, cancel_error):
        """撤单失败推送"""
        logger.error(f"撤单失败: {cancel_error}")


class TradeService:
    """交易服务

    封装 xttrader 交易功能，提供买入、卖出、撤单、查询等操作。
    """

    def __init__(self):
        self._trader = None
        self._account = None
        self._connected = False
        self._account_id = None
        self._qmt_path = None

    def connect(self, account_id: str, qmt_path: str) -> bool:
        """连接交易接口

        Args:
            account_id: 资金账号
            qmt_path: QMT 安装路径

        Returns:
            是否连接成功
        """
        try:
            self._account_id = account_id
            self._qmt_path = qmt_path

            # 延迟导入 xtquant
            XtQuantTrader = _get_xtquant("XtQuantTrader")
            StockAccount = _get_xtquant("StockAccount")
            xtconstant = _get_xtquant("xtconstant")

            # 创建账户对象
            self._account = StockAccount(account_id, "stock")

            # 创建交易对象
            session_id = int(account_id) if account_id.isdigit() else 1
            self._trader = XtQuantTrader(qmt_path, session_id)

            # 注册回调
            callback = TradeCallback()
            self._trader.register_callback(callback)

            # 启动交易接口
            self._trader.start()

            # 连接
            connect_result = self._trader.connect()
            if connect_result != 0:
                logger.error(f"连接交易接口失败，错误码: {connect_result}")
                self._trader.stop()
                self._trader = None
                return False

            # 订阅账户
            subscribe_result = self._trader.subscribe(self._account)
            if subscribe_result != 0:
                logger.error(f"订阅账户失败，错误码: {subscribe_result}")
                self._trader.stop()
                self._trader = None
                return False

            self._connected = True
            logger.info(f"交易接口连接成功，账号: {account_id}")
            return True

        except Exception as e:
            logger.error(f"连接交易接口失败: {e}")
            self._connected = False
            return False

    def disconnect(self):
        """断开交易接口连接"""
        if self._trader is not None:
            try:
                self._trader.stop()
                logger.info("交易接口已断开")
            except Exception as e:
                logger.error(f"断开交易接口失败: {e}")
            finally:
                self._trader = None
                self._connected = False

    def buy(self, symbol: str, price: float, shares: int) -> dict:
        """买入股票

        Args:
            symbol: 股票代码
            price: 委托价格（0 表示市价）
            shares: 委托数量

        Returns:
            委托结果
        """
        if not self._connected:
            return {"success": False, "error": "交易接口未连接"}

        try:
            xtconstant = _get_xtquant("xtconstant")

            # 确定价格类型
            if price <= 0:
                # 市价买入
                market = symbol.split(".")[1].upper() if "." in symbol else "SH"
                if market == "SH":
                    price_type = xtconstant.MARKET_SH_CONVERT_5_CANCEL
                else:
                    price_type = xtconstant.MARKET_SZ_CONVERT_5_CANCEL
                price = 0
            else:
                price_type = xtconstant.FIX_PRICE

            # 下单
            order_id = self._trader.order_stock(
                account=self._account,
                stock_code=symbol,
                order_type=xtconstant.STOCK_BUY,
                order_volume=shares,
                price_type=price_type,
                price=price,
                strategy_name="gateway",
                order_remark="",
            )

            if order_id == -1:
                return {"success": False, "error": "下单失败"}

            return {"success": True, "order_id": str(order_id)}

        except Exception as e:
            logger.error(f"买入失败: {e}")
            return {"success": False, "error": str(e)}

    def sell(self, symbol: str, price: float, shares: int) -> dict:
        """卖出股票

        Args:
            symbol: 股票代码
            price: 委托价格（0 表示市价）
            shares: 委托数量

        Returns:
            委托结果
        """
        if not self._connected:
            return {"success": False, "error": "交易接口未连接"}

        try:
            xtconstant = _get_xtquant("xtconstant")

            # 确定价格类型
            if price <= 0:
                # 市价卖出
                market = symbol.split(".")[1].upper() if "." in symbol else "SH"
                if market == "SH":
                    price_type = xtconstant.MARKET_SH_CONVERT_5_CANCEL
                else:
                    price_type = xtconstant.MARKET_SZ_CONVERT_5_CANCEL
                price = 0
            else:
                price_type = xtconstant.FIX_PRICE

            # 下单
            order_id = self._trader.order_stock(
                account=self._account,
                stock_code=symbol,
                order_type=xtconstant.STOCK_SELL,
                order_volume=shares,
                price_type=price_type,
                price=price,
                strategy_name="gateway",
                order_remark="",
            )

            if order_id == -1:
                return {"success": False, "error": "下单失败"}

            return {"success": True, "order_id": str(order_id)}

        except Exception as e:
            logger.error(f"卖出失败: {e}")
            return {"success": False, "error": str(e)}

    def cancel_order(self, order_id: str) -> dict:
        """撤单

        Args:
            order_id: 订单 ID

        Returns:
            撤单结果
        """
        if not self._connected:
            return {"success": False, "error": "交易接口未连接"}

        try:
            result = self._trader.cancel_order_stock(self._account, int(order_id))
            if result == 0:
                return {"success": True}
            else:
                return {"success": False, "error": f"撤单失败，错误码: {result}"}

        except Exception as e:
            logger.error(f"撤单失败: {e}")
            return {"success": False, "error": str(e)}

    def get_asset(self) -> Optional[dict]:
        """获取账户资产

        Returns:
            账户资产信息字典
        """
        if not self._connected:
            return None

        try:
            xt_asset = self._trader.query_stock_asset(self._account)
            if xt_asset is None:
                return None

            return {
                "total": xt_asset.total_asset,
                "cash": xt_asset.cash,
                "market_value": xt_asset.market_value,
                "frozen_cash": xt_asset.frozen_cash,
            }

        except Exception as e:
            logger.error(f"获取资产失败: {e}")
            return None

    def get_positions(self) -> list[dict]:
        """获取持仓列表

        Returns:
            持仓列表（字典列表）
        """
        if not self._connected:
            return []

        try:
            xt_positions = self._trader.query_stock_positions(self._account)
            if xt_positions is None:
                return []

            positions = []
            for p in xt_positions:
                pos = {
                    "symbol": p.stock_code,
                    "name": "",  # 需要通过其他方式获取名称
                    "shares": p.volume,
                    "avail": p.can_use_volume,
                    "price": p.avg_price,
                    "cost": p.avg_price,
                    "profit": p.profit_rate,
                    "market_value": p.market_value,
                }
                positions.append(pos)

            return positions

        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
            return []

    def get_orders(self) -> list[dict]:
        """获取当日委托列表

        Returns:
            委托列表（字典列表）
        """
        if not self._connected:
            return []

        try:
            xt_orders = self._trader.query_stock_orders(self._account)
            if xt_orders is None:
                return []

            orders = []
            for o in xt_orders:
                order = {
                    "symbol": o.stock_code,
                    "name": "",
                    "side": "buy" if o.order_type == 23 else "sell",  # 23 = STOCK_BUY
                    "price": o.price,
                    "shares": o.order_volume,
                    "filled": o.traded_volume,
                    "status": self._convert_order_status(o.order_status),
                    "time": datetime.datetime.fromtimestamp(o.order_time).strftime("%H:%M:%S"),
                    "qtoid": str(o.order_id),
                }
                orders.append(order)

            return orders

        except Exception as e:
            logger.error(f"获取委托失败: {e}")
            return []

    def get_trades(self) -> list[dict]:
        """获取当日成交列表

        Returns:
            成交列表（字典列表）
        """
        if not self._connected:
            return []

        try:
            xt_trades = self._trader.query_stock_trades(self._account)
            if xt_trades is None:
                return []

            trades = []
            for t in xt_trades:
                trade = {
                    "symbol": t.stock_code,
                    "name": "",
                    "side": "buy" if t.order_type == 23 else "sell",
                    "price": t.traded_price,
                    "shares": t.traded_volume,
                    "amount": t.traded_amount,
                    "time": datetime.datetime.fromtimestamp(t.traded_time).strftime("%H:%M:%S"),
                }
                trades.append(trade)

            return trades

        except Exception as e:
            logger.error(f"获取成交失败: {e}")
            return []

    def _convert_order_status(self, xt_status: int) -> str:
        """转换订单状态"""
        # xtquant 订单状态
        # 0: 未报
        # 1: 待报
        # 2: 已报
        # 3: 已报待撤
        # 4: 部成待撤
        # 5: 部撤
        # 6: 已撤
        # 7: 部成
        # 8: 已成
        # 9: 废单
        status_map = {
            0: "unreported",
            1: "pending",
            2: "reported",
            3: "canceling",
            4: "partial_canceling",
            5: "partial_cancelled",
            6: "cancelled",
            7: "partial",
            8: "filled",
            9: "rejected",
        }
        return status_map.get(xt_status, "unknown")


# 全局交易服务实例
trade_service = TradeService()
