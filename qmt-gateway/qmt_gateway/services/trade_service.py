"""交易服务

基于 qmt_broker.py 封装 xttrader 交易功能。
"""

import datetime
from uuid import uuid4
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from qmt_gateway.config import config
from qmt_gateway.core.enums import BidType, OrderSide, OrderStatus
from qmt_gateway.db.models import Order, Trade
from qmt_gateway.db.sqlite import db


# xtquant 模块（延迟导入）
_xtquant_modules: dict[str, Any] = {}
DEFAULT_PORTFOLIO_ID = "default"


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

    def __init__(self, service: "TradeService"):
        self._service = service

    def on_disconnected(self):
        """连接断开"""
        logger.warning("交易接口连接断开")

    def on_stock_order(self, order):
        """委托回报推送"""
        logger.info(f"委托回报: {order.stock_code} {order.order_status}")
        self._service.persist_callback_order(order)

    def on_stock_asset(self, asset):
        """资产回报推送"""
        logger.info(f"资产回报: {asset}")

    def on_stock_position(self, position):
        """持仓回报推送"""
        logger.info(f"持仓回报: {position}")

    def on_stock_trade(self, trade):
        """成交回报推送"""
        logger.info(f"成交回报: {trade}")
        self._service.persist_callback_trade(trade)

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
            callback = TradeCallback(self)
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

    def buy(self, symbol: str, price: float, shares: int, qtoid: str = "", strategy_id: str = "") -> dict:
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
            resolved_qtoid = qtoid or str(uuid4())

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
                strategy_name=strategy_id or "gateway",
                order_remark=resolved_qtoid,
            )

            if order_id == -1:
                return {"success": False, "error": "下单失败"}

            self._persist_submitted_order(
                symbol=symbol,
                side=OrderSide.BUY,
                price=price,
                shares=shares,
                qtoid=resolved_qtoid,
                foid=str(order_id),
                strategy_id=strategy_id,
            )
            return {"success": True, "qtoid": resolved_qtoid, "order_id": str(order_id)}

        except Exception as e:
            logger.error(f"买入失败: {e}")
            return {"success": False, "error": str(e)}

    def sell(self, symbol: str, price: float, shares: int, qtoid: str = "", strategy_id: str = "") -> dict:
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
            resolved_qtoid = qtoid or str(uuid4())

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
                strategy_name=strategy_id or "gateway",
                order_remark=resolved_qtoid,
            )

            if order_id == -1:
                return {"success": False, "error": "下单失败"}

            self._persist_submitted_order(
                symbol=symbol,
                side=OrderSide.SELL,
                price=price,
                shares=shares,
                qtoid=resolved_qtoid,
                foid=str(order_id),
                strategy_id=strategy_id,
            )
            return {"success": True, "qtoid": resolved_qtoid, "order_id": str(order_id)}

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
            db_order = db.get_order(order_id) or db.get_order_by_foid(order_id)
            foid = str(db_order.foid if db_order and db_order.foid else order_id)
            result = self._trader.cancel_order_stock(self._account, int(foid))
            if result == 0:
                if db_order is not None:
                    db.update_order(db_order.qtoid, status=OrderStatus.CANCELED)
                return {"success": True, "qtoid": db_order.qtoid if db_order else order_id}
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
                status_code = self._convert_order_status_code(o.order_status)
                qtoid = str(getattr(o, "order_remark", "") or "")
                db_order = db.get_order_by_foid(str(o.order_id))
                if db_order is not None:
                    qtoid = db_order.qtoid
                if not qtoid:
                    qtoid = str(o.order_id)
                order = {
                    "symbol": o.stock_code,
                    "name": "",
                    "side": self._convert_order_side(o.order_type),
                    "price": o.price,
                    "shares": o.order_volume,
                    "filled": o.traded_volume,
                    "status": self._status_code_to_text(status_code),
                    "status_code": status_code,
                    "time": datetime.datetime.fromtimestamp(o.order_time).strftime("%H:%M:%S"),
                    "qtoid": qtoid,
                    "foid": str(o.order_id),
                    "cid": str(getattr(o, "order_sysid", "") or ""),
                }
                orders.append(order)
                self._persist_order_snapshot(order)

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
                qtoid = str(getattr(t, "order_remark", "") or "")
                db_order = db.get_order_by_foid(str(getattr(t, "order_id", "") or ""))
                if db_order is not None:
                    qtoid = db_order.qtoid
                trade = {
                    "tid": str(getattr(t, "traded_id", "") or ""),
                    "qtoid": qtoid,
                    "symbol": t.stock_code,
                    "name": "",
                    "side": "buy" if t.order_type == 23 else "sell",
                    "price": t.traded_price,
                    "shares": t.traded_volume,
                    "amount": t.traded_amount,
                    "time": datetime.datetime.fromtimestamp(t.traded_time).strftime("%H:%M:%S"),
                }
                trades.append(trade)
                self._persist_trade_snapshot(
                    trade,
                    foid=str(getattr(t, "order_id", "") or ""),
                    cid=str(getattr(t, "order_sysid", "") or ""),
                )

            return trades

        except Exception as e:
            logger.error(f"获取成交失败: {e}")
            return []

    def _convert_order_side(self, order_type: Any) -> str:
        try:
            xtconstant = _get_xtquant("xtconstant")
            buy_type = int(getattr(xtconstant, "STOCK_BUY", 23))
        except Exception:
            buy_type = 23
        return "buy" if int(order_type) == buy_type else "sell"

    def _status_code_to_text(self, status_code: int) -> str:
        status_map = {
            48: "unreported",
            49: "pending",
            50: "reported",
            51: "canceling",
            52: "partial_canceling",
            53: "partial_cancelled",
            54: "cancelled",
            55: "partial",
            56: "filled",
            57: "rejected",
        }
        return status_map.get(status_code, "unknown")

    def _convert_order_status(self, xt_status: Any) -> str:
        return self._status_code_to_text(self._convert_order_status_code(xt_status))

    def _convert_order_status_code(self, xt_status: Any) -> int:
        aliases = {
            "unreported": 48,
            "wait_reporting": 49,
            "pending": 49,
            "reported": 50,
            "reported_cancel": 51,
            "canceling": 51,
            "partsucc_cancel": 52,
            "partial_canceling": 52,
            "part_cancel": 53,
            "partial_cancelled": 53,
            "canceled": 54,
            "cancelled": 54,
            "part_succ": 55,
            "partial": 55,
            "succeeded": 56,
            "filled": 56,
            "junk": 57,
            "rejected": 57,
            "unknown": 255,
        }
        text = str(xt_status).strip().lower()
        if text in aliases:
            return aliases[text]
        try:
            code = int(text)
        except (TypeError, ValueError):
            return 255
        if 0 <= code <= 9:
            return 48 + code
        if code in {48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 255}:
            return code
        return 255

    def _persist_order_snapshot(self, order: dict) -> None:
        qtoid = str(order.get("qtoid", "")).strip()
        symbol = str(order.get("symbol", "")).strip()
        if not qtoid or not symbol:
            return
        now = datetime.datetime.now()
        tm = now
        tm_text = str(order.get("time", "")).strip()
        if tm_text:
            try:
                tm = datetime.datetime.fromisoformat(
                    f"{now.date().isoformat()} {tm_text}"
                )
            except ValueError:
                tm = now
        side_text = str(order.get("side", "buy")).strip().lower()
        side = OrderSide.BUY if side_text == "buy" else OrderSide.SELL
        status_code = self._convert_order_status_code(
            order.get("status_code", order.get("status", "unknown"))
        )
        foid = str(order.get("foid", "") or qtoid)
        db["orders"].upsert(
            Order(
                qtoid=qtoid,
                portfolio_id=DEFAULT_PORTFOLIO_ID,
                asset=symbol,
                side=side,
                shares=float(order.get("shares", 0) or 0),
                bid_type=BidType.UNKNOWN,
                tm=tm,
                price=float(order.get("price", 0) or 0),
                filled=float(order.get("filled", 0) or 0),
                foid=foid,
                status=OrderStatus(status_code),
                status_msg="",
                cid=str(order.get("cid", "") or ""),
                strategy=str(order.get("strategy_id", "") or "gateway"),
            ).to_dict(),
            pk=Order.__pk__,
        )

    def _persist_submitted_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        price: float,
        shares: int,
        qtoid: str,
        foid: str,
        strategy_id: str,
    ) -> None:
        """在 gateway 提交成功后立即建立 qtoid 映射。"""
        db["orders"].upsert(
            Order(
                qtoid=qtoid,
                portfolio_id=DEFAULT_PORTFOLIO_ID,
                asset=symbol,
                side=side,
                shares=float(shares),
                bid_type=BidType.UNKNOWN,
                tm=datetime.datetime.now(),
                price=float(price),
                filled=0.0,
                foid=foid,
                status=OrderStatus.REPORTED,
                status_msg="",
                strategy=strategy_id or "gateway",
            ).to_dict(),
            pk=Order.__pk__,
        )

    def _persist_trade_snapshot(self, trade: dict, *, foid: str, cid: str) -> None:
        """落地成交快照并维持 qtoid 关联。"""
        qtoid = str(trade.get("qtoid", "") or "").strip()
        if not qtoid and foid:
            db_order = db.get_order_by_foid(foid)
            if db_order is not None:
                qtoid = db_order.qtoid
        if not qtoid:
            return
        time_text = str(trade.get("time", "") or "").strip()
        now = datetime.datetime.now()
        tm = now
        if time_text:
            try:
                tm = datetime.datetime.fromisoformat(
                    f"{now.date().isoformat()} {time_text}"
                )
            except ValueError:
                tm = now
        side_text = str(trade.get("side", "buy")).strip().lower()
        db.insert_trade(
            Trade(
                tid=str(trade.get("tid", "") or str(uuid4())),
                portfolio_id=DEFAULT_PORTFOLIO_ID,
                qtoid=qtoid,
                foid=foid,
                asset=str(trade.get("symbol", "") or ""),
                shares=float(trade.get("shares", 0) or 0),
                price=float(trade.get("price", 0) or 0),
                amount=float(trade.get("amount", 0) or 0),
                tm=tm,
                side=OrderSide.BUY if side_text == "buy" else OrderSide.SELL,
                cid=cid,
            )
        )

    def persist_callback_trade(self, xt_trade: Any) -> None:
        """处理成交回调并将其映射回 qtoid。"""
        try:
            trade = {
                "tid": str(getattr(xt_trade, "traded_id", "") or ""),
                "qtoid": str(getattr(xt_trade, "order_remark", "") or ""),
                "symbol": getattr(xt_trade, "stock_code", ""),
                "side": self._convert_order_side(getattr(xt_trade, "order_type", 23)),
                "price": float(getattr(xt_trade, "traded_price", 0) or 0),
                "shares": float(getattr(xt_trade, "traded_volume", 0) or 0),
                "amount": float(getattr(xt_trade, "traded_amount", 0) or 0),
                "time": datetime.datetime.fromtimestamp(
                    getattr(xt_trade, "traded_time", 0) or 0
                ).strftime("%H:%M:%S"),
            }
            foid = str(getattr(xt_trade, "order_id", "") or "")
            cid = str(getattr(xt_trade, "order_sysid", "") or "")
            self._persist_trade_snapshot(trade, foid=foid, cid=cid)
            db_order = db.get_order_by_foid(foid)
            if db_order is not None:
                db.update_order(db_order.qtoid, status=OrderStatus.SUCCEEDED)
        except Exception as e:
            logger.error(f"回调成交落库失败: {e}")

    def persist_callback_order(self, xt_order: Any) -> None:
        try:
            order_time = getattr(xt_order, "order_time", None)
            if order_time:
                time_text = datetime.datetime.fromtimestamp(order_time).strftime(
                    "%H:%M:%S"
                )
            else:
                time_text = datetime.datetime.now().strftime("%H:%M:%S")
            qtoid = str(getattr(xt_order, "order_remark", "") or "")
            db_order = db.get_order_by_foid(str(getattr(xt_order, "order_id", "") or ""))
            if db_order is not None:
                qtoid = db_order.qtoid
            order = {
                "symbol": getattr(xt_order, "stock_code", ""),
                "side": self._convert_order_side(getattr(xt_order, "order_type", 23)),
                "price": float(getattr(xt_order, "price", 0) or 0),
                "shares": float(getattr(xt_order, "order_volume", 0) or 0),
                "filled": float(getattr(xt_order, "traded_volume", 0) or 0),
                "status_code": self._convert_order_status_code(
                    getattr(xt_order, "order_status", 255)
                ),
                "time": time_text,
                "qtoid": qtoid,
                "foid": str(getattr(xt_order, "order_id", "")),
                "cid": str(getattr(xt_order, "order_sysid", "") or ""),
                "strategy_id": str(getattr(xt_order, "strategy_name", "") or "gateway"),
            }
            self._persist_order_snapshot(order)
        except Exception as e:
            logger.error(f"回调委托落库失败: {e}")


# 全局交易服务实例
trade_service = TradeService()
