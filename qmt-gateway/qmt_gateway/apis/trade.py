"""交易 API

提供账户资金、持仓、订单查询和交易执行功能。
"""

import datetime
from fasthtml.common import *
from loguru import logger

from qmt_gateway.config import config
from qmt_gateway.db.models import Asset, Position
from qmt_gateway.db.sqlite import db
from qmt_gateway.services.quote_service import quote_service
from qmt_gateway.services.stock_service import stock_service
from qmt_gateway.services.trade_service import trade_service

DEFAULT_PORTFOLIO_ID = "default"


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_value(item, key: str, default=None):
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _fetch_one_dict(sql: str, params: tuple = ()) -> dict | None:
    cursor = db.conn.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [c[0] for c in cursor.description]
    return dict(zip(columns, row, strict=False))


def _fetch_all_dicts(sql: str, params: tuple = ()) -> list[dict]:
    cursor = db.conn.execute(sql, params)
    rows = cursor.fetchall()
    if not rows:
        return []
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _get_stock_name_map(symbols: list[str]) -> dict[str, str]:
    result = {}
    for symbol in symbols:
        stock = stock_service.get_stock(symbol)
        if stock:
            result[symbol] = stock.name
    if len(result) == len(symbols):
        return result
    if not stock_service.get_all_stocks():
        stock_service.update_stock_list()
    for symbol in symbols:
        if symbol in result:
            continue
        stock = stock_service.get_stock(symbol)
        if stock:
            result[symbol] = stock.name
    return result


def _normalize_order_status(status: str) -> str:
    value = str(status or "").strip().lower()
    aliases = {
        "48": "unreported",
        "49": "pending",
        "50": "reported",
        "51": "canceling",
        "52": "partial_canceling",
        "53": "partial_cancelled",
        "54": "cancelled",
        "55": "partial",
        "56": "filled",
        "57": "rejected",
        "wait_reporting": "pending",
        "reported_cancel": "canceling",
        "partsucc_cancel": "partial_canceling",
        "part_cancel": "partial_cancelled",
        "part_succ": "partial",
        "succeeded": "filled",
        "junk": "rejected",
    }
    return aliases.get(value, value or "unknown")


def _is_order_cancellable(status: str) -> bool:
    normalized = _normalize_order_status(status)
    return normalized in {
        "unreported",
        "pending",
        "reported",
        "canceling",
        "partial_canceling",
        "partial",
    }


def _get_latest_asset(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> Asset | None:
    row = _fetch_one_dict(
        "select * from assets where portfolio_id = ? order by dt desc limit 1",
        (portfolio_id,),
    )
    if row is None:
        return None
    return Asset.from_dict(row)


def _get_latest_positions(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> list[Position]:
    rows = _fetch_all_dicts(
        """
        select * from positions
        where portfolio_id = ?
          and dt = (
              select dt from positions
              where portfolio_id = ?
              order by dt desc
              limit 1
          )
        """,
        (portfolio_id, portfolio_id),
    )
    return [Position.from_dict(row) for row in rows]


def _snapshot_asset(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> Asset | None:
    live = trade_service.get_asset()
    if not live:
        return None
    today = datetime.date.today()
    asset = Asset(
        portfolio_id=portfolio_id,
        dt=today,
        principal=_as_float(_get_value(live, "total", 0)),
        cash=_as_float(_get_value(live, "cash", 0)),
        frozen_cash=_as_float(_get_value(live, "frozen_cash", 0)),
        market_value=_as_float(_get_value(live, "market_value", 0)),
        total=_as_float(_get_value(live, "total", 0)),
    )
    db["assets"].upsert(asset.to_dict(), pk=Asset.__pk__)
    return asset


def _snapshot_positions(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> None:
    rows = trade_service.get_positions()
    today = datetime.date.today()
    if rows is None:
        return
    db.conn.execute(
        "delete from positions where portfolio_id = ? and dt = ?",
        (portfolio_id, today),
    )
    for row in rows:
        symbol = str(_get_value(row, "symbol", "")).strip()
        if not symbol:
            continue
        shares = _as_float(_get_value(row, "shares", 0))
        if shares <= 0:
            continue
        price = _as_float(_get_value(row, "cost", _get_value(row, "price", 0)))
        position = Position(
            portfolio_id=portfolio_id,
            dt=today,
            asset=symbol,
            shares=shares,
            avail=_as_float(_get_value(row, "avail", 0)),
            price=price,
            profit=_as_float(_get_value(row, "profit", 0)),
            mv=_as_float(_get_value(row, "market_value", 0)),
        )
        db["positions"].upsert(position.to_dict(), pk=Position.__pk__)


def get_latest_asset_data(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> dict:
    asset = _get_latest_asset(portfolio_id)
    if asset is None:
        asset = _snapshot_asset(portfolio_id)
    if asset is None:
        return {
            "principal": 0,
            "total": 0,
            "cash": 0,
            "market_value": 0,
            "frozen_cash": 0,
        }
    return {
        "principal": asset.principal,
        "total": asset.total,
        "cash": asset.cash,
        "market_value": asset.market_value,
        "frozen_cash": asset.frozen_cash,
    }


def get_latest_positions_data(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> list[dict]:
    _snapshot_positions(portfolio_id)
    positions = [p for p in _get_latest_positions(portfolio_id) if p.shares > 0]
    total = get_latest_asset_data(portfolio_id).get("total", 0)
    name_map = _get_stock_name_map([p.asset for p in positions])
    data = []
    for p in positions:
        cost_price = _as_float(p.price)
        hold_cost = p.shares * cost_price
        current_price = quote_service.get_latest_price(p.asset)
        if current_price <= 0 and p.shares > 0:
            current_price = _as_float(p.mv) / p.shares
        market_value = p.shares * current_price if current_price > 0 else _as_float(p.mv)
        float_profit = market_value - hold_cost
        profit_ratio = (float_profit / hold_cost * 100) if hold_cost > 0 else 0
        position_ratio = (market_value / total * 100) if total > 0 else 0
        data.append(
            {
                "symbol": p.asset,
                "name": name_map.get(p.asset, p.asset),
                "shares": p.shares,
                "avail": p.avail,
                "price": current_price,
                "cost": cost_price,
                "profit_ratio": profit_ratio,
                "float_profit": float_profit,
                "market_value": market_value,
                "hold_cost": hold_cost,
                "sell_profit": 0,
                "position_ratio": position_ratio,
            }
        )
    return data


def get_latest_orders_data(status: str = "all") -> list[dict]:
    orders = trade_service.get_orders()
    if not orders:
        data = _fetch_all_dicts(
            """
            select qtoid, foid, asset, side, price, shares, filled, status, tm
            from orders
            where portfolio_id = ?
            order by tm desc
            limit 200
            """,
            (DEFAULT_PORTFOLIO_ID,),
        )
        symbols = [str(_get_value(order, "asset", "")).strip() for order in data]
        name_map = _get_stock_name_map(symbols)
        rows = []
        for order in data:
            symbol = str(_get_value(order, "asset", "")).strip()
            side_value = _as_float(_get_value(order, "side", 1))
            normalized_status = _normalize_order_status(
                _get_value(order, "status", "unknown")
            )
            tm_raw = _get_value(order, "tm", "")
            time_text = ""
            if isinstance(tm_raw, datetime.datetime):
                time_text = tm_raw.strftime("%H:%M:%S")
            elif isinstance(tm_raw, str):
                try:
                    time_text = datetime.datetime.fromisoformat(tm_raw).strftime(
                        "%H:%M:%S"
                    )
                except ValueError:
                    time_text = tm_raw
            rows.append(
                {
                    "time": time_text,
                    "symbol": symbol,
                    "name": name_map.get(symbol, symbol),
                    "side": "buy" if side_value == 1 else "sell",
                    "price": _as_float(_get_value(order, "price", 0)),
                    "shares": _as_float(_get_value(order, "shares", 0)),
                    "filled": _as_float(_get_value(order, "filled", 0)),
                    "status": normalized_status,
                    "qtoid": str(
                        _get_value(order, "foid", "")
                        or _get_value(order, "qtoid", "")
                    ),
                    "can_cancel": _is_order_cancellable(normalized_status),
                }
            )
        if status == "all":
            return rows
        target = _normalize_order_status(status)
        return [row for row in rows if row["status"] == target]

    symbols = [str(_get_value(o, "symbol", "")).strip() for o in orders]
    name_map = _get_stock_name_map(symbols)
    data = []
    for order in orders:
        symbol = str(_get_value(order, "symbol", "")).strip()
        normalized_status = _normalize_order_status(_get_value(order, "status", "unknown"))
        row = {
            "time": _get_value(order, "time", ""),
            "symbol": symbol,
            "name": _get_value(order, "name", "") or name_map.get(symbol, symbol),
            "side": _get_value(order, "side", "buy"),
            "price": _as_float(_get_value(order, "price", 0)),
            "shares": _as_float(_get_value(order, "shares", 0)),
            "filled": _as_float(_get_value(order, "filled", 0)),
            "status": normalized_status,
            "qtoid": _get_value(order, "qtoid", ""),
            "can_cancel": _is_order_cancellable(normalized_status),
        }
        data.append(row)
    if status == "all":
        return data
    target = _normalize_order_status(status)
    return [row for row in data if row["status"] == target]


def login_required(request):
    """检查用户是否登录"""
    user = request.scope.get("session", {}).get("user")
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    return user


def register_routes(app):
    """注册交易路由"""

    @app.on_event("startup")
    async def startup_event():
        """启动时连接交易接口"""
        try:
            if config.qmt_account_id and config.qmt_path:
                success = trade_service.connect(
                    account_id=config.qmt_account_id,
                    qmt_path=str(config.qmt_path),
                )
                if success:
                    logger.info("交易接口已连接")
                else:
                    logger.warning("交易接口连接失败")
        except Exception as e:
            logger.error(f"启动时连接交易接口失败: {e}")

    @app.on_event("shutdown")
    async def shutdown_event():
        """关闭时断开交易接口"""
        trade_service.disconnect()

    @app.get("/api/trade/asset")
    def get_asset(request):
        """获取账户资金"""
        login_required(request)
        return get_latest_asset_data()

    @app.get("/api/trade/positions")
    def get_positions(request, view: str = "json"):
        """获取持仓列表

        Args:
            view: 返回格式，json 或 table
        """
        login_required(request)
        positions_data = get_latest_positions_data()

        if view == "table":
            from qmt_gateway.web.pages.trading import PositionTable
            return PositionTable(positions_data)

        return positions_data

    @app.get("/api/trade/orders")
    def get_orders(request, status: str = "all", view: str = "json"):
        """获取订单列表

        Args:
            status: 订单状态过滤
            view: 返回格式，json 或 table
        """
        login_required(request)
        orders_data = get_latest_orders_data(status)

        if view == "table":
            from qmt_gateway.web.pages.trading import OrdersTable
            return OrdersTable(orders_data)

        return orders_data

    @app.get("/api/trade/trades")
    def get_trades(request):
        """获取成交列表"""
        login_required(request)
        trades = trade_service.get_trades()
        return [
            {
                "time": _get_value(t, "time", ""),
                "symbol": _get_value(t, "symbol", ""),
                "name": _get_value(t, "name", ""),
                "side": _get_value(t, "side", "buy"),
                "price": _as_float(_get_value(t, "price", 0)),
                "shares": _as_float(_get_value(t, "shares", 0)),
                "amount": _as_float(_get_value(t, "amount", 0)),
            }
            for t in trades
        ]

    @app.post("/api/trade/buy")
    def buy_stock(request, symbol: str, price: float, shares: int):
        """买入股票"""
        login_required(request)
        result = trade_service.buy(symbol, price, shares)
        return result

    @app.post("/api/trade/sell")
    def sell_stock(request, symbol: str, price: float, shares: int):
        """卖出股票"""
        login_required(request)
        result = trade_service.sell(symbol, price, shares)
        return result

    @app.post("/api/trade/cancel")
    def cancel_order(request, order_id: str, view: str = "json"):
        """撤单"""
        login_required(request)
        result = trade_service.cancel_order(order_id)
        if view == "table":
            from qmt_gateway.web.pages.trading import OrdersTable
            return OrdersTable(get_latest_orders_data())
        return result

    @app.post("/api/asset/principal")
    def update_principal(request, principal: float):
        """修改本金

        修改本金会在 asset 表中插入一条新的记录。
        如果当天已有记录，则更新该记录的本金字段。

        Args:
            principal: 新本金金额

        Returns:
            JSONResponse: 操作结果
        """
        user = login_required(request)

        if principal <= 0:
            return JSONResponse(
                {"code": 1, "message": "本金金额必须大于0"},
                status_code=400
            )

        try:
            portfolio_id = DEFAULT_PORTFOLIO_ID
            existing = _get_latest_asset(portfolio_id)

            if existing:
                existing.principal = principal
                db["assets"].upsert(existing.to_dict(), pk=Asset.__pk__)
                logger.info(
                    f"更新本金: portfolio_id={portfolio_id}, dt={existing.dt}, principal={principal}"
                )
            else:
                today = datetime.date.today()
                current_asset = get_latest_asset_data(portfolio_id)
                new_asset = Asset(
                    portfolio_id=portfolio_id,
                    dt=today,
                    principal=principal,
                    cash=_as_float(current_asset.get("cash", 0)),
                    frozen_cash=_as_float(current_asset.get("frozen_cash", 0)),
                    market_value=_as_float(current_asset.get("market_value", 0)),
                    total=_as_float(current_asset.get("total", principal), principal),
                )

                db.insert_asset(new_asset)
                logger.info(
                    f"插入新本金记录: portfolio_id={portfolio_id}, dt={today}, principal={principal}"
                )

            latest_asset = _get_latest_asset(portfolio_id)
            latest_date = latest_asset.dt if latest_asset else datetime.date.today()
            return JSONResponse({
                "code": 0,
                "message": "本金修改成功",
                "data": {
                    "principal": principal,
                    "date": latest_date.isoformat(),
                }
            })

        except Exception as e:
            logger.error(f"修改本金失败: {e}")
            return JSONResponse(
                {"code": 1, "message": f"修改本金失败: {str(e)}"},
                status_code=500
            )
