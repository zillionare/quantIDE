"""板块 API

提供板块列表、成分股和历史行情查询功能。
"""

import datetime
from typing import Any

from fasthtml.common import *
from loguru import logger

from qmt_gateway.db import db
from qmt_gateway.services.sector_sync import sector_sync


def register_routes(app):
    """注册板块路由"""

    @app.get("/api/v1/sectors")
    def list_sectors(sector_type: str | None = None, trade_date: str | None = None):
        """获取板块列表"""
        try:
            date = datetime.date.fromisoformat(trade_date) if trade_date else datetime.date.today()
            sectors = db.list_sectors(sector_type=sector_type, trade_date=date)

            return {
                "success": True,
                "data": [s.to_dict() for s in sectors],
            }
        except Exception as e:
            logger.error(f"获取板块列表失败: {e}")
            return {
                "success": False,
                "message": str(e),
            }

    @app.get("/api/v1/sectors/{sector_id}/constituents")
    def get_constituents(sector_id: str, trade_date: str | None = None):
        """获取板块成分股"""
        try:
            date = datetime.date.fromisoformat(trade_date) if trade_date else datetime.date.today()
            constituents = db.get_sector_constituents(sector_id, date)

            return {
                "success": True,
                "data": [c.to_dict() for c in constituents],
            }
        except Exception as e:
            logger.error(f"获取成分股失败: {e}")
            return {
                "success": False,
                "message": str(e),
            }

    @app.get("/api/v1/sectors/{sector_id}/bars")
    def get_bars(
        sector_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        """获取板块历史行情"""
        try:
            end = datetime.date.fromisoformat(end_date) if end_date else datetime.date.today()
            start = datetime.date.fromisoformat(start_date) if start_date else (end - datetime.timedelta(days=365))

            rows = db["sector_bars"].rows_where(
                "sector_id = ? AND dt >= ? AND dt <= ?",
                (sector_id, start, end),
            )

            bars = [dict(row) for row in rows]

            return {
                "success": True,
                "data": bars,
            }
        except Exception as e:
            logger.error(f"获取行情失败: {e}")
            return {
                "success": False,
                "message": str(e),
            }

    @app.post("/api/v1/sectors/sync")
    def sync_sectors():
        """手动触发板块同步"""
        try:
            result = sector_sync.sync_all()
            return {
                "success": True,
                "data": result,
            }
        except Exception as e:
            logger.error(f"板块同步失败: {e}")
            return {
                "success": False,
                "message": str(e),
            }
