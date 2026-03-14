"""板块订阅 API 模块

提供板块实时行情的订阅和取消订阅功能。
"""

from fasthtml.common import *
from loguru import logger

from qmt_gateway.apis.auth import login_required
from qmt_gateway.db import db
from qmt_gateway.services.sector_subscription import sector_subscription


def register_sector_subscription_routes(app):
    """注册板块订阅 API 路由"""

    @app.post("/api/sectors/{sector_id}/subscribe")
    @login_required
    def subscribe_sector(sector_id: str, request):
        """订阅板块实时行情

        Args:
            sector_id: 板块代码

        Returns:
            JSON 响应
        """
        try:
            # 检查板块是否存在
            sector = db.get_sector(sector_id)
            if not sector:
                return JSONResponse(
                    {"success": False, "error": f"板块不存在: {sector_id}"},
                    status_code=404,
                )

            # 订阅板块
            success = sector_subscription.subscribe(sector_id)
            if success:
                return JSONResponse(
                    {
                        "success": True,
                        "message": f"已订阅板块: {sector.name}",
                        "sector_id": sector_id,
                        "sector_name": sector.name,
                    }
                )
            else:
                return JSONResponse(
                    {
                        "success": True,
                        "message": f"板块已在订阅列表中: {sector.name}",
                        "sector_id": sector_id,
                        "sector_name": sector.name,
                    }
                )

        except Exception as e:
            logger.error(f"订阅板块失败: {e}")
            return JSONResponse(
                {"success": False, "error": str(e)},
                status_code=500,
            )

    @app.post("/api/sectors/{sector_id}/unsubscribe")
    @login_required
    def unsubscribe_sector(sector_id: str, request):
        """取消订阅板块实时行情

        Args:
            sector_id: 板块代码

        Returns:
            JSON 响应
        """
        try:
            success = sector_subscription.unsubscribe(sector_id)
            if success:
                return JSONResponse(
                    {
                        "success": True,
                        "message": f"已取消订阅板块: {sector_id}",
                        "sector_id": sector_id,
                    }
                )
            else:
                return JSONResponse(
                    {
                        "success": False,
                        "error": f"板块未在订阅列表中: {sector_id}",
                        "sector_id": sector_id,
                    },
                    status_code=400,
                )

        except Exception as e:
            logger.error(f"取消订阅板块失败: {e}")
            return JSONResponse(
                {"success": False, "error": str(e)},
                status_code=500,
            )

    @app.get("/api/sectors/subscriptions")
    @login_required
    def get_subscriptions(request):
        """获取当前订阅的板块列表

        Returns:
            JSON 响应，包含订阅的板块列表
        """
        try:
            subscribed_ids = sector_subscription.get_subscribed_sectors()
            
            # 获取板块详细信息
            sectors = []
            for sector_id in subscribed_ids:
                sector = db.get_sector(sector_id)
                if sector:
                    sectors.append({
                        "id": sector.id,
                        "name": sector.name,
                        "sector_type": sector.sector_type,
                    })

            return JSONResponse(
                {
                    "success": True,
                    "count": len(sectors),
                    "sectors": sectors,
                }
            )

        except Exception as e:
            logger.error(f"获取订阅列表失败: {e}")
            return JSONResponse(
                {"success": False, "error": str(e)},
                status_code=500,
            )

    @app.delete("/api/sectors/subscriptions")
    @login_required
    def clear_all_subscriptions(request):
        """清空所有板块订阅

        Returns:
            JSON 响应
        """
        try:
            count = sector_subscription.clear_all()
            return JSONResponse(
                {
                    "success": True,
                    "message": f"已清空所有订阅，共 {count} 个板块",
                    "count": count,
                }
            )

        except Exception as e:
            logger.error(f"清空订阅失败: {e}")
            return JSONResponse(
                {"success": False, "error": str(e)},
                status_code=500,
            )
