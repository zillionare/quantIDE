"""定时任务调度服务

管理定时任务，如板块数据同步、历史行情更新等。
"""

from loguru import logger

from qmt_gateway.runtime import runtime
from qmt_gateway.services.sector_subscription import sector_subscription
from qmt_gateway.services.sector_sync import sector_sync


class SchedulerService:
    """定时任务调度服务"""

    def __init__(self):
        self._started = False

    def start(self) -> None:
        """启动定时任务"""
        if self._started:
            return

        # 确保运行时已初始化
        runtime.start_scheduler()

        # 添加定时任务
        self._add_jobs()

        self._started = True
        logger.info("定时任务调度服务已启动")

    def stop(self) -> None:
        """停止定时任务"""
        if not self._started:
            return

        runtime.stop_scheduler()
        self._started = False
        logger.info("定时任务调度服务已停止")

    def _add_jobs(self) -> None:
        """添加定时任务"""
        # 每天 9:00 清空板块订阅（开盘前）
        runtime.add_job(
            self._clear_sector_subscriptions_job,
            "cron",
            hour=9,
            minute=0,
            id="clear_sector_subscriptions",
            replace_existing=True,
        )

        # 每天 9:20 同步板块列表和成分股
        runtime.add_job(
            self._sync_sectors_job,
            "cron",
            hour=9,
            minute=20,
            id="sync_sectors",
            replace_existing=True,
        )

        # 每天 16:00 同步板块历史行情（盘后）
        runtime.add_job(
            self._sync_bars_job,
            "cron",
            hour=16,
            minute=0,
            id="sync_bars",
            replace_existing=True,
        )

        logger.info("定时任务已添加: 9:00 清空订阅, 9:20 同步板块, 16:00 同步行情")

    def _clear_sector_subscriptions_job(self) -> None:
        """清空板块订阅任务"""
        try:
            logger.info("开始清空板块订阅")
            count = sector_subscription.clear_all()
            logger.info(f"已清空 {count} 个板块订阅")
        except Exception as e:
            logger.error(f"清空板块订阅任务失败: {e}")

    def _sync_sectors_job(self) -> None:
        """同步板块任务"""
        try:
            logger.info("开始定时同步板块数据")
            result = sector_sync.sync_sectors()
            logger.info(f"板块同步完成: {result}")
        except Exception as e:
            logger.error(f"板块同步任务失败: {e}")

    def _sync_bars_job(self) -> None:
        """同步行情任务"""
        try:
            logger.info("开始定时同步板块行情")
            result = sector_sync.sync_sector_bars()
            logger.info(f"行情同步完成: {result}")
        except Exception as e:
            logger.error(f"行情同步任务失败: {e}")

    def is_started(self) -> bool:
        """检查服务是否已启动"""
        return self._started


# 全局服务实例
scheduler = SchedulerService()
