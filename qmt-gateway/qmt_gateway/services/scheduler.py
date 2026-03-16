"""定时任务调度器

提供定时更新股票列表等功能。
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from qmt_gateway.services.stock_service import stock_service


class TaskScheduler:
    """任务调度器"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self._initialized = False

    def start(self):
        """启动调度器"""
        if self._initialized:
            return

        # 每天早上 9:00 更新股票列表（开盘前）
        self.scheduler.add_job(
            func=self._update_stock_list,
            trigger=CronTrigger(hour=9, minute=0),
            id="update_stock_list",
            name="更新股票列表",
            replace_existing=True,
        )

        # 启动时立即更新一次
        self._update_stock_list()

        self.scheduler.start()
        self._initialized = True
        logger.info("定时任务调度器已启动")

    def init_scheduler(self):
        """初始化调度器（兼容旧接口）"""
        self.start()

    def _update_stock_list(self):
        """更新股票列表任务"""
        logger.info("开始执行定时任务：更新股票列表")
        try:
            success = stock_service.update_stock_list()
            if success:
                logger.info("定时任务完成：股票列表更新成功")
            else:
                logger.warning("定时任务失败：股票列表更新失败")
        except Exception as e:
            logger.error(f"定时任务异常：{e}")

    def stop(self):
        """停止调度器"""
        if self._initialized:
            self.scheduler.shutdown()
            self._initialized = False
            logger.info("定时任务调度器已关闭")

    def shutdown(self):
        """关闭调度器（兼容旧接口）"""
        self.stop()


# 全局调度器实例
scheduler = TaskScheduler()
