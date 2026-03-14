from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

from pyqmt.config import cfg
from pyqmt.core.singleton import singleton


@singleton
class SchedulerManager:
    def __init__(self):
        self._scheduler = None
        self._is_running = False

    def init(self, timezone=None):
        if self._scheduler is not None:
            return

        tz = timezone or getattr(cfg, "TIMEZONE", None)
        # 默认使用 BackgroundScheduler，因项目多为同步/混合调用
        self._scheduler = BackgroundScheduler(timezone=tz)
        logger.info(f"Scheduler initialized with timezone: {tz}")

    @property
    def scheduler(self):
        if self._scheduler is None:
            self.init()
        return self._scheduler

    def start(self):
        if self._scheduler and not self._is_running:
            self._scheduler.start()
            self._is_running = True
            logger.info("Scheduler started")

    def stop(self):
        if self._scheduler and self._is_running:
            self._scheduler.shutdown()
            self._is_running = False
            logger.info("Scheduler stopped")

    def add_job(self, func, trigger=None, args=None, kwargs=None, id=None, name=None, **trigger_args):
        """代理 add_job 方法"""
        return self.scheduler.add_job(func, trigger=trigger, args=args, kwargs=kwargs, id=id, name=name, **trigger_args)

    def add_listener(self, callback, mask=None):
        """代理 add_listener 方法"""
        return self.scheduler.add_listener(callback, mask)

scheduler = SchedulerManager()
