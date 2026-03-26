"""数据同步定时任务配置

使用 APScheduler 实现每日数据同步。

本模块仅保留为本地 xtdata 兼容工具，不属于发布态正式路径。
"""

import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from pyqmt.config import cfg
from pyqmt.core.legacy_qmt import ensure_legacy_local_qmt_enabled
from pyqmt.data.dal.index_dal import IndexDAL
from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.services.index_sync import IndexSyncService
from pyqmt.data.services.sector_sync import SectorSyncService
from pyqmt.data.sqlite import db


class DataSyncScheduler:
    """数据同步调度器"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.scheduler = BackgroundScheduler()
        self._initialized = True
        self._jobs_started = False

    def init_db(self, db_path: str | None = None):
        """初始化数据库连接

        Args:
            db_path: 数据库路径，默认从配置获取
        """
        if db_path is None:
            db_path = getattr(cfg, "db_path", "~/.quantide/data/quantide.db")

        db_path = Path(db_path).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db.init(str(db_path))

    def _create_services(self):
        """创建同步服务实例"""
        sector_dal = SectorDAL(db)
        index_dal = IndexDAL(db)

        self.sector_sync = SectorSyncService(sector_dal)
        self.index_sync = IndexSyncService(index_dal)

    def sync_sectors_job(self):
        """板块同步任务"""
        logger.info("执行板块数据同步任务...")
        try:
            result = self.sector_sync.sync_daily()
            logger.info(f"板块数据同步完成: {result}")
        except Exception as e:
            logger.error(f"板块数据同步失败: {e}")

    def sync_indices_job(self):
        """指数同步任务"""
        logger.info("执行指数数据同步任务...")
        try:
            result = self.index_sync.sync_daily()
            logger.info(f"指数数据同步完成: {result}")
        except Exception as e:
            logger.error(f"指数数据同步失败: {e}")

    def setup_jobs(
        self,
        sector_hour: int = 19,
        sector_minute: int = 0,
        index_hour: int = 19,
        index_minute: int = 30,
    ):
        """配置定时任务

        Args:
            sector_hour: 板块同步小时（默认19点）
            sector_minute: 板块同步分钟（默认0分）
            index_hour: 指数同步小时（默认19点）
            index_minute: 指数同步分钟（默认30分）
        """
        # 板块同步任务 - 每天收盘后
        self.scheduler.add_job(
            self.sync_sectors_job,
            trigger=CronTrigger(hour=sector_hour, minute=sector_minute),
            id="sync_sectors",
            name="板块数据同步",
            replace_existing=True,
        )

        # 指数同步任务 - 每天收盘后（板块同步之后）
        self.scheduler.add_job(
            self.sync_indices_job,
            trigger=CronTrigger(hour=index_hour, minute=index_minute),
            id="sync_indices",
            name="指数数据同步",
            replace_existing=True,
        )

        logger.info(
            f"定时任务配置完成: "
            f"板块同步 {sector_hour:02d}:{sector_minute:02d}, "
            f"指数同步 {index_hour:02d}:{index_minute:02d}"
        )

    def start(self):
        """启动调度器"""
        if self._jobs_started:
            return

        ensure_legacy_local_qmt_enabled(
            "本地 xtdata 数据同步调度",
            "远程数据服务或非 xtquant 数据源",
        )

        self.init_db()
        self._create_services()
        self.setup_jobs()

        self.scheduler.start()
        self._jobs_started = True
        logger.info("数据同步调度器已启动")

    def shutdown(self):
        """关闭调度器"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("数据同步调度器已关闭")

    def run_once(self, full_history: bool = False):
        """立即执行一次同步

        Args:
            full_history: 是否执行全量历史同步
        """
        ensure_legacy_local_qmt_enabled(
            "本地 xtdata 数据同步调度",
            "远程数据服务或非 xtquant 数据源",
        )
        self.init_db()
        self._create_services()

        if full_history:
            logger.info("执行全量历史数据同步...")
            self.sector_sync.sync_full_history()
            self.index_sync.sync_full_history()
        else:
            logger.info("执行增量数据同步...")
            self.sector_sync.sync_daily()
            self.index_sync.sync_daily()


# 全局调度器实例
sync_scheduler = DataSyncScheduler()


def start_scheduler():
    """启动数据同步调度器"""
    sync_scheduler.start()


def stop_scheduler():
    """停止数据同步调度器"""
    sync_scheduler.shutdown()


def run_sync_once(full_history: bool = False):
    """立即执行一次数据同步

    Args:
        full_history: 是否执行全量历史同步
    """
    sync_scheduler.run_once(full_history)


def init_and_sync(db_path: str | None = None, full_history: bool = False):
    """初始化并执行同步

    首次启动时调用，下载历史数据。

    Args:
        db_path: 数据库路径
        full_history: 是否执行全量历史同步
    """
    ensure_legacy_local_qmt_enabled(
        "本地 xtdata 数据同步调度",
        "远程数据服务或非 xtquant 数据源",
    )
    scheduler = DataSyncScheduler()
    scheduler.init_db(db_path)
    scheduler._create_services()

    if full_history:
        logger.info("首次启动，执行全量历史数据同步...")
        result = scheduler.sector_sync.sync_full_history()
        logger.info(f"板块同步结果: {result}")
        result = scheduler.index_sync.sync_full_history()
        logger.info(f"指数同步结果: {result}")
    else:
        logger.info("执行增量数据同步...")
        result = scheduler.sector_sync.sync_daily()
        logger.info(f"板块同步结果: {result}")
        result = scheduler.index_sync.sync_daily()
        logger.info(f"指数同步结果: {result}")

    return scheduler
