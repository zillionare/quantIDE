"""运行时生命周期管理模块

管理应用的初始化、启动和关闭流程。
"""

import datetime
import os
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

from qmt_gateway import setup_logging
from qmt_gateway.config import config
from qmt_gateway.core import add_xtquant_path
from qmt_gateway.db import db


class Runtime:
    """运行时管理器

    管理应用的生命周期，包括：
    - 初始化配置和数据库
    - 设置日志
    - 启动定时任务
    - 管理 QMT 连接
    """

    def __init__(self):
        self._initialized = False
        self._scheduler: BackgroundScheduler | None = None
        self._home_path: Path | None = None

    def _get_home_path(self) -> Path:
        """获取数据主目录"""
        if self._home_path is None:
            home = os.getenv("QMT_GATEWAY_HOME", "~/.qmt-gateway")
            self._home_path = Path(home).expanduser()
            self._home_path.mkdir(parents=True, exist_ok=True)
        return self._home_path

    def init(self, home_path: str | Path | None = None) -> None:
        """初始化运行时环境

        Args:
            home_path: 数据主目录，默认 ~/.qmt-gateway
        """
        if self._initialized:
            return

        # 设置主目录
        if home_path:
            self._home_path = Path(home_path).expanduser()
            self._home_path.mkdir(parents=True, exist_ok=True)
        else:
            self._home_path = self._get_home_path()

        # 初始化数据库
        db_path = self._home_path / "data" / "app.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db.init(db_path)
        logger.info(f"数据库初始化完成: {db_path}")

        # 重新加载配置
        config.reload()

        # 设置日志
        log_path = config.log_path
        setup_logging(
            log_path=log_path / "qmt-gateway.log",
            rotation=config.log_rotation,
            retention=config.log_retention,
        )

        # 添加 xtquant 路径
        if config.xtquant_path or config.qmt_path:
            add_xtquant_path(
                xtquant_path=str(config.xtquant_path) if config.xtquant_path else None,
                qmt_path=str(config.qmt_path) if config.qmt_path else None,
            )

        self._initialized = True
        logger.info(f"运行时初始化完成，数据目录: {self._home_path}")

    def start_scheduler(self) -> None:
        """启动定时任务调度器"""
        if self._scheduler is None:
            self._scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

        if not self._scheduler.running:
            self._scheduler.start()
            logger.info("定时任务调度器已启动")

    def stop_scheduler(self) -> None:
        """停止定时任务调度器"""
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown()
            logger.info("定时任务调度器已停止")

    def add_job(self, *args, **kwargs) -> None:
        """添加定时任务"""
        if self._scheduler is None:
            self._scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        self._scheduler.add_job(*args, **kwargs)

    def is_initialized(self) -> bool:
        """检查是否已初始化"""
        return self._initialized

    def require_init(self) -> None:
        """要求已初始化，否则抛出异常"""
        if not self._initialized:
            raise RuntimeError("运行时未初始化，请先调用 init()")

    @property
    def home_path(self) -> Path:
        """数据主目录"""
        self.require_init()
        return self._home_path

    @property
    def scheduler(self) -> BackgroundScheduler | None:
        """定时任务调度器"""
        return self._scheduler


# 全局运行时实例
runtime = Runtime()


def ensure_ready(home_path: str | Path | None = None) -> None:
    """确保运行时已就绪

    如果未初始化，则自动初始化。

    Args:
        home_path: 数据主目录
    """
    if not runtime.is_initialized():
        runtime.init(home_path)
