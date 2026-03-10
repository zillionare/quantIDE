"""应用程序初始化向导服务

提供初始化状态管理、配置保存、历史数据下载等功能。
"""

import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from pyqmt.data.models.app_state import AppState
from pyqmt.data.sqlite import db


class InitWizardService:
    """初始化向导服务

    管理应用程序的初始化流程，包括：
    1. 初始化状态检查
    2. 配置保存和读取
    3. 历史数据下载
    4. 初始化完成标志管理
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._state: AppState | None = None
        self._initialized = True

    def _ensure_db(self):
        """确保数据库已初始化"""
        if not db._initialized:
            raise RuntimeError("数据库未初始化，请先调用 db.init()")

    def get_state(self, force_refresh: bool = False) -> AppState:
        """获取当前应用状态

        Args:
            force_refresh: 是否强制从数据库刷新，忽略缓存

        Returns:
            AppState: 应用状态对象（如果不存在则返回默认状态）
        """
        self._ensure_db()

        if not force_refresh and self._state is not None:
            return self._state

        try:
            row = db["app_state"].get(1)
            if row:
                self._state = AppState.from_dict(dict(row))
                return self._state
        except Exception as e:
            logger.warning(f"读取应用状态失败: {e}")

        # 返回默认状态
        self._state = AppState()
        return self._state

    def save_state(self, state: AppState | None = None) -> None:
        """保存应用状态

        Args:
            state: 要保存的状态，None 则保存当前缓存的状态
        """
        self._ensure_db()

        if state is not None:
            self._state = state

        if self._state is None:
            self._state = AppState()

        self._state.updated_at = datetime.datetime.now()

        try:
            db["app_state"].upsert(self._state.to_dict(), pk="id")
            # 强制提交，确保其他连接能看到更新
            db.conn.commit()
            logger.info("应用状态已保存")
        except Exception as e:
            logger.error(f"保存应用状态失败: {e}")
            raise

    def is_initialized(self) -> bool:
        """检查应用是否已完成初始化

        Returns:
            bool: True 表示已完成初始化
        """
        state = self.get_state()
        return state.is_fully_initialized

    def start_initialization(self) -> AppState:
        """开始初始化流程

        Returns:
            AppState: 当前状态对象
        """
        state = self.get_state()
        state.init_started_at = datetime.datetime.now()
        state.init_step = 1
        self.save_state(state)
        logger.info("初始化流程开始")
        return state

    def update_step(self, step: int) -> None:
        """更新当前初始化步骤

        Args:
            step: 当前步骤（1-5）
        """
        state = self.get_state()
        state.init_step = step
        self.save_state(state)
        logger.info(f"初始化步骤更新为: {step}")

    def save_data_source_config(
        self,
        tushare_token: str,
        qmt_account_id: str,
        qmt_account_type: str,
        qmt_path: str,
    ) -> None:
        """保存数据源配置

        Args:
            tushare_token: Tushare API Token
            qmt_account_id: QMT账号ID
            qmt_account_type: 账号类型（simulation/live）
            qmt_path: QMT安装路径
        """
        state = self.get_state()
        state.tushare_token = tushare_token
        state.qmt_account_id = qmt_account_id
        state.qmt_account_type = qmt_account_type
        state.qmt_path = qmt_path
        self.save_state(state)
        logger.info("数据源配置已保存")

    def save_schedule_config(
        self,
        daily_fetch_time: str = "16:00",
        limit_refresh_time: str = "09:00",
        adj_factor_time: str = "09:20",
        sector_sync_time: str = "19:00",
        index_sync_time: str = "19:30",
    ) -> None:
        """保存任务调度配置

        Args:
            daily_fetch_time: 日线数据获取时间
            limit_refresh_time: 涨跌停刷新时间
            adj_factor_time: 复权因子获取时间
            sector_sync_time: 板块数据同步时间
            index_sync_time: 指数数据同步时间
        """
        state = self.get_state()
        state.daily_fetch_time = daily_fetch_time
        state.limit_refresh_time = limit_refresh_time
        state.adj_factor_time = adj_factor_time
        state.sector_sync_time = sector_sync_time
        state.index_sync_time = index_sync_time
        self.save_state(state)
        logger.info("任务调度配置已保存")

    def save_history_config(self, start_date: datetime.date) -> None:
        """保存历史数据下载配置

        Args:
            start_date: 历史数据起始日期
        """
        state = self.get_state()
        state.history_start_date = start_date
        self.save_state(state)
        logger.info(f"历史数据配置已保存: 从 {start_date} 开始")

    def complete_initialization(self) -> None:
        """完成初始化流程"""
        state = self.get_state()
        state.init_completed = True
        state.init_completed_at = datetime.datetime.now()
        state.init_step = 5
        self.save_state(state)
        logger.info("初始化流程完成")

    def get_progress(self) -> dict[str, Any]:
        """获取初始化进度信息

        Returns:
            包含进度信息的字典
        """
        state = self.get_state()

        steps = [
            {"id": 1, "name": "欢迎", "completed": state.init_step > 1},
            {"id": 2, "name": "数据源配置", "completed": state.init_step > 2},
            {"id": 3, "name": "任务调度", "completed": state.init_step > 3},
            {"id": 4, "name": "下载数据", "completed": state.init_step > 4},
            {"id": 5, "name": "完成", "completed": state.init_step >= 5},
        ]

        return {
            "current_step": state.init_step,
            "total_steps": 5,
            "init_completed": state.init_completed,
            "steps": steps,
            "started_at": state.init_started_at,
            "completed_at": state.init_completed_at,
        }

    def reset_initialization(self) -> None:
        """重置初始化状态（用于重新初始化）"""
        state = AppState()  # 创建全新的默认状态
        self.save_state(state)
        logger.warning("初始化状态已重置")


# 全局服务实例
init_wizard = InitWizardService()
