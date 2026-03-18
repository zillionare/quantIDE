"""应用程序全局状态模型

用于存储初始化标志、全局配置等应用级状态数据。
"""

import datetime
from dataclasses import dataclass, field

from pyqmt.data.models.base import Entity


@dataclass
class AppState(Entity):
    """应用程序全局状态

    单例表设计，只存储一条记录（id=1）。
    用于管理初始化状态、全局配置等。
    """

    __table_name__ = "app_state"
    __pk__ = "id"
    __indexes__ = ([], False)  # 无索引

    id: int = 1  # 固定为1，单例表

    # ========== 初始化相关标志 ==========
    init_completed: bool = False
    """初始化是否完成"""

    init_started_at: datetime.datetime | None = None
    """初始化开始时间"""

    init_completed_at: datetime.datetime | None = None
    """初始化完成时间"""

    init_version: int = 1
    """初始化版本，用于未来升级"""

    init_step: int = 0
    """当前初始化步骤（0-5），用于意外中断后恢复"""

    # ========== 数据源配置 ==========
    tushare_token: str = ""
    """Tushare API Token"""

    # ========== 任务调度配置 ==========
    daily_fetch_time: str = "16:00"
    """日线数据获取时间，格式 HH:MM"""

    limit_refresh_time: str = "09:00"
    """涨跌停刷新时间，格式 HH:MM"""

    adj_factor_time: str = "09:20"
    """复权因子获取时间，格式 HH:MM"""

    index_sync_time: str = "19:30"
    """指数数据同步时间，格式 HH:MM"""

    # ========== 历史数据下载配置 ==========
    history_start_date: datetime.date | None = None
    """历史数据起始日期"""

    # ========== 元数据 ==========
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    """记录创建时间"""

    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    """记录更新时间"""

    def __post_init__(self):
        if isinstance(self.init_started_at, str):
            self.init_started_at = datetime.datetime.fromisoformat(
                self.init_started_at
            )
        if isinstance(self.init_completed_at, str):
            self.init_completed_at = datetime.datetime.fromisoformat(
                self.init_completed_at
            )
        if isinstance(self.history_start_date, str):
            self.history_start_date = datetime.datetime.strptime(
                self.history_start_date, "%Y-%m-%d"
            ).date()
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)

    def to_dict(self) -> dict:
        """转换为字典，用于数据库存储"""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime.datetime):
                result[key] = value.isoformat()
            elif isinstance(value, datetime.date):
                result[key] = value.strftime("%Y-%m-%d")
            else:
                result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "AppState":
        """从字典创建实例"""
        # 过滤掉类中不存在的字段
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)

    @property
    def is_fully_initialized(self) -> bool:
        """检查是否完全初始化

        Returns:
            bool: True 表示已完成初始化
        """
        return self.init_completed and self.init_step >= 5

    def can_use_live_trading(self) -> bool:
        """检查是否可以使用实盘/仿真交易功能

        Returns:
            bool: True 表示可以使用实盘/仿真功能
        """
        return self.is_fully_initialized

    def can_use_backtest(self) -> bool:
        """检查是否可以使用回测功能

        Returns:
            bool: True 表示可以使用回测功能
        """
        return self.is_fully_initialized and bool(self.tushare_token)
