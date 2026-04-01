"""应用程序全局状态模型

用于存储初始化标志、全局配置等应用级状态数据。
"""

import datetime
from dataclasses import dataclass, field

from quantide.core.init_wizard_steps import WIZARD_FINAL_STEP
from quantide.data.models.base import Entity


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
    """当前初始化步骤（0-6），用于意外中断后恢复"""

    # ========== 运行环境 ==========
    app_home: str = ""
    """应用 home 目录"""

    app_host: str = "0.0.0.0"
    """应用监听地址"""

    app_port: int = 8130
    """应用监听端口"""

    app_prefix: str = "/quantide"
    """应用 API 前缀"""

    # ========== 网关配置 ==========
    gateway_server: str = ""
    """网关服务地址"""

    gateway_port: int = 8000
    """网关服务端口"""

    gateway_base_url: str = ""
    """网关 HTTP 地址"""

    gateway_scheme: str = "http"
    """网关协议"""

    gateway_api_key: str = ""
    """网关 API Key"""

    gateway_username: str = ""
    """网关登录用户名"""

    gateway_password: str = ""
    """网关登录密码"""

    gateway_timeout: int = 10
    """网关请求超时秒数"""

    gateway_enabled: bool = False
    """是否启用 gateway（未启用则禁用仿真与实盘）"""

    livequote_mode: str = "gateway"
    """实时行情模式"""

    runtime_mode: str = "live"
    """运行模式"""

    runtime_market_adapter: str = ""
    """运行时行情适配器"""

    runtime_broker_adapter: str = ""
    """运行时交易适配器"""

    # ========== 通知配置（运行时使用，不由 init wizard 管理） ==========
    notify_dingtalk_access_token: str = ""
    """钉钉 access_token"""

    notify_dingtalk_secret: str = ""
    """钉钉 secret"""

    notify_dingtalk_keyword: str = ""
    """钉钉 keyword"""

    notify_mail_to: str = ""
    """邮件收件人"""

    notify_mail_from: str = ""
    """邮件发件人"""

    notify_mail_server: str = ""
    """邮件服务器"""

    # ========== 数据初始化 ==========
    epoch: datetime.date = field(default_factory=lambda: datetime.date(2005, 1, 1))
    """数据起始日期"""

    data_source: str = "tushare"
    """当前标准数据源适配器名称"""

    tushare_token: str = ""
    """Tushare API Token"""

    history_years: int = 3
    """首次历史数据下载年数"""

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
        if isinstance(self.epoch, str):
            self.epoch = datetime.datetime.strptime(
                self.epoch, "%Y-%m-%d"
            ).date()
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)
        if not isinstance(self.init_completed, bool):
            self.init_completed = str(self.init_completed).lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        if not isinstance(self.gateway_enabled, bool):
            self.gateway_enabled = str(self.gateway_enabled).lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        if not isinstance(self.app_port, int):
            try:
                self.app_port = int(self.app_port)
            except Exception:
                self.app_port = 8130
        if not isinstance(self.gateway_port, int):
            try:
                self.gateway_port = int(self.gateway_port)
            except Exception:
                self.gateway_port = 8000
        if not isinstance(self.gateway_timeout, int):
            try:
                self.gateway_timeout = max(1, int(self.gateway_timeout))
            except Exception:
                self.gateway_timeout = 10
        if not isinstance(self.history_years, int):
            try:
                self.history_years = max(1, int(self.history_years))
            except Exception:
                self.history_years = 3

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
        return (
            self.init_completed
            and self.init_step >= WIZARD_FINAL_STEP
            and bool(str(self.app_home or "").strip())
        )

    def can_use_live_trading(self) -> bool:
        """检查是否可以使用实盘/仿真交易功能

        Returns:
            bool: True 表示可以使用实盘/仿真功能
        """
        return self.is_fully_initialized and self.gateway_enabled

    def can_use_backtest(self) -> bool:
        """检查是否可以使用回测功能

        Returns:
            bool: True 表示可以使用回测功能
        """
        return self.is_fully_initialized and bool(self.tushare_token)
