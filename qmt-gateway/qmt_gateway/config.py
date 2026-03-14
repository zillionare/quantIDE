"""配置管理模块

所有配置存储在数据库 Settings 表中，提供统一的配置访问接口。
"""

from pathlib import Path
from typing import Any

from qmt_gateway.db import db
from qmt_gateway.db.models import Settings


class ConfigManager:
    """配置管理器

    提供配置的读取、写入和默认值管理。
    """

    # 默认配置值
    DEFAULTS = {
        "server_port": 8130,
        "log_path": "~/.qmt-gateway/log",
        "log_rotation": "10 MB",
        "log_retention": 10,
        "qmt_account_id": "",
        "qmt_account_type": "live",
        "qmt_path": "",
        "xtquant_path": "",
        "data_home": "~/.qmt-gateway/data",
        "data_start_date": None,
        "init_completed": False,
        "init_step": 0,
    }

    def __init__(self):
        self._cache: dict[str, Any] = {}
        self._loaded = False

    def _ensure_loaded(self):
        """确保配置已加载"""
        if not self._loaded:
            self.reload()

    def reload(self):
        """重新加载配置"""
        try:
            settings = db.get_settings()
            self._cache = settings.to_dict()
            self._loaded = True
        except Exception:
            # 如果数据库未初始化，使用默认值
            self._cache = dict(self.DEFAULTS)
            self._loaded = True

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置项

        Args:
            key: 配置项名称
            default: 默认值

        Returns:
            配置值
        """
        self._ensure_loaded()
        return self._cache.get(key, default if default is not None else self.DEFAULTS.get(key))

    def set(self, key: str, value: Any) -> None:
        """设置配置项

        Args:
            key: 配置项名称
            value: 配置值
        """
        self._ensure_loaded()
        self._cache[key] = value

        # 保存到数据库
        try:
            settings = db.get_settings()
            if hasattr(settings, key):
                setattr(settings, key, value)
                db.save_settings(settings)
        except Exception:
            pass

    def set_many(self, **kwargs) -> None:
        """批量设置配置项"""
        self._ensure_loaded()
        for key, value in kwargs.items():
            self._cache[key] = value

        # 保存到数据库
        try:
            settings = db.get_settings()
            for key, value in kwargs.items():
                if hasattr(settings, key):
                    setattr(settings, key, value)
            db.save_settings(settings)
        except Exception:
            pass

    def get_all(self) -> dict[str, Any]:
        """获取所有配置"""
        self._ensure_loaded()
        return dict(self._cache)

    def get_expanded_path(self, key: str) -> Path:
        """获取展开后的路径配置"""
        path_str = self.get(key, "")
        return Path(path_str).expanduser()

    def init_defaults(self) -> None:
        """初始化默认配置"""
        try:
            settings = db.get_settings()
            # 只设置未设置的值
            for key, value in self.DEFAULTS.items():
                if getattr(settings, key) is None or getattr(settings, key) == "":
                    if isinstance(value, str) and "~" in value:
                        value = str(Path(value).expanduser())
                    setattr(settings, key, value)
            db.save_settings(settings)
            self.reload()
        except Exception:
            pass

    @property
    def server_port(self) -> int:
        """服务器端口"""
        return self.get("server_port", 8130)

    @property
    def log_path(self) -> Path:
        """日志路径"""
        return self.get_expanded_path("log_path")

    @property
    def log_rotation(self) -> str:
        """日志轮转大小"""
        return self.get("log_rotation", "10 MB")

    @property
    def log_retention(self) -> int:
        """日志保留数量"""
        return self.get("log_retention", 10)

    @property
    def qmt_account_id(self) -> str:
        """QMT 账号"""
        return self.get("qmt_account_id", "")

    @property
    def qmt_path(self) -> Path:
        """QMT 路径"""
        return self.get_expanded_path("qmt_path")

    @property
    def xtquant_path(self) -> Path:
        """xtquant 路径"""
        return self.get_expanded_path("xtquant_path")

    @property
    def data_home(self) -> Path:
        """数据主目录"""
        return self.get_expanded_path("data_home")

    @property
    def init_completed(self) -> bool:
        """初始化是否完成"""
        return self.get("init_completed", False)

    @property
    def is_configured(self) -> bool:
        """检查是否已配置 QMT"""
        return bool(self.qmt_account_id and self.qmt_path)


# 全局配置管理器实例
config = ConfigManager()
