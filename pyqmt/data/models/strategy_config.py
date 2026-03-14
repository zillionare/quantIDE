"""策略配置和扫描结果数据模型"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar, List

from pyqmt.data.models.base import Entity, new_uuid_id


@dataclass
class StrategyConfig(Entity):
    """策略扫描配置"""

    __table_name__ = "strategy_config"
    __pk__ = "id"
    __indexes__ = (["key"], True)  # key 唯一

    id: str = field(default_factory=new_uuid_id)
    key: str = ""  # 配置键，如 'scan_directory'
    value: str = ""  # 配置值
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class StrategyInfo(Entity):
    """扫描发现的策略信息"""

    __table_name__ = "strategy_info"
    __pk__ = "id"
    __indexes__ = (["name"], True)  # 策略名唯一

    id: str = field(default_factory=new_uuid_id)
    name: str = ""  # 策略类名
    module_path: str = ""  # 模块路径
    file_path: str = ""  # 文件路径
    description: str = ""  # 策略描述
    version: str = "1.0.0"  # 版本号
    params: str = ""  # 参数配置(JSON)
    scan_dir: str = ""  # 扫描目录
    scanned_at: datetime = field(default_factory=datetime.now)
