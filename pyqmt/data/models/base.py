"""数据模型基类"""

import types
import uuid
from dataclasses import asdict, dataclass, fields
from enum import Enum
from typing import ClassVar, List, Union, get_args, get_origin


def new_uuid_id() -> str:
    return "qtide-" + uuid.uuid4().hex[:16]


@dataclass
class Entity:
    """模型基类，提供数据库操作方法， 提供转换为数据库 schema 字典的方法"""

    __table_name__: ClassVar[str]
    __pk__: ClassVar[Union[str, List[str]]]
    __indexes__: ClassVar[tuple[List[str], bool]]

    @classmethod
    def to_db_schema(cls) -> dict:
        """类方法：解析当前 dataclass 为 fastlite 兼容的 schema 字典"""
        schema = {}

        for f in fields(cls):
            if f.type is uuid.UUID:
                schema[f.name] = str
            elif f.type in (str, int, float, bool):
                schema[f.name] = f.type
            # 处理所有联合类型（Union[A, B] 和 A | B 语法）
            elif (
                hasattr(f.type, "__origin__") and get_origin(f.type) is Union
            ) or isinstance(f.type, types.UnionType):
                # 提取非 None 的类型
                args = get_args(f.type)
                non_none_types = [t for t in args if t is not type(None)]
                if non_none_types:
                    base_type = non_none_types[0]
                    schema[f.name] = (
                        base_type if base_type in (str, int, float, bool) else str
                    )
                else:
                    schema[f.name] = str
            elif isinstance(f.type, type) and issubclass(f.type, Enum):
                schema[f.name] = int
            else:
                schema[f.name] = str
        return schema

    def to_dict(self) -> dict:
        """将 dataclass 转换为字典，处理 Enum 类型"""
        d = asdict(self)
        for k, v in d.items():
            if isinstance(v, Enum):
                d[k] = v.value
        return d
