from functools import wraps
from typing import Any, Dict, Type

_instances: Dict[Type[Any], Any] = {}


def singleton(cls: Type[Any]) -> Type[Any]:
    """单例装饰器，用于将类转换为单例模式"""

    @wraps(cls)
    def get_instance(*args: Any, **kwargs: Any) -> Any:
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    get_instance._instances = _instances  # type: ignore[attr-defined]
    return get_instance
