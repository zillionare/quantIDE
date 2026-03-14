from functools import wraps
from typing import Any, Dict, Type, TypeVar, cast

_instances: Dict[Type[Any], Any] = {}

T = TypeVar("T")


def singleton(cls: Type[T]) -> Type[T]:
    """单例装饰器，用于将类转换为单例模式"""

    @wraps(cls)
    def get_instance(*args: Any, **kwargs: Any) -> T:
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    # 保持原始类的属性
    get_instance.__name__ = cls.__name__
    get_instance.__doc__ = cls.__doc__
    get_instance.__module__ = cls.__module__
    get_instance.__qualname__ = cls.__qualname__

    # 添加对实例字典的访问
    get_instance._instances = _instances  # type: ignore[attr-defined]

    return cast(Type[T], get_instance)
