"""运行时适配器注册表."""

from dataclasses import dataclass
from typing import Any


@dataclass
class AdapterSpec:
    """适配器定义."""

    name: str
    capability: str
    adapter: Any


class AdapterRegistry:
    """按能力注册和解析适配器."""

    def __init__(self):
        """初始化注册表."""
        self._items: dict[str, dict[str, Any]] = {}

    def register(self, capability: str, name: str, adapter: Any) -> None:
        """注册适配器.

        Args:
            capability: 能力名称。
            name: 适配器名称。
            adapter: 适配器实例。
        """
        bucket = self._items.setdefault(capability, {})
        bucket[name] = adapter

    def resolve(self, capability: str, name: str) -> Any:
        """解析适配器.

        Args:
            capability: 能力名称。
            name: 适配器名称。

        Returns:
            对应适配器实例。
        """
        bucket = self._items.get(capability, {})
        if name not in bucket:
            raise KeyError(f"adapter not found: {capability}:{name}")
        return bucket[name]

    def list_specs(self) -> list[AdapterSpec]:
        """列出全部适配器规格."""
        result: list[AdapterSpec] = []
        for capability, adapters in self._items.items():
            for name, adapter in adapters.items():
                result.append(
                    AdapterSpec(name=name, capability=capability, adapter=adapter)
                )
        return result
