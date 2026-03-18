"""存储端口抽象."""

from typing import Any, Protocol


class StoragePort(Protocol):
    """存储端口."""

    def save_order(self, order: Any) -> None:
        """保存订单."""
        ...

    def save_trade(self, trade: Any) -> None:
        """保存成交."""
        ...

    def save_position(self, position: Any) -> None:
        """保存持仓."""
        ...

    def save_asset(self, asset: Any) -> None:
        """保存资产."""
        ...

    def save_metric(self, metric: Any) -> None:
        """保存指标."""
        ...
