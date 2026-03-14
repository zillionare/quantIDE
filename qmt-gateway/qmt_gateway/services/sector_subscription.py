"""板块订阅管理模块

提供板块实时行情的按需订阅功能。
"""

import datetime
import threading
from typing import Set

from loguru import logger


class SectorSubscriptionManager:
    """板块订阅管理器

    管理用户对板块实时行情的订阅，支持：
    - 添加/取消订阅
    - 获取当前订阅列表
    - 每日自动清空订阅
    """

    def __init__(self):
        self._subscribed_sectors: Set[str] = set()
        self._lock = threading.Lock()
        self._last_clear_date: datetime.date | None = None

    def subscribe(self, sector_id: str) -> bool:
        """订阅板块

        Args:
            sector_id: 板块代码

        Returns:
            是否成功订阅（如果已订阅则返回False）
        """
        with self._lock:
            if sector_id in self._subscribed_sectors:
                return False
            self._subscribed_sectors.add(sector_id)
            logger.info(f"订阅板块: {sector_id}")
            return True

    def unsubscribe(self, sector_id: str) -> bool:
        """取消订阅板块

        Args:
            sector_id: 板块代码

        Returns:
            是否成功取消（如果未订阅则返回False）
        """
        with self._lock:
            if sector_id not in self._subscribed_sectors:
                return False
            self._subscribed_sectors.remove(sector_id)
            logger.info(f"取消订阅板块: {sector_id}")
            return True

    def get_subscribed_sectors(self) -> list[str]:
        """获取当前订阅的板块列表

        Returns:
            板块代码列表
        """
        with self._lock:
            return list(self._subscribed_sectors)

    def is_subscribed(self, sector_id: str) -> bool:
        """检查是否已订阅板块

        Args:
            sector_id: 板块代码

        Returns:
            是否已订阅
        """
        with self._lock:
            return sector_id in self._subscribed_sectors

    def clear_all(self) -> int:
        """清空所有订阅

        Returns:
            清空的订阅数量
        """
        with self._lock:
            count = len(self._subscribed_sectors)
            self._subscribed_sectors.clear()
            logger.info(f"清空所有板块订阅，共 {count} 个")
            return count

    def auto_clear_if_needed(self) -> bool:
        """如果需要，自动清空订阅（每天开盘前）

        Returns:
            是否执行了清空操作
        """
        today = datetime.date.today()
        
        # 如果今天已经清空过，跳过
        if self._last_clear_date == today:
            return False
        
        # 检查是否是交易日开盘前（9:00前）
        now = datetime.datetime.now()
        if now.hour < 9 or (now.hour == 9 and now.minute < 15):
            self.clear_all()
            self._last_clear_date = today
            return True
        
        return False


# 全局订阅管理器实例
sector_subscription = SectorSubscriptionManager()
