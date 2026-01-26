import datetime
from typing import Any, Dict, Optional

import polars as pl
from loguru import logger

from pyqmt.core.enums import FrameType
from pyqmt.service.base_broker import Broker


class BaseStrategy:
    """策略基类"""

    def __init__(self, broker: Broker, config: Dict[str, Any]):
        self.broker = broker
        self.config = config
        self.logger = logger.bind(strategy=self.__class__.__name__)
        self.interval: str = "1d"  # Default, set by Runner

    async def init(self):
        """策略初始化，在实例化后立即调用"""
        pass

    async def on_start(self):
        """回测/实盘开始前调用"""
        pass

    async def on_stop(self):
        """回测/实盘结束后调用"""
        pass

    async def on_day_open(self):
        """每日开盘前调用"""
        pass

    async def on_day_close(self):
        """每日收盘后调用"""
        pass

    async def on_bar(
        self, tm: datetime.datetime, quote: Dict[str, Any], frame_type: FrameType
    ):
        """核心驱动方法，每个周期调用一次"""
        pass

    def get_history(
        self,
        asset: str,
        count: int,
        end_dt: datetime.datetime | None = None,
        frame_type: str = "1d",
    ) -> pl.DataFrame:
        """获取历史数据

        Args:
            asset: 资产代码
            count: 数量
            end_dt: 截止时间 (包含)，默认为当前回测/实盘时间
            frame_type: 周期

        Returns:
            pl.DataFrame: 历史数据
        """
        return self.broker.get_history(asset, count, end_dt, frame_type)

    def log(self, msg: str, level: str = "INFO"):
        """日志辅助方法"""
        self.logger.log(level, msg)
