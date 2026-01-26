import datetime
from typing import Any, Dict

from pyqmt.core.enums import FrameType
from pyqmt.core.strategy import BaseStrategy


class DualMAStrategy(BaseStrategy):
    """双均线策略示例"""

    def __init__(self, broker, config):
        super().__init__(broker, config)
        self.fast_window = int(self.config.get("fast", 5))
        self.slow_window = int(self.config.get("slow", 10))
        self.symbol = self.config.get("symbol", "000001.SZ")
        self.invest_amount = float(self.config.get("invest", 100000))

    async def init(self):
        self.log(
            f"DualMAStrategy Initialized: {self.symbol} Fast={self.fast_window} Slow={self.slow_window}"
        )

    async def on_day_open(self):
        # self.log(f"Day Open: {self.broker._clock}")
        pass

    async def on_bar(
        self, tm: datetime.datetime, quote: Dict[str, Any], frame_type: FrameType
    ):
        # 仅在日线级别运行
        if frame_type != FrameType.DAY:
            return

        # 获取历史数据 (slow_window + 2 用于计算上一期均线，预留多一点buffer)
        count = self.slow_window + 5
        hist = self.get_history(self.symbol, count, tm, "1d")

        # 数据不足
        if len(hist) < self.slow_window + 2:
            return

        closes = hist["close"].to_numpy()

        # 计算当前均线 (倒数第1个点是当前点)
        curr_fast = closes[-self.fast_window :].mean()
        curr_slow = closes[-self.slow_window :].mean()

        # 计算上一期均线 (排除最后一个点)
        prev_fast = closes[-(self.fast_window + 1) : -1].mean()
        prev_slow = closes[-(self.slow_window + 1) : -1].mean()

        # 获取当前持仓
        pos = self.broker.positions.get(self.symbol)
        shares = pos.shares if pos else 0

        # 金叉：快线上穿慢线
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            if shares == 0:
                self.log(f"Golden Cross at {tm}: Buy {self.symbol}")
                # 买入
                await self.broker.buy_amount(
                    self.symbol, self.invest_amount, price=0, order_time=tm
                )

        # 死叉：快线下穿慢线
        elif prev_fast >= prev_slow and curr_fast < curr_slow:
            if shares > 0:
                self.log(f"Death Cross at {tm}: Sell {self.symbol}")
                # 卖出
                await self.broker.sell(self.symbol, shares, price=0, order_time=tm)
