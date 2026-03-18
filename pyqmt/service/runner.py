import datetime
import uuid
from typing import Any, Dict, Type

from loguru import logger

from pyqmt.core.enums import FrameType
from pyqmt.core.ports import ClockPort
from pyqmt.core.runtime.clock_bridge import BacktestClockAdapter
from pyqmt.core.strategy import BaseStrategy
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.sqlite import db
from pyqmt.service.backtest_broker import BacktestBroker
from pyqmt.service.metrics import metrics


class BacktestRunner:
    """回测运行器，负责管理回测的生命周期和时间循环。"""

    def __init__(self, clock: ClockPort | None = None):
        """初始化回测运行器.

        Args:
            clock: 时钟端口实现。
        """
        self._clock = clock or BacktestClockAdapter()

    def _align_backtest_dates(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> tuple[datetime.date, datetime.date]:
        """对齐回测起止日期到交易日。

        Args:
            start_date: 回测开始日期
            end_date: 回测结束日期

        Returns:
            tuple[datetime.date, datetime.date]: 对齐后的起止日期
        """
        start_date = calendar.ceiling(start_date, FrameType.DAY)
        end_date = calendar.floor(end_date, FrameType.DAY)
        if start_date > end_date:
            raise ValueError(f"回测开始日期 {start_date} 不能晚于结束日期 {end_date}")
        return start_date, end_date

    def _init_backtest(
        self,
        strategy_cls: Type[BaseStrategy],
        config: Dict[str, Any],
        start_date: datetime.date,
        end_date: datetime.date,
        frame_type: FrameType,
        initial_cash: float,
        portfolio_id: str | None,
        db_path: str | None,
    ) -> tuple[str, BacktestBroker, BaseStrategy]:
        """初始化回测环境，包括 Broker、Strategy 和数据库。

        Args:
            strategy_cls: 策略类
            config: 策略配置
            start_date: 回测开始日期
            end_date: 回测结束日期
            frame_type: 回测周期类型
            initial_cash: 初始资金
            portfolio_id: 组合 ID，如果为 None 则自动生成
            db_path: 数据库路径，如果为 None 则使用默认路径

        Returns:
            tuple: (portfolio_id, broker, strategy)
        """
        if portfolio_id is None:
            portfolio_id = uuid.uuid4().hex

        # Initialize DB if provided (e.g. for multi-process isolation with :memory:)
        if db_path:
            db.init(db_path)

        # Use patched logger
        self.logger = logger.bind(runner="BacktestRunner")
        self.logger.info(f"Starting backtest for {strategy_cls.__name__} ({portfolio_id})")

        # 1. Init Broker
        broker = BacktestBroker(
            bt_start=start_date,
            bt_end=end_date,
            portfolio_id=portfolio_id,
            data_feed=daily_bars,  # type: ignore
            principal=initial_cash,
            match_level="day" if frame_type == FrameType.DAY else "minute",
            portfolio_name=strategy_cls.__name__,
        )

        # 2. Init Strategy
        strategy = strategy_cls(broker, config)
        strategy.interval = frame_type.value

        return portfolio_id, broker, strategy

    async def _handle_day_switch(
        self,
        strategy: BaseStrategy,
        broker: BacktestBroker,
        current_date: datetime.date,
        last_trade_day: datetime.date | None,
    ) -> datetime.date:
        """处理日间切换逻辑（收盘和开盘）。

        Args:
            strategy: 策略实例
            broker: Broker 实例
            current_date: 当前交易日
            last_trade_day: 上一个交易日

        Returns:
            datetime.date: 更新后的 last_trade_day (即 current_date)
        """
        if last_trade_day is None or current_date != last_trade_day:
            # Close previous day if exists
            if last_trade_day is not None:
                close_tm = calendar.replace_time(last_trade_day, 15, 30)
                self._clock.set_now(close_tm)
                broker.set_clock(close_tm)
                strategy._current_time = close_tm
                await strategy.on_day_close(close_tm)

            # Open new day
            open_tm = calendar.replace_time(current_date, 9, 30)
            self._clock.set_now(open_tm)
            broker.set_clock(open_tm)
            strategy._current_time = open_tm
            await strategy.on_day_open(open_tm)

            return current_date
        return last_trade_day

    def _get_bar_quote(
        self,
        broker: BacktestBroker,
        current_date: datetime.date,
        config: Dict[str, Any],
        frame_type: FrameType,
    ) -> Dict[str, Any]:
        """获取当前 Bar 的行情快照。

        Args:
            broker: Broker 实例
            current_date: 当前日期
            config: 策略配置
            frame_type: 当前 Bar 的周期类型

        Returns:
            Dict[str, Any]: 行情快照字典
        """
        quote = {}
        if frame_type == FrameType.DAY:
            universe = config.get("universe", [])
            assets = list(set(list(broker.positions.keys()) + universe))

            if assets:
                df = daily_bars.get_bars_in_range(current_date, current_date, assets)
                if not df.is_empty():
                    for row in df.iter_rows(named=True):
                        quote[row["asset"]] = {
                            "lastPrice": row["close"],
                            "volume": row["volume"],
                        }
        else:
            # TODO: Implement minute bar quote fetching
            pass
        return quote

    async def run(
        self,
        strategy_cls: Type[BaseStrategy],
        config: Dict[str, Any],
        start_date: datetime.date,
        end_date: datetime.date,
        frame_type: FrameType = FrameType.DAY,
        initial_cash: float = 1_000_000,
        portfolio_id: str | None = None,
        db_path: str | None = None,
    ) -> Dict[str, Any]:
        """运行回测。

        Args:
            strategy_cls: 策略类
            config: 策略配置
            start_date: 回测开始日期
            end_date: 回测结束日期
            frame_type: 回测周期类型
            initial_cash: 初始资金
            portfolio_id: 组合 ID，如果为 None 则自动生成
            db_path: 数据库路径，如果为 None 则使用默认路径

        Returns:
            Dict[str, Any]: 回测结果，包含 metrics 和 portfolio_id
        """
        start_date, end_date = self._align_backtest_dates(start_date, end_date)
        portfolio_id, broker, strategy = self._init_backtest(
            strategy_cls,
            config,
            start_date,
            end_date,
            frame_type,
            initial_cash,
            portfolio_id,
            db_path,
        )

        await strategy.init()
        await strategy.on_start()

        # 3. Time Loop
        if frame_type in [FrameType.MIN1, FrameType.MIN5]:
            start_tm = calendar.first_min_frame(start_date, frame_type)
            end_tm = calendar.last_min_frame(end_date, frame_type)
            frames = self._clock.iter_frames(start_tm, end_tm, frame_type)
        else:
            frames = self._clock.iter_frames(start_date, end_date, frame_type)

        last_trade_day = None

        try:
            for tm in frames:
                if isinstance(tm, datetime.datetime):
                    current_date = tm.date()
                    bar_tm = tm
                else:
                    current_date = tm
                    bar_tm = calendar.replace_time(tm, 15, 0)

                last_trade_day = await self._handle_day_switch(
                    strategy, broker, current_date, last_trade_day
                )

                # Bar Logic
                self._clock.set_now(bar_tm)
                broker.set_clock(bar_tm)
                strategy._current_time = bar_tm
                quote = self._get_bar_quote(broker, current_date, config, frame_type)
                await strategy.on_bar(bar_tm, quote, frame_type)

            # Close the last day
            if last_trade_day is not None:
                close_tm = calendar.replace_time(last_trade_day, 15, 30)
                self._clock.set_now(close_tm)
                broker.set_clock(close_tm)
                strategy._current_time = close_tm
                await strategy.on_day_close(close_tm)

        finally:
            await strategy.on_stop()
        await broker.stop_backtest()

        self.logger.info(f"Backtest finished: {portfolio_id}")

        # 4. Metrics
        stats = metrics(portfolio_id)

        return {
            "portfolio_id": portfolio_id,
            "metrics": stats.to_dict() if stats is not None else {},
        }
