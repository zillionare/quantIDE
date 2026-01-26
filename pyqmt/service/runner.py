import datetime
import uuid
from typing import Any, Dict, Type

from loguru import logger

from pyqmt.core.enums import FrameType
from pyqmt.core.strategy import BaseStrategy
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.service.backtest_broker import BacktestBroker
from pyqmt.service.metrics import metrics


class BacktestRunner:
    async def run(
        self,
        strategy_cls: Type[BaseStrategy],
        config: Dict[str, Any],
        start_date: datetime.date,
        end_date: datetime.date,
        interval: str = "1d",
        initial_cash: float = 1_000_000,
        portfolio_id: str | None = None,
    ) -> Dict[str, Any]:

        if portfolio_id is None:
            portfolio_id = uuid.uuid4().hex

        logger.info(f"Starting backtest for {strategy_cls.__name__} ({portfolio_id})")

        # 1. Init Broker
        # 使用 daily_bars 作为 data_feed
        broker = BacktestBroker(
            bt_start=start_date,
            bt_end=end_date,
            portfolio_id=portfolio_id,
            data_feed=daily_bars, # type: ignore
            principal=initial_cash,
            match_level="day" if interval == "1d" else "minute"
        )

        # 2. Init Strategy
        strategy = strategy_cls(broker, config)
        strategy.interval = interval

        await strategy.init()
        await strategy.on_start()

        # 3. Time Loop
        days = calendar.get_trade_dates(start_date, end_date)

        frame_type = FrameType.DAY
        if interval == "1m":
            frame_type = FrameType.MIN1
        elif interval == "5m":
            frame_type = FrameType.MIN5

        for day in days:
            # Day Open (09:30)
            open_tm = calendar.replace_time(day, 9, 30)
            broker.set_clock(open_tm)
            await strategy.on_day_open()

            if interval == "1d":
                # 日线模式：每天一次 on_bar (15:00)
                bar_tm = calendar.replace_time(day, 15, 0)
                broker.set_clock(bar_tm)

                # 构建 quote: 获取当前持仓的最新价格
                quote = {}
                assets = list(broker.positions.keys())
                if assets:
                    # 获取当日收盘价
                    df = daily_bars.get_bars_in_range(day, day, assets)
                    if not df.is_empty():
                        for row in df.iter_rows(named=True):
                            quote[row['asset']] = {
                                'lastPrice': row['close'],
                                'volume': row['volume']
                            }

                await strategy.on_bar(bar_tm, quote, FrameType.DAY)

            else:
                # 分钟线模式 (暂略，需遍历分钟)
                pass

            # Day Close (15:30 ? usually after market close)
            close_tm = calendar.replace_time(day, 15, 30)
            broker.set_clock(close_tm)
            await strategy.on_day_close()

        await strategy.on_stop()
        await broker.stop_backtest()

        logger.info(f"Backtest finished: {portfolio_id}")

        # 4. Metrics
        stats = metrics(portfolio_id)

        return {
            "portfolio_id": portfolio_id,
            "metrics": stats.to_dict() if stats is not None else {}
        }
