"""实时行情服务

订阅 QMT 全推行情，合成 K 线并通过 WebSocket 发布。
"""

import datetime
import threading
from collections import defaultdict
from typing import Callable

from loguru import logger

from qmt_gateway.config import config
from qmt_gateway.core import require_xtdata


class QuoteService:
    """实时行情服务

    订阅 QMT 全推行情，合成 1分钟、30分钟和日线 K 线。
    1. 通过 subscribe_whole_quote 订阅个股行情
    2. 通过 subscribe_whole_quote 单独订阅指数行情（指数不能通过市场代码订阅）
    """

    # 主要指数代码列表
    INDEX_CODES = [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深成指
        "000300.SH",  # 沪深300
        "000905.SH",  # 中证500
        "000852.SH",  # 中证1000
        "000688.SH",  # 科创50
    ]

    def __init__(self):
        self._xtdata = None
        self._running = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._callbacks: list[Callable] = []
        self._bars_1m: dict[str, dict] = defaultdict(dict)
        self._bars_30m: dict[str, dict] = defaultdict(dict)
        self._bars_1d: dict[str, dict] = defaultdict(dict)

    def _get_xtdata(self):
        """获取 xtdata 模块"""
        if self._xtdata is None:
            self._xtdata = require_xtdata(
                xtquant_path=str(config.xtquant_path) if config.xtquant_path else None,
                qmt_path=str(config.qmt_path) if config.qmt_path else None,
            )
        return self._xtdata

    def start(self) -> None:
        """启动行情服务"""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("实时行情服务已启动")

    def stop(self) -> None:
        """停止行情服务"""
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("实时行情服务已停止")

    def _run(self) -> None:
        """运行行情订阅循环"""
        try:
            xtdata = self._get_xtdata()

            # 1. 订阅个股行情（通过市场代码）
            code_list = ["SH", "SZ", "BJ"]
            xtdata.subscribe_whole_quote(code_list, self._on_tick)
            logger.info(f"已订阅个股行情，市场: {code_list}")

            # 2. 单独订阅指数行情（指数不能通过市场代码订阅，必须单独订阅）
            xtdata.subscribe_whole_quote(self.INDEX_CODES, self._on_tick)
            logger.info(f"已订阅指数行情: {self.INDEX_CODES}")

            # 保持运行，使用事件等待（比 time.sleep 更优雅，不占用 CPU）
            self._stop_event.wait()

        except Exception as e:
            logger.error(f"行情服务运行错误: {e}")
            self._running = False

    def _on_tick(self, data: dict) -> None:
        """处理 tick 数据

        Args:
            data: tick 数据字典
        """
        try:
            symbol = data.get("code")
            if not symbol:
                return

            now = datetime.datetime.now()

            # 更新 1分钟 K 线
            bar_1m = self._update_bar(
                self._bars_1m[symbol],
                data,
                now,
                interval=60,
            )

            # 更新 30分钟 K 线
            bar_30m = self._update_bar(
                self._bars_30m[symbol],
                data,
                now,
                interval=1800,
            )

            # 更新日线 K 线
            bar_1d = self._update_bar(
                self._bars_1d[symbol],
                data,
                now,
                interval=86400,
            )

            # 触发回调
            for callback in self._callbacks:
                try:
                    callback({
                        "symbol": symbol,
                        "timestamp": now.isoformat(),
                        "1m": bar_1m,
                        "30m": bar_30m,
                        "1d": bar_1d,
                    })
                except Exception as e:
                    logger.error(f"行情回调错误: {e}")

        except Exception as e:
            logger.error(f"处理 tick 数据错误: {e}")

    def _update_bar(
        self,
        bar_cache: dict,
        tick: dict,
        now: datetime.datetime,
        interval: int,
    ) -> dict:
        """更新 K 线数据

        Args:
            bar_cache: K 线缓存
            tick: tick 数据
            now: 当前时间
            interval: 时间间隔（秒）

        Returns:
            更新后的 K 线数据
        """
        price = tick.get("lastPrice", 0)
        volume = tick.get("volume", 0)
        amount = tick.get("amount", 0)

        # 计算当前 K 线的时间戳
        if interval == 86400:  # 日线
            bar_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            timestamp = int(now.timestamp())
            bar_timestamp = (timestamp // interval) * interval
            bar_time = datetime.datetime.fromtimestamp(bar_timestamp)

        bar_key = bar_time.isoformat()

        if bar_key not in bar_cache:
            # 新 K 线
            bar_cache.clear()
            bar_cache[bar_key] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "amount": amount,
                "time": bar_time.isoformat(),
            }
        else:
            # 更新现有 K 线
            bar = bar_cache[bar_key]
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["close"] = price
            bar["volume"] += volume
            bar["amount"] += amount

        return bar_cache[bar_key]

    def subscribe(self, callback: Callable) -> None:
        """订阅行情数据

        Args:
            callback: 回调函数，接收行情数据字典
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def unsubscribe(self, callback: Callable) -> None:
        """取消订阅"""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def is_running(self) -> bool:
        """检查服务是否运行中"""
        return self._running


# 全局服务实例
quote_service = QuoteService()
