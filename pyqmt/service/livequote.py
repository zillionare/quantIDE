import datetime
import threading
import time
from typing import Any, Dict, Optional

import msgpack
import pandas as pd
import redis
from loguru import logger

from pyqmt.config import cfg
from pyqmt.core.enums import Topics
from pyqmt.core.message import msg_hub
from pyqmt.core.scheduler import scheduler
from pyqmt.core.singleton import singleton
from pyqmt.data.fetchers.tushare import fetch_limit_price

try:
    from xtquant import xtdata as xt
except ImportError:
    xt = None


@singleton
class LiveQuote:
    """实时行情服务

    支持从 QMT 或 Redis 订阅全推数据，并维护一个进程内字典缓存。
    """

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._limits: Dict[str, Dict[str, float]] = {}
        self._limit_date: datetime.date | None = None
        self._is_running = False
        self._mode: str|None = None
        self._redis_client = None

    def start(self):
        """启动订阅"""
        if self._is_running:
            return

        # 无论何种模式，都启动涨跌停限制的定时刷新
        self._start_limit_schedule()

        self._mode = cfg.livequote.mode
        if self._mode == "qmt":
            if xt is None:
                raise ImportError("xtquant is required for qmt mode")

            # 订阅全量行情 (SH/SZ)
            xt.subscribe_whole_quote(["SH", "SZ", "BJ"], self._cache_and_broadcast)
        else:
            self._start_redis_subscription()

        self._is_running = True
        logger.info("LiveQuote service started in {} mode", self._mode)

    def stop(self):
        """停止服务"""
        self._is_running = False
        if self._redis_client:
            try:
                self._redis_client.close()
            except Exception:
                pass
        logger.info("LiveQuote service stopped")

    def _start_limit_schedule(self):
        # 仅在交易日 9:00 之后立即刷新一次
        now = datetime.datetime.now()
        if 9 <= now.hour < 15: # 简单判断交易时间段
             self._refresh_limits()

        scheduler.add_job(
            self._refresh_limits,
            "cron",
            hour=9,
            minute=0,
            name="livequote.limit.refresh",
        )

    def _start_redis_subscription(self):
        """从 Redis 订阅全推数据"""
        if getattr(cfg, "redis", None) is not None:
            rc = cfg.redis
            # 注意：不设置 decode_responses=True 以支持 msgpack 二进制数据
            self._redis_client = redis.Redis(
                host=rc.host, port=rc.port, decode_responses=False
            )
        if self._redis_client is None:
            raise RuntimeError("Redis client is not configured")

        def redis_listener():
            try:
                pubsub = self._redis_client.pubsub()  # type: ignore

                # 订阅全推行情频道
                channels = [Topics.QUOTES_ALL.value, Topics.STOCK_LIMIT.value]
                pubsub.subscribe(*channels)
                logger.info(f"Subscribed to Redis channels: {channels}")

                for item in pubsub.listen():
                    if item["type"] == "message":
                        self._on_redis_message(item["channel"], item["data"])
            except Exception as e:
                logger.exception(f"Redis listener crashed: {e}")
            finally:
                logger.info("Redis listener exited")

        thread = threading.Thread(
            target=redis_listener, name="RedisQuoteListener", daemon=True
        )
        thread.start()

    def _on_redis_message(self, channel: bytes | str, raw_data: bytes):
        """处理来自 Redis 的原始消息字节流"""
        start_time = time.perf_counter()
        try:
            # 约定：发布端必须使用 msgpack 序列化
            data = msgpack.unpackb(raw_data)

            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")

            if channel == Topics.QUOTES_ALL.value:
                self._cache_and_broadcast(data)
            elif channel == Topics.STOCK_LIMIT.value:
                self._cache_limits_and_broadcast(data)

            # 性能监控：单条消息处理超过 50ms 报警
            duration = (time.perf_counter() - start_time) * 1000
            if duration > 50:
                logger.warning(
                    "Slow quote processing: {:.2f}ms for {} items",
                    duration,
                    len(data),
                )
        except Exception as e:
            logger.error("Error decoding msgpack quote: {}", e)

    def _cache_and_broadcast(self, data: Dict[str, Any]):
        """处理行情数据并广播"""
        self._cache.update(data)

        # 发布通知
        msg_hub.publish(Topics.QUOTES_ALL.value, data)

    def _cache_limits(self, data: Dict[str, Any]):
        if not data:
            return

        # 假设 data 已经是 {asset: {'up_limit': float, 'down_limit': float}} 格式
        # 即使包含其他字段，只要包含 up_limit/down_limit 即可
        # 如果需要严格校验或转换，可以在发送端（Redis Publisher）保证
        self._limits.update(data)

    def _cache_limits_and_broadcast(self, data: Dict[str, Any]):
        """缓存涨跌停数据并广播"""
        self._cache_limits(data)
        # 发布通知
        msg_hub.publish(Topics.STOCK_LIMIT.value, data)

    def _refresh_limits(self, dt: datetime.date | None = None):
        dt = dt or datetime.date.today()
        df, _ = fetch_limit_price(dt)
        if df is None or df.empty:
            return
        if "asset" not in df.columns and "ts_code" in df.columns:
            df = df.rename(columns={"ts_code": "asset"})
        if "asset" not in df.columns:
            return
        self._limit_date = dt

        # 优化：使用向量化操作替代循环
        df = df[df["asset"].notna() & (df["asset"] != "")]

        for col in ["up_limit", "down_limit"]:
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["asset"] = df["asset"].astype(str)
        self._limits.update(df.set_index("asset")[["up_limit", "down_limit"]].to_dict("index")) # type: ignore

    def get_quote(self, asset: str) -> Optional[Dict[str, Any]]:
        """获取指定资产的最新行情字典"""
        return self._cache.get(asset)

    def get_price_limits(self, asset: str) -> tuple[float, float]:
        limits = self._limits.get(asset)
        if not limits:
            return 0.0, 0.0
        return limits.get("down_limit", 0.0), limits.get("up_limit", 0.0)

    def get_limit(self, asset: str) -> Optional[Dict[str, float]]:
        limits = self._limits.get(asset)
        if not limits:
            return None
        return limits.copy()

    @property
    def all_limits(self) -> Dict[str, Dict[str, float]]:
        return self._limits.copy()

    @property
    def all_quotes(self) -> Dict[str, Dict[str, Any]]:
        """获取所有缓存的行情"""
        return self._cache.copy()


# 创建全局单例
live_quote = LiveQuote()
