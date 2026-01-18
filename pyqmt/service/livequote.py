
import logging
import threading
import time
from typing import Any, Dict, Optional

import cfg4py
import msgpack
import redis

from pyqmt.core.enums import Topics
from pyqmt.core.message import msg_hub
from pyqmt.core.singleton import singleton

logger = logging.getLogger(__name__)

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
        self._cfg = cfg4py.get_instance()
        self._is_running = False
        self._mode = self._cfg.server.get("quote_mode", "qmt")  # qmt or redis
        self._redis_client = None

        # 初始化 Redis 连接（如果配置了 Redis）
        if self._cfg.get("redis"):
            rc = self._cfg.redis
            # 注意：不设置 decode_responses=True 以支持 msgpack 二进制数据
            self._redis_client = redis.Redis(
                host=rc.host, port=rc.port, decode_responses=False
            )

    def start(self):
        """启动订阅"""
        if self._is_running:
            return

        if self._mode == "qmt":
            if xt is None:
                raise ImportError("xtquant is required for qmt mode")

            # 订阅全量行情 (SH/SZ)
            xt.subscribe_whole_quote(["SH", "SZ", "BJ"], self._cache_and_broadcast)
        else:
            self._start_redis_subscription()

        self._is_running = True
        logger.info(f"LiveQuote service started in {self._mode} mode")


    def _start_redis_subscription(self):
        """从 Redis 订阅全推数据"""
        if self._redis_client is None:
            raise RuntimeError("Redis client is not configured")

        def redis_listener():
            pubsub = self._redis_client.pubsub() #type: ignore

            # 订阅全推行情频道
            channel = Topics.QUOTES_ALL.value
            pubsub.subscribe(channel)
            logger.info(f"Subscribed to Redis channel: {channel}")

            for message in pubsub.listen():
                # 过滤掉 logistic message，比如订阅成功
                if message["type"] == "message":
                    self._on_redis_message(message["data"])

        thread = threading.Thread(
            target=redis_listener, name="RedisQuoteListener", daemon=True
        )
        thread.start()

    def _on_redis_message(self, raw_data: bytes):
        """处理来自 Redis 的原始消息字节流"""
        start_time = time.perf_counter()
        try:
            # 约定：发布端必须使用 msgpack 序列化
            data = msgpack.unpackb(raw_data)
            self._cache_and_broadcast(data)

            # 性能监控：单条消息处理超过 50ms 报警
            duration = (time.perf_counter() - start_time) * 1000
            if duration > 50:
                logger.warning(
                    f"Slow quote processing: {duration:.2f}ms for {len(data)} items"
                )
        except Exception as e:
            logger.error(f"Error decoding msgpack quote: {e}")

    def _cache_and_broadcast(self, data: Dict[str, Any]):
        """处理行情数据并广播"""
        self._cache.update(data)

        # 发布通知
        msg_hub.publish("quote.all", data)

    def get_quote(self, asset: str) -> Optional[Dict[str, Any]]:
        """获取指定资产的最新行情字典"""
        return self._cache.get(asset)

    @property
    def all_quotes(self) -> Dict[str, Dict[str, Any]]:
        """获取所有缓存的行情"""
        return self._cache.copy()


# 创建全局单例
live_quote = LiveQuote()
