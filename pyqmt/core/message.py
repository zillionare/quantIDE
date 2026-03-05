"""
MessageHub 模块：进程内异步消息中心

本模块提供了一个高性能、线程安全的单例消息总线 `msg_hub`，用于系统内部各组件之间的解耦通信。

主要特性：
1. **异步分发**：`publish` 调用立即返回，消息由内部后台线程异步分发，不会卡住发布者（如行情推送线程）。
2. **线程安全**：支持多线程环境下的安全订阅、取消订阅和消息发布。
3. **混合模式**：
   - **推送 (Push)**：通过 `subscribe` 注册回调函数，消息到达时自动调用。
   - **拉取 (Pull)**：如果没有订阅者，消息会自动进入内部队列，可通过 `get` 或 `get_no_wait` 获取。
4. **异常隔离**：单个订阅者回调函数的崩溃不会影响其他订阅者或系统运行。

使用说明：

1. **初始化与容量控制**：
   - 默认队列大小为 10000。如果消息积压超过此值，新消息将被丢弃并记录警告。
   - 内部通过 `MessageHub(queue_size=N)` 进行配置。

2. **订阅与反订阅**：
   ```python
   def my_callback(content):
       print(f"收到消息: {content}")

   # 订阅主题
   msg_hub.subscribe("trade_event", my_callback)

   # 取消订阅
   msg_hub.unsubscribe("trade_event", my_callback)
   ```

3. **异步发布**：
   ```python
   # 立即返回，不阻塞
   msg_hub.publish("trade_event", {"order_id": 123, "status": "filled"})
   ```

4. **拉取消息**：
   ```python
   # 阻塞获取，带超时
   try:
       msg = msg_hub.get("task_result", timeout=5.0)
   except Empty:
       print("等待超时")

   # 非阻塞获取
   msg = msg_hub.get_no_wait("task_result")
   ```

5. **停止服务**：
   - 系统退出时可调用 `msg_hub.stop()` 安全停止分发后台线程。

"""

import datetime
import threading
from queue import Empty, Full, Queue
from typing import Any, Callable, Dict, List, Optional

from loguru import logger

from pyqmt.core.singleton import singleton


@singleton
class MessageHub:
    """进程内消息中心，管理消息的发布与订阅（异步分发且线程安全）"""

    def __init__(self, queue_size: int = 10000):
        self._lock = threading.Lock()
        self._subscribers: Dict[str, List[Callable]] = {}
        self._queues: Dict[str, Queue] = {}

        # 异步分发队列
        self._dispatch_queue: Queue = Queue(maxsize=queue_size)
        self._stop_event = threading.Event()
        self._worker = threading.Thread(
            target=self._dispatch_loop, name="MessageHubWorker", daemon=True
        )
        self._worker.start()

    def subscribe(self, topic: str, callback: Callable) -> None:
        """
        订阅指定主题的消息
        Args:
            topic: 消息主题（字符串标识，如 "data_updated"）
            callback: 消息处理函数（参数为消息内容）
        """
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []

            if callback not in self._subscribers[topic]:
                self._subscribers[topic].append(callback)

    def unsubscribe(self, topic: str, callback: Callable) -> None:
        """取消订阅指定主题的消息"""
        with self._lock:
            if topic in self._subscribers:
                if callback in self._subscribers[topic]:
                    self._subscribers[topic].remove(callback)
                if not self._subscribers[topic]:
                    del self._subscribers[topic]

    def publish(self, topic: str, msg_content: Any) -> None:
        """
        异步发布消息
        Args:
            topic: 消息主题
            msg_content: 消息内容
        """
        try:
            self._dispatch_queue.put_nowait((topic, msg_content))
        except Full:
            logger.warning(
                "MessageHub dispatch queue is full. Discarding message of topic: {}",
                topic,
            )

    def _dispatch_loop(self):
        """后台分发线程主循环"""
        logger.info("MessageHub dispatch loop started")
        try:
            while not self._stop_event.is_set():
                try:
                    # 使用超时以便能够响应停止事件
                    topic, msg_content = self._dispatch_queue.get(timeout=0.1)
                except Empty:
                    continue

                try:
                    # 获取订阅者副本，减少锁持有时间
                    with self._lock:
                        callbacks = self._subscribers.get(topic, []).copy()

                    if callbacks:
                        for callback in callbacks:
                            try:
                                callback(msg_content)
                            except Exception as e:
                                logger.error(f"Error handling message {topic}: {e}")
                    else:
                        # 如果没有订阅者，则存入对应的 pull 队列（保持原有逻辑）
                        self._put_to_pull_queue(topic, msg_content)
                finally:
                    self._dispatch_queue.task_done()
        except Exception as e:
            logger.exception(f"MessageHub dispatch loop crashed: {e}")
        finally:
            logger.info("MessageHub dispatch loop exited")

    def _put_to_pull_queue(self, topic: str, msg_content: Any):
        """将消息放入 pull 模式的队列中"""
        with self._lock:
            if topic not in self._queues:
                self._queues[topic] = Queue(maxsize=1000)
            queue = self._queues[topic]

        try:
            queue.put_nowait(msg_content)
        except Full:
            logger.info("Pull queue for topic {} is full. Discarding oldest.", topic)
            try:
                queue.get_nowait()
                queue.put_nowait(msg_content)
            except (Empty, Full):
                pass

    def get_no_wait(self, topic: str) -> Any:
        """非阻塞获取消息"""
        with self._lock:
            queue = self._queues.get(topic)

        if queue:
            return queue.get_nowait()

        raise ValueError(f"消息主题 {topic} 不存在")

    def get(self, topic: str, timeout: Optional[float] = None) -> Any:
        """阻塞获取消息"""
        with self._lock:
            if topic not in self._queues:
                self._queues[topic] = Queue(maxsize=1000)
            queue = self._queues[topic]

        return queue.get(timeout=timeout)

    def stop(self):
        """停止分发线程"""
        self._stop_event.set()
        if self._worker.is_alive():
            self._worker.join(timeout=1.0)


msg_hub = MessageHub()

__all__ = ["msg_hub"]
