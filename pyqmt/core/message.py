from queue import Full, Queue
from typing import Any, Callable, Dict, List

from loguru import logger

from pyqmt.core.singleton import singleton


@singleton
class MessageHub:
    """进程内消息中心，管理消息的发布与订阅"""

    def __init__(self):
        self._queues: Dict[str, Queue] = {}
        self._subscribers: Dict[str, List[Callable]] = {}

    def subscribe(self, msg_type: str, callback: Callable) -> None:
        """
        订阅指定类型的消息
        Args:
            msg_type: 消息类型（字符串标识，如 "data_updated"）
            callback: 消息处理函数（参数为消息内容）
        """
        if msg_type not in self._subscribers:
            self._subscribers[msg_type] = []

        if callback not in self._subscribers[msg_type]:
            self._subscribers[msg_type].append(callback)

    def unsubscribe(self, msg_type: str, callback: Callable) -> None:
        """取消订阅指定类型的消息"""
        if msg_type in self._subscribers and callback in self._subscribers[msg_type]:
            self._subscribers[msg_type].remove(callback)
            if not self._subscribers[msg_type]:
                del self._subscribers[msg_type]

    def publish(self, msg_type: str, msg_content: Any) -> None:
        """
        发布消息
        Args:
            msg_type: 消息类型
            msg_content: 消息内容（任意类型，如字典、字符串、对象）
        """
        if msg_type in self._subscribers:
            for callback in self._subscribers.get(msg_type, []):
                try:
                    callback(msg_content)
                except Exception as e:
                    print(f"处理消息 {msg_type} 时出错：{e}")
        else:
            queue = self._queues.get(msg_type, Queue(maxsize=1000))
            if msg_type not in self._queues:
                self._queues[msg_type] = queue
            try:
                queue.put_nowait(msg_content)
            except Full:
                logger.warning(
                    "Queue for message type {} is full. Discarding oldest message.",
                    msg_type,
                )
                queue.get_nowait()
                queue.put_nowait(msg_content)

    def get_no_wait(self, msg_type: str) -> Any:
        if self._queues.get(msg_type):
            queue = self._queues[msg_type]
            return queue.get_nowait()

        raise ValueError(f"消息队列 {msg_type} 不存在")

    def get(self, msg_type: str, timeout: int | None) -> Any:
        if self._queues.get(msg_type):
            queue = self._queues[msg_type]
            return queue.get(timeout=timeout)

        self._queues[msg_type] = Queue(1000)
        return self._queues[msg_type].get(timeout=timeout)


msg_hub = MessageHub()

__all__ = [
    "msg_hub",
]
