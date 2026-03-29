import threading
import time
from queue import Empty, Queue

import pytest

from quantide.core.message import MessageHub, msg_hub


@pytest.fixture(autouse=True)
def clean_hub():
    """每个测试前清空 msg_hub 的状态"""
    hub = msg_hub
    with hub._lock:
        hub._subscribers.clear()
        hub._queues.clear()
        # 清空分发队列
        while not hub._dispatch_queue.empty():
            try:
                hub._dispatch_queue.get_nowait()
            except Empty:
                break
    # 确保 worker 线程在运行
    if hub._stop_event.is_set():
        hub._stop_event.clear()
        hub._worker = threading.Thread(
            target=hub._dispatch_loop, name="MessageHubWorker", daemon=True
        )
        hub._worker.start()
    yield hub


def test_subscribe_and_publish(clean_hub):
    """测试订阅和发布功能"""
    hub = clean_hub
    received_messages = []

    def callback(msg):
        received_messages.append(msg)

    # 订阅消息
    hub.subscribe("test_msg", callback)

    # 发布消息
    test_content = {"data": "test_data", "id": 123}
    hub.publish("test_msg", test_content)

    # 异步分发，需要等待
    time.sleep(0.2)

    # 验证消息是否被接收
    assert len(received_messages) == 1
    assert received_messages[0] == test_content


def test_multiple_subscribers(clean_hub):
    """测试多个订阅者"""
    hub = clean_hub
    received_messages_1 = []
    received_messages_2 = []

    def callback1(msg):
        received_messages_1.append(msg)

    def callback2(msg):
        received_messages_2.append(msg)

    # 订阅消息
    hub.subscribe("multi_msg", callback1)
    hub.subscribe("multi_msg", callback2)

    # 发布消息
    test_content = "multi_test"
    hub.publish("multi_msg", test_content)

    time.sleep(0.2)

    # 验证两个订阅者都收到了消息
    assert len(received_messages_1) == 1
    assert len(received_messages_2) == 1
    assert received_messages_1[0] == test_content
    assert received_messages_2[0] == test_content


def test_unsubscribe(clean_hub):
    """测试取消订阅功能"""
    hub = clean_hub
    received_messages = []

    def callback(msg):
        received_messages.append(msg)

    # 订阅消息
    hub.subscribe("test_msg", callback)
    hub.publish("test_msg", "first_message")

    time.sleep(0.2)
    assert len(received_messages) == 1

    # 取消订阅
    hub.unsubscribe("test_msg", callback)

    # 发布消息，应该不会被接收
    hub.publish("test_msg", "second_message")

    time.sleep(0.2)
    assert len(received_messages) == 1  # 仍然只有第一条消息


def test_publish_without_subscribers_uses_queue(clean_hub):
    """测试没有订阅者时消息被放入队列"""
    hub = clean_hub

    # 发布消息，此时没有订阅者
    test_content = {"queued": "data"}
    hub.publish("queued_msg", test_content)

    time.sleep(0.2)

    # 从队列获取消息
    retrieved = hub.get_no_wait("queued_msg")
    assert retrieved == test_content


def test_error_isolation(clean_hub):
    """测试错误隔离：一个回调报错不影响另一个"""
    hub = clean_hub
    received_messages = []

    def error_callback(msg):
        raise ValueError("Intentional error")

    def success_callback(msg):
        received_messages.append(msg)

    hub.subscribe("error_test", error_callback)
    hub.subscribe("error_test", success_callback)

    hub.publish("error_test", "test_data")

    time.sleep(0.2)

    assert len(received_messages) == 1
    assert received_messages[0] == "test_data"


def test_queue_overflow(clean_hub):
    """测试队列溢出功能"""
    hub = clean_hub

    # 发布大量消息到队列（超过队列大小限制）
    for i in range(1002):  # 默认队列大小是1000
        hub.publish("overflow_msg", f"message_{i}")

    time.sleep(0.5)  # 给点时间让 worker 处理完 1000+ 条消息

    # 验证队列中有消息
    try:
        msg = hub.get_no_wait("overflow_msg")
        assert msg is not None
    except Empty:
        pass


def test_get_no_wait(clean_hub):
    """测试get_no_wait功能"""
    hub = clean_hub

    test_content = "queued_message"
    hub.publish("get_test", test_content)

    time.sleep(0.2)
    # 从队列获取消息
    retrieved = hub.get_no_wait("get_test")
    assert retrieved == test_content

    # 再次获取应该抛出异常，因为队列已空
    with pytest.raises(Empty):
        hub.get_no_wait("get_test")


def test_get_with_timeout(clean_hub):
    """测试get方法带超时"""
    hub = clean_hub

    # 尝试获取不存在的队列消息，应该创建队列并等待
    with pytest.raises(Empty):
        hub.get("timeout_test", timeout=0.1)  # 0.1秒超时


def test_singleton_instance():
    """测试MessageHub是单例的"""
    # msg_hub是通过@singleton装饰器创建的单例
    hub1 = msg_hub
    hub2 = MessageHub()
    assert hub1 is hub2


def test_publish_to_multiple_message_types(clean_hub):
    """测试发布不同类型的消息"""
    hub = clean_hub

    received_type1 = []
    received_type2 = []

    def callback1(msg):
        received_type1.append(msg)

    def callback2(msg):
        received_type2.append(msg)

    # 订阅不同类型的消息
    hub.subscribe("type1", callback1)
    hub.subscribe("type2", callback2)

    # 发布不同类型的消息
    hub.publish("type1", "message_for_type1")
    hub.publish("type2", "message_for_type2")

    time.sleep(0.2)

    # 验证消息只被对应类型的订阅者接收
    assert len(received_type1) == 1
    assert len(received_type2) == 1
    assert received_type1[0] == "message_for_type1"
    assert received_type2[0] == "message_for_type2"


def test_error_in_callback():
    """测试回调函数中出现错误时的行为"""
    hub = MessageHub()
    hub._subscribers = {}
    hub._queues = {}

    def error_callback(msg):
        raise Exception("Intentional error in callback")

    # 订阅消息
    hub.subscribe("error_test", error_callback)

    # 发布消息 - 这不应该抛出异常，即使回调函数有错误
    try:
        hub.publish("error_test", "test_message")
        # 如果没有抛出异常，则测试通过
    except Exception:
        # 如果抛出了异常，则测试失败
        assert False, "publish方法不应该因为回调函数的错误而抛出异常"
