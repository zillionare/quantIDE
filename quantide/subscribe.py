"""Removed local quote subscription helpers.

The quantide subject application no longer opens a local quote subscription loop.
Use qmt-gateway for live quote delivery.
"""


_REMOVED_MESSAGE = "本地行情订阅功能已从 quantide 主体移除，请改用 qmt-gateway。"


def batch(iterable, size):
    raise RuntimeError(_REMOVED_MESSAGE)


def on_subscribe_callback(data):
    raise RuntimeError(_REMOVED_MESSAGE)


def subscribe_live():
    raise RuntimeError(_REMOVED_MESSAGE)


def sync_1m_bars(codes):
    raise RuntimeError(_REMOVED_MESSAGE)


def _run_qmt_loop():
    raise RuntimeError(_REMOVED_MESSAGE)