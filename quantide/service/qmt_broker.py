"""Removed local QMT broker.

The quantide subject application no longer provides a direct local broker path.
Use qmt-gateway for live trading.
"""


_REMOVED_MESSAGE = "QMTBroker 已从 quantide 主体移除，请改用 qmt-gateway。"


class QMTBroker:
    """Stub kept only to provide a clear migration error."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError(_REMOVED_MESSAGE)