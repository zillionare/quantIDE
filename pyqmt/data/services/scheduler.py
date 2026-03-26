"""Removed local sector/index sync scheduler.

The subject application no longer schedules local sector or index data sync.
"""


_REMOVED_MESSAGE = "本地板块/指数数据同步调度已从 pyqmt 主体移除。"


class DataSyncScheduler:
    """Stub kept only to surface the removal explicitly."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError(_REMOVED_MESSAGE)


def start_scheduler():
    raise RuntimeError(_REMOVED_MESSAGE)


def stop_scheduler():
    raise RuntimeError(_REMOVED_MESSAGE)


def run_sync_once(full_history: bool = False):
    raise RuntimeError(_REMOVED_MESSAGE)


def init_and_sync(db_path: str | None = None, full_history: bool = False):
    raise RuntimeError(_REMOVED_MESSAGE)