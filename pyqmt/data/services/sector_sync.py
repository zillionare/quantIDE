"""Removed local sector sync service.

This module exists only to report that subject-side sector synchronization has
been removed.
"""


_REMOVED_MESSAGE = "板块同步功能已从 pyqmt 主体移除。"


class SectorSyncService:
    """Stub kept only to surface the removal explicitly."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError(_REMOVED_MESSAGE)