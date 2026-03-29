"""Removed local index sync service.

This module exists only to report that subject-side index synchronization has
been removed.
"""


_REMOVED_MESSAGE = "指数同步功能已从 quantide 主体移除。"


class IndexSyncService:
    """Stub kept only to surface the removal explicitly."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError(_REMOVED_MESSAGE)