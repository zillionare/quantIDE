from typing import Callable, Optional

from pyqmt.sametime.actor import Actor, StopOnSightActor
from pyqmt.sametime.executor import Executor, ExecutorPool

pool: ExecutorPool | None = None


class PoolExistsError(BaseException):
    pass


def create_get_executor_pool(
    before_start: Callable, before_end: Callable, max_workers: Optional[int] = None
):
    global pool

    if pool is not None:
        raise PoolExistsError()

    pool = ExecutorPool(before_start, before_end, max_workers)
    return pool


__all__ = [
    "Actor",
    "StopOnSightActor",
    "Executor",
    "ExecutorPool",
    "pool",
    "PoolExistsError",
]
