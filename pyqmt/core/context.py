from typing import Optional

from pyqmt.dal.cache import RedisCache
from pyqmt.dal.chores import Chores
from pyqmt.dal.haystore import Haystore
from pyqmt.sametime.executor import Callable, ExecutorPool


class Context:
    """全局作用域的用户变量，比如进程池、haystore, cache等"""
    pool: ExecutorPool
    cache: RedisCache
    chores: Chores
    haystore: Haystore

    def init_dal(self):
        # init chores database connection
        self.chores = Chores()

        # init haystore client
        self.haystore = Haystore()  # type: ignore

        # redis
        self.cache = RedisCache()  # type: ignore


    def create_executors_pool(self, before_start: Optional[Callable] = None,
        before_end: Optional[Callable] = None,
        max_workers: Optional[int] = None,):
        self.pool = ExecutorPool(before_start, before_end, max_workers)

    def close(self):
        if self.haystore is not None:
            self.haystore.close()

        if self.chores is not None:
            self.chores.close()

        if self.cache is not None:
            self.cache.close()

g = Context()
