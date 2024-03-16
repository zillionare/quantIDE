
from tkinter import N

import cfg4py

from pyqmt.dal.cache import RedisCache
from pyqmt.dal.chores import Chores
from pyqmt.dal.haystore import Haystore

chorse: Chores | None = None
cache: RedisCache | None = None
haystore: Haystore | None = None

def init():
    global chores, haystore, cache

    # init chores database connection
    chores = Chores()

    # init haystore client
    haystore = Haystore()  # type: ignore

    # redis
    cache = RedisCache()  # type: ignore

def close():
    global chores, haystore, cache

    if haystore is not None:
        haystore.close()

    if chorse is not None:
        chores.close()

    if cache is not None:
        cache.close()

__all__ = ["chores", "haystore", "cache"]
