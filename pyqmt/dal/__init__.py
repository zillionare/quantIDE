
import cfg4py
import duckdb

from pyqmt.core.timeframe import tf

from .cache import RedisCache
from .haystore import Haystore


def init_dal():
    cfg = cfg4py.get_instance()

    # init chores database connection
    cfg.chores_db = duckdb.connect(cfg.chores_db_path) # type: ignore

    # init haystore client
    cfg.haystore = Haystore()  # type: ignore

    # redis
    cfg.cache = RedisCache()  # type: ignore

    tf.init()
