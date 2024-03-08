import sqlite3

import cfg4py

from pyqmt.config import get_config_dir
from pyqmt.core.timeframe import tf

from .haystore import Haystore
from .cache import RedisCache


def init_dal():
    cfg = cfg4py.init(get_config_dir())

    # init chores database connection
    cfg.chores_db = sqlite3.connect(cfg.chores_db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)  # type: ignore
    cfg.chores_db.row_factory = sqlite3.Row

    # init haystore client
    cfg.haystore = Haystore()  # type: ignore
    cfg.haystore.connect()

    # redis
    cfg.cache = RedisCache()  # type: ignore

    tf.init()
