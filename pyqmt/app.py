"""Main module."""

import logging
from concurrent.futures import ProcessPoolExecutor

import cfg4py
from apscheduler.schedulers.background import BackgroundScheduler
from blacksheep import Application, get

from pyqmt import dal
from pyqmt.config import get_config_dir
from pyqmt.service import sync

logger = logging.getLogger(__name__)

app = Application()
sched = BackgroundScheduler(timezone="Asia/Shanghai")

@get("/status")
async def status():
    return "OK"


@app.on_start
async def before_start(app: Application) -> None:
    cfg = cfg4py.init(get_config_dir())
    # init chores database connection
    dal.init()

    cfg.executor = ProcessPoolExecutor() #type: ignore
    sched.add_job(sync.create_sync_jobs, args=(sched,))
    sched.start()


@app.after_start
async def after_start(app: Application) -> None:
    pass


@app.on_stop
async def on_stop(app: Application) -> None:
    cfg = cfg4py.get_instance()
    dal.close()
