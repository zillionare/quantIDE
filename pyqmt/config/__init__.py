#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import datetime
from importlib.metadata import version
from pathlib import Path

import cfg4py
import pytz
from loguru import logger

from .schema import Config


def get_config_dir() -> str:
    _dir = Path(__file__).parent
    logger.info(f"config dir: {_dir}")
    return str(_dir)


def endpoint():
    cfg = cfg4py.get_instance()

    major, minor, *_ = version("zillionare-pyqmt").split(".")
    prefix = cfg.server.prefix.rstrip("/")
    return f"{prefix}/v{major}.{minor}"


def init_config(config_dir: str | Path | None = None):
    config_dir = config_dir or get_config_dir()
    cfg4py.init(str(config_dir))

    cfg_ = cfg4py.get_instance()
    if not hasattr(cfg_, "epoch"):
        cfg_.epoch = datetime.date(2005, 1, 1)  # type: ignore
    cfg_.TIMEZONE = pytz.timezone("Asia/Shanghai")

    # 展开 home 路径中的 ~
    if hasattr(cfg_, "home") and isinstance(cfg_.home, str):
        cfg_.home = str(Path(cfg_.home).expanduser())


cfg: Config = cfg4py.get_instance()



__all__ = ["cfg", "endpoint", "init_config"]
