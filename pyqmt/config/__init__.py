#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import datetime
import os
from importlib.metadata import version
from pathlib import Path

import cfg4py
import pytz

from .schema import Config


def get_config_dir() -> str:
    server_role = os.environ.get(cfg4py.envar)

    if server_role == "DEV":
        _dir = Path(__file__).parent
    elif server_role == "TEST":
        _dir = Path.home() / ".zillionare" / "pyqmt_test" / "config"
    else:
        _dir = Path.home() / ".zillionare" / "pyqmt" / "config"

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


cfg: Config = cfg4py.get_instance()

cfg.TIMEZONE = pytz.timezone("Asia/Shanghai")

__all__ = ["cfg", "endpoint", "init_config"]
