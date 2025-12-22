#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import os
from importlib.metadata import version
from pathlib import Path
import pytz
from pytz.tzinfo import DstTzInfo

import cfg4py


def get_config_dir()->str:
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


def init_config(config_dir: str|Path|None=None):
    config_dir = config_dir or get_config_dir()
    cfg =cfg4py.init(str(config_dir))

    return cfg4py.get_instance()


cfg = cfg4py.get_instance()

cfg.TIMEZONE: DstTzInfo = pytz.timezone("Asia/Shanghai") # type: ignore

__all__ = ["cfg", "endpoint", "init_config"]
