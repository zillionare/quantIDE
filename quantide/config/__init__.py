#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import datetime
import copy
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytz
from loguru import logger


def _build_default_config() -> SimpleNamespace:
    return SimpleNamespace(
        TIMEZONE=pytz.timezone("Asia/Shanghai"),
        server=SimpleNamespace(
            key="quantide",
            host="0.0.0.0",
            port=8130,
            prefix="/quantide",
        ),
        users=[
            {"name": "admin", "password": "admin-password"},
            {"name": "trader", "password": "trader-password"},
        ],
        apikeys=SimpleNamespace(
            timeout=300,
            clients=[{"client": "client1", "key": "key1"}],
        ),
        livequote=SimpleNamespace(mode="gateway"),
        gateway=SimpleNamespace(
            base_url="http://127.0.0.1:8000",
            username="admin",
            password="1234",
            timeout=10,
        ),
        runtime=SimpleNamespace(
            mode="live",
            market_adapter="",
            broker_adapter="",
        ),
        brokers=[
            {"kind": "backtest", "id": "backtest", "default": True},
            {"kind": "simulation", "id": "sim1"},
            {"kind": "simulation", "id": "sim2"},
        ],
        notify=SimpleNamespace(
            dingtalk=SimpleNamespace(
                access_token="dingtalk-access-token",
                secret="dingtalk-secret",
                keyword="dingtalk-keyword",
            ),
            mail=SimpleNamespace(
                mail_to=["nonexist@quantide.cn"],
                mail_from="nonexist@quantide.cn",
                mail_server="smtp.exmail.qq.com",
            ),
        ),
        data=SimpleNamespace(source="tushare"),
        home=str(Path("~/.quantide").expanduser()),
        tushare_token="",
        epoch=datetime.date(2005, 1, 1),
    )


cfg = SimpleNamespace()


def get_config_dir() -> str:
    _dir = Path(__file__).parent
    logger.info(f"config dir: {_dir}")
    return str(_dir)


def endpoint():
    from .runtime import get_runtime_config

    major, minor, *_ = version("quantide").split(".")
    prefix = get_runtime_config().app_prefix.rstrip("/")
    return f"{prefix}/v{major}.{minor}"


def init_config() -> Any:
    defaults = copy.deepcopy(_build_default_config())
    cfg.__dict__.clear()
    cfg.__dict__.update(defaults.__dict__)
    cfg.home = str(Path(str(cfg.home)).expanduser())
    return cfg


init_config()



__all__ = ["cfg", "endpoint", "get_config_dir", "init_config"]
