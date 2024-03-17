#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import os
from importlib.metadata import version

import cfg4py
import duckdb
import pytest
from pytest import fixture

from pyqmt.core.context import g
from pyqmt.core.timeframe import tf
from pyqmt.service.sync import sync_calendar

TABLE_PARAMETER = "{TABLE_PARAMETER}"
DROP_TABLE_SQL = f"DROP TABLE {TABLE_PARAMETER};"
GET_TABLES_SQL = "SELECT name FROM sqlite_schema WHERE type='table';"


def get_config_dir():
    return os.path.dirname(__file__)


def endpoint():
    cfg = cfg4py.get_instance()

    major, minor, *_ = version("zillionare-pyqmt").split(".")
    prefix = cfg.server.prefix.rstrip("/")
    return f"{prefix}/v{major}.{minor}"


def init_chores():
    cfg = cfg4py.init(get_config_dir())

    scripts = os.path.join(os.path.dirname(__file__), "../../scripts/duckdb.txt")
    with open(scripts, "r", encoding="utf-8") as f:
        create_tables = f.read()

    with duckdb.connect(cfg.chores_db_path) as conn:
        for (name,) in conn.sql("show tables").fetchall():
            conn.sql(f"drop table {name}")

        conn.sql(create_tables)


def init_haystore():
    cmd = "truncate database if exists tests"
    g.haystore.client.command(cmd)

    # create tables
    scripts = os.path.join(os.path.dirname(__file__), "../../scripts/clickhouse.txt")
    with open(scripts, "r", encoding="utf-8") as f:
        content = f.read()

        for sql in content.split("\n\n"):
            if len(sql) < 5:
                continue
            g.haystore.client.command(sql)

@pytest.fixture(scope="function", autouse=True)
def setup():
    cfg4py.init(get_config_dir())

    g.init_dal()
    init_haystore()
    init_chores()
    sync_calendar()
    tf.init()
