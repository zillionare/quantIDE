import datetime
import os
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from quantide.config.paths import normalize_data_home
from quantide.config.settings import DEFAULT_TIMEZONE
from quantide.data.sqlite import db as _db


@pytest.fixture(scope="session")
def cfg():
    """Provide a small test-only config object for legacy-style test helpers."""
    yield SimpleNamespace(
        TIMEZONE=DEFAULT_TIMEZONE,
        epoch=datetime.date(2024, 1, 1),
        home=normalize_data_home(),
    )


@pytest.fixture(scope="session")
def db():
    """设置测试数据库 - session作用域"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        test_db_path = f.name

    try:
        # 初始化数据库
        _db._initialized = False
        _db.init(test_db_path)
        yield _db

    finally:
        try:
            os.unlink(test_db_path)
        except OSError:
            pass


@pytest.fixture
def clean_failed_tasks(db):
    """清理失败任务表"""
    db.execute("DELETE FROM failed_tasks")
    db.execute("DELETE FROM retry_logs")
    yield
    db.execute("DELETE FROM failed_tasks")
    db.execute("DELETE FROM retry_logs")


@pytest.fixture(scope="session")
def asset_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        src = Path(__file__).parent / "assets"
        dst = Path(tmpdir) / "assets"
        shutil.copytree(src, dst)
        stock_list_src = Path(__file__).resolve().parent.parent / "data" / "stock_list.parquet"
        if stock_list_src.exists():
            shutil.copy2(stock_list_src, dst / "stock_list.parquet")

        yield dst
        try:
            dst.unlink()
        except PermissionError:
            pass


@pytest.fixture(scope="session")
def calendar(asset_dir):
    from quantide.data.models.calendar import Calendar

    c = Calendar()
    c.load(asset_dir / "baseline_calendar.parquet")

    return c


@pytest.fixture(scope="session")
def calendar_data(asset_dir):
    file = asset_dir / "baseline_calendar.parquet"
    return pq.read_table(file)


@pytest.fixture(scope="session")
def year_2024_trade_dates(calendar_data):
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 12, 31)
    # Arrow Table 无 query 接口，需转 pandas 并设定索引为 date
    df = calendar_data.to_pandas()
    return df.query("is_open == 1 and index >= @start and index <= @end").index.tolist()


@pytest.fixture(scope="session")
def limit_price(asset_dir):
    file = asset_dir / "2024_limit_price.parquet"
    return pd.read_parquet(file)


@pytest.fixture(scope="session")
def st(asset_dir):
    file = asset_dir / "2024_st_info.parquet"
    return pd.read_parquet(file)


@pytest.fixture(scope="session")
def bars(asset_dir):
    return pd.read_parquet(asset_dir / "2024_bars.parquet")


@pytest.fixture(scope="session")
def adjust_factor(asset_dir):
    df = pd.read_parquet(asset_dir / "2024_adjust_factor.parquet")

    return df


@pytest.fixture(scope="session")
def bars_ext(asset_dir):
    return pd.read_parquet(asset_dir / "2024_bars_ext_cols.parquet")


@pytest.fixture(scope="session")
def bars_mini_set(asset_dir):
    return pd.read_parquet(asset_dir / "bars_2021_2024.small.parquet")
