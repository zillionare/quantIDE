import datetime
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest
from loguru import logger

from quantide.core.enums import FrameType
from quantide.core.strategy import BaseStrategy
from quantide.data.sqlite import db as main_db
from quantide.service.grid_search import GridSearch


class MockStrategy(BaseStrategy):
    async def init(self):
        # Record initial parameter
        param1 = self.config.get("param1", 0)

        # Debug: Check if portfolio exists
        from quantide.data.sqlite import db
        pfs = db.portfolios_all()
        logger.info(f"Portfolios in DB: {pfs}")
        logger.info(f"My portfolio_id: {self.broker.portfolio_id}")

        self.record("param1", float(param1), self._current_time or datetime.datetime.now())

    async def on_start(self):
        pass

    async def on_day_open(self, tm: datetime.datetime):
        pass

    async def on_bar(
        self, tm: datetime.datetime, quote: Dict[str, Any], frame_type: FrameType
    ):
        pass


@pytest.fixture
def grid_search_env(asset_dir):
    """Setup a temporary environment for grid search worker processes."""
    # Create a temporary directory structure mimicking the production environment
    tmp_home = tempfile.mkdtemp()
    tmp_path = Path(tmp_home)

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)

    # Copy necessary data files from asset_dir
    # Note: asset_dir comes from conftest.py which copies from tests/assets

    # 1. Calendar
    shutil.copy(asset_dir / "baseline_calendar.parquet", data_dir / "calendar.parquet")

    # 2. Stock List (create a dummy one if not present)
    stock_list_path = data_dir / "stock_list.parquet"
    if not stock_list_path.exists():
        # Create a dummy stock list
        df = pd.DataFrame({
            "asset": ["000001.SZ"],
            "name": ["PingAn"],
            "list_date": [datetime.date(2000, 1, 1)],
            "delist_date": [None]
        })
        df.to_parquet(stock_list_path)

    # 3. Daily Bars
    bars_dir = data_dir / "bars/daily"
    bars_dir.mkdir(parents=True)

    # Create dummy bars for 2024 (partitioned or single file depending on implementation)
    # The implementation in stores/bars.py checks suffix or partition by year.
    # DailyBarsStore uses "DailyBars" and partition_by="year" if not .parquet suffix.
    # But init_data points to "data/bars/daily" directory.
    # So we should create "year=2024/part.parquet" or similar if hive partitioned.
    # However, DailyBarsStore logic:
    # if path.suffix == ".parquet": partition_by = None
    # else: partition_by = "year"

    # We are pointing to a directory "data/bars/daily". So it expects partitioned data.
    # Let's create partition_key_year=2024 directory.
    # quantide uses 'partition_key_year' as the partition column name.
    year_dir = bars_dir / "partition_key_year=2024"
    year_dir.mkdir(parents=True)

    dates = pd.date_range("2024-01-01", "2024-01-31", freq="B")
    df = pd.DataFrame({
        "date": dates,
        "asset": "000001.SZ",
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1000.0,
        "amount": 10000.0,
        "adjust": 1.0,
        "turnover": 0.1,
        "st": False,
        "up_limit": 11.0,
        "down_limit": 9.0
    })
    # Polars/PyArrow writes partitioned datasets, but we can just write a file inside year=2024
    # Note: If we manually write inside year=2024, we don't need 'year' column in file usually,
    # but PyArrow dataset discovery handles it.
    df.to_parquet(year_dir / "data.parquet")

    yield str(tmp_path)

    # Cleanup
    shutil.rmtree(tmp_home)


def test_grid_search_save_logs(grid_search_env, db):
    """Test grid search running and merging logs."""

    # Clean up strategy logs before test (db fixture is session scoped but we can clean tables)
    # The 'db' fixture yields the singleton which is connected to a temp file for the session.
    if "strategy_logs" in db.tables:
        db.execute("DELETE FROM strategy_logs")

    base_config = {
        "universe": ["000001.SZ"],
    }

    param_grid = {
        "param1": [1, 2]
    }

    start_date = datetime.date(2024, 1, 4)
    end_date = datetime.date(2024, 1, 8)

    gs = GridSearch(
        strategy_cls=MockStrategy,
        base_config=base_config,
        param_grid=param_grid,
        start_date=start_date,
        end_date=end_date,
        initial_cash=100000,
        max_workers=2
    )

    # Run with save_logs=True and passing the temp home dir
    # Note: This runs in parallel processes.
    results_df = gs.run(save_logs=True, home_dir=grid_search_env)

    # Check results
    assert len(results_df) == 2
    # Check that varying parameter is in result
    assert "param1" in results_df.columns
    assert set(results_df["param1"]) == {1, 2}

    # Check if logs are merged into the main DB
    logs = db.get_strategy_logs()

    # We expect logs from both runs
    # MockStrategy records "param1"
    assert len(logs) >= 2

    # Verify content
    # Convert polars df to pandas or list for checking
    logs_df = logs.to_pandas()

    # Check keys
    assert "param1" in logs_df["key"].values

    # Check values match params
    logged_values = set(logs_df[logs_df["key"] == "param1"]["value"])
    assert 1.0 in logged_values
    assert 2.0 in logged_values

    # Check portfolio_ids match results
    result_pids = set(results_df["portfolio_id"])
    log_pids = set(logs_df["portfolio_id"])
    assert result_pids == log_pids
