import asyncio
import itertools
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List, Type

import pandas as pd
from loguru import logger

from pyqmt.config import cfg, init_config
from pyqmt.core.enums import FrameType
from pyqmt.core.strategy import BaseStrategy
from pyqmt.data import init_data
from pyqmt.service.runner import BacktestRunner


def _run_task(
    strategy_cls: Type[BaseStrategy],
    config: Dict[str, Any],
    start_date: date,
    end_date: date,
    interval: str,
    initial_cash: float,
    db_path: str = ":memory:",
    home_dir: str | None = None,
) -> Dict[str, Any]:
    """Worker function for running backtest in a separate process."""
    # Initialize config and data for this process
    init_config()

    # Use provided home_dir or default from config
    data_home = home_dir or cfg.home
    # Only init db if db_path is NOT provided or if it's not :memory:
    # Actually, if db_path is provided (like :memory:), we should skip default solo.db init
    # because runner.run will init db with db_path.
    init_data(data_home, init_db=False)

    # Re-configure logger for subprocess if needed
    # logger.remove()
    # logger.add(sys.stderr, level="INFO")

    runner = BacktestRunner()
    # Run the async backtest in a new event loop
    result = asyncio.run(
        runner.run(
            strategy_cls,
            config,
            start_date,
            end_date,
            FrameType(interval),
            initial_cash,
            db_path=db_path,
        )
    )

    # Retrieve strategy logs from the local (in-memory) DB
    from pyqmt.data.sqlite import db
    portfolio_id = result.get("portfolio_id")
    if portfolio_id:
        # 1. Get Portfolio record (required for FK)
        pf = db.get_portfolio(portfolio_id)
        if pf:
            result["portfolio"] = pf.to_dict()

        # 2. Get Strategy Logs
        logs_df = db.get_strategy_logs(portfolio_id)
        if not logs_df.is_empty():
            # Convert to list of dicts for safe serialization/transport
            result["strategy_logs"] = logs_df.to_dicts()

    return result


class GridSearch:
    def __init__(
        self,
        strategy_cls: Type[BaseStrategy],
        base_config: Dict[str, Any],
        param_grid: Dict[str, List[Any]],
        start_date: date,
        end_date: date,
        interval: str = "1d",
        initial_cash: float = 1_000_000,
        max_workers: int | None = None,
    ):
        self.strategy_cls = strategy_cls
        self.base_config = base_config
        self.param_grid = param_grid
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.initial_cash = initial_cash
        self.max_workers = max_workers

    def run(self, save_logs: bool = False, home_dir: str | None = None) -> pd.DataFrame:
        """Run grid search in parallel processes.

        Args:
            save_logs: Whether to merge strategy logs from workers into the main database.
            home_dir: Optional path to data home directory (useful for testing)
        """
        # Generate all parameter combinations
        keys = self.param_grid.keys()
        values = self.param_grid.values()
        combinations = list(itertools.product(*values))

        configs = []
        for combo in combinations:
            # Create a new config for this combination
            new_config = self.base_config.copy()
            new_config.update(dict(zip(keys, combo)))
            configs.append(new_config)

        logger.info(f"Starting grid search with {len(configs)} combinations...")

        results = []
        from pyqmt.data.sqlite import StrategyLog, db

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_config = {
                executor.submit(
                    _run_task,
                    self.strategy_cls,
                    config,
                    self.start_date,
                    self.end_date,
                    self.interval,
                    self.initial_cash,
                    db_path=":memory:",  # Use isolated memory DB
                    home_dir=home_dir,
                ): config
                for config in configs
            }

            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    res = future.result()
                    metrics = res.get("metrics", {})

                    # Merge strategy logs if requested
                    if save_logs:
                        # 1. Insert Portfolio first (to satisfy FK)
                        if "portfolio" in res:
                            pf_data = res["portfolio"]
                            if pf_data:
                                from pyqmt.data.sqlite import Portfolio
                                pf = Portfolio(**pf_data)
                                # Check if portfolio already exists to avoid duplicates
                                existing_pf = db.get_portfolio(pf.portfolio_id)
                                if not existing_pf:
                                    db.insert_portfolio(pf)

                        # 2. Insert Strategy Logs
                        if "strategy_logs" in res:
                            logs_data = res["strategy_logs"]
                            if logs_data:
                                logs = [StrategyLog(**log) for log in logs_data]
                                db.insert_strategy_logs(logs)

                    # Add config params to result
                    row = metrics.copy()
                    # Flatten config into row for easier analysis
                    # Only add the varying parameters
                    for k in self.param_grid.keys():
                        row[k] = config.get(k)

                    row["portfolio_id"] = res.get("portfolio_id")
                    results.append(row)
                except Exception as e:
                    logger.error(f"Backtest failed for config {config}: {e}")

        df = pd.DataFrame(results)
        if not df.empty and "sharpe" in df.columns:
             df = df.sort_values(by="sharpe", ascending=False)

        return df
