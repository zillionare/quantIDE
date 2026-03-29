from pathlib import Path

from quantide.service.strategy_runtime import StrategyRuntimeManager


def test_strategy_runtime_manager_extract_symbols():
    manager = StrategyRuntimeManager()
    assert manager._extract_symbols({"symbol": "000001.SZ"}) == ["000001.SZ"]
    assert manager._extract_symbols({"assets": ["000001.SZ", "000002.SZ"]}) == [
        "000001.SZ",
        "000002.SZ",
    ]
    assert manager._extract_symbols({}) == []


def test_strategy_runtime_manager_backtest_runtime_lifecycle():
    manager = StrategyRuntimeManager()
    manager.create_backtest_runtime(
        portfolio_id="p1",
        strategy_name="DemoStrategy",
        config={"symbol": "000001.SZ"},
        interval="1d",
        start_date="2024-01-01",
        end_date="2024-01-31",
        initial_cash=1000000,
    )
    run = manager.get_backtest_run("p1")
    assert run is not None
    assert run.status == "running"
    manager.complete_backtest_runtime("p1")
    run2 = manager.get_backtest_run("p1")
    assert run2 is not None
    assert run2.status == "finished"


def test_strategy_runtime_manager_save_and_load_specs(tmp_path: Path):
    manager = StrategyRuntimeManager()
    manager._state_file = lambda: tmp_path / "strategy_runtimes.json"
    manager._runtime_specs = {
        "paper:p1:s1": {
            "runtime_id": "paper:p1:s1",
            "mode": "paper",
            "strategy_name": "DemoStrategy",
            "strategy_id": "s1",
            "portfolio_id": "p1",
            "account_kind": "sim",
            "status": "stopped",
            "config": {"symbol": "000001.SZ"},
            "symbols": ["000001.SZ"],
            "principal": 1000000,
            "interval": "1m",
        }
    }
    manager._save_specs()

    manager2 = StrategyRuntimeManager()
    manager2._state_file = lambda: tmp_path / "strategy_runtimes.json"
    manager2._load_specs()
    assert "paper:p1:s1" in manager2._runtime_specs
    assert manager2._runtime_specs["paper:p1:s1"]["status"] == "stopped"
