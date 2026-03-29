import datetime
import tempfile
from pathlib import Path

import polars as pl
import pandas as pd
import pytest
from quantide.core.enums import BidType, OrderSide
from quantide.data.sqlite import Asset, Order, Position, Trade, db
from quantide.service.metrics import bills, metrics


@pytest.fixture(scope="function")
def setup_db():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_metrics.db"
        db.init(db_path)
        yield db
        db.close()


def test_bills(setup_db):
    portfolio_id = "test_p"
    tm = datetime.datetime(2024, 1, 1, 10, 0)
    dt = tm.date()

    # 1. 准备当前组合的数据
    db.insert_order(
        Order(
            portfolio_id,
            "000001.SZ",
            OrderSide.BUY,
            100,
            BidType.MARKET,
            tm=tm,
            qtoid="o1",
        )
    )
    db.insert_trades(
        Trade(
            portfolio_id,
            "t1",
            "o1",
            "f1",
            "000001.SZ",
            100,
            10.0,
            1000.0,
            tm,
            OrderSide.BUY,
            "c1",
        )
    )
    db.upsert_positions(
        Position(portfolio_id, dt, "000001.SZ", 100, 100, 10.0, 0.0, 1000.0)
    )
    db.upsert_asset(Asset(portfolio_id, dt, 1000000.0, 1000000.0, 0.0, 0.0, 1000000.0))

    # 2. 准备另一个组合的数据以验证过滤
    other_p = "other_p"
    db.insert_order(
        Order(
            other_p, "000002.SZ", OrderSide.BUY, 200, BidType.MARKET, tm=tm, qtoid="o2"
        )
    )
    db.insert_trades(
        Trade(
            other_p,
            "t2",
            "o2",
            "f2",
            "000002.SZ",
            200,
            20.0,
            4000.0,
            tm,
            OrderSide.BUY,
            "c2",
        )
    )

    # 3. 调用 bills
    res = bills(portfolio_id)

    # 4. 验证
    assert isinstance(res, dict)
    assert len(res["orders"]) == 1
    assert res["orders"]["portfolio_id"][0] == portfolio_id
    assert len(res["trades"]) == 1
    assert res["trades"]["portfolio_id"][0] == portfolio_id
    assert len(res["positions"]) == 1
    assert len(res["assets"]) == 1


def test_metrics_basic(setup_db):
    portfolio_id = "test_metrics_p"

    # 准备 5 天的资产数据以计算收益率
    for i in range(5):
        dt = datetime.date(2024, 1, i + 1)
        total = 1000000.0 * (1 + 0.01 * i)  # 每日增长
        db.upsert_asset(Asset(portfolio_id, dt, 1000000.0, 1000000.0, 0.0, 0.0, total))

    # 调用 metrics
    stats = metrics(portfolio_id)

    assert isinstance(stats, pd.DataFrame)
    assert "Strategy" in stats.columns
    # 验证包含核心指标
    assert "Sharpe" in stats.index
    assert "Cumulative Return" in stats.index


def test_metrics_with_benchmark(setup_db):
    portfolio_id = "test_bench_p"

    # 准备策略资产数据 (1月1日到1月5日)
    for i in range(5):
        dt = datetime.date(2024, 1, i + 1)
        total = 1000000.0 * (1 + 0.01 * i)
        db.upsert_asset(Asset(portfolio_id, dt, 1000000.0, 1000000.0, 0.0, 0.0, total))

    # 准备基准数据 (范围必须覆盖 1月2日到1月5日，因为收益率是从第二天开始的)
    bench_data = pl.DataFrame(
        {
            "dt": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 4),
                datetime.date(2024, 1, 5),
            ],
            "returns": [0.001, 0.002, 0.001, 0.003, 0.002],
        }
    )

    stats = metrics(portfolio_id, baseline_returns=bench_data)

    assert isinstance(stats, pd.DataFrame)
    assert "Strategy" in stats.columns
    assert "Benchmark" in stats.columns


def test_metrics_insufficient_benchmark(setup_db):
    portfolio_id = "test_fail_p"

    # 策略日期 1-5
    for i in range(5):
        db.upsert_asset(
            Asset(
                portfolio_id,
                datetime.date(2024, 1, i + 1),
                100.0,
                100.0,
                0.0,
                0.0,
                100.0,
            )
        )

    # 基准日期不足 (缺 1月5日)
    bench_data = pl.DataFrame(
        {
            "dt": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 4),
            ],
            "returns": [0.01, 0.01, 0.01, 0.01],
        }
    )

    with pytest.raises(ValueError, match="is insufficient"):
        metrics(portfolio_id, baseline_returns=bench_data)


def test_metrics_empty_data(setup_db):
    # 数据不足（少于 2 天无法计算收益率）
    db.upsert_asset(
        Asset("empty", datetime.date(2024, 1, 1), 100.0, 100.0, 0.0, 0.0, 100.0)
    )
    assert metrics("empty") is None
