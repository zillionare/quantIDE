import datetime

import polars as pl
import pytest

from pyqmt.data.models.calendar import calendar
from pyqmt.data.sqlite import Asset, Position, db
from pyqmt.service.backtest_broker import BacktestBroker


@pytest.fixture
def broker():
    # 初始化数据库
    db.init(":memory:")

    # 初始化日历，包含 2023 年最后一天作为 Day 0
    import pyarrow as pa

    calendar.day_frames = pa.array(
        [datetime.date(2023, 12, 31)]
        + [datetime.date(2024, 1, i) for i in range(1, 31)]
    )

    class MockDataFeed:
        def get_trade_price_limits(self, asset, dt):
            return 9.0, 11.0

        def get_price_for_match(self, asset, tm):
            return pl.DataFrame([])

        def get_close_factor(self, assets, start, end):
            return pl.DataFrame(
                {"dt": [], "asset": [], "close": [], "factor": []},
                schema={"dt": pl.Date, "asset": pl.String, "close": pl.Float64, "factor": pl.Float64},
            )

    # 使用内存数据库或临时数据库进行测试
    # 假设 db 已经配置好
    b = BacktestBroker(
        bt_start=datetime.date(2024, 1, 1),
        bt_end=datetime.date(2024, 1, 10),
        portfolio_id="test_bt",
        data_feed=MockDataFeed(),
        principal=1000000,
    )
    return b


def test_set_clock_filling(broker):
    # 1. 初始状态
    dt1 = datetime.date(2024, 1, 1)
    broker._clock = datetime.datetime.combine(dt1, datetime.time(15, 0))

    # 插入初始资产和持仓
    db.upsert_asset(
        Asset(
            portfolio_id="test_bt",
            dt=dt1,
            principal=1000000,
            cash=1000000,
            frozen_cash=0,
            market_value=0,
            total=1000000,
        )
    )

    # 2. 模拟拨动时钟到 2024-01-03
    dt3 = datetime.date(2024, 1, 3)
    broker.set_clock(datetime.datetime.combine(dt3, datetime.time(9, 30)))

    # 3. 验证 2024-01-02 的资产记录是否已填实
    dt2 = datetime.date(2024, 1, 2)
    asset2 = db.get_asset(dt2, "test_bt")
    assert asset2 is not None
    assert asset2.dt == dt2
    assert asset2.cash == 1000000
    assert asset2.market_value == 0
    assert asset2.total == 1000000


def test_equity_conservation_on_factor_change(broker, monkeypatch):
    """验证复权事件发生时，权益是否守恒（现金补偿逻辑）"""
    # 模拟行情数据：2024-01-02 发生 10 送 10
    # 01-01: price=10.0, factor=1.0
    # 01-02: price=5.0,  factor=2.0
    class MockDataFeed:
        def get_close_factor(self, assets, start, end):
            data = [
                {"dt": datetime.date(2024, 1, 1), "asset": "000001.SZ", "close": 10.0, "factor": 1.0},
                {"dt": datetime.date(2024, 1, 2), "asset": "000001.SZ", "close": 5.0, "factor": 2.0},
            ]
            return pl.DataFrame([
                row for row in data
                if start <= row["dt"] <= end and row["asset"] in assets
            ])
        def get_trade_price_limits(self, asset, dt): return 4.5, 5.5

    # 替换 broker 中的 data_feed
    monkeypatch.setattr(broker, "_data_feed", MockDataFeed())

    dt1 = datetime.date(2024, 1, 1)
    broker._clock = datetime.datetime.combine(dt1, datetime.time(15, 0))
    broker._cash = 900000

    # 1. 插入 01-01 的初始状态
    db.upsert_asset(Asset(
        portfolio_id="test_bt", dt=dt1, principal=1000000,
        cash=900000, frozen_cash=0, market_value=100000, total=1000000
    ))
    db.upsert_positions(Position(
        portfolio_id="test_bt", dt=dt1, asset="000001.SZ",
        shares=10000, price=10.0, avail=10000, mv=100000, profit=0
    ))

    # 2. 拨动时钟到 2024-01-03，触发 01-02 的展仓
    dt3 = datetime.date(2024, 1, 3)
    # 在拨动之前，需要确保内存中的 _positions 也是同步的，因为 set_clock 内部可能用到
    broker._positions["000001.SZ"] = Position(
        portfolio_id="test_bt", dt=dt1, asset="000001.SZ",
        shares=10000, price=10.0, avail=10000, mv=100000, profit=0
    )

    broker.set_clock(datetime.datetime.combine(dt3, datetime.time(9, 30)))

    # 3. 验证 01-02 的数据
    asset2 = db.get_asset(datetime.date(2024, 1, 2), "test_bt")
    pos2 = db.get_positions(datetime.date(2024, 1, 2), "test_bt")

    # 验证逻辑：
    # CashAdj = (new_factor - prev_factor) * shares * close
    #         = (2.0 - 1.0) * 10000 * 5.0 = 50000
    # New Cash = 900000 + 50000 = 950000
    # New MV   = 10000 * 5.0 = 50000
    # Total    = 950000 + 50000 = 1000000 (守恒)

    assert asset2.cash == 950000
    assert asset2.market_value == 50000
    assert asset2.total == 1000000

    # 验证持仓记录
    assert pos2.height == 1
    assert pos2["mv"][0] == 50000
    assert pos2["price"][0] == 10.0 # 成本价不变


def test_set_clock_with_halted_stock(broker, monkeypatch):
    """测试停牌情况：如果 close 为 null，则应沿用上一日价格"""

    # 模拟行情数据：2024-01-02 停牌 (返回空 DataFrame 或 close 为 null)
    class MockDataFeed:
        def get_close_factor(self, assets, start, end):
            # 返回一个空的 DataFrame 模拟停牌
            return pl.DataFrame(
                {"dt": [], "asset": [], "close": [], "factor": []},
                schema={"dt": pl.Date, "asset": pl.String, "close": pl.Float64, "factor": pl.Float64},
            )
        def get_trade_price_limits(self, asset, dt): return 9.0, 11.0

    monkeypatch.setattr(broker, "_data_feed", MockDataFeed())

    dt1 = datetime.date(2024, 1, 1)
    broker._clock = datetime.datetime.combine(dt1, datetime.time(15, 0))
    broker._cash = 900000

    # 插入初始持仓
    db.upsert_positions(
        Position(
            portfolio_id="test_bt",
            dt=dt1,
            asset="000001.SZ",
            shares=1000,
            avail=1000,
            price=10.0,
            mv=10000.0,
            profit=0.0,
        )
    )
    db.upsert_asset(
        Asset(
            portfolio_id="test_bt",
            dt=dt1,
            principal=1000000,
            cash=900000,
            frozen_cash=0,
            market_value=10000,
            total=910000,
        )
    )

    # 拨动到 2024-01-03
    dt3 = datetime.date(2024, 1, 3)
    broker.set_clock(datetime.datetime.combine(dt3, datetime.time(9, 30)))

    # 验证 2024-01-02 的持仓（应沿用 10.0 的价格）
    dt2 = datetime.date(2024, 1, 2)
    pos2 = db.get_positions(dt2, "test_bt")
    assert len(pos2) == 1
    assert pos2["price"][0] == 10.0  # 沿用上一日价格
    assert pos2["mv"][0] == 10000.0
    assert pos2["profit"][0] == 0.0


def test_initialization_day_0(broker):
    # 验证 __init__ 是否创建了 Day 0 的记录
    # calendar.day_shift(2024-01-01, -1) 在我们的 MockCalendar 中应该是 2023-12-31?
    # 不，MockCalendar.day_frames 只定义了 2024-01-01 之后。
    # 我们需要检查 day_shift 的实现。

    # 检查数据库中是否存在 bt_start 之前的记录
    assets = db.assets_all("test_bt")
    assert len(assets) == 1

    # 因为我们的 MockCalendar 只有 2024-01-01 之后的日期，
    # day_shift(2024-01-01, -1) 可能会返回 2023-12-31 (简单日期减法)
    # 或者是 None 如果它严格依赖 day_frames。

    # 让我们直接检查记录的日期
    day0_dt = assets["dt"][0]
    assert day0_dt < datetime.date(2024, 1, 1)


def test_set_clock_no_redundant_fill(broker):
    # 初始状态：Day 0 已存在，_clock 为 Day 0
    # 调用 set_clock(bt_start) 不应触发填补，也不应增加记录

    dt1 = datetime.date(2024, 1, 1)
    broker.set_clock(dt1)

    assets = db.assets_all("test_bt")
    # 仍然只有 Day 0 一条记录
    assert len(assets) == 1

    # 调用 set_clock(bt_start + 1) 应增加一条记录 (bt_start)
    dt2 = datetime.date(2024, 1, 2)
    broker.set_clock(dt2)

    assets = db.assets_all("test_bt")
    assert len(assets) == 2
    assert dt1 in assets["dt"].to_list()


def test_set_clock_multi_assets(broker, monkeypatch):
    """测试多资产向量化展仓"""

    class MockDataFeed:
        def get_close_factor(self, assets, start, end):
            return pl.DataFrame([
                {"dt": datetime.date(2024, 1, 2), "asset": "000001.SZ", "close": 11.0, "factor": 1.0},
                {"dt": datetime.date(2024, 1, 2), "asset": "000002.SZ", "close": 22.0, "factor": 1.0},
            ])
        def get_trade_price_limits(self, asset, dt): return 9.0, 25.0

    monkeypatch.setattr(broker, "_data_feed", MockDataFeed())

    dt1 = datetime.date(2024, 1, 1)
    broker._clock = datetime.datetime.combine(dt1, datetime.time(15, 0))
    broker._cash = 700000

    # 插入多个初始持仓
    db.upsert_positions(
        [
            Position(
                portfolio_id="test_bt",
                dt=dt1,
                asset="000001.SZ",
                shares=1000,
                avail=1000,
                price=10.0,
                mv=10000.0,
                profit=0.0,
            ),
            Position(
                portfolio_id="test_bt",
                dt=dt1,
                asset="000002.SZ",
                shares=1000,
                avail=1000,
                price=20.0,
                mv=20000.0,
                profit=0.0,
            ),
        ]
    )
    db.upsert_asset(
        Asset(
            portfolio_id="test_bt",
            dt=dt1,
            principal=1000000,
            cash=700000,
            frozen_cash=0,
            market_value=30000,
            total=730000,
        )
    )

    # 拨动到 2024-01-03
    dt3 = datetime.date(2024, 1, 3)
    broker.set_clock(datetime.datetime.combine(dt3, datetime.time(9, 30)))

    # 验证 2024-01-02 的持仓
    dt2 = datetime.date(2024, 1, 2)
    pos2 = db.get_positions(dt2, "test_bt")
    assert len(pos2) == 2

    p1 = pos2.filter(pl.col("asset") == "000001.SZ")
    assert p1["price"][0] == 10.0  # 成本价保持不变
    assert p1["mv"][0] == 11000.0  # 市值更新为 1000 * 11.0
    assert p1["profit"][0] == 1000.0

    p2 = pos2.filter(pl.col("asset") == "000002.SZ")
    assert p2["price"][0] == 20.0  # 成本价保持不变
    assert p2["mv"][0] == 22000.0  # 市值更新为 1000 * 22.0
    assert p2["profit"][0] == 2000.0

    # 验证资产
    asset2 = db.get_asset(dt2, "test_bt")
    assert asset2.market_value == 11000.0 + 22000.0
    assert asset2.total == 700000 + 33000.0


def test_set_clock_not_overwrite_existing(broker, monkeypatch):
    """验证 set_clock 不会覆盖已存在的记录"""
    dt1 = datetime.date(2024, 1, 1)
    broker._clock = datetime.datetime.combine(dt1, datetime.time(15, 0))
    broker._cash = 1000000

    # 手动插入 2024-01-02 的资产记录，并给一个特殊的值
    special_total = 1234567.0
    db.upsert_asset(
        Asset(
            portfolio_id="test_bt",
            dt=datetime.date(2024, 1, 2),
            principal=1000000,
            cash=special_total,
            frozen_cash=0,
            market_value=0,
            total=special_total,
        )
    )

    # 拨动时钟到 2024-01-03
    dt3 = datetime.date(2024, 1, 3)
    broker.set_clock(datetime.datetime.combine(dt3, datetime.time(9, 30)))

    # 验证 2024-01-02 的记录没有被覆盖
    asset2 = db.get_asset(datetime.date(2024, 1, 2), "test_bt")
    assert asset2.total == special_total
    assert asset2.cash == special_total
