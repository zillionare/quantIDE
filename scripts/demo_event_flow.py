import asyncio
import datetime

from pyqmt.core.message import msg_hub
from pyqmt.service.simulation_broker import QuoteEvent, SimulationBroker


class Cfg:
    principal = 1000000


async def publish_quotes(asset: str, base_tm: datetime.datetime, price: float, volumes: list[int]):
    for i, v in enumerate(volumes):
        tm = base_tm + datetime.timedelta(seconds=i)
        msg_hub.publish(
            "md:quote:1s",
            {
                "asset": asset,
                "last_price": price,
                "volume": v,
                "up_limit": None,
                "down_limit": None,
                "low": None,
                "high": None,
                "tm": tm,
            },
        )
        await asyncio.sleep(0.01)


async def main():
    from pathlib import Path

    import pandas as pd

    from pyqmt.data import init_data
    base = Path("./.demo_store")
    (base / "data").mkdir(parents=True, exist_ok=True)
    # 预创建最小交易日历，避免远程获取
    t0 = datetime.datetime.now()
    d0 = t0.date()
    d1 = d0 + datetime.timedelta(days=1)
    d2 = d1 + datetime.timedelta(days=1)
    cal = pd.DataFrame(
        {"is_open": [1, 1, 1], "prev": [d0, d0, d1]},
        index=[d0, d1, d2],
    )
    cal.index.name = "date"
    cal.to_parquet(base / "data/calendar.parquet")
    # 预创建最小股票列表，避免远程获取
    stocks = pd.DataFrame(
        {
            "asset": ["000001.SZ"],
            "name": ["平安银行"],
            "pinyin": ["PAYH"],
            "list_date": [pd.Timestamp("1991-04-03").date()],
            "delist_date": [pd.NaT],
        }
    )
    stocks.to_parquet(base / "data/stock_list.parquet", index=False)
    # 初始化本地数据与SQLite
    init_data(base)
    broker = SimulationBroker(Cfg())
    asset = "000001.SZ"
    buy_task = asyncio.create_task(broker.buy(asset=asset, shares=500, price=10.0, bid_time=t0, timeout=2.0))
    await publish_quotes(asset, t0, 10.0, [100, 100, 100, 100, 100])
    await buy_task
    t1_date = (t0.date() + datetime.timedelta(days=1))
    broker.on_day_open(t1_date)
    sell_tm = t0 + datetime.timedelta(days=1)
    sell_task = asyncio.create_task(broker.sell(asset=asset, shares=500, price=10.5, bid_time=sell_tm, timeout=2.0))
    await publish_quotes(asset, sell_tm, 10.5, [200, 300])
    await sell_task
    broker.on_day_close(t1_date)
    t2_date = t1_date + datetime.timedelta(days=1)
    broker.on_day_open(t2_date)
    from pyqmt.data.sqlite import db
    trades = db.trades_all()
    positions = db.positions_all()
    assets = db.assets_all()
    print(
        {
            "trades": None if trades is None else trades.head(5).to_dicts(),
            "positions": [] if positions is None else positions.to_dicts(),
            "assets": [] if assets is None else assets.to_dicts(),
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
