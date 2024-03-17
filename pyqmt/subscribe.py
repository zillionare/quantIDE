import datetime
import json
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from multiprocessing import Queue
from threading import Thread
from typing import List

import cfg4py
import numpy as np
import pandas as pd
from coretypes import FrameType
from xtquant import xtdata as xtd
from xtquant.xtdata import run

from pyqmt.core.constants import key_price
from pyqmt.core.context import g
from pyqmt.core.xtwrapper import get_stock_list

cfg = cfg4py.get_instance()

import itertools


def batch(iterable, size):
    it = iter(iterable)
    while item := list(itertools.islice(it, size)):
        yield item


def on_subscribe_callback(data):
    """从订阅数据中提取lastPrice，存入缓存中

    Args:
        data: 具有如下结果的json:

    ```json
    {
        '000001.SZ':
        {
            'time': 1710127194000,
            'lastPrice': 10.42,
            'open': 10.38,
            'high': 10.47,
            'low': 10.34,
            'lastClose': 10.38,
            'amount': 710422600.0,
            'volume': 683450,
            'pvolume': 68345011,
            'stockStatus': 0,
            'openInt': 13,
            'transactionNum': 0,
            'lastSettlementPrice': 0.0,
            'settlementPrice': 0.0,
            'pe': 0.0,
            'askPrice': [10.42, 10.43, 10.44, 0.0, 0.0],
            'bidPrice': [10.41, 10.4, 10.39, 0.0, 0.0],
            'askVol': [9349, 7557, 6217, 0, 0],
            'bidVol': [3119, 6685, 4865, 0, 0],
            'volRatio': 0.0,
            'speed1Min': 0.0,
            'speed5Min': 0.0
        }
    }
    ```
    """
    global cfg, f
    last_prices = {code: item["lastPrice"] for code, item in data.items()}
    g.cache.security.hset(key_price, mapping=last_prices)

    bars = [
        (
            code,
            item["time"],
            item["open"],
            item["high"],
            item["low"],
            item["lastClose"],
            item["volume"],
            item["amount"],
        )
        for code, item in data.items()
    ]
    df = pd.DataFrame(
        bars,
        columns=["symbol", "frame", "open", "high", "low", "close", "volume", "money"],
    )

    df.frame = np.array(df.frame, dtype="datetime64[ms]").astype(datetime.datetime)
    df.frame = df["frame"].dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")

    print(f"timestamp: {pd.unique(df.frame)}")
    # haystore.save_bars(FrameType.MIN1, df)
    f.write(json.dumps(data))
    f.flush()


def subscribe_live():
    os.environ[cfg4py.envar] = "DEV"
    g.init_dal()
    xtd.subscribe_whole_quote(["SH", "SZ"], on_subscribe_callback)


def sync_1m_bars(codes: List[str]):
    start_ = "20240312"
    xtd.download_history_data2(
        codes, period="1m", start_time=start_, end_time="", callback=lambda x: x
    )

    print(f"{os.getpid()} get result...")
    barss = xtd.get_market_data_ex(
        [], codes, "1m", start_time="", count=241, dividend_type="front", fill_data=True
    )
    # print(barss)
    print(f"{os.getpid()} records: {len(barss)}, {codes[0]}")


if __name__ == "__main__":
    # t = Thread(target = subscribe_live)
    # t.start()
    # t.join()
    global f
    f = open("live.json", "w", encoding="utf-8")
    subscribe_live()
    run()
