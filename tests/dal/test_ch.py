import datetime
import random
import time

import arrow
import cfg4py
import numpy as np
import pandas as pd
import pytest
from coretypes import FrameType, SecurityType

from pyqmt.dal import init_dal
from tests.config import get_config_dir, init_haystore


@pytest.fixture(scope="module", autouse=True)
def cfg():
    init_haystore()
    init_dal()
    return cfg4py.get_instance()


@pytest.mark.parametrize("n", [100, 1000, 10000])
def test_performance(n, cfg):
    n = n * 10000
    codes = [f"{i:06d}.XSHG" for i in range(1, 8000)]
    df = pd.DataFrame([], columns=["frame","symbol","open", "high", "low", "close", "volume", "money", "factor"])

    end = datetime.datetime(2023, 12, 31)
    df.frame = [end - datetime.timedelta(minutes=i) for i in range(0, n)]
    sampled = random.sample(codes, 5000) * int(n / 5000)
    df.symbol = sampled
    df.open = np.random.random(n)
    df.close = np.random.random(n)
    df.low = np.random.random(n)
    df.high = np.random.random(n)
    df.volume = np.random.random(n) * 100_0000
    df.money = np.random.random(n) * 1_0000_0000
    df.factor = np.random.random(n)

    t0 = time.time()
    cfg.haystore.save_bars(FrameType.MIN1, df.to_numpy())
    t1 = time.time()
    bars = cfg.haystore.get_bars(sampled[-1], -1, FrameType.MIN1, end)
    t2 = time.time()
    print(f"query returns {len(bars)}")
    print(f"insert cost {t1-t0:.1f} seconds, read cost {t2-t1:.1f} seconds")
    pass


    


def test_save_securities():
    cfg = cfg4py.get_instance()

    shares = ["000001.SZ", "600001.SH"]
    cfg.haystore.save_ashare_list(shares, SecurityType.STOCK, arrow.now().date())
