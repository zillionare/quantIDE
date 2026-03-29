import numpy as np
import pandas as pd
import polars as pl
import pytest

from quantide.data.helper import hfq_adjustment, qfq_adjustment


@pytest.fixture
def sample_data():
    data = {
        "asset": ["000001.SZ"] * 3 + ["000002.SZ"] * 3,
        "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"] * 2),
        "open": [10.0, 11.0, 12.0, 20.0, 21.0, 22.0],
        "high": [10.5, 11.5, 12.5, 20.5, 21.5, 22.5],
        "low": [9.5, 10.5, 11.5, 19.5, 20.5, 21.5],
        "close": [10.2, 11.2, 12.2, 20.2, 21.2, 22.2],
        "volume": [1000, 1100, 1200, 2000, 2100, 2200],
        "adjust": [1.0, 1.1, 1.2, 1.0, 1.0, 1.0],
    }
    return pd.DataFrame(data)


def test_qfq_adjustment_return_types(sample_data):
    # Test eager_mode=True returns pl.DataFrame
    result = qfq_adjustment(sample_data, eager_mode=True)
    assert isinstance(result, pl.DataFrame)

    # Test eager_mode=False returns pl.LazyFrame
    result = qfq_adjustment(sample_data, eager_mode=False)
    assert isinstance(result, pl.LazyFrame)

    # Test input as pl.DataFrame
    result = qfq_adjustment(pl.from_pandas(sample_data), eager_mode=True)
    assert isinstance(result, pl.DataFrame)


def test_hfq_adjustment_return_types(sample_data):
    # Test eager_mode=True returns pl.DataFrame
    result = hfq_adjustment(sample_data, eager_mode=True)
    assert isinstance(result, pl.DataFrame)

    # Test eager_mode=False returns pl.LazyFrame
    result = hfq_adjustment(sample_data, eager_mode=False)
    assert isinstance(result, pl.LazyFrame)


def test_qfq_adjustment_values(sample_data):
    result = qfq_adjustment(sample_data, eager_mode=True)

    # For 000001.SZ, latest_adj_factor is 1.2
    # 2024-01-01: close = 10.2 * 1.0 / 1.2 = 8.5
    # 2024-01-02: close = 11.2 * 1.1 / 1.2 = 10.2666...
    # 2024-01-03: close = 12.2 * 1.2 / 1.2 = 12.2

    s1 = result.filter(pl.col("asset") == "000001.SZ").sort("date")
    assert np.allclose(
        s1["close"].to_list(), [10.2 * 1.0 / 1.2, 11.2 * 1.1 / 1.2, 12.2], atol=1e-5
    )

    # Volume: volume * latest_adj_factor / adj_factor
    # 2024-01-01: 1000 * 1.2 / 1.0 = 1200
    assert np.allclose(
        s1["volume"].to_list(), [1000 * 1.2 / 1.0, 1100 * 1.2 / 1.1, 1200], atol=1e-5
    )


def test_hfq_adjustment_values(sample_data):
    result = hfq_adjustment(sample_data, eager_mode=True)

    # For 000001.SZ, base_adj_factor is 1.0
    # 2024-01-01: close = 10.2 * 1.0 / 1.0 = 10.2
    # 2024-01-02: close = 11.2 * 1.1 / 1.0 = 12.32
    # 2024-01-03: close = 12.2 * 1.2 / 1.0 = 14.64

    s1 = result.filter(pl.col("asset") == "000001.SZ").sort("date")
    assert np.allclose(s1["close"].to_list(), [10.2, 11.2 * 1.1, 12.2 * 1.2], atol=1e-5)

    # Volume should remain unchanged for hfq
    assert np.allclose(s1["volume"].to_list(), [1000, 1100, 1200], atol=1e-5)
